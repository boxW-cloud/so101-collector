"""Actual ACT -> PolicySession/InferenceRunner -> independent Isaac, with stop probes.
No physical device I/O; no expert or exported action replay supplies policy actions.
"""
import argparse,json,time,threading,queue
from dataclasses import asdict
from pathlib import Path
from contextlib import contextmanager


@contextmanager
def prepared_inference(manager,model_path,config,backend,device):
    from lerobot_sdk import RobotSession
    with manager.load(model_path,device=device) as model:
        with model.create_session('Move the simulated arm',config) as session:
            with RobotSession(config,backend) as robot:
                yield session,robot


def validate_model_binding(contract, fixture):
    expected={'robot_id':fixture['robot_id'],'calibration_hash':fixture['calibration_hash'],
              'joint_names':fixture['names'],'units':[fixture['units'][n] for n in fixture['names']]}
    for k,v in expected.items():
        actual=list(contract[k]) if isinstance(v,list) else contract[k]
        if actual!=v:
            raise ValueError('Model/Isaac contract mismatch: '+k)


def video_pts(stamp,start):
    if stamp<start:raise ValueError('Video timestamp precedes first frame')
    return (stamp-start)//1000


def main():
    import torch,numpy as np,av
    from fractions import Fraction
    from lerobot_sdk import ModelManager,RobotSession,RobotConfig,JOINT_NAMES,InferenceRunner,RunConfig,RunState,OperationFailed
    from lerobot_sdk.testing.file_sim import FileSimulationBackend
    p=argparse.ArgumentParser();p.add_argument('--model',type=Path,required=True);p.add_argument('--ipc',type=Path,required=True);p.add_argument('--contract',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--device',default='cpu');p.add_argument('--observation-budget',type=float,default=2);p.add_argument('--steps',type=int,default=40)
    p.add_argument('--viewer-root',type=Path);a=p.parse_args()
    a.output.mkdir(parents=True,exist_ok=False)
    torch.set_num_threads(2)
    manager=ModelManager();manifest=manager.inspect(a.model);fixture=json.loads(a.contract.read_text())
    validate_model_binding(manifest.contract,fixture)
    config=RobotConfig(robot_id=fixture['robot_id'],calibration_hash=fixture['calibration_hash'],units=fixture['units'],limits={k:tuple(v) for k,v in fixture['limits'].items()},
        max_velocity={k:30. for k in JOINT_NAMES},max_step={k:3. for k in JOINT_NAMES},tolerance={k:.75 for k in JOINT_NAMES},gripper_open=80,gripper_closed=20,
        fps=manifest.contract['fps'],stable_frames=2,action_timeout_s=10,io_timeout_s=2,max_observation_age_s=a.observation_budget,max_sensor_skew_s=.5,required_cameras=tuple(manifest.contract['camera_keys']),event_capacity=256)
    report={'scope':'real '+manifest.policy_type+' model; functional simulation inference, not grasp success','model_hash':manifest.content_hash,'dataset_hash':manifest.dataset_hash,
            'source_is_policy':True,'physical_motor_writes':0,'observation_budget_s':a.observation_budget,'runs':{},'video_dropped_frames':0}
    frames=queue.Queue(64);video_error=[];trace=(a.output/'io-trace.jsonl').open('x');policy_trace=(a.output/'policy-trace.jsonl').open('x')
    phase={'name':'initializing'};active_run=[None];monitor_exit=threading.Event()
    def monitor():
        while not monitor_exit.wait(.05):
            if a.viewer_root and (a.viewer_root/'stop.request').exists() and active_run[0] is not None:
                (a.viewer_root/'stop.request').unlink();active_run[0].stop()
    monitor_thread=threading.Thread(target=monitor,daemon=True);monitor_thread.start()
    def write_video():
        sinks={};starts={};last={}
        try:
            while True:
                item=frames.get()
                if item is None:break
                camera,stamp,rgb=item
                if a.viewer_root:
                    from PIL import Image
                    temp=a.viewer_root/(camera+'-next.jpg');Image.fromarray(rgb).save(temp,quality=80);temp.replace(a.viewer_root/(camera+'-latest.jpg'))
                if camera not in sinks:
                    container=av.open(str(a.output/(camera+'.mp4')),'w');stream=container.add_stream('libx264',rate=10);stream.width=rgb.shape[1];stream.height=rgb.shape[0];stream.pix_fmt='yuv420p';stream.time_base=Fraction(1,1000000);stream.codec_context.time_base=Fraction(1,1000000);stream.options={'preset':'ultrafast','crf':'23'};sinks[camera]=(container,stream);starts[camera]=stamp
                pts=video_pts(stamp,starts[camera])
                if pts<=last.get(camera,-1):continue
                last[camera]=pts;container,stream=sinks[camera];frame=av.VideoFrame.from_ndarray(rgb,format='rgb24');frame.pts=pts;frame.time_base=Fraction(1,1000000)
                for packet in stream.encode(frame):container.mux(packet)
        except Exception as e:video_error.append(repr(e))
        finally:
            for container,stream in sinks.values():
                for packet in stream.encode():container.mux(packet)
                container.close()
    video=threading.Thread(target=write_video);video.start()
    class AuditedIsaac(FileSimulationBackend):
        def __init__(self,*args,**kwargs):super().__init__(*args,**kwargs);self.sends=0;self.holds=0;self.latest=None
        def sample(self,timeout_s):
            snap=super().sample(timeout_s);self.latest=snap
            if a.viewer_root:
                data={'phase':phase['name'],'policy':manifest.policy_type,'model_hash':manifest.content_hash,'stamp':snap.captured_monotonic_ns,'positions':dict(snap.positions),'sends':self.sends,'holds':self.holds}
                temp=a.viewer_root/'status.tmp';temp.write_text(json.dumps(data));temp.replace(a.viewer_root/'status.json')
            trace.write(json.dumps({'phase':phase['name'],'kind':'sample','stamp':snap.captured_monotonic_ns,'positions':dict(snap.positions)})+'\n');trace.flush()
            for k,image in snap.images.items():
                try:frames.put_nowait((k,image.captured_monotonic_ns,image.rgb))
                except queue.Full:report['video_dropped_frames']+=1
            return snap
        def send(self,positions,timeout_s):
            sent=super().send(positions,timeout_s);self.sends+=1
            trace.write(json.dumps({'phase':phase['name'],'kind':'send','stamp':time.monotonic_ns(),'positions':dict(sent),'send_count':self.sends})+'\n');trace.flush();return sent
        def hold(self,positions,timeout_s):
            super().hold(positions,timeout_s);self.holds+=1
            trace.write(json.dumps({'phase':phase['name'],'kind':'hold','stamp':time.monotonic_ns(),'send_count':self.sends})+'\n');trace.flush()
    backend=AuditedIsaac(a.ipc,calibration_hash=config.calibration_hash)
    def config_run(steps=None,mode='chunk'):return RunConfig(mode=mode,max_steps=a.steps if steps is None else steps,max_duration_s=180,max_observation_age_s=a.observation_budget,inference_timeout_s=max(15,a.observation_budget+5),queue_capacity=16,low_watermark=2,stop_timeout_s=5)
    try:
        with prepared_inference(manager,a.model,config,backend,a.device) as (session,robot):
            obs=robot.get_observation('Move the simulated arm')
            for k,shape in manifest.contract['camera_shapes'].items():assert tuple(obs.images[k].rgb.shape)==tuple(shape)
            runner=InferenceRunner();native_packet=session._packet
            def observed(*args,**kwargs):
                packet=native_packet(*args,**kwargs)
                policy_trace.write(json.dumps({'phase':phase['name'],'source_sequence':packet.chunk.source_sequence,'generated_ns':packet.generated_monotonic_ns,'source_ns':packet.captured_monotonic_ns,'actions':packet.chunk.values.tolist(),'model_hash':packet.model_version})+'\n');policy_trace.flush()
                return packet
            session._packet=observed
            phase['name']='continuous'
            run=runner.start(robot,session,config_run());active_run[0]=run
            try:result=run.wait(100)
            finally:
                if not run._done.is_set():run.stop()
                run.wait_inference(30)
            report['runs']['continuous']={'result':asdict(result),'status':asdict(run.status())}
            assert result.steps==a.steps and result.holding
            phase['name']='step_mode';session.reset()
            run=runner.start(robot,session,config_run(4,mode='step'));active_run[0]=run
            try:step_result=run.wait(120)
            finally:
                if not run._done.is_set():run.stop()
                run.wait_inference(30)
            report['runs']['step_mode']={'result':asdict(step_result),'status':asdict(run.status())};assert step_result.steps==4
            phase['name']='explicit_stop'
            run=runner.start(robot,session,config_run(500));active_run[0]=run;deadline=time.monotonic()+40
            while run.status().steps<3:
                if run._done.is_set():run.wait();raise AssertionError('Run ended before stop probe')
                if time.monotonic()>deadline:raise TimeoutError('No model actions before stop')
                time.sleep(.03)
            t=time.monotonic()
            if a.viewer_root:
                (a.viewer_root/'stop.request').write_text('external UI stop verification')
                run.wait(5)
            else:run.stop()
            elapsed=time.monotonic()-t;run.wait(5);run.wait_inference(30);count=backend.sends;time.sleep(.6)
            report['runs']['explicit_stop']={'stop_s':elapsed,'status':asdict(run.status()),'no_late_sends':backend.sends==count};assert backend.sends==count and run.status().holding
            # Execute the real model, then hold its completed result in a
            # test barrier while stop proceeds through the independent I/O thread.
            phase['name']='blocked_compute_stop';entered=threading.Event();release=threading.Event()
            def blocked(*args,**kwargs):
                packet=observed(*args,**kwargs);entered.set()
                if not release.wait(10):raise TimeoutError('Test barrier deadline')
                return packet
            session._packet=blocked
            run=runner.start(robot,session,config_run(500));active_run[0]=run
            try:
                if not entered.wait(30):raise TimeoutError('Real model did not reach barrier')
                t=time.monotonic();run.stop();elapsed=time.monotonic()-t;pending=run.status().inference_pending;count=backend.sends
            finally:release.set()
            run.wait(5);run.wait_inference(30);time.sleep(.6)
            report['runs']['blocked_compute_stop']={'stop_s':elapsed,'inference_pending_when_stopped':pending,'no_late_sends':backend.sends==count,'status':asdict(run.status())};assert pending and backend.sends==count and run.status().holding
            phase['name']='expired_packet';session._packet=observed
            def expired(*args,**kwargs):
                packet=observed(*args,**kwargs);time.sleep(a.observation_budget+.2);return packet
            session._packet=expired;count=backend.sends
            run=runner.start(robot,session,config_run(500));active_run[0]=run
            try:run.wait(a.observation_budget+30)
            except OperationFailed as e:report['expiry_error']=str(e)
            else:raise AssertionError('Expired model packet was not rejected')
            run.wait_inference(30)
            st=run.status();report['runs']['expired_packet']={'status':asdict(st),'no_sends':backend.sends==count};assert st.rejected_actions>0 and st.holding and backend.sends==count
            session._packet=observed
            report['robot_borrowing_preserved']=robot.get_arm_data().sequence>0
        report['passed']=True
    except BaseException as e:
        report.update(passed=False,error=repr(e));raise
    finally:
        monitor_exit.set();monitor_thread.join(2)
        frames.put(None);video.join(30);trace.close();policy_trace.close()
        if video.is_alive() or video_error:report.update(passed=False,video_error=video_error or ['writer did not finish'])
        report['total_sends']=backend.sends;report['total_holds']=backend.holds
        (a.output/'report.json').write_text(json.dumps(report,default=str,indent=2),encoding='utf-8')
    if not report['passed']:raise RuntimeError('Video/evidence validation failed')
    print(json.dumps(report,default=str))

if __name__=='__main__':main()
