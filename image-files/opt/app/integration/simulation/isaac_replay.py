"""Verified exported-action replay with fixed declared timing and true final feedback.

Defaults preserve original recorded send offsets. --time-scale N explicitly uses
one constant slow-motion factor; no per-frame convergence waits alter cadence.
Pass gates: inspected contract, exact sent actions (1e-3), actual schedule errors
<=0.30s, post-send terminal observations, target tracking and recorded trajectory.
Body RMSE/max limits remain 1.5/4 deg for target tracking; recorded-state replay
uses 2/4 deg. Gripper normalized limits are 3/8. These are interface acceptance
bounds, not task accuracy. Every failure writes a report and exits nonzero.
"""
import argparse
from dataclasses import replace
from fractions import Fraction
import json
from pathlib import Path
import time
import numpy as np
from lerobot_sdk import RobotSession, JointTarget, JOINT_NAMES, DatasetManager
from examples.isaac_collect import build_robot_config
from integration.replay_support import validate_binding,timing_metrics,fresh_observation,accepted,interpolate_schedule


def read_episode(dataset_dir, episode_index):
    import pyarrow.parquet as pq
    rows=[]
    for path in sorted((Path(dataset_dir)/'data').rglob('*.parquet')):
        rows.extend(r for r in pq.read_table(path).to_pylist() if r['episode_index']==episode_index)
    rows.sort(key=lambda r:r['frame_index'])
    if not rows or [r['frame_index'] for r in rows]!=list(range(len(rows))):
        raise ValueError('Missing episode or non-contiguous frame indices')
    return rows


def write_video(path, frames, captured_ns):
    import av
    with av.open(str(path),'w') as container:
        stream=container.add_stream('libx264',rate=30)
        stream.width=frames[0].shape[1];stream.height=frames[0].shape[0];stream.pix_fmt='yuv420p'
        stream.time_base=Fraction(1,1000000);stream.codec_context.time_base=Fraction(1,1000000)
        previous=-1
        for rgb,ts in zip(frames,captured_ns):
            frame=av.VideoFrame.from_ndarray(rgb,format='rgb24')
            frame.pts=max(previous+1,round((ts-captured_ns[0])/1000));previous=frame.pts
            frame.time_base=Fraction(1,1000000)
            for packet in stream.encode(frame):container.mux(packet)
        for packet in stream.encode():container.mux(packet)


def metrics(error, body_rmse, body_max, gripper_rmse=3, gripper_max=8):
    result={}
    for i,name in enumerate(JOINT_NAMES):
        rmse=float(np.sqrt(np.mean(error[:,i]**2)));maximum=float(np.abs(error[:,i]).max())
        rlim=gripper_rmse if i==5 else body_rmse;mlim=gripper_max if i==5 else body_max
        result[name]={'rmse':rmse,'max':maximum,'rmse_limit':rlim,'max_limit':mlim,'passed':rmse<=rlim and maximum<=mlim}
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--dataset-dir',type=Path,required=True);p.add_argument('--episode',type=int,default=0)
    p.add_argument('--ipc-dir',type=Path,required=True);p.add_argument('--contract',type=Path,default=Path('integration/contract.json'))
    p.add_argument('--out-dir',type=Path,required=True);p.add_argument('--time-scale',type=float,default=1)
    p.add_argument('--substeps',type=int,default=1,help='Fixed linear subdivisions; original sample values/times preserved')
    p.add_argument('--smoke',action='store_true')
    p.add_argument('--human-sim-envelope',action='store_true',help='Simulation only: max step6 and velocity60 in each SDK joint unit; position limits and pass gates unchanged')
    args=p.parse_args()
    if not np.isfinite(args.time_scale) or not 1<=args.time_scale<=10:p.error('time-scale must be 1..10')
    args.out_dir.mkdir(parents=True,exist_ok=False)
    report={'scope':'fixed-cadence exported-action replay; not task success', 'dataset_dir':str(args.dataset_dir),
            'episode':args.episode,'time_scale':args.time_scale,'passed':False,
            'pass_rule':{'sent_fidelity_max':.001,'schedule_max_error_s':.30,'tracking_body_rmse_max':[1.5,4],
                         'recorded_state_body_rmse_max':[2,4],'gripper_rmse_max':[3,8],
                         'final_observation':'fresh after last send, three stable samples within body1.5/gripper3',
                         'init_max_error_body_deg':1.5,'init_max_error_gripper':3},
            'backend':'FakeRobot smoke' if args.smoke else 'FileSimulationBackend'}
    ticks=[];videos={'front':[],'wrist':[]};video_ns={'front':[],'wrist':[]}
    try:
        # All checks precede backend creation or any motion.
        inspection=DatasetManager().inspect(args.dataset_dir)
        manifest=json.loads((args.dataset_dir/'manifest.json').read_text())
        contract=json.loads(args.contract.read_text(encoding='utf-8'))
        validate_binding(manifest,contract)
        episode=read_episode(args.dataset_dir,args.episode)
        actions=np.asarray([r['action'] for r in episode],dtype=np.float64)
        recorded=np.asarray([r['observation.state'] for r in episode],dtype=np.float64)
        ts=[int(r['sdk.sent_monotonic_ns']) for r in episode]
        if any(b<=a for a,b in zip(ts,ts[1:])):raise ValueError('Invalid original send timeline')
        config=build_robot_config(contract,{k:tuple(v) for k,v in manifest['camera_shapes'].items()},int(manifest['fps']))
        if args.human_sim_envelope:
            config=replace(config,max_step={k:6.0 for k in JOINT_NAMES},max_velocity={k:60.0 for k in JOINT_NAMES})
        report['execution_envelope']={'simulation_only':True,'human_sim_envelope':args.human_sim_envelope,
            'max_step':dict(config.max_step),'max_velocity':dict(config.max_velocity),'position_limits':dict(config.limits)}
        for row in actions:config.validate_target(JointTarget(dict(zip(JOINT_NAMES,row)),config.units,config.calibration_hash))
        source_actions=actions.copy();source_ts=list(ts)
        expanded,ts,source_indices=interpolate_schedule(actions.tolist(),ts,args.substeps)
        actions=np.asarray(expanded,dtype=np.float64)
        report.update(content_hash=inspection.ref.content_hash,frames=len(actions),source_frames=len(source_actions),
                      substeps=args.substeps,source_indices=source_indices,preflight_passed=True)
        if args.smoke:
            from lerobot_sdk.testing import FakeRobot
            backend=FakeRobot(resource_id='replay-fixed-smoke',cameras=('front','wrist'))
        else:
            from lerobot_sdk.testing.file_sim import FileSimulationBackend
            backend=FileSimulationBackend(args.ipc_dir,calibration_hash=config.calibration_hash)
        task=manifest['task_labels'][0]
        def vector(obs):return np.array([obs.arm.positions[k] for k in JOINT_NAMES])
        def frame(obs):
            for key in videos:
                videos[key].append(obs.images[key].rgb);video_ns[key].append(obs.images[key].captured_monotonic_ns)
        with RobotSession(config,backend,lock_dir=args.out_dir/'locks') as robot:
            getter=lambda:robot.get_observation(task,max_age_s=1)
            report['initial_before']=vector(getter()).tolist()
            # New session, fixed target and unchanged envelope. Do not manufacture measured state.
            init=JointTarget(dict(zip(JOINT_NAMES,recorded[0])),config.units,config.calibration_hash)
            init_limit=np.array([1.5]*5+[3.]);post=None
            with robot._control_scope('inference',deadline_s=30):
                for _ in range(80):
                    receipt=robot.move_to(init,mode='servo')
                    post=fresh_observation(getter,after_ns=receipt.sent_monotonic_ns,timeout_s=2)
                    if np.all(np.abs(vector(post)-recorded[0])<=init_limit):break
                report['initial_after']=vector(post).tolist()
                report['initial_error']=np.abs(vector(post)-recorded[0]).tolist()
                if not np.all(np.abs(vector(post)-recorded[0])<=init_limit):raise RuntimeError('Recorded initial state not reached within preset tolerance')
                # Ending correction HOLDs the measured pose; recheck the initial state below.
                duration=(ts[-1]-ts[0])/1e9*args.time_scale
            with robot._control_scope('inference',deadline_s=duration+60):
                # Scope exit above holds; use and report a final fresh initial check.
                post=fresh_observation(getter,after_ns=post.arm.captured_monotonic_ns,timeout_s=2)
                report['replay_start_error']=np.abs(vector(post)-recorded[0]).tolist()
                if not np.all(np.abs(vector(post)-recorded[0])<=init_limit):raise RuntimeError('Initial state drift after HOLD')
                start_ns=time.monotonic_ns()+200_000_000
                for i,row in enumerate(actions):
                    due=start_ns+round((ts[i]-ts[0])*args.time_scale)
                    time.sleep(max(0,(due-time.monotonic_ns())/1e9))
                    obs=(fresh_observation(getter,after_ns=ticks[-1]['replay_send_ns'],timeout_s=2)
                         if ticks else getter())
                    receipt=robot.move_to(JointTarget(dict(zip(JOINT_NAMES,row)),config.units,config.calibration_hash),mode='servo')
                    tick={'index':i,'recorded_send_ns':ts[i],'planned_send_ns':due,'replay_send_ns':receipt.sent_monotonic_ns,
                          'before_capture_ns':obs.arm.captured_monotonic_ns,'before_sequence':obs.arm.sequence,
                          'action':row.tolist(),'sent':[receipt.sent_target.positions[k] for k in JOINT_NAMES],
                          'measured_before':vector(obs).tolist()}
                    ticks.append(tick);frame(obs)
                    # Persist evidence during the run, including failed attempts.
                    with (args.out_dir/'ticks.jsonl').open('a',encoding='utf-8') as f:f.write(json.dumps(tick)+'\n')
                # Last target gets fresh feedback BEFORE any stop/HOLD. No pre-send substitution.
                last_send=ticks[-1]['replay_send_ns'];last_target=np.asarray(ticks[-1]['sent'])
                samples=[];stable=0;end=time.monotonic()+3;after=last_send
                while time.monotonic()<end:
                    obs=fresh_observation(getter,after_ns=after,timeout_s=2);after=obs.arm.captured_monotonic_ns
                    error=np.abs(vector(obs)-last_target);stable=stable+1 if np.all(error<=init_limit) else 0
                    samples.append({'captured_monotonic_ns':after,'sequence':obs.arm.sequence,'positions':vector(obs).tolist(),'error':error.tolist()})
                    frame(obs)
                    if stable>=3:break
                report['final_samples']=samples;report['final_settled']=stable>=3
                final_obs=obs
            report['holding_after_replay']=robot.status().state.value
        before=np.array([t['measured_before'] for t in ticks]);sent=np.array([t['sent'] for t in ticks])
        after=np.vstack([before[1:],vector(final_obs)])
        after_ns=[t['before_capture_ns'] for t in ticks[1:]]+[final_obs.arm.captured_monotonic_ns]
        actual_ts=[t['replay_send_ns'] for t in ticks]
        timing=timing_metrics(ts,actual_ts,start_ns,args.time_scale,.30)
        track=metrics(after-sent,1.5,4);trajectory=metrics(before[source_indices]-recorded,2,4)
        fidelity=float(np.abs(sent-actions).max())
        fresh_all=all(a>b for a,b in zip(after_ns,actual_ts))
        checks={'fidelity':fidelity<=.001,'timing':timing['passed'],'fresh_final':fresh_all and report['final_settled'],
                'tracking':all(x['passed'] for x in track.values()),'recorded_trajectory':all(x['passed'] for x in trajectory.values())}
        report.update(timing=timing,tracking=track,recorded_trajectory=trajectory,sent_fidelity_max=fidelity,
                      all_post_send_feedback_fresh=fresh_all,checks=checks,passed=accepted(checks))
        np.savez(args.out_dir/'replay-trajectories.npz',actions=actions,sent=sent,recorded_states=recorded,
                 source_actions=source_actions,source_send_monotonic_ns=np.array(source_ts,dtype=np.int64),
                 source_indices=np.array(source_indices,dtype=np.int64),
                 measured_before=before,measured_after=after,recorded_send_monotonic_ns=np.array(ts,dtype=np.int64),
                 replay_send_monotonic_ns=np.array(actual_ts,dtype=np.int64),
                 measured_before_monotonic_ns=np.array([t['before_capture_ns'] for t in ticks],dtype=np.int64),
                 measured_after_monotonic_ns=np.array(after_ns,dtype=np.int64),
                 planned_send_monotonic_ns=np.array([t['planned_send_ns'] for t in ticks],dtype=np.int64))
        for key in videos:write_video(args.out_dir/f'replay-{key}.mp4',videos[key],video_ns[key])
        report['video']={k:{'frames':len(videos[k]),'capture_span_s':(video_ns[k][-1]-video_ns[k][0])/1e9,'pts_source':'actual camera capture time'} for k in videos}
    except Exception as exc:
        report.update(passed=False,error=repr(exc))
    finally:
        (args.out_dir/'replay-report.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps({'passed':report['passed'],'report':str(args.out_dir/'replay-report.json'),'error':report.get('error')}))
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
