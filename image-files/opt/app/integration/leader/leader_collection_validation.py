"""Interactive acceptance through public CollectionTask, real leader and ROS2 Isaac."""
from dataclasses import asdict
import argparse
import json
from pathlib import Path
import threading
import time
from PIL import Image
from lerobot_sdk import RobotSession, CollectionManager, CollectionConfig, DatasetManager, CollectionState
from lerobot_sdk.testing.file_sim import FileSimulationBackend
from integration.leader_collection_input import BorrowedLeader, atomic
from examples.isaac_collect import build_robot_config, episode_stats
from integration.replay_support import committed_snapshot


def attach_source(workspace, parameters):
    (workspace/'meta/simulation-source.json').write_text(json.dumps(parameters,indent=2))
    return {}


def main():
    p=argparse.ArgumentParser()
    for key in ('ipc','contract','profile','leader','output','viewer','ros_contract','scene_record'):
        p.add_argument('--'+key.replace('_','-'),type=Path,required=True)
    a=p.parse_args()
    a.output.mkdir(exist_ok=True)
    contract=json.loads(a.contract.read_text()); profile=json.loads(a.profile.read_text())
    config=build_robot_config(contract,{'front':(480,640,3),'wrist':(480,640,3)},10)
    # Expensive native imports before connecting or borrowing the robot lease.
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()
    leader_trace=(a.output/'leader-used.jsonl').open('x')
    leader=BorrowedLeader(a.leader,profile,config,{k:0 for k in config.units},leader_trace)
    report={'scope':'physical leader read-only -> CollectionTask -> ROS2 -> Isaac virtual follower',
            'physical_follower_connected':False,'control_writes_to_leader':0,'profile_hash':profile['profile_hash'],
            'mapping':'ID2/3/4 accepted three-point; ID1/5 rebased relative +/-20deg; gripper Seeed normalized; current session inward simulation margin 2deg/.5gripper',
            'saved':[],'operations':[]}
    cm=CollectionManager()
    def new_task(name):
        return cm.create_task(CollectionConfig(name=name,description='human native leader; ROS2 Isaac follower',
            root=a.output/'tasks',fps=10,task='Human teleoperation functional collection; not task-success demonstration',
            robot_id=config.robot_id,calibration_hash=config.calibration_hash,units=config.units,
            camera_shapes={'front':(480,640,3),'wrist':(480,640,3)},leader_timeout_s=.25,max_duration_s=1200))
    task_path=a.output/'tasks/human-leader-isaac'
    if task_path.exists():
        task=cm.open_task(task_path)
        report['recovered_committed_episodes']=len(task._commits)
        report['recovered_committed_hashes']=[e['content_hash'] for e in task._commits]
    else:task=new_task('human-leader-isaac')
    report['task_directory']=str(task.directory)
    backend=FileSimulationBackend(a.ipc,calibration_hash=config.calibration_hash)
    exit_view=threading.Event()
    try:
        with RobotSession(config,backend,lock_dir=a.output/'locks') as robot:
            leader.anchor_sim=dict(robot.get_arm_data().positions)
            def view():
                while not exit_view.wait(.2):
                    try:
                        obs=robot.get_observation('human leader collection')
                        for k,frame in obs.images.items():
                            tmp=a.viewer/(k+'-next.jpg');Image.fromarray(frame.rgb).save(tmp,quality=80);tmp.replace(a.viewer/(k+'-latest.jpg'))
                        state={'phase':task.status().state.value,'task':asdict(task.status()),'robot_state':robot.status().state.value,
                            'positions':dict(obs.arm.positions),'leader_connected':leader.is_connected,'physical_follower':False}
                        state['task']['state']=task.status().state.value
                        atomic(a.viewer/'status.json',state)
                    except Exception as exc:
                        atomic(a.viewer/'status.json',{'phase':task.status().state.value,'error':repr(exc),
                            'task':{'state':task.status().state.value,'current_frames':task.status().current_frames},'robot_state':robot.status().state.value})
            viewer=threading.Thread(target=view,daemon=True);viewer.start()
            atomic(a.output/'ready.json',{'ready':True,'task_dir':str(task.directory)})
            last=-1;deadline=time.monotonic()+1500
            while time.monotonic()<deadline:
                if (a.viewer/'stop.request').exists():
                    robot.stop('viewer stop');task.stop(False).wait(120)
                    raise RuntimeError('User stopped acceptance')
                try:command=json.loads((a.output/'operator-command.json').read_text())
                except (OSError,ValueError):time.sleep(.05);continue
                if command['sequence']<=last:time.sleep(.05);continue
                last=command['sequence'];op=command['operation'];started=time.monotonic()
                before_sends=None
                if op=='start': task.start(robot,leader).wait(120)
                elif op=='next': task.start_episode().wait(60)
                elif op=='save': report['saved'].append(asdict(task.save_episode().wait(180)))
                elif op=='redo':
                    before=committed_snapshot(task.directory,task._commits);discard=task._episode_dir;frames=task.status().current_frames
                    task.redo_episode().wait(120)
                    report['redo']={'discarded_frames':frames,'discarded_removed':not discard.exists(),
                        'committed_unchanged':before==committed_snapshot(task.directory,task._commits)}
                    assert all(report['redo'][k] for k in ('discarded_removed','committed_unchanged')) and frames>0
                elif op=='finish':
                    ref=task.stop(True).wait(300)
                    report.update(final_state=task.status().state.value,borrowed_leader_connected=leader.is_connected,
                        borrowed_robot_connected=robot.get_arm_data().sequence>0,robot_after_stop=robot.status().state.value)
                    report['source_ref']={k:str(v) for k,v in asdict(ref).items()}
                    manager=DatasetManager();checked=manager.inspect(ref)
                    export=manager.export(ref,a.output/'export').wait(300)
                    assert export.content_hash==ref.content_hash
                    again=manager.export(ref,a.output/'export').wait(300)
                    assert again==export
                    source={'source_kind':'human_native_leader_simulation','physical_leader':True,'physical_follower':False,
                        'profile':profile,'mapping':report['mapping'],'relative_anchor_raw':leader.anchor_raw,'relative_anchor_sim':leader.anchor_sim,
                        'ros_contract':json.loads(a.ros_contract.read_text()),'scene':json.loads(a.scene_record.read_text()),
                        'leader_input_time':'Spark-issued one-use .8s challenge; Linux receiver monotonic expiry; no cross-host clock subtraction'}
                    source['leader_input_sessions']=[{'file':path.name,'records':[json.loads(line) for line in path.read_text().splitlines() if line]}
                        for path in (a.output/'prior-leader-used.jsonl',a.output/'leader-used.jsonl') if path.exists()]
                    if report.get('recovered_committed_hashes'):
                        assert [e['content_hash'] for e in task._commits][:len(report['recovered_committed_hashes'])]==report['recovered_committed_hashes']
                        report['recovered_committed_unchanged']=True
                    final=manager.migrate(export,a.output/'final',attach_source,parameters=source).wait(300)
                    inspection=manager.inspect(final)
                    report.update(final_ref={k:str(v) for k,v in asdict(final).items()},episodes=inspection.episodes,frames=inspection.frames,
                        schema=inspection.schema,camera_keys=inspection.camera_keys,native_readable=inspection.native_readable,
                        export_idempotent=True,episode_stats=episode_stats(final.root))
                    report['collection_passed']=bool(inspection.episodes>=2 and report.get('redo',{}).get('committed_unchanged')
                        and report['borrowed_robot_connected'] and report['borrowed_leader_connected'] and report['final_state']=='LOCAL_READY')
                elif op=='fault_start':
                    assert report.get('collection_passed')
                    task=new_task('human-leader-stale-probe');task.start(robot,leader).wait(120)
                elif op=='fault_check':
                    end=time.monotonic()+5
                    while task.status().state!=CollectionState.RECOVERABLE and time.monotonic()<end:time.sleep(.05)
                    report['stale_probe']={'state':task.status().state.value,'error':task.status().error,
                        'robot_state':robot.status().state.value,'leader_connected':leader.is_connected}
                    assert task.status().state==CollectionState.RECOVERABLE and robot.status().state.value=='HOLDING'
                    report['stale_probe']['passed']=True
                elif op=='exit':
                    report['passed']=bool(report.get('collection_passed') and report.get('stale_probe',{}).get('passed'))
                    atomic(a.output/'report.json',report)
                    break
                else:raise ValueError('Unknown operation')
                record={'sequence':last,'operation':op,'elapsed_s':time.monotonic()-started,'state':task.status().state.value,
                    'frames':task.status().current_frames,'committed':task.status().committed_episodes}
                report['operations'].append(record)
                atomic(a.output/'response.json',record);atomic(a.output/'progress.json',report)
            else:raise TimeoutError('Acceptance interaction deadline')
    except BaseException as exc:
        report['error']=repr(exc);report['passed']=False;atomic(a.output/'report.json',report)
        raise
    finally:
        exit_view.set()
        if task._thread and task._thread.is_alive():task.stop(False).wait(180)
        leader_trace.close()

if __name__=='__main__':main()
