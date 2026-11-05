"""Actual loaded policy -> ROS RobotSession -> measured feedback and confirmed stop."""
import json,time
from pathlib import Path
from dataclasses import asdict
from .store import Fault

def run_inference(platform,key,p,a,cancel):
    from lerobot_sdk import ModelManager,RobotConfig,RobotSession,InferenceRunner,RunConfig
    from .ros_transport import RosBackend
    import torch
    torch.set_num_threads(min(2,platform.config.get('resources',{}).get('cpu_threads',2)))
    robot=p['resolved_robot'];registered=p['resolved_model'];models=ModelManager()
    if models.inspect(registered['path']).content_hash!=p['model']['content_hash']:raise Fault('MODEL_HASH_MISMATCH','Model changed before loading')
    cfg=RobotConfig(**robot['sdk_config']);backend=RosBackend(robot);out=platform.root/'jobs'/key;out.mkdir(parents=True,exist_ok=True)
    settings=p.get('parameters',{});run_cfg=RunConfig(**settings)
    with RobotSession(cfg,backend,lock_dir=platform.root/'locks') as session,models.load(registered['path'],device=p.get('device','cpu')) as model:
        with model.create_session(p['task'],session.contract) as policy:
            run=InferenceRunner().start(session,policy,run_cfg);last=-1
            try:
                with (out/'trajectory.jsonl').open('x',encoding='utf-8') as trace:
                    while not run._done.is_set():
                        state=run.status()
                        if state.steps!=last:
                            measured=session.get_arm_data();value={'state':asdict(state),'measured':dict(measured.positions),'sequence':measured.sequence,'time_ns':measured.captured_monotonic_ns}
                            trace.write(json.dumps(value,default=str)+'\n');trace.flush();platform.store.event(key,'inference',value);last=state.steps
                        if cancel.is_set():run.stop();break
                        time.sleep(.05)
                result=run.wait(10);run.wait_inference(10)
                count=len(backend.sent_history);time.sleep(.25);no_late=len(backend.sent_history)==count
                (out/'sent-targets.jsonl').write_text(''.join(json.dumps(v)+'\n' for v in backend.sent_history),encoding='utf-8')
                return {**asdict(result),'model_hash':registered['content_hash'],'source':robot.get('provenance','simulation'),'holding':result.holding,'action_queue_empty':run.status().queue_size==0,'no_late_sends':no_late,'sent_count':count}
            finally:
                if not run._done.is_set():run.stop()
