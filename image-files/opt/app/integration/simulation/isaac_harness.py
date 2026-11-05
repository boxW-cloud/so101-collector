"""Isolated, simulation-only harness using the installed Workshop/Isaac packages.
No motor SDK is imported. No change to the original GR00T evaluator or containers.
"""
import argparse,json,time,hashlib
from pathlib import Path
import serial
def no_serial(*a,**kw):raise RuntimeError('Isaac SDK tests cannot open physical serial ports')
serial.Serial.open=no_serial
from isaaclab.app import AppLauncher
parser=argparse.ArgumentParser()
parser.add_argument('--ipc-directory',required=True)
parser.add_argument('--contract-file',required=True)
parser.add_argument('--generation',default='legacy')
parser.add_argument('--ros-config')
parser.add_argument('--ros-token')
AppLauncher.add_app_launcher_args(parser)
args=parser.parse_args();args.enable_cameras=True
launcher=AppLauncher(args);app=launcher.app

import numpy as np
import torch
import gymnasium as gym
import isaaclab_tasks
from isaaclab_tasks.utils import parse_env_cfg
from isaaclab.managers import TerminationTermCfg
import sim_to_real_so101.tasks
from passive_preview import publish_preview
from watchdog_hold import PROTOCOL as HOLD_PROTOCOL,WatchdogHold

ROOT=Path(args.ipc_directory);ROOT.mkdir(parents=True,exist_ok=True)
contract=json.loads(Path(args.contract_file).read_text())
NAMES=tuple(contract['names']);LIMITS=contract['limits'];HASH=contract['calibration_hash']
assert len(NAMES)==6 and len(set(NAMES))==6
def publish(name,value):
    p=ROOT/(name+'.tmp');p.write_text(json.dumps(value),encoding='utf-8');p.replace(ROOT/name)
def joint_values(obs):
    from sim_feedback import normalized_gripper_feedback
    q=obs['policy']['joint_pos_obs'][0].detach().cpu().numpy()
    values=np.rad2deg(q)
    raw_degrees=float(values[-1])
    opening,rounded=normalized_gripper_feedback(raw_degrees)
    if rounded:
        publish('gripper-roundoff.json',{'raw_degrees':raw_degrees,'raw_normalized':(raw_degrees+10)/110*100,
            'sdk_normalized':opening,'numerical_budget_normalized':1e-4,'time_ns':time.monotonic_ns()})
    values[-1]=opening
    return {k:float(v) for k,v in zip(NAMES,values,strict=True)}
def to_radians(values):
    q=np.array([values[k] for k in NAMES],dtype=np.float32)
    q[-1]=q[-1]/100*110-10
    return torch.tensor(np.deg2rad(q),device=args.device).unsqueeze(0)

env=None;owner=None;lease_until=0;generation=0;seen=None;last_sequence=0;count=0
preview_sequence=0;preview_at=0.0
hold_proof=WatchdogHold(args.generation,LIMITS)
ros=None
try:
    if args.ros_config:
        from ros_sim_transport import RosSimulatorTransport
        ros=RosSimulatorTransport(args.ros_config,args.ros_token)
    cfg=parse_env_cfg('Lerobot-So101-Teleop-Vials-To-Rack-Eval',device=args.device,num_envs=1)
    cfg.episode_length_s=3600
    # Lifecycle test, not a policy success benchmark: no automatic task resets.
    for key,value in vars(cfg.terminations).items():
        if isinstance(value,TerminationTermCfg):setattr(cfg.terminations,key,None)
    env=gym.make('Lerobot-So101-Teleop-Vials-To-Rack-Eval',cfg=cfg)
    obs,_=env.reset()
    # PhysX may penetrate a hard joint boundary slightly while solving contact.
    # Put the physics constraints conservatively inside the SDK contract. Never
    # conceal an out-of-range observation or loosen its numerical fault budget.
    from joint_bounds import compute_joint_bounds
    articulation=env.unwrapped.scene['robot']
    action_term=env.unwrapped.action_manager.get_term('joint_positions')
    bounded,audit=compute_joint_bounds(sdk_names=NAMES,
        action_names=action_term._joint_names,asset_names=articulation.data.joint_names,
        original_limits_rad=articulation.data.joint_pos_limits.detach().cpu().numpy(),
        sdk_limits=LIMITS,sdk_units=contract['units'])
    articulation.write_joint_position_limit_to_sim(torch.as_tensor(bounded,
        device=articulation.device,dtype=articulation.data.joint_pos_limits.dtype))
    publish('physics-bounds.json',{**audit,'run_generation':args.generation,
        'calibration_hash':HASH,'sdk_feedback_budget_changed':False})
    initial=torch.tensor([[-.2736,-.6109,-.0745,1.5148,-1.6034,-.1465]],device=args.device)
    for _ in range(60):obs,*_=env.step(initial)
    target=joint_values(obs);actions=to_radians(target)
    stamp=time.monotonic_ns();capture_unix_ns=time.time_ns()
    publish('hold-state.json',hold_proof.startup(stamp,target))
    publish('ready.json',{'calibration_hash':HASH,'robot_id':contract['robot_id'],'initial':target,
                         'kind':'Isaac physics simulation only','serial_blocked':True,'pid':__import__('os').getpid(),
                         'generation':args.generation,'ready_unix_ns':time.time_ns(),
                         'hold_confirmation_protocol':HOLD_PROTOCOL if not ros else None})
    while app.is_running() and not (ROOT/'shutdown.request').exists():
        tick=time.monotonic();now=time.monotonic_ns()
        if owner and now>lease_until:
            target=joint_values(obs);actions=to_radians(target)
            held=hold_proof.expired(owner,generation,last_sequence,now,target);owner=None
            publish('watchdog.json',held);publish('hold-state.json',held)
        if ros:req=ros.poll()
        else:
            try:req=json.loads((ROOT/'request.json').read_text())
            except (FileNotFoundError,json.JSONDecodeError):req=None
        if req and req.get('id')!=seen:
            seen=req['id'];response={'id':seen};op=req.get('operation')
            try:
                if now>req['deadline_ns']:raise ValueError('Expired command')
                if req['calibration_hash']!=HASH:raise ValueError('Calibration hash mismatch')
                if op in ('confirm_watchdog_hold','confirm_startup_hold'):
                    # Private same-host stop confirmation only. The authoritative
                    # owner, run and actual observations are checked atomically;
                    # no reconnect, target write, lease renewal or scene reset.
                    if ros:raise ValueError('HOLD proof requires the private same-host file IPC')
                    reason='watchdog' if op=='confirm_watchdog_hold' else 'startup'
                    response.update(hold_proof.confirm(req,owner,target,joint_values(obs),stamp,now,reason=reason))
                else:
                    if op=='connect':
                        if owner and owner!=req['client']:
                            response['code']='busy';raise ValueError('Simulation already leased')
                        hold_proof.invalidate()
                        owner=req['client'];generation=req['generation'];last_sequence=0
                        target=joint_values(obs);actions=to_radians(target)
                    if req['client']!=owner:raise ValueError('No current control lease')
                    if req['sequence']<=last_sequence or req['generation']<generation:raise ValueError('Old sequence/generation')
                    last_sequence=req['sequence'];generation=req['generation'];lease_until=now+2_000_000_000
                    if op=='sample':
                        response.update(positions=joint_values(obs),captured_monotonic_ns=stamp,images={})
                        for key,source in [('front','rgb_external_D455'),('wrist','rgb_ego')]:
                            array=obs['visual'][source][0].detach().cpu().numpy().astype(np.uint8)
                            if ros:ros.image(key,array,seen,stamp)
                            else:
                                temp=ROOT/(key+'.npy.tmp')
                                with temp.open('wb') as f:np.save(f,array,allow_pickle=False)
                                temp.replace(ROOT/(key+'.npy'))
                            response['images'][key]={'filename':key+'.npy','captured_monotonic_ns':stamp}
                    elif op=='send':
                        values=req['positions']
                        if set(values)!=set(NAMES):raise ValueError('Expected exactly six named joints')
                        if any(isinstance(v,bool) or not np.isfinite(v) or not LIMITS[k][0]<=v<=LIMITS[k][1] for k,v in values.items()):raise ValueError('Target violates simulation contract')
                        target=values;actions=to_radians(target);response['sent']=target
                    elif op=='reset':
                        # Explicit owner-only full scene reset, never a side effect of
                        # saving/redoing an episode. Refresh state and all camera epochs.
                        obs,_=env.reset()
                        for _ in range(60):obs,*_=env.step(initial)
                        target=joint_values(obs);actions=to_radians(target);stamp=time.monotonic_ns();capture_unix_ns=time.time_ns()
                        generation+=1;lease_until=stamp+2_000_000_000
                        response.update(scene_reset=True,holding=True,action_queue_empty=True,captured_monotonic_ns=stamp,generation=generation)
                    elif op in ('hold','close'):
                        target=joint_values(obs);actions=to_radians(target)
                        if op=='close':owner=None
                    elif op not in ('connect','heartbeat'):raise ValueError('Unsupported operation')
            except Exception as e:response['error']=str(e)
            if ros:ros.reply(response)
            else:publish('response.json',response)
        obs,*_=env.step(actions);stamp=time.monotonic_ns();capture_unix_ns=time.time_ns();count+=1
        hold_proof.observe(stamp,owner,target,joint_values(obs))
        # Passive rendering is independent of sampling/leases. The UI cannot
        # connect, claim, hold or move the simulator merely by viewing a frame.
        if time.monotonic()-preview_at>=.2:
            preview_sequence+=1
            images={key:obs['visual'][source][0].detach().cpu().numpy().astype(np.uint8)
                    for key,source in [('front','rgb_external_D455'),('wrist','rgb_ego')]}
            publish_preview(ROOT,images,generation=args.generation,sequence=preview_sequence,
                            captured_monotonic_ns=stamp,captured_unix_ns=capture_unix_ns)
            preview_at=time.monotonic()
        if ros:ros.tick(float(env.unwrapped.sim.current_time))
        if count%30==0:publish('sim-status.json',{'steps':count,'time_ns':stamp,'owner':owner,'target':target,
            'measured':joint_values(obs),'run_generation':args.generation,'control_generation':generation,
            'lease_until_ns':lease_until,'hold_reason':hold_proof.record['reason'] if hold_proof.record else None})
        time.sleep(max(0,1/30-(time.monotonic()-tick)))
    publish('shutdown.json',{'reason':'fixture shutdown requested','time_ns':time.monotonic_ns(),'generation':args.generation})
except BaseException as e:
    import traceback
    publish('failure.json',{'error':repr(e),'traceback':traceback.format_exc()});raise
finally:
    if ros:ros.close()
    if env is not None:env.close()
    app.close()
