"""Isolated Isaac preview: installed Workshop mapping, physical serial denied."""
import argparse
import json
from pathlib import Path
import time
import serial


def deny_serial(*args, **kwargs):
    raise RuntimeError('This Isaac process never opens physical serial devices')


serial.Serial.open = deny_serial
from isaaclab.app import AppLauncher
parser = argparse.ArgumentParser()
parser.add_argument('--root', required=True)
AppLauncher.add_app_launcher_args(parser)
args = parser.parse_args()
args.enable_cameras = True
launcher = AppLauncher(args)
app = launcher.app

import numpy as np
import torch
import gymnasium as gym
import isaaclab_tasks
from isaaclab_tasks.utils import parse_env_cfg
from isaaclab.managers import TerminationTermCfg
from isaaclab.sensors import CameraCfg
import isaaclab.sim as sim_utils
import sim_to_real_so101.tasks
from sim_to_real_so101.utils.lerobot_interface import LeRobotSO101Interface
from lerobot.motors import Motor, MotorNormMode, MotorCalibration
from lerobot.motors.feetech.feetech import FeetechMotorsBus
from PIL import Image
from three_point_preview import mapped_degrees
from three_point_preview import mapped_degrees

root = Path(args.root)
profile = json.loads((root/'profile.json').read_text())
axes = profile['axes']
names = list(axes)
cal = {n: MotorCalibration(**v['spark_normalization_calibration']) for n, v in axes.items()}
bus = FeetechMotorsBus('UNUSED', {n: Motor(v['motor_id'], 'sts3215',
    MotorNormMode.RANGE_0_100 if n == 'gripper' else MotorNormMode.RANGE_M100_100)
    for n, v in axes.items()}, calibration=cal)
iface = LeRobotSO101Interface(device=args.device, port='UNUSED', id='preview-no-serial', cameras={}, fps=30, kind='leader')
status = {'state': 'STARTING', 'serial_blocked': True, 'mode': 'three_point_mixed_preview',
          'profile_hash': profile['profile_hash'], 'recording': False, 'simulation_steps': 0,
          'envelope_deg': 20, 'relative_motor_ids': [1, 5], 'fixed_motor_ids': [2, 3, 4], 'physical_follower_connected': False}


def publish(name, obj):
    tmp = root/(name+'.tmp')
    tmp.write_text(json.dumps(obj, ensure_ascii=False), encoding='utf-8')
    tmp.replace(root/name)


def read_json(name):
    try:
        return json.loads((root/name).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def publish_image(key, array):
    image = array.detach().cpu().numpy().astype('uint8') if hasattr(array, 'detach') else array
    tmp = root/(key+'-next.jpg')
    Image.fromarray(image[:, :, :3]).save(tmp, quality=85)
    tmp.replace(root/(key+'-latest.jpg'))


env = None
publish('viewer-status.json', status)
try:
    cfg = parse_env_cfg('Lerobot-So101-Teleop-Vials-To-Rack-Eval', device=args.device, num_envs=1)
    cfg.episode_length_s = 3600
    for key, value in vars(cfg.terminations).items():
        if isinstance(value, TerminationTermCfg):
            setattr(cfg.terminations, key, None)
    cfg.scene.camera_preview = CameraCfg(
        prim_path='{ENV_REGEX_NS}/PreviewCamera', update_period=.1,
        height=720, width=960, data_types=['rgb'],
        spawn=sim_utils.PinholeCameraCfg(focal_length=9, horizontal_aperture=24, clipping_range=(.01, 100)),
        offset=CameraCfg.OffsetCfg(pos=(.7, -.7, .6), rot=(1., 0., 0., 0.), convention='world'))
    env = gym.make('Lerobot-So101-Teleop-Vials-To-Rack-Eval', cfg=cfg)
    obs, _ = env.reset()
    initial = torch.tensor([[-.2736, -.6109, -.0745, 1.5148, -1.6034, -.1465]], device=args.device)
    robot = env.unwrapped.scene['robot']
    pos = robot.data.root_pos_w.clone()
    env.unwrapped.scene['camera_preview'].set_world_poses_from_view(
        eyes=env.unwrapped.scene['camera_external_D455'].data.pos_w.clone(),
        targets=pos+torch.tensor([[.10, 0., .16]], device=args.device))
    for _ in range(60):
        obs, *_ = env.step(initial)
    target = obs['policy']['joint_pos_obs'][0].clone()
    anchor_native = None
    anchor_sim = None
    old_epoch = None
    stale = True
    old_seq = None
    last_picture = 0.
    last_tick = time.monotonic()
    lo = iface.joint_mins * torch.pi/180
    hi = iface.joint_maxs * torch.pi/180
    while app.is_running() and not (root/'shutdown.request').exists():
        now = time.monotonic()
        dt = min(.1, max(.001, now-last_tick))
        last_tick = now
        control = read_json('control.json') or {'paused': False, 'epoch': 0}
        packet = read_json('leader-input.json')
        measured = obs['policy']['joint_pos_obs'][0].clone()
        age = now-packet['received_monotonic'] if packet else 1e9
        valid = bool(packet and 0 <= age < .8 and packet['profile_hash'] == profile['profile_hash'])
        if control['epoch'] != old_epoch:
            anchor_native = None
            old_epoch = control['epoch']
        if not valid or control['paused']:
            if not stale:
                target = measured
            stale = True
            anchor_native = None
            status['state'] = 'PAUSED_HOLD' if control['paused'] else 'WAITING_LEADER_HOLD'
        else:
            raw = packet['native_positions']
            normalized = bus._normalize({i: 4095-raw[str(i)] for i in range(1, 7)})
            tensor = torch.tensor([normalized[i] for i in range(1, 7)], device=args.device)
            mapped = iface.get_mapped_actions_vectorized(tensor)
            if anchor_native is None:
                anchor_native = mapped.clone()
                anchor_sim = measured.clone()
                target = measured.clone()
            desired = anchor_sim+torch.clamp(mapped-anchor_native, -20*torch.pi/180, 20*torch.pi/180)
            desired[5] = mapped[5]  # Gripper uses the measured open/closed endpoints.
            for i, name in enumerate(names):
                if axes[name].get('mapping_mode') == 'fixed_three_point':
                    degrees, _ = mapped_degrees(raw[str(i+1)], axes[name])
                    desired[i] = degrees*torch.pi/180
            for i, name in enumerate(names):
                if axes[name].get('mapping_mode') == 'fixed_three_point':
                    degrees, _ = mapped_degrees(raw[str(i+1)], axes[name])
                    desired[i] = degrees*torch.pi/180
            desired = torch.maximum(lo, torch.minimum(hi, desired))
            speed = torch.tensor([30., 30., 30., 30., 30., 60.], device=args.device)*torch.pi/180
            target = target+torch.clamp(desired-target, -speed*dt, speed*dt)
            stale = False
            status['state'] = 'FOLLOWING_THREE_POINT'
            status['source_sequence'] = packet['sequence']
            status['native_positions'] = raw
            status['input_clamped_axes'] = [n for n, v in axes.items()
                if not v['native_min'] <= raw[str(v['motor_id'])] <= v['native_max']]
            status['relative_limit_axes'] = [names[i] for i in range(5)
                if axes[names[i]].get('mapping_mode') != 'fixed_three_point' and abs(float(mapped[i]-anchor_native[i])) > 20*torch.pi/180]
        obs, *_ = env.step(target.unsqueeze(0))
        status['simulation_steps'] += 1
        if now-last_picture >= .15:
            for key in ['rgb_external_D455', 'rgb_ego']:
                publish_image(key, obs['visual'][key][0])
            publish_image('overview', env.unwrapped.scene['camera_preview'].data.output['rgb'][0])
            status.update(updated_unix=time.time(), leader_age_s=None if age>100 else round(age, 3),
                          target_deg=torch.rad2deg(target).cpu().tolist(),
                          measured_deg=torch.rad2deg(obs['policy']['joint_pos_obs'][0]).cpu().tolist())
            publish('viewer-status.json', status)
            last_picture = now
        time.sleep(max(0, 1/30-(time.monotonic()-now)))
except BaseException as exc:
    status.update(state='STOPPED' if isinstance(exc, KeyboardInterrupt) else 'FAILED', error=repr(exc), updated_unix=time.time())
    publish('viewer-status.json', status)
    raise
finally:
    if env is not None:
        env.close()
    app.close()
