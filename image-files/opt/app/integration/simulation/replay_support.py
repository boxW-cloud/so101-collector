"""Audit helpers for simulation acceptance, not a physical control backend."""
import hashlib
import json
import math
from pathlib import Path
import time


def file_sha(path):
    return 'sha256:'+hashlib.sha256(Path(path).read_bytes()).hexdigest()


def validate_binding(manifest, contract):
    expected={'robot_id':contract['robot_id'], 'calibration_hash':contract['calibration_hash'],
              'joint_names':contract['names'], 'units':[contract['units'][n] for n in contract['names']]}
    for key,value in expected.items():
        if manifest.get(key)!=value:
            raise ValueError(f'Dataset/robot contract mismatch: {key}')


def timing_metrics(recorded_ns, replay_ns, schedule_start_ns, scale, tolerance_s):
    if not math.isfinite(scale) or scale<1 or len(recorded_ns)!=len(replay_ns) or not recorded_ns:
        raise ValueError('Invalid replay timing inputs')
    if any(b<=a for a,b in zip(recorded_ns,recorded_ns[1:])) or any(b<=a for a,b in zip(replay_ns,replay_ns[1:])):
        raise ValueError('Timing must be strictly increasing')
    planned=[schedule_start_ns+round((t-recorded_ns[0])*scale) for t in recorded_ns]
    error=[(a-b)/1e9 for a,b in zip(replay_ns,planned)]
    return {'time_scale':scale,'mode':'original_time' if scale==1 else 'fixed_time_scale',
            'recorded_duration_s':(recorded_ns[-1]-recorded_ns[0])/1e9,
            'planned_duration_s':(planned[-1]-planned[0])/1e9,
            'actual_duration_s':(replay_ns[-1]-replay_ns[0])/1e9,
            'schedule_error_s':error,'max_abs_schedule_error_s':max(map(abs,error)),
            'tolerance_s':tolerance_s,'passed':max(map(abs,error))<=tolerance_s}


def fresh_observation(getter, *, after_ns, timeout_s):
    end=time.monotonic()+timeout_s
    while time.monotonic()<end:
        obs=getter()
        if obs.arm.captured_monotonic_ns>after_ns:
            return obs
        time.sleep(.005)
    raise TimeoutError('No genuinely new post-send observation before deadline')


def prepare_ipc(root, generation):
    root=Path(root).resolve()
    if not generation or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in generation):
        raise ValueError('Invalid generation')
    ipc=root/'ipc'
    if ipc.is_symlink():
        raise ValueError('Do not archive linked IPC directories')
    archive=root/'ipc-history'/generation
    if archive.exists():raise FileExistsError(archive)
    archive.parent.mkdir(parents=True,exist_ok=True)
    if ipc.exists():ipc.rename(archive)
    ipc.mkdir()
    (ipc/'generation.json').write_text(json.dumps({'generation':generation,'created_unix_ns':time.time_ns()}))
    return archive


def committed_snapshot(task_root, commits):
    root=Path(task_root).resolve()
    result={}
    for episode in commits:
        for entry in episode['files']:
            p=(root/entry['path']).resolve()
            if not p.is_relative_to(root):raise ValueError('Commit escapes task root')
            actual=file_sha(p)
            if actual!=entry['sha256']:raise ValueError('Committed frame content changed: '+entry['path'])
            result[entry['path']]=actual
    return result


def accepted(checks):
    keys=('fidelity','timing','fresh_final','tracking','recorded_trajectory')
    return all(checks.get(k) is True for k in keys)


def interpolate_schedule(actions, timestamps, substeps):
    """Insert fixed linear samples; original values/timestamps remain untouched."""
    if type(substeps) is not int or not 1<=substeps<=8 or not actions or len(actions)!=len(timestamps):
        raise ValueError('Invalid interpolation inputs')
    values=[];times=[];indices=[]
    for i, row in enumerate(actions):
        indices.append(len(values));values.append(list(row));times.append(int(timestamps[i]))
        if i+1==len(actions):break
        if timestamps[i+1]<=timestamps[i]:raise ValueError('Non-increasing source time')
        for step in range(1,substeps):
            f=step/substeps
            values.append([a+(b-a)*f for a,b in zip(row,actions[i+1],strict=True)])
            times.append(int(timestamps[i])+round((int(timestamps[i+1])-int(timestamps[i]))*f))
    return values,times,indices
