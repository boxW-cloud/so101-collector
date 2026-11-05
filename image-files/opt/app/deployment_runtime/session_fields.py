"""Public session references and diagnostics, without changing control behavior."""
from copy import deepcopy
from collections.abc import Mapping
from .field_bindings import public_ros_config as public_ros, unverified_ros_status
import math
import re
from .store import Fault


def trusted_runtime_provenance(receipt,robot):
    """Only backend READY evidence may bind the source, never browser payloads."""
    from lerobot_sdk.core.provenance import bounded_metadata
    value=receipt.get('runtime_provenance') if isinstance(receipt,Mapping) else None
    required=robot.get('require_runtime_provenance') is True
    if value is None:
        if required:raise Fault('PROVENANCE_REQUIRED','Signed READY source provenance is required')
        return None
    try:value=bounded_metadata(public_metadata(value))
    except Exception as error:raise Fault('PROVENANCE_INVALID','READY source evidence is not bounded JSON') from error
    if value.get('source_kind')!=robot['kind']:
        raise Fault('SOURCE_MISMATCH','READY source kind differs from registered target')
    if robot['kind']=='physical':
        if value.get('schema')!='so101.native_relative_runtime.v1' or value.get('source')!='physical_native_relative_encoder':
            raise Fault('PROVENANCE_INVALID','Registered native relative source evidence required')
        names=('shoulder_pan','shoulder_lift','elbow_flex','wrist_flex','wrist_roll','gripper')
        for key in ('runtime_config_sha256','profile_hash','follower_profile_id','device_id','model_mapping_id','source_instance'):
            if not isinstance(value.get(key),str) or not value[key]:raise Fault('PROVENANCE_INVALID','Missing READY '+key)
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',value['runtime_config_sha256']):raise Fault('PROVENANCE_INVALID','READY configuration digest is invalid')
        if value.get('source_identity_verified') not in ('windows_child_lineage_and_birth','popen_pid'):
            raise Fault('PROVENANCE_INVALID','Owned source process identity is not verified')
        if type(value.get('anchor_source_sequence')) is not int or value['anchor_source_sequence']<0:raise Fault('PROVENANCE_INVALID','READY source sequence is invalid')
        for key,expected in (('anchor_raw',set('123456')),('anchor_follower',set(names))):
            item=value.get(key)
            if not isinstance(item,dict) or set(item)!=expected or any(type(v) is not int or not 0<=v<=4095 for v in item.values()):
                raise Fault('PROVENANCE_INVALID','READY '+key+' encoder contract differs')
        active=value.get('active_axes');held=value.get('held_axes')
        if (not isinstance(active,list) or not active or not isinstance(held,list) or
                any(not isinstance(v,str) for v in active+held) or len(set(active+held))!=len(active+held) or set(active+held)!=set(names)):
            raise Fault('PROVENANCE_INVALID','READY active/held axes must partition six named axes')
        pins=value.get('held_goal_anchor')
        if not isinstance(pins,dict) or set(pins)!=set(held) or any(type(v) is not int or not 0<=v<=4095 for v in pins.values()):
            raise Fault('PROVENANCE_INVALID','READY held-goal encoder pins differ from held axes')
        for key in ('gains','signs','directions_verified'):
            item=value.get(key)
            if not isinstance(item,dict) or set(item)!=set(names):raise Fault('PROVENANCE_INVALID','READY '+key+' contract differs')
        if any(type(v) not in (int,float) or not math.isfinite(v) or not 0<v<=1 for v in value['gains'].values()):raise Fault('PROVENANCE_INVALID','READY motion gain is invalid')
        if any(type(v) is not int or v not in (-1,1) for v in value['signs'].values()):raise Fault('PROVENANCE_INVALID','READY motion sign is invalid')
        if any(type(v) is not bool for v in value['directions_verified'].values()) or not all(value['directions_verified'][name] for name in active):raise Fault('PROVENANCE_INVALID','Active READY directions are unverified')
        if any(value.get(key) is not False for key in ('absolute_zero_verified','tcp_calibrated','model_mapping_verified')):
            raise Fault('PROVENANCE_INVALID','Relative source must not claim absolute/TCP calibration')
    expected=robot.get('runtime_config_sha256')
    if expected is not None and expected!=value.get('runtime_config_sha256'):
        raise Fault('CONFIG_PROVENANCE_MISMATCH','Actual READY configuration differs from trusted registration')
    if value.get('runtime_config_snapshot') is not None:
        import hashlib,json
        encoded=json.dumps(value['runtime_config_snapshot'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        if 'sha256:'+hashlib.sha256(encoded).hexdigest()!=value.get('runtime_config_sha256'):
            raise Fault('CONFIG_PROVENANCE_MISMATCH','READY public configuration does not match its digest')
    return value


def trusted_frame_provenance(value,runtime):
    from lerobot_sdk.core.provenance import bounded_metadata
    try:value=bounded_metadata(public_metadata(value))
    except Exception as error:raise Fault('PROVENANCE_INVALID','USED source evidence is not bounded JSON') from error
    if value is None:raise Fault('PROVENANCE_REQUIRED','Actual USED source evidence is missing')
    if runtime['source_kind']=='physical':
        if value.get('schema')!='so101.native_relative_frame.v1':raise Fault('PROVENANCE_INVALID','Native USED frame evidence required')
        for key,item in runtime.items():
            if key!='schema' and value.get(key)!=item:raise Fault('PROVENANCE_MISMATCH','USED '+key+' differs from frozen READY')
        raw=value.get('native_positions');sequence=value.get('source_sequence')
        if not isinstance(raw,dict) or set(raw)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in raw.values()) or type(sequence) is not int or sequence<0:
            raise Fault('PROVENANCE_INVALID','USED main encoder/source sequence contract differs')
        received=value.get('received_monotonic');expires=value.get('expires_monotonic')
        if any(type(v) not in (int,float) or not math.isfinite(v) for v in (received,expires)) or not received<expires<=received+.8+1e-6:
            raise Fault('PROVENANCE_INVALID','USED source time/TTL contract differs')
    elif value.get('source_kind')!=runtime['source_kind']:raise Fault('SOURCE_MISMATCH','USED source kind differs from READY')
    return value


def _without_private(value):
    if isinstance(value, Mapping):
        return {key: _without_private(item) for key, item in value.items()
                if not any(word in str(key).lower() for word in
                           ('key_file', 'password', 'secret', 'credential', 'token', 'authorization', 'bridge_url'))}
    if isinstance(value, (list, tuple)):
        return [_without_private(item) for item in value]
    return deepcopy(value)


def ros_status(backend, config=None):
    result = unverified_ros_status(config)
    timestamps = result['timestamp_status']
    diagnostics = getattr(backend, 'diagnostics', None)
    if callable(diagnostics):
        diagnostics = diagnostics()
    if isinstance(diagnostics, Mapping):
        reported = _without_private(diagnostics)
        result.update({key: value for key, value in reported.items() if key != 'timestamp_status'})
        if isinstance(reported.get('timestamp_status'), Mapping):
            timestamps.update(reported['timestamp_status'])
    # r16 reports the existing implementation; it does not add a time-reversal check.
    timestamps['monotonicity_checked'] = False
    result['timestamp_status'] = timestamps
    return result


def session_binding(robot, payload):
    ros = public_ros(payload.get('ros_config', robot.get('ros', {})))
    return {'robot_id': robot.get('robot_id'), 'robot_revision': robot.get('revision'),
            'config_revision': payload.get('config_revision', robot.get('revision')),
            'calibration_revision': payload.get('calibration_revision'),
            'scene_id': payload.get('scene_id', robot.get('scene_id')),
            'scene_revision': payload.get('scene_revision', robot.get('scene_revision')),
            'ros_config': ros, 'ros': deepcopy(ros), 'task_hash': payload.get('task_hash')}


def session_fields(key, value):
    binding = value.get('binding', {})
    result = {field: deepcopy(binding.get(field)) for field in
              ('robot_id', 'robot_revision', 'config_revision', 'calibration_revision', 'scene_id', 'scene_revision')}
    result.update(session_id=key, dataset_id=None if value.get('verification') else key,
                  mode=value.get('mode'), source=value.get('source'),
                  ros_config=public_ros(binding.get('ros_config', binding.get('ros', {}))),
                  ros_status=ros_status(value.get('backend'), binding.get('ros_config', binding.get('ros', {}))))
    if binding.get('runtime_provenance') is not None:result['runtime_provenance']=public_metadata(binding['runtime_provenance'])
    return result


def public_metadata(parameters):
    result = _without_private(parameters)
    for name in ('ros', 'ros_config'):
        if name in result:
            result[name] = public_ros(result[name])
    return result
