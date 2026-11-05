"""Section17 boundary mapping and immutable session binding (no device I/O)."""
import copy,hashlib,json
from pathlib import Path
from .protocol import ApiError

def digest(value):return 'sha256:'+hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def public_ros(ros):
    if 'topics' not in ros:return copy.deepcopy(ros)  # legacy test/config compatibility
    result={k:copy.deepcopy(ros[k]) for k in ('domain_id','namespace','rmw_implementation')}
    for key in ('dds_config_uri',):
        if ros.get(key) is not None:result[key]=ros[key]
    result.update(command_topic=copy.deepcopy(ros['topics']['command']),state_topic=copy.deepcopy(ros['topics']['state']),
        observation_topic=copy.deepcopy(ros['topics']['observation']),camera_topics={k:copy.deepcopy(ros['topics'][k]) for k in ('front','wrist')})
    if 'clock' in ros['topics']:result['clock_topic']=copy.deepcopy(ros['topics']['clock'])
    return result
def match_ros(value,configured,*,legacy=False):
    if value==public_ros(configured) or legacy and value==configured:return True
    raise ApiError('ROS_CONTRACT_MISMATCH','ROS fields must match the registered target whitelist, types and QoS')
def binding(config,source,revision='baseline'):
    source_config=copy.deepcopy(config['sources'][source])
    record={'robot':copy.deepcopy(config['robot']),'contract':copy.deepcopy(config.get('contract',{})),
        'ros':copy.deepcopy(config['ros']),'source':source,'source_config':source_config,
        'calibration_revision':revision,'device_config_revision':config['robot'].get('device_config_revision',config['robot']['config_revision'])}
    profile=source_config.get('profile')
    if profile:
        path=Path(profile)
        if not path.is_file():raise ApiError('SOURCE_NOT_READY','Registered leader profile missing')
        record['input_profile']=json.loads(path.read_text())
        record['input_profile_file_hash']='sha256:'+hashlib.sha256(path.read_bytes()).hexdigest()
    record['binding_hash']=digest(record)
    return record
def assert_binding(saved,current):
    if not saved:raise ApiError('SESSION_REBIND_REQUIRED','Legacy session lacks a frozen binding; create a new session')
    if saved!=current:raise ApiError('SESSION_VERSION_MISMATCH','Session source/device/scene/ROS versions changed; old session cannot silently switch')
def episode_result(task,info):
    import numpy as np
    item=task._commits[info.episode_index]
    def stamp(entry):
        path=(task.directory/entry['path']).resolve()
        if not path.is_relative_to(task.directory.resolve()):raise ValueError('Frame path escapes task')
        with np.load(path,allow_pickle=False) as data:return int(data['sdk.captured_monotonic_ns'].item())
    duration=(stamp(item['files'][-1])-stamp(item['files'][0]))/1e9
    if duration<0:raise ValueError('Non-monotonic episode')
    return {'episode_index':info.episode_index,'frames':info.frames,'duration_s':duration,'content_hash':info.content_hash,
        'task':info.task,'committed_episodes':len(task._commits),'state':'RESETTING','status':'SUCCEEDED'}
