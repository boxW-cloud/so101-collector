"""Public field projections and immutable target/configuration references."""
import copy
from .store import Fault

ROS_PUBLIC_FIELDS=('domain_id','namespace','rmw_implementation','contract_version','clock_domain','state_topic_units','dds_config_uri','dds_config_sha256','command_topic','result_topic','state_topic','observation_topic','clock_topic','camera_topics')
TOPIC_FIELDS=('name','type','qos')
QOS_FIELDS=('reliability','durability','history','depth')

def _topic(value):
    if not isinstance(value,dict):return {}
    result={k:value[k] for k in ('name','type') if isinstance(value.get(k),str)}
    if isinstance(value.get('qos'),dict):
        result['qos']={k:v for k,v in value['qos'].items() if k in QOS_FIELDS and type(v) in (str,int)}
    return result

def public_ros_config(config):
    """Never return deployment credentials or arbitrary nested Topic metadata."""
    config=config or {};result={}
    for key in ROS_PUBLIC_FIELDS:
        if key not in config:continue
        value=config[key]
        if key=='camera_topics':
            if isinstance(value,dict):result[key]={name:_topic(topic) for name,topic in value.items() if isinstance(name,str)}
        elif key.endswith('_topic'):result[key]=_topic(value)
        elif type(value) in (str,int,bool) or value is None:result[key]=value
    return copy.deepcopy(result)

def unverified_ros_status(config=None):
    return {'connection_state':'UNVERIFIED','checked_at_monotonic_ns':None,
            'nodes':[],'topics':[],'topics_verified':0,'configuration_verified':False,
            'timestamp_status':{'clock_domain':(config or {}).get('clock_domain','same_host_monotonic'),
                'clock_domain_verified':None,'last_state_captured_monotonic_ns':None,
                'last_camera_captured_monotonic_ns':{},'age_ms':None,'camera_age_ms':{},
                'sample_within_configured_age':None,'monotonicity_checked':False}}

def validate_scene_registration(robot):
    if robot['kind']!='simulation':return
    for name in ('scene_id','scene_revision'):
        if not isinstance(robot.get(name),str) or not robot[name].strip() or len(robot[name])>128:
            raise Fault('SCENE_REQUIRED','Registered simulation scene_id and scene_revision are required',400)

def bind_robot_fields(robot,payload):
    """Caller-supplied references assert the registered target, never override it."""
    revision=robot['revision']
    if payload.get('config_revision',revision)!=revision:
        raise Fault('CONFIG_REVISION_MISMATCH','config_revision must match registered robot_revision')
    if robot['kind']=='simulation':
        validate_scene_registration(robot)
        if any(payload.get(name)!=robot[name] for name in ('scene_id','scene_revision')):
            raise Fault('SCENE_MISMATCH','Requested scene/version differs from registered robot configuration')
    elif any(k in payload for k in ('scene_id','scene_revision')):
        raise Fault('WRONG_TARGET','Physical target cannot claim a simulation scene')
    public=public_ros_config(robot['ros'])
    requested=payload.get('ros',{})
    if not isinstance(requested,dict) or any(k not in public or v!=public[k] for k,v in requested.items()):
        raise Fault('ROS_CONFIG_MISMATCH','Requested ROS fields differ from the registered public contract')
    return {**payload,'config_revision':revision,'ros_config':public}

def operation_fields(job):
    value=copy.deepcopy(job);kind=value['kind'];key=value['operation_id']
    value['payload']={k:v for k,v in value.get('payload',{}).items() if not k.startswith('resolved_')}
    if kind=='training.start':value['training_run_id']=key
    if kind in ('collections.start','verifications.start'):
        value.update(session_id=key,dataset_id=key if kind=='collections.start' else None)
    return value

def resource_fields(entry):
    value=copy.deepcopy(entry)
    if value.get('kind')=='robots' and 'ros' in value.get('body',{}):
        value['body']['ros']=public_ros_config(value['body']['ros'])
    return value
