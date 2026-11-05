"""Frozen ROS2 simulation contract. Stdlib validation, no hardware access."""
import hashlib,hmac,json,re,math

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def contract_hash(c):return 'sha256:'+hashlib.sha256(canonical(c)).hexdigest()
def make_contract(robot_id,calibration_hash,names,limits):
    namespace='/so101_sdk_sim'
    types={'command':'std_msgs/msg/String','observation':'std_msgs/msg/String','state':'sensor_msgs/msg/JointState',
           'front':'sensor_msgs/msg/Image','wrist':'sensor_msgs/msg/Image','clock':'rosgraph_msgs/msg/Clock'}
    return {'schema':'so101.ros2.sim.v1','robot_id':robot_id,'calibration_hash':calibration_hash,'names':names,'limits':limits,
      'domain_id':71,'namespace':namespace,'rmw_implementation':'rmw_fastrtps_cpp','scene_revision':'workshop-vials-eval-pinned',
      'time_source':'same_linux_host_monotonic_ns','sensor_header_clock':'host_monotonic','clock_topic_time':'simulation_seconds',
      'state_topic_units':'rad','sdk_units':['deg']*5+['normalized_0_100'],
      'topics':{k:{'name':namespace+'/'+k,'type':v,'qos':{'reliability':'reliable','durability':'volatile','history':'keep_last','depth':4}} for k,v in types.items()}}
def validate_contract(c):
    if c.get('schema') not in ('so101.ros2.sim.v1','so101.ros2.sim.v2'):raise ValueError('Unsupported ROS contract schema')
    if type(c.get('domain_id')) is not int or not 0<=c['domain_id']<=232:raise ValueError('Invalid ROS domain')
    if c.get('rmw_implementation')!='rmw_fastrtps_cpp':raise ValueError('Unsupported frozen RMW')
    ns=c.get('namespace','')
    if not re.fullmatch(r'/[a-z][a-z0-9_]*',ns):raise ValueError('Invalid namespace')
    if c.get('time_source')!='same_linux_host_monotonic_ns':raise ValueError('Unsupported clock source')
    if len(c.get('names',[]))!=6 or len(set(c['names']))!=6 or set(c.get('limits',{}))!=set(c['names']):raise ValueError('Six named joint limits required')
    if c.get('state_topic_units')!='rad' or c.get('sdk_units')!=['deg']*5+['normalized_0_100']:raise ValueError('Invalid ROS/SDK units')
    if c.get('dds_config_uri') not in (None,'fastdds.xml'):raise ValueError('DDS config must be the local allowlisted profile')
    expected=make_contract(c['robot_id'],c['calibration_hash'],c['names'],c['limits'])['topics']
    if c['schema']=='so101.ros2.sim.v2':
        if c.get('camera_transport')!={'encoding':'jpeg','quality':90}:raise ValueError('Unsupported frozen camera transport')
        for key in ('front','wrist'):expected[key]['type']='sensor_msgs/msg/CompressedImage'
    if set(c['topics'])!=set(expected):raise ValueError('Topic mapping keys differ')
    for key,topic in c['topics'].items():
        if topic!={**expected[key],'name':ns+'/'+key}:raise ValueError('Topic not on target whitelist/type/QoS mismatch')
    return c
def sign_request(req,c,token):
    payload={'target':c['robot_id'],'config_hash':contract_hash(c),'request':req}
    return {**payload,'hmac':hmac.new(token.encode(),canonical(payload),hashlib.sha256).hexdigest()}
def verify_packet(packet,c,token):
    payload={k:v for k,v in packet.items() if k!='hmac'}
    if not hmac.compare_digest(packet.get('hmac',''),hmac.new(token.encode(),canonical(payload),hashlib.sha256).hexdigest()):raise ValueError('Invalid simulation capability signature')
    if payload.get('target')!=c['robot_id'] or payload.get('config_hash')!=contract_hash(c):raise ValueError('Target/config mismatch')
    return payload['request']
def verify_request(packet,c,token,now):
    req=verify_packet(packet,c,token)
    if type(req.get('deadline_ns')) is not int or not now<req['deadline_ns']<=now+30_000_000_000:raise ValueError('Expired or excessive deadline')
    if req.get('calibration_hash')!=c['calibration_hash']:raise ValueError('Calibration mismatch')
    if req.get('operation') not in ('connect','sample','send','hold','close','describe','reset'):raise ValueError('Unsupported operation')
    if type(req.get('sequence')) is not int or type(req.get('generation')) is not int:raise ValueError('Invalid sequence/epoch')
    if not isinstance(req.get('client'),str) or not isinstance(req.get('id'),str):raise ValueError('Invalid client/request')
    return req
def check_snapshot(images,state,identity,stamp,previous):
    if stamp<=previous or state!=(identity,stamp) or set(images)!= {'front','wrist'} or any(v!=(identity,stamp) for v in images.values()):
        raise ValueError('Inconsistent or non-monotonic ROS observation')
    return True

def watchdog_confirmed(watchdog,snapshot,after):
    return (watchdog.get('event')=='lease expired; simulator holding' and watchdog.get('time_ns',0)>after
            and snapshot.get('time_ns',0)>=watchdog['time_ns'] and snapshot.get('owner') is None)

def joints_to_ros(values):
    return [math.radians(float(v)) for v in values[:5]]+[math.radians(float(values[5])/100*110-10)]

def joints_from_ros(values):
    return [math.degrees(float(v)) for v in values[:5]]+[(math.degrees(float(values[5]))+10)/110*100]

def validate_ros_environment(c):
    import os
    from pathlib import Path
    if int(os.environ.get('ROS_DOMAIN_ID','-1'))!=c['domain_id'] or os.environ.get('RMW_IMPLEMENTATION')!=c['rmw_implementation']:raise ValueError('ROS environment differs from frozen contract')
    if c.get('dds_config_uri'):
        path=Path(os.environ.get('FASTRTPS_DEFAULT_PROFILES_FILE',''))
        if not path.is_file() or path.name!=c['dds_config_uri'] or hashlib.sha256(path.read_bytes()).hexdigest()!=c.get('dds_config_sha256'):raise ValueError('DDS profile missing or hash changed')
