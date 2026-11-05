"""Registered topic/type/QoS allowlist shared by peers and SDK clients."""
import re
from .store import Fault

def runtime_profile(value):
    """Optional registered DDS file must match this process's deployed profile."""
    if not value.get('dds_config_uri'):return
    import os,hashlib
    from pathlib import Path
    from urllib.parse import urlparse,unquote
    uri=urlparse(value['dds_config_uri'])
    if uri.scheme!='file' or uri.netloc:raise Fault('ROS_CONFIG','DDS profile must be a locally mounted file URI')
    path=Path(unquote(uri.path)).resolve()
    configured=os.environ.get('FASTDDS_DEFAULT_PROFILES_FILE',os.environ.get('FASTRTPS_DEFAULT_PROFILES_FILE'))
    if not configured or Path(configured).resolve()!=path:raise Fault('ROS_CONFIG','DDS URI differs from the process startup profile')
    expected=value.get('dds_config_sha256')
    if not expected or not path.is_file() or 'sha256:'+hashlib.sha256(path.read_bytes()).hexdigest()!=expected:raise Fault('ROS_CONFIG','DDS profile file/hash mismatch')

def complete(value,cameras=('front','wrist')):
    namespace=value['namespace'];domain=value['domain_id']
    if not re.fullmatch(r'/[a-z][a-z0-9_/]*',namespace) or type(domain)!=int or not 0<=domain<=232:raise Fault('ROS_CONFIG','Invalid registered namespace/domain')
    if not cameras or any(not re.fullmatch(r'[a-z][a-z0-9_]*',k) for k in cameras):raise Fault('ROS_CONFIG','Camera names must be registered ROS identifiers')
    q={'reliability':'reliable','durability':'volatile','history':'keep_last','depth':16}
    def topic(suffix,type_):return {'name':namespace+'/'+suffix,'type':type_,'qos':dict(q)}
    wanted={'command_topic':topic('command','std_msgs/msg/String'),'result_topic':topic('result','std_msgs/msg/String'),
            'state_topic':topic('state','sensor_msgs/msg/JointState'),'observation_topic':topic('observation','std_msgs/msg/String'),
            'camera_topics':{k:topic('cameras/'+k,'sensor_msgs/msg/CompressedImage') for k in cameras},'clock_topic':topic('clock','rosgraph_msgs/msg/Clock')}
    for k,v in wanted.items():
        if k in value and value[k]!=v:raise Fault('ROS_CONFIG','Topic/type/QoS differs from registered allowlist')
    if value.get('rmw_implementation','rmw_fastrtps_cpp')!='rmw_fastrtps_cpp':raise Fault('ROS_CONFIG','Frozen runtime uses FastDDS')
    if value.get('dds_config_uri') and not re.fullmatch(r'sha256:[a-f0-9]{64}',value.get('dds_config_sha256','')):raise Fault('ROS_CONFIG','Registered DDS URI requires a fixed sha256')
    return {**value,**wanted,'rmw_implementation':'rmw_fastrtps_cpp','clock_domain':'same_host_monotonic','state_topic_units':'rad','contract_version':'so101.delivery.ros.v1'}
