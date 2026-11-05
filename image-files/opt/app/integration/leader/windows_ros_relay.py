"""Fresh Windows input -> signed ROS leader Topic; read-only camera/status view."""
import json,time,argparse
from pathlib import Path
import rclpy
from std_msgs.msg import String
from sensor_msgs.msg import CompressedImage,JointState
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy
from collection_service.client import Client
from collection_service.protocol import signed
from windows_ros_contract import ros_sample

parser=argparse.ArgumentParser();parser.add_argument('--input',type=Path,default=Path('/data/manual/windows-input/input.json'));parser.add_argument('--state',type=Path,default=Path('/data/manual/session.json'));parser.add_argument('--root',type=Path,default=Path('/data/manual/live'))
options=parser.parse_args();ROOT=options.root;INPUT=options.input;SESSION=options.state
cfg=json.loads(Path('/run/edge/config.json').read_text());source=cfg['sources']['com16_leader']
profile=json.loads(Path(source['profile']).read_text());key=Path(source['key_file']).read_text().strip()
api_key=json.loads(Path('/run/edge/api-keys.json').read_text())['student']
clock_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip()
client=Client(cfg,'student',api_key)
qos=QoSProfile(depth=4,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE)
pub=client.node.create_publisher(String,cfg['api_namespace']+'/leader',qos)
status={'source':'com16_leader','physical_follower':False,'leader_control_writes':0,'published':0,'camera_frames':{},'phase':'WAITING'}
last=-1;pending_stop=None;last_status=0;last_stop_query=0
def put(path,value):
    tmp=path.with_suffix('.tmp');tmp.write_text(json.dumps(value));tmp.replace(path)
def camera(name,msg):
    if 'jpeg' not in msg.format.lower() and 'jpg' not in msg.format.lower():return
    stamp=msg.header.stamp.sec*1_000_000_000+msg.header.stamp.nanosec
    if not 0<=time.monotonic_ns()-stamp<2_000_000_000:return
    tmp=ROOT/(name+'-next.jpg');tmp.write_bytes(bytes(msg.data));tmp.replace(ROOT/(name+'-latest.jpg'))
    status['camera_frames'][name]=status['camera_frames'].get(name,0)+1
    status.setdefault('camera_stamps_ns',{})[name]=stamp
def joints(msg):
    status['virtual_feedback_rad']=dict(zip(msg.name,msg.position))
subs=[client.node.create_subscription(CompressedImage,cfg['ros']['topics'][name]['name'],lambda msg,k=name:camera(k,msg),qos) for name in ('front','wrist')]
subs.append(client.node.create_subscription(JointState,cfg['ros']['topics']['state']['name'],joints,qos))
ROOT.mkdir(exist_ok=True)
try:
    while not (ROOT/'shutdown.request').exists():
        try:
            packet=json.loads(INPUT.read_text())
            status['leader_online']=time.monotonic()<packet['expires_monotonic']
            status['leader_age_s']=max(0,time.monotonic()-(packet['expires_monotonic']-.8))
            if packet['sequence']>last:
                value=ros_sample(packet,profile['profile_hash'],clock_id,last,time.monotonic_ns())
                msg=String();msg.data=json.dumps(signed(value,key));pub.publish(msg)
                last=packet['sequence'];status['published']+=1;status['native_positions']=value['native_positions']
        except (OSError,ValueError,KeyError):status['leader_online']=False
        rclpy.spin_once(client.node,timeout_sec=.015)
        while client.events:
            event=client.events.pop(0)
            if event['kind']=='collection.status':status['collection']=event['payload'];status['phase']=event['payload'].get('state','UNKNOWN')
            if event['kind']=='simulation.verification.status':status['verification']=event['payload'];status['phase']=event['payload'].get('state','UNKNOWN');status['recording']=False
        if (ROOT/'stop.request').exists():
            (ROOT/'stop.request').unlink()
            try:
                saved=json.loads(SESSION.read_text())
                context={k:saved['claim'][k] for k in ('robot_id','config_revision','epoch','control_token')}
                verify=saved.get('mode')=='verification'
                payload={**context,'session_id':saved['session_id']}
                if not verify:payload['save_current']=True
                answer=client.call('simulation.verify.stop' if verify else 'collection.stop',payload)
                if not answer['ok']:raise RuntimeError(answer.get('error'))
                pending_stop=(answer['result']['operation_id'],saved['session_id']);status['stop']='REQUESTED'
            except Exception as exc:status['stop']='FAILED: '+str(exc)
        if pending_stop and time.monotonic()-last_stop_query>1:
            last_stop_query=time.monotonic()
            try:
                answer=client.call('operation.get',{'operation_id':pending_stop[0]},timeout=2)
                job=answer['result']
                if job['status'] not in ('QUEUED','RUNNING'):
                    status['stop']=job['status']
                    if job['status']=='SUCCEEDED':
                        saved=json.loads(SESSION.read_text())
                        if saved.get('session_id')==pending_stop[1]:
                            if job['result'].get('dataset_id'):saved['dataset_id']=job['result']['dataset_id']
                            saved['last_operation_id']=pending_stop[0];saved['operation_id']=pending_stop[0]
                            put(SESSION,saved);SESSION.chmod(0o600)
                    pending_stop=None
            except Exception as exc:status['stop']='QUERY_PENDING: '+str(exc)
        if time.monotonic()-last_status>.5:
            try:
                manual=json.loads(SESSION.read_text());status['source']=manual.get('source','NOT_STARTED');status['mode']=manual.get('mode','idle')
            except (OSError,ValueError):status['source']='NOT_STARTED'
            status['updated_unix']=time.time();put(ROOT/'status.json',status);last_status=time.monotonic()
finally:client.close()

