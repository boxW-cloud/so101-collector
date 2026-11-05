"""Same-host Python ABI adapter: SDK local RPC ↔ real ROS2 Topics ↔ Isaac.

No connection to physical hardware. Local files terminate here, not at Isaac.
"""
import argparse,json,time,os,uuid
from pathlib import Path
from collections import OrderedDict
import numpy as np
import rclpy
from std_msgs.msg import String
from sensor_msgs.msg import Image,JointState,CompressedImage
from rosgraph_msgs.msg import Clock
from ros_sim_transport import qos
from ros_sim_contract import validate_contract,sign_request,verify_packet,check_snapshot,contract_hash,joints_from_ros,validate_ros_environment

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--contract',type=Path,required=True);p.add_argument('--token',type=Path,required=True)
    args=p.parse_args();root=args.root;root.mkdir(parents=True,exist_ok=True)
    c=validate_contract(json.loads(args.contract.read_text()));token=args.token.read_text().strip()
    validate_ros_environment(c)
    if int(os.environ.get('ROS_DOMAIN_ID','-1'))!=c['domain_id'] or os.environ.get('RMW_IMPLEMENTATION')!=c['rmw_implementation']:raise ValueError('ROS environment differs from frozen contract')
    rclpy.init();node=rclpy.create_node('sdk_gateway',namespace=c['namespace']);q=qos(c);topics=c['topics']
    command=node.create_publisher(String,topics['command']['name'],q);frames={k:OrderedDict() for k in ('front','wrist')};states=OrderedDict();acks=OrderedDict()
    stats={'commands':0,'responses':0,'frames':0,'rejected':0,'clock_messages':0,'config_hash':contract_hash(c),'domain_id':c['domain_id']}
    trace=(root/'transport-trace.jsonl').open('a')
    def event(kind,identity):trace.write(json.dumps({'kind':kind,'id':identity,'ns':time.monotonic_ns()})+'\n');trace.flush()
    def publish(name,value):
        tmp=root/(name+'.tmp');tmp.write_text(json.dumps(value));tmp.replace(root/name)
    def remember(cache,key,value):
        cache[key]=value
        while len(cache)>8:cache.popitem(last=False)
    def get_stamp(header):return header.stamp.sec*1000000000+header.stamp.nanosec
    def receive_image(camera,msg):
        if c['schema']=='so101.ros2.sim.v2':
            from PIL import Image as PILImage
            from io import BytesIO
            try:
                if msg.format!='rgb8; jpeg compressed rgb8' or len(msg.data)>2_000_000:raise ValueError('Invalid compressed image')
                with PILImage.open(BytesIO(bytes(msg.data))) as picture:
                    if picture.size!=(640,480):raise ValueError('Unexpected camera dimensions')
                    rgb=np.array(picture.convert('RGB'),dtype=np.uint8)
            except (OSError,ValueError):stats['rejected']+=1;return
        else:
            if msg.encoding!='rgb8' or msg.step!=msg.width*3 or len(msg.data)!=msg.height*msg.step:stats['rejected']+=1;return
            rgb=np.frombuffer(bytes(msg.data),dtype=np.uint8).reshape(msg.height,msg.width,3).copy()
        remember(frames[camera],msg.header.frame_id,(get_stamp(msg.header),rgb));stats['frames']+=1
        event('image_'+camera,msg.header.frame_id)
    subscriptions=[]
    for camera in frames:subscriptions.append(node.create_subscription(CompressedImage if c['schema']=='so101.ros2.sim.v2' else Image,topics[camera]['name'],lambda msg,k=camera:receive_image(k,msg),q))
    def receive_state(msg):
        if list(msg.name)!=c['names'] or len(msg.position)!=6:stats['rejected']+=1;return
        remember(states,msg.header.frame_id,(get_stamp(msg.header),dict(zip(msg.name,joints_from_ros(msg.position)))))
        event('state',msg.header.frame_id)
    def receive_ack(msg):
        try:
            value=verify_packet(json.loads(msg.data),c,token);remember(acks,value['id'],value)
            event('ack',value['id'])
        except Exception:stats['rejected']+=1
    subscriptions += [node.create_subscription(JointState,topics['state']['name'],receive_state,q),node.create_subscription(String,topics['observation']['name'],receive_ack,q)]
    last_clock=-1
    def receive_clock(msg):
        nonlocal last_clock
        value=msg.clock.sec*1000000000+msg.clock.nanosec
        if value<last_clock:stats['rejected']+=1;stats['clock_regressions']=stats.get('clock_regressions',0)+1
        else:last_clock=value;stats['clock_messages']+=1
    subscriptions.append(node.create_subscription(Clock,topics['clock']['name'],receive_clock,q))
    previous=0;seen=None;pending=None;last_status=0
    try:
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            rclpy.spin_once(node,timeout_sec=.05)
            if node.count_subscribers(topics['command']['name']) and node.count_publishers(topics['observation']['name']):break
        else:raise TimeoutError('Isaac ROS peers not connected')
        discovered=dict(node.get_topic_names_and_types())
        endpoint_status={}
        for entry in topics.values():
            if discovered.get(entry['name'])!=[entry['type']]:raise ValueError('Actual ROS Topic type mismatch')
            for info in node.get_publishers_info_by_topic(entry['name'])+node.get_subscriptions_info_by_topic(entry['name']):
                if info.qos_profile.reliability!=q.reliability or info.qos_profile.durability!=q.durability:raise ValueError('Actual ROS QoS incompatible')
                history=info.qos_profile.history
                if history.name not in ('UNKNOWN','SYSTEM_DEFAULT') and history!=q.history:raise ValueError('Actual ROS history incompatible')
                if info.qos_profile.depth and info.qos_profile.depth!=q.depth:raise ValueError('Actual ROS depth incompatible')
                endpoint_status.setdefault(entry['name'],[]).append({'node_name':info.node_name,'node_namespace':info.node_namespace,
                    'reliability':info.qos_profile.reliability.name,'durability':info.qos_profile.durability.name,
                    'history':history.name,'depth':info.qos_profile.depth})
        peer=None
        if c.get('peer_handshake_required'):
            identity='describe-'+uuid.uuid4().hex
            request={'id':identity,'client':identity,'sequence':0,'generation':0,'deadline_ns':time.monotonic_ns()+10_000_000_000,'operation':'describe','calibration_hash':c['calibration_hash']}
            msg=String();msg.data=json.dumps(sign_request(request,c,token));end=time.monotonic()+10;sent=0
            while identity not in acks and time.monotonic()<end:
                if time.monotonic()-sent>.3:command.publish(msg);sent=time.monotonic()
                rclpy.spin_once(node,timeout_sec=.05)
            peer=acks.pop(identity,None)
            expected=next(iter(topics.values()))['qos']
            if not peer or peer.get('peer_contract')!=c or peer.get('factory_qos')!=expected or not peer.get('read_only'):raise ValueError('Signed peer contract/factory QoS handshake failed')
        publish('ready.json',{'robot_id':c['robot_id'],'calibration_hash':c['calibration_hash'],'kind':'ROS2 simulation gateway','config_hash':contract_hash(c),'topics':discovered,'serial_blocked':True,
            'nodes':[{'name':name,'namespace':namespace} for name,namespace in node.get_node_names_and_namespaces()],
            'topic_status':endpoint_status,'qos_status':{'discovery_reliability_durability':True,'configured_history_depth':True,
                'signed_peer_factory_verified':bool(peer),'history_depth_note':'DDS may omit history/depth; verify configured local and signed peer factory profiles, reject any observable mismatch'}})
        while not (root/'shutdown.request').exists():
            rclpy.spin_once(node,timeout_sec=.002)
            try:req=json.loads((root/'request.json').read_text())
            except (FileNotFoundError,json.JSONDecodeError):req=None
            if req and req.get('id')!=seen:
                seen=req['id'];pending=req;msg=String();msg.data=json.dumps(sign_request(req,c,token));command.publish(msg);stats['commands']+=1
                event('request_'+req['operation'],seen)
            if pending and pending['id'] in acks:
                identity=pending['id'];reply=acks[identity]
                if reply.get('images') and not reply.get('error'):
                    if identity not in states or any(identity not in cache for cache in frames.values()):
                        stats['waiting_snapshot']={'id':identity,'state_ready':identity in states,'images_ready':{k:identity in cache for k,cache in frames.items()}}
                        publish('ros-status.json',stats)
                        continue
                    captured=reply['captured_monotonic_ns']
                    try:
                        if last_clock<0 or stats.get('clock_regressions',0):raise ValueError('Simulation clock missing or regressed; reconnect after explicit scene recovery')
                        check_snapshot({k:(identity,cache[identity][0]) for k,cache in frames.items()},(identity,states[identity][0]),identity,captured,previous)
                        if any(abs(states[identity][1][n]-reply['positions'][n])>1e-5 for n in c['names']):raise ValueError('State Topic differs from observation metadata')
                        if not 0<=time.monotonic_ns()-captured<=2_000_000_000:raise ValueError('ROS observation is stale')
                        for k,cache in frames.items():
                            with (root/(k+'.npy.tmp')).open('wb') as f:np.save(f,cache[identity][1],allow_pickle=False)
                            (root/(k+'.npy.tmp')).replace(root/(k+'.npy'))
                        previous=captured
                        stats['timestamp_status']={'validated':True,'captured_monotonic_ns':captured,'monotonic':True,'state_camera_skew_ns':0,
                            'observation_age_ms':(time.monotonic_ns()-captured)/1e6,'time_source':c['time_source'],'clock_regressions':stats.get('clock_regressions',0)}
                        publish('timestamp-status.json',stats['timestamp_status'])
                    except Exception as exc:reply={'id':identity,'error':str(exc)};stats['rejected']+=1
                publish('response.json',reply);acks.pop(identity);pending=None;stats['responses']+=1
                event('response',identity)
            if time.monotonic()-last_status>1:
                publish('ros-status.json',{**stats,'stamp':time.monotonic_ns(),'pending':pending['id'] if pending else None});last_status=time.monotonic()
    finally:
        publish('ros-status.json',{**stats,'closed':True,'state_ids':list(states),'frame_ids':{k:list(v) for k,v in frames.items()},'ack_ids':list(acks)});trace.close();node.destroy_node();rclpy.shutdown()

if __name__=='__main__':main()
