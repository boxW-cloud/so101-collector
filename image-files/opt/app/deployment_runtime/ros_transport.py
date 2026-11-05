"""Actual rclpy Topic transport; imported only in ROS-enabled process."""
import base64,copy,json,os,threading,time,uuid
from concurrent.futures import Future,TimeoutError as FutureTimeout
from pathlib import Path
from .ros_wire import request,verify,Peer,FixtureAdapter,clock_id
from .store import Fault

def set_image_stamp(image,frame):
    captured=frame['captured_monotonic_ns'];image.header.stamp.sec=captured//10**9;image.header.stamp.nanosec=captured%10**9

class TopicRPC:
    def __init__(self,config):
        import rclpy
        from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
        from std_msgs.msg import String
        self.rclpy=rclpy;self.String=String;self.config=config;self.responses={};self.waiting=set();self.waiting_operations={};self.lock=threading.RLock();self.closed=False
        from .ros_contract import complete,runtime_profile
        config=complete(config,tuple(config.get('camera_topics',{'front':None})));self.config=config
        runtime_profile(config)
        namespace=config['namespace']
        import re
        if not re.fullmatch(r'/[a-z][a-z0-9_/]*',namespace):raise Fault('INVALID_NAMESPACE','Configured ROS namespace required')
        if str(config['domain_id'])!=os.environ.get('ROS_DOMAIN_ID'):raise Fault('ROS_DOMAIN_MISMATCH','Container ROS Domain differs from robot configuration')
        if config.get('rmw_implementation','rmw_fastrtps_cpp')!=os.environ.get('RMW_IMPLEMENTATION'):raise Fault('ROS_RMW_MISMATCH','RMW differs')
        self.key=Path(config['key_file']).read_text().strip()
        self.context=rclpy.context.Context();rclpy.init(context=self.context)
        self.node=rclpy.create_node('sdk_client_'+uuid.uuid4().hex[:8],namespace=namespace,context=self.context)
        q=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE,history=HistoryPolicy.KEEP_LAST)
        self.pub=self.node.create_publisher(String,namespace+'/command',q)
        def reply(msg):
            try:
                v=verify(json.loads(msg.data),self.key)
                with self.lock:
                    rid=v['request_id']
                    # Every client sees the result Topic. Retain only replies
                    # to its currently awaited requests, not other clients'
                    # camera payloads or late replies from an old timeout.
                    if rid not in self.waiting:return
                    prior=self.responses.get(rid)
                    if prior and (prior.get('ok') or prior.get('error',{}).get('code')!='IN_PROGRESS'):return
                    self.responses[rid]=v
            except Exception:pass
        self.sub=self.node.create_subscription(String,namespace+'/result',reply,q)
        from rclpy.executors import SingleThreadedExecutor
        self.executor=SingleThreadedExecutor(context=self.context);self.executor.add_node(self.node);self.spin_error=None
        self.thread=threading.Thread(target=self.spin,daemon=True);self.thread.start()
    def spin(self):
        try:
            while not self.closed:self.executor.spin_once(timeout_sec=.05)
        except Exception as e:self.spin_error=str(e)
    def call(self,packet,timeout):
        rid=packet['request_id'];end=time.monotonic()+timeout;last=0
        with self.lock:
            if rid in self.waiting:raise Fault('IN_PROGRESS','Request already awaited by this ROS client')
            self.responses.pop(rid,None);self.waiting.add(rid)
            if not hasattr(self,'waiting_operations'):self.waiting_operations={}
            self.waiting_operations[rid]=packet.get('operation')
        try:
            while time.monotonic()<end:
                if self.closed:raise Fault('ROS_RPC_CLOSED','ROS client was closed')
                if self.spin_error:raise Fault('ROS_EXECUTOR_FAILED',self.spin_error)
                if time.monotonic()-last>.2:
                    m=self.String();m.data=json.dumps(packet);self.pub.publish(m);last=time.monotonic()
                with self.lock:result=self.responses.pop(rid,None)
                if result:
                    if not result['ok'] and result['error']['code']=='IN_PROGRESS':continue
                    if not result['ok']:raise Fault(result['error']['code'],result['error']['message'])
                    return result['result']
                time.sleep(.005)
            raise TimeoutError('ROS response unknown; retain stop responsibility')
        finally:
            with self.lock:self.waiting.discard(rid);self.waiting_operations.pop(rid,None);self.responses.pop(rid,None)
    def cancel_pending(self):
        """Wake local ordinary callers; this does not claim remote stop success."""
        with self.lock:
            for rid in self.waiting:
                if getattr(self,'waiting_operations',{}).get(rid) not in ('hold','close'):
                    self.responses[rid]={'request_id':rid,'ok':False,'error':{'code':'STOPPED','message':'Ordinary ROS request cancelled by local stop intent'}}
    def close(self):self.closed=True;self.thread.join(2);self.executor.shutdown(timeout_sec=1);self.node.destroy_node();self.context.shutdown()
    def preflight(self,timeout):
        from rclpy.qos import ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
        topics=[(self.config['command_topic'],False)]+[(self.config[n],True) for n in ('result_topic','state_topic','observation_topic','clock_topic')]+[(v,True) for v in self.config['camera_topics'].values()]
        end=time.monotonic()+timeout
        while True:
            missing=[];described=[]
            for topic,publisher in topics:
                infos=(self.node.get_publishers_info_by_topic if publisher else self.node.get_subscriptions_info_by_topic)(topic['name'])
                if not infos:missing.append(topic['name']);continue
                for info in infos:
                    q=info.qos_profile
                    # FastDDS discovery reports History/Depth UNKNOWN; require
                    # the signed peer contract for those local endpoint fields.
                    if info.topic_type!=topic['type'] or q.reliability!=ReliabilityPolicy.RELIABLE or q.durability!=DurabilityPolicy.VOLATILE or q.history not in (HistoryPolicy.KEEP_LAST,HistoryPolicy.UNKNOWN):raise Fault('ROS_GRAPH_CONTRACT','Actual Topic type/QoS differs: '+topic['name'])
                    if q.history==HistoryPolicy.KEEP_LAST and q.depth!=16:raise Fault('ROS_GRAPH_CONTRACT','Actual Topic depth differs: '+topic['name'])
                described.append({**copy.deepcopy(topic),'direction':'publisher' if publisher else 'subscription','endpoint_count':len(infos),
                                  'history_depth_source':'peer_declaration' if any(i.qos_profile.history==HistoryPolicy.UNKNOWN for i in infos) else 'discovery'})
            if not missing:
                nodes=[{'name':n,'namespace':ns} for n,ns in self.node.get_node_names_and_namespaces() if ns==self.config['namespace']]
                return {'topics_verified':len(topics),'nodes':nodes,'topics':described}
            if time.monotonic()>=end:raise Fault('ROS_GRAPH_MISSING','Missing registered endpoints: '+', '.join(missing))
            time.sleep(.05)

class RosBackend:
    def __init__(self,robot):
        from collections import deque
        self.robot=robot;self.kind=robot['kind'];self.commissioned=robot.get('commissioned') is True
        self.receipt_envelope_tolerance={}
        if self.kind=='physical' and robot.get('provenance')=='physical_native_relative_teleop':
            supplied=robot.get('native_goal_receipt_tolerance',{})
            if isinstance(supplied,dict):
                self.receipt_envelope_tolerance={name:float(value) for name,value in supplied.items()
                    if type(value) in (int,float) and 0<=value<=5}
        from .direct_follower import is_direct
        if is_direct(robot):
            # 舵机目标按格写入（1 格约 0.088°，夹爪 1 格不到 0.1%），回执与请求最多差半格
            self.receipt_envelope_tolerance={name:0.1 for name in robot['sdk_config']['units']}
        self.resource_id='ros:'+robot['robot_id'];self.client=uuid.uuid4().hex;self.epoch=0;self.io_lock=threading.RLock()
        self._state_lock=threading.RLock();self._priority_lock=threading.Lock();self._generation=0
        self._interrupt_future=None;self._stop_latched=False
        # Import/create the DDS client before the bounded SDK I/O worker starts.
        # This does not acquire a peer lease or send any device command.
        self.rpc=TopicRPC(self.robot['ros'])
        from .field_bindings import unverified_ros_status
        self._diagnostic_lock=threading.Lock();self._diagnostic=unverified_ros_status(robot['ros'])
        self.acquire_attempted=False
        self.sent_history=deque(maxlen=2048)
    def _cancel_local_normal(self):
        cancel=getattr(self.rpc,'cancel_pending',None)
        if callable(cancel):cancel()
    def _await_interrupt(self,deadline):
        with self._state_lock:future=self._interrupt_future
        if future is not None:
            try:future.result(timeout=max(0,deadline-time.monotonic()))
            except FutureTimeout as error:raise Fault('STOP_UNCONFIRMED','Priority ROS HOLD result is still unknown') from error
    def _priority_call(self,op,payload,deadline,*,already_fenced=False):
        if not self._priority_lock.acquire(timeout=max(0,deadline-time.monotonic())):
            raise Fault('STOP_UNCONFIRMED','Another priority stop is still pending')
        try:
            with self._state_lock:
                if not already_fenced:self._generation+=1
                self._stop_latched=True;epoch=self.epoch;rpc=self.rpc
            self._cancel_local_normal()
            if rpc is None:raise Fault('ROS_RPC_CLOSED','ROS client has been closed')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise Fault('STOP_UNCONFIRMED','Priority ROS stop deadline expired')
            value=rpc.call(request(op,payload,self.client,epoch,rpc.key),remaining)
            with self._state_lock:
                if self.rpc is not rpc or self.epoch!=epoch:raise Fault('STALE_EPOCH','Priority result does not match the current client epoch')
                if 'epoch' in value:self.epoch=value['epoch']
            return value
        finally:self._priority_lock.release()
    def interrupt_stop(self,timeout_s):
        """Optional SDK hook: fence locally and asynchronously request HOLD."""
        if type(timeout_s) not in (int,float) or not 0<timeout_s<=10:raise ValueError('Bounded priority stop timeout required')
        with self._state_lock:
            self._generation+=1;self._stop_latched=True
            existing=self._interrupt_future
            if existing is not None and (not existing.done() or existing.exception() is None):
                self._cancel_local_normal();return None
            if not self.acquire_attempted or self.rpc is None:
                self._cancel_local_normal();return None  # Never acquire a peer from stop intent.
            future=Future();self._interrupt_future=future
        self._cancel_local_normal()
        deadline=time.monotonic()+timeout_s
        def stop():
            try:
                value=self._priority_call('hold',{},deadline,already_fenced=True)
                if value.get('holding') is not True:raise Fault('STOP_UNCONFIRMED','Priority peer did not confirm actual holding')
                future.set_result(value)
            except BaseException as error:future.set_exception(error)
        threading.Thread(target=stop,daemon=True,name='ros-backend-priority-hold').start()
        return None
    def call(self,op,payload,timeout):
        deadline=time.monotonic()+timeout
        if op in ('hold','close'):
            try:self._await_interrupt(deadline)
            except Exception:pass  # Explicit stop may retry unknown priority HOLD.
            return self._priority_call(op,payload,deadline)
        with self._state_lock:generation=self._generation
        self._await_interrupt(deadline)
        with self.io_lock:
            with self._state_lock:
                if generation!=self._generation:raise Fault('STOPPED','Older ROS operation cancelled before issue')
                if self._stop_latched and op in ('connect','send','reset','primitive','teleop.start'):raise Fault('STOPPED','Target commands remain fenced until actual HOLD is confirmed')
                epoch=self.epoch;rpc=self.rpc
            if rpc is None:raise Fault('ROS_RPC_CLOSED','ROS client has been closed')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('ROS operation deadline expired before issue')
            value=rpc.call(request(op,payload,self.client,epoch,rpc.key),remaining)
            with self._state_lock:
                if generation!=self._generation or self.rpc is not rpc or epoch!=self.epoch:raise Fault('STOPPED','Late ordinary ROS response cannot restore a stopped epoch')
                if 'epoch' in value:self.epoch=value['epoch']
            return value
    def connect(self,timeout_s):
        with self._state_lock:generation=self._generation
        v=self.call('inspect',{},timeout_s)
        if v.get('clock_id')!=clock_id():raise Fault('CLOCK_DOMAIN_MISMATCH','Direct ROS observations require the same host clock; deploy a validated clock bridge for Windows-to-Linux')
        if v['kind']!=self.kind or v['robot_id']!=self.robot['robot_id'] or v['calibration_hash']!=self.robot['sdk_config']['calibration_hash']:raise Fault('PEER_MISMATCH','ROS peer identity/calibration differs')
        if self.kind=='physical' and not v.get('commissioned'):raise Fault('NOT_COMMISSIONED','Physical peer not commissioned')
        if 'collection' not in v['capabilities']:raise Fault('CAPABILITY_MISSING','Peer cannot provide full state/action/camera collection')
        if v['source']=='test_fixture' and self.robot.get('provenance')!='test_fixture':raise Fault('SOURCE_MISMATCH','Fixture is not an Isaac or physical source')
        expected={k:self.rpc.config[k] for k in ('command_topic','result_topic','state_topic','observation_topic','camera_topics','clock_topic','contract_version')}
        if v.get('transport_contract')!=expected:raise Fault('ROS_PEER_CONTRACT','Signed peer Topic/type/QoS declaration differs')
        graph=self.rpc.preflight(timeout_s)
        with self._state_lock:
            if generation!=self._generation:raise Fault('STOPPED','Connect was cancelled before peer acquisition')
            self.acquire_attempted=True
        self.call('connect',{},timeout_s)
        with self._diagnostic_lock:
            self._diagnostic.update(connection_state='CONNECTED',checked_at_monotonic_ns=time.monotonic_ns(),configuration_verified=True,
                                    topics_verified=graph['topics_verified'],nodes=graph.get('nodes',[]),topics=graph.get('topics',[]))
            self._diagnostic['timestamp_status']['clock_domain_verified']=True
    def sample(self,timeout_s):
        from lerobot_sdk.control.backend import BackendSnapshot
        from .observation_frames import decode_sample_frames
        v=self.call('sample',{},timeout_s)
        images,frame_records=decode_sample_frames(v['images'],self.robot['camera_shapes'])
        from .media import publish
        publish(self.robot['robot_id'],images)
        with self._diagnostic_lock:
            self._diagnostic['checked_at_monotonic_ns']=time.monotonic_ns()
            self._diagnostic['timestamp_status']={**self._diagnostic['timestamp_status'],
                'last_state_captured_monotonic_ns':v['captured_monotonic_ns'],
                'last_camera_captured_monotonic_ns':{k:f.captured_monotonic_ns for k,f in images.items()}}
        # source_action：数据源按观测编号配好对的"这一帧实际下发的目标"（redis_capture，COLLECTION_REDIS_ACTION=paired）
        metadata={'clock_domain':'ROS_recording_host_monotonic_lower_bound','image_frames':frame_records,
                  **{key:v[key] for key in ('clock_mapping','device_profile','source_action') if v.get(key) is not None}}
        return BackendSnapshot(v['captured_monotonic_ns'],v['positions'],images,metadata)
    def diagnostics(self):
        with self._diagnostic_lock:value=copy.deepcopy(self._diagnostic)
        if self.rpc is None:value['connection_state']='CLOSED'
        elif self.rpc.spin_error:value['connection_state']='UNVERIFIED'
        stamp=value['timestamp_status'];now=time.monotonic_ns();state=stamp['last_state_captured_monotonic_ns']
        stamp['age_ms']=(now-state)/1e6 if type(state)==int else None
        stamp['camera_age_ms']={k:(now-t)/1e6 for k,t in stamp['last_camera_captured_monotonic_ns'].items() if type(t)==int}
        ages=[stamp['age_ms'],*stamp['camera_age_ms'].values()]
        maximum=self.robot['sdk_config'].get('max_observation_age_s',.2)*1000
        stamp['sample_within_configured_age']=all(a is not None and 0<=a<=maximum for a in ages) if state is not None else None
        stamp['monotonicity_checked']=False
        return value
    def send(self,positions,timeout_s):
        sent=self.call('send',{'positions':dict(positions)},timeout_s)['sent']
        self.sent_history.append({'sent_monotonic_ns':time.monotonic_ns(),'requested':dict(positions),'sent':sent})
        return sent
    def hold(self,positions,timeout_s):
        if not self.call('hold',{},timeout_s).get('holding'):raise Fault('STOP_UNCONFIRMED','Peer did not confirm hold')
        with self._state_lock:self._stop_latched=False;self._interrupt_future=None
    def close(self,*,hold,timeout_s):
        if self.rpc:
            if self.acquire_attempted:self.call('close',{},timeout_s)
            self.rpc.close();self.rpc=None
    def leader(self,timeout_s):return self.call('leader',{},timeout_s)
    def start_teleop(self,timeout_s):
        from .managed_source import bounded_timeout
        return self.call('teleop.start',{'timeout_s':bounded_timeout(timeout_s)},timeout_s)
    def stop_teleop(self,timeout_s):
        from .managed_source import bounded_timeout
        return self.call('teleop.stop',{'timeout_s':bounded_timeout(timeout_s)},timeout_s)
    def reset(self,timeout_s):return self.call('reset',{},timeout_s)

def serve_peer(adapter,config):
    from .ros_contract import runtime_profile
    runtime_profile(config)
    import rclpy
    from std_msgs.msg import String
    from sensor_msgs.msg import JointState,CompressedImage
    from rosgraph_msgs.msg import Clock
    from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy
    key=Path(config['key_file']).read_text().strip()
    if config.get('bridge_url'):
        from .device_bridge import ForwardPeer
        peer=ForwardPeer(config['bridge_url'],key)
    else:peer=Peer(adapter,key)
    rclpy.init();node=rclpy.create_node('device_owner',namespace=config['namespace']);q=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE)
    pub=node.create_publisher(String,config['namespace']+'/result',q)
    state_pub=node.create_publisher(JointState,config['namespace']+'/state',q)
    obs_pub=node.create_publisher(String,config['namespace']+'/observation',q)
    clock_pub=node.create_publisher(Clock,config['namespace']+'/clock',q)
    from .ros_contract import complete
    config=complete(config,tuple(config.get('camera_topics',{'front':None})))
    camera_pubs={k:node.create_publisher(CompressedImage,v['name'],q) for k,v in config['camera_topics'].items()}
    from queue import Queue,Empty,Full
    from .ros_dispatch import BoundedRpcDispatcher
    from .ros_wire import sign
    normal_replies=Queue(16);priority_replies=Queue(8)
    def enqueue(value,reply,generation,priority):
        try:(priority_replies if priority else normal_replies).put_nowait((value,reply,generation,priority))
        except Full:pass  # Actual outcomes remain on the peer; timeout is not success.
    enqueue.dispatch_metadata=True
    def execute(value):
        try:return peer.call(value)
        except Exception as error:return sign({'request_id':value['request_id'],'ok':False,'error':{'code':getattr(error,'code',type(error).__name__),'message':str(error)}},key)
    dispatcher=BoundedRpcDispatcher(execute,enqueue)
    def receive(msg):
        if len(msg.data)>2*1024**2:return
        try:
            value=json.loads(msg.data);verify(value,key)
        except Exception:return
        try:dispatcher.submit(value)
        except Exception as error:
            reply=sign({'request_id':value.get('request_id'),'ok':False,'error':{'code':getattr(error,'code','INVALID_REQUEST'),'message':str(error)}},key)
            enqueue(value,reply,dispatcher.generation,value.get('operation') in ('hold','close'))
    def publish_reply(value,reply):
        if value.get('operation')=='inspect' and reply.get('ok'):
            plain=verify(reply,key);plain['result']['transport_contract']={k:config[k] for k in ('command_topic','result_topic','state_topic','observation_topic','camera_topics','clock_topic','contract_version')};reply=sign(plain,key)
        out=String();out.data=json.dumps(reply);pub.publish(out)
        if value.get('operation')=='sample' and reply.get('ok'):
            result=reply['result'];stamp=result['captured_monotonic_ns'];identity=value['request_id']
            import math
            from lerobot_sdk import JOINT_NAMES
            positions=[result['positions'][n] for n in JOINT_NAMES];angles=[math.radians(x) for x in positions[:5]]
            # Existing SO101 Isaac asset's calibrated gripper range is -10..100
            # degrees. A physical adapter must supply its own measured range.
            gripper=config.get('gripper_angle_deg',[-10,100] if adapter and adapter.kind=='simulation' else None)
            if gripper is None:raise Fault('GRIPPER_MAPPING_REQUIRED','Physical ROS JointState needs a registered angle mapping')
            angles.append(math.radians(gripper[0]+positions[5]/100*(gripper[1]-gripper[0])))
            state=JointState();state.name=list(JOINT_NAMES);state.position=angles;state.header.frame_id=identity
            state.header.stamp.sec=stamp//10**9;state.header.stamp.nanosec=stamp%10**9;state_pub.publish(state)
            for camera,frame in result['images'].items():
                if camera not in camera_pubs:raise Fault('CAMERA_NOT_ALLOWED','Peer camera not in registered Topic map')
                image=CompressedImage();image.header.frame_id=identity
                set_image_stamp(image,frame)
                image.format='rgb8; png compressed rgb8';image.data=base64.b64decode(frame['png']);camera_pubs[camera].publish(image)
            obs=String();obs.data=json.dumps({k:v for k,v in result.items() if k!='images'});obs_pub.publish(obs)
            clock=Clock();clock.clock=state.header.stamp;clock_pub.publish(clock)
    sub=node.create_subscription(String,config['namespace']+'/command',receive,q)
    shutdown=threading.Event()
    def watchdog():
        while not shutdown.wait(.05):peer.tick()
    watchdog_thread=threading.Thread(target=watchdog,daemon=True);watchdog_thread.start()
    try:
        while rclpy.ok():
            rclpy.spin_once(node,timeout_sec=.02)
            for replies in (priority_replies,normal_replies):
                for _ in range(16):
                    try:value,reply,generation,priority=replies.get_nowait()
                    except Empty:break
                    dispatcher.publish_if_current(generation,priority,lambda:publish_reply(value,reply))
    finally:
        dispatcher.shutdown(wait=False)  # Latch before any publisher/node is torn down.
        shutdown.set();watchdog_thread.join(3)
        if getattr(peer,'owner',None):peer.shutdown()
        dispatcher.shutdown(wait=True)
        node.destroy_node();rclpy.shutdown()

if __name__=='__main__':
    import argparse
    # v10r15：没有 --isaac（采集容器不再驱动 Isaac）和 --direct-follower（不再直连真实主从臂）；SO101 走 Redis 数据源
    p=argparse.ArgumentParser();p.add_argument('--fixture',action='store_true');p.add_argument('--native-owner',action='store_true');p.add_argument('--bridge',action='store_true');p.add_argument('--config',required=True);a=p.parse_args();config=json.loads(Path(a.config).read_text())
    if sum([a.fixture,a.native_owner,a.bridge])!=1:raise SystemExit('Select exactly one explicit device adapter')
    if a.fixture:adapter=FixtureAdapter()
    elif a.native_owner:
        from .native_adapter import NativeOwnerAdapter
        adapter=NativeOwnerAdapter(config['adapter'])
    elif a.bridge:
        if not config.get('bridge_url') or not config.get('gripper_angle_deg'):raise SystemExit('Bridge endpoint and calibrated gripper angle mapping required')
        adapter=None
    else:raise SystemExit('Explicit adapter required; fixture never substitutes for hardware')
    serve_peer(adapter,config)
