"""Real DDS endpoint used inside Isaac's bundled Python3.11 runtime."""
import json,time
from collections import deque
from pathlib import Path
import rclpy
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
from std_msgs.msg import String
from sensor_msgs.msg import Image,JointState,CompressedImage
from rosgraph_msgs.msg import Clock
from ros_sim_contract import validate_contract,sign_request,verify_request,joints_to_ros,validate_ros_environment

def qos(c):
    return QoSProfile(depth=4,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE,history=HistoryPolicy.KEEP_LAST)
def stamp(msg,value):msg.sec=value//1000000000;msg.nanosec=value%1000000000

class RosSimulatorTransport:
    def __init__(self,config,token_file):
        self.config=validate_contract(json.loads(Path(config).read_text()));self.token=Path(token_file).read_text().strip()
        validate_ros_environment(self.config)
        rclpy.init();self.node=rclpy.create_node('isaac_simulator',namespace=self.config['namespace'])
        self.requests=deque(maxlen=8);self.rejected=0;self.published=0
        topics=self.config['topics'];q=qos(self.config)
        self.pub=self.node.create_publisher(String,topics['observation']['name'],q)
        self.state=self.node.create_publisher(JointState,topics['state']['name'],q)
        self.compressed=self.config['schema']=='so101.ros2.sim.v2'
        self.images={k:self.node.create_publisher(CompressedImage if self.compressed else Image,topics[k]['name'],q) for k in ('front','wrist')}
        self.clock=self.node.create_publisher(Clock,topics['clock']['name'],q)
        self.subscription=self.node.create_subscription(String,topics['command']['name'],self.receive,q)
    def receive(self,msg):
        try:
            req=verify_request(json.loads(msg.data),self.config,self.token,time.monotonic_ns())
            if req['operation']=='describe':
                local=qos(self.config)
                self.reply({'id':req['id'],'peer_contract':self.config,'node':{'name':self.node.get_name(),'namespace':self.node.get_namespace()},
                    'factory_qos':{'reliability':local.reliability.name.lower(),'durability':local.durability.name.lower(),'history':local.history.name.lower(),'depth':local.depth},
                    'read_only':True})
                return
            if len(self.requests)==self.requests.maxlen:raise ValueError('Command queue full')
            self.requests.append(req)
        except Exception:self.rejected+=1
    def poll(self):
        for _ in range(8):rclpy.spin_once(self.node,timeout_sec=0)
        return self.requests.popleft() if self.requests else None
    def image(self,key,rgb,request_id,captured_ns):
        if self.compressed:
            from PIL import Image as PILImage
            from io import BytesIO
            stream=BytesIO();PILImage.fromarray(rgb[:,:,:3]).save(stream,format='JPEG',quality=90)
            msg=CompressedImage();msg.header.frame_id=request_id;stamp(msg.header.stamp,captured_ns)
            msg.format='rgb8; jpeg compressed rgb8';msg.data=stream.getvalue();self.images[key].publish(msg);return
        msg=Image();msg.header.frame_id=request_id;stamp(msg.header.stamp,captured_ns)
        msg.height=int(rgb.shape[0]);msg.width=int(rgb.shape[1]);msg.encoding='rgb8';msg.is_bigendian=0;msg.step=msg.width*3
        msg.data=rgb[:,:,:3].tobytes();self.images[key].publish(msg)
    def reply(self,response):
        if 'positions' in response:
            msg=JointState();msg.header.frame_id=response['id'];stamp(msg.header.stamp,response['captured_monotonic_ns'])
            msg.name=list(self.config['names']);msg.position=joints_to_ros([response['positions'][n] for n in msg.name]);self.state.publish(msg)
        message=String();message.data=json.dumps(sign_request(response,self.config,self.token));self.pub.publish(message);self.published+=1
    def tick(self,seconds):
        msg=Clock();stamp(msg.clock,int(seconds*1e9));self.clock.publish(msg)
    def close(self):self.node.destroy_node();rclpy.shutdown()
