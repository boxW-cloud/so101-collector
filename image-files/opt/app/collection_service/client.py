"""Run via ros_entry.sh; external clients can use the same ROS2 JSON contract."""
import argparse,json,time,uuid
from pathlib import Path
import rclpy
from std_msgs.msg import String
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy
from collection_service.protocol import signed,verified

class Client:
    def __init__(self,config,principal,key):
        self.config=config;self.principal=principal;self.key=key;self.responses={};self.events=[]
        rclpy.init();self.node=rclpy.create_node('collection_client_'+uuid.uuid4().hex[:8])
        qos=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE)
        self.pub=self.node.create_publisher(String,config['api_namespace']+'/requests',qos)
        def receive(msg):
            try:
                result=verified(json.loads(msg.data),self.key);self.responses[result['request_id']]=result
            except Exception:pass
        self.sub=self.node.create_subscription(String,config['api_namespace']+'/responses',receive,qos)
        def event(msg):
            try:
                value=verified(json.loads(msg.data),self.key)
                self.events.append(value)
                if len(self.events)>512:self.events.pop(0)
            except Exception:pass
        self.event_subs=[self.node.create_subscription(String,config['api_namespace']+'/'+name,event,qos) for name in ('status','events','progress')]
    def call(self,operation,payload,request_id=None,timeout=15):
        ident=request_id or uuid.uuid4().hex
        req=signed({'request_id':ident,'principal':self.principal,'operation':operation,'payload':payload,'issued_at':time.time()},self.key)
        msg=String();msg.data=json.dumps(req);end=time.monotonic()+timeout;last=0
        while time.monotonic()<end:
            if time.monotonic()-last>.5:self.pub.publish(msg);last=time.monotonic()
            rclpy.spin_once(self.node,timeout_sec=.05)
            if ident in self.responses:return self.responses.pop(ident)
        raise TimeoutError('ROS2 request timed out; operation may continue, retry same request_id')
    def wait(self,operation_id,timeout=300):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            answer=self.call('operation.get',{'operation_id':operation_id})
            if not answer['ok']:raise RuntimeError(answer)
            job=answer['result']
            if job['status'] not in ('QUEUED','RUNNING'):return job
            time.sleep(.2)
        raise TimeoutError('Operation wait timeout does not cancel the job')
    def close(self):self.node.destroy_node();rclpy.shutdown()

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True);p.add_argument('--secrets',type=Path,required=True)
    p.add_argument('--principal',default='student');p.add_argument('--operation',required=True);p.add_argument('--payload',default='{}');p.add_argument('--request-id');p.add_argument('--wait',action='store_true');a=p.parse_args()
    config=json.loads(a.config.read_text());client=Client(config,a.principal,json.loads(a.secrets.read_text())[a.principal])
    try:
        result=client.call(a.operation,json.loads(a.payload),a.request_id)
        if a.wait and result['ok'] and 'operation_id' in result['result']:result=client.wait(result['result']['operation_id'])
        print(json.dumps(result,ensure_ascii=False))
    finally:client.close()
if __name__=='__main__':main()
