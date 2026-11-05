"""Signed ROS publisher for physical follower tests. No automatic workspace confirmation."""
import argparse,json,time,uuid
from pathlib import Path
import rclpy
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
from std_msgs.msg import String
from .protocol import *
def main():
    p=argparse.ArgumentParser();p.add_argument('--key-file',type=Path,required=True);p.add_argument('--operation',required=True);p.add_argument('--payload',default='{}');p.add_argument('--request-id');p.add_argument('--wait',action='store_true');a=p.parse_args();key=a.key_file.read_text().strip();payload=json.loads(a.payload)
    rid=a.request_id or 'request-'+uuid.uuid4().hex;packet=envelope(a.operation,payload,rid,key)
    rclpy.init();node=rclpy.create_node('follower_test_'+uuid.uuid4().hex[:8]);q=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE,history=HistoryPolicy.KEEP_LAST)
    topic=ACTION_TOPIC if a.operation=='execute' else CONTROL_TOPIC;pub=node.create_publisher(String,topic,q);control=node.create_publisher(String,CONTROL_TOPIC,q);results=[]
    def receive(msg):
        import hmac
        v=json.loads(msg.data);unsigned={k:x for k,x in v.items() if k!='signature'}
        if not hmac.compare_digest(v.get('signature',''),sign(unsigned,key)['signature']):return
        if v.get('request_id')==rid:results.append(unsigned)
    sub=node.create_subscription(String,RESULT_TOPIC if a.operation=='execute' else CONTROL_RESULT_TOPIC,receive,q)
    end=time.monotonic()+(120 if a.wait else 15);last=heartbeat=0;accepted=False
    try:
        while time.monotonic()<end:
            now=time.monotonic()
            if not accepted and now-last>.5:
                packet=envelope(a.operation,payload,rid,key);m=String();m.data=json.dumps(packet);pub.publish(m);last=now
            if accepted and 'epoch' in payload and now-heartbeat>2:
                m=String();m.data=json.dumps(envelope('heartbeat',{'epoch':payload['epoch']},'hb-'+uuid.uuid4().hex,key));control.publish(m);heartbeat=now
            rclpy.spin_once(node,timeout_sec=.05)
            while results:
                result=results.pop(0);state=result.get('result',{}).get('state');accepted=True
                if not a.wait or state not in ('QUEUED','RUNNING') or not result.get('ok'):
                    print(json.dumps(result,ensure_ascii=False));return
        raise TimeoutError('Result unknown; reuse same request_id; do not blindly replay movement')
    finally:node.destroy_node();rclpy.shutdown()
if __name__=='__main__':main()
