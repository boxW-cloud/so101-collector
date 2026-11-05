"""Public ROS2 request/response transport; long operations return operation_id."""
import argparse,json,urllib.request,time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from queue import Queue,Empty
import rclpy
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
from std_msgs.msg import String
from collection_service.leader_topic import accept_leader

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);a=p.parse_args()
    with open(a.config) as f:config=json.load(f)
    rclpy.init();node=rclpy.create_node('collection_api',namespace=config['api_namespace'])
    q=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE,history=HistoryPolicy.KEEP_LAST)
    publisher=node.create_publisher(String,config['api_namespace']+'/responses',q)
    pushes={k:node.create_publisher(String,config['api_namespace']+'/'+k,q) for k in ('events','status','progress')}
    pool=ThreadPoolExecutor(4);out=Queue(64);pending=set()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def process(text):
        try:
            req=urllib.request.Request(f"http://127.0.0.1:{config['internal_port']}/dispatch",data=text.encode(),headers={'Content-Type':'application/json'})
            with opener.open(req,timeout=10) as response:value=response.read().decode()
            out.put_nowait(value)
        except Exception:pass  # Caller times out; no unsigned successful reply is fabricated.
    def receive(msg):
        if len(msg.data.encode())>65536 or len(pending)>=16:return
        future=pool.submit(process,msg.data);pending.add(future)
        future.add_done_callback(pending.discard)
    sub=node.create_subscription(String,config['api_namespace']+'/requests',receive,q)
    leader_sub=None;leader_seq=-1
    source=config['sources'].get('com16_leader')
    if source and source['kind']=='native_topic':
        profile=json.loads(Path(source['profile']).read_text());leader_key=Path(source['key_file']).read_text().strip()
        clock_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip();directory=Path(source['root']);directory.mkdir(parents=True,exist_ok=True)
        counts={'accepted':0,'rejected':0}
        def leader(msg):
            nonlocal leader_seq
            try:
                packet=accept_leader(json.loads(msg.data),leader_key,profile['profile_hash'],clock_id,leader_seq)
                temp=directory/'input.tmp';temp.write_text(json.dumps(packet));temp.replace(directory/'input.json');leader_seq=packet['sequence']
                counts['accepted']+=1
            except (ValueError,KeyError,TypeError):counts['rejected']+=1
            temp=directory/'status.tmp';temp.write_text(json.dumps({**counts,'last_sequence':leader_seq}));temp.replace(directory/'status.json')
        leader_sub=node.create_subscription(String,config['api_namespace']+'/leader',leader,QoSProfile(depth=4,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE))
    generation=None;last_event=0;last_poll=0
    try:
        while rclpy.ok():
            rclpy.spin_once(node,timeout_sec=.02)
            if time.monotonic()-last_poll>.1:
                last_poll=time.monotonic()
                try:events=json.loads((Path(config['data_root'])/'events.json').read_text())
                except (OSError,ValueError):events=[]
                for event in events:
                    if event['generation']!=generation:generation=event['generation'];last_event=0
                    if event['sequence']<=last_event:continue
                    topic='status' if event['kind'] in ('collection.status','simulation.verification.status') else 'progress' if event['kind']=='upload.progress' else 'events'
                    msg=String();msg.data=json.dumps(event);pushes[topic].publish(msg);last_event=event['sequence']
            while True:
                try:value=out.get_nowait()
                except Empty:break
                msg=String();msg.data=value;publisher.publish(msg)
    finally:pool.shutdown(wait=True);node.destroy_node();rclpy.shutdown()

if __name__=='__main__':main()
