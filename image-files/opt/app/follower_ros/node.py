"""Spark ROS2 Topics -> SSH reverse tunnel -> Windows native-controller adapter."""
import argparse,json,time,uuid,urllib.request,urllib.error,threading,os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from queue import Queue,Empty
import rclpy
from rclpy.qos import QoSProfile,ReliabilityPolicy,DurabilityPolicy,HistoryPolicy
from std_msgs.msg import String
from .protocol import *

def main():
    p=argparse.ArgumentParser();p.add_argument('--key-file',type=Path,required=True);p.add_argument('--agent-url',default='http://127.0.0.1:18871');a=p.parse_args()
    key=a.key_file.read_text().strip();opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    rclpy.init();node=rclpy.create_node('real_follower_actions',namespace='/so101_follower')
    q=QoSProfile(depth=16,reliability=ReliabilityPolicy.RELIABLE,durability=DurabilityPolicy.VOLATILE,history=HistoryPolicy.KEEP_LAST)
    pubs={t:node.create_publisher(String,t,q) for t in (RESULT_TOPIC,CONTROL_RESULT_TOPIC,STATUS_TOPIC,FEEDBACK_TOPIC,CATALOG_TOPIC)}
    queue=Queue(128);pool=ThreadPoolExecutor(4);admission=threading.BoundedSemaphore(12);known={};lock=threading.Lock()
    def emit(topic,value):
        try:queue.put_nowait((topic,sign(value,key)))
        except Exception:pass  # Actual operation results remain queryable by request_id.
    def rpc(packet):
        request=urllib.request.Request(a.agent_url+'/rpc',data=json.dumps(packet,ensure_ascii=False).encode(),headers={'Content-Type':'application/json'})
        try:response=opener.open(request,timeout=12)
        except urllib.error.HTTPError as exc:response=exc
        with response:reply=json.load(response)
        unsigned={k:v for k,v in reply.items() if k!='signature'}
        import hmac
        if not hmac.compare_digest(reply.get('signature',''),sign(unsigned,key)['signature']):raise Error('AGENT_SIGNATURE','Untrusted agent reply')
        return unsigned
    def process(packet,action):
        rid=packet.get('request_id','');topic=RESULT_TOPIC if action else CONTROL_RESULT_TOPIC
        try:
            req=verify(packet,key)
            if action!=(req['operation']=='execute'):raise Error('WRONG_TOPIC','execute belongs on actions/request; control operations on control/request')
            if not action and req['operation'] not in ('inspect','catalog','bind','heartbeat','confirm_workspace','stop','release_control','job'):raise Error('UNSUPPORTED_OPERATION','Unsupported control request')
            reply=rpc(packet);reply['request_id']=rid
            emit(topic,reply)
            if action and reply.get('ok') and reply['result']['state'] in ('QUEUED','RUNNING'):
                with lock:known[rid]=reply['result']
        except Exception as exc:emit(topic,{'request_id':rid,'ok':False,'error':{'code':getattr(exc,'code','AGENT_UNAVAILABLE'),'message':str(exc)},'outcome_unknown':not isinstance(exc,Error)})
        finally:admission.release()
    def receive(message,action):
        if len(message.data.encode())>32768:return
        try:packet=json.loads(message.data);verify(packet,key)
        except Exception as exc:
            emit(RESULT_TOPIC if action else CONTROL_RESULT_TOPIC,{'ok':False,'error':{'code':getattr(exc,'code','INVALID_JSON'),'message':str(exc)}});return
        if not admission.acquire(blocking=False):
            emit(RESULT_TOPIC if action else CONTROL_RESULT_TOPIC,{'request_id':packet['request_id'],'ok':False,'error':{'code':'BUSY','message':'Gateway request bound reached'}});return
        pool.submit(process,packet,action)
    subs=[node.create_subscription(String,ACTION_TOPIC,lambda m:receive(m,True),q),node.create_subscription(String,CONTROL_TOPIC,lambda m:receive(m,False),q)]
    stopping=threading.Event()
    def observe():
        last_catalog=0
        while not stopping.is_set():
            try:
                reply=rpc(envelope('inspect',{},'inspect-'+uuid.uuid4().hex,key));emit(FEEDBACK_TOPIC,{'at':time.time(),**reply})
                if time.monotonic()-last_catalog>5:
                    catalog=rpc(envelope('catalog',{},'catalog-'+uuid.uuid4().hex,key));emit(CATALOG_TOPIC,catalog);last_catalog=time.monotonic()
                with lock:ids=list(known)
                for rid in ids:
                    result=rpc(envelope('job',{'request_id':rid},'query-'+uuid.uuid4().hex,key));emit(STATUS_TOPIC,{**result,'request_id':rid})
                    if result.get('ok') and result['result']['state'] not in ('QUEUED','RUNNING'):
                        emit(RESULT_TOPIC,{**result,'request_id':rid})
                        with lock:known.pop(rid,None)
            except Exception as exc:emit(FEEDBACK_TOPIC,{'at':time.time(),'ok':False,'error':{'code':'AGENT_UNAVAILABLE','message':str(exc)}})
            stopping.wait(.5)
    observer=threading.Thread(target=observe,daemon=True);observer.start()
    ready_file=os.environ.get('SO101_READY_FILE')
    if ready_file:Path(ready_file).write_text(json.dumps({'ros_node_ready':True,'at':time.time()}))
    try:
        while rclpy.ok():
            rclpy.spin_once(node,timeout_sec=.02)
            while True:
                try:topic,value=queue.get_nowait()
                except Empty:break
                msg=String();msg.data=json.dumps(value,ensure_ascii=False);pubs[topic].publish(msg)
    finally:stopping.set();pool.shutdown(wait=False,cancel_futures=True);node.destroy_node();rclpy.shutdown()
if __name__=='__main__':main()
