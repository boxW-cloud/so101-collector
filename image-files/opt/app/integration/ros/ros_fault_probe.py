"""Direct real-ROS fault probes using an owned capability; never touches physical serial."""
import argparse,json,time,uuid
from pathlib import Path
import rclpy
from std_msgs.msg import String
from ros_sim_transport import qos
from ros_sim_contract import sign_request,verify_packet,validate_contract,watchdog_confirmed

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--config',type=Path,required=True);p.add_argument('--token',type=Path,required=True);p.add_argument('--report',type=Path,required=True);a=p.parse_args()
    c=validate_contract(json.loads(a.config.read_text()));token=a.token.read_text().strip();rclpy.init();node=rclpy.create_node('fault_verifier',namespace=c['namespace'])
    pub=node.create_publisher(String,c['topics']['command']['name'],qos(c));replies={};client=uuid.uuid4().hex;seq=0
    def response(msg):
        try:
            reply=verify_packet(json.loads(msg.data),c,token);replies[reply['id']]=reply
        except ValueError:pass
    sub=node.create_subscription(String,c['topics']['observation']['name'],response,qos(c))
    def spin(seconds):
        end=time.monotonic()+seconds
        while time.monotonic()<end:rclpy.spin_once(node,timeout_sec=.01)
    def request(op,*,sequence=None,epoch=0,ttl=2,secret=None,wait=True,**values):
        nonlocal seq
        seq+=1;req={'id':client+':'+str(seq),'client':client,'sequence':seq if sequence is None else sequence,'generation':epoch,'deadline_ns':time.monotonic_ns()+int(ttl*1e9),'calibration_hash':c['calibration_hash'],'operation':op,**values}
        msg=String();msg.data=json.dumps(sign_request(req,c,secret or token));pub.publish(msg)
        until=time.monotonic()+(3 if wait else .3)
        while time.monotonic()<until and req['id'] not in replies:rclpy.spin_once(node,timeout_sec=.02)
        return replies.get(req['id'])
    try:
        spin(1)
        result={}
        assert 'error' not in request('connect')
        state=request('sample');assert 'positions' in state
        result['bad_signature_rejected']=request('send',secret='invalid',wait=False,positions=state['positions']) is None
        result['expired_rejected']=request('send',ttl=-1,wait=False,positions=state['positions']) is None
        result['old_sequence_rejected']='error' in request('send',sequence=1,positions=state['positions'])
        assert 'error' not in request('hold',epoch=1)
        result['old_epoch_rejected']='error' in request('send',epoch=0,positions=state['positions'])
        # Withhold the next lease heartbeat; the simulator must hold and release ownership.
        after=time.monotonic_ns();until=time.monotonic()+6;result['lease_loss_holds']=False
        while time.monotonic()<until:
            spin(.1)
            try:
                watchdog=json.loads((a.root/'watchdog.json').read_text());sim=json.loads((a.root/'sim-status.json').read_text())
                if watchdog_confirmed(watchdog,sim,after):result['lease_loss_holds']=True;break
            except (FileNotFoundError,json.JSONDecodeError):pass
        assert 'error' not in request('connect',epoch=2)
        assert 'error' not in request('close',epoch=3)
        result['passed']=all(result.values());a.report.write_text(json.dumps(result,indent=2));assert result['passed'],result
    finally:node.destroy_node();rclpy.shutdown()
if __name__=='__main__':main()
