"""Operator-guided software calibration revisions; never writes EEPROM."""
import json,time,uuid,threading
from pathlib import Path
from .store import Fault,digest
from .datasets import atomic

def serialized(method):
    from functools import wraps
    @wraps(method)
    def call(self,key,*args,**kwargs):
        with self.locks_guard:lock=self.locks.setdefault(key,threading.RLock())
        with lock:return method(self,key,*args,**kwargs)
    return call

class Calibrations:
    def __init__(self,store,factory,root):
        self.store=store;self.factory=factory;self.root=Path(root)/'calibration-work';self.root.mkdir(parents=True,exist_ok=True);self.readers={};self.locks={};self.locks_guard=threading.Lock()
    def start(self,robot,role,a,project):
        if role not in ('leader','follower'):raise Fault('INVALID_ROLE','leader/follower required')
        key=uuid.uuid4().hex;reader=self.factory(robot);self.readers[key]=reader
        try:
            sample=reader.read()
            if not sample.get('fresh') or sample.get('device_profile')!=robot.get('device_profile'):raise Fault('PROFILE_MISMATCH','Fresh registered device identity required')
        except BaseException:
            self._close(key);raise
        value={'calibration_id':key,'robot_id':robot['robot_id'],'robot_revision':robot['revision'],'device_profile':sample['device_profile'],'role':role,'points':{},'state':'WAITING_SAMPLES','motor_writes':False}
        self.store.register('calibration_work',key,'v1',project,a,{'robot_id':robot['robot_id']});atomic(self.root/(key+'.json'),value);self.readers[key]=reader;return value
    @serialized
    def status(self,key,a):
        self.store.get('calibration_work',key,'v1',a);return json.loads((self.root/(key+'.json')).read_text(encoding='utf-8'))
    @serialized
    def sample(self,key,point,a,motor_id=None):
        value=self.status(key,a)
        if value['state']!='WAITING_SAMPLES' or key not in self.readers:raise Fault('RESTART_REQUIRED','Calibration interrupted/finished; start a new revision')
        if point not in ('minimum','center','maximum'):raise Fault('INVALID_POINT','minimum/center/maximum required')
        sample=self.readers[key].read()
        if not sample.get('fresh') or sample.get('device_profile')!=value['device_profile']:raise Fault('PROFILE_MISMATCH','Changed/stale device cannot contribute samples')
        motors=[motor_id] if motor_id is not None else list(range(1,7))
        for motor in motors:
            if type(motor)!=int or not 1<=motor<=6:raise Fault('INVALID_MOTOR','Motor must be 1..6')
            raw=sample['raw_positions'][str(motor)]
            if type(raw)!=int or not 0<=raw<=4095:raise Fault('INVALID_ENCODER','0..4095 measured encoder required')
            if point in value['points'].get(str(motor),{}):raise Fault('POINT_EXISTS','Reference point immutable; restart to replace')
        for motor in motors:value['points'].setdefault(str(motor),{})[point]={'encoder':sample['raw_positions'][str(motor)],'recorded_unix':time.time()}
        atomic(self.root/(key+'.json'),value);return value
    @serialized
    def finish(self,key,a):
        value=self.status(key,a);entry=self.store.get('calibration_work',key,'v1',a)
        if value['state']=='SUCCEEDED':return self.store.get('calibrations',key,'v1',a)
        axes={}
        for motor in range(1,7):
            pts=value['points'].get(str(motor),{})
            if not all(k in pts for k in ('minimum','maximum')) or motor!=6 and 'center' not in pts:raise Fault('INCOMPLETE','Missing reference points')
            lo,hi=pts['minimum']['encoder'],pts['maximum']['encoder'];mid=pts.get('center',{}).get('encoder',(lo+hi)//2)
            if not lo+20<mid<hi-20:raise Fault('INVALID_RANGE','Center must be inside two separated ordered endpoints')
            axes[str(motor)]={'minimum':lo,'center':mid,'maximum':hi}
        body={**value,'state':'SUCCEEDED','axes':axes,'physical_accuracy_verified':False,'activation':'explicit new robot revision and ROS peer mapping required'}
        body['calibration_hash']=digest(body)
        self._close(key)
        result=self.store.register('calibrations',key,'v1',entry['project'],a,body);value['state']='SUCCEEDED';atomic(self.root/(key+'.json'),value);return result
    @serialized
    def cancel(self,key,a):
        value=self.status(key,a);self._close(key);value['state']='CANCELLED';atomic(self.root/(key+'.json'),value);return value
    @serialized
    def _close(self,key):
        reader=self.readers.get(key)
        if reader and hasattr(reader,'close'):reader.close()
        self.readers.pop(key,None)

class RosCalibrationReader:
    def __init__(self,robot):
        from .ros_transport import TopicRPC
        from .ros_wire import request
        self.rpc=TopicRPC(robot['ros']);self.owner=uuid.uuid4().hex;self.robot_id=robot['robot_id'];self.closed=False
        inspected=self.rpc.call(request('inspect',{},self.owner,0,self.rpc.key),5)
        if inspected['robot_id']!=robot['robot_id']:self.rpc.close();raise Fault('WRONG_TARGET','Calibration ROS target differs')
        self.epoch=inspected['epoch']
        self.rpc.call(request('calibration.connect',{},self.owner,self.epoch,self.rpc.key),5)
        self.lock=threading.Lock();self.stopped=threading.Event();self.error=None
        def heartbeat():
            while not self.stopped.wait(.5):
                try:
                    with self.lock:self.rpc.call(request('heartbeat',{},self.owner,self.epoch,self.rpc.key),1)
                except Exception as exc:self.error=str(exc);self.stopped.set()
        self.thread=threading.Thread(target=heartbeat,daemon=True);self.thread.start()
    def read(self):
        from .ros_wire import request
        if self.error:raise Fault('CALIBRATION_LEASE_LOST',self.error)
        with self.lock:return self.rpc.call(request('calibration.read',{},self.owner,self.epoch,self.rpc.key),5)
    def close(self):
        from .ros_wire import request
        if self.closed:return
        self.stopped.set();self.thread.join(2)
        try:
            with self.lock:self.rpc.call(request('close',{},self.owner,self.epoch,self.rpc.key),5)
        except Exception:
            with self.lock:value=self.rpc.call(request('inspect',{},self.owner,0,self.rpc.key),5)
            if value.get('robot_id')!=self.robot_id or value.get('owner') or value.get('fault') or not value.get('read_only_released'):raise Fault('STOP_UNCONFIRMED','Keep calibration binding for explicit stop retry')
        self.rpc.close();self.closed=True
