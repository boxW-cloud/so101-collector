"""Low-rate physical adapter to the existing commissioned owner, never COM.

Run beside the Windows owner with ROS2 installed, or expose it through a
separately deployed authenticated transport preserving the clock contract.
All hardware settings and consent remain owned by the existing controller.
"""
import base64,json,math,time,uuid
from io import BytesIO
from pathlib import Path
from .store import Fault

class NativeOwnerAdapter:
    kind='physical';source='physical_owner'
    def __init__(self,config,owner=None):
        from follower_ros.native_backend import NativeClient
        self.config=config;self.owner=owner or NativeClient();self.session=None;self.operation=None;self.sent=None;self.leader_reader=None;self.model=None;self.teleop_source=None
    def describe(self):
        ready=self.config.get('commissioned') is True and self.config.get('model_mapping_verified') is True
        return {'kind':'physical','source':self.source,'robot_id':self.config['robot_id'],'calibration_hash':self.config['calibration_hash'],
                'commissioned':ready,'capabilities':['calibration']+(['collection','teleop','task'] if ready else []),
                'teleop_start_configured':bool(self.config.get('teleop_source')),
                'limitations':['single-joint bounded owner operations; measured collection FPS must be accepted','no automatic torque release or fault reset','no full pose6d/m/s claim']}
    def state(self,motion=False):
        s=self.owner.call('state',{})
        device=s.get('device_id')
        if device is None and self.config.get('identity_receipt'):
            receipt=json.loads(Path(self.config['identity_receipt']).read_text())
            if receipt.get('session_id')==s.get('session_id'):device=receipt.get('device_id')
        if s.get('mode')!='real' or not s.get('connected') or device!=self.config['device_id'] or s.get('profile_id')!=self.config['expected_profile_id']:raise Fault('DEVICE_MISMATCH','Registered physical identity is not connected')
        if self.session and s['session_id']!=self.session:raise Fault('SESSION_CHANGED','Explicit rebind required after controller restart')
        if not 0<=time.time()-s['sampled_at']<1.5:raise Fault('STALE_FEEDBACK','Fresh controller feedback required')
        if s.get('fault'):raise Fault('FAULT_LATCHED',str(s['fault']))
        if motion and (not self.describe()['commissioned'] or s.get('mapping_id')!=self.config.get('mapping_id') or not s.get('consent_active')):raise Fault('NOT_COMMISSIONED','Verified mapping and local workspace consent required')
        return s
    def connect(self):
        s=self.state()
        if (s.get('operation') or {}).get('status')=='running':raise Fault('BUSY','Native owner already has an active operation')
        self.session=s['session_id'];self.model=self.owner.call('model',{})
        if self.model['mapping_id']!=s['mapping_id']:raise Fault('MAPPING_MISMATCH','Owner model changed')
        self.sent=self.goals(s)
    def queue_status(self):
        """Read the actual owner without claiming it or inferring torque."""
        state=self.state()
        if 'operation' not in state:raise Fault('QUEUE_STATUS_UNAVAILABLE','Native owner operation field is missing; queue state is unknown')
        operation=state['operation']
        if operation is None:
            empty=True
        elif not isinstance(operation,dict):
            raise Fault('QUEUE_STATUS_UNAVAILABLE','Native owner returned an unknown action status')
        else:
            status=operation.get('status')
            if status in ('running','queued','cancelling','stopping'):empty=False
            elif status in ('reached','settled_short','cancelled','failed','stopped'):empty=True
            else:raise Fault('QUEUE_STATUS_UNAVAILABLE','Native operation status is not understood')
        return {'action_queue_empty':empty,'queue_scope':'native_owner','sampled_at':state['sampled_at'],
                'session_id':state['session_id'],'device_id':self.config['device_id'],
                'profile_id':state['profile_id'],'external_operation_id':operation.get('id') if operation else None}
    def positions(self,s):
        out={n+'.pos':math.degrees(s['model_angles'][n]) for n in ('shoulder_pan','shoulder_lift','elbow_flex','wrist_flex','wrist_roll')}
        jaw=next(x for x in s['joints'] if x['id']==6);lo,hi=self.config['gripper_ticks']
        if hi<=lo:raise Fault('CALIBRATION_MISMATCH','Explicit closed/open encoder bounds required')
        out['gripper.pos']=(jaw['position']-lo)/(hi-lo)*100
        if not -.1<=out['gripper.pos']<=100.1:raise Fault('LIMIT','Jaw outside calibrated range')
        return out
    def goals(self,s):
        result={};lo,hi=self.config['gripper_ticks']
        for row in s['joints']:
            if 'goal' not in row:raise Fault('RECEIPT_MISSING','Actual hardware goal feedback is required; enqueue is not sent')
            if row['id']==6:result['gripper.pos']=(row['goal']-lo)/(hi-lo)*100
            else:
                m=self.model['mapping'][row['name']];result[row['name']+'.pos']=math.degrees((row['goal']-m['zero_ticks'])/m['ticks_per_radian'])
        return result
    def sample(self):
        from PIL import Image
        start=time.monotonic_ns();s=self.state();stamp=start-int(max(0,time.time()-s['sampled_at'])*1e9);images={}
        for name,camera in self.config['cameras'].items():
            before=time.monotonic_ns();frame=self.owner.call('capture',{'camera_id':camera})
            if frame.get('simulated') or not 0<=time.time()-frame['captured_at']<1.5:raise Fault('STALE_CAMERA','Real fresh camera capture required')
            age=max(0,time.time()-frame['captured_at']);captured=time.monotonic_ns()-int(age*1e9)
            with Image.open(BytesIO(base64.b64decode(frame['jpeg_base64'],validate=True))) as image:
                stream=BytesIO();image.convert('RGB').save(stream,format='PNG')
            images[name]={'png':base64.b64encode(stream.getvalue()).decode(),'captured_monotonic_ns':captured}
        return {'positions':self.positions(s),'captured_monotonic_ns':stamp,'images':images,'clock_mapping':'same-host wall-to-monotonic; require bounded sensor skew','device_profile':s['profile_id']}
    def send(self,target):
        s=self.state(motion=True);current=self.positions(s)
        if set(target)!=set(current) or any(type(v) not in (int,float) or not math.isfinite(v) for v in target.values()):raise Fault('INVALID_TARGET','Six named finite positions required')
        for name,value in target.items():
            lo,hi=self.config['limits'][name]
            if not lo<=value<=hi:raise Fault('LIMIT','Target outside commissioned limits')
        if self.operation:
            op=self.owner.call('operation',{'session_id':self.session,'operation_id':self.operation['id']})
            if (s.get('operation') or {}).get('id')!=self.operation['id']:raise Fault('CONTROL_CHANGED','Native operation replaced by another client')
            if op['status']=='running':self.sent=self.goals(s);return dict(self.sent)
            if op['status']!='reached' or not op.get('hold_confirmed'):self.hold();raise Fault('TARGET_NOT_REACHED','Physical target incomplete; no larger retry')
            self.operation=None
        elif (s.get('operation') or {}).get('status')=='running':raise Fault('BUSY','Native controller owned elsewhere')
        # The existing hardware owner permits one bounded model joint at a time.
        name=max(target,key=lambda n:abs(target[n]-current[n]));delta=target[name]-current[name]
        if abs(delta)<self.config.get('deadband',.25):return dict(self.sent)
        args={'session_id':self.session,'revision':s['revision'],'idempotency_key':uuid.uuid4().hex}
        try:
            if name=='gripper.pos':
                lo,hi=self.config['gripper_ticks'];ticks=round(delta*(hi-lo)/100);ticks=max(-100,min(100,ticks))
                if abs(ticks)<5:return dict(self.sent)
                self.operation=self.owner.call('gripper_move',{**args,'delta_ticks':ticks});self.sent[name]=current[name]+ticks/(hi-lo)*100
            else:
                # <=1 degree stays below the known owner's 40 encoder-tick cap;
                # the owner independently enforces current limits and mapping.
                target_angle=current[name]+max(-1,min(1,delta))
                self.operation=self.owner.call('move_model',{**args,'mapping_id':s['mapping_id'],'angles':{name.removesuffix('.pos'):math.radians(target_angle)},'speed':100})
                self.sent[name]=target_angle
        except BaseException:
            # HTTP failure can occur after native acceptance; preserve stop responsibility.
            self.hold();raise
        self.sent=self.goals(self.state(motion=True));return dict(self.sent)
    def hold(self):
        if not self.session:raise Fault('NOT_BOUND','No adopted owner session')
        value=self.owner.call('stop',{'session_id':self.session})
        if not value.get('hold_confirmed'):raise Fault('STOP_UNCONFIRMED','Native owner has not confirmed holding')
        self.operation=None
        return {'holding':True,'action_queue_empty':True,'native_result':value}
    def reset(self):raise Fault('WRONG_TARGET','Scene reset cannot move physical robot')
    def calibration(self,op,p):
        if op!='calibration.read':raise Fault('UNSUPPORTED','Only read-only reference sampling is exposed')
        s=self.state();return {'raw_positions':{str(j['id']):int(j['position']) for j in s['joints']},'device_profile':s['profile_id'],'fresh':True}
    def leader(self):
        from integration.leader_collection_input import BorrowedLeader
        from lerobot_sdk import RobotConfig
        if self.leader_reader is None:
            cfg=RobotConfig(**self.config['sdk_config']);profile=json.loads(Path(self.config['leader_profile']).read_text())
            self.leader_reader=BorrowedLeader(self.config['leader_input'],profile,cfg,self.positions(self.state()))
        return {'positions':dict(self.leader_reader.get_action(timeout_s=.2).positions)}
    def start_teleop(self,timeout_s):
        if not self.config.get('teleop_source'):raise Fault('TELEOP_UNAVAILABLE','Automatic leader source is not configured')
        if not self.session:raise Fault('NOT_BOUND','Connect the registered target before starting its input source')
        self.state()  # Identity/session/fault checks; no torque or motion writes.
        if self.teleop_source is None:
            from .managed_source import ManagedTeleopSource
            profile=json.loads(Path(self.config['leader_profile']).read_text())
            self.teleop_source=ManagedTeleopSource(self.config['teleop_source'],input_root=self.config['leader_input'],profile_hash=profile['profile_hash'])
        receipt=self.teleop_source.start(timeout_s)
        self.leader_reader=None  # Anchor a new managed input to this actual session.
        return receipt
    def stop_teleop(self,timeout_s):
        if self.teleop_source is None:return {'stopped':True,'managed':True,'process_started':False}
        return self.teleop_source.stop(timeout_s)
    def primitive(self,p):raise Fault('UNSUPPORTED','Use the separately commissioned §17.1 action bridge, not a second simultaneous controller')
