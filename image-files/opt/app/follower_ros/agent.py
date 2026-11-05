"""Windows loopback action agent. Talks only to existing arm controller, never COM."""
import argparse,hashlib,json,threading,time,uuid
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from integration.action_primitives import ActionWrapper,ActionContext,action_catalog
from .native_backend import NativeBackend,NativeClient
from .protocol import Error,verify,sign,canonical,ROBOT

class Agent:
    def __init__(self,config,key,native=None):
        self.config,self.key=config,key;self.native=native or NativeClient(config.get('controller_url','http://127.0.0.1:8765'));self.lock=threading.RLock();self.issue_lock=threading.Lock();self.cancel=threading.Event()
        self.path=Path(config['journal']);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.data=json.loads(self.path.read_text(encoding='utf-8')) if self.path.exists() else {'epoch':0,'requests':{},'jobs':{}}
        self.data['epoch']+=1;self.epoch=self.data['epoch'];self.bound_session=None;self.lease_until=0;self.active=None;self.closed=False;self.workspace_confirmed=False
        for job in self.data['jobs'].values():
            if job['state'] in ('QUEUED','RUNNING'):job.update(state='INTERRUPTED',success=False,error_code='AGENT_RESTART',message='Not replayed; inspect native controller before rebinding')
        self.pool=ThreadPoolExecutor(1);self.backend=NativeBackend(self);self.wrapper=ActionWrapper(self.backend,part_ids_by_side={'right':(2,)},cache_capacity=10000);self.persist()
        threading.Thread(target=self.watchdog,daemon=True).start()
    def persist(self):
        with self.lock:
            tmp=self.path.with_suffix('.tmp');tmp.write_text(json.dumps(self.data,ensure_ascii=False,indent=2),encoding='utf-8');tmp.replace(self.path)
    def check_identity(self,state,*,check_session=True):
        if state.get('mode')!='real' or not state.get('connected'):raise Error('NOT_CONNECTED','Real controller is not connected')
        if check_session and self.bound_session and state['session_id']!=self.bound_session:raise Error('SESSION_CHANGED','Controller session changed; rebind explicitly')
        if state.get('profile_id')!=self.config['expected_profile_id']:raise Error('DEVICE_MISMATCH','Registered COM15 hardware fingerprint differs')
        if state.get('device_id') and state['device_id']!=self.config['device_id']:raise Error('DEVICE_MISMATCH','Controller is connected to another port/device')
        if self.config.get('handshake_file'):
            handshake=json.loads(Path(self.config['handshake_file']).read_text(encoding='utf-8'))
            if handshake.get('device_id')!=self.config['device_id']:raise Error('DEVICE_MISMATCH','Native owner handshake is not COM15')
    def check_lease(self,epoch):
        if not self.bound_session or type(epoch) is not int or epoch!=self.epoch:raise Error('STALE_CONTROL','Bind this physical arm and use the current epoch')
        if time.monotonic()>=self.lease_until:raise Error('LEASE_EXPIRED','Send authenticated heartbeat every2 seconds while controlling')
    def watchdog(self):
        while not self.closed:
            if self.active and time.monotonic()>=self.lease_until:
                self.cancel.set()
                try:
                    with self.issue_lock:self.native.call('stop',{'session_id':self.bound_session})
                except Exception:pass  # Never fabricate holding; execution result reports uncertainty.
            time.sleep(.2)
    def state(self):
        try:
            value=self.native.call('state',{});age=time.time()-value.get('sampled_at',0)
            native={'connected':value.get('connected'),'session_id':value.get('session_id'),'revision':value.get('revision'),'phase':value.get('phase'),
                'profile_id':value.get('profile_id'),'fault':value.get('fault'),'feedback_age_s':age,'feedback_fresh':0<=age<1.5,
                'consent_active':value.get('consent_active'),'joints':value.get('joints'),'native_control_available':value.get('native_control_available')}
        except Exception as exc:native={'connected':False,'error':str(exc),'feedback_fresh':False}
        with self.lock:return {'robot_id':ROBOT,'device_id':self.config['device_id'],'bound':bool(self.bound_session),'epoch':self.epoch,'lease_remaining_s':max(0,self.lease_until-time.monotonic()),'workspace_confirmed_for_ros':self.workspace_confirmed,'active_request_id':self.active,'native':native}
    def catalog(self):
        return {'robot_id':ROBOT,'device_id':self.config['device_id'],'part_ids_by_side':{'right':[2]},'actions':action_catalog(),
            'executors':{'GRASP':'bounded_native_gripper','LOOSEN':'bounded_native_gripper','ARM_MOVE':'bounded_relative_xyz','ARM_MOVETO':'unsupported_full_pose6d_and_linear_speed',
            'ARM_RELEASE':'configured_park_pose','HOLD_ARM':'configured_chest_pose','HOLD_GRASP':'close_gripper_then_chest_pose'},
            'stop_preserves_torque':True,'automatic_fault_reset':False,'mode':'physical_follower_only'}
    def dispatch(self,packet):
        req=verify(packet,self.key);rid=req['request_id'];op=req['operation'];p=req['payload']
        digest=hashlib.sha256(canonical({k:req[k] for k in ('robot_id','principal','operation','payload')})).hexdigest()
        if op in ('inspect','catalog'):
            if p:raise Error('INVALID_FIELDS','Empty payload required')
            return self.state() if op=='inspect' else self.catalog()
        if op=='stop':
            with self.lock:
                if set(p)!={'epoch'} or not self.bound_session or p['epoch']!=self.epoch:raise Error('STALE_CONTROL','Stop requires the bound physical session epoch')
                self.cancel.set();sid=self.bound_session
            with self.issue_lock:result=self.native.call('stop',{'session_id':sid})
            with self.lock:
                if result.get('hold_confirmed') and self.active and self.data['jobs'][self.active]['state']=='STOP_UNCONFIRMED':
                    self.data['jobs'][self.active].update(state='FAILED',stop=result);self.active=None;self.persist()
            return {'state':'STOPPED' if result.get('hold_confirmed') else 'STOP_UNCONFIRMED',**result}
        with self.lock:
            previous=self.data['requests'].get(rid)
            if previous:
                if previous['digest']!=digest:raise Error('IDEMPOTENCY_CONFLICT','Same request_id with different content')
                return self.data['jobs'].get(rid,previous['result'])
            if op in ('inspect','catalog'):
                if p:raise Error('INVALID_FIELDS','Empty payload required')
                return self.state() if op=='inspect' else self.catalog()
            if op=='bind':
                if set(p)!={'device_id','controller_session_id','adopt_existing_session'} or p['device_id']!=self.config['device_id']:raise Error('INVALID_BIND','Exact registered COM15 device and inspected controller session required')
                if self.active:raise Error('BUSY','An action is still active')
                state=self.native.call('state',{});self.check_identity(state,check_session=False)
                if state['session_id']!=p['controller_session_id'] or p['adopt_existing_session'] is not True:raise Error('BIND_CONFIRMATION_REQUIRED','Explicit adoption of the inspected session is required')
                self.epoch+=1;self.data['epoch']=self.epoch;self.bound_session=state['session_id'];self.lease_until=time.monotonic()+120;self.workspace_confirmed=False
                result={'bound':True,'epoch':self.epoch,'session_id':self.bound_session,'device_id':p['device_id'],'motion_started':False,'fault_retained':state.get('fault'),
                    'binding_method':'operator_confirmed_device_and_pinned_profile_session'}
            elif op=='heartbeat':
                if set(p)!={'epoch'}:raise Error('INVALID_FIELDS','epoch only')
                self.check_lease(p['epoch']);self.lease_until=time.monotonic()+(5 if self.active else 120);return {'epoch':self.epoch,'lease_seconds':5 if self.active else 120}
            elif op=='confirm_workspace':
                if set(p)!={'epoch','secured','clear'} or p['secured'] is not True or p['clear'] is not True:raise Error('WORKSPACE_NOT_CONFIRMED','Operator must confirm secured base and clear workspace')
                self.check_lease(p['epoch'])
                if self.active:raise Error('BUSY','Workspace confirmation is only accepted while idle')
                # Do not use confirmation to clear any pre-existing hardware fault.
                state=self.native.call('state',{});self.check_identity(state)
                if state.get('fault'):raise Error('FAULT_LATCHED','Resolve the fault in the native controller; this bridge never resets it')
                self.native.call('confirm_workspace',{'session_id':self.bound_session,'secured':True,'clear':True});self.workspace_confirmed=True;result={'confirmed':True,'epoch':self.epoch,'motion_started':False}
            elif op=='stop':
                if set(p)!={'epoch'}:raise Error('INVALID_FIELDS','epoch only')
                if not self.bound_session or p['epoch']!=self.epoch:raise Error('STALE_CONTROL','Stop requires the bound physical session epoch')
                self.cancel.set();result=self.native.call('stop',{'session_id':self.bound_session});result={'state':'STOPPED' if result.get('hold_confirmed') else 'STOP_UNCONFIRMED',**result}
            elif op=='release_control':
                if set(p)!={'epoch'}:raise Error('INVALID_FIELDS','epoch only')
                self.check_lease(p['epoch'])
                if self.active:raise Error('BUSY','Stop first; never release while action is running')
                self.bound_session=None;self.workspace_confirmed=False;self.lease_until=0;self.epoch+=1;self.data['epoch']=self.epoch;result={'released':True,'torque_changed':False}
            elif op=='execute':
                required={'epoch','action_id','actionType','actionCategory','part_id','parameters'}
                if set(p)!=required or p['actionType']!='ExecutionAction' or p['actionCategory']!='BuildinAction':raise Error('INVALID_ACTION','Exact chapter17.1 action fields required')
                if p['action_id'] not in action_catalog():raise Error('UNKNOWN_ACTION','Unknown primitive')
                self.check_lease(p['epoch'])
                if self.active:raise Error('BUSY','Physical actions are not queued behind each other')
                self.cancel.clear();self.active=rid;self.lease_until=time.monotonic()+5
                result={'request_id':rid,'action_id':p['action_id'],'state':'QUEUED','success':False,'accepted_at':time.time()}
                self.data['jobs'][rid]=result
                self.data['requests'][rid]={'digest':digest,'result':result};self.persist();self.pool.submit(self.execute,req);return dict(result)
            else:raise Error('UNSUPPORTED_OPERATION','Use catalog, inspect, bind, heartbeat, confirm_workspace, execute, stop or release_control')
            self.data['requests'][rid]={'digest':digest,'result':result};self.persist();return result
    def execute(self,req):
        rid=req['request_id'];p=req['payload'];job=self.data['jobs'][rid]
        self.backend.last_device_result=None
        self.backend.possibly_issued=False
        with self.lock:job['state']='RUNNING';self.persist()
        try:
            context=ActionContext(ROBOT,p['epoch'],'agent-internal-authority',rid)
            result=self.wrapper.execute(p['action_id'],p['parameters'],context=context,part_id=p['part_id']).to_dict()
            if not result['success'] and self.backend.possibly_issued:
                try:
                    with self.issue_lock:result['stop']=self.native.call('stop',{'session_id':self.bound_session})
                except Exception as exc:result['stop']={'hold_confirmed':False,'error':str(exc)}
                if not result['stop'].get('hold_confirmed'):result['state']='STOP_UNCONFIRMED'
            result['device_result']=self.backend.last_device_result;result['object_grasp_verified']=False
        except Exception as exc:result={'state':'FAILED','success':False,'error_code':getattr(exc,'code','AGENT_ERROR'),'message':str(exc)}
        with self.lock:
            job.update(result,finished_at=time.time())
            if result['state']!='STOP_UNCONFIRMED':self.active=None
            if time.monotonic()<self.lease_until:self.lease_until=time.monotonic()+120
            self.persist()

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',type=Path,required=True);a=p.parse_args();cfg=json.loads(a.config.read_text(encoding='utf-8'));key=Path(cfg['key_file']).read_text().strip();agent=Agent(cfg,key)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*a):pass
        def reply(self,status,value):
            raw=json.dumps(sign(value,key),ensure_ascii=False).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
        def do_POST(self):
            try:
                if self.path!='/rpc' or self.headers.get('Transfer-Encoding'):raise Error('INVALID_ROUTE','Use /rpc')
                n=int(self.headers.get('Content-Length',0))
                if not 0<n<=32768:raise Error('INVALID_SIZE','Maximum32KiB')
                self.connection.settimeout(3);packet=json.loads(self.rfile.read(n));req=verify(packet,key)
                if req['operation']=='job':
                    rid=req['payload'].get('request_id');result=agent.data['jobs'].get(rid)
                    if result is None:raise Error('NOT_FOUND','No operation with this ID')
                else:result=agent.dispatch(packet)
                self.reply(200,{'request_id':req['request_id'],'ok':True,'result':result})
            except Exception as exc:self.reply(400,{'ok':False,'error':{'code':getattr(exc,'code','INVALID_REQUEST'),'message':str(exc)}})
    server=ThreadingHTTPServer(('127.0.0.1',cfg.get('port',8871)),Handler)
    try:server.serve_forever()
    finally:agent.closed=True;agent.cancel.set();server.server_close()
if __name__=='__main__':main()
