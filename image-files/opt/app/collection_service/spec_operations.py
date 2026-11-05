"""Section17 verification/version boundary; business work delegates to SDK."""
import copy,json,time,uuid,threading,weakref
from pathlib import Path
from dataclasses import replace
from .protocol import ApiError
from .contracts import binding,assert_binding,public_ros,match_ros,digest
from .verification import VerificationSession
from .calibration import CalibrationOperations
from .conversion import ConversionOperations

class SpecOperations(CalibrationOperations,ConversionOperations):
    def initialize_spec(self):
        self.verifying={}
        self.startup_events={}
        self.startup_cleanups=weakref.WeakKeyDictionary()
        self.state.setdefault('verifications',{})
        for value in self.state['verifications'].values():
            if value['state'] not in ('STOPPED','FAILED'):value.update(state='INTERRUPTED',error='Restarted; explicit new verification required')
        self.strict=self.config.get('schema')=='nexusedge.collection.v2'
        self.initialize_calibration()

    def registered_robot(self,robot_id):
        for key in ('robot','leader_robot'):
            value=self.config.get(key,{})
            if value.get('robot_id')==robot_id:return value
        raise ApiError('CONFIG_MISMATCH','Robot is not registered')

    def busy(self):
        return bool(self.running or self.verifying or any(not event.is_set() for event in self.startup_events.values())
            or getattr(self,'calibration_active',None) or any(s.get('pending') for s in self.state['sessions'].values()))

    def freeze_binding(self,source,revision='baseline'):
        value=binding(self.config,source,revision)
        if revision!='baseline':
            record=self.calibration_record(revision)
            if record['source']!=source:raise ApiError('CALIBRATION_MISMATCH','Revision belongs to another source')
            if record['device_config_revision']!=self.config.get('leader_robot',{}).get('device_config_revision'):raise ApiError('CALIBRATION_MISMATCH','Revision belongs to another device configuration')
            value['selected_profile']=copy.deepcopy(record['profile']);value['selected_calibration_hash']=record['content_hash']
        elif 'input_profile' in value:value['selected_profile']=copy.deepcopy(value['input_profile'])
        value['binding_hash']=digest({k:v for k,v in value.items() if k!='binding_hash'})
        return value

    def connection_status(self):
        root=Path(self.config['ipc_root']);result={}
        for filename,key in (('ready.json','ros_status'),('timestamp-status.json','timestamp_status')):
            try:result[key]=json.loads((root/filename).read_text())
            except (OSError,ValueError):result[key]={'validated':False,'reason':'Not yet available'}
        stamp=result['timestamp_status'].get('captured_monotonic_ns')
        if stamp is not None:
            age=(time.monotonic_ns()-stamp)/1e9
            result['timestamp_status'].update(current_age_s=age,fresh=0<=age<2)
        return result

    def check_session(self,session,claim,*,recover=False):
        if session.get('robot_id',self.config['robot']['robot_id'])!=claim['robot_id']:raise ApiError('WRONG_TARGET','Session cannot borrow another target claim')
        if session.get('binding'):
            assert_binding(session['binding'],self.freeze_binding(session['source'],session['binding']['calibration_revision']))
        elif self.strict:raise ApiError('SESSION_REBIND_REQUIRED','Old session has no frozen binding')
        if not recover and session.get('epoch')!=claim['epoch']:raise ApiError('STALE_SESSION','Session belongs to a previous claim epoch')
        if recover:session['epoch']=claim['epoch']  # Explicit restore only, after exact binding match.

    def make_leader(self,source_name,config,base,frozen):
        from integration.leader_collection_input import BorrowedLeader
        source=self.config['sources'][source_name]
        if source['kind']=='scripted_test':
            from lerobot_sdk.testing.leader import TrajectoryLeader
            from examples.isaac_collect import build_patterns
            leader=TrajectoryLeader(config,base,patterns=build_patterns(2));leader.begin_episode(0,base=base);return leader
        if source['kind']=='native_topic':
            profile=frozen.get('selected_profile') or json.loads(Path(source['profile']).read_text())
            return BorrowedLeader(source['root'],profile,config,base,input_profile_hash=frozen.get('input_profile',profile)['profile_hash'])
        raise ApiError('UNSUPPORTED_SOURCE','Leader adapter not installed')

    def handle_verification(self,op,p,principal):
        if op=='simulation.verify.start':
            self.fields(p,['robot_id','project','config_revision','epoch','control_token','scene','ros','source','control_parameters','calibration_revision','device_config_revision'])
            claim=self.context(p,principal);self.permission(principal,p['project'])
            if p['robot_id']!=self.config['robot']['robot_id']:raise ApiError('WRONG_TARGET','Simulation must use its own target claim')
            if p['project']!=claim['project']:raise ApiError('FORBIDDEN','Project differs from claim')
            if self.busy():raise ApiError('BUSY','Calibration, verification or collection already owns the target')
            if self.config['robot'].get('kind','simulation')!='simulation':raise ApiError('WRONG_TARGET','Verification requires an independently registered simulation target')
            if p['scene']!=self.config['robot']['scene']:raise ApiError('CONFIG_MISMATCH','Scene differs')
            match_ros(p['ros'],self.config['ros'])
            if p['source'] not in self.config['sources']:raise ApiError('INVALID_SOURCE','Source is not registered')
            expected=self.config['robot'].get('device_config_revision',self.config['robot']['config_revision'])
            if p['device_config_revision']!=expected:raise ApiError('DEVICE_CONFIG_MISMATCH','Device configuration revision differs')
            controls=p['control_parameters'];self.fields(controls,[],['fps','duration_s','speed_scale','initial_alignment'])
            initial_alignment=controls.get('initial_alignment','require_close')
            if initial_alignment not in ('require_close','explicit_move'):raise ApiError('INVALID_CONTROL','Unsupported initial alignment mode')
            fps=controls.get('fps',5);duration=controls.get('duration_s',600);speed=controls.get('speed_scale',1)
            if type(fps) is not int or not 1<=fps<=10 or type(duration) is not int or not 1<=duration<=1800 or type(speed) not in (int,float) or not 0<speed<=1:raise ApiError('INVALID_CONTROL','Invalid bounded control parameters')
            self.calibration_access(principal,p['project'],p['calibration_revision'])
            frozen=self.freeze_binding(p['source'],p['calibration_revision']);sid=uuid.uuid4().hex
            value={'session_id':sid,'principal':principal,'project':p['project'],'source':p['source'],'epoch':claim['epoch'],
                'state':'STARTING','binding':frozen,'control_parameters':{'fps':fps,'duration_s':duration,'speed_scale':speed,'initial_alignment':initial_alignment},'dataset_id':None,'recording':False}
            self.state['verifications'][sid]=value;self.verifying[sid]=None
            self.startup_events[sid]=threading.Event()
            return self.submit(op,principal,p['project'],lambda:self.start_verification(sid),verification_id=sid,session_id=sid)
        self.fields(p,['session_id'] if op=='simulation.verify.status' else ['session_id','robot_id','config_revision','epoch','control_token'])
        value=self.owner('verifications',p['session_id'],principal);live=self.verifying.get(p['session_id'])
        if op=='simulation.verify.status':return {**value,'feedback_status':live.status() if live else value.get('result'),**self.connection_status()}
        claim=self.context(p,principal)
        if claim['epoch']!=value['epoch']:raise ApiError('STALE_SESSION','Verification epoch differs')
        if not live and value['state']!='STARTING' and not value.get('result'):raise ApiError('NOT_ACTIVE','Verification not active')
        value['stop_requested']=True
        if live:live.stop_event.set()
        return self.submit(op,principal,value['project'],lambda:self.stop_verification(p['session_id']),verification_id=p['session_id'],urgent=True)

    def wait_startup_barrier(self,sid):
        event=self.startup_events.get(sid)
        timeout=min(10,max(.01,float(self.config.get('startup_stop_timeout_s',10))))
        if event is not None and not event.wait(timeout):
            raise ApiError('STOP_UNCONFIRMED','Stop remains latched; initialization has not yet confirmed HOLD/close')

    def close_startup_robot(self,robot):
        if robot is None:raise ApiError('STOP_UNCONFIRMED','Robot connection was not established; HOLD could not be measured')
        with self.lock:record=self.startup_cleanups.setdefault(robot,{'lock':threading.Lock(),'result':None})
        with record['lock']:
            if record['result'] is not None:return dict(record['result'])
            stopped=robot.stop('cancelled or failed startup')
            empty=robot.status().control_owner is None
            robot.close()
            if not stopped.holding or not empty:raise ApiError('STOP_UNCONFIRMED','Startup cleanup did not confirm HOLD and empty ownership')
            record['result']={'holding':True,'action_queue_empty':True,'stop_epoch':stopped.epoch,'closed':True}
            return dict(record['result'])

    def start_verification(self,sid):
        from lerobot_sdk import RobotSession
        from lerobot_sdk.testing.file_sim import FileSimulationBackend
        from examples.isaac_collect import build_robot_config
        value=self.state['verifications'][sid];controls=value['control_parameters'];robot=None;live=None
        event=self.startup_events.setdefault(sid,threading.Event())
        try:
            assert_binding(value['binding'],self.freeze_binding(value['source'],value['binding']['calibration_revision']))
            config=build_robot_config(value['binding']['contract'],{'front':(480,640,3),'wrist':(480,640,3)},controls['fps'])
            config=replace(config,max_velocity={k:v*controls['speed_scale'] for k,v in config.max_velocity.items()})
            backend=FileSimulationBackend(self.config['ipc_root'],calibration_hash=config.calibration_hash)
            robot=RobotSession(config,backend,lock_dir=self.root/'locks');robot.connect()
            with self.lock:
                if value.get('stop_requested'):raise ApiError('START_CANCELLED','Verification startup was stopped before activation')
            leader=self.make_leader(value['source'],config,dict(robot.get_arm_data().positions),value['binding'])
            initial=None;alignment=None
            if self.config['sources'][value['source']]['kind']=='native_topic':
                from .initial_alignment import preflight_alignment
                initial,alignment=preflight_alignment(robot,leader,explicit=controls.get('initial_alignment')=='explicit_move')
            live=VerificationSession(robot,leader,fps=controls['fps'],duration_s=controls['duration_s'],initial_target=initial,alignment=alignment)
            with self.lock:
                if value.get('stop_requested'):raise ApiError('START_CANCELLED','Verification startup was stopped before activation')
                self.verifying[sid]=live;result=live.start();value['state']='RUNNING'
            return {**result,'session_id':sid,'config_status':{'binding_hash':value['binding']['binding_hash'],'applied':True},
                'ros_status':json.loads((Path(self.config['ipc_root'])/'ready.json').read_text()),
                'timestamp_status':json.loads((Path(self.config['ipc_root'])/'timestamp-status.json').read_text()) if (Path(self.config['ipc_root'])/'timestamp-status.json').exists() else {'validated':False,'reason':'waiting for first sample'},'recording':False,'dataset_id':None}
        except Exception as exc:
            cleanup_error=None;result=None
            try:
                if live is not None and hasattr(live,'thread'):
                    result=live.stop();robot.close();result['closed']=True
                elif robot is not None:result=self.close_startup_robot(robot)
            except Exception as close_error:cleanup_error=close_error
            with self.lock:
                self.verifying.pop(sid,None)
                value['state']='STOPPED' if value.get('stop_requested') and result else 'FAILED'
                value['startup_error']={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc)}
                if getattr(exc,'details',None) is not None:value['startup_error']['details']=exc.details
                if result:value['result']={**result,'state':value['state'],'recording':False,'dataset_id':None}
                if cleanup_error:value['stop_error']=str(cleanup_error)
                self.persist()
            if cleanup_error:raise ApiError('STOP_UNCONFIRMED',str(cleanup_error)) from exc
            raise
        finally:event.set()

    def stop_verification(self,sid):
        with self.lock:
            self.state['verifications'][sid]['stop_requested']=True
            pending=self.verifying.get(sid) is None and sid in self.startup_events and not self.startup_events[sid].is_set()
        if pending:self.wait_startup_barrier(sid)
        with self.lock:
            live=self.verifying.get(sid)
            if live is None:
                result=self.state['verifications'][sid].get('result')
                if result is None:raise ApiError('STOP_UNCONFIRMED','Verification startup did not confirm HOLD/close')
                return {**result,'session_id':sid,'recording':False,'dataset_id':None}
            # Auto-completion and explicit stop can race. Only one owner closes
            # the RobotSession; the second path observes the persisted result.
            result=live.stop();live.robot.close()
            self.verifying.pop(sid,None);self.state['verifications'][sid].update(state=result['state'],result=result);self.persist()
            self.emit('simulation.verification.status',self.state['verifications'][sid]['principal'],{'session_id':sid,**result})
        return {**result,'session_id':sid,'recording':False,'dataset_id':None}
