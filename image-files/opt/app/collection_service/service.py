"""Persistent outer API; no business/platform fields are added to SDK types."""
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from dataclasses import asdict
import hashlib,json,secrets,threading,time,uuid
from pathlib import Path
from lerobot_sdk import CollectionManager,CollectionConfig,DatasetManager,RobotSession
from lerobot_sdk.dataset_sinks import LocalDirectorySink,RetryingDatasetSink
from lerobot_sdk.testing.file_sim import FileSimulationBackend
from integration.ros_sim_contract import validate_contract,contract_hash
from integration.leader_collection_input import BorrowedLeader
from examples.isaac_collect import build_robot_config,build_patterns
from .protocol import ApiError,identifier,canonical,request_payload,signed
from .spec_operations import SpecOperations
from .contracts import public_ros,match_ros,assert_binding,episode_result

def encode(value):
    if isinstance(value,Path):return str(value)
    if hasattr(value,'value'):return value.value
    raise TypeError(type(value).__name__)

def attach_provenance(workspace,parameters):
    (workspace/'meta/edge-source.json').write_text(json.dumps(parameters,default=encode,indent=2))
    return {}

class Service(SpecOperations):
    def __init__(self,config,keys):
        self.config=config;self.keys=keys;self.root=Path(config['data_root']);self.root.mkdir(parents=True,exist_ok=True)
        self.lock=threading.RLock();self.running={};self.stopping=False
        self.events=deque(maxlen=256);self.event_generation=uuid.uuid4().hex;self.event_sequence=0;self.last_status={}
        self.control=ThreadPoolExecutor(1,thread_name_prefix='CollectionAPI')
        self.transfer=ThreadPoolExecutor(2,thread_name_prefix='DatasetTransfer')
        self.cm=CollectionManager();self.dm=DatasetManager();self.path=self.root/'service-state.json'
        self.state=json.loads(self.path.read_text()) if self.path.exists() else {'requests':{},'jobs':{},'sessions':{},'datasets':{},'claim':None,'epoch':0}
        for job in self.state['jobs'].values():
            if job['status'] in ('QUEUED','RUNNING'):job.update(status='INTERRUPTED',error='Container restarted; explicit recovery/retry required')
        for session in self.state['sessions'].values():
            if session.get('state') not in ('LOCAL_READY','CLOSED'):session['state']='RECOVERABLE'
            session['pending']=None
        for data in self.state['datasets'].values():
            if data.get('upload_state')=='UPLOADING':data['upload_state']='PENDING_RETRY'
        self.initialize_spec()
        self.state['claim']=None;self.state['epoch']+=1;self.persist()
        # Native package warm-up before acquiring any robot lease.
        from lerobot_sdk.adapters.seeed_dataset import upstream
        upstream()

    def persist(self):
        temp=self.path.with_suffix('.tmp');temp.write_text(json.dumps(self.state,default=encode,indent=2));temp.chmod(0o600);temp.replace(self.path)

    def emit(self,kind,principal,payload):
        with self.lock:
            self.event_sequence+=1
            value={'kind':kind,'principal':principal,'generation':self.event_generation,'sequence':self.event_sequence,'time':time.time(),'payload':payload}
            value=json.loads(json.dumps(value,default=encode))
            self.events.append(signed(value,self.keys[principal]))
            temp=self.root/'events.tmp';temp.write_text(json.dumps(list(self.events)));temp.chmod(0o600);temp.replace(self.root/'events.json')

    def monitor(self):
        with self.lock:
            for sid,live in list(self.verifying.items()):
                if live:
                    self.emit('simulation.verification.status',self.state['verifications'][sid]['principal'],{'session_id':sid,**live.status()})
                    if live.done.is_set():self.stop_verification(sid)
            for sid,active in list(self.running.items()):
                status=asdict(active['task'].status());status['state']=status['state'].value
                if self.last_status.get(sid)!=status:
                    self.last_status[sid]=status
                    s=self.state['sessions'][sid]
                    self.emit('collection.status',s['principal'],{'session_id':sid,'dataset_id':s['dataset_id'],**status})

    def fields(self,data,required,optional=()):
        if not set(required)<=set(data) or set(data)-set(required)-set(optional):raise ApiError('INVALID_FIELDS','Required/allowed fields differ')

    def permission(self,principal,project):
        rule=self.config['principals'].get(principal,{})
        if project not in rule.get('projects',[]) or 'collector' not in rule.get('roles',[]):raise ApiError('FORBIDDEN','Principal/project not authorized')

    def owner(self,kind,key,principal):
        value=self.state[kind].get(identifier(key))
        if value is None or value['principal']!=principal:raise ApiError('NOT_FOUND','Resource unavailable')
        self.permission(principal,value['project']);return value

    def context(self,payload,principal):
        c=self.state['claim']
        if not c or c['principal']!=principal:raise ApiError('NOT_OWNER','Claim robot first')
        if payload.get('robot_id')!=c['robot_id'] or payload.get('epoch')!=c['epoch'] or not secrets.compare_digest(str(payload.get('control_token','')),c['control_token']):raise ApiError('STALE_CONTROL','Robot/epoch/control token mismatch')
        if payload.get('config_revision')!=self.registered_robot(c['robot_id'])['config_revision']:raise ApiError('CONFIG_MISMATCH','Robot config revision differs')
        return c

    def dispatch(self,envelope):
        principal=envelope.get('principal') if isinstance(envelope,dict) else None
        try:
            req=request_payload(envelope,self.keys);principal=req['principal'];request_id=req['request_id']
            digest=hashlib.sha256(canonical({k:req[k] for k in ('principal','operation','payload')})).hexdigest()
            with self.lock:
                mutating=req['operation'] not in ('catalog','operation.get','collection.status','physical.preflight','dataset.status','simulation.verify.status','calibration.status','calibration.list')
                saved=self.state['requests'].get(principal+':'+request_id) if mutating else None
                if saved:
                    if saved['digest']!=digest:raise ApiError('IDEMPOTENCY_CONFLICT','request_id reused with different content')
                    result=saved['result']
                else:
                    result=self.handle(req['operation'],req['payload'],principal)
                    if mutating:
                        self.state['requests'][principal+':'+request_id]={'digest':digest,'result':result};self.persist()
            answer={'request_id':request_id,'ok':True,'result':result}
        except Exception as exc:
            answer={'request_id':envelope.get('request_id','') if isinstance(envelope,dict) else '', 'ok':False,
                'error':{'code':getattr(exc,'code','INVALID_REQUEST'),'message':str(exc)}}
            if getattr(exc,'details',None) is not None:answer['error']['details']=exc.details
        answer=json.loads(json.dumps(answer,default=encode))
        return signed(answer,self.keys[principal]) if principal in self.keys else answer

    def handle(self,op,p,principal):
        if self.stopping:raise ApiError('STOPPING','Service shutting down')
        if op in ('simulation.verify.start','simulation.verify.status','simulation.verify.stop'):return self.handle_verification(op,p,principal)
        if op.startswith('calibration.'):return self.handle_calibration(op,p,principal)
        if op=='dataset.convert':return self.handle_conversion(p,principal)
        if op=='dataset.conversion_log':
            self.fields(p,['operation_id']);job=self.owner('jobs',p['operation_id'],principal)
            if job['operation']!='dataset.convert':raise ApiError('INVALID_OPERATION','Not a conversion operation')
            return self.conversion_log(job)
        if op=='teleoperation.start':raise ApiError('HARDWARE_NOT_READY','Physical follower teleoperation remains deferred; it is never routed to simulation')
        if op=='catalog':
            self.fields(p,[])
            return {'robot':self.config['robot'],'ros':public_ros(self.config['ros']) if self.strict else self.config['ros'],'sinks':list(self.config['sinks']),
                'datasets':list(self.config.get('import_datasets',{})),'physical_robot':self.config.get('physical_robot',{}),
                'sources':list(self.config['sources']),'schema':self.config.get('schema'),'calibration_revisions':self.calibration_catalog(principal),'leader_robot':self.config.get('leader_robot'),
                'converters':[{k:v[k] for k in ('id','version','source_format','target_format')} for v in self.config.get('converters',{}).values()],
                'scope':'NexusEdge collection/upload; training and model deployment excluded'}
        if op=='robot.claim':
            self.fields(p,['robot_id','project','config_revision'])
            self.permission(principal,p['project'])
            if p['robot_id']==self.config.get('physical_robot',{}).get('robot_id'):
                raise ApiError('HARDWARE_NOT_READY',self.config['physical_robot']['reason'])
            if p['config_revision']!=self.registered_robot(p['robot_id'])['config_revision']:raise ApiError('CONFIG_MISMATCH','Registered robot/revision required')
            claim=self.state['claim']
            if claim and (claim['principal']!=principal or self.busy()):raise ApiError('BUSY','Robot assigned or active')
            self.state['epoch']+=1
            claim={**p,'principal':principal,'epoch':self.state['epoch'],'control_token':secrets.token_urlsafe(24)}
            self.state['claim']=claim;return dict(claim)
        if op=='physical.preflight':
            self.fields(p,[])
            return self.config.get('physical_robot',{'ready':False,'reason':'No commissioned physical backend'})
        if op=='robot.release':
            self.fields(p,['robot_id','config_revision','epoch','control_token']);self.context(p,principal)
            if self.busy():raise ApiError('BUSY','Stop and seal active session before releasing assignment')
            self.state['epoch']+=1;self.state['claim']=None
            return {'released_assignment':True,'epoch':self.state['epoch'],'physical_torque_changed':False}
        if op=='collection.start_physical':
            self.fields(p,['robot_id','project','name','description','calibration_revision','device_config_revision','fps'])
            self.permission(principal,p['project'])
            physical=self.config.get('physical_robot',{})
            if p['robot_id']!=physical.get('robot_id'):raise ApiError('NOT_FOUND','Physical target is not registered')
            raise ApiError('HARDWARE_NOT_READY',physical.get('reason','Physical driver not commissioned'))
        if op=='operation.get':
            self.fields(p,['operation_id']);return dict(self.owner('jobs',p['operation_id'],principal))
        if op=='collection.status':
            self.fields(p,['session_id']);s=self.owner('sessions',p['session_id'],principal)
            active=self.running.get(p['session_id'])
            if active:
                status=asdict(active['task'].status());status['state']=status['state'].value
                robot=active['robot'];arm=robot.get_arm_data();mapping=getattr(active['leader'],'last_mapping',None)
                # Read the exact final target accepted by the sole I/O worker;
                # never substitute requested mapping or measured feedback.
                with robot._lock:sent=dict(robot._last_sent) if robot._last_sent is not None else None
                feedback={'feedback':dict(arm.positions),'sent_target':sent,'requested_target':mapping.get('requested') if mapping else None,
                    'leader_mapping':mapping,'observation_age_ms':arm.age_ms,'units':dict(arm.units),'sequence':arm.sequence}
                return {**s,'sdk_status':status,'robot_status':asdict(robot.status()),'feedback_status':feedback,**self.connection_status()}
            return dict(s)
        if op=='collection.start':
            required=['robot_id','project','config_revision','epoch','control_token','name','description','scene','ros','source','fps']
            self.fields(p,required+(['calibration_revision','device_config_revision'] if self.strict else []),[] if self.strict else ['calibration_revision','device_config_revision'])
            c=self.context(p,principal);self.permission(principal,p['project'])
            if p['robot_id']!=self.config['robot']['robot_id']:raise ApiError('WRONG_TARGET','Simulation collection cannot borrow leader/follower target permissions')
            if p['project']!=c['project']:raise ApiError('FORBIDDEN','Claim project differs')
            if self.busy():raise ApiError('BUSY','Target already active')
            if p['scene']!=self.config['robot']['scene']:raise ApiError('CONFIG_MISMATCH','Scene differs from registered whitelist')
            try:match_ros(p['ros'],self.config['ros'],legacy=not self.strict)
            except ApiError as exc:raise ApiError('CONFIG_MISMATCH',str(exc)) from exc
            if p['source'] not in self.config['sources']:raise ApiError('INVALID_SOURCE','Unregistered leader source')
            if not isinstance(p['name'],str) or not p['name'].strip() or len(p['name'])>128:raise ApiError('INVALID_NAME','Task name required, max128')
            if not isinstance(p['description'],str) or len(p['description'])>2048:raise ApiError('INVALID_DESCRIPTION','Description max2048')
            if type(p['fps']) is not int or not 1<=p['fps']<=10:raise ApiError('INVALID_FPS','Bench collection supports integer1..10Hz')
            if self.strict and p['device_config_revision']!=self.config['robot']['device_config_revision']:raise ApiError('DEVICE_CONFIG_MISMATCH','Device revision differs')
            self.calibration_access(principal,p['project'],p.get('calibration_revision','baseline'))
            frozen=self.freeze_binding(p['source'],p.get('calibration_revision','baseline'))
            sid=uuid.uuid4().hex
            self.state['sessions'][sid]={'session_id':sid,'dataset_id':sid,'principal':principal,'project':p['project'],'state':'STARTING','epoch':c['epoch'],
                'source':p['source'],'name':p['name'],'description':p['description'],'fps':p['fps'],'episode_id':uuid.uuid4().hex,'pending':'start','upload_state':'NOT_REQUESTED',
                'robot_id':p['robot_id'],'config_revision':p['config_revision'],'binding':frozen}
            self.startup_events[sid]=threading.Event()
            return self.submit(op,principal,p['project'],lambda:self.start(sid),session_id=sid,dataset_id=sid)
        if op in ('collection.save','collection.redo','collection.next','collection.stop','collection.recover'):
            required=['session_id','robot_id','config_revision','epoch','control_token']
            required += ['episode_id'] if op in ('collection.save','collection.redo') else []
            required += ['confirm'] if op=='collection.redo' else []
            required += ['save_current'] if op=='collection.stop' else []
            self.fields(p,required,['sink_id'] if op=='collection.stop' else []);claim=self.context(p,principal)
            s=self.owner('sessions',p['session_id'],principal)
            if s['project']!=claim['project']:raise ApiError('FORBIDDEN','Session project differs from claim')
            if op=='collection.stop':
                # A configuration mismatch must never remove the active owner's stop path.
                if s.get('robot_id',self.config['robot']['robot_id'])!=claim['robot_id'] or s['epoch']!=claim['epoch']:raise ApiError('STALE_SESSION','Stop target/epoch differs')
            else:self.check_session(s,claim,recover=op=='collection.recover')
            if s.get('pending') and op!='collection.stop':raise ApiError('BUSY','Previous operation pending')
            if op in ('collection.save','collection.redo') and p['episode_id']!=s['episode_id']:raise ApiError('STALE_EPISODE','Current episode identifier differs')
            if op=='collection.redo' and p['confirm'] is not True:raise ApiError('CONFIRM_REQUIRED','Redo requires confirm=true')
            if op=='collection.stop' and type(p['save_current']) is not bool:raise ApiError('INVALID_FIELDS','save_current must be boolean')
            if op=='collection.stop' and p.get('sink_id',self.config.get('default_upload_sink')) not in (None,*self.config['sinks']):raise ApiError('INVALID_SINK','Upload sink is not registered')
            if op=='collection.recover' and s['state']!='RECOVERABLE':raise ApiError('INVALID_STATE','Only interrupted/recoverable task can recover')
            if op=='collection.recover':
                s['stop_requested']=False;self.startup_events[s['session_id']]=threading.Event()
            if op=='collection.stop':
                s['stop_requested']=True
                active=self.running.get(s['session_id'])
                if active:
                    # This only sets the SDK cancellation latch and queues the
                    # transaction. The wait/finalization stays off the API lock.
                    try:active['stop_operation']=active['task'].stop(p['save_current'])
                    except Exception as exc:active['stop_dispatch_error']=str(exc)
            s['pending']=op
            return self.submit(op,principal,s['project'],lambda:self.control_task(op,s,p),session_id=s['session_id'],urgent=op=='collection.stop')
        if op=='dataset.import':
            self.fields(p,['source_id','project']);self.permission(principal,p['project'])
            name=identifier(p['source_id'])
            if name not in self.config.get('import_datasets',{}):raise ApiError('NOT_FOUND','Dataset not registered')
            return self.submit(op,principal,p['project'],lambda:self.import_dataset(name,principal,p['project']),transfer=True)
        if op=='dataset.status':
            self.fields(p,['dataset_id']);return dict(self.owner('datasets',p['dataset_id'],principal))
        if op in ('dataset.inspect','dataset.export','dataset.upload'):
            self.fields(p,['dataset_id']+(['sink_id'] if op=='dataset.upload' else []))
            data=self.owner('datasets',p['dataset_id'],principal)
            if op=='dataset.upload' and p['sink_id'] not in self.config['sinks']:raise ApiError('INVALID_SINK','Sink not registered')
            return self.submit(op,principal,data['project'],lambda:self.dataset_op(op,data,p),transfer=True,dataset_id=data['dataset_id'])
        raise ApiError('UNSUPPORTED','Unsupported operation; no training, model or arbitrary command API')

    def submit(self,operation,principal,project,function,*,transfer=False,urgent=False,**ids):
        ident=uuid.uuid4().hex
        job={'operation_id':ident,'operation':operation,'principal':principal,'project':project,'status':'QUEUED','created_at':time.time(),**ids}
        self.state['jobs'][ident]=job
        self.emit('operation.queued',principal,{'operation_id':ident,'operation':operation,**ids})
        def work():
            with self.lock:job['status']='RUNNING';self.persist();self.emit('operation.running',principal,{'operation_id':ident,**ids})
            try:
                value=function()
                with self.lock:
                    job.update(status='SUCCEEDED',result=value,finished_at=time.time());self.persist()
                    self.emit('operation.succeeded',principal,{'operation_id':ident,**ids})
            except Exception as exc:
                with self.lock:
                    job.update(status='FAILED',error={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc)},finished_at=time.time())
                    if getattr(exc,'details',None) is not None:job['error']['details']=exc.details
                    if 'session_id' in ids and ids['session_id'] in self.state['sessions']:
                        s=self.state['sessions'][ids['session_id']];active=self.running.get(ids['session_id'])
                        s.update(pending=None)
                        if s.get('state')!='CLOSED':s['state']=active['task'].status().state.value if active else 'RECOVERABLE'
                    self.persist()
                    self.emit('operation.failed',principal,{'operation_id':ident,'error':job['error'],**ids})
        if urgent:threading.Thread(target=work,daemon=True).start()
        else:(self.transfer if transfer else self.control).submit(work)
        return {'operation_id':ident,'status':'QUEUED',**ids}

    def start(self,sid,recover=False):
        s=self.state['sessions'][sid];fixture=self.config['contract'];robot=None;task=None;activated=False
        event=self.startup_events.setdefault(sid,threading.Event())
        try:
            if s.get('binding'):assert_binding(s['binding'],self.freeze_binding(s['source'],s['binding']['calibration_revision']))
            if recover and sid in self.running:self.running.pop(sid)['robot'].close()
            config=build_robot_config(fixture,{'front':(480,640,3),'wrist':(480,640,3)},s['fps'])
            root=self.root/'tasks'
            task=self.cm.open_task(root/sid) if recover else self.cm.create_task(CollectionConfig(name=sid,description=s['description'],root=root,fps=s['fps'],
                task=s['name'],robot_id=config.robot_id,calibration_hash=config.calibration_hash,units=config.units,
                camera_shapes={'front':(480,640,3),'wrist':(480,640,3)},leader_timeout_s=.25,max_duration_s=1800))
            # Real backends must be commissioned separately; never spoof kind='simulation'.
            backend=FileSimulationBackend(self.config['ipc_root'],calibration_hash=config.calibration_hash)
            robot=RobotSession(config,backend,lock_dir=self.root/'locks')
            robot.connect();base=dict(robot.get_arm_data().positions)
            with self.lock:
                if s.get('stop_requested'):raise ApiError('START_CANCELLED','Collection startup was stopped before activation')
            source=self.config['sources'][s['source']]
            leader=self.make_leader(s['source'],config,base,s.get('binding',{}))
            if source['kind']=='native_topic':
                from .initial_alignment import preflight_alignment
                preflight_alignment(robot,leader)  # Formal recording never performs hidden startup alignment.
            with self.lock:
                if s.get('stop_requested'):raise ApiError('START_CANCELLED','Collection startup was stopped before activation')
                self.running[sid]={'robot':robot,'leader':leader,'task':task}
                started=task.start(robot,leader);activated=True
            started.wait(180)
            with self.lock:
                if not s.get('stop_requested'):s.update(state=task.status().state.value,pending=None)
                self.persist()
            return {'session_id':sid,'dataset_id':sid,'episode_id':s['episode_id'],'state':s['state'],'source':source['kind'],'source_id':s['source'],
                'committed_episodes':task.status().committed_episodes,'current_frames':task.status().current_frames,'binding':s.get('binding'),
                'ros_status':json.loads((Path(self.config['ipc_root'])/'ready.json').read_text()),
                'timestamp_status':json.loads((Path(self.config['ipc_root'])/'timestamp-status.json').read_text()) if (Path(self.config['ipc_root'])/'timestamp-status.json').exists() else {'validated':True,'method':'SDK observation monotonicity/age checks'}}
        except Exception as exc:
            result=None;cleanup_error=None
            if activated:
                try:task.stop(False).wait(10)
                except Exception:pass  # Actual robot HOLD/close is checked below.
            try:
                if robot is not None:result=self.close_startup_robot(robot)
            except Exception as close_error:cleanup_error=close_error
            with self.lock:
                self.running.pop(sid,None)
                has_frames=bool(task and (task.status().committed_episodes or task.status().current_frames))
                s.update(state='RECOVERABLE' if has_frames else 'CLOSED',pending=None,
                    startup_error={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc)})
                if getattr(exc,'details',None) is not None:s['startup_error']['details']=exc.details
                if result:s['startup_stop_result']={**result,'state':s['state'],'session_id':sid,'dataset_published':False,'sealed_state':'NOT_CREATED'}
                if cleanup_error:s['stop_error']=str(cleanup_error)
                self.persist()
            if cleanup_error:raise ApiError('STOP_UNCONFIRMED',str(cleanup_error)) from exc
            raise
        finally:event.set()

    def control_task(self,op,s,p):
        if op=='collection.recover':return self.start(s['session_id'],recover=True)
        active=self.running.get(s['session_id'])
        if not active and op=='collection.stop':
            self.wait_startup_barrier(s['session_id'])
            active=self.running.get(s['session_id'])
            if not active:
                result=s.get('startup_stop_result')
                if not result:raise ApiError('STOP_UNCONFIRMED','Startup stop is latched but HOLD/close is not confirmed')
                with self.lock:s['pending']=None;self.persist()
                return result
        if not active:raise ApiError('NOT_ACTIVE','No connected collection session')
        task=active['task']
        if op=='collection.save':result=episode_result(task,task.save_episode().wait(180))
        elif op=='collection.redo':result=task.redo_episode().wait(180)
        elif op=='collection.next':result=task.start_episode().wait(180);s['episode_id']=uuid.uuid4().hex
        else:
            with self.lock:s['state']='FINALIZING';self.persist()
            try:
                stopped=active.get('stop_operation') or task.stop(p['save_current'])
                ref=stopped.wait(300)
            except Exception:
                status=task.status()
                if not status.committed_episodes and not status.current_frames:
                    result=self.close_startup_robot(active['robot'])
                    with self.lock:
                        self.running.pop(s['session_id'],None);s.update(state='CLOSED',pending=None)
                        result.update(state='CLOSED',session_id=s['session_id'],dataset_published=False,sealed_state='NOT_CREATED')
                        s['startup_stop_result']=result;self.persist()
                    return result
                raise
            checked=self.dm.inspect(ref)
            metadata={'robot_id':self.config['robot']['robot_id'],'project':s['project'],'principal':s['principal'],'name':s['name'],'source':s['source'],
                'ros':self.config['ros'],'ros_contract_hash':contract_hash(self.config['ros']),'scene':self.config['robot']['scene'],'config_revision':self.config['robot']['config_revision']}
            metadata['session_binding']=s.get('binding')
            metadata['operation_log']=[{k:j[k] for k in ('operation_id','operation','status','created_at','finished_at') if k in j}
                for j in self.state['jobs'].values() if j.get('session_id')==s['session_id']]
            (self.root/('source-'+s['dataset_id']+'.json')).write_text(json.dumps(metadata,indent=2))
            ref=self.dm.migrate(ref,self.root/'sealed'/s['dataset_id'],attach_provenance,parameters=metadata).wait(300)
            self.state['datasets'][s['dataset_id']]={'dataset_id':s['dataset_id'],'root':str(ref.root),'content_hash':ref.content_hash,'version':ref.version,
                'principal':s['principal'],'project':s['project'],'local_state':'LOCAL_READY','upload_state':'NOT_REQUESTED'}
            result={'dataset_id':s['dataset_id'],'root':str(ref.root),'version':ref.version,'manifest_hash':ref.content_hash,'episodes':checked.episodes,'frames':checked.frames,
                'sealed_state':'LOCAL_READY','upload_state':'NOT_REQUESTED','borrowed_robot_connected_after_task_stop':active['robot'].get_arm_data().sequence>0}
            # Service owns the connection; task.stop itself did not close borrowed devices.
            active['robot'].close();self.running.pop(s['session_id'],None)
            sink=p.get('sink_id',self.config.get('default_upload_sink') if self.strict else None)
            if sink:
                if sink not in self.config['sinks']:raise ApiError('INVALID_SINK','Stop upload sink is not registered')
                try:
                    uploaded=self.dataset_op('dataset.upload',self.state['datasets'][s['dataset_id']],{'sink_id':sink})
                    result.update(upload_state='READY',upload_verified=True,storage_ref=uploaded['storage_ref'],upload_hash=uploaded['content_hash'],business_complete=True)
                except Exception as exc:
                    result.update(upload_state='PENDING_RETRY',upload_verified=False,business_complete=False,upload_error=str(exc))
            else:result['business_complete']=False
            s['upload_state']=result['upload_state']
        with self.lock:
            s.update(state=task.status().state.value,pending=None);self.persist()
            self.emit('collection.status',s['principal'],{'session_id':s['session_id'],'dataset_id':s['dataset_id'],'source':s['source'],**asdict(task.status())})
        return json.loads(json.dumps(result,default=lambda x:asdict(x) if hasattr(x,'__dataclass_fields__') else encode(x)))

    def import_dataset(self,name,principal,project):
        inspected=self.dm.inspect(Path(self.config['import_datasets'][name]));ident=uuid.uuid4().hex
        with self.lock:
            self.state['datasets'][ident]={'dataset_id':ident,'root':str(inspected.ref.root),'content_hash':inspected.ref.content_hash,'version':inspected.ref.version,
                'principal':principal,'project':project,'local_state':'LOCAL_READY','upload_state':'NOT_REQUESTED'};self.persist()
        return {'dataset_id':ident,'version':inspected.ref.version,'manifest_hash':inspected.ref.content_hash,'episodes':inspected.episodes,'frames':inspected.frames}

    def dataset_op(self,op,data,p):
        if data.get('format')=='gr00t-v2.1':return self.converted_dataset_op(op,data,p)
        info=self.dm.inspect(Path(data['root']))
        if info.ref.content_hash!=data['content_hash']:raise ApiError('HASH_CHANGED','Immutable dataset changed')
        if op=='dataset.inspect':return json.loads(json.dumps(asdict(info),default=encode))
        if op=='dataset.export':
            ref=self.dm.export(info.ref,Path(self.config['export_root'])/data['dataset_id']).wait(300)
            return {'storage_ref':str(ref.root),'content_hash':ref.content_hash}
        sink=self.config['sinks'][p['sink_id']]
        if sink['kind']!='mounted_directory':raise ApiError('UNSUPPORTED_SINK','Install an authorized DatasetSink adapter')
        target=Path(sink['root'])/data['dataset_id']
        with self.lock:data['upload_state']='UPLOADING';self.persist()
        try:
            operation=self.dm.upload(info.ref,RetryingDatasetSink(LocalDirectorySink(target),max_attempts=2))
            # Upload is independent of the control worker and its I/O thread.
            while operation.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):
                progress=operation.progress()
                if progress:
                    with self.lock:
                        data['upload_progress']=asdict(progress);self.persist()
                        self.emit('upload.progress',data['principal'],{'dataset_id':data['dataset_id'],**asdict(progress)})
                time.sleep(.1)
            result=operation.wait(300)
            with self.lock:data.update(upload_state='READY',storage_ref=result.storage_ref);self.persist()
            self.emit('upload.progress',data['principal'],{'dataset_id':data['dataset_id'],'phase':'READY','content_hash':result.content_hash})
            return asdict(result)
        except Exception:
            with self.lock:data['upload_state']='PENDING_RETRY';self.persist()
            self.emit('upload.progress',data['principal'],{'dataset_id':data['dataset_id'],'phase':'PENDING_RETRY'})
            raise

    def shutdown(self):
        self.stopping=True
        with self.lock:
            for sid,event in self.startup_events.items():
                if not event.is_set():
                    record=self.state['verifications'].get(sid) or self.state['sessions'].get(sid)
                    if record is not None:record['stop_requested']=True
        results=[]
        for sid,live in list(self.verifying.items()):
            if live:
                try:results.append({'session_id':sid,'hold_confirmed':self.stop_verification(sid)['holding'],'closed':True})
                except Exception as exc:results.append({'session_id':sid,'hold_confirmed':False,'error':str(exc)})
        for sid,active in list(self.running.items()):
            result={'session_id':sid,'hold_confirmed':False,'closed':False}
            try:
                active['robot'].stop('container shutdown');result['hold_confirmed']=True
                active['task'].stop(False).wait(120)
            except Exception as exc:result['stop_error']=str(exc)
            try:active['robot'].close();result['closed']=True
            except Exception as exc:result['close_error']=str(exc)
            results.append(result)
        self.control.shutdown(wait=True);self.transfer.shutdown(wait=True)
        return results
