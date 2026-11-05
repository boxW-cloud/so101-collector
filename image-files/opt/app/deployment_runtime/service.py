"""Role-separated public service over shared SDK and versioned ROS adapters."""
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import json,os,threading,time
from .store import Store,Fault,Principal,digest
from .datasets import Datasets
from .sessions import SessionRuntime,MODES
from .field_bindings import (bind_robot_fields,operation_fields,public_ros_config,
                            unverified_ros_status,validate_scene_registration)

class Platform:
    def __init__(self,config,*,sessions=None,training=None,inspector=None):
        target_file=os.environ.get('SO101_COLLECTION_TARGETS_JSON')
        if config.get('role')=='collection' and 'collection_targets' not in config and target_file:
            targets=json.loads(Path(target_file).read_text(encoding='utf-8'))
            if not isinstance(targets,dict):raise ValueError('Collection target deployment file must contain an object')
            config={**config,'collection_targets':targets}
        self.config=config;self.role=config['role'];self.root=Path(config['root']);self.root.mkdir(parents=True,exist_ok=True)
        if self.role not in ('collection','training'):raise ValueError('Explicit service role required')
        self.store=Store(self.root/'registry.sqlite');self.pool=ThreadPoolExecutor(4);self.lock=threading.RLock();self.stop_flags={};self.runtime_training=training;self.closing=False
        self.datasets=Datasets(self.store,self.root/'datasets',**({'inspect':inspector} if inspector else {}))
        if sessions is None:
            from .ros_transport import RosBackend
            sessions=SessionRuntime(self.root/'capture',RosBackend)
        self.sessions=sessions;self.collection=None;self.minimal_collection=None;self.current_collection=None
        self.collection_admission_lock=threading.RLock();self.collection_command_apis=[];self.collection_legacy_inflight=0
        self.compute=threading.Semaphore(1)
        from .calibration import Calibrations,RosCalibrationReader
        self.calibrations=Calibrations(self.store,RosCalibrationReader,self.root)
    def capabilities(self,principal):
        from .api_contract import CONTRACT_VERSION
        return {'role':self.role,'api_version':'v2','contract_version':CONTRACT_VERSION,'collection_modes':list(MODES),'projects':list(principal.projects),'roles':list(principal.roles),
                'physical_validation':'requires registered commissioned ROS peer; no implicit fake fallback',
                'training_policies':['act','pi05'],'training_dataset_count':1,'gpu_jobs_at_once':1,
                'image_id':self.config.get('image_id'),'resource_limits':self.config.get('resources',{}),
                'requires_live_robot_preflight':True,'physical_acceptance':False,
                'field_support':{'stable_dataset_id_on_start':True,'scene_version_binding':True,'structured_ros_status':True,'conversion_log_reference':True},
                'execution_support':{'multi_dataset_training':False,'per_job_memory_limit':False,'per_job_gpu_isolation':False,
                    'external_platform_control_token':False,'retained_collector_assignment':False,'sensor_timestamp_monotonicity':False},
                'collection_contract':{'version':'so101.collection.v1','per_episode_upload':True,'automatic_next':True,'status_states':['RUNNING','IDLE','FINISHING'],
                    'one_click_finish':True,'automatic_delivery':True,'local_capacity_cleanup':False,
                    'project_target_binding':True,
                    'minimal_api':{'version':'minimal.v9','asynchronous':True,'one_robot_per_container':True,
                        'server_managed_project':True,'recording_attempt_ids':True},
                    'current_api':{'version':'minimal.v10','asynchronous':True,'one_robot_per_container':True,
                        'external_request_id':False,'server_managed_session':True,'send_batch_on_finish':True,
                        'frontend_polling_required':False,'acceptance_is_completion':False}},
                'gr00t':{'available':False,'reason':'Register a frozen GR00T runtime adapter; ACT/pi05 are the built-in managed SDK adapters'}}
    def get_collection(self):
        if self.role!='collection':raise Fault('WRONG_SERVICE','Use collection container')
        with self.lock:
            if self.collection is None:
                from .collection_contract import CollectionContract
                self.collection=CollectionContract(self)
            return self.collection
    def get_minimal_collection(self):
        if self.role!='collection':raise Fault('WRONG_SERVICE','Use collection container')
        with self.lock:
            if self.minimal_collection is None:
                from .collection_minimal import MinimalCollectionAPI
                self.minimal_collection=MinimalCollectionAPI(self)
            return self.minimal_collection
    def get_current_collection(self):
        if self.role!='collection':raise Fault('WRONG_SERVICE','Use collection container')
        with self.lock:
            if self.current_collection is None:
                from .collection_current import CurrentCollectionAPI
                self.current_collection=CurrentCollectionAPI(self)
            return self.current_collection
    def run_legacy_collection(self,call,*args):
        """Reserve legacy admission without holding a lock through device I/O."""
        with self.collection_admission_lock:
            for api in self.collection_command_apis:
                if api.closed or api.api_contract!='minimal.v10':continue
                with api.lock:
                    pending=api.db.execute("SELECT 1 FROM commands WHERE state IN ('QUEUED','RUNNING')").fetchone()
                if pending or any(row.get('api_contract')=='minimal.v10' for row in api._rows()):
                    raise Fault('BUSY','A current collection command or unfinished batch owns this container')
            self.collection_legacy_inflight+=1
        try:return call(*args)
        finally:
            with self.collection_admission_lock:self.collection_legacy_inflight-=1
    def probe(self,key,revision,a):
        from .ros_transport import TopicRPC
        from .ros_wire import request
        import uuid
        registered=self.store.get('robots',key,revision,a)['body'];rpc=TopicRPC(registered['ros'])
        try:
            value=rpc.call(request('inspect',{},'probe-'+uuid.uuid4().hex,0,rpc.key),5)
            if value['robot_id']!=key or value['kind']!=registered['kind'] or value['calibration_hash']!=registered['sdk_config']['calibration_hash']:raise Fault('PEER_MISMATCH','Actual ROS robot differs from registered target')
            expected={k:registered['ros'][k] for k in ('command_topic','result_topic','state_topic','observation_topic','camera_topics','clock_topic','contract_version')}
            if value.get('transport_contract')!=expected:raise Fault('ROS_PEER_CONTRACT','Signed peer configuration differs')
            graph=rpc.preflight(5);diagnostics=unverified_ros_status(registered['ros'])
            diagnostics.update(connection_state='CONNECTED',checked_at_monotonic_ns=time.monotonic_ns(),nodes=graph.get('nodes',[]),topics=graph.get('topics',[]),topics_verified=graph['topics_verified'],configuration_verified=True)
            return {'robot_id':key,'revision':revision,'config_revision':revision,'scene_id':registered.get('scene_id'),'scene_revision':registered.get('scene_revision'),
                    'ros_config':public_ros_config(registered['ros']),'ros_status':diagnostics,'ros_connected':True,'read_only':True,'peer':value,
                    'device_motion_verified':False,'camera_health':'See media frame age; configuration is not a fresh-camera check'}
        finally:rpc.close()
    def register(self,kind,p,principal):
        principal.require(p['project'],'admin')
        if kind not in ('robots','tasks','agents','models'):raise Fault('INVALID_KIND','Use verified intake for data')
        body=p['body']
        if kind=='robots':
            from lerobot_sdk import RobotConfig
            if body['kind'] not in ('physical','simulation'):raise Fault('INVALID_KIND','Robot kind required')
            validate_scene_registration(body)
            RobotConfig(**body['sdk_config'])
            if body['robot_id']!=p['id']:raise Fault('INVALID_ID','Robot ID differs from directory entry')
            if not isinstance(body.get('capabilities'),list) or not body.get('ros'):raise Fault('INVALID_ROBOT','ROS contract and capability list required')
            if body['sdk_config']['robot_id']!=body['robot_id']:raise Fault('INVALID_ID','SDK robot ID differs')
            from .ros_contract import complete
            body={**body,'ros':complete(body['ros'],tuple(body['camera_shapes']))}
        if kind=='tasks':
            if body.get('type')!='joint_sequence' or not body.get('steps'):raise Fault('INVALID_TASK','Frozen joint_sequence task required')
        if kind=='agents' and body.get('policy_type') not in ('act','pi05'):raise Fault('UNSUPPORTED_POLICY','Built-in training supports act/pi05 only')
        if kind=='models':
            from lerobot_sdk import ModelManager
            path=Path(body['path']).resolve()
            if not any(path.is_relative_to(Path(r).resolve()) for r in self.config.get('model_roots',[])):raise Fault('INVALID_PATH','Model outside configured roots')
            inspected=ModelManager().inspect(path)
            if inspected.content_hash!=body['content_hash']:raise Fault('MODEL_HASH_MISMATCH','Model manifest/hash differs')
        return self.store.register(kind,p['id'],p['revision'],p['project'],principal,body)
    def _robot(self,p,a):
        r=self.store.get('robots',p['robot_id'],p['robot_revision'],a)
        if r['project']!=p['project']:raise Fault('FORBIDDEN','Robot project differs',403)
        return {**r['body'],'revision':r['revision']}
    def session(self,key,a):
        j=self.store.job(key,a)
        if j['kind'] not in ('collections.start','verifications.start'):raise Fault('INVALID_SESSION','Not a control session')
        try:return self.sessions.status(key)
        except Fault:
            result=j.get('result') or {};p=j['payload']
            return {**result,'session_id':key,'dataset_id':key if j['kind']=='collections.start' else None,
                'state':j['state'],'result':j['result'],'error':j['error'],'active':False,
                **{name:p.get(name) for name in ('robot_id','robot_revision','config_revision','calibration_revision','scene_id','scene_revision')},
                'ros_config':p.get('ros_config',{}),'ros_status':result.get('ros_status',unverified_ros_status(p.get('ros_config',{})))}
    def operation(self,key,a):return operation_fields(self.store.job(key,a))
    def operation_log(self,key,a,*,conversion=False):
        import re
        job=self.store.job(key,a);log_id=None
        if conversion:
            if job['kind']!='datasets.convert':raise Fault('INVALID_OPERATION','This operation has no conversion log')
            log_id=(job.get('result') or {}).get('log_id') or ((job.get('error') or {}).get('details') or {}).get('log_id')
            if log_id is not None and (not isinstance(log_id,str) or not re.fullmatch(r'[a-f0-9]{32}',log_id)):
                raise Fault('INVALID_LOG_REFERENCE','Stored conversion log reference is invalid')
            root=(self.root/'conversion/conversion-logs').resolve();raw=root/(log_id+'.log') if log_id else None
        else:
            if job['kind']!='training.start':raise Fault('INVALID_OPERATION','Training log is only available for training operations')
            root=(self.root/'jobs'/key).resolve();raw=root/'attempt/worker.log'
        value={'operation_id':key,'log_id':log_id,'state':job['state'],'available':False,'text':'','truncated':False,'limit_bytes':32000}
        if raw is None or not raw.exists():return value
        path=raw.resolve()
        if raw.is_symlink() or not path.is_relative_to(root) or not path.is_file():raise Fault('INVALID_LOG_REFERENCE','Log path is outside its operation root')
        size=path.stat().st_size
        with path.open('rb') as stream:
            stream.seek(max(0,size-32000));body=stream.read(32000)
        return {**value,'available':True,'text':body.decode('utf-8',errors='replace'),'truncated':size>32000,'bytes_total':size}
    def submit(self,op,p,request_id,a):
        with self.lock:
            if self.closing:raise Fault('SHUTTING_DOWN','Service is shutting down',503)
            return self._submit_locked(op,p,request_id,a)
    def _submit_locked(self,op,p,request_id,a):
        if op.startswith('collections.') and op!='collections.start' and self.collection is not None and self.collection.owns_session(p.get('session_id')):
            raise Fault('LEGACY_CONTRACT_DISABLED','This session uses /collection routes with recording generations',422)
        if self.role=='collection' and self.config.get('collection_contract_only') and (op.startswith('collections.') or op=='datasets.upload'):
            raise Fault('LEGACY_CONTRACT_DISABLED','Use /collection/sessions operations; uploads recover automatically',422)
        from .api_contract import validate
        validate(op,p)
        project=p['project'];role='trainer' if op.startswith(('training.','inference.')) else 'operator';a.require(project,role)
        import copy
        original=copy.deepcopy(p)
        p=copy.deepcopy(original)
        previous=self.store.prior_job(op,original,request_id,a,project)
        if previous:return operation_fields(previous)
        resource=None;robot=None;parent=None
        if op.startswith(('collections.','verifications.','scene.','calibration.')) and self.role!='collection':raise Fault('WRONG_SERVICE','Use collection container')
        if op.startswith(('training.','inference.')) and self.role!='training':raise Fault('WRONG_SERVICE','Use training container')
        if op=='robots.reconcile':
            robot=self._robot(p,a);claims=[x for x in self.store.claims() if x['resource']=='robot:'+robot['robot_id']]
            if not claims:raise Fault('NOT_CLAIMED','No interrupted claim to reconcile')
            parent=self.store.job(claims[0]['job_id'],a)
            if claims[0]['state']=='ACTIVE':raise Fault('BUSY','Use session stop for an active claim')
        elif op=='calibration.start':
            robot=self._robot(p,a);resource='robot:'+robot['robot_id']
            if p.get('config_revision',robot['revision'])!=robot['revision']:raise Fault('CONFIG_REVISION_MISMATCH','config_revision differs from registered robot_revision')
            p={**p,'config_revision':robot['revision']}
        elif op.startswith('calibration.'):
            parent=self.store.job(p['parent_operation_id'],a)
            if parent['kind']!='calibration.start' or parent['project']!=project or parent.get('result',{}).get('calibration_id')!=p.get('calibration_id'):raise Fault('INVALID_SESSION','Calibration operation binding differs')
        elif op in ('collections.start','verifications.start','scene.reset'):
            robot=self._robot(p,a);resource='robot:'+robot['robot_id']
            p=bind_robot_fields(robot,p)
            if op!='scene.reset':
                SessionRuntime.validate_mode(self.sessions,robot,p['mode'])
                if not isinstance(p.get('name','verification'),str) or not p.get('name','verification').strip():raise Fault('INVALID_NAME','Task name required')
                if p.get('calibration_revision') is None:raise Fault('CALIBRATION_REQUIRED','Explicit calibration revision required')
                if p['calibration_revision']!=robot.get('calibration_revision','baseline'):raise Fault('CALIBRATION_MISMATCH','Activate a new immutable robot/peer revision before using a different calibration')
                if robot['kind']=='physical' and p.get('workspace_confirmed') is not True:raise Fault('WORKSPACE_CONFIRMATION_REQUIRED','Explicit physical workspace confirmation is required; native consent is also checked')
                if p['mode'].endswith('_task'):
                    task=self.store.get('tasks',p['task_id'],p['task_revision'],a)
                    if task['project']!=project or task['body']['robot_revision']!=robot['revision'] or task['body']['robot_id']!=robot['robot_id'] or task['body'].get('calibration_hash',robot['sdk_config']['calibration_hash'])!=robot['sdk_config']['calibration_hash']:raise Fault('TASK_VERSION_MISMATCH','Task robot/project/calibration revision differs')
                    p={**p,'task':task['body'],'task_hash':task['hash']}
            elif robot['kind']!='simulation':raise Fault('WRONG_TARGET','Reset only applies to simulation')
        elif op in ('collections.save','collections.redo','collections.next','collections.stop','verifications.stop'):
            parent=self.store.job(p['session_id'],a)
            if parent['project']!=project:raise Fault('FORBIDDEN','Session project differs',403)
            if parent['kind'] not in ('collections.start','verifications.start'):raise Fault('INVALID_SESSION','Invalid parent session')
            if op in ('collections.save','collections.redo') and p.get('episode_id')!=self.sessions.status(p['session_id']).get('episode_id'):raise Fault('STALE_EPISODE','Current episode_id required')
        elif op in ('datasets.convert','datasets.upload','datasets.grant'):
            if op!='datasets.grant' and self.role!='collection':raise Fault('WRONG_SERVICE','Conversion/upload belongs to collection container')
            value=self.store.get('datasets',p['dataset_id'],p['version'],a)
            if value['project']!=project:raise Fault('FORBIDDEN','Source project differs',403)
            if op=='datasets.convert':
                from .conversion import validate_output_scope
                owner,output_project=validate_output_scope(p,a)
                p={**p,'output_owner':owner,'output_project':output_project}
        elif op=='training.start':
            from .training import validate_training
            p=validate_training(self,p,a)
        elif op=='inference.start':
            robot=self._robot(p,a)
            if robot['kind']!='simulation':raise Fault('WRONG_TARGET','Training container only verifies simulation policies')
            p=bind_robot_fields(robot,p)
            model=p['model'];entry=self.store.get('models',model['model_id'],model['version'],a)
            if entry['project']!=project or entry['body']['content_hash']!=model['content_hash']:raise Fault('MODEL_HASH_MISMATCH','Model project/hash differs')
            from lerobot_sdk import RunConfig
            RunConfig(**p.get('parameters',{}))
            if not isinstance(p.get('task'),str) or not p['task'].strip():raise Fault('INVALID_TASK','Task text required')
            resource='robot:'+robot['robot_id'];p={**p,'resolved_robot':robot,'resolved_model':entry['body']}
        else:raise Fault('UNSUPPORTED_OPERATION','Operation is not registered')
        j,fresh=self.store.create_job(op,p,request_id,a,project,resource=resource,request_payload=original)
        if not fresh:return operation_fields(j)
        key=j['operation_id'];cancel=threading.Event();self.stop_flags[key]=cancel
        def work():
            self.store.update_job(key,'RUNNING');self.store.event(key,'started',{'operation':op})
            try:
                if op=='robots.reconcile':
                    if self.collection is not None and self.collection.owns_session(parent['operation_id']):
                        result=self.collection.stop(parent['operation_id'],{'project':project,'workspace_confirmed':p.get('workspace_confirmed')},'reconcile-'+key,a)
                    else:
                        result=self.sessions.reconcile(robot,read_only=parent['kind']=='calibration.start');self.store.release('robot:'+robot['robot_id'],parent['operation_id'],holding=result['safe_to_release'])
                elif op=='calibration.start':result={**self.calibrations.start(robot,p['role'],a,project),'config_revision':p['config_revision']}
                elif op.startswith('calibration.'):
                    action=op.split('.')[1];cid=p['calibration_id']
                    if action=='sample':result=self.calibrations.sample(cid,p['point'],a,p.get('motor_id'))
                    elif action=='finish':result=self.calibrations.finish(cid,a)
                    elif action=='cancel':result=self.calibrations.cancel(cid,a)
                    else:raise Fault('UNSUPPORTED_OPERATION','Unknown calibration operation')
                    if action in ('finish','cancel'):self.store.release('robot:'+parent['payload']['robot_id'],parent['operation_id'],holding=True)
                elif op in ('collections.start','verifications.start'):
                    result=self.sessions.start(key,robot,p,verification=op.startswith('verifications'))
                    result['session_id']=key
                    self.store.register('sessions',key,'v1',project,a,{'robot_id':robot['robot_id'],'robot_revision':robot['revision'],'mode':p['mode'],'source':robot.get('provenance',robot['kind']),'calibration_revision':p['calibration_revision'],'task_hash':p.get('task_hash'),
                        'session_id':key,'dataset_id':key if op=='collections.start' else None,'config_revision':p['config_revision'],
                        'scene_id':p.get('scene_id'),'scene_revision':p.get('scene_revision'),'ros_config':p['ros_config']})
                elif parent:
                    result=self.sessions.command(p['session_id'],op.split('.')[1],p)
                    if op.endswith('.stop'):
                        if result.get('dataset_root'):
                            registered=self.datasets.register_local(result['dataset_root'],result['content_hash'],a,project,[self.root/'capture'])
                            result['dataset']=registered;result['upload_state']='NOT_REQUESTED';result['business_complete']=False
                        holding=result.get('holding') is True and result.get('action_queue_empty') is True
                        self.store.release('robot:'+parent['payload']['robot_id'],parent['operation_id'],holding=holding)
                        sink=p.get('sink_id',self.config.get('default_upload_sink'))
                        if sink and result.get('dataset'):
                            from .transfers import push
                            data=result['dataset']['body']
                            try:result.update(push(self,{'project':project,'dataset_id':data['dataset_id'],'version':data['version'],'content_hash':data['content_hash'],'sink_id':sink},a,key))
                            except Exception as e:result.update(upload_state='PENDING_RETRY',business_complete=False,upload_verified=False,upload_error=str(e))
                        self.store.update_job(parent['operation_id'],'CLOSED' if holding else 'STOP_UNCONFIRMED',result=result)
                elif op=='scene.reset':
                    result=self.sessions.reset(robot);self.store.release(resource,key,holding=result.get('holding') is True)
                elif op=='datasets.convert':
                    from .conversion import convert
                    result=convert(self,p,a,operation_id=key)
                elif op=='datasets.upload':
                    from .transfers import push
                    result=push(self,p,a,key)
                elif op=='datasets.grant':result=self.store.grant('datasets',p['dataset_id'],p['version'],a,p['subject'])
                elif op=='inference.start':
                    from .inference import run_inference
                    with self.compute:
                        if cancel.is_set():raise Fault('CANCELLED','Cancelled while queued')
                        result=run_inference(self,key,p,a,cancel)
                    self.store.release(resource,key,holding=result.get('holding') is True and result.get('action_queue_empty') is True)
                else:
                    from .training import run_training
                    with self.compute:
                        if cancel.is_set():raise Fault('CANCELLED','Cancelled while queued')
                        result=run_training(self,key,p,a,cancel)
                terminal='CANCELLED' if isinstance(result,dict) and result.get('state')=='CANCELLED' else 'SUCCEEDED'
                self.store.update_job(key,terminal,result=result);self.store.event(key,'completed',result)
            except BaseException as exc:
                error={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc)}
                if getattr(exc,'details',None):error['details']=exc.details
                self.store.update_job(key,'CANCELLED' if error['code']=='CANCELLED' else 'FAILED',error=error);self.store.event(key,'failed',error)
                # A failed start may have reached hardware; do not silently drop
                # its claim. Explicit reconciliation is required before reuse.
                if resource:
                    never_or_no_longer_owned=op in ('collections.start','verifications.start') and hasattr(self.sessions,'live') and key not in self.sessions.live
                    self.store.release(resource,key,holding=never_or_no_longer_owned)
            finally:self.stop_flags.pop(key,None)
        self.pool.submit(work);return operation_fields(j)
    def cancel(self,key,a):
        job=self.store.job(key,a)
        if job['kind'] in ('collections.start','verifications.start'):
            if job['kind']=='collections.start' and self.config.get('collection_contract_only'):
                return self.get_collection().stop(key,{'project':job['project']},'cancel-'+key,a)
            if hasattr(self.sessions,'cancel_start'):self.sessions.cancel_start(key)
            payload={'project':job['project'],'session_id':key}
            if job['kind']=='collections.start':payload['save_current']=True
            return self.submit('collections.stop' if job['kind']=='collections.start' else 'verifications.stop',payload,'stop-'+key,a)
        flag=self.stop_flags.get(key)
        if flag:flag.set();self.store.update_job(key,'CANCELLING')
        return self.operation(key,a)
    def metrics(self,key,a,after=0):
        self.store.job(key,a);rows=[];path=self.root/'jobs'/key/'attempt/events.jsonl'
        if path.exists():
            for line in path.read_text(encoding='utf-8').splitlines():
                try:v=json.loads(line)
                except ValueError:continue
                if v.get('kind')=='training_step' and v['detail'].get('step',0)>after:rows.append(v['detail'])
        return {'training_run_id':key,'items':rows[:1000],'next_step':rows[min(999,len(rows)-1)]['step'] if rows else after}
    def close(self):
        with self.lock:
            cancel_all=getattr(self.sessions,'cancel_all_starts',None)
            if callable(cancel_all):cancel_all()
            self.closing=True
            for key,flag in list(self.stop_flags.items()):
                flag.set()
                if hasattr(self.sessions,'cancel_start'):self.sessions.cancel_start(key)
        if self.current_collection is not None:self.current_collection.close()
        if self.minimal_collection is not None:self.minimal_collection.close()
        if self.collection is not None:self.collection.close()
        self.pool.shutdown(wait=True)
        errors=self.sessions.close()
        for key in list(self.calibrations.readers):
            try:self.calibrations._close(key)
            except Exception as e:errors.append('calibration '+key+': '+str(e))
        (self.root/'shutdown.json').write_text(json.dumps({'hold_errors':errors,'all_owned_stops_confirmed':not errors},indent=2),encoding='utf-8')
        self.store.close()
