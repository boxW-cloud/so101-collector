"""Four collection modes share SDK capture/stop, with explicit source identity."""
import threading,time,json,uuid
from pathlib import Path
from dataclasses import asdict
from .store import Fault
from .direct_follower import LOCAL_SOURCES,is_direct
# 2026-10-29：本机数据源（直连真实从臂、Redis 采集队列）在停止和核对时按同一类处理，和 is_direct 一致
from .session_fields import session_binding, session_fields, public_metadata, trusted_runtime_provenance, trusted_frame_provenance

MODES=('physical_teleop','physical_task','simulation_teleop','simulation_task')

def source_metadata(workspace,parameters):
    (workspace/'meta/deployment-source.json').write_text(json.dumps(public_metadata(parameters),ensure_ascii=False,indent=2),encoding='utf-8')
    return {}

class TaskLeader:
    is_connected=True
    resource_id='versioned-task-sequence'
    def __init__(self,config,task):
        from lerobot_sdk import JointTarget
        self.config=config;self.steps=[];total=0
        if not isinstance(task.get('steps'),list) or not 1<=len(task['steps'])<=1000:raise Fault('INVALID_TASK','1..1000 frozen steps required')
        for s in task['steps']:
            duration=s['duration_s']
            if type(duration) not in (int,float) or not .01<=duration<=300:raise Fault('INVALID_TASK','Invalid step duration')
            target=JointTarget(s['positions'],config.units,config.calibration_hash)
            if any(not config.limits[k][0]<=v<=config.limits[k][1] for k,v in target.positions.items()):raise Fault('LIMIT','Task target exceeds registered limits')
            total+=duration;self.steps.append((total,target))
        self.restart()
    def restart(self):self.started=time.monotonic()
    @property
    def completed(self):return time.monotonic()-self.started>=self.steps[-1][0]
    def get_action(self,*,timeout_s):
        elapsed=time.monotonic()-self.started
        return next((target for end,target in self.steps if elapsed<end),self.steps[-1][1])

class RemoteLeader:
    is_connected=True
    def __init__(self,backend,config,*,runtime_provenance=None,require_provenance=False):
        self.backend=backend;self.config=config;self.resource_id=backend.resource_id+':leader';self._used_provenance=None
        from lerobot_sdk.core.provenance import bounded_metadata
        self.runtime_provenance=bounded_metadata(runtime_provenance);self.require_provenance=require_provenance
    def get_action(self,*,timeout_s):
        from lerobot_sdk import JointTarget
        v=self.backend.leader(timeout_s)
        from lerobot_sdk.core.provenance import bounded_metadata
        if self.require_provenance:
            self._used_provenance=trusted_frame_provenance(v.get('provenance'),self.runtime_provenance)
        else:self._used_provenance=bounded_metadata(public_metadata(v['provenance'])) if v.get('provenance') is not None else None
        return JointTarget(v['positions'],self.config.units,self.config.calibration_hash)
    def frame_provenance(self):
        from lerobot_sdk.core.provenance import bounded_metadata
        return bounded_metadata(self._used_provenance)

class SessionRuntime:
    def __init__(self,root,backend_factory):self.root=Path(root);self.factory=backend_factory;self.live={};self.lock=threading.RLock();self.cancelled_starts=set();self.closing=False
    def cancel_all_starts(self):
        with self.lock:
            self.closing=True
            for key,value in self.live.items():
                self.cancelled_starts.add(key);value['cancelled']=True
                if value.get('task') and not value['verification']:
                    try:value['task'].suspend()  # Immediate stop intent; do not wait under admission lock.
                    except Exception:pass  # Subsequent explicit stop/reconciliation retains responsibility.
    def cancel_start(self,key):
        with self.lock:
            self.cancelled_starts.add(key)
            if key in self.live:self.live[key]['cancelled']=True
    def validate_mode(self,robot,mode):
        if mode not in MODES:raise Fault('INVALID_MODE','Unknown collection mode')
        if mode.split('_')[0]!=robot['kind']:raise Fault('WRONG_TARGET','Mode kind differs from registered robot kind')
        if robot['kind']=='physical' and robot.get('commissioned') is not True:raise Fault('NOT_COMMISSIONED','Physical target commissioning is required')
    def start(self,key,robot,payload,*,verification=False):
        from lerobot_sdk import RobotSession,RobotConfig,CollectionManager,CollectionConfig
        from collection_service.verification import VerificationSession
        self.validate_mode(robot,payload['mode'])
        if not verification:
            # Heavy native imports must finish before acquiring a robot. Starting
            # them inside the recording worker can exceed its startup deadline.
            from lerobot_sdk.adapters.seeed_dataset import upstream
            upstream()
        if verification and payload['mode'].endswith('_task'):raise Fault('INVALID_MODE','Teleoperation verification requires a teleop mode')
        cfg=RobotConfig(**robot['sdk_config'])
        with self.lock:
            if self.closing or key in self.cancelled_starts:raise Fault('STOPPED','Cancelled before acquiring device')
        backend=self.factory(robot)
        if backend.kind!=robot['kind']:raise Fault('WRONG_TARGET','Backend identity differs from registered target')
        session=RobotSession(cfg,backend,lock_dir=self.root/'locks',physical_calibration_hash=cfg.calibration_hash if robot['kind']=='physical' else None)
        with self.lock:
            if key in self.live:raise Fault('BUSY','Session already exists')
            self.live[key]={'robot':session,'backend':backend,'lock':threading.RLock(),'starting':True,'startup_finished':False,'cancelled':False,
                            'verification':verification,'mode':payload['mode'],'source':robot.get('provenance',robot['kind']),
                            'binding':session_binding(robot,payload)}
        try:
            with self.lock:
                if self.closing or key in self.cancelled_starts:raise Fault('STOPPED','Cancelled before device connection')
                session.connect()
            if robot['kind']=='physical' and payload['mode']=='physical_teleop' and not is_direct(robot):
                starter=getattr(backend,'start_teleop',None)
                if not callable(starter):raise Fault('TELEOP_UNAVAILABLE','Physical ROS peer must support managed teleoperation startup')
                started=starter(min(8,max(5,cfg.io_timeout_s)))
                if started.get('started') is not True:raise Fault('TELEOP_UNAVAILABLE','Physical input startup not confirmed')
                proof=trusted_runtime_provenance(started,robot)
                if proof is not None:self.live[key]['binding']['runtime_provenance']=proof
            proof=self.live[key]['binding'].get('runtime_provenance')
            from .redis_capture import CurrentStateLeader,is_redis_capture
            if payload['mode'].endswith('_task'):leader=TaskLeader(cfg,payload['task'])
            # Redis 数据源：SDK 流程里的"主臂"目标用当前实测状态占位（什么都不下发），直接取设备会话最近的读数，不再每帧发 leader 请求。
            # 数据集的 action 另按登记的 dataset_schema 规则写（v10r13 起 Bobac3 是下一帧的实测状态，见 CollectionTask._frame）
            elif is_redis_capture(robot):leader=CurrentStateLeader(session)
            else:leader=RemoteLeader(backend,cfg,runtime_provenance=proof,require_provenance=robot.get('require_runtime_provenance') is True)
            # v10r15：不检查主从相差、不做对齐移动。对齐由操作者自己负责，平台不管。
            with self.live[key]['lock']:
                if self.live[key]['cancelled'] or key in self.cancelled_starts:raise Fault('STOPPED','Start cancelled before activation')
                if verification:
                    task=VerificationSession(session,leader,fps=payload.get('fps',5),duration_s=payload.get('duration_s',600));task.start()
                else:
                    task=CollectionManager().open_task(self.root/'tasks'/key) if payload.get('_resume') else CollectionManager().create_task(CollectionConfig(name=key,description=payload.get('description',''),root=self.root/'tasks',
                        fps=payload.get('fps',5),task=payload['name'],robot_id=cfg.robot_id,calibration_hash=cfg.calibration_hash,units=cfg.units,
                        camera_shapes={k:tuple(v) for k,v in robot['camera_shapes'].items()},max_duration_s=1800,leader_timeout_s=cfg.io_timeout_s,
                        dataset_schema=robot.get('dataset_schema')))
                    self.live[key]['task']=task
                    if proof is not None:task._bind_source_metadata(proof)
                    task.start(session,leader).wait(120)
                self.live[key].update(starting=False,task=task,leader=leader,verification=verification,source=robot.get('provenance',robot['kind']),mode=payload['mode'],
                    episode_id=uuid.uuid4().hex)
            return self.status(key)
        except BaseException as original:
            entry=self.live.get(key,{})
            # A sensor fault stops the SDK and cancels an in-flight leader RPC.
            # Preserve that original control fault before cleanup changes state;
            # STOPPED from the cancelled RPC is only a secondary symptom.
            failure=original
            entry['startup_error']=str(original)
            task=entry.get('task')
            if task:
                try:task.suspend().wait(30)
                except Exception:pass
            # If HOLD/close cannot be confirmed, retain the live object and lock
            # so a stop/reconciliation can retry without giving another owner control.
            cleanup_error=None
            try:
                if session.status().state.value!='DISCONNECTED':session.stop('startup failed');session.close()
                else:backend.close(hold=True,timeout_s=cfg.io_timeout_s)
                self.live.pop(key,None)
            except BaseException as cleanup:cleanup_error=str(cleanup)
            if cleanup_error:
                raise Fault(getattr(failure,'code',type(failure).__name__),str(failure)+'; cleanup not confirmed: '+cleanup_error) from original
            if failure is not original:raise failure from original
            raise
        finally:
            with self.lock:
                entry=self.live.get(key)
                if entry is not None and entry.get('robot') is session:entry['startup_finished']=True
    def status(self,key):
        if key not in self.live:raise Fault('NOT_ACTIVE','No active session; inspect operation history')
        v=self.live[key]
        fields=session_fields(key,v)
        if v['starting']:return {**fields,'state':v.get('phase','STARTING')}
        status=v['task'].status()
        if getattr(status,'error',None) and not v.get('fault_logged'):
            # 诊断（2026-10-29）：录制出错时记一次设备会话自己的状态，原始故障在这里（录制器记的常是被取消的后续请求）
            v['fault_logged']=True
            import logging
            logging.getLogger(__name__).warning('collection %s recorder error=%s robot=%s',key,status.error,v['robot'].status())
        result=dict(status) if v['verification'] else asdict(status)
        if hasattr(result.get('state'),'value'):result['state']=result['state'].value
        return {**result,**fields,'episode_id':v['episode_id'],
                'task_sequence_exhausted':getattr(v['leader'],'completed',False),'task_success_verified':False}
    def command(self,key,operation,payload):
        from collection_service.contracts import episode_result
        v=self.live.get(key)
        if not v:raise Fault('NOT_ACTIVE','Session not active')
        # Immediate stop intent is independent of waiting for encoder/command lock.
        pending_stop=None
        if operation=='stop':
            self.cancel_start(key)
            if not v['starting'] and not v['verification']:
                # Mark recorder stop first. Calling robot.stop before this can
                # cancel its in-flight servo and turn a valid episode RECOVERABLE.
                pending_stop=v['task'].stop(payload.get('save_current',True))
            elif v['starting']:v['robot'].stop('cancel startup')
        with v['lock']:
            if v['starting']:raise Fault('STARTING','Wait for start cancellation to finish')
            task=v['task']
            if v['verification']:
                if operation!='stop':raise Fault('INVALID_OPERATION','Verification does not record data')
                result=task.stop();fields=session_fields(key,v);v['robot'].close();self.live.pop(key,None)
                return {**result,**fields,'committed_episodes':0,'saved_episodes':0,'manifest_hash':None,'seal_state':'NOT_APPLICABLE'}
            if operation=='save':return {**episode_result(task,task.save_episode().wait(180)),**session_fields(key,v)}
            if operation=='redo':
                if payload.get('confirm') is not True:raise Fault('CONFIRM_REQUIRED','Confirm discarding unsaved episode')
                task.redo_episode().wait(120);return self.status(key)
            if operation=='next':
                if self.closing or v['cancelled'] or key in self.cancelled_starts:raise Fault('STOPPED','Next recording cancelled')
                if isinstance(v['leader'],TaskLeader):v['leader'].restart()
                if self.closing or v['cancelled'] or key in self.cancelled_starts:raise Fault('STOPPED','Next recording cancelled')
                task.start_episode().wait(120);v['episode_id']=uuid.uuid4().hex;return self.status(key)
            if operation!='stop':raise Fault('INVALID_OPERATION','Unknown session command')
            try:ref=(pending_stop or task.stop(payload.get('save_current',True))).wait(300)
            except Exception:
                if task.status().committed_episodes or payload.get('save_current',True) and task.status().current_frames:raise
                stop=v['robot'].stop('empty collection closed');v['robot'].close();self.live.pop(key,None)
                return {**session_fields(key,v),'state':'CLOSED','holding':stop.holding,'action_queue_empty':True,
                        'committed_episodes':0,'saved_episodes':0,'manifest_hash':None,'seal_state':'CLOSED'}
            if ref.dataset_id!=key:raise Fault('DATASET_ID_MISMATCH','SDK sealed dataset differs from the preallocated collection dataset_id')
            from lerobot_sdk import DatasetManager
            saved=task.status().committed_episodes
            ref=DatasetManager().migrate(ref,self.root/'sealed'/key,source_metadata,parameters=public_metadata({'mode':v['mode'],'source':v['source'],**v['binding']})).wait(300)
            if ref.dataset_id!=key:raise Fault('DATASET_ID_MISMATCH','Migrated dataset differs from the preallocated collection dataset_id')
            stop=v['robot'].stop('sealed');v['robot'].close();self.live.pop(key,None)
            return {**session_fields(key,v),'state':'LOCAL_READY','dataset_root':str(ref.root),'version':ref.version,
                    'content_hash':ref.content_hash,'manifest_hash':ref.content_hash,'seal_state':'LOCAL_READY',
                    'committed_episodes':saved,'saved_episodes':saved,'holding':stop.holding,'action_queue_empty':True}
    def reset(self,robot):
        if robot['kind']!='simulation':raise Fault('WRONG_TARGET','Scene reset is simulation-only')
        backend=self.factory(robot)
        try:
            backend.connect(5);result=backend.reset(20)
            if not result.get('holding') or not result.get('scene_reset'):raise Fault('RESET_UNCONFIRMED','Peer did not confirm complete scene reset')
            return result
        finally:backend.close(hold=True,timeout_s=5)
    def capture_checkpoint(self,key):
        v=self.live.get(key)
        if not v or v['verification']:raise Fault('NOT_ACTIVE','No recording task')
        return {'task_directory':str(v['task'].directory),'commits':v['task'].frozen_commits()}
    def set_episode_id(self,key,value):
        v=self.live.get(key)
        if not v:raise Fault('NOT_ACTIVE','No active recording')
        v['episode_id']=value
    def suspend_capture(self,key,*,failed_before_recording=False,robot=None):
        v=self.live.get(key)
        if not v:raise Fault('RECONCILE_REQUIRED','Recording process absent; reconcile preserved journal and target stop')
        self.cancel_start(key)
        with v['lock']:
            if v['starting']:
                if (failed_before_recording is not True or v.get('startup_finished') is not True or
                    not v.get('startup_error') or v.get('task') is not None or robot is None):
                    raise Fault('STARTING','Startup cancellation not yet confirmed')
                directory=self.root/'tasks'/key
                if directory.exists() or directory.is_symlink():raise Fault('RECONCILE_REQUIRED','Preserve and reconcile the existing recorder journal')
                # The contract has already checked the terminal durable start
                # and absent files. Stop through the existing owner; no rebind,
                # target replay, fault reset or torque release is permitted.
                if not v.get('failed_start_cleanup_closed'):
                    if not v.get('stopped_target_recheck'):
                        stopped=v['robot'].stop('failed start stopped before recorder creation')
                        if stopped.holding is not True:raise Fault('STOP_UNCONFIRMED','Failed-start HOLD is not confirmed')
                        if v['mode']=='physical_teleop' and v['source'] not in LOCAL_SOURCES:
                            source_stop=v['backend'].stop_teleop(v['robot'].config.io_timeout_s)
                            if source_stop.get('stopped') is not True:raise Fault('STOP_UNCONFIRMED','Physical input stop not confirmed')
                    # A failed R08 CLOSE has a retained cleanup worker/lease;
                    # retry its normal close, never connect or begin again.
                    v['robot'].close();v['failed_start_cleanup_closed']=True
                proof=self.reconcile(robot)
                self.live.pop(key,None)
                return {**proof,'preserved_current_frames':0,'recording_never_started':True,'state':'STOPPED'}
            cleanup=v.get('capture_cleanup')
            if cleanup is not None:return self._close_preserved_capture(key,v,cleanup)
            task=v['task'];frames=task.status().current_frames
            preservation=task.suspend()
            try:preservation.wait(120)
            except Exception:
                from lerobot_sdk import CollectionManager,CollectionState
                from lerobot_sdk.core.operation import OperationState
                worker=getattr(task,'_thread',None);status=task.status()
                # An input failure may terminate the recorder while its suspend
                # is pending. Its failed Operation stays failed; only re-open a
                # terminated RECOVERABLE journal, never bypass a live writer or
                # a wait timeout. Re-opening verifies the recorder lock and every
                # committed/unsubmitted frame hash before any independent HOLD.
                if (preservation.status().state!=OperationState.FAILED or
                    status.state!=CollectionState.RECOVERABLE or worker is None or worker.is_alive()):raise
                frames=status.current_frames
                directory=Path(task.directory);expected=self.root/'tasks'/key
                if directory.is_symlink() or directory.resolve()!=expected.resolve():
                    raise
                restored=CollectionManager().open_task(directory)
                rechecked=restored.status()
                if (rechecked.task_id!=status.task_id or rechecked.state!=CollectionState.RECOVERABLE or
                    rechecked.current_frames!=frames or restored.frozen_commits()!=task.frozen_commits() or
                    restored._journal.get('current')!=task._journal.get('current') or
                    restored.config.to_dict()!=task.config.to_dict()):
                    raise Fault('RECONCILE_REQUIRED','Preserved recorder changed before stop revalidation')
                restored.suspend().wait(10)
                v['task']=restored
            frames=v['task'].status().current_frames
            stop=v['robot'].stop('collection stopped; unsubmitted data preserved')
            if stop.holding is not True:raise Fault('STOP_UNCONFIRMED','Preserved collection HOLD is not confirmed')
            if v['mode']=='physical_teleop' and v['source'] not in LOCAL_SOURCES:
                source_stop=v['backend'].stop_teleop(v['robot'].config.io_timeout_s)
                if source_stop.get('stopped') is not True:raise Fault('STOP_UNCONFIRMED','Physical input stop not confirmed')
            # Persist only in this live entry after actual target/input stop.
            # CLOSE may terminate its I/O worker and then fail adapter cleanup;
            # retry the same close, not a stop that now requires that dead worker.
            cleanup={'phase':'HOLD_CONFIRMED','robot':v['robot'],'backend':v['backend'],
                     'task':v['task'],'mode':v['mode'],'epoch':stop.epoch,'stop':stop,
                     'result':{'holding':True,'action_queue_empty':True,'preserved_current_frames':frames,'state':'STOPPED'}}
            v['capture_cleanup']=cleanup
            return self._close_preserved_capture(key,v,cleanup)
    def _close_preserved_capture(self,key,v,cleanup):
        status=v['robot'].status()
        recorder=v['task'].status();worker=getattr(v['task'],'_thread',None)
        if (self.live.get(key) is not v or cleanup['robot'] is not v['robot'] or
            cleanup['backend'] is not v['backend'] or cleanup['task'] is not v['task'] or
            cleanup['mode']!=v['mode'] or cleanup['stop'].holding is not True or
            status.epoch!=cleanup['epoch'] or status.control_owner is not None or
            v.get('cancelled') is not True or key not in self.cancelled_starts or
            cleanup['phase']=='CLOSING' and (not v['robot']._exit.is_set() or
                worker is not None and worker.is_alive() or recorder.state.value!='RECOVERABLE' or
                recorder.current_frames!=cleanup['result']['preserved_current_frames'])):
            raise Fault('RECONCILE_REQUIRED','Preserved CLOSE receipt no longer belongs to this stopped session')
        cleanup['phase']='CLOSING'
        try:v['robot'].close()
        finally:cleanup['epoch']=v['robot'].status().epoch  # close has its own stop epoch.
        if v['robot'].status().state.value!='DISCONNECTED':
            raise Fault('STOP_UNCONFIRMED','Preserved collection CLOSE is not confirmed')
        self.live.pop(key,None)
        return dict(cleanup['result'])
    def resume_capture(self,key,robot,payload):
        with self.lock:self.cancelled_starts.discard(key)
        self.start(key,robot,{**payload,'_resume':True})
        v=self.live[key]
        with v['lock']:
            if v['task'].status().current_frames:v['task'].resume_episode().wait(120)
            else:v['task'].start_episode().wait(120)
            return self.status(key)
    def _verify_stopped_target(self,key,robot,*,simulation_end_only=False):
        """Same failed-session stop responsibility; no leader, servo or recorder."""
        from lerobot_sdk import RobotConfig,RobotSession
        cfg=RobotConfig(**robot['sdk_config']);backend=self.factory(robot)
        if backend.kind!=robot['kind'] or backend.kind!='physical' and not (simulation_end_only and backend.kind=='simulation'):
            raise Fault('WRONG_TARGET','Stopped-target recheck requires the registered backend and explicit simulation end intent')
        session=RobotSession(cfg,backend,lock_dir=self.root/'locks',physical_calibration_hash=cfg.calibration_hash)
        with self.lock:
            if self.closing or key in self.live:raise Fault('RECONCILE_REQUIRED','An existing process retains this failed session')
            mode=robot['kind']+'_teleop'
            entry={'robot':session,'backend':backend,'lock':threading.RLock(),'starting':True,'startup_finished':False,
                   'cancelled':True,'verification':False,'mode':mode,'source':robot.get('provenance',robot['kind']),
                   'binding':session_binding(robot,{'mode':mode}),'phase':'RECONCILING_FAILED_START',
                   'stopped_target_recheck':True}
            self.live[key]=entry
        try:
            # NativeRelative.connect only adopts fresh current identity. It
            # does not arm, begin teleoperation or send a position. The caller
            # must have established a real native HOLD before this check.
            session.connect()
            stopped=session.stop('R08 current stopped-target feedback and HOLD verification')
            if stopped.holding is not True:raise Fault('STOP_UNCONFIRMED','Actual stopped-target HOLD is not confirmed')
            session.close();entry['failed_start_cleanup_closed']=True
            self.live.pop(key,None)
        except BaseException as original:
            entry['startup_error']=str(original)
            try:
                session.close();entry['failed_start_cleanup_closed']=True
                self.live.pop(key,None)
            except BaseException as cleanup:
                raise Fault('STOP_UNCONFIRMED',str(original)+'; stopped-target cleanup unconfirmed: '+str(cleanup)) from original
            raise
        finally:
            with self.lock:
                if self.live.get(key) is entry:entry['startup_finished']=True

    def reconcile(self,robot,*,read_only=False,verify_stopped_target=False,session_id=None,simulation_end_only=False):
        from .ros_transport import TopicRPC
        from .ros_wire import request
        rpc=TopicRPC(robot['ros'])
        try:
            value=rpc.call(request('inspect',{},'reconcile-'+uuid.uuid4().hex,0,rpc.key),5)
            if not isinstance(value,dict) or not {'owner','fault','epoch','holding','action_queue_empty','teleop_active'}<=value.keys() or not value['epoch']:
                raise Fault('RECONCILE_REQUIRED','Peer stop/ownership proof is incomplete')
            if value.get('robot_id')!=robot['robot_id'] or value.get('calibration_hash')!=robot['sdk_config']['calibration_hash']:raise Fault('PEER_MISMATCH','Reconciliation target changed')
            safe=value.get('read_only_released') if read_only else value.get('holding')
            if (value.get('owner') is not None or value.get('fault') or
                value.get('action_queue_empty') is not True or value.get('teleop_active') is not False):
                raise Fault('RECONCILE_REQUIRED','Peer must confirm released ownership, stopped input and an empty queue')
            if robot['kind']=='physical':
                queue=value.get('native_queue_status')
                stamp=queue.get('sampled_at') if isinstance(queue,dict) else None
                if (value.get('source') not in ('physical_native_relative_teleop',*LOCAL_SOURCES) or value.get('queue_scope')!='peer_and_native_owner' or not isinstance(queue,dict) or
                    queue.get('action_queue_empty') is not True or type(stamp) not in (int,float) or
                    not all(queue.get(name) for name in ('session_id','device_id','profile_id'))):
                    raise Fault('RECONCILE_REQUIRED','Fresh native feedback and actual queue proof are required')
                # This signed physical source calls NativeOwnerAdapter.state,
                # which verifies freshness against the Windows owner's clock.
                # sampled_at remains that source-host Unix timestamp; comparing
                # it to the Spark wall clock is invalid and must not re-age it.
            if safe is not True:
                if not (verify_stopped_target is True and (robot['kind']=='physical' or robot['kind']=='simulation' and simulation_end_only) and not read_only and session_id):
                    raise Fault('RECONCILE_REQUIRED','Actual target holding is unknown')
                self._verify_stopped_target(session_id,robot,**({'simulation_end_only':True} if robot['kind']=='simulation' else {}))
                # A new peer starts with unknown HOLD. Prove it through the
                # current owner's normal stop/close path, then independently
                # inspect the released peer; never simply set holding=True.
                return self.reconcile(robot)
            return {'holding':value.get('holding') is True,'read_only_released':bool(read_only and safe),'safe_to_release':True,'action_queue_empty':True,'epoch':value['epoch'],
                    **({'native_queue_status':queue,'native_feedback_clock':'source_host_unix',
                        'native_freshness':'verified_by_local_direct_owner' if is_direct(robot) else 'verified_on_Windows_owner_before_signed_inspect'} if robot['kind']=='physical' else {})}
        finally:rpc.close()
    def close(self):
        errors=[]
        for key in list(self.live):
            try:
                if self.live[key]['verification']:self.command(key,'stop',{})
                else:self.suspend_capture(key)
            except Exception as e:errors.append(str(e))
        return errors
