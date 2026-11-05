"""Durable per-episode collection lifecycle; uploads never own the robot claim."""
import copy
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from .store import Fault, Principal, canonical, digest, ident


class CollectionContract:
    def __init__(self, platform, *, uploads=None, target_probe=None):
        self.platform=platform;self.runtime=platform.sessions;self.root=platform.root
        self.lock=threading.RLock();self.session_locks={};self.closed=False;self.closing=False
        self.active_requests=0;self.drained=threading.Condition(self.lock)
        self.finish_workers={};self.finish_exit=threading.Event()
        self.probe=target_probe or platform.probe
        # v10 结束采集后的后台同步（rsync + 通知平台）。配置文件此时可以不存在，推送时才读取。
        from .collection_sync import create_for_platform
        self.sync=create_for_platform(platform)
        self.db=sqlite3.connect(self.root/'collection-contract.sqlite',check_same_thread=False,isolation_level=None)
        self.db.row_factory=sqlite3.Row;self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('''
CREATE TABLE IF NOT EXISTS sessions(id TEXT PRIMARY KEY,owner TEXT,project TEXT,robot_id TEXT,body TEXT);
CREATE TABLE IF NOT EXISTS requests(owner TEXT,request_id TEXT,fingerprint TEXT,state TEXT,result TEXT,error TEXT,PRIMARY KEY(owner,request_id));
CREATE TABLE IF NOT EXISTS episodes(session_id TEXT,episode_id TEXT,generation INTEGER,body TEXT,PRIMARY KEY(session_id,episode_id,generation));
''')
        with self.lock:
            for pending in self.db.execute("SELECT * FROM requests WHERE state='PENDING'").fetchall():
                context=json.loads(pending['result']) if pending['result'] else {}
                prior=self.platform.store.db.execute('SELECT id FROM jobs WHERE owner=? AND request_id=?',(pending['owner'],pending['request_id'])).fetchone()
                if prior:context.setdefault('session_id',prior['id'])
                error={'code':'REQUEST_INTERRUPTED','message':'Request interrupted; inspect preserved same-session state','status':409,'details':context}
                self.db.execute("UPDATE requests SET state='FAILED',error=? WHERE owner=? AND request_id=?",(canonical(error),pending['owner'],pending['request_id']))
            for record in self.db.execute('SELECT * FROM sessions').fetchall():
                value=json.loads(record['body'])
                if value['internal_state'] not in ('STOPPED','START_FAILED'):
                    value['internal_state']='NEEDS_RECONCILE';self._write(value)
        if uploads is None:
            from .collection_uploads import DurableUploads
            uploads=DurableUploads(self.root/'collection-uploads',self._upload)
        self.uploads=uploads
        self.sync.start()
        self._restore_outbox()
        self.uploads.start()
        # A durable user end request survives restart without resuming motion.
        with self.lock:finishing=[json.loads(r['body']) for r in self.db.execute('SELECT body FROM sessions').fetchall()]
        for row in finishing:
            if row.get('finish',{}).get('state') not in (None,'QUEUED_UPLOAD','EMPTY','ABANDONED'):
                self._launch_finish(row['session_id'])

    def _restore_outbox(self):
        # SDK commit is a separate durable transaction. Recover a persisted
        # commit intent if a crash happened before the platform/outbox insert.
        with self.lock:sessions=[json.loads(r['body']) for r in self.db.execute('SELECT body FROM sessions').fetchall()]
        for row in sessions:
            if row.get('commit_intent'):
                try:self._repair_commit(row)
                except Exception as exc:
                    row.update(internal_state='NEEDS_RECONCILE',error=str(exc),recovery_alert=True);self._write(row)
                    self.platform.store.event(row['session_id'],'commit_recovery_failed',{'error':str(exc),'files_preserved':True})
                row=self._read(row['session_id'],Principal(row['operator'],(row['project'],),('operator',)))
            count=len(row['collection_episodes'])
            if row['episode_index']<count:
                # Crash after intent/outbox commit, before assigning the next
                # business identity. The submitted episode is never current.
                row.update(episode_index=count,episode_id=uuid.uuid4().hex,recording_generation=1,internal_state='NEEDS_RECONCILE')
                self._write(row)
            elif row['episode_index']>count:
                raise Fault('JOURNAL_CONFLICT','Current episode index exceeds committed identities')
        with self.lock:rows=self.db.execute('SELECT body FROM episodes').fetchall()
        for record in rows:
            episode=json.loads(record['body'])
            if episode.get('delivery_deferred'):
                owner=Principal(episode['operator'],(episode['project'],),('operator',))
                row=self._read(episode['session_id'],owner)
                # Before finish the immutable local commits must stay local.
                # A partially finalized batch is resumed by its finish worker.
                if row.get('finish',{}).get('state')!='QUEUED_UPLOAD':continue
                self._enqueue_episode(episode,row)
            else:self.uploads.enqueue(episode['upload_operation'],episode['upload_payload'])

    def _repair_commit(self,row):
        from lerobot_sdk import CollectionManager
        directory=self.root/'capture/tasks'/row['session_id']
        if not directory.is_dir():return
        task=CollectionManager().open_task(directory);commits=task.frozen_commits()
        count=len(row['collection_episodes'])
        if len(commits)==count:return  # No SDK commit happened; current data remain preserved.
        if len(commits)!=count+1:raise Fault('JOURNAL_CONFLICT','SDK/platform commit counts differ; no automatic reinterpretation')
        stopped=row.get('commit_intent',{}).get('stopped') is True
        identity=self._queue_commit(row,{'task_directory':str(directory),'commits':commits},commits[-1])
        row.update(episode_index=count+1,episode_id=uuid.uuid4().hex,recording_generation=1,internal_state='STOPPED' if stopped else 'NEEDS_RECONCILE')
        if stopped:row['stop_result'].update(preserved_current_frames=0,committed_episodes=count+1)
        row.pop('commit_intent',None);self._write(row)
        self.platform.store.event(row['session_id'],'committed_episode_recovered',identity)

    def _upload(self, job):
        from .collection_snapshots import prepare_snapshot
        from .transfers import push
        p=job['payload'];a=Principal(p['operator'],(p['project'],),('operator','trainer'))
        if job.get('manifest'):
            root=Path(job['prepared_root']);manifest=job['manifest']
        else:
            prepared=prepare_snapshot(p['task_directory'],p['commits'],p,self.root/'collection-snapshots',
                                      trusted_task_root=self.root/'capture/tasks')
            root=Path(prepared['dataset_root']);manifest=prepared['manifest']
            self.uploads.bind_manifest(job['operation_id'],manifest,prepared_root=str(root))
        registered=self.platform.datasets.register_local(root,manifest['content_hash'],a,p['project'],[self.root/'collection-snapshots'])
        request={'project':p['project'],'dataset_id':registered['id'],'version':registered['revision'],
                 'content_hash':manifest['content_hash'],'sink_id':p['sink_id']}
        from .collection_sync import SYNC_PROTOCOL
        if (p.get('destination') or {}).get('protocol')==SYNC_PROTOCOL:
            # v10：交给同步队列（推到 upload_url 或 <部署根目录>/action-composer/data/artifacts/data_set、逐文件校验、写完成标记）。
            # 首次登记做一次完整校验，之后每次只是查队列状态；没 READY 前同步队列自己一直重试。
            state=self.sync.submit(p['session_id'],root,p['destination'].get('upload_url'))
            if state['state']!='READY':
                from .collection_uploads import UploadPending
                raise UploadPending()
            result={'upload_verified':True,'business_complete':True,'content_hash':manifest['content_hash'],
                'target_dataset_id':manifest['dataset_id'],'target_version':manifest['version'],
                'storage_ref':(state['result'] or {}).get('target'),'delivery_kind':'sync',
                'ready_file':(state['result'] or {}).get('ready_file')}
        elif 'destination' in p:
            from .collection_destinations import push_destination
            result=push_destination(self.platform,request,a,job['operation_id'],p['destination'])
        else:result=push(self.platform,request,a,job['operation_id'])
        # Receipt and original manifest are persisted by the independent outbox.
        return {**result,'session_id':p['session_id'],'episode_id':p['episode_id'],
                'recording_generation':p['recording_generation'],'upload_operation':job['operation_id'],
                'owner':a.name,'project':p['project']}

    def _write(self, value):
        with self.lock:self.db.execute('INSERT OR REPLACE INTO sessions VALUES(?,?,?,?,?)',
            (value['session_id'],value['operator'],value['project'],value['robot_id'],canonical(value)))

    def _read(self, key, a, project=None):
        ident(key)
        with self.lock:record=self.db.execute('SELECT body FROM sessions WHERE id=?',(key,)).fetchone()
        if not record:raise Fault('NOT_FOUND','Collection session not found',404)
        value=json.loads(record['body']);a.require(value['project'],'operator')
        if value['operator']!=a.name or project is not None and project!=value['project']:raise Fault('FORBIDDEN','Session owner/project differs',403)
        return value

    def _serial(self,key):
        with self.lock:return self.session_locks.setdefault(key,threading.RLock())
    def owns_session(self,key):
        with self.lock:return self.db.execute('SELECT 1 FROM sessions WHERE id=?',(key,)).fetchone() is not None
    def dataset_fields(self,value):
        binding=value.get('body',{}).get('manifest',{}).get('collection_binding',{})
        operation=binding.get('upload_operation')
        if not operation:return value
        status=self.uploads.get(operation)
        from .collection_sync import SYNC_PROTOCOL
        payload=status.get('payload') or {}
        if (payload.get('destination') or {}).get('protocol')==SYNC_PROTOCOL:
            try:inner=self.sync.public_status(payload['session_id'])
            except KeyError:inner=None  # 快照还在封存，尚未进入同步队列
            return {**value,'upload_operation':operation,'delivery_kind':'sync',
                'upload_state':inner['state'] if inner else 'PREPARING',
                'upload_attempts':inner['attempts'] if inner else 0,
                'upload_error':inner['error'] if inner else status.get('error'),
                'upload_alert':bool(inner and inner['error']) or status.get('alert',False),
                'receiver_verified':bool(inner and inner['state']=='READY'),
                'sync_target':inner['sync_target'] if inner else None,
                'upload_url':payload['destination'].get('upload_url'),
                'sync_folder':inner['folder'] if inner else None,
                'ready_file':inner['ready_file'] if inner else None}
        receipt=status.get('receipt') or {}
        destination=(status.get('payload') or {}).get('destination') or {}
        delivery_kind=receipt.get('delivery_kind') or ('directory' if destination.get('protocol')=='so101.directory.v1' else 'http')
        return {**value,'upload_operation':operation,'upload_state':status['state'],
                'delivery_kind':delivery_kind,
                'upload_attempts':status.get('attempts',0),'upload_error':status.get('error'),
                'upload_alert':status.get('alert',False),'receiver_verified':status['state']=='READY',
                'center_receipt':status.get('receipt')}

    def _request(self,op,p,request_id,a,action,*,during_close=False):
        if self.closed or self.closing and not during_close:raise Fault('SHUTTING_DOWN','Collection service is closing',503)
        ident(request_id);a.require(p['project'],'operator')
        fingerprint=digest([op,p]);key=(a.name,request_id)
        with self.lock:
            if self.closed or self.closing and not during_close:raise Fault('SHUTTING_DOWN','Collection service is closing',503)
            record=self.db.execute('SELECT * FROM requests WHERE owner=? AND request_id=?',key).fetchone()
            if record:
                if record['fingerprint']!=fingerprint:raise Fault('IDEMPOTENCY_CONFLICT','request_id conflict')
                if record['state']=='SUCCEEDED':return json.loads(record['result'])
                if record['state']=='FAILED':
                    error=json.loads(record['error']);fault=Fault(error['code'],error['message'],error['status'])
                    if error.get('details'):fault.details=error['details']
                    raise fault
                raise Fault('REQUEST_IN_PROGRESS','Same request is still executing')
            context={k:p[k] for k in ('session_id','episode_id','recording_generation') if k in p}
            self.db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?)',(*key,fingerprint,'PENDING',canonical(context),None))
            self.active_requests+=1
        try:
            result=action()
            with self.lock:self.db.execute("UPDATE requests SET state='SUCCEEDED',result=? WHERE owner=? AND request_id=?",(canonical(result),*key))
            return result
        except BaseException as exc:
            error={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc),'status':getattr(exc,'status',409)}
            if getattr(exc,'details',None):error['details']=exc.details
            with self.lock:self.db.execute("UPDATE requests SET state='FAILED',error=? WHERE owner=? AND request_id=?",(canonical(error),*key))
            if isinstance(exc,Fault):raise
            raise Fault(error['code'],error['message'],error['status']) from exc
        finally:
            with self.drained:self.active_requests-=1;self.drained.notify_all()

    def _fields(self,p,required,optional=()):
        if not isinstance(p,dict) or set(p)-set(required)-set(optional) or set(required)-set(p):raise Fault('INVALID_FIELDS','Required fields: '+', '.join(required),400)

    def _minimal_row(self,key,p,a):
        row=self._read(key,a,p['project'])
        if row.get('api_contract')!='minimal.v9' or row.get('managed') is not True:
            raise Fault('PROTOCOL_MISMATCH','Session does not belong to minimal.v9')
        return row

    def minimal_start(self,p,request_id,a):
        return self.start(p,request_id,a,api_contract='minimal.v9',managed=True)

    def minimal_redo(self,key,episode_id,p,request_id,a):
        self._minimal_row(key,p,a)
        return self.redo(key,episode_id,p,request_id,a,rotate_episode_id=True)

    def minimal_complete(self,key,episode_id,p,request_id,a):
        self._minimal_row(key,p,a)
        return self.complete(key,episode_id,p,request_id,a)

    def minimal_finish(self,key,p,request_id,a):
        row=self._minimal_row(key,p,a)
        cancel=getattr(self.runtime,'cancel_start',None)
        if row['internal_state']!='STOPPED' and callable(cancel):cancel(key)
        return self.finish(key,p,request_id,a)

    def minimal_resume(self,key,p,request_id,a):
        self._minimal_row(key,p,a)
        return self.resume(key,p,request_id,a,rotate_episode_id=True,require_workspace_confirmation=False)

    def _current_row(self,key,p,a):
        row=self._read(key,a,p['project'])
        if row.get('api_contract')!='minimal.v10' or row.get('managed') is not True:
            raise Fault('PROTOCOL_MISMATCH','Session does not belong to minimal.v10')
        return row

    def current_start(self,p,request_id,a,*,session_key=None):
        return self.start(p,request_id,a,api_contract='minimal.v10',managed=True,session_key=session_key)

    def id_in_use(self,key):
        """上游下发的 id 只能用于一个采集批次：已有会话或作业用过就拒绝。"""
        with self.lock:
            if self.db.execute('SELECT 1 FROM sessions WHERE id=?',(key,)).fetchone():return True
        with self.platform.store.lock:
            return self.platform.store.db.execute('SELECT 1 FROM jobs WHERE id=?',(key,)).fetchone() is not None

    def current_redo(self,key,episode_id,p,request_id,a):
        self._current_row(key,p,a)
        return self.redo(key,episode_id,p,request_id,a,rotate_episode_id=True)

    def current_complete(self,key,episode_id,p,request_id,a):
        self._current_row(key,p,a)
        return self.complete(key,episode_id,p,request_id,a)

    def current_finish(self,key,p,request_id,a,*,destination=None):
        row=self._current_row(key,p,a)
        if destination is None:
            from .collection_destinations import resolve_destination
            destination=resolve_destination(self.platform,None,a,row['project'])
        cancel=getattr(self.runtime,'cancel_start',None)
        if row['internal_state']!='STOPPED' and callable(cancel):cancel(key)
        return self.finish(key,p,request_id,a,_destination=copy.deepcopy(destination))

    def _running(self, row):
        state=self.runtime.status(row['session_id'])
        if state['state'] not in ('RECORDING','SAVING','REDOING','STARTING','RESETTING'):
            row.update(internal_state='NEEDS_RECONCILE',error=state.get('error'));self._write(row)
            raise Fault('COLLECTION_STATE_UNKNOWN','Recorder fault requires stop/preservation and reconciliation',503)
        return state

    def _result(self,row,state=None):
        state=state or self._running(row)
        return {k:copy.deepcopy(row[k]) for k in ('session_id','dataset_id','project','robot_id','robot_revision',
                'target_type','mode','operator','episode_id','episode_index','recording_generation') } | {
                'state':'RUNNING','current_frames':state.get('current_frames',0),
                'committed_episodes':state.get('committed_episodes',0),
                'manual_connection_required':row['target_type']=='simulation',
                'notice':'Connect the registered teleoperation input manually before simulation capture.' if row['target_type']=='simulation' else None}

    def start(self,p,request_id,a,*,api_contract=None,managed=False,session_key=None):
        if type(managed)!=bool or (managed,api_contract) not in ((False,None),(True,'minimal.v9'),(True,'minimal.v10')):
            raise Fault('PROTOCOL_MISMATCH','Unsupported private collection contract')
        if session_key is not None:
            if api_contract!='minimal.v10':raise Fault('PROTOCOL_MISMATCH','Only the v10 API accepts a caller id')
            ident(session_key)
        original=copy.deepcopy(p)
        def action():
            from .api_contract import validate
            from .collection_targets import resolve_target
            from .field_bindings import bind_robot_fields
            from .sessions import SessionRuntime
            from lerobot_sdk.data.filesystem import disk_check
            if 'mode' not in original:raise Fault('INVALID_FIELDS','payload missing mode',400)
            p,robot=resolve_target(self.platform,original,a)
            validate('collections.start',p);disk_check(self.root,self.platform.config.get('collection_min_free_bytes',256*1024**2))
            from .direct_follower import is_direct
            direct=is_direct(robot)
            if managed and robot['kind']=='physical' and not direct:
                raise Fault('PHYSICAL_READINESS_REQUIRED','Managed physical collection has no accepted takeover readiness proof')
            SessionRuntime.validate_mode(self.runtime,robot,p['mode'])
            bound=bind_robot_fields(robot,p)
            if bound['calibration_revision']!=robot.get('calibration_revision','baseline'):raise Fault('CALIBRATION_MISMATCH','Registered calibration revision required')
            # 直连真实从臂：现场操作员按下开始即确认（部署时登记这个目标就是对这一点的同意）
            if robot['kind']=='physical' and p.get('workspace_confirmed') is not True and not (managed and direct):raise Fault('WORKSPACE_CONFIRMATION_REQUIRED','Confirm physical workspace')
            if not p['mode'].endswith('_teleop'):raise Fault('INVALID_MODE','These four buttons are teleoperation collection')
            with self.lock:
                if self.closing or self.platform.closing:raise Fault('SHUTTING_DOWN','Start cancelled before target acquisition',503)
                if session_key is not None and self.id_in_use(session_key):
                    raise Fault('ID_ALREADY_USED','This id has already been used by an earlier collection',409)
                # 会话 ID = 数据集 ID = 作业 ID：v10 用上游下发的 id，旧接口仍由容器生成 uuid。
                job,_=self.platform.store.create_job('collections.start',bound,request_id,a,p['project'],'robot:'+p['robot_id'],
                    request_payload={'api_contract':api_contract,'payload':original} if managed else original,key=session_key)
                key=job['operation_id']
                row={**bound,'session_id':key,'dataset_id':key,'target_type':robot['kind'],'operator':a.name,
                     'episode_id':uuid.uuid4().hex,'episode_index':0,'recording_generation':1,'internal_state':'STARTING',
                     'created_at':time.time(),'holding_confirmed':False,'collection_episodes':[]}
                if managed:row.update(api_contract=api_contract,managed=True)
                if direct:row['direct_follower']=True
                row['require_runtime_provenance']=robot.get('require_runtime_provenance') is True
                from .ros_wire import clock_id
                row['time_source']={'clock_id':clock_id(),'clock_domain':robot.get('ros',{}).get('clock_domain'),'basis':'host_monotonic_ns'}
                self._write(row)
                self.db.execute('UPDATE requests SET result=? WHERE owner=? AND request_id=?',
                    (canonical({'session_id':key,'dataset_id':key}),a.name,request_id))
                self.platform.store.register('sessions',key,'v1',p['project'],a,{
                    'session_id':key,'dataset_id':key,'robot_id':robot['robot_id'],'robot_revision':robot['revision'],
                    'mode':p['mode'],'calibration_revision':p['calibration_revision'],'config_revision':row['config_revision'],
                    'scene_id':row.get('scene_id'),'scene_revision':row.get('scene_revision'),'ros_config':row.get('ros_config')})
            try:
                # v10（8910）：对齐和采集解耦，主从臂对得上由操作员自己保证（事先运行对齐脚本），会话不再检查相差
                actual=self.runtime.start(key,robot,bound)
                if self.closing or self.platform.closing:raise Fault('SHUTTING_DOWN','Start cancelled during service shutdown',503)
                if actual.get('state')!='RECORDING' or actual.get('current_frames',0)<1:raise Fault('RECORDING_NOT_STARTED','No durable first frame confirmed')
                if actual.get('runtime_provenance') is not None:
                    from .session_fields import trusted_runtime_provenance
                    row['runtime_provenance']=trusted_runtime_provenance({'runtime_provenance':actual['runtime_provenance']},robot)
                elif robot.get('require_runtime_provenance') is True:raise Fault('PROVENANCE_REQUIRED','Actual recording start has no frozen source evidence')
                if managed:
                    with self._serial(key):
                        latest=self._read(key,a,p['project'])
                        if 'runtime_provenance' in row:latest['runtime_provenance']=row['runtime_provenance']
                        row=latest
                        if row.get('finish'):raise Fault('SESSION_ENDED','End intent cancelled the pending managed start')
                        row.update(episode_id=actual['episode_id'],internal_state='RUNNING');self._write(row)
                        result=self._result(row,actual);self.platform.store.update_job(key,'SUCCEEDED',result=result)
                else:
                    row.update(episode_id=actual['episode_id'],internal_state='RUNNING');self._write(row)
                    result=self._result(row,actual);self.platform.store.update_job(key,'SUCCEEDED',result=result)
                return result
            except BaseException as exc:
                # A start can fail after acquiring the target; preserve and confirm
                # stop before freeing its claim. Never infer this from an exception.
                safe=False;stopped=None
                if key in getattr(self.runtime,'live',{}):
                    try:
                        stopped=self.runtime.suspend_capture(key);safe=stopped.get('holding') is True and stopped.get('action_queue_empty') is True
                    except BaseException:pass
                else:safe=not any(c['job_id']==key and c['state']=='STOP_UNCONFIRMED' for c in self.platform.store.claims())
                if managed:
                    with self._serial(key):
                        latest=self._read(key,a,p['project'])
                        if 'runtime_provenance' in row:latest['runtime_provenance']=row['runtime_provenance']
                        row=latest
                        if row.get('finish') and safe and stopped is not None:
                            row.update(internal_state='STOPPED',holding_confirmed=True,error=str(exc),
                                stop_result={'state':'IDLE','session_id':key,'dataset_id':key,
                                    'preserved_current_frames':stopped.get('preserved_current_frames',0),
                                    'holding':True,'action_queue_empty':True,'committed_episodes':len(row['collection_episodes'])})
                        elif row['internal_state']!='STOPPED':
                            row.update(internal_state='START_FAILED' if safe else 'NEEDS_RECONCILE',holding_confirmed=safe,error=str(exc))
                        self._write(row)
                else:
                    row.update(internal_state='START_FAILED' if safe else 'NEEDS_RECONCILE',holding_confirmed=safe,error=str(exc));self._write(row)
                self.platform.store.release('robot:'+p['robot_id'],key,holding=safe)
                self.platform.store.update_job(key,'FAILED',error={'code':getattr(exc,'code',type(exc).__name__),'message':str(exc)})
                raise
        request_payload={**original,'session_key':session_key} if session_key is not None else original
        return self._request('minimal.start' if managed else 'start',request_payload,request_id,a,action)

    def _current(self,key,episode_id,p,a):
        self._fields(p,('project','recording_generation'))
        row=self._read(key,a,p['project'])
        if row.get('finish'):raise Fault('SESSION_ENDED','Session has been ended; start a new batch instead of resuming or changing its episode')
        if row['internal_state']!='RUNNING':raise Fault('NOT_RECORDING','Resume the same preserved session after reconciliation')
        if row['episode_id']!=episode_id:raise Fault('STALE_EPISODE','Only current unsubmitted episode may change')
        if type(p['recording_generation'])!=int or row['recording_generation']!=p['recording_generation']:raise Fault('STALE_GENERATION','Recording generation differs')
        self._running(row);return row

    def _identity(self,row,operation):
        return {k:copy.deepcopy(row[k]) for k in ('session_id','dataset_id','project','robot_id','robot_revision','target_type',
                'mode','operator','episode_id','episode_index','recording_generation')} | {'manifest_version':1,'upload_operation':operation}

    def _queue_commit(self,row,checkpoint,info):
        operation=row.get('commit_intent',{}).get('upload_operation') or uuid.uuid4().hex
        identity=self._identity(row,operation)
        identities=[*row['collection_episodes'],identity]
        payload={**identity,'upload_operation':operation,'collection_episodes':identities,
                 'task_directory':checkpoint['task_directory'],'commits':checkpoint['commits'],
                 'version':row['episode_id']+'-g'+str(row['recording_generation']),
                 'sink_id':self.platform.config.get('default_upload_sink','training'),
                 'time_source':copy.deepcopy(row.get('time_source')),
                  'require_runtime_provenance':row.get('require_runtime_provenance') is True,
                  'source_metadata':{k:copy.deepcopy(row.get(k)) for k in ('scene_id','scene_revision','config_revision','calibration_revision','ros_config','runtime_provenance','runtime_provenance_history')}}
        sink=self.platform.config.get('sinks',{}).get(payload['sink_id'],{})
        if row.get('api_contract')!='minimal.v10' and sink.get('kind')=='directory':
            from .collection_destinations import resolve_destination
            actor=Principal(row['operator'],(row['project'],),('operator',))
            payload['destination']=resolve_destination(self.platform,None,actor,row['project'])
        episode={**identity,'commit':info,'upload_payload':payload}
        if row.get('api_contract')=='minimal.v10':episode['delivery_deferred']=True
        # Durable intent precedes background enqueue. Startup repairs that boundary.
        with self.lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                self.db.execute('INSERT INTO episodes VALUES(?,?,?,?)',(row['session_id'],row['episode_id'],row['recording_generation'],canonical(episode)))
                row.update(collection_episodes=identities,internal_state='BETWEEN_EPISODES',last_upload_operation=operation)
                if row.get('commit_intent',{}).get('stopped') is True:
                    row.update(internal_state='STOPPED',episode_index=len(identities),episode_id=uuid.uuid4().hex,recording_generation=1)
                    row['stop_result'].update(preserved_current_frames=0,committed_episodes=len(identities))
                row.pop('commit_intent',None)
                self._write(row);self.db.execute('COMMIT')
            except BaseException:self.db.execute('ROLLBACK');raise
        if not episode.get('delivery_deferred'):self.uploads.enqueue(operation,payload)
        return identity

    def _enqueue_episode(self,episode,row):
        from .collection_sync import SYNC_PROTOCOL
        if ((row.get('finish',{}).get('destination') or {}).get('protocol')==SYNC_PROTOCOL
                and episode['upload_operation']!=row.get('last_upload_operation')):
            return None  # 一个批次只同步包含全部已提交条目的最终快照，中间累计版本不重复发送
        payload=copy.deepcopy(episode['upload_payload'])
        if episode.get('delivery_deferred'):
            destination=row.get('finish',{}).get('destination')
            if not isinstance(destination,dict):
                raise Fault('UPLOAD_DESTINATION_REQUIRED','Deferred batch has no frozen finish destination')
            payload['destination']=copy.deepcopy(destination)
        return self.uploads.enqueue(episode['upload_operation'],payload)

    def complete(self,key,episode_id,p,request_id,a):
        packet={**p,'session_id':key,'episode_id':episode_id}
        def action():
            with self._serial(key):
                self._fields(p,('project','recording_generation'))
                existing=self._read(key,a,p['project'])
                if existing['internal_state']=='STOPPED':
                    return self._complete_stopped(existing,episode_id,p,request_id)
                row=self._current(key,episode_id,p,a)
                row.update(internal_state='COMMITTING',commit_intent={'upload_operation':uuid.uuid4().hex,'request_id':request_id,
                    'episode_id':episode_id,'episode_index':row['episode_index'],'recording_generation':row['recording_generation']})
                self._write(row)
                try:
                    info=self.runtime.command(key,'save',{})
                    checkpoint=self.runtime.capture_checkpoint(key)
                    identity=self._queue_commit(row,checkpoint,info)
                except BaseException:
                    row['internal_state']='NEEDS_RECONCILE';self._write(row);raise
                try:
                    if self.closing:raise Fault('SHUTTING_DOWN','Next recording cancelled during shutdown',503)
                    actual=self.runtime.command(key,'next',{})
                    if actual.get('current_frames',0)<1:raise Fault('RECORDING_NOT_STARTED','Next episode has no durable first frame')
                    row.update(episode_id=actual['episode_id'],episode_index=row['episode_index']+1,recording_generation=1,internal_state='RUNNING')
                    self._write(row)
                    return {**self._result(row,actual),'committed_episode':identity,'upload_operation':identity['upload_operation']}
                except BaseException as exc:
                    row.update(episode_index=row['episode_index']+1,episode_id=uuid.uuid4().hex,recording_generation=1,
                               internal_state='NEEDS_RECONCILE',error=str(exc));self._write(row)
                    fault=Fault('NEXT_EPISODE_FAILED','Episode committed and queued; next recording failed: '+str(exc),503)
                    fault.details={'committed_episode':identity,'upload_operation':identity['upload_operation'],'session_id':key}
                    raise fault from exc
        return self._request('complete',packet,request_id,a,action)

    def _complete_stopped(self,row,episode_id,p,request_id):
        from dataclasses import asdict
        from lerobot_sdk import CollectionManager
        if row.get('holding_confirmed') is not True:raise Fault('STOP_UNCONFIRMED','Recording stop must be confirmed before submitting preserved data')
        if row['episode_id']!=episode_id:raise Fault('STALE_EPISODE','Only current unsubmitted episode may change')
        if type(p['recording_generation'])!=int or row['recording_generation']!=p['recording_generation']:raise Fault('STALE_GENERATION','Recording generation differs')
        if row['session_id'] in getattr(self.runtime,'live',{}):raise Fault('NOT_STOPPED','Recorder still owns a live session')
        directory=self.root/'capture/tasks'/row['session_id']
        task=CollectionManager().open_task(directory)
        if len(task.frozen_commits())!=len(row['collection_episodes']):raise Fault('JOURNAL_CONFLICT','Submitted episode counts differ')
        if task.status().current_frames<1:raise Fault('EMPTY_EPISODE','No preserved frames to submit')
        row.update(internal_state='COMMITTING',commit_intent={'upload_operation':uuid.uuid4().hex,'request_id':request_id,
            'episode_id':episode_id,'episode_index':row['episode_index'],'recording_generation':row['recording_generation'],'stopped':True})
        self._write(row)
        try:
            info=asdict(task.commit_preserved_episode())
            identity=self._queue_commit(row,{'task_directory':str(directory),'commits':task.frozen_commits()},info)
            row.update(episode_index=len(row['collection_episodes']),episode_id=uuid.uuid4().hex,recording_generation=1,internal_state='STOPPED')
            row['stop_result'].update(preserved_current_frames=0,committed_episodes=len(row['collection_episodes']))
            self._write(row)
            result={**row['stop_result'],'committed_episode':identity,'upload_operation':identity['upload_operation'],
                'episode_id':row['episode_id'],'episode_index':row['episode_index'],'recording_generation':1,'current_frames':0,'next_recording_started':False}
            self.platform.store.event(row['session_id'],'preserved_episode_committed',result)
            return result
        except BaseException:
            row['internal_state']='NEEDS_RECONCILE';self._write(row);raise

    def redo(self,key,episode_id,p,request_id,a,*,rotate_episode_id=False):
        if type(rotate_episode_id)!=bool:raise Fault('PROTOCOL_MISMATCH','Attempt rotation must be an internal boolean')
        packet={**p,'session_id':key,'episode_id':episode_id}
        def action():
            with self._serial(key):
                row=self._current(key,episode_id,p,a)
                if rotate_episode_id:
                    if row.get('api_contract')=='minimal.v10':self._current_row(key,p,a)
                    else:self._minimal_row(key,p,a)
                row['internal_state']='REDOING';self._write(row)
                try:
                    self.runtime.command(key,'redo',{'confirm':True})
                    row['recording_generation']+=1;self._write(row)
                    if self.closing:raise Fault('SHUTTING_DOWN','Redo recording cancelled during shutdown',503)
                    actual=self.runtime.command(key,'next',{})
                    if actual.get('current_frames',0)<1:raise Fault('RECORDING_NOT_STARTED','Redo has no durable first frame')
                    if rotate_episode_id:row['episode_id']=uuid.uuid4().hex
                    self.runtime.set_episode_id(key,row['episode_id'])
                    row['internal_state']='RUNNING';self._write(row)
                    return self._result(row,actual)
                except BaseException:
                    row['internal_state']='NEEDS_RECONCILE';self._write(row);raise
        return self._request('minimal.redo' if rotate_episode_id else 'redo',packet,request_id,a,action)

    def finish(self,key,p,request_id,a,*,_destination=None):
        """One user action durably requests stop, seal and background delivery."""
        self._fields(p,('project',));packet={**p,'session_id':key}
        def action():
            with self._serial(key):
                row=self._read(key,a,p['project'])
                if row.get('api_contract')=='minimal.v10':
                    old=row.get('finish',{}).get('destination')
                    if old is not None and _destination is not None and old!=_destination:
                        raise Fault('UPLOAD_DESTINATION_FROZEN','Batch destination was already fixed by its end request')
                    if _destination is None:
                        from .collection_destinations import resolve_destination
                        _selected=old or resolve_destination(self.platform,None,a,row['project'])
                    else:_selected=copy.deepcopy(_destination)
                if not row.get('finish'):
                    row['finish']={'state':'QUEUED','request_id':request_id,'attempts':0,'requested_at':time.time()}
                    if row.get('api_contract')=='minimal.v10':row['finish']['destination']=_selected
                    self._write(row)
                phase=row['finish']['state']
                result={k:row[k] for k in ('session_id','dataset_id','episode_id','episode_index','recording_generation')}
                result.update(state='IDLE' if phase in ('QUEUED_UPLOAD','EMPTY') else 'FINISHING',
                    finish_state=phase,finished=phase in ('QUEUED_UPLOAD','EMPTY'),automatic_upload=True)
                self._launch_finish(key)
                return result
        if _destination is not None:packet['destination']=copy.deepcopy(_destination)
        return self._request('finish',packet,request_id,a,action)

    def _launch_finish(self,key):
        with self.lock:
            previous=self.finish_workers.get(key)
            if previous and previous.is_alive():return
            worker=threading.Thread(target=self._finish_loop,args=(key,),name='CollectionFinish-'+key[:8],daemon=True)
            self.finish_workers[key]=worker;self.active_requests+=1;worker.start()

    def _finish_loop(self,key):
        try:self._finish_session(key)
        finally:
            with self.drained:self.active_requests-=1;self.drained.notify_all()

    def _finish_session(self,key):
        while not self.finish_exit.is_set():
            try:
                with self._serial(key):
                    with self.lock:
                        record=self.db.execute('SELECT body FROM sessions WHERE id=?',(key,)).fetchone()
                    row=json.loads(record['body']);finish=row.get('finish',{})
                    if finish.get('state') in ('QUEUED_UPLOAD','EMPTY','ABANDONED'):return
                    actor=Principal(row['operator'],(row['project'],),('operator',))
                    finish.update(state='STOPPING',attempts=finish.get('attempts',0)+1,error=None)
                    row['finish']=finish;self._write(row)
                    # The end request authorizes stopping the same target, not
                    # starting/arming/aligning or bypassing actual HOLD proof.
                    self.stop(key,{'project':row['project']},'finish-stop-'+digest([key,finish['request_id'],finish['attempts']]).removeprefix('sha256:')[:40],actor,_finish_only=True)
                    row=self._read(key,actor);row['finish']['state']='FINALIZING';self._write(row)
                    if self.finish_exit.is_set():return
                    if row.get('commit_intent'):
                        self._repair_commit(row);row=self._read(key,actor)
                    if row['stop_result'].get('preserved_current_frames',0)>0:
                        self._complete_stopped(row,row['episode_id'],{'project':row['project'],'recording_generation':row['recording_generation']},finish['request_id'])
                        row=self._read(key,actor)
                    # Business commit precedes outbox enqueue. Replay that
                    # enqueue idempotently before reporting queued delivery.
                    with self.lock:episodes=self.db.execute('SELECT body FROM episodes WHERE session_id=?',(key,)).fetchall()
                    for record in episodes:
                        episode=json.loads(record['body']);self._enqueue_episode(episode,row)
                    phase='QUEUED_UPLOAD' if row['collection_episodes'] else 'EMPTY'
                    row['finish'].update(state=phase,error=None,completed_at=time.time())
                    row['stop_result'].update(finished=True,finish_state=phase,automatic_upload=True,
                        upload_operation=row.get('last_upload_operation'))
                    self._write(row)
                    self.platform.store.event(key,'collection_finished',row['stop_result'])
                    return
            except Exception as exc:
                if self.finish_exit.is_set():return
                with self._serial(key):
                    actor=locals().get('actor')
                    if actor is not None:
                        row=self._read(key,actor)
                        if row['finish'].get('state')=='ABANDONED':return   # 放弃异常批次：不再重试结束
                        row['finish'].update(state='RETRY_WAIT',error=str(exc),last_attempt_at=time.time())
                        self._write(row)
                if self.finish_exit.wait(3):return

    def stop(self,key,p,request_id,a,*,_during_close=False,_finish_only=False):
        self._fields(p,('project',),('workspace_confirmed',));packet={**p,'session_id':key}
        def action():
            with self._serial(key):
                row=self._read(key,a,p['project'])
                if row['internal_state']=='STOPPED':return row['stop_result']
                row['internal_state']='STOPPING';self._write(row)
                try:
                    live=getattr(self.runtime,'live',{}).get(key)
                    if live is not None:
                        if live.get('starting') and not live.get('task'):
                            if p.get('workspace_confirmed') is not True and not _finish_only:raise Fault('WORKSPACE_CONFIRMATION_REQUIRED','Confirm onsite safety before failed-start reconciliation')
                            # A failed CLOSE can retain this explicit simulation-end
                            # recheck after the original failed-start claim was released.
                            # Only retry cleanup of that completed, cancelled owner;
                            # normal startup and foreign/active claims retain their gates.
                            direct=row.get('direct_follower') is True
                            retained_end=(_finish_only is True and (row.get('target_type')=='simulation' or direct) and
                                bool(row.get('finish',{}).get('request_id')) and
                                live.get('stopped_target_recheck') is True and live.get('startup_finished') is True and
                                live.get('cancelled') is True and live.get('verification') is False and
                                live.get('mode')==('physical_teleop' if direct else 'simulation_teleop'))
                            self._failed_unrecorded(row,request_id,allow_released=retained_end)
                            stopped=self.runtime.suspend_capture(key,failed_before_recording=True,robot=self.platform._robot(row,a))
                        else:stopped=self.runtime.suspend_capture(key)
                    else:stopped=self._recover_stopped(row,p,request_id=request_id,finish_only=_finish_only)
                    if stopped.get('holding') is not True or stopped.get('action_queue_empty') is not True:raise Fault('STOP_UNCONFIRMED','Target stop/queue confirmation missing',503)
                    result={'state':'IDLE','session_id':key,'dataset_id':key,'preserved_current_frames':stopped.get('preserved_current_frames',0),
                            'holding':True,'action_queue_empty':True,'committed_episodes':len(row['collection_episodes'])}
                    row.update(internal_state='STOPPED',holding_confirmed=True,stop_result=result);self._write(row)
                    self.platform.store.release('robot:'+row['robot_id'],key,holding=True)
                    self.platform.store.event(key,'collection_stopped',result)
                    return result
                except BaseException as exc:
                    row.update(internal_state='NEEDS_RECONCILE',error=str(exc));self._write(row)
                    self.platform.store.release('robot:'+row['robot_id'],key,holding=False);raise
        return self._request('stop',packet,request_id,a,action,during_close=_during_close)

    def _failed_unrecorded(self,row,request_id,*,allow_released=False):
        """Prove a returned failed start never created a recorder, without edits."""
        key=row['session_id'];actor=Principal(row['operator'],(row['project'],),('operator',))
        job=self.platform.store.job(key,actor)
        if (job['kind']!='collections.start' or job['state']!='FAILED' or job['project']!=row['project'] or
            job['payload'].get('robot_id')!=row['robot_id'] or job['payload'].get('robot_revision')!=row['robot_revision']):
            raise Fault('RECONCILE_REQUIRED','Only the same terminal failed start can have an absent recorder')
        claim=next((value for value in self.platform.store.claims() if value['resource']=='robot:'+row['robot_id']),None)
        if (claim is None and not allow_released) or claim is not None and (claim['job_id']!=key or claim['state'] not in ('STOP_UNCONFIRMED','RECONCILE_REQUIRED')):
            raise Fault('RECONCILE_REQUIRED','Failed-start target claim is absent, active or belongs to another session')
        with self.lock:
            original=self.db.execute('SELECT * FROM requests WHERE owner=? AND request_id=?',(row['operator'],job['request_id'])).fetchone()
            if not original or original['state']!='FAILED' or json.loads(original['result'] or '{}').get('session_id')!=key:
                raise Fault('RECONCILE_REQUIRED','Original start request has not completed with a durable failure')
            pending=self.db.execute("SELECT * FROM requests WHERE state='PENDING'").fetchall()
            for value in pending:
                if value['owner']==row['operator'] and value['request_id']==request_id:continue
                if json.loads(value['result'] or '{}').get('session_id')==key:
                    raise Fault('RECONCILE_REQUIRED','A request for this session is still executing')
            committed=self.db.execute('SELECT 1 FROM episodes WHERE session_id=?',(key,)).fetchone()
        if row.get('collection_episodes') or row.get('commit_intent') or row.get('episode_index')!=0 or committed:
            raise Fault('RECONCILE_REQUIRED','Submitted data cannot be reinterpreted as an empty failed start')
        paths=(self.root/'capture',self.root/'capture/tasks',self.root/'capture/tasks'/key)
        if any(path.is_symlink() for path in paths) or paths[-1].exists():
            raise Fault('RECONCILE_REQUIRED','An existing recorder directory or link must use journal preservation')

    def _recover_stopped(self,row,p,*,request_id=None,finish_only=False):
        if p.get('workspace_confirmed') is not True and not finish_only:raise Fault('WORKSPACE_CONFIRMATION_REQUIRED','Confirm onsite safety before fault reconciliation')
        directory=self.root/'capture/tasks'/row['session_id']
        if not directory.exists() and not directory.is_symlink():
            self._failed_unrecorded(row,request_id,allow_released=finish_only)
            actor=Principal(row['operator'],(row['project'],),('operator',))
            robot=self.platform._robot(row,actor)
            from .direct_follower import sim_like
            stopped=self.runtime.reconcile(robot,verify_stopped_target=True,session_id=row['session_id'],
                **({'simulation_end_only':True} if finish_only and sim_like(robot) else {}))
            return {**stopped,'preserved_current_frames':0,'recording_never_started':True}
        from lerobot_sdk import CollectionManager
        # open_task verifies hashes and acquires the recorder lock. A running
        # recorder or corrupt journal must block environment recovery.
        task=CollectionManager().open_task(directory)
        task.suspend().wait(10)
        a=Principal(row['operator'],(row['project'],),('operator',))
        robot=self.platform._robot(row,a)
        from .direct_follower import sim_like
        stopped=self.runtime.reconcile(robot,**({'verify_stopped_target':True,'session_id':row['session_id'],'simulation_end_only':True} if finish_only and sim_like(robot) else {}))
        return {**stopped,'preserved_current_frames':task.status().current_frames,
                'holding':stopped.get('holding') is True,'action_queue_empty':stopped.get('action_queue_empty') is True}

    def status(self,p,a):
        self._fields(p,('project',),('robot_id','robot_revision','mode'));a.require(p['project'],'operator')
        from .collection_targets import resolve_target
        p=copy.deepcopy(p)
        if 'robot_id' not in p and 'robot_revision' not in p:
            if 'mode' in p:
                from .collection_targets import COLLECTION_MODES
                if p['mode'] not in COLLECTION_MODES:raise Fault('INVALID_MODE','Supported collection teleoperation mode required',400)
            with self.lock:project_records=self.db.execute('SELECT body FROM sessions WHERE project=?',(p['project'],)).fetchall()
            active=[json.loads(r['body']) for r in project_records]
            active=[r for r in active if (r['internal_state'] not in ('STOPPED','START_FAILED') or
                r.get('finish',{}).get('state') not in (None,'QUEUED_UPLOAD','EMPTY','ABANDONED')) and
                ('mode' not in p or r['mode']==p['mode'])]
            if any(r['operator']!=a.name for r in active):raise Fault('FORBIDDEN','Project collection belongs to another operator',403)
            targets={(r['robot_id'],r['robot_revision'],r['mode']) for r in active}
            if len(targets)>1:raise Fault('PROJECT_TARGET_AMBIGUOUS','Project has multiple unfinished targets; specify robot_id and robot_revision')
            if targets:
                robot_id,revision,mode=next(iter(targets));p.update(robot_id=robot_id,robot_revision=revision,mode=mode)
        p,robot=resolve_target(self.platform,p,a)
        identity={k:p[k] for k in ('project','robot_id','robot_revision','mode')} | {'target_type':robot['kind']}
        with self.lock:records=self.db.execute('SELECT body FROM sessions WHERE project=? AND robot_id=?',(p['project'],p['robot_id'])).fetchall()
        rows=[v for r in records if (v:=json.loads(r['body']))['robot_revision']==p['robot_revision']]
        pending_finish=[r for r in rows if r.get('finish',{}).get('state') not in (None,'QUEUED_UPLOAD','EMPTY','ABANDONED')]
        if pending_finish:
            row=max(pending_finish,key=lambda r:r['created_at'])
            if row['operator']!=a.name:raise Fault('FORBIDDEN','Collection belongs to another operator',403)
            return {k:row[k] for k in ('session_id','dataset_id','robot_id','robot_revision','target_type','mode','project','episode_id','episode_index','recording_generation')} | {
                'state':'FINISHING','finish_state':row['finish']['state'],'finished':False,'automatic_upload':True,
                'current_frames':row.get('stop_result',{}).get('preserved_current_frames',0)}
        try:probe=self.probe(robot['robot_id'],robot['revision'],a)
        except Exception as exc:raise Fault('TARGET_STATUS_UNAVAILABLE','Target status query failed: '+str(exc),503) from exc
        peer=probe.get('peer') if isinstance(probe,dict) else None
        if not isinstance(peer,dict) or not {'owner','fault','holding','action_queue_empty'}<=peer.keys() or type(peer['holding'])!=bool or type(peer['action_queue_empty'])!=bool or peer['owner'] is not None and not isinstance(peer['owner'],str):
            raise Fault('TARGET_STATUS_UNAVAILABLE','Target returned an incomplete status result',503)
        if peer.get('fault'):raise Fault('TARGET_FAULT','Target requires controlled reconciliation',503)
        with self.lock:records=self.db.execute('SELECT body FROM sessions WHERE project=? AND robot_id=?',(p['project'],p['robot_id'])).fetchall()
        rows=[v for r in records if (v:=json.loads(r['body']))['robot_revision']==p['robot_revision']]
        unresolved=[r for r in rows if r['internal_state'] not in ('STOPPED','START_FAILED')]
        if unresolved:
            row=max(unresolved,key=lambda r:r['created_at'])
            if row['operator']!=a.name:raise Fault('FORBIDDEN','Active collection belongs to another operator',403)
            if row['internal_state']!='RUNNING':
                fault=Fault('COLLECTION_STATE_UNKNOWN','Collection transition/fault not confirmed',503)
                fault.details={k:row[k] for k in ('session_id','dataset_id','episode_id','episode_index','recording_generation')}
                raise fault
            backend=getattr(self.runtime,'live',{}).get(row['session_id'],{}).get('backend')
            owner=getattr(backend,'client',None)
            if owner is not None and peer['owner']!=owner:raise Fault('TARGET_STATE_CONFLICT','Actual target owner differs from recorder',503)
            # Polling must not overwrite STOPPED with a stale RUNNING snapshot.
            with self._serial(row['session_id']):
                row=self._read(row['session_id'],a,p['project'])
                if row['internal_state']=='STOPPED':return {'state':'IDLE',**identity}
                if row['internal_state']!='RUNNING':raise Fault('COLLECTION_STATE_UNKNOWN','Collection transition not confirmed',503)
                return self._result(row)
        claim=next((v for v in self.platform.store.claims() if v['resource']=='robot:'+p['robot_id']),None)
        if claim or peer.get('owner') or not peer['action_queue_empty']:raise Fault('TARGET_STATE_CONFLICT','Target ownership/queue requires reconciliation',503)
        finished=[r for r in rows if r.get('finish',{}).get('state') in ('QUEUED_UPLOAD','EMPTY')]
        latest=max(rows,key=lambda r:r['created_at']) if rows else None
        result={'state':'IDLE',**identity}
        if latest and latest in finished:
            result.update(session_id=latest['session_id'],finished=True,finish_state=latest['finish']['state'],
                automatic_upload=True,upload_operation=latest.get('last_upload_operation'))
        return result

    def resume(self,key,p,request_id,a,*,rotate_episode_id=False,require_workspace_confirmation=True):
        if type(rotate_episode_id)!=bool or type(require_workspace_confirmation)!=bool:
            raise Fault('PROTOCOL_MISMATCH','Recovery flags must be internal booleans')
        self._fields(p,('project','workspace_confirmed') if require_workspace_confirmation else ('project',),
            () if require_workspace_confirmation else ('workspace_confirmed',))
        if require_workspace_confirmation and p['workspace_confirmed'] is not True:
            raise Fault('WORKSPACE_CONFIRMATION_REQUIRED','Confirm onsite safety before recovery')
        def action():
            with self._serial(key):
                row=self._read(key,a,p['project'])
                if rotate_episode_id or not require_workspace_confirmation:self._minimal_row(key,p,a)
                if row.get('finish'):raise Fault('SESSION_ENDED','Session has been ended; start a new batch instead of resuming')
                if row['internal_state']!='STOPPED':raise Fault('RECONCILE_REQUIRED','Stop/preserve and reconcile the old environment first')
                robot=self.platform._robot(row,a)
                from .direct_follower import is_direct
                if not require_workspace_confirmation and robot['kind']=='physical' and not is_direct(robot):
                    raise Fault('PHYSICAL_READINESS_REQUIRED','Managed physical recovery requires accepted takeover readiness proof')
                self.probe(robot['robot_id'],robot['revision'],a)
                claim=next((v for v in self.platform.store.claims() if v['resource']=='robot:'+row['robot_id']),None)
                if claim:raise Fault('BUSY','Target is already claimed')
                with self.platform.store.lock:self.platform.store.db.execute('INSERT INTO claims VALUES(?,?,?,?)',('robot:'+row['robot_id'],key,uuid.uuid4().hex,'ACTIVE'))
                try:
                    actual=self.runtime.resume_capture(key,robot,row)
                    if actual.get('runtime_provenance') is not None:
                        from .session_fields import trusted_runtime_provenance
                        proof=trusted_runtime_provenance({'runtime_provenance':actual['runtime_provenance']},robot)
                        history=row.get('runtime_provenance_history',[row['runtime_provenance']] if row.get('runtime_provenance') else [])
                        if proof not in history:history.append(proof)
                        row.update(runtime_provenance=proof,runtime_provenance_history=history)
                    elif row.get('require_runtime_provenance'):raise Fault('PROVENANCE_REQUIRED','Resumed recording has no trusted source evidence')
                    if rotate_episode_id:row['episode_id']=uuid.uuid4().hex
                    self.runtime.set_episode_id(key,row['episode_id'])
                    row.update(recording_generation=row['recording_generation']+1,internal_state='RUNNING',holding_confirmed=False);self._write(row)
                    return self._result(row,actual)
                except BaseException:
                    row['internal_state']='NEEDS_RECONCILE';self._write(row);self.platform.store.release('robot:'+row['robot_id'],key,holding=False);raise
        return self._request('minimal.resume' if rotate_episode_id else 'resume',{**p,'session_id':key},request_id,a,action)

    def close(self):
        if self.closed:return
        with self.lock:
            self.closing=True
            rows=[json.loads(r['body']) for r in self.db.execute('SELECT body FROM sessions').fetchall()]
        self.finish_exit.set()
        # Cancellation is registered even while heavy native imports precede
        # runtime.live. Such a start must never acquire a device after shutdown.
        cancel=getattr(self.runtime,'cancel_start',None)
        if callable(cancel):
            for row in rows:
                if row['internal_state']!='STOPPED':cancel(row['session_id'])
        deadline=time.monotonic()+self.platform.config.get('collection_shutdown_timeout_s',30)
        with self.drained:
            while self.active_requests and time.monotonic()<deadline:self.drained.wait(min(.1,deadline-time.monotonic()))
        with self.lock:rows=[json.loads(r['body']) for r in self.db.execute('SELECT body FROM sessions').fetchall()]
        for row in rows:
            if row['session_id'] in getattr(self.runtime,'live',{}):
                if any(w.is_alive() for w in self.finish_workers.values()) and time.monotonic()>=deadline:break
                a=Principal(row['operator'],(row['project'],),('operator',))
                try:self.stop(row['session_id'],{'project':row['project']},'shutdown-'+uuid.uuid4().hex,a,_during_close=True)
                except Exception as exc:self.platform.store.event(row['session_id'],'shutdown_stop_unconfirmed',{'error':str(exc),'files_preserved':True})
        for worker in list(self.finish_workers.values()):
            if worker is not threading.current_thread():worker.join(max(0,deadline-time.monotonic()))
        if any(w.is_alive() for w in self.finish_workers.values()):
            raise Fault('FINISH_PENDING','End intent is durable; final worker is still quiescing, shared state remains open',503)
        if self.uploads.stop(timeout_s=3) is False:
            raise Fault('UPLOAD_PENDING','Snapshot preparation worker is still shutting down; retry close',503)
        if not self.sync.stop(timeout_s=5):
            raise Fault('UPLOAD_PENDING','Upload process is still shutting down; its worker lock is retained',503)
        self.closed=True
        if not self.active_requests and not any(w.is_alive() for w in self.finish_workers.values()):self.db.close()
