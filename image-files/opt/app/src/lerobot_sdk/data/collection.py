"""Episode transactions around imported Seeed writers and a borrowed RobotSession."""
from pathlib import Path
import os
import copy
import re
import queue
import threading
import time
import uuid
import json
from functools import wraps
import numpy as np
from lerobot_sdk.data.contracts import (CollectionConfig,CollectionState,CollectionStatus,EpisodeInfo,
                               DatasetRef,DATA_SCHEMA,SEEED_COMMIT)
from lerobot_sdk.core.async_operation import Operation
from lerobot_sdk.data.filesystem import atomic_json,read_json,digest,sha_file,disk_check,remove_owned_tree
from lerobot_sdk.adapters.seeed_dataset import (SeeedWriter,make_frame,upstream,joint_names_of,joint_units_of,robot_type_of,
                                                 action_rule_of,action_semantics_of,dataset_vector)
from lerobot_sdk.data.manager import DatasetManager,seal_dataset
from lerobot_sdk.core.errors import (SDKError,ConfigurationError,StateError,BusyError,ContractMismatch,OperationFailed,OperationCancelled)
from lerobot_sdk.core.types import JOINT_NAMES,JointTarget
from lerobot_sdk.core.events import EventHub
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.provenance import bounded_metadata
from .provenance import SOURCE_FIELD,training_frame,export_frame_provenance


# action=source_action：连续这么多帧都没有配上数据源给的 action，就判这一条失败（不拿状态顶替）
MAX_MISSING_SOURCE_ACTIONS=10


def _sdk_errors(method):
    @wraps(method)
    def call(*args,**kwargs):
        try:return method(*args,**kwargs)
        except SDKError:raise
        except Exception as exc:raise OperationFailed(f'{method.__name__} failed: {exc}') from exc
    return call


def _raw_frame(path):
    with np.load(path,allow_pickle=False) as data:
        return {key:str(data[key].item()) if key=='task' else data[key].copy() for key in data.files}


def _verify_commits(directory,commits):
    for index,entry in enumerate(commits):
        if entry['episode_index']!=index or entry['frames']!=len(entry['files']) or entry['frames']<1:
            raise ContractMismatch('Invalid committed episode log')
        payload={k:v for k,v in entry.items() if k!='content_hash'}
        if digest(payload)!=entry['content_hash']:raise ContractMismatch('Episode journal hash differs')
        for item in entry['files']:
            path=directory/item['path']
            if path.is_symlink() or not path.resolve().is_relative_to(directory.resolve()):
                raise ContractMismatch('Journal frame path leaves task directory')
            if not path.is_file() or sha_file(path)!=item['sha256']:
                raise ContractMismatch('Committed recovery frame is missing or corrupt')


def _episode_directory(directory,name):
    if not isinstance(name,str) or not re.fullmatch(r'episodes/[0-9a-f]{32}',name):
        raise ContractMismatch('Episode directory is not a canonical service identity')
    root=directory/Path(name)
    if root.is_symlink() or (directory/'episodes').is_symlink() or not root.resolve().is_relative_to((directory/'episodes').resolve()):
        raise ContractMismatch('Episode directory is linked or leaves the task')
    return root


def _finish_discard_tombstones(directory,state):
    """Finish only a durably authorized current-episode discard after a crash.

    Unknown directories and unindexed raw frames are never inferred to be
    discardable. Already committed/current directories always remain protected.
    """
    pending=state.get('discard_tombstones',[])
    if not isinstance(pending,list):raise ContractMismatch('Invalid discard journal')
    if not pending:return state
    protected={Path(item['path']).parent.as_posix() for ep in state['commits'] for item in ep['files']}
    if state.get('current'):protected.add(state['current']['directory'])
    for item in pending:
        if not isinstance(item,dict) or set(item)!={'directory','reason'} or item['reason']!='explicit_unsubmitted_discard':
            raise ContractMismatch('Discard intent lacks an explicit unsubmitted authorization')
        root=_episode_directory(directory,item['directory'])
        if item['directory'] in protected:raise ContractMismatch('Discard intent intersects protected recording data')
        remove_owned_tree(root,directory/'episodes')
    value={**state,'discard_tombstones':[],'discard_history':[*state.get('discard_history',[]),*pending]}
    atomic_json(directory/'task.json',value)
    return value


def _verify_current(directory,current,commits):
    """Verify preserved frames without treating them as published commits."""
    if not current:return
    if not isinstance(current,dict) or not isinstance(current.get('files'),list):
        raise ContractMismatch('Invalid unsubmitted episode journal')
    name=current.get('directory');root=_episode_directory(directory,name)
    if not root.is_dir():
        raise ContractMismatch('Unsubmitted episode directory is missing or linked')
    submitted={item['path'] for commit in commits for item in commit['files']}
    last_stamp=last_sequence=-1
    for index,item in enumerate(current['files']):
        if not isinstance(item,dict) or set(item)!={'path','sha256'} or not isinstance(item['path'],str) or not isinstance(item['sha256'],str) or not re.fullmatch(r'sha256:[0-9a-f]{64}',item['sha256']):
            raise ContractMismatch('Invalid unsubmitted frame identity/hash')
        relative=Path(item['path'])
        if relative.is_absolute() or relative.as_posix()!=item['path'] or len(relative.parts)!=3:
            raise ContractMismatch('Unsubmitted frame path is not canonical and relative')
        path=directory/item['path']
        if item['path'] in submitted or path.parent!=root or not re.fullmatch(rf'{index:08d}(?:-[0-9a-f]{{32}})?\.npz',path.name) or path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
            raise ContractMismatch('Invalid unsubmitted frame path')
        if not path.is_file() or sha_file(path)!=item['sha256']:raise ContractMismatch('Unsubmitted recovery frame is missing or corrupt')
        try:
            frame=_raw_frame(path);stamp=int(frame['sdk.captured_monotonic_ns'].item());sequence=int(frame['sdk.source_sequence'].item())
        except Exception as exc:raise ContractMismatch('Invalid unsubmitted frame metadata') from exc
        if stamp<=last_stamp or sequence<=last_sequence:raise ContractMismatch('Unsubmitted source timeline is not monotonic')
        last_stamp,last_sequence=stamp,sequence


class CollectionManager:
    @_sdk_errors
    def create_task(self,config):
        if not isinstance(config,CollectionConfig):raise ConfigurationError('CollectionConfig required')
        config.root.mkdir(parents=True,exist_ok=True)
        directory=config.root/config.name
        try:directory.mkdir()
        except FileExistsError as exc:raise StateError('Task already exists; open_task instead') from exc
        (directory/'episodes').mkdir();(directory/'attempts').mkdir();(directory/'versions').mkdir()
        state={'journal_version':1,'schema':DATA_SCHEMA,'task_id':uuid.uuid4().hex,
               'config':config.to_dict(),'state':'CREATED','commits':[],'error':None,'dataset':None}
        atomic_json(directory/'task.json',state)
        return CollectionTask(directory,state)

    @_sdk_errors
    def open_task(self,task_dir):
        directory=Path(task_dir).expanduser().resolve()
        lock=DeviceLock('collection-task:'+str(directory));lock.acquire()
        try:
            state=read_json(directory/'task.json')
            if state.get('journal_version')!=1 or state.get('schema')!=DATA_SCHEMA:
                raise ContractMismatch('Unsupported task journal schema')
            _verify_commits(directory,state['commits'])
            _verify_current(directory,state.get('current'),state['commits'])
            state=_finish_discard_tombstones(directory,state)
            if state['state']=='LOCAL_READY':
                ref=state['dataset']
                if not (directory/ref['path']).resolve().is_relative_to(directory):
                    raise ContractMismatch('Dataset reference leaves task directory')
                DatasetManager().inspect(DatasetRef(directory/ref['path'],ref['dataset_id'],ref['version'],ref['content_hash']))
            else:
                # Rebuild only committed episodes on an explicit subsequent start.
                state['state']='CREATED' if not state['commits'] and state['state']=='CREATED' else 'RECOVERABLE'
            return CollectionTask(directory,state)
        finally:lock.release()


class CollectionTask:
    def __init__(self,directory,state):
        self.directory=Path(directory)
        self.config=CollectionConfig(**state['config'])
        self._journal=state
        self._state=CollectionState(state['state'])
        self._recovering=self._state==CollectionState.RECOVERABLE
        self._commits=list(state['commits'])
        self._error=state['error']
        self._discard_tombstones=copy.deepcopy(state.get('discard_tombstones',[]))
        self._lock=threading.RLock()
        self._commands=queue.Queue()
        self._pending=None;self._stop_operation=None
        self._suspend_operation=None
        self._thread=None;self._writer=None;self._scope=None
        self._recording=False;self._stop_requested=False
        self._held=None   # action=next_observation_state：等下一帧观测的那一帧（只在内存里）
        self._missing_actions=0;self._skipped_actions=0   # action=source_action：连续 / 自上次写帧以来没配上的帧数
        current=state.get('current') or {}
        self._current=copy.deepcopy(current.get('files',[]))
        self._episode_dir=self.directory/current['directory'] if current.get('directory') else None
        self._frame_count=len(self._current);self._last_sequence=-1;self._last_stamp=-1
        if self._current:
            frame=_raw_frame(self.directory/self._current[-1]['path'])
            self._last_sequence=int(frame['sdk.source_sequence'].item());self._last_stamp=int(frame['sdk.captured_monotonic_ns'].item())
        self._robot=None;self._leader=None;self._leader_lock=None
        self._task_lock=DeviceLock('collection-task:'+str(self.directory.resolve()))
        self.events=EventHub(64)
        self._writer_factory=SeeedWriter  # private fault-injection seam; production always imports Seeed

    def status(self):
        with self._lock:
            return CollectionStatus(self._journal['task_id'],self._state,len(self._commits),self._frame_count,self._error)

    def _persist(self,state,*,dataset=None):
        with self._lock:
            current=None if self._episode_dir is None else {'directory':self._episode_dir.relative_to(self.directory).as_posix(),'files':copy.deepcopy(self._current)}
            value={**self._journal,'state':state.value,'commits':copy.deepcopy(self._commits),'current':current,'error':self._error,
                   'discard_tombstones':copy.deepcopy(self._discard_tombstones)}
            if dataset is not None:value['dataset']=dataset
            atomic_json(self.directory/'task.json',value)
            self._journal=value;self._state=state
        self.events.emit('collection_state',state=state.value,episodes=len(self._commits))

    def frozen_commits(self):
        """Deep-copy durable commits for snapshots; exclude unsubmitted frames.

        Callers cannot mutate the live journal or existing commit hashes through
        the returned value. The established commit format remains unchanged.
        """
        with self._lock:return copy.deepcopy(self._journal['commits'])

    def _bind_source_metadata(self,value):
        """Internal trusted start evidence, durably kept before capture begins."""
        value=bounded_metadata(value)
        with self._lock:
            if self._thread and self._thread.is_alive():raise StateError('Cannot change source evidence during recording')
            history=copy.deepcopy(self._journal.get('source_runtime_provenance',[]))
            if value is not None and value not in history:history.append(value)
            updated={**self._journal,'source_runtime_provenance':history}
            atomic_json(self.directory/'task.json',updated);self._journal=updated

    def start(self,robot,leader):
        with self._lock:
            if self._pending and self._pending[0]=='start':return self._pending[1]
            if self._thread and self._thread.is_alive():raise StateError('Task already started')
            if self._state not in (CollectionState.CREATED,CollectionState.RECOVERABLE):raise StateError('Task cannot start in current state')
            if self._stop_operation is not None:raise StateError('Reopen interrupted tasks before restarting')
            self._task_lock.acquire()
            self._robot,self._leader=robot,leader
            if self._current:
                # RobotSession sequence is an SDK-local observation counter,
                # not an encoder/device clock. A reopened session must retain
                # its episode's numbering; real capture times remain untouched.
                with robot._lock:robot._sequence=max(robot._sequence,self._last_sequence+1)
            self._stop_requested=False;self._suspend_operation=None
            operation=Operation();self._pending=('start',operation)
            self._state=CollectionState.STARTING
            self._thread=threading.Thread(target=self._run,args=(operation,),name='CollectionTask-'+self.config.name,daemon=True)
            self._thread.start()
            return operation

    def _submit(self,kind):
        with self._lock:
            if self._pending:
                if self._pending[0]==kind:return self._pending[1]
                raise BusyError('Another episode transaction is in progress')
            if self._stop_operation is not None:raise StateError('Task stop has begun')
            required=CollectionState.RESETTING if kind in ('next','resume') else CollectionState.RECORDING
            if self._state!=required:raise StateError(f'{kind} requires {required.value}')
            operation=Operation();self._pending=(kind,operation)
            self._state={'save':CollectionState.SAVING,'redo':CollectionState.REDOING,'next':CollectionState.STARTING,'resume':CollectionState.STARTING}[kind]
            self._commands.put((kind,operation,None))
            return operation

    def save_episode(self):return self._submit('save')

    def commit_preserved_episode(self):
        """Commit stopped raw frames only; encoding is done by the snapshot worker.

        This never connects a robot, starts a writer or records another frame.
        The recorder lock and freshly read journal prevent a live/stale submit.
        """
        with self._lock:
            if self._thread and self._thread.is_alive():raise StateError('Recorder is still running')
            if self._state!=CollectionState.RECOVERABLE:raise StateError('Only preserved stopped data may be committed')
            self._task_lock.acquire()
            try:
                fresh=read_json(self.directory/'task.json')
                if fresh.get('commits')!=self._commits or fresh.get('current')!=self._journal.get('current'):
                    raise ContractMismatch('Preserved journal changed; reopen before submitting')
                _verify_commits(self.directory,self._commits)
                _verify_current(self.directory,fresh.get('current'),self._commits)
                return self._commit_raw(CollectionState.RECOVERABLE)
            finally:self._task_lock.release()
    def redo_episode(self):return self._submit('redo')
    def start_episode(self):return self._submit('next')

    def resume_episode(self):
        """Explicitly resume the same preserved episode after start/rebuild.

        Source time and sequence must advance. This operation succeeds only
        after an additional raw frame is durable; opening/start never moves.
        """
        return self._submit('resume')

    def suspend(self):
        """Stop writing, confirm HOLD, preserve frames and terminate the worker.

        Does not discard, commit or publish the current episode. Reopen/start
        then explicitly resume to continue; a wait timeout does not cancel.
        """
        with self._lock:
            if self._suspend_operation is not None:return self._suspend_operation
            if self._stop_operation is not None:raise BusyError('Final stop already requested')
            if not self._thread or not self._thread.is_alive():
                if self._state!=CollectionState.RECOVERABLE:raise StateError('Task is not running')
                _verify_commits(self.directory,self._commits)
                current=None if self._episode_dir is None else {'directory':self._episode_dir.relative_to(self.directory).as_posix(),'files':copy.deepcopy(self._current)}
                _verify_current(self.directory,current,self._commits)
                self._persist(CollectionState.RECOVERABLE)
                operation=Operation();operation._finish(self.status());return operation
            operation=Operation();self._suspend_operation=operation;self._stop_requested=True
            self._commands.put(('suspend',operation,None))
        # Reach an opted-in priority channel before the recorder waits on a
        # blocked frame/servo callback. No I/O or waiting under the journal lock.
        self._robot._signal_stop_intent('collection suspended; preserve unsubmitted frames')
        return operation

    def stop(self,save_current=True):
        if not isinstance(save_current,bool):raise ConfigurationError('save_current must be bool')
        with self._lock:
            if self._suspend_operation is not None:raise BusyError('Preservation stop already requested; reopen before finalizing')
            if self._stop_operation is not None:return self._stop_operation
            operation=Operation()
            if self._state==CollectionState.LOCAL_READY:
                d=self._journal['dataset']
                operation._finish(DatasetRef(self.directory/d['path'],d['dataset_id'],d['version'],d['content_hash']))
                self._stop_operation=operation
                return operation
            if not self._thread or not self._thread.is_alive():
                raise StateError('Task is not running; explicitly start or reopen it')
            self._stop_requested=True;self._stop_operation=operation
            self._commands.put(('stop',operation,save_current))
        self._robot._signal_stop_intent('collection final stop')
        return operation

    def _validate_borrowed(self):
        c=self.config;r=self._robot
        if (r.config.robot_id!=c.robot_id or r.config.calibration_hash!=c.calibration_hash or r.config.units!=c.units):
            raise ContractMismatch('Collection device reference differs from connected RobotSession')
        if c.fps>r.config.fps:raise ConfigurationError('Collection fps cannot exceed robot sampling fps')
        if not getattr(self._leader,'is_connected',False):raise ConfigurationError('Borrow an already connected leader')
        if not isinstance(getattr(self._leader,'resource_id',None),str) or not self._leader.resource_id:
            raise ConfigurationError('Leader needs a canonical resource_id')
        if not callable(getattr(self._leader,'get_action',None)):raise ConfigurationError('Leader get_action(timeout_s) required')
        self._observation()
        disk_check(self.directory,c.min_free_bytes)

    def _observation(self):
        obs=self._robot.get_observation(self.config.task)
        for key,shape in self.config.camera_shapes.items():
            if key not in obs.images or obs.images[key].rgb.shape!=shape:
                raise ContractMismatch('Required camera missing or wrong shape')
        return obs

    def _begin_episode(self):
        if self._current:raise StateError('Preserved current episode must be resumed or explicitly discarded')
        scope=self._robot._control_scope('collection',deadline_s=self.config.max_duration_s)
        scope.__enter__()
        self._scope=scope
        self._episode_dir=self.directory/'episodes'/uuid.uuid4().hex
        self._episode_dir.mkdir()
        self._current=[];self._frame_count=0;self._last_sequence=-1;self._last_stamp=-1;self._held=None
        self._missing_actions=0;self._skipped_actions=0
        self._episode_started=time.monotonic();self._next_tick=self._episode_started
        self._recording=True
        self._persist(CollectionState.STARTING)

    def _resume_episode(self):
        if self._episode_dir is None:raise StateError('No preserved current episode')
        obs=self._observation()
        if obs.arm.captured_monotonic_ns<=self._last_stamp:raise ContractMismatch('Resumed source clock is not after the preserved frame')
        if obs.arm.sequence<=self._last_sequence:raise ContractMismatch('Resumed source sequence restarted before the preserved frame')
        scope=self._robot._control_scope('collection',deadline_s=self.config.max_duration_s)
        scope.__enter__();self._scope=scope
        self._episode_started=time.monotonic();self._next_tick=self._episode_started
        self._held=None;self._missing_actions=0;self._skipped_actions=0
        self._recording=True;self._persist(CollectionState.STARTING)

    def _first_frame(self):
        """A recording-start receipt certifies a frame, not just a live thread."""
        while not self._stop_requested:
            if self._frame():return
            time.sleep(min(.01,1/self.config.fps))
        raise StateError('Recording interrupted before the first durable frame')

    def _pause(self):
        self._recording=False
        self._held=None   # 压着的最后一帧没有"下一帧"，丢掉（action=next_observation_state）
        if self._scope is not None:
            scope=self._scope;self._scope=None
            scope.__exit__(None,None,None)  # HOLD before encoding/disk finalization, never disconnect

    def _frame(self):
        c=self.config
        if self._frame_count>=c.max_episode_frames or time.monotonic()-self._episode_started>=c.max_duration_s:
            raise OperationFailed('Episode recording budget exhausted; recover committed episodes')
        disk_check(self.directory,c.min_free_bytes)
        obs=self._observation()
        if obs.arm.sequence<=self._last_sequence:return
        if obs.arm.captured_monotonic_ns<=self._last_stamp:raise ContractMismatch('Source capture time is not monotonic')
        requested=self._leader.get_action(timeout_s=c.leader_timeout_s)
        if not isinstance(requested,JointTarget):raise ContractMismatch('Leader returns an explicitly mapped JointTarget')
        provenance={}
        if obs.source_metadata is not None:provenance['observation']=bounded_metadata(obs.source_metadata)
        getter=getattr(self._leader,'frame_provenance',None)
        if callable(getter):
            used=getter()
            if used is not None:provenance['leader']=bounded_metadata(used)
        provenance=bounded_metadata(provenance) if provenance else None
        if self._stop_requested:return
        try:receipt=self._robot._collection_step(requested)
        except (OperationCancelled,StateError):
            if self._stop_requested:return
            raise
        if self._stop_requested:return  # A late accepted callback is not a new durable frame.
        frame=make_frame(c,obs,receipt)
        rule=action_rule_of(c)
        if rule=='sent_target':
            self._write(frame,provenance)
            self._last_sequence=obs.arm.sequence
            self._last_stamp=obs.arm.captured_monotonic_ns
            self._finish_write()
            return True
        if rule in ('source_action','leader_action'):   # leader_action（2026-11-05）：主臂指令，数据源按时间配好对，写法相同
            return self._source_action_frame(frame,provenance,obs,rule)
        # 2026-11-03 数据源没有下发的关节指令（Bobac3）：action = 下一帧的实测状态。
        # 这一帧先压在内存里；下一帧观测到了，把它的实测状态填进压着那一帧的 action 再写盘。
        held,self._held=self._held,(frame,provenance)
        self._last_sequence=obs.arm.sequence
        self._last_stamp=obs.arm.captured_monotonic_ns
        if held is None:return False
        previous,previous_provenance=held
        following=np.array(dataset_vector(c,obs.arm.positions),dtype=np.float32)
        for key in ('action','action.executed','action.requested'):previous[key]=following.copy()
        previous_provenance=bounded_metadata({**(previous_provenance or {}),'action':{
            'rule':'next_observation_state','observation_sequence':obs.arm.sequence,
            'observation_captured_monotonic_ns':obs.arm.captured_monotonic_ns}})
        self._write(previous,previous_provenance)
        self._finish_write()
        return True

    def _source_action_frame(self,frame,provenance,obs,rule='source_action'):
        """2026-11-03 数据源随观测给出这一帧实际下发的目标（数据源按观测编号配好对，放在 source_metadata['source_action']）。

        action / action.executed = 数据源报的实际下发目标，action.requested = 数据源报的原本想要的目标（没有就同上）。
        没配上（或数值不合法）的帧跳过不写，不拿状态顶替；连续 MAX_MISSING_SOURCE_ACTIONS 帧都没配上就判这一条失败。
        """
        c=self.config
        self._last_sequence=obs.arm.sequence
        self._last_stamp=obs.arm.captured_monotonic_ns
        source=(obs.source_metadata or {}).get('source_action')
        try:
            if not isinstance(source,dict):raise ContractMismatch('No paired source action for this observation')
            sent=JointTarget(source['positions'],c.units,c.calibration_hash)
            requested=JointTarget(source.get('requested_positions') or source['positions'],c.units,c.calibration_hash)
            self._robot.config.validate_target(sent);self._robot.config.validate_target(requested)
        except (SDKError,KeyError,TypeError,ValueError) as exc:
            self._missing_actions+=1;self._skipped_actions+=1
            if self._missing_actions>=MAX_MISSING_SOURCE_ACTIONS:
                raise OperationFailed(f'No valid paired source action for {self._missing_actions} consecutive frames: {exc}') from exc
            return False
        frame['action']=np.array(dataset_vector(c,sent.positions),dtype=np.float32)
        frame['action.executed']=frame['action'].copy()
        frame['action.requested']=np.array(dataset_vector(c,requested.positions),dtype=np.float32)
        details={k:source[k] for k in ('action_id','observation_id','process_id','delay_s','seq','action_timestamp',
                                       'state_sampled_at','leader_serial') if k in source}
        provenance=bounded_metadata({**(provenance or {}),'action':{'rule':rule,**details,
                                                                   'skipped_frames_before':self._skipped_actions}})
        self._missing_actions=0;self._skipped_actions=0
        self._write(frame,provenance)
        self._finish_write()
        return True

    def _write(self,frame,provenance):
        self._writer.add(frame)
        if provenance is not None:
            # Native training features stay unchanged. The raw file hash binds
            # this exact USED packet and original clock evidence to its frame.
            frame[SOURCE_FIELD]=np.asarray(json.dumps(provenance,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False))
        # A process can die after raw fsync but before the current journal update.
        # Such an unacknowledged tail stays preserved and is never overwritten,
        # adopted as a successful frame, or allowed to collide with a retry.
        path=self._episode_dir/f'{len(self._current):08d}-{uuid.uuid4().hex}.npz'
        with path.open('xb') as stream:
            np.savez(stream,**frame);stream.flush();os.fsync(stream.fileno())
        self._current.append({'path':path.relative_to(self.directory).as_posix(),'sha256':sha_file(path)})

    def _finish_write(self):
        with self._lock:self._frame_count=len(self._current)
        self._persist(CollectionState.RECORDING)

    def _save(self):
        if not self._current:raise StateError('Cannot save an empty episode')
        self._writer.save()
        return self._commit_raw(CollectionState.RESETTING)

    def _commit_raw(self,state):
        if not self._current:raise StateError('Cannot save an empty episode')
        entry={'episode_index':len(self._commits),'frames':len(self._current),'task':self.config.task,'files':list(self._current)}
        entry['content_hash']=digest(entry)
        with self._lock:
            previous=list(self._commits);current=self._current;directory=self._episode_dir;count=self._frame_count
            self._commits.append(entry);self._current=[];self._episode_dir=None;self._frame_count=0
            try:self._persist(state)
            except Exception:
                self._commits=previous;self._current=current;self._episode_dir=directory;self._frame_count=count
                raise
        return EpisodeInfo(entry['episode_index'],entry['frames'],entry['task'],entry['content_hash'])

    def _discard(self):
        self._writer.clear()
        with self._lock:
            current=self._current;directory=self._episode_dir;count=self._frame_count
            previous=copy.deepcopy(self._discard_tombstones)
        if directory is not None:
            committed_paths={Path(item['path']).parent for ep in self._commits for item in ep['files']}
            if directory.relative_to(self.directory) in committed_paths:
                raise ContractMismatch('Cannot discard a committed episode')
            _episode_directory(self.directory,directory.relative_to(self.directory).as_posix())
        with self._lock:
            self._current=[];self._episode_dir=None;self._frame_count=0
            if directory is not None:
                self._discard_tombstones.append({'directory':directory.relative_to(self.directory).as_posix(),'reason':'explicit_unsubmitted_discard'})
            try:self._persist(CollectionState.RESETTING)
            except BaseException:
                self._current=current;self._episode_dir=directory;self._frame_count=count;self._discard_tombstones=previous
                raise
        # Destructive work starts only after the journal can recover without it.
        state=_finish_discard_tombstones(self.directory,self._journal)
        with self._lock:self._journal=state;self._discard_tombstones=copy.deepcopy(state.get('discard_tombstones',[]))

    def _prepare(self):
        upstream()  # Fail before claiming devices when the fixed optional dependency is missing.
        self._validate_borrowed()
        self._leader_lock=DeviceLock('collection-leader:'+self._leader.resource_id)
        self._leader_lock.acquire()
        _verify_commits(self.directory,self._commits)
        _verify_current(self.directory,self._journal.get('current'),self._commits)
        self._attempt=self.directory/'attempts'/uuid.uuid4().hex
        self._attempt.mkdir()
        self._data_dir=self._attempt/'dataset'
        self._writer=self._writer_factory(self._data_dir,self.config)
        for episode in self._commits:
            for frame in episode['files']:self._writer.add(training_frame(_raw_frame(self.directory/frame['path'])))
            self._writer.save()
        for frame in self._current:self._writer.add(training_frame(_raw_frame(self.directory/frame['path'])))
        self._error=None
        if self._commits or self._recovering:
            self._persist(CollectionState.RESETTING)  # Recovery never starts an episode or motion implicitly.
        else:self._begin_episode()

    def _finalize(self,save_current):
        self._pause()
        if self._current:
            if save_current:self._save()
            else:self._discard()
        if not self._commits:raise StateError('No committed episodes; cannot publish an empty dataset')
        self._persist(CollectionState.FINALIZING)
        self._writer.finalize()
        provenance=export_frame_provenance(self.directory,self._commits,self._data_dir)
        c=self.config
        metadata={'dataset_id':c.name,'version':self._attempt.name,'robot_type':robot_type_of(c),
                  'robot_id':c.robot_id,'fps':c.fps,'joint_names':joint_names_of(c),
                  'units':joint_units_of(c),'calibration_hash':c.calibration_hash,
                  'camera_keys':list(c.camera_shapes),'camera_shapes':dict(c.camera_shapes),
                  'task_labels':[c.task],'description':c.description,'episodes':len(self._commits),
                  'episode_lengths':[ep['frames'] for ep in self._commits],
                  'action_semantics':action_semantics_of(c),
                   'collection_id':self._journal['task_id'],'source_episode_hashes':[ep['content_hash'] for ep in self._commits],
                   'source_provenance':provenance,'source_runtime_provenance':copy.deepcopy(self._journal.get('source_runtime_provenance',[]))}
        if c.dataset_schema and 'from_internal' in c.dataset_schema:   # 只写了 action 的标准 SO101：关节不换算
            metadata['joint_mapping']={'internal_names':list(JOINT_NAMES),'internal_units':[c.units[k] for k in JOINT_NAMES],
                                       'from_internal':c.dataset_schema['from_internal']}
        ref=seal_dataset(self._data_dir,metadata)
        final=self.directory/'versions'/self._attempt.name
        os.rename(self._data_dir,final)
        d={'path':final.relative_to(self.directory).as_posix(),'dataset_id':ref.dataset_id,
           'version':ref.version,'content_hash':ref.content_hash}
        self._persist(CollectionState.LOCAL_READY,dataset=d)
        return DatasetRef(final,ref.dataset_id,ref.version,ref.content_hash)

    def _complete(self,kind,operation,result=None):
        with self._lock:
            if self._pending and self._pending[1] is operation:self._pending=None
            operation._finish(result)

    def _run(self,start_operation):
        active=start_operation
        terminal_result=None;terminal_error=None
        try:
            self._prepare()
            if self._recording:self._first_frame()
            self._complete('start',start_operation,self.status())
            while True:
                wait=max(0,self._next_tick-time.monotonic()) if self._recording else None
                try:kind,active,arg=self._commands.get(timeout=wait)
                except queue.Empty:
                    self._frame();self._next_tick=max(self._next_tick+1/self.config.fps,time.monotonic())
                    continue
                if kind=='next':
                    self._validate_borrowed();self._begin_episode();self._first_frame();result=self.status()
                elif kind=='resume':
                    self._validate_borrowed();self._resume_episode();self._first_frame();result=self.status()
                elif kind=='save':
                    self._pause()
                    if not self._current:
                        self._persist(CollectionState.RESETTING)
                        with self._lock:
                            self._pending=None
                            active._finish(error=StateError('Cannot save an empty episode'))
                        continue
                    result=self._save()
                elif kind=='redo':
                    self._pause();self._discard();result=self.status()
                elif kind=='suspend':
                    self._pause()
                    stopped=self._robot.stop('collection suspended; unsubmitted data preserved')
                    if stopped.holding is not True:raise OperationFailed('Collection suspend HOLD is unconfirmed')
                    self._persist(CollectionState.RECOVERABLE)
                    terminal_result=self.status();break
                else:
                    terminal_result=self._finalize(arg);break
                self._complete(kind,active,result)
        except Exception as exc:
            error=exc if isinstance(exc,SDKError) else OperationFailed(str(exc))
            try:self._pause()
            except Exception as stop_error:error=OperationFailed(f'{error}; HOLD unconfirmed: {stop_error}')
            self._error=str(error)
            try:self._persist(CollectionState.RECOVERABLE)
            except Exception:
                with self._lock:self._state=CollectionState.RECOVERABLE
            terminal_error=error
        finally:
            if self._writer is not None:
                try:self._writer.abort()
                except Exception as exc:self.events.emit('writer_cleanup_failed',error=str(exc))
            if self._leader_lock is not None:self._leader_lock.release()
            self._task_lock.release()
            with self._lock:
                if terminal_error:
                    if active is not None:active._finish(error=terminal_error)
                    if self._pending:self._pending[1]._finish(error=terminal_error)
                    if self._stop_operation:self._stop_operation._finish(error=terminal_error)
                    if self._suspend_operation:self._suspend_operation._finish(error=terminal_error)
                    while not self._commands.empty():self._commands.get()[1]._finish(error=terminal_error)
                elif self._stop_operation is not None:self._stop_operation._finish(terminal_result)
                elif self._suspend_operation is not None:self._suspend_operation._finish(terminal_result)
                self._pending=None
