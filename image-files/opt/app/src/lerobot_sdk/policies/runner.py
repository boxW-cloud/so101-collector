"""Bounded action scheduling. GPU work never owns the robot stop path."""
from collections import deque,OrderedDict
import queue
import threading
import time
import uuid
from lerobot_sdk.core.types import JOINT_NAMES,JointTarget,SessionState,finite
from lerobot_sdk.policies.contracts import RunConfig,RunState,RunStatus,RunResult,ActionEnvelope
from lerobot_sdk.core.errors import BusyError,ConfigurationError,ContractMismatch,OperationFailed,WaitTimeout,StateError
from lerobot_sdk.core.events import EventHub


class InferenceRunner:
    def __init__(self):self._lock=threading.Lock();self._run=None

    def start(self,robot,session,config=None):
        config=config or RunConfig()
        if not isinstance(config,RunConfig):raise ConfigurationError('RunConfig required')
        with self._lock:
            if self._run is not None and not self._run._done.is_set():raise BusyError('Runner already active')
            r=RunHandle(robot,session,config)
            self._run=r;r._start()
            return r

    def stop(self):
        if self._run is None:raise StateError('No inference run')
        return self._run.stop()

    def wait(self,timeout_s=None):
        if self._run is None:raise StateError('No inference run')
        return self._run.wait(timeout_s)

    def status(self):
        if self._run is None:raise StateError('No inference run')
        return self._run.status()

    def iter_events(self,timeout_s=None):
        if self._run is None:raise StateError('No inference run')
        return self._run.iter_events(timeout_s)


class RunHandle:
    def __init__(self,robot,session,config):
        self.id=uuid.uuid4().hex;self.robot=robot;self.session=session;self.config=config
        if (robot.config.robot_id!=session.contract.robot_id or robot.config.calibration_hash!=session.contract.calibration_hash or
            robot.config.units!=session.contract.units or robot.config.limits!=session.contract.limits):
            raise ContractMismatch('Runner robot and policy contracts differ')
        caps=session.capabilities
        if config.mode=='step' and not caps.supports_step or config.mode=='chunk' and not caps.supports_chunk:
            raise ConfigurationError('Requested native policy mode is unsupported')
        self.period_s=session.period_s
        if config.fps is not None and abs(config.fps-1/self.period_s)>1e-7:
            raise ContractMismatch('Runner fps must match model action period')
        if 1/self.period_s>robot.config.fps:raise ConfigurationError('Model control rate exceeds robot sample rate')
        self._reservation=caps.chunk_size if config.mode=='chunk' else 1
        if self._reservation>config.queue_capacity:raise ConfigurationError('Queue cannot hold a native prediction chunk')
        self._max_age=min(config.max_observation_age_s,session.max_age_s)
        self._lock=threading.RLock();self._halt_lock=threading.Lock()
        self._stop=threading.Event();self._done=threading.Event();self._ready=threading.Event()
        self._model_done=threading.Event();self._halted=False;self._claimed=False
        self._requests=queue.Queue(maxsize=1);self._responses=queue.Queue(maxsize=1)
        self._queue=deque();self._periods=deque(maxlen=config.metrics_capacity)
        self._sources=OrderedDict();self._computing=False;self._pending=False
        self._state=RunState.STARTING;self._error=None;self._holding=False
        self._steps=0;self._rejected=0;self._underflows=0;self._reason=''
        self._expected_chunk=0;self._expected_action=0;self._last_executed=-1
        self.events=EventHub(128);self._result=None
        self._startup_error=None

    def _start(self):
        self.session._borrow(self.id)
        self._session_epoch=self.session.epoch
        self._controller=threading.Thread(target=self._control,name='InferenceControl-'+self.id,daemon=True)
        self._controller.start()
        if not self._ready.wait(self.config.stop_timeout_s):
            self._stop.set();raise OperationFailed('Control startup did not finish within its deadline')
        if self._startup_error is not None:raise self._startup_error
        if self._done.is_set() and self._error:raise OperationFailed(self._error)

    def status(self):
        with self._lock:
            samples=sorted(self._periods)
            def percentile(p):return samples[min(len(samples)-1,int((len(samples)-1)*p))]*1000 if samples else 0.0
            return RunStatus(self.id,self._state,self._steps,len(self._queue),self._computing,self._holding,
                             self._rejected,self._underflows,percentile(.5),percentile(.95),percentile(.99),self._error)

    def _inference_worker(self):
        try:
            while not self._stop.is_set():
                try:observation=self._requests.get(timeout=.05)
                except queue.Empty:continue
                if self._stop.is_set():break
                with self._lock:self._computing=True
                try:result=self.session._packet(observation,self.config.mode,borrower=self.id);error=None
                except Exception as exc:result=None;error=exc
                finally:
                    with self._lock:self._computing=False
                if not self._stop.is_set():self._responses.put_nowait((result,error))
        finally:
            try:self.session._release(self.id)
            except Exception as exc:self.events.emit('session_cleanup_failed',error=str(exc))
            finally:self._model_done.set()

    def _accept(self,packet):
        before=self._rejected
        try:return self._validate_packet(packet)
        except Exception as exc:
            if self._rejected==before:
                self._rejected+=len(packet.chunk.values) if isinstance(packet,ActionEnvelope) else 1
            self.events.emit('chunk_rejected',reason=str(exc))
            raise

    def _validate_packet(self,packet):
        if not isinstance(packet,ActionEnvelope):raise ContractMismatch('Policy must provide a typed lineage envelope')
        if packet.session_id!=self.session.id or packet.epoch!=self._session_epoch or packet.model_version!=self.session.model_version:
            raise ContractMismatch('Old session/epoch or wrong model result')
        if packet.chunk_sequence!=self._expected_chunk or packet.first_action_index!=self._expected_action:
            raise ContractMismatch('Duplicate, out-of-order or missing action index')
        chunk=packet.chunk
        stamp=self._sources.get(chunk.source_sequence)
        if stamp is None or stamp!=packet.captured_monotonic_ns:raise ContractMismatch('Unknown observation origin')
        if chunk.names!=JOINT_NAMES or tuple(chunk.units)!=tuple(self.robot.config.units[n] for n in JOINT_NAMES):
            raise ContractMismatch('Chunk joint or unit contract mismatch')
        if abs(chunk.period_s-self.period_s)>1e-8:raise ContractMismatch('Chunk period differs from model contract')
        budget=min(self._max_age,chunk.max_observation_age_s)
        deadline=stamp+int(budget*1e9)
        if not stamp<=packet.generated_monotonic_ns<=time.monotonic_ns() or time.monotonic_ns()>deadline:
            self._rejected+=len(chunk.values);raise OperationFailed('Inference result expired before execution')
        if len(self._queue)+len(chunk.values)>self.config.queue_capacity:raise ContractMismatch('Policy exceeded reserved queue capacity')
        for index,row in enumerate(chunk.values):
            target=JointTarget(dict(zip(JOINT_NAMES,row,strict=True)),self.robot.config.units,self.robot.config.calibration_hash)
            self.robot.config.validate_target(target)
            self._queue.append((target,deadline,packet.first_action_index+index,chunk.source_sequence))
        self._expected_chunk+=1;self._expected_action+=len(chunk.values)
        self.events.emit('chunk_accepted',sequence=packet.chunk_sequence,source_sequence=chunk.source_sequence,actions=len(chunk.values))

    def _request(self,observation):
        self._sources[observation.arm.sequence]=observation.arm.captured_monotonic_ns
        while len(self._sources)>max(256,self.config.queue_capacity*2):self._sources.popitem(last=False)
        self._requests.put_nowait(observation);self._pending=True;self._pending_since=time.monotonic()

    def _halt(self):
        with self._halt_lock:
            if self._halted:return
            if not self._claimed:return  # startup may claim the token later; don't cache a fictitious stop
            if self._claimed:
                status=self.robot.status()
                if status.epoch==self._robot_epoch and status.control_owner=='inference':
                    result=self.robot.stop('inference run '+self.id+' stopped')
                    self._holding=result.holding
                elif status.state==SessionState.HOLDING and status.control_owner is None:
                    self._holding=True  # another explicit stop already invalidated our token
                else:
                    self._holding=False
                    raise OperationFailed('Run no longer owns the robot; cannot claim a confirmed HOLD')
            self._halted=True

    def _control(self):
        worker_started=False;success=False
        try:
            with self.robot._control_scope('inference',deadline_s=self.config.max_duration_s+self.config.stop_timeout_s):
                self._claimed=True;self._robot_epoch=self.robot.status().epoch
                self.robot._inference_hold();self._holding=True
                if self._stop.is_set():return
                threading.Thread(target=self._inference_worker,name='PolicyCompute-'+self.id,daemon=True).start()
                worker_started=True;self._state=RunState.RUNNING;self._ready.set()
                start=time.monotonic();previous=start
                while not self._stop.is_set():
                    tick=time.monotonic()
                    if self._steps>=self.config.max_steps or tick-start>=self.config.max_duration_s:
                        self._reason='max_steps' if self._steps>=self.config.max_steps else 'max_duration'
                        success=True;break
                    state=self.robot.status()
                    if state.epoch!=self._robot_epoch and state.state==SessionState.HOLDING and state.control_owner is None:
                        self._reason='robot_stop';break
                    if state.epoch!=self._robot_epoch or state.state in (SessionState.FAULT,SessionState.E_STOP):
                        raise OperationFailed('Robot control epoch changed or a fault was latched')
                    observation=self.robot.get_observation(self.session.task,max_age_s=self._max_age)
                    if self._pending:
                        try:packet,error=self._responses.get_nowait()
                        except queue.Empty:
                            if tick-self._pending_since>self.config.inference_timeout_s:raise OperationFailed('Inference exceeded its deadline')
                        else:
                            self._pending=False
                            if error:raise OperationFailed('Policy inference failed: '+str(error))
                            with self._lock:self._accept(packet)
                    with self._lock:
                        while self._queue and time.monotonic_ns()>self._queue[0][1]:
                            self._queue.popleft();self._rejected+=1
                        action=self._queue.popleft() if self._queue else None
                    if action is None:
                        if not self._holding:
                            self.robot._inference_hold();self._holding=True;self._underflows+=1
                            self.events.emit('queue_empty_hold')
                    elif not self._stop.is_set():
                        target,deadline,index,source_sequence=action
                        if index<=self._last_executed:raise ContractMismatch('Action index was already executed')
                        if time.monotonic_ns()>deadline:
                            self._rejected+=1
                            self.robot._inference_hold();self._holding=True
                        else:
                            receipt=self.robot.move_to(target,mode='servo')
                            with self._lock:
                                if not self._stop.is_set():self._holding=False
                                self._last_executed=index;self._steps+=1
                            self.events.emit('action_sent',index=index,source_sequence=source_sequence,
                                             sent_monotonic_ns=receipt.sent_monotonic_ns,
                                             measured_before_send=dict(observation.arm.positions),measured_sequence=observation.arm.sequence,
                                             requested=dict(receipt.requested_target.positions),sent=dict(receipt.sent_target.positions))
                    if (not self._pending and not self._stop.is_set() and len(self._queue)<=self.config.low_watermark
                        and self.config.queue_capacity-len(self._queue)>=self._reservation):self._request(observation)
                    with self._lock:self._periods.append(tick-previous)
                    previous=tick
                    self._stop.wait(max(0,self.period_s-(time.monotonic()-tick)))
                self._stop.set()
                self._halt()
        except Exception as exc:
            if not self._ready.is_set():self._startup_error=exc
            if not (self._stop.is_set() and self._reason=='requested'):self._error=str(exc)
        finally:
            self._stop.set()
            with self._lock:self._queue.clear()
            try:self._halt()
            except Exception as exc:self._error=self._error or str(exc)
            if not worker_started:
                try:self.session._release(self.id)
                finally:self._model_done.set()
            with self._lock:
                self._state=RunState.FAULT if self._error else RunState.SUCCEEDED if success else RunState.STOPPED
                self._reason=self._reason or ('fault' if self._error else 'requested')
                self._result=RunResult(self.id,self._state,self._steps,self._reason,self._holding,self._computing)
                self.events.emit('run_stopped',state=self._state.value,steps=self._steps,holding=self._holding,error=self._error)
                self._ready.set();self._done.set()

    def stop(self):
        with self._lock:
            if self._done.is_set():return self.status()
            self._state=RunState.STOPPING;self._reason='requested';self._stop.set();self._queue.clear()
        try:self._halt()
        except Exception as exc:
            with self._lock:self._error=str(exc)
        if not self._done.wait(self.config.stop_timeout_s):raise OperationFailed('Control stop was not confirmed in time')
        if not self._holding:raise OperationFailed(self._error or 'HOLD not confirmed')
        return self.status()

    def wait(self,timeout_s=None):
        if timeout_s is not None:timeout_s=finite(timeout_s,'wait timeout',nonnegative=True)
        if not self._done.wait(timeout_s):raise WaitTimeout('Run wait expired; run was not stopped')
        if self._error:raise OperationFailed(self._error)
        return self._result

    def wait_inference(self,timeout_s=None):
        if timeout_s is not None:timeout_s=finite(timeout_s,'inference cleanup wait',nonnegative=True)
        if not self._model_done.wait(timeout_s):raise WaitTimeout('Robot is stopped but native inference has not returned yet')

    def iter_events(self,timeout_s=None):
        subscription=self.events.subscribe()
        try:
            while not self._done.is_set():
                try:yield subscription.get(timeout_s if timeout_s is not None else .1)
                except queue.Empty:
                    if timeout_s is not None:break
        finally:self.events.unsubscribe(subscription)
