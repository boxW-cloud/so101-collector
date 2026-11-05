from __future__ import annotations
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import threading
import time
import uuid

from lerobot_sdk.control.backend import BackendSnapshot
from lerobot_sdk.core.errors import (SDKError, BusyError, ConfigurationError, ContractMismatch, DataNotReady,
                     DeviceDisconnected, OperationCancelled, OperationFailed, StateError)
from lerobot_sdk.core.events import EventHub
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.operation import ActionHandle
from lerobot_sdk.core.types import (JOINT_NAMES, JointTarget, ArmState, Observation, ImageFrame, ActionReceipt,
                    ActionResult, StopResult, SessionState, SessionStatus, RobotConfig, finite)

@dataclass(frozen=True)
class _Context:
    robot:object
    nonce:str
    epoch:int
    deadline_ns:int
    kind:str

_CONTROL=ContextVar('lerobot_sdk_control',default=None)

class RobotSession:
    """Own a simulation backend and one bounded I/O worker.

    Targets are never measured feedback. stop() waits for a confirmed backend
    HOLD, invalidates old epochs, and does not disable support. This is not an
    implementation of a physical independent emergency-stop circuit.
    """
    def __init__(self,config:RobotConfig,backend=None,*,lock_dir=None,physical_calibration_hash=None):
        if not isinstance(config,RobotConfig):raise ConfigurationError('RobotConfig required')
        if getattr(backend,'kind',None)!='simulation':
            if not (getattr(backend,'kind',None)=='physical' and getattr(backend,'commissioned',False) is True and physical_calibration_hash==config.calibration_hash):
                raise ConfigurationError('Physical backends require explicit commissioned calibration authorization')
        if not isinstance(getattr(backend,'resource_id',None),str) or not backend.resource_id:
            raise ConfigurationError('Backend must identify its canonical resource')
        self.config=config;self._backend=backend;self._device_lock=DeviceLock(backend.resource_id,lock_dir)
        self._lock=threading.RLock();self._ready=threading.Event();self._exit=threading.Event()
        self._thread=None;self._sample=None;self._sample_started_ns=0;self._sequence=0;self._epoch=0;self._state=SessionState.DISCONNECTED
        self._fault=None;self._connection_error=None;self._owner=None;self._nonce=None
        self._close_error=None
        self._deadline_ns=0;self._motion=None;self._servo=None;self._stop_request=None
        self._stop_guard=threading.Lock();self._stop_intent=threading.Event();self._priority_stop=None
        self._last_sent=None;self._last_send_time=None
        self.events=EventHub(config.event_capacity)

    @property
    def contract(self):return self.config
    def status(self):
        with self._lock:return SessionStatus(self._state,self._epoch,self._owner,self._fault)
    def connect(self):
        with self._lock:
            if self._thread and self._thread.is_alive():raise StateError('Already connected/connecting')
            self._device_lock.acquire();self._ready.clear();self._exit.clear();self._connection_error=None;self._close_error=None
            self._fault=None;self._sample=None;self._sample_started_ns=0;self._sequence=0
            with self._stop_guard:self._priority_stop=None;self._stop_intent.clear()
            self._thread=threading.Thread(target=self._run,name=f'RobotSession-{self.config.robot_id}',daemon=True)
            self._thread.start()
        if not self._ready.wait(2*self.config.io_timeout_s+2):
            self._exit.set();raise OperationFailed('Backend did not honor the connection timeout')
        if self._connection_error:
            self._thread.join(2*self.config.io_timeout_s+1)
            raise self._connection_error
        return self
    def __enter__(self):return self.connect()
    def __exit__(self,*exc):self.close()

    def _connected(self):
        if not self._thread or not self._thread.is_alive() or self._state==SessionState.DISCONNECTED:
            raise DeviceDisconnected('Session is not connected')
    def _controllable(self):
        self._connected()
        if self._stop_intent.is_set() or self._stop_request is not None:raise StateError('Stop is pending; new control is not permitted')
        if self._state in (SessionState.FAULT,SessionState.E_STOP):raise StateError('Fault is latched; close and explicitly reconnect after investigation')
    def _target(self,values):return JointTarget(values,self.config.units,self.config.calibration_hash)

    def _snapshot_view(self,max_age_s):
        self._connected()
        if self._sample is None:raise DataNotReady('No measured observation')
        now=time.monotonic_ns();sample=self._sample
        age=(now-sample.captured_monotonic_ns)/1e9
        if age<0 or age>max_age_s:raise DataNotReady('Measured state is stale or in the future')
        arm=ArmState(self._sequence,sample.captured_monotonic_ns,self.config.calibration_hash,
                     sample.positions,self.config.units,age*1000)
        images={}
        for key,image in sample.images.items():
            image_age=(now-image.captured_monotonic_ns)/1e9
            if image_age<0 or image_age>max_age_s:raise DataNotReady(f'Camera {key} is stale')
            if abs(image.captured_monotonic_ns-sample.captured_monotonic_ns)/1e9>self.config.max_sensor_skew_s:
                raise DataNotReady(f'Camera/state skew exceeds budget: {key}')
            images[key]=ImageFrame(image.rgb,image.captured_monotonic_ns,image_age*1000,key)
        return arm,images
    def get_arm_data(self,*,max_age_s=None):
        age=self.config.max_observation_age_s if max_age_s is None else finite(max_age_s,'max_age_s',positive=True)
        with self._lock:
            # Arm-only queries must not depend on cameras being fresh.
            self._connected()
            if self._sample is None:raise DataNotReady('No measured state')
            now=time.monotonic_ns();elapsed=(now-self._sample.captured_monotonic_ns)/1e9
            if not 0<=elapsed<=age:raise DataNotReady('Measured state is stale')
            return ArmState(self._sequence,self._sample.captured_monotonic_ns,self.config.calibration_hash,
                            self._sample.positions,self.config.units,elapsed*1000)
    def get_observation(self,task,*,max_age_s=None):
        age=self.config.max_observation_age_s if max_age_s is None else finite(max_age_s,'max_age_s',positive=True)
        with self._lock:
            arm,images=self._snapshot_view(age)
            if not set(self.config.required_cameras)<=set(images):raise DataNotReady('Required camera missing')
            return Observation(arm,images,task,self._sample.source_metadata)

    def _wait_for_fresh_observation(self,task,*,timeout_s,expected_epoch,cancelled=None):
        """Wait for the sole worker to start a new sensor request after this call.

        A response already in flight before READY cannot satisfy this barrier.
        Sensor capture times retain their original conservative age checks.
        """
        cutoff=time.monotonic_ns();deadline=time.monotonic()+finite(timeout_s,'timeout_s',positive=True)
        while True:
            if cancelled is not None and cancelled():raise OperationCancelled('Observation refresh cancelled')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise DataNotReady('Fresh observation refresh timed out')
            if not self._lock.acquire(timeout=min(.05,remaining)):continue
            try:
                self._connected()
                if self._epoch!=expected_epoch or self._stop_intent.is_set() or self._stop_request is not None:
                    raise OperationCancelled('Observation refresh control epoch invalidated')
                if self._state in (SessionState.FAULT,SessionState.E_STOP):
                    raise StateError('Observation refresh failed: '+str(self._fault or self._state.value))
                if self._sample_started_ns>cutoff:
                    try:return self.get_observation(task)
                    except DataNotReady:pass
            finally:self._lock.release()
            self._exit.wait(min(.01,max(0,deadline-time.monotonic())))

    def move_to(self,data:JointTarget,*,mode='trajectory',duration_s=None):
        entered_epoch=self._epoch
        self.config.validate_target(data)
        if self._stop_intent.is_set():raise StateError('Stop is pending; new targets are not permitted')
        if mode=='servo':
            if duration_s is not None:raise ConfigurationError('servo does not start a trajectory duration')
            return self._submit_servo(data)
        if mode!='trajectory':raise ConfigurationError('mode must be trajectory or servo')
        with self._lock:
            self._controllable()
            if entered_epoch!=self._epoch:raise StateError('A stop invalidated this queued target')
            if self._owner is not None:raise BusyError(f'Control owned by {self._owner}')
            current=self.get_arm_data()
            minimum=max(abs(data.positions[k]-current.positions[k])/self.config.max_velocity[k] for k in JOINT_NAMES)
            duration=max(1/self.config.fps,minimum) if duration_s is None else finite(duration_s,'duration_s',positive=True)
            if duration+1e-9<minimum:raise ConfigurationError('duration would exceed configured velocity')
            if duration>=self.config.action_timeout_s:raise ConfigurationError('duration must leave time to settle before action timeout')
            handle=ActionHandle(lambda op:self._stop('cancel',only_operation=op))
            self._owner='manual';self._nonce=uuid.uuid4().hex
            self._motion={'handle':handle,'target':data,'duration':duration,'epoch':self._epoch,
                          'created':time.monotonic(),'start':None,'positions':None,'stable':0,'receipt':None}
            self._state=SessionState.EXECUTING;self.events.emit('action_started',operation=handle.id,epoch=self._epoch)
            return handle

    def _gripper(self,value):
        entered_epoch=self._epoch
        if self._stop_intent.is_set():raise StateError('Stop is pending; new gripper targets are not permitted')
        with self._lock:
            self._controllable()
            if entered_epoch!=self._epoch:raise StateError('A stop invalidated this queued gripper target')
            if self._owner is not None:raise BusyError('Control already borrowed')
            positions=dict(self.get_arm_data().positions);positions['gripper.pos']=value
            return self.move_to(self._target(positions))
    def open_gripper(self):return self._gripper(self.config.gripper_open)
    def close_gripper(self):return self._gripper(self.config.gripper_closed)
    def release(self):
        entered_epoch=self._epoch
        if self._stop_intent.is_set():raise StateError('Stop is pending; release is not permitted')
        with self._lock:
            self._controllable()
            if entered_epoch!=self._epoch:raise StateError('A stop invalidated this queued release')
            if self._owner is not None:raise BusyError('Control already borrowed')
            if self.config.release_pose is None:raise ConfigurationError('No configured release pose for this robot/calibration')
            positions=dict(self.config.release_pose.positions)
            positions['gripper.pos']=self.get_arm_data().positions['gripper.pos']
            return self.move_to(self._target(positions))

    @contextmanager
    def _control_scope(self,kind,*,deadline_s):
        """Internal seam reserved for future CollectionTask/InferenceRunner."""
        if kind not in ('inference','collection','verification'):raise ConfigurationError('Unknown control owner')
        deadline_s=finite(deadline_s,'control deadline',positive=True)
        entered_epoch=self._epoch
        if self._stop_intent.is_set():raise StateError('Stop is pending; new control is not permitted')
        with self._lock:
            self._controllable()
            if entered_epoch!=self._epoch:raise StateError('A stop invalidated this queued control scope')
            if self._owner is not None:raise BusyError('Control token is exclusive')
            self._owner=kind;self._nonce=uuid.uuid4().hex
            self._deadline_ns=time.monotonic_ns()+int(deadline_s*1e9)
            ctx=_Context(self,self._nonce,self._epoch,self._deadline_ns,kind)
            self._state=SessionState.INFERENCING if kind=='inference' else SessionState.RECORDING if kind=='collection' else SessionState.EXECUTING
        token=_CONTROL.set(ctx)
        try:yield
        finally:
            _CONTROL.reset(token)
            with self._lock:owned=self._nonce==ctx.nonce
            if owned:self.stop('control scope ended')
    def _submit_servo(self,target):
        return self._submit_setpoint(target,'inference')

    def _collection_step(self,target):
        """Collection worker seam; public servo remains inference-only."""
        self.config.validate_target(target)
        return self._submit_setpoint(target,'collection')

    def _verification_step(self,target):
        """Internal simulation verification seam; uses the same I/O and stop checks."""
        self.config.validate_target(target)
        return self._submit_setpoint(target,'verification')

    def _inference_hold(self):
        """Hold through the sole I/O thread without relinquishing the run token."""
        return self._submit_setpoint(self._target(self.get_arm_data().positions),'inference',hold_only=True)

    def _submit_setpoint(self,target,kind,*,hold_only=False):
        ctx=_CONTROL.get()
        with self._lock:
            self._controllable()
            if not isinstance(ctx,_Context) or ctx.robot is not self or ctx.kind!=kind:raise BusyError(f'Setpoint requires the {kind} control context')
            if ctx.epoch!=self._epoch or ctx.nonce!=self._nonce or time.monotonic_ns()>=ctx.deadline_ns:
                raise StateError('Expired or invalidated inference context')
            if self._servo is not None:raise BusyError('A servo receipt is pending')
            request={'target':target,'context':ctx,'hold_only':hold_only,'done':threading.Event(),'result':None,'error':None}
            self._servo=request
        if not request['done'].wait(3/self.config.fps+2*self.config.io_timeout_s):
            self.stop('servo I/O timeout');raise OperationFailed('Servo was not acknowledged in time')
        if request['error']:raise request['error']
        return request['result']

    def stop(self,reason='requested'):return self._stop(reason)
    def emergency_stop(self,reason='software E_STOP'):
        """Latched simulation stop, NOT a physical hardware emergency circuit."""
        return self._stop(reason,emergency=True)
    def _stop(self,reason,*,emergency=False,only_operation=None):
        interrupt=getattr(self._backend,'interrupt_stop',None)
        if only_operation is None and callable(interrupt):
            return self._priority_stop_call(reason,emergency,interrupt)
        with self._lock:
            self._connected()
            if only_operation is not None and (self._motion is None or self._motion['handle'].id!=only_operation):return None
            if emergency:self._state=SessionState.E_STOP
            if self._stop_request is None:
                self._epoch+=1;self._nonce=None
                self._stop_request={'done':threading.Event(),'reason':str(reason),'result':None,'error':None,
                                    'deadline':time.monotonic()+3/self.config.fps+3*self.config.io_timeout_s+1}
            request=self._stop_request
        if not request['done'].wait(3/self.config.fps+3*self.config.io_timeout_s+1):
            raise OperationFailed('Stop not confirmed; backend may have violated its timeout contract')
        if request['error']:raise request['error']
        return request['result']

    def _priority_stop_call(self,reason,emergency,interrupt):
        # This separate, brief lock never protects ordinary backend I/O. Only
        # explicitly opted-in adapters may receive this cancellation hook from
        # a caller; regular sample/send/hold/close remain on the sole worker.
        request=self._signal_stop_intent(reason,emergency=emergency)
        deadline=request['deadline']
        remaining=max(0,deadline-time.monotonic())
        if not self._lock.acquire(timeout=remaining):
            raise OperationFailed('Stop intent retained but normal backend I/O did not release the session lock')
        try:
            if not request['done'].is_set():
                self._connected();self._adopt_priority_stop()
        finally:self._lock.release()
        if not request['done'].wait(max(0,deadline-time.monotonic())):
            raise OperationFailed('Stop not confirmed; priority cancellation is not a holding proof')
        if request['error']:raise request['error']
        return request['result']

    def _signal_stop_intent(self,reason,*,emergency=False):
        """Internal recorder seam: signal cancellation without waiting for HOLD.

        The returned private ticket is never a successful stop receipt. Legacy
        adapters lacking the thread-safe hook keep their normal worker path.
        """
        interrupt=getattr(self._backend,'interrupt_stop',None)
        if not callable(interrupt):return None
        with self._stop_guard:
            self._connected()
            request=self._priority_stop;created=request is None
            if created:
                request={'done':threading.Event(),'reason':str(reason),'result':None,'error':None,
                         'interrupt_done':threading.Event(),'interrupt_error':None,'emergency':emergency,
                         'deadline':time.monotonic()+3/self.config.fps+3*self.config.io_timeout_s+1}
                self._priority_stop=request;self._stop_intent.set()
            elif emergency:request['emergency']=True
        if created:
            try:interrupt(self.config.io_timeout_s)
            except Exception as error:request['interrupt_error']=error
            finally:request['interrupt_done'].set()
        return request

    def _adopt_priority_stop(self):
        # Called with the normal session lock by either caller or worker.
        with self._stop_guard:request=self._priority_stop
        if request is None or request['done'].is_set() or not request['interrupt_done'].is_set():return
        if self._stop_request is not request:
            request['prior_request']=self._stop_request
            self._epoch+=1;self._nonce=None;self._stop_request=request
        if request['emergency']:self._state=SessionState.E_STOP

    def _finish_stop_request(self,request):
        with self._stop_guard:
            if self._priority_stop is request:
                # An emergency call arriving while HOLD is being proved must
                # remain latched before any waiter observes completion.
                if request['emergency'] and self._state!=SessionState.FAULT:
                    self._state=SessionState.E_STOP
                    if request['result'] is not None:
                        value=request['result']
                        request['result']=StopResult(self._state,value.epoch,value.holding,value.measured_state,value.reason)
                self._priority_stop=None;self._stop_intent.clear()
        prior=request.get('prior_request')
        if prior is not None:
            prior['result']=request['result'];prior['error']=request['error'];prior['done'].set()
        request['done'].set()

    def close(self):
        with self._lock:
            thread=self._thread
            if thread is None:return
            if not thread.is_alive():
                if self._close_error:
                    # Retry cleanup on a sole worker, retaining the device lock
                    # until the adapter confirms release. Never on caller I/O.
                    thread=threading.Thread(target=self._finish_backend_close,name='RobotSession-cleanup',daemon=True)
                    self._thread=thread;thread.start();retry=True
                else:return
            else:retry=False
        if retry:
            thread.join(2*self.config.io_timeout_s+1)
            if thread.is_alive():raise OperationFailed('Cleanup backend exceeded timeout; lock retained')
            if self._close_error:raise self._close_error
            return
        error=None
        try:self.stop('close')
        except Exception as e:error=e
        self._exit.set();thread.join(3*self.config.io_timeout_s+2)
        if thread.is_alive():raise OperationFailed('Worker did not exit; resource lock intentionally retained')
        if error:raise error
        if self._close_error:raise self._close_error

    def _finish_backend_close(self):
        try:
            self._backend.close(hold=True,timeout_s=self.config.io_timeout_s)
        except Exception as e:
            with self._lock:
                self._close_error=OperationFailed(f'Backend close not confirmed: {e}')
                self._fault=str(self._close_error);self._state=SessionState.FAULT
        else:
            with self._lock:self._close_error=None;self._state=SessionState.DISCONNECTED
            self._device_lock.release()

    def _accept_sample(self,s,*,started_ns=None):
        if not isinstance(s,BackendSnapshot):raise ContractMismatch('BackendSnapshot required')
        now=time.monotonic_ns()
        if not isinstance(s.captured_monotonic_ns,int) or not 0<=now-s.captured_monotonic_ns<=self.config.max_observation_age_s*1e9:
            raise DataNotReady('Backend supplied stale/future state')
        self.config.validate_target(self._target(s.positions))
        images={}
        for key,value in s.images.items():
            if not isinstance(value,ImageFrame):raise ContractMismatch('Backend camera must be ImageFrame')
            if value.camera_key and value.camera_key!=key:raise ContractMismatch('Backend camera identity mismatch')
            images[key]=ImageFrame(value.rgb,value.captured_monotonic_ns,value.age_ms,key)
        if not set(self.config.required_cameras)<=set(images):raise DataNotReady('Required camera missing')
        for key in self.config.required_cameras:
            image=images[key]
            if not 0<=now-image.captured_monotonic_ns<=self.config.max_observation_age_s*1e9:
                raise DataNotReady(f'Stale required camera: {key}')
            if abs(image.captured_monotonic_ns-s.captured_monotonic_ns)>self.config.max_sensor_skew_s*1e9:
                raise DataNotReady(f'Required camera skew: {key}')
        from lerobot_sdk.core.provenance import bounded_metadata
        self._sample=BackendSnapshot(s.captured_monotonic_ns,self._target(s.positions).positions,images,
                                     bounded_metadata(s.source_metadata))
        self._sample_started_ns=now if started_ns is None else started_ns
        self._sequence+=1
    def _limited_send(self,target,epoch,now):
        if epoch!=self._epoch or self._stop_request is not None or self._stop_intent.is_set():raise OperationCancelled('Control epoch invalidated')
        dt=max(0,min(now-self._last_send_time,2/self.config.fps))
        sent={};envelope={}
        for k in JOINT_NAMES:
            previous=self._last_sent[k];measured=self._sample.positions[k]
            v=self.config.max_velocity[k]*dt;step=self.config.max_step[k]
            lo=max(self.config.limits[k][0],previous-v,measured-step)
            hi=min(self.config.limits[k][1],previous+v,measured+step)
            if lo>hi:raise OperationFailed(f'Measured motion left permitted setpoint envelope: {k}')
            envelope[k]=(lo,hi)
            sent[k]=min(hi,max(lo,target.positions[k]))
        try:actual=self._backend.send(sent,self.config.io_timeout_s)
        except Exception as error:
            if self._stop_intent.is_set():raise OperationCancelled('Pending setpoint interrupted by stop') from error
            raise
        if epoch!=self._epoch or self._stop_request is not None or self._stop_intent.is_set():
            raise OperationCancelled('Late setpoint receipt invalidated by stop')
        actual_target=self._target(actual);self.config.validate_target(actual_target)
        tolerance=getattr(self._backend,'receipt_envelope_tolerance',{})
        if any(not envelope[k][0]-float(tolerance.get(k,0))-1e-7<=actual_target.positions[k]<=envelope[k][1]+float(tolerance.get(k,0))+1e-7 for k in JOINT_NAMES):
            raise ContractMismatch('Backend returned a target outside the permitted envelope')
        self._last_sent=dict(actual_target.positions);self._last_send_time=time.monotonic()
        receipt=ActionReceipt(target,actual_target,epoch,time.monotonic_ns())
        self.events.emit('target_sent',epoch=epoch)
        return receipt
    def _hold(self):
        if self._sample is None:raise DataNotReady('Cannot confirm HOLD without measured state')
        if time.monotonic_ns()-self._sample.captured_monotonic_ns>self.config.max_observation_age_s*1e9:
            raise DataNotReady('Cannot confirm HOLD from stale feedback')
        self._backend.hold(self._sample.positions,self.config.io_timeout_s)
        self._last_sent=dict(self._sample.positions);self._last_send_time=time.monotonic()
    def _invalidate_pending(self,error):
        if self._motion:
            self._motion['handle']._finish(error=error);self._motion=None
        if self._servo:
            self._servo['error']=error;self._servo['done'].set();self._servo=None
        self._owner=None;self._nonce=None
    def _tick(self,now):
        if self._stop_request:
            request=self._stop_request
            try:
                if 'interrupt_done' in request:
                    if request['interrupt_error'] is not None:
                        raise OperationFailed('Priority stop transport failed: '+str(request['interrupt_error']))
                    # The adapter resolves its priority HOLD/epoch before this
                    # new sample. Discarded pre-stop callbacks never establish
                    # freshness or get stamped as an accepted observation.
                    started_ns=time.monotonic_ns()
                    self._accept_sample(self._backend.sample(self.config.io_timeout_s),started_ns=started_ns)
                epoch=self._epoch;pre_hold_stamp=self._sample.captured_monotonic_ns
                self._hold()
                # HOLD may outlast the pre-HOLD cached feedback. Only a new
                # actual sensor request can supply the final stopped state.
                deadline=request.get('deadline',now+self.config.io_timeout_s)
                while True:
                    remaining=deadline-time.monotonic()
                    if remaining<=0:raise DataNotReady('Post-HOLD feedback is not newer before stop deadline')
                    started_ns=time.monotonic_ns()
                    sample=self._backend.sample(min(self.config.io_timeout_s,remaining))
                    if self._epoch!=epoch or self._stop_request is not request:
                        raise OperationCancelled('Post-HOLD feedback control epoch invalidated')
                    if time.monotonic()>=deadline:
                        raise OperationFailed('Post-HOLD feedback deadline expired; stop unconfirmed')
                    if sample.captured_monotonic_ns>pre_hold_stamp:
                        self._accept_sample(sample,started_ns=started_ns);break
                    self._exit.wait(min(.01,max(0,deadline-time.monotonic())))
                self._invalidate_pending(OperationCancelled(request['reason']))
                if self._state not in (SessionState.FAULT,SessionState.E_STOP):self._state=SessionState.HOLDING
                request['result']=StopResult(self._state,self._epoch,True,self.get_arm_data(),request['reason'])
                self.events.emit('stopped',epoch=self._epoch,reason=request['reason'])
            except Exception as e:
                request['error']=OperationFailed(f'HOLD failed: {e}');self._fault=str(e);self._state=SessionState.FAULT
                self._invalidate_pending(request['error'])
            finally:self._stop_request=None;self._finish_stop_request(request)
            return
        if self._state in (SessionState.FAULT,SessionState.E_STOP):return
        if self._owner in ('inference','collection') and time.monotonic_ns()>=self._deadline_ns:
            self._epoch+=1;self._hold();self._invalidate_pending(OperationCancelled('Control deadline expired'))
            self._state=SessionState.HOLDING;self.events.emit('control_expired',epoch=self._epoch);return
        if self._servo:
            q=self._servo;self._servo=None
            try:
                if q.get('hold_only'):
                    if q['context'].epoch!=self._epoch:raise OperationCancelled('Control epoch invalidated')
                    self._hold()
                    q['result']=StopResult(self._state,self._epoch,True,self.get_arm_data(),'inference queue empty')
                else:q['result']=self._limited_send(q['target'],q['context'].epoch,now)
            except Exception as e:q['error']=e;raise
            finally:q['done'].set()
        if self._motion:
            m=self._motion
            if now-m['created']>self.config.action_timeout_s:raise OperationFailed('Action deadline exceeded before measured arrival')
            if m['start'] is None:m['start']=now;m['positions']=dict(self._sample.positions)
            f=min(1,(now-m['start'])/m['duration'])
            values={k:m['positions'][k]+f*(m['target'].positions[k]-m['positions'][k]) for k in JOINT_NAMES}
            m['receipt']=self._limited_send(self._target(values),m['epoch'],now)
            inside=f==1 and all(abs(self._sample.positions[k]-m['target'].positions[k])<=self.config.tolerance[k] for k in JOINT_NAMES)
            m['stable']=m['stable']+1 if inside else 0
            if m['stable']>=self.config.stable_frames:
                m['handle']._finish(ActionResult(m['target'],m['receipt'],self.get_arm_data(),m['stable']))
                self._motion=None;self._owner=None;self._nonce=None;self._state=SessionState.HOLDING
                self.events.emit('action_completed',operation=m['handle'].id)
    def _faulted(self,error):
        with self._lock:
            self._fault=str(error);self._state=SessionState.FAULT;self._epoch+=1
            self._invalidate_pending(error if isinstance(error,OperationFailed) else OperationFailed(str(error)))
            try:self._hold()
            except Exception as hold_error:self._fault+=f'; HOLD unconfirmed: {hold_error}'
            if self._stop_request:
                r=self._stop_request;self._stop_request=None;r['error']=OperationFailed(self._fault);self._finish_stop_request(r)
            self.events.emit('fault',reason=self._fault)
    def _run(self):
        connected=False
        try:
            self._backend.connect(self.config.io_timeout_s);connected=True
            with self._lock:
                started_ns=time.monotonic_ns()
                self._accept_sample(self._backend.sample(self.config.io_timeout_s),started_ns=started_ns)
                self._last_sent=dict(self._sample.positions);self._last_send_time=time.monotonic()
                self._state=SessionState.IDLE
            self._ready.set();period=1/self.config.fps
            while not self._exit.is_set():
                start=time.monotonic()
                try:
                    if self._stop_intent.is_set():
                        with self._lock:
                            self._adopt_priority_stop()
                            if self._stop_request:self._tick(time.monotonic())
                        self._exit.wait(min(period,.01));continue
                    started_ns=time.monotonic_ns()
                    sample=self._backend.sample(self.config.io_timeout_s)
                    with self._lock:
                        if self._stop_intent.is_set():
                            self._adopt_priority_stop()
                            if self._stop_request:self._tick(time.monotonic())
                        else:self._accept_sample(sample,started_ns=started_ns);self._tick(time.monotonic())
                except Exception as e:
                    if self._stop_intent.is_set():
                        self.events.emit('stop_interrupted_io',error=str(e))
                    else:self._faulted(e)
                self._exit.wait(max(0,period-(time.monotonic()-start)))
        except Exception as e:
            self._connection_error=e if isinstance(e,SDKError) else OperationFailed(f'Backend connection failed: {e}')
            self._fault=str(e);self._state=SessionState.FAULT;self._ready.set()
        finally:
            with self._lock:
                self._invalidate_pending(DeviceDisconnected('Session closed'));self._state=SessionState.DISCONNECTED
                if self._stop_request:
                    r=self._stop_request;self._stop_request=None;r['error']=DeviceDisconnected('Session closed');self._finish_stop_request(r)
                with self._stop_guard:pending=self._priority_stop
                if pending is not None:
                    pending['error']=DeviceDisconnected('Session closed before priority stop was confirmed')
                    self._finish_stop_request(pending)
            if connected:self._finish_backend_close()
            else:self._device_lock.release()
