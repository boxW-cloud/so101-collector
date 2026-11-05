"""Per-session native policy/processor state with explicit observation lineage."""
from collections import deque
import threading
import time
import uuid
import numpy as np
from lerobot_sdk.core.errors import BusyError,StateError,ConfigurationError,ContractMismatch,DataNotReady,OperationFailed
from lerobot_sdk.core.types import JOINT_NAMES,RobotConfig,Observation,JointTarget,ActionChunk
from lerobot_sdk.policies.contracts import PolicyCapabilities,ActionEnvelope


class PolicySession:
    @staticmethod
    def validate_contract(owner,task,contract):
        if not isinstance(task,str) or not task.strip():raise ConfigurationError('Nonempty task text required')
        if not isinstance(contract,RobotConfig):raise ConfigurationError('RobotConfig contract required')
        expected=owner.manifest.contract
        if (contract.robot_id!=expected['robot_id'] or contract.calibration_hash!=expected['calibration_hash'] or
            tuple(contract.units[n] for n in JOINT_NAMES)!=tuple(expected['units'])):
            raise ContractMismatch('Policy and robot identity/units/calibration differ')
        return expected

    def __init__(self,owner,runtime,task,contract):
        expected=self.validate_contract(owner,task,contract)
        self._owner=owner;self._runtime=runtime;self.contract=contract;self.task=task
        self.id=uuid.uuid4().hex;self.model_version=owner.manifest.content_hash
        self.period_s=1/expected['fps'];self.max_age_s=contract.max_observation_age_s
        self._expected=expected;self._lock=threading.RLock();self._infer_lock=threading.Lock()
        self._borrower=None;self._closed=False;self._busy=False;self.epoch=0
        self._sequence=0;self._next_index=0;self._mode=None;self._step_source=None
        cfg=runtime._policy.config
        rtc=getattr(cfg,'rtc_config',None)
        rtc_enabled=bool(rtc and getattr(rtc,'enabled',True))
        ensemble=getattr(cfg,'temporal_ensemble_coeff',None) is not None
        self.capabilities=PolicyCapabilities(owner.manifest.policy_type,self.model_version,
            not rtc_enabled,not rtc_enabled and not ensemble,bool(getattr(cfg,'use_relative_actions',False)),
            bool(getattr(cfg,'use_visual_memory',False) or getattr(cfg,'use_proprioceptive_memory',False)),
            rtc_enabled,cfg.chunk_size,self.period_s)
        self._reset_native()

    def _reset_native(self):
        self._runtime._policy.reset();self._runtime._pre.reset();self._runtime._post.reset()
        self.epoch+=1;self._sequence=0;self._next_index=0;self._step_source=None;self._mode=None

    def _check_open(self):
        if self._closed:raise StateError('PolicySession is closed')

    def _borrow(self,run_id):
        with self._lock:
            self._check_open()
            if self._busy or self._borrower:raise BusyError('Policy session already in use')
            self._reset_native();self._borrower=run_id

    def _release(self,run_id):
        with self._lock:
            if self._borrower!=run_id:return
            try:self._reset_native()
            finally:self._borrower=None

    def reset(self,task=None):
        with self._lock:
            self._check_open()
            if self._busy or self._borrower:raise BusyError('Cannot reset during inference or a borrowed run')
            if task is not None:
                if not isinstance(task,str) or not task.strip():raise ConfigurationError('Nonempty task required')
                self.task=task
            self._reset_native()

    def close(self):
        with self._lock:
            if self._closed:return
            if self._busy or self._borrower:raise BusyError('Inference still owns this session')
            self._runtime.close();self._closed=True
        self._owner._release_session(self)

    def __enter__(self):
        self._check_open();return self
    def __exit__(self,*exc):self.close()

    def _prepare(self,observation):
        import torch
        if not isinstance(observation,Observation) or observation.task!=self.task:
            raise ContractMismatch('Observation task must match this session')
        arm=observation.arm
        self.contract.validate_target(JointTarget(arm.positions,arm.units,arm.calibration_hash))
        now=time.monotonic_ns()
        if not 0<=now-arm.captured_monotonic_ns<=self.max_age_s*1e9:raise DataNotReady('Policy observation is stale')
        raw={'observation.state':torch.tensor([arm.positions[n] for n in JOINT_NAMES],dtype=torch.float32),'task':self.task}
        for key,shape in self._expected['camera_shapes'].items():
            if key not in observation.images:raise ContractMismatch('Required policy camera missing: '+key)
            image=observation.images[key]
            if tuple(image.rgb.shape)!=tuple(shape):raise ContractMismatch('Policy camera shape mismatch: '+key)
            if not 0<=now-image.captured_monotonic_ns<=self.max_age_s*1e9:raise DataNotReady('Policy camera is stale: '+key)
            if abs(image.captured_monotonic_ns-arm.captured_monotonic_ns)>self.contract.max_sensor_skew_s*1e9:
                raise DataNotReady('Policy camera/state skew exceeds contract')
            # Native LeRobotDataset supplies CHW floats [0,1]. Perform this conversion exactly once.
            raw['observation.images.'+key]=torch.from_numpy(image.rgb.copy()).permute(2,0,1).float()/255
        return self._runtime._pre(raw)

    def _packet(self,observation,mode,borrower=None):
        with self._lock:
            self._check_open()
            if self._borrower is not None and self._borrower!=borrower:raise BusyError('Policy is borrowed by an inference run')
            if self._busy:raise BusyError('One inference at a time per PolicySession')
            if mode=='step' and not self.capabilities.supports_step:raise ConfigurationError('RTC step is unsupported; no silent fallback')
            if mode=='chunk' and not self.capabilities.supports_chunk:raise ConfigurationError('RTC/temporal-ensemble chunk mode is unsupported')
            if self._mode is not None and self._mode!=mode:raise StateError('Reset the session before switching step/chunk modes')
            self._busy=True;self._mode=mode;epoch=self.epoch
        try:
            import torch
            with torch.inference_mode():
                p=self._runtime._policy;post=self._runtime._post
                batch=self._prepare(observation)
                if mode=='chunk':
                    # Native pi05 unpads using output_features. Postprocess once per entire chunk,
                    # so relative actions are anchored to this generating observation only.
                    output=post(p.predict_action_chunk(batch))
                    source=(observation.arm.sequence,observation.arm.captured_monotonic_ns)
                else:
                    ensemble=getattr(p.config,'temporal_ensemble_coeff',None) is not None
                    new_chunk=ensemble or len(p._action_queue)==0
                    if new_chunk:self._step_source=(observation.arm.sequence,observation.arm.captured_monotonic_ns)
                    if self._step_source is None:raise StateError('Missing cached action observation origin')
                    if time.monotonic_ns()-self._step_source[1]>self.max_age_s*1e9:raise DataNotReady('Cached native action has expired; reset before reuse')
                    if self.capabilities.relative_actions:
                        # Fixed native pi05 select_action only slices/transposes/caches this result.
                        # Convert the whole newly generated chunk before caching, preserving native
                        # memory updates while preventing later observations re-anchoring old actions.
                        original=p.predict_action_chunk
                        def absolute_chunk(*args,**kwargs):return post(original(*args,**kwargs))
                        p.predict_action_chunk=absolute_chunk
                        try:output=p.select_action(batch)
                        finally:p.predict_action_chunk=original
                    else:output=post(p.select_action(batch))
                    output=output.unsqueeze(1);source=self._step_source
                values=output.detach().to(device='cpu',dtype=torch.float32).numpy()
                if values.ndim!=3 or values.shape[0]!=1 or values.shape[-1]!=6 or values.shape[1]<1:
                    raise ContractMismatch('Policy must return batch=1 and six actual action dimensions')
                values=values[0].copy()
                if borrower is not None and mode=='chunk':
                    # Native policies execute n_action_steps then request a fresh chunk.
                    # Public infer_chunk still exposes the complete prediction horizon.
                    values=values[:p.config.n_action_steps]
                for row in values:self.contract.validate_target(JointTarget(dict(zip(JOINT_NAMES,row,strict=True)),self.contract.units,self.contract.calibration_hash))
                if time.monotonic_ns()-source[1]>self.max_age_s*1e9:
                    raise DataNotReady('Policy finished after its observation validity window')
                chunk=ActionChunk(source[0],JOINT_NAMES,tuple(self.contract.units[n] for n in JOINT_NAMES),values,self.period_s,self.max_age_s)
                envelope=ActionEnvelope(self.id,epoch,self.model_version,self._sequence,self._next_index,source[1],time.monotonic_ns(),chunk)
                self._sequence+=1;self._next_index+=len(values)
                return envelope
        finally:
            with self._lock:self._busy=False

    def infer_step(self,observation):
        packet=self._packet(observation,'step')
        row=packet.chunk.values[0]
        return JointTarget(dict(zip(JOINT_NAMES,row,strict=True)),self.contract.units,self.contract.calibration_hash)

    def infer_chunk(self,observation):return self._packet(observation,'chunk').chunk
