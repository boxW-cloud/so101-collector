"""Deterministic protocol fixture for scheduling failures, not a neural-network verification."""
from dataclasses import replace
import threading,time,uuid
import numpy as np
from lerobot_sdk.core.types import JOINT_NAMES,ActionChunk
from lerobot_sdk.policies.contracts import PolicyCapabilities,ActionEnvelope
from lerobot_sdk.core.errors import BusyError


class FakePolicySession:
    def __init__(self,contract,*,fps=10,chunk_size=2,delay_s=0,task='test policy'):
        self.contract=contract;self.task=task;self.id=uuid.uuid4().hex;self.model_version='fake-policy-test-only'
        self.period_s=1/fps;self.max_age_s=contract.max_observation_age_s;self.epoch=0
        self.capabilities=PolicyCapabilities('fake',self.model_version,True,True,False,False,False,chunk_size,self.period_s)
        self.delay_s=delay_s;self.gate=None;self.entered=threading.Event();self.borrower=None
        self.sequence=0;self.index=0;self.corrupt=None;self.raise_error=False

    def _borrow(self,run_id):
        if self.borrower:raise BusyError('Fake session already borrowed')
        self.borrower=run_id;self.epoch+=1;self.sequence=0;self.index=0

    def _release(self,run_id):
        if self.borrower==run_id:self.borrower=None

    def _packet(self,observation,mode,borrower=None):
        assert borrower==self.borrower
        self.entered.set()
        if self.gate:self.gate.wait()
        if self.delay_s:time.sleep(self.delay_s)
        if self.raise_error:raise RuntimeError('Injected policy failure')
        n=self.capabilities.chunk_size if mode=='chunk' else 1
        values=np.array([[observation.arm.positions[k] for k in JOINT_NAMES]]*n,dtype=np.float32)
        values[:,0]+=2
        chunk=ActionChunk(observation.arm.sequence,JOINT_NAMES,tuple(self.contract.units[k] for k in JOINT_NAMES),values,self.period_s,self.max_age_s)
        packet=ActionEnvelope(self.id,self.epoch,self.model_version,self.sequence,self.index,
                              observation.arm.captured_monotonic_ns,time.monotonic_ns(),chunk)
        self.sequence+=1;self.index+=n
        return self.corrupt(packet) if self.corrupt else packet
