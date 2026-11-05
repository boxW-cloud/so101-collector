from dataclasses import dataclass
from enum import Enum
import threading
import uuid
from lerobot_sdk.core.errors import WaitTimeout, OperationCancelled
from lerobot_sdk.core.types import finite

class OperationState(str,Enum):
    RUNNING='RUNNING';SUCCEEDED='SUCCEEDED';CANCELLED='CANCELLED';FAILED='FAILED'

@dataclass(frozen=True)
class OperationStatus:
    operation_id:str
    state:OperationState
    error:str|None

class ActionHandle:
    def __init__(self,cancel):
        self.id=uuid.uuid4().hex;self._cancel=cancel;self._done=threading.Event();self._lock=threading.Lock()
        self._state=OperationState.RUNNING;self._result=None;self._error=None
    def status(self):
        with self._lock:return OperationStatus(self.id,self._state,str(self._error) if self._error else None)
    def wait(self,timeout_s=None):
        if timeout_s is not None:timeout_s=finite(timeout_s,'wait timeout',nonnegative=True)
        if not self._done.wait(timeout_s):raise WaitTimeout('Wait expired; operation was not cancelled')
        if self._error is not None:raise self._error
        return self._result
    def cancel(self):
        if not self._done.is_set():self._cancel(self.id)
        return self.status()
    def _finish(self,result=None,error=None):
        with self._lock:
            if self._done.is_set():return
            self._result=result;self._error=error
            self._state=OperationState.CANCELLED if isinstance(error,OperationCancelled) else OperationState.FAILED if error else OperationState.SUCCEEDED
            self._done.set()
