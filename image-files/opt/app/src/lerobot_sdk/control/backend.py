from dataclasses import dataclass
from typing import Protocol,Mapping
from lerobot_sdk.core.types import ImageFrame

@dataclass(frozen=True)
class BackendSnapshot:
    captured_monotonic_ns:int
    positions:Mapping[str,float]
    images:Mapping[str,ImageFrame]
    source_metadata:Mapping[str,object]|None=None

class RobotBackend(Protocol):
    """Every method runs on the single I/O worker and MUST honor timeout_s.

    close(hold=True) releases ownership without removing position support.
    This stage accepts only kind='simulation'; no physical adapter is included.

    An adapter may additionally opt in with interrupt_stop(timeout_s)->None.
    This cancellation hook can run concurrently on a stop caller, must return
    within its bound, and must not replay positions or acquire a new lease.
    It is not a holding proof. The normal worker still samples fresh feedback
    and calls hold before RobotSession returns a successful StopResult.
    """
    kind:str
    resource_id:str
    def connect(self,timeout_s:float)->None:...
    def sample(self,timeout_s:float)->BackendSnapshot:...
    def send(self,positions:Mapping[str,float],timeout_s:float)->Mapping[str,float]:...
    def hold(self,positions:Mapping[str,float],timeout_s:float)->None:...
    def close(self,*,hold:bool,timeout_s:float)->None:...
