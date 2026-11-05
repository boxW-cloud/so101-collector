from dataclasses import dataclass
from types import MappingProxyType
import queue
import threading
import time

@dataclass(frozen=True)
class Event:
    sequence:int
    monotonic_ns:int
    kind:str
    detail:object

class Subscription:
    """Pull-only bounded queue: callbacks never execute on the I/O thread."""
    def __init__(self,capacity):self._queue=queue.Queue(capacity);self.dropped=0
    def get(self,timeout_s=None):return self._queue.get(timeout=timeout_s)

class EventHub:
    def __init__(self,capacity):self.capacity=capacity;self._subs=[];self._lock=threading.Lock();self._sequence=0
    def subscribe(self):
        s=Subscription(self.capacity)
        with self._lock:self._subs.append(s)
        return s
    def unsubscribe(self,subscription):
        with self._lock:
            if subscription in self._subs:self._subs.remove(subscription)
    def emit(self,kind,**detail):
        with self._lock:
            self._sequence+=1;e=Event(self._sequence,time.monotonic_ns(),kind,MappingProxyType(dict(detail)))
            for s in self._subs:
                try:s._queue.put_nowait(e)
                except queue.Full:
                    try:s._queue.get_nowait()
                    except queue.Empty:pass
                    s.dropped+=1
                    try:s._queue.put_nowait(e)
                    except queue.Full:s.dropped+=1
