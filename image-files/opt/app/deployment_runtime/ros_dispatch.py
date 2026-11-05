"""Bounded RPC execution with a separate HOLD/CLOSE lane; no ROS imports."""
from concurrent.futures import ThreadPoolExecutor
import threading
from .store import Fault,canonical


class BoundedRpcDispatcher:
    def __init__(self,call,publish,*,normal_workers=2,normal_capacity=16,priority_capacity=8):
        if type(normal_workers) is not int or not 1<=normal_workers<=2 or type(normal_capacity) is not int or not normal_workers<=normal_capacity<=16 or type(priority_capacity) is not int or not 1<=priority_capacity<=8:
            raise ValueError('Bounded normal/priority capacities are required')
        self.call=call;self.publish=publish;self.lock=threading.RLock();self.closed=False;self.generation=0
        self.active={};self.normal=ThreadPoolExecutor(normal_workers,thread_name_prefix='ros-normal-rpc')
        self.priority=ThreadPoolExecutor(1,thread_name_prefix='ros-priority-stop')
        self.normal_slots=threading.BoundedSemaphore(normal_capacity);self.priority_slots=threading.BoundedSemaphore(priority_capacity)

    def submit(self,packet):
        rid=packet.get('request_id')
        if not isinstance(rid,str) or not rid:raise Fault('INVALID_REQUEST','A request identity is required')
        fingerprint=canonical(packet);priority=packet.get('operation') in ('hold','close')
        slots=self.priority_slots if priority else self.normal_slots
        with self.lock:
            if self.closed:raise Fault('STOPPING','RPC dispatcher is closed')
            old=self.active.get(rid)
            if old is not None:
                if old!=fingerprint:raise Fault('IDEMPOTENCY_CONFLICT','Pending request identity changed')
                raise Fault('IN_PROGRESS','Original request is in progress')
            if not slots.acquire(blocking=False):raise Fault('BUSY','Bounded RPC capacity is full')
            if priority:self.generation+=1
            generation=self.generation;self.active[rid]=fingerprint
            try:future=(self.priority if priority else self.normal).submit(self._run,packet,generation,priority)
            except BaseException:self.active.pop(rid,None);slots.release();raise
            def done(_future):
                with self.lock:
                    if self.active.get(rid)==fingerprint:self.active.pop(rid,None)
                slots.release()
            future.add_done_callback(done)
        return future

    def _run(self,packet,generation,priority):
        with self.lock:
            if self.closed or not priority and generation!=self.generation:return
        result=self.call(packet)
        with self.lock:
            if self.closed or not priority and generation!=self.generation:return
            # The production callback only enqueues a bounded completed reply;
            # it never calls HTTP, hardware, or ROS publish under this lock.
            self.publish(packet,result,generation,priority) if getattr(self.publish,'dispatch_metadata',False) else self.publish(packet,result)

    def publish_if_current(self,generation,priority,publish):
        """Called by the ROS main loop immediately before actual publication."""
        with self.lock:
            if self.closed or not priority and generation!=self.generation:return False
            publish()
            return True

    def shutdown(self,*,wait=True):
        with self.lock:self.closed=True;self.generation+=1
        self.normal.shutdown(wait=wait,cancel_futures=True)
        self.priority.shutdown(wait=wait,cancel_futures=True)
