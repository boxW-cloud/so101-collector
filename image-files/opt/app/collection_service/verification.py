"""A bounded leader-following loop with no dataset writer or formal recording."""
import threading,time

class VerificationSession:
    def __init__(self,robot,leader,*,fps=5,duration_s=600,task='Simulation verification',initial_target=None,alignment=None):
        self.robot,self.leader,self.fps,self.duration_s,self.task=robot,leader,fps,duration_s,task
        self.stop_event=threading.Event();self.ready=threading.Event();self.done=threading.Event()
        self.steps=0;self.samples=0;self.error=None;self.holding=False;self.state='STARTING'
        self.last_feedback=None;self.last_target=None;self.last_age_ms=None
        self.last_requested=None;self.initial_target=initial_target;self.alignment=alignment or {'required':False,'completed':True,'phase':'NOT_REQUIRED'}
    def start(self):
        self.thread=threading.Thread(target=self._run,daemon=True,name='SimulationVerification');self.thread.start()
        if not self.ready.wait(5):raise TimeoutError('Verification startup timed out')
        if self.error:raise RuntimeError(self.error)
        return self.status()
    def _run(self):
        try:
            with self.robot._control_scope('verification',deadline_s=self.duration_s):
                self.state='ALIGNING' if self.alignment['required'] else 'RUNNING';self.ready.set();until=time.monotonic()+self.duration_s
                align_until=time.monotonic()+30;stable=0;last_alignment_sequence=None
                while not self.stop_event.is_set() and time.monotonic()<until:
                    tick=time.monotonic();obs=self.robot.get_observation(self.task);self.samples+=1
                    target=self.leader.get_action(timeout_s=.25)
                    if self.stop_event.is_set():break
                    if self.state=='ALIGNING':
                        if any(abs(target.positions[k]-self.initial_target.positions[k])>2 for k in target.positions):raise RuntimeError('LEADER_MOVED_DURING_ALIGNMENT: keep leader still and retry explicit alignment')
                        if time.monotonic()>align_until:raise TimeoutError('INITIAL_ALIGNMENT_TIMEOUT')
                        target=self.initial_target
                        errors={k:abs(obs.arm.positions[k]-target.positions[k]) for k in target.positions}
                        self.alignment['current_error_by_joint']=errors
                        if obs.arm.sequence!=last_alignment_sequence:
                            stable=stable+1 if all(v<=self.alignment['thresholds'][k] for k,v in errors.items()) else 0
                            last_alignment_sequence=obs.arm.sequence
                        if stable>=3:self.alignment.update(completed=True,phase='ALIGNED');self.state='RUNNING'
                    receipt=self.robot._verification_step(target);self.steps+=1
                    self.last_requested=dict(target.positions)
                    self.last_feedback=dict(obs.arm.positions);self.last_target=dict(receipt.sent_target.positions);self.last_age_ms=obs.arm.age_ms
                    self.stop_event.wait(max(0,1/self.fps-(time.monotonic()-tick)))
        except Exception as exc:
            if not self.stop_event.is_set():self.error=str(exc)
        finally:
            try:self.holding=self.robot.stop('simulation verification ended').holding
            except Exception as exc:self.error=str(exc)
            self.state='FAILED' if self.error else 'STOPPED';self.ready.set();self.done.set()
    def status(self):
        return {'state':self.state,'recording':False,'dataset_id':None,'steps':self.steps,'samples':self.samples,
            'feedback':self.last_feedback,'sent_target':self.last_target,'observation_age_ms':self.last_age_ms,
            'requested_target':self.last_requested,'alignment':dict(self.alignment),'leader_mapping':getattr(self.leader,'last_mapping',None),
            'holding':self.holding,'action_queue_empty':self.done.is_set() and self.robot.status().control_owner is None,'error':self.error}
    def stop(self):
        self.stop_event.set();stopped=self.robot.stop('explicit verification stop')
        self.thread.join(5)
        if self.thread.is_alive():raise TimeoutError('Verification loop has not exited; do not release ownership')
        result=self.status();result.update(holding=stopped.holding,stop_epoch=stopped.epoch)
        if not result['holding'] or not result['action_queue_empty']:raise RuntimeError('Verification stop not confirmed')
        return result
