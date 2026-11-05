"""Signed ROS peer protocol, ownership and independent expiry protection."""
import hashlib,hmac,json,math,threading,time,uuid,platform,itertools
from pathlib import Path
from .store import Fault,canonical

def sign(value,key):return {**value,'signature':hmac.new(key.encode(),canonical(value).encode(),hashlib.sha256).hexdigest()}
def verify(value,key):
    data={k:v for k,v in value.items() if k!='signature'}
    if not hmac.compare_digest(str(value.get('signature','')),sign(data,key)['signature']):raise Fault('UNAUTHORIZED','Invalid ROS signature',403)
    return data
_SEQUENCE=itertools.count(time.monotonic_ns());_SEQUENCE_LOCK=threading.Lock()
def request(op,payload,owner,epoch,key):
    with _SEQUENCE_LOCK:sequence=next(_SEQUENCE)
    now=time.time();return sign({'schema':'so101.delivery.ros.v1','request_id':uuid.uuid4().hex,'owner':owner,'epoch':epoch,'sequence':sequence,'operation':op,'payload':payload,'issued_at':now,'expires_at':now+(10 if op in ('reset','teleop.start','teleop.stop') else 5)},key)
def clock_id():
    path=Path('/proc/sys/kernel/random/boot_id')
    return path.read_text().strip() if path.exists() else 'host:'+platform.node()

class Peer:
    CACHE_MAX_BYTES=32*1024*1024
    CACHE_MAX_ENTRIES=4096
    def __init__(self,adapter,key):
        self.adapter=adapter;self.key=key;self.owner=None;self.epoch=uuid.uuid4().hex;self.until=0
        self.cache={};self.cache_bytes=0;self._cache_sizes={};self._seen_requests={}
        self.pending={};self.lock=threading.RLock();self.issue_lock=threading.Lock()
        self.fault=None;self.holding=False;self.sequence=-1;self.write_sequence=-1;self.stopping=False;self.read_only=False;self.read_only_released=False
        self.teleop_active=False;self.teleop_attempted=False
    def _evict_cached(self,rid):
        self.cache.pop(rid,None);self.cache_bytes-=self._cache_sizes.pop(rid,0)
    def _clear_cache(self):
        self.cache.clear();self._cache_sizes.clear();self._seen_requests.clear();self.cache_bytes=0
    def _prune_cache(self,now):
        # Expired envelopes already fail admission; their large image replies
        # can no longer serve a valid retry. Never restamp them as fresh.
        for rid,(_,expires) in list(self._seen_requests.items()):
            if now>=expires:self._seen_requests.pop(rid,None);self._evict_cached(rid)
    def _remember(self,rid,fingerprint,response,expires):
        size=len(canonical(response).encode())
        self._prune_cache(time.time())
        self._evict_cached(rid)
        # Keep small fingerprints after byte eviction so an unexpired request
        # ID cannot be reused with a changed payload and newer sequence.
        if time.time()<expires:
            while len(self._seen_requests)>=self.CACHE_MAX_ENTRIES:
                oldest=next(iter(self._seen_requests));self._seen_requests.pop(oldest);self._evict_cached(oldest)
            self._seen_requests[rid]=(fingerprint,expires)
        # Budget serialized reply payload, independently of the small bounded
        # dictionaries. Entry count alone could retain gigabytes of PNGs.
        while self.cache and (len(self.cache)>=self.CACHE_MAX_ENTRIES or self.cache_bytes+size>self.CACHE_MAX_BYTES):
            self._evict_cached(next(iter(self.cache)))
        if size>self.CACHE_MAX_BYTES or time.time()>=expires:return
        self.cache[rid]=(fingerprint,response);self._cache_sizes[rid]=size;self.cache_bytes+=size
    def shutdown(self):
        with self.lock:owner,epoch=self.owner,self.epoch
        if owner:return self._stop(owner,epoch,close=True)
    def _stop(self,owner,epoch,close=False):
        with self.lock:
            if self.owner is None and self.holding:return {'holding':True,'action_queue_empty':True,'epoch':self.epoch}
            if self.owner!=owner or self.epoch!=epoch:raise Fault('STALE_EPOCH','Stop owner changed')
            self.stopping=True;readonly=self.read_only
        try:
            # An opted-in physical adapter can cancel native acquisition/updates
            # before waiting for a blocked start/send to release issue_lock.
            # Its result is NOT a holding proof; the normal stop below still
            # verifies source shutdown, actual HOLD and the empty action queue.
            preempt=getattr(self.adapter,'preempt_stop',None)
            if callable(preempt):preempt(close=close or readonly)
            # Reads do not hold issue_lock. Stop can run while a camera/sample
            # blocks. In-flight bounded writes finish before HOLD; queued writes
            # recheck the stopping latch and cannot execute afterwards.
            with self.issue_lock:
                with self.lock:
                    if self.owner is None and self.holding:return {'holding':True,'action_queue_empty':True,'epoch':self.epoch}
                    if self.owner!=owner or self.epoch!=epoch:raise Fault('STALE_EPOCH','Stop owner changed')
                readonly=self.read_only;source_error=None
                # Per-episode save/redo pauses issue HOLD while retaining the
                # collection lease. Only full close/expiry/readonly release
                # owns source shutdown; automatic next still needs live input.
                if self.teleop_attempted and (close or readonly):
                    try:
                        stopped=self.adapter.stop_teleop(2.0)
                        if stopped.get('stopped') is not True:raise Fault('TELEOP_STOP_UNCONFIRMED','Leader input stop was not confirmed')
                        self.teleop_active=False;self.teleop_attempted=False
                    except Exception as exc:source_error=exc
                result={'read_only_released':True,'action_queue_empty':True,'motion_started':False} if readonly else self.adapter.hold()
                if not readonly and (result.get('holding') is not True or result.get('action_queue_empty') is not True):raise Fault('STOP_UNCONFIRMED','Peer hold and empty action queue not confirmed')
                if source_error:raise Fault('STOP_UNCONFIRMED','Robot hold confirmed but input source stop is unconfirmed: '+str(source_error))
                with self.lock:
                    self.epoch=uuid.uuid4().hex;self.sequence=-1;self.write_sequence=-1;self._clear_cache();self.pending.clear()
                    self.holding=not readonly;self.read_only_released=readonly;self.read_only=False;self.fault=None;self.stopping=False;self.until=time.monotonic()+2
                    if close or readonly:self.owner=None
                    return {**result,'epoch':self.epoch}
        except BaseException:
            with self.lock:self.fault='STOP_UNCONFIRMED';self.stopping=True
            raise
    def tick(self):
        with self.lock:
            if not self.owner or time.monotonic()<self.until or self.stopping:return
            owner,epoch=self.owner,self.epoch;self.stopping=True
        try:self._stop(owner,epoch,close=True)
        except Exception:pass  # Latched and queryable; never fabricate HOLD.
    def call(self,packet):
        rid=packet.get('request_id') if isinstance(packet,dict) else None
        try:
            v=verify(packet,self.key)
            required={'schema','request_id','owner','epoch','sequence','operation','payload','issued_at','expires_at'}
            if set(v)!=required or v['schema']!='so101.delivery.ros.v1':raise Fault('INVALID_ENVELOPE','Exact ROS envelope required')
            if type(v['sequence'])!=int or v['sequence']<0:raise Fault('INVALID_SEQUENCE','Monotonic sender sequence required')
            now=time.time();a,b=v['issued_at'],v['expires_at']
            if any(type(x) not in (int,float) or not math.isfinite(x) for x in (a,b)) or not 0<b-a<=10 or not a-1<=now<b:raise Fault('EXPIRED','Expired ROS request')
            op=v['operation'];p=v['payload'];fingerprint=hashlib.sha256(canonical(v).encode()).hexdigest()
            # External identity/queue reads can block. They must not hold our
            # control lock or delay the independent HOLD watchdog.
            description=None;external_queue=None
            if op=='inspect':
                description=self.adapter.describe()
                read_queue=getattr(self.adapter,'queue_status',None)
                if description.get('kind')=='physical' and callable(read_queue):
                    external_queue=read_queue()
                    if not isinstance(external_queue,dict) or type(external_queue.get('action_queue_empty'))!=bool:
                        raise Fault('QUEUE_STATUS_UNAVAILABLE','Actual native owner queue status is required')
            with self.lock:
                self._prune_cache(now)
                if op=='inspect':
                    # An unowned fresh peer has never admitted an owned action;
                    # queue emptiness is independent of actuator holding.
                    empty=not self.stopping and not self.pending and (self.owner is None or self.holding)
                    scope='peer'
                    if description.get('kind')=='physical':
                        empty=empty and external_queue is not None and external_queue['action_queue_empty']
                        scope='peer_and_native_owner' if external_queue is not None else 'external_target_unverified'
                    result={**description,'clock_id':clock_id(),'epoch':self.epoch,'owner':self.owner,'fault':self.fault,'holding':self.holding,'read_only_released':self.read_only_released,
                            'teleop_active':self.teleop_active,'action_queue_empty':empty,'queue_scope':scope}
                    if external_queue is not None:result['native_queue_status']=external_queue
                    return sign({'request_id':rid,'ok':True,'result':result},self.key)
                if rid in self._seen_requests and self._seen_requests[rid][0]!=fingerprint:
                    raise Fault('IDEMPOTENCY_CONFLICT','ROS request changed after response eviction')
                if rid in self.cache:
                    old,response=self.cache[rid]
                    if old!=fingerprint:raise Fault('IDEMPOTENCY_CONFLICT','ROS request changed')
                    return response
                if rid in self.pending:
                    if self.pending[rid]!=fingerprint:raise Fault('IDEMPOTENCY_CONFLICT','Pending ROS request changed')
                    raise Fault('IN_PROGRESS','Original request still executing')
                if op not in ('hold','close') and (self.stopping or self.fault):raise Fault('STOP_UNCONFIRMED','Stop is pending/unconfirmed')
                if op not in ('hold','close') and v['sequence']<=self.sequence:raise Fault('STALE_SEQUENCE','Older command cannot overwrite newer target')
                if op in ('connect','calibration.connect'):
                    if self.owner:raise Fault('BUSY','Peer already owned')
                    if v['epoch']!=self.epoch:raise Fault('STALE_EPOCH','Peer epoch changed')
                    self.owner=v['owner']
                    self.read_only=op=='calibration.connect';self.read_only_released=False
                elif self.owner!=v['owner'] or self.epoch!=v['epoch']:raise Fault('STALE_EPOCH','No current peer control lease')
                if self.read_only and op not in ('calibration.connect','calibration.read','heartbeat','hold','close'):raise Fault('READ_ONLY_LEASE','Calibration lease cannot send actuator commands')
                if op not in ('connect','calibration.connect','hold','close') and time.monotonic()>=self.until:raise Fault('LEASE_EXPIRED','Expired operation cannot renew lease')
                self.sequence=max(self.sequence,v['sequence']);self.pending[rid]=fingerprint
                if op in ('send','primitive'):self.write_sequence=v['sequence']
                if op not in ('hold','close'):self.until=time.monotonic()+(min(10,b-now) if op in ('reset','teleop.start','teleop.stop') else 2)
                admitted_epoch=self.epoch
            if op in ('hold','close'):result=self._stop(v['owner'],v['epoch'],close=op=='close')
            elif op in ('connect','calibration.connect','send','reset','primitive','teleop.start','teleop.stop','heartbeat'):
                with self.issue_lock:
                    with self.lock:
                        if self.stopping or self.epoch!=admitted_epoch or time.monotonic()>=self.until:raise Fault('STALE_EPOCH','Queued write invalidated before issue')
                        if op in ('send','primitive') and v['sequence']<self.write_sequence:raise Fault('STALE_SEQUENCE','Newer admitted target superseded this queued write')
                    if op=='connect':self.adapter.connect();result={'connected':True,'epoch':self.epoch}
                    elif op=='calibration.connect':
                        self.adapter.calibration('calibration.read',{})
                        result={'connected':True,'epoch':self.epoch,'read_only':True}
                    elif op=='heartbeat':
                        heartbeat=getattr(self.adapter,'heartbeat',None)
                        if callable(heartbeat):heartbeat()
                        result={'epoch':self.epoch,'lease_seconds':2}
                    elif op=='send':
                        with self.lock:self.holding=False
                        result={'sent':self.adapter.send(p['positions'])}
                    elif op=='reset':
                        result=self.adapter.reset()
                        if not result.get('holding') or not result.get('scene_reset'):raise Fault('RESET_UNCONFIRMED','Full scene reset not confirmed')
                        with self.lock:
                            if self.stopping or time.monotonic()>=self.until:raise Fault('LEASE_EXPIRED','Reset deadline expired; wait for independent HOLD')
                            self.epoch=uuid.uuid4().hex;self.sequence=-1;self.write_sequence=-1;self._clear_cache();self.holding=True;result['epoch']=self.epoch
                    elif op in ('teleop.start','teleop.stop'):
                        from .managed_source import bounded_timeout
                        if not isinstance(p,dict) or set(p)-{'timeout_s'}:raise Fault('INVALID_TELEOP_REQUEST','Only a bounded timeout can be supplied by a ROS caller')
                        timeout=bounded_timeout(p.get('timeout_s',2.0))
                        method=getattr(self.adapter,'start_teleop' if op=='teleop.start' else 'stop_teleop',None)
                        if not callable(method):raise Fault('TELEOP_UNAVAILABLE','No registered automatic leader source capability')
                        if op=='teleop.start':self.teleop_attempted=True
                        result=method(timeout)
                        flag='started' if op=='teleop.start' else 'stopped'
                        if not isinstance(result,dict) or result.get(flag) is not True or result.get('managed') is not True:
                            raise Fault('TELEOP_START_UNCONFIRMED' if op=='teleop.start' else 'TELEOP_STOP_UNCONFIRMED','Managed source '+flag+' receipt required')
                        self.teleop_active=op=='teleop.start'
                        if op=='teleop.stop':self.teleop_attempted=False
                    else:result=self.adapter.primitive(p)
            elif op=='sample':result=self.adapter.sample()
            elif op=='leader':result=self.adapter.leader()
            elif op.startswith('calibration.'):result=self.adapter.calibration(op,p)
            else:raise Fault('UNSUPPORTED','Peer operation is not registered')
            with self.lock:
                if op not in ('hold','close','reset') and (self.epoch!=admitted_epoch or self.stopping or time.monotonic()>=self.until):raise Fault('LEASE_EXPIRED','Late response cannot revive a stopped lease')
                response=sign({'request_id':rid,'ok':True,'result':result},self.key)
                self._remember(rid,fingerprint,response,b);self.pending.pop(rid,None)
                return response
        except Exception as e:
            with self.lock:
                if getattr(e,'code',None)!='IN_PROGRESS':self.pending.pop(rid,None)
            return sign({'request_id':rid,'ok':False,'error':{'code':getattr(e,'code',type(e).__name__),'message':str(e)}},self.key)

class FixtureAdapter:
    """Software-only peer for delivery tests; never labels data physical/Isaac."""
    kind='simulation';source='test_fixture'
    def __init__(self):
        from lerobot_sdk.testing import simulation_config,FakeRobot
        self.config=simulation_config();self.robot=FakeRobot();self.names=list(self.config.units);self.sent=[]
    def describe(self):return {'kind':self.kind,'source':self.source,'robot_id':self.config.robot_id,'calibration_hash':self.config.calibration_hash,'capabilities':['collection','teleop','task','reset'],'commissioned':False}
    def connect(self):self.robot.connect(.2)
    def sample(self):
        import base64
        from io import BytesIO
        from PIL import Image
        obs=self.robot.sample(.2);images={}
        for k,v in obs.images.items():
            out=BytesIO();Image.fromarray(v.rgb).save(out,format='PNG');images[k]={'png':base64.b64encode(out.getvalue()).decode(),'captured_monotonic_ns':v.captured_monotonic_ns}
        return {'positions':dict(obs.positions),'captured_monotonic_ns':obs.captured_monotonic_ns,'images':images}
    def send(self,positions):
        if set(positions)!=set(self.names) or any(type(v) not in (int,float) or not math.isfinite(v) or not self.config.limits[k][0]<=v<=self.config.limits[k][1] for k,v in positions.items()):raise Fault('INVALID_TARGET','Six finite named targets within limits required')
        self.sent.append(dict(positions));return self.robot.send(positions,.2)
    def leader(self):return {'positions':dict(self.robot.positions)}
    def hold(self):self.robot.hold(self.robot.positions,.2);return {'holding':True,'action_queue_empty':True}
    def reset(self):self.robot.positions=dict.fromkeys(self.names,0.0);self.robot.positions['gripper.pos']=50;self.hold();return {'holding':True,'scene_reset':True,'source':self.source}
    def calibration(self,*a):raise Fault('NOT_COMMISSIONED','Fixture cannot calibrate real hardware')
    def primitive(self,*a):raise Fault('UNSUPPORTED','Fixture has no Cartesian primitive implementation')
