"""Relative encoder teleoperation through the sole Windows hardware owner.

This is an explicitly commissioned bench mapping, not absolute zero/TCP/IK
calibration. It never opens a serial port or uses the Isaac/BorrowedLeader map.
The native owner enforces actual acquisition, bounded motor writes and HOLD.
"""
import copy
import hashlib
import json
import math
import re
from pathlib import Path
import threading
import time
import uuid

from .native_adapter import NativeOwnerAdapter
from .managed_source import ManagedTeleopSource, bounded_timeout
from .store import Fault

NAMES=('shoulder_pan','shoulder_lift','elbow_flex','wrist_flex','wrist_roll','gripper')
SCHEMA='so101.native_relative_teleop.v1'


def finite(value):
    return type(value) in (int,float) and math.isfinite(value)


def checked_mapping(value):
    if not isinstance(value,dict) or value.get('schema')!=SCHEMA or value.get('scope')!='relative_encoder_bench':
        raise Fault('PHYSICAL_MAPPING_REQUIRED','An explicit native relative bench mapping is required')
    signs=value.get('signs');verified=value.get('directions_verified')
    if not isinstance(signs,dict) or set(signs)!=set(NAMES) or any(type(v) is not int or v not in (-1,1) for v in signs.values()):
        raise Fault('PHYSICAL_MAPPING_REQUIRED','Six explicit native PDA to follower encoder directions are required')
    if not isinstance(verified,dict) or set(verified)!=set(NAMES) or any(type(v) is not bool for v in verified.values()):
        raise Fault('PHYSICAL_MAPPING_REQUIRED','Six explicit direction verification flags are required')
    active=value.get('active_axes',list(NAMES))
    if not isinstance(active,list) or not active or any(not isinstance(name,str) or name not in NAMES for name in active) or len(active)!=len(set(active)):
        raise Fault('PHYSICAL_MAPPING_REQUIRED','Active axes must be a nonempty trusted list of unique named physical axes')
    active=[name for name in NAMES if name in active]
    gains=value.get('gains',dict.fromkeys(NAMES,1.0))
    if not isinstance(gains,dict) or set(gains)!=set(NAMES) or any(not finite(v) or not 0<v<=1 for v in gains.values()):
        raise Fault('PHYSICAL_MAPPING_REQUIRED','Six explicit finite motion gains within (0, 1] are required')
    maximum=value.get('max_anchor_delta_deg')
    jaw=value.get('max_anchor_gripper_ticks')
    if not finite(maximum) or not 0<maximum<=360 or type(jaw) is not int or not 1<=jaw<=100:
        raise Fault('PHYSICAL_MAPPING_REQUIRED','Explicit relative range and registered hardware limits are required')
    return {**copy.deepcopy(value),'gains':dict(gains),'active_axes':active}


def runtime_config_snapshot(config):
    """Public approved fields and hashes of private routing/file bindings."""
    mapping=checked_mapping(config.get('physical_mapping'))
    public_keys=('robot_id','calibration_hash','device_id','expected_profile_id','mapping_id',
                 'commissioned','relative_mapping_verified','gripper_ticks','limits','cameras')
    body={key:copy.deepcopy(config.get(key)) for key in public_keys}
    body['schema']='so101.native_relative_config.v1'
    body['physical_mapping']={key:mapping[key] for key in ('schema','scope','signs','directions_verified','gains',
                    'active_axes','max_anchor_delta_deg','max_anchor_gripper_ticks')}
    body['physical_mapping']['clamp_to_registered_limits']=mapping.get('clamp_to_registered_limits') is True
    source=config.get('teleop_source') or {}
    body['source_module']=source.get('module');body['source_seconds']=source.get('seconds')
    for name,path in [('leader_profile',config.get('leader_profile')),('leader_reference',source.get('reference'))]:
        if path is None:body[name+'_sha256']=None
        else:
            try:body[name+'_sha256']=hashlib.sha256(Path(path).read_bytes()).hexdigest()
            except OSError as error:raise Fault('CONFIG_IDENTITY_UNAVAILABLE','Registered source identity file cannot be frozen') from error
    # Bind local routing locations without publishing absolute paths or URLs.
    for name,value in [('leader_input',config.get('leader_input')),('source_root',source.get('root')),
                       ('receiver',source.get('receiver_url'))]:
        body[name+'_binding_sha256']=hashlib.sha256(str(value).encode()).hexdigest() if value is not None else None
    body['native_arm_anchor_cap_ticks']=4095;body['native_gripper_anchor_cap_ticks']=150
    return body


def runtime_config_digest(config):
    body=runtime_config_snapshot(config)
    try:encoded=json.dumps(body,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    except (TypeError,ValueError) as error:raise Fault('CONFIG_IDENTITY_UNAVAILABLE','Trusted adapter configuration is not canonical finite JSON') from error
    return 'sha256:'+hashlib.sha256(encoded).hexdigest()


class RelativeNativeLeader:
    """One immutable main/follower anchor and a validated, fresh local snapshot."""
    def __init__(self,mapping,model,follower_ticks,profile_hash,gripper_ticks,limits,*,held_goal_ticks=None):
        self.mapping=checked_mapping(mapping);self.model=copy.deepcopy(model)
        self.profile_hash=profile_hash;self.limits=copy.deepcopy(limits)
        self.gripper_ticks=tuple(gripper_ticks);self.anchor_raw=None;self.last_sequence=-1;self.last_mapping=None
        if not isinstance(profile_hash,str) or not profile_hash:
            raise Fault('PROFILE_MISMATCH','Registered leader read-only identity hash required')
        if set(follower_ticks)!=set(NAMES) or any(type(v) is not int or not 0<=v<=4095 for v in follower_ticks.values()):
            raise Fault('INVALID_ANCHOR','Six measured follower encoder ticks required')
        self.anchor_measured=dict(follower_ticks)
        self.active_axes=list(self.mapping['active_axes']);self.held_axes=[name for name in NAMES if name not in self.active_axes]
        if self.held_axes and (not isinstance(held_goal_ticks,dict) or set(held_goal_ticks)!=set(self.held_axes) or
                any(type(v) is not int or not 0<=v<=4095 for v in held_goal_ticks.values())):
            raise Fault('INVALID_ANCHOR','Inactive axes require their actual verified issued-goal anchors')
        self.held_goal_anchor=dict(held_goal_ticks or {})
        self.anchor_follower={**follower_ticks,**self.held_goal_anchor}
        self.anchor_basis={name:'ready_measured_encoder' if name in self.active_axes else 'verified_issued_goal' for name in NAMES}
        low,high=self.gripper_ticks
        if type(low) is not int or type(high) is not int or not 0<=low<high<=4095:
            raise Fault('CALIBRATION_MISMATCH','Explicit closed/open follower encoder bounds required')
        if set(limits)!={name+'.pos' for name in NAMES}:
            raise Fault('CALIBRATION_MISMATCH','Six named physical target limits required')
        for name in NAMES[:5]:
            m=self.model['mapping'][name]
            if not finite(m['zero_ticks']) or not finite(m['ticks_per_radian']) or m['ticks_per_radian']<=0:
                raise Fault('MAPPING_MISMATCH','Owner encoder/angle mapping is invalid')
        self.positions(self.anchor_follower)

    def validate_packet(self,packet):
        now=time.monotonic()
        if not isinstance(packet,dict) or packet.get('profile_hash')!=self.profile_hash:
            raise Fault('PROFILE_MISMATCH','Leader read-only profile identity changed')
        received=packet.get('received_monotonic');expires=packet.get('expires_monotonic')
        if not finite(received) or not finite(expires) or not received<=now<expires or expires-received>.8+1e-6:
            raise Fault('SOURCE_EXPIRED','Fresh same-host physical leader input within 0.8 seconds is required')
        raw=packet.get('native_positions');sequence=packet.get('sequence')
        if not isinstance(raw,dict) or set(raw)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in raw.values()) or type(sequence) is not int or sequence<0:
            raise Fault('INVALID_LEADER_INPUT','Six native PDA integers and increasing sequence required')
        return raw

    def anchor(self,packet):
        if self.anchor_raw is not None:
            raise Fault('ANCHOR_EXISTS','A running relative mapping cannot change its original anchor')
        if not all(self.mapping['directions_verified'][name] for name in self.active_axes):
            raise Fault('DIRECTION_UNVERIFIED','Only physically verified active directions can be anchored')
        self.anchor_raw=dict(self.validate_packet(packet));self.anchor_source_sequence=packet['sequence']

    def positions(self,ticks):
        result={}
        for name in NAMES[:5]:
            mapping=self.model['mapping'][name]
            result[name+'.pos']=math.degrees((ticks[name]-mapping['zero_ticks'])/mapping['ticks_per_radian'])
        low,high=self.gripper_ticks
        result['gripper.pos']=(ticks['gripper']-low)/(high-low)*100
        for name,value in result.items():
            low,high=self.limits[name]
            if not finite(value) or not low<=value<=high:
                raise Fault('LIMIT','Relative physical target leaves the registered limits; no silent clamp')
        return result

    def validate_relative_ticks(self,ticks):
        if not isinstance(ticks,dict) or set(ticks)!=set(NAMES) or any(type(v) is not int or not 0<=v<=4095 for v in ticks.values()):
            raise Fault('LIMIT','Six named physical encoder integers within the actual range required')
        for name in self.held_axes:
            if abs(ticks[name]-self.anchor_follower[name])>1:
                raise Fault('INACTIVE_AXIS_TARGET','Inactive axes must retain their immutable issued-goal anchors')
        for name in NAMES[:5]:
            if abs(ticks[name]-self.anchor_follower[name])>4095:
                raise Fault('RELATIVE_ENVELOPE','Physical target exceeds the encoder range')
            degrees=math.degrees(abs(ticks[name]-self.anchor_follower[name])/self.model['mapping'][name]['ticks_per_radian'])
            if degrees>self.mapping['max_anchor_delta_deg']+1e-8:
                raise Fault('RELATIVE_ENVELOPE','Physical target leaves the original relative bench anchor')
        if abs(ticks['gripper']-self.anchor_follower['gripper'])>self.mapping['max_anchor_gripper_ticks']:
            raise Fault('RELATIVE_ENVELOPE','Gripper target leaves the original relative bench anchor')
        self.positions(ticks)

    def map(self,packet):
        raw=self.validate_packet(packet)
        if self.anchor_raw is None:raise Fault('ANCHOR_MISSING','READY follower and main input must be anchored before following')
        if packet['sequence']<=self.last_sequence:raise Fault('STALE_SEQUENCE','Older leader input cannot replace a new mapping')
        ticks={}
        for motor,name in enumerate(NAMES,1):
            delta=(raw[str(motor)]-self.anchor_raw[str(motor)]+2048)%4096-2048
            ticks[name]=self.anchor_follower[name]+round(self.mapping['signs'][name]*self.mapping['gains'][name]*delta) if name in self.active_axes else self.anchor_follower[name]
            if self.mapping.get('clamp_to_registered_limits') is True and name in self.active_axes:
                low,high=self.limits[name+'.pos']
                if name=='gripper':
                    closed,opened=self.gripper_ticks
                    lower=math.ceil(closed+low*(opened-closed)/100);upper=math.floor(closed+high*(opened-closed)/100)
                else:
                    m=self.model['mapping'][name]
                    lower=math.ceil(m['zero_ticks']+math.radians(low)*m['ticks_per_radian'])
                    upper=math.floor(m['zero_ticks']+math.radians(high)*m['ticks_per_radian'])
                ticks[name]=min(upper,max(lower,ticks[name]))
        self.validate_relative_ticks(ticks)
        result={'positions':self.positions(ticks),'target_ticks':ticks,'sequence':packet['sequence'],
                'source':'physical_native_relative_encoder','mapping_scope':'relative_encoder_bench',
                'native_positions':dict(raw),'received_monotonic':packet['received_monotonic'],
                'expires_monotonic':packet['expires_monotonic'],'anchor_raw':dict(self.anchor_raw),
                'anchor_follower':dict(self.anchor_follower),'gains':dict(self.mapping['gains']),
                'active_axes':list(self.active_axes),'held_axes':list(self.held_axes),
                'held_goal_anchor':dict(self.held_goal_anchor),'anchor_basis':dict(self.anchor_basis)}
        self.last_sequence=packet['sequence'];self.last_mapping=result
        return copy.deepcopy(result)


class NativeRelativeTeleopAdapter(NativeOwnerAdapter):
    source='physical_native_relative_teleop'
    def __init__(self,config,owner=None,*,source_factory=ManagedTeleopSource):
        super().__init__(config,owner)
        checked_mapping(config.get('physical_mapping'))
        self.source_factory=source_factory;self._lock=threading.RLock();self._update_lock=threading.Lock()
        self._pump_stop=threading.Event();self._pump_thread=None;self._generation=0;self._sequence=0
        self._desired_ticks=None;self._source_active=False;self._closing=False;self._held_reusable=False;self._preempting=False
        self._error=None;self._native_attempted=False;self.profile_hash=None;self._begin_key=None
        self._runtime_provenance=None;self._source_receipt=None;self._owner_anchor=None;self._starting_config_digest=None

    def describe(self):
        mapping=checked_mapping(self.config.get('physical_mapping'))
        ready=(self.config.get('commissioned') is True and self.config.get('relative_mapping_verified') is True
               and all(mapping['directions_verified'][name] for name in mapping['active_axes']))
        digest=runtime_config_digest(self.config)
        with self._lock:frozen=copy.deepcopy(self._runtime_provenance)
        changed=frozen is not None and frozen['runtime_config_sha256']!=digest
        ready=ready and not changed
        with self._lock:error=copy.deepcopy(self._error)
        return {'kind':'physical','source':self.source,'robot_id':self.config['robot_id'],
                'calibration_hash':self.config['calibration_hash'],'commissioned':ready,
                'capabilities':['calibration']+(['collection','teleop'] if ready else []),
                'teleop_start_configured':bool(self.config.get('teleop_source')),
                'mapping_scope':'relative_encoder_bench','absolute_zero_verified':False,'tcp_calibrated':False,
                'model_mapping_verified':False,'active_axes':list(mapping['active_axes']),
                'held_axes':[name for name in NAMES if name not in mapping['active_axes']],
                'directions_verified':dict(mapping['directions_verified']),'gains':dict(mapping['gains']),
                'runtime_config_sha256':digest,'configuration_changed_since_ready':changed,
                'teleop_error':error,'limitations':['bounded relative encoder bench only; no task/pose6d/TCP claim',
                                                 'only selected active axes are driven; held directions remain unverified',
                                                 'actual native/profile/model joint limits remain enforced',
                                                 'no automatic fault clear or torque release']}

    def state(self,motion=False):
        value=super().state(motion)
        with self._lock:error=copy.deepcopy(self._error)
        if motion and error:raise Fault(error['code'],error['message'])
        return value

    def connect(self):
        state=self.state()
        if (state.get('operation') or {}).get('status') in ('running','queued','cancelling','stopping'):
            raise Fault('BUSY','Another native owner operation is active')
        with self._lock:
            if self._source_active:raise Fault('BUSY','An existing physical input source is still active')
            if self._pump_thread and self._pump_thread.is_alive():raise Fault('STOP_PENDING','Previous native input watchdog has not exited; retain stop responsibility')
        model=self.owner.call('model',{})
        if model['mapping_id']!=state['mapping_id'] or model['mapping_id']!=self.config.get('mapping_id'):
            raise Fault('MAPPING_MISMATCH','Owner model differs from the commissioned relative coordinate mapping')
        profile=json.loads(Path(self.config['leader_profile']).read_text(encoding='utf-8'))
        if not isinstance(profile.get('profile_hash'),str) or not profile['profile_hash']:
            raise Fault('PROFILE_MISMATCH','Explicit leader read-only identity required')
        with self._lock:
            self.session=state['session_id'];self.model=model;self.profile_hash=profile['profile_hash']
            self.sent=self.positions(state)  # Fresh measured baseline; never replay old stored goals.
            self.leader_reader=None;self._error=None;self._closing=False;self._held_reusable=False
            self._preempting=False
            self.operation=None;self._native_attempted=False;self._begin_key=None
            self._runtime_provenance=None;self._source_receipt=None;self._owner_anchor=None;self._starting_config_digest=None

    def _assert_config_frozen(self):
        with self._lock:
            value=copy.deepcopy(self._runtime_provenance);starting=self._starting_config_digest
        expected=value['runtime_config_sha256'] if value is not None else starting
        if expected is not None and expected!=runtime_config_digest(self.config):
            raise Fault('CONFIG_CHANGED','Trusted mapping/source configuration changed after the batch READY anchor')

    def _freeze_provenance(self,packet):
        reader=self.leader_reader;source=self._source_receipt or {}
        value={'schema':'so101.native_relative_runtime.v1','source_kind':'physical',
               'source':'physical_native_relative_encoder','mapping_scope':'relative_encoder_bench',
               'source_instance':source.get('source_instance'),'source_process_pid':source.get('pid'),
               'source_launcher_pid':source.get('launcher_pid'),'source_identity_verified':source.get('process_identity_verified'),
               'profile_hash':self.profile_hash,'follower_profile_id':self.config['expected_profile_id'],
               'device_id':self.config['device_id'],'model_mapping_id':self.model['mapping_id'],
               'runtime_config_sha256':runtime_config_digest(self.config),
               'runtime_config_snapshot':runtime_config_snapshot(self.config),
               'model_encoder_mapping':{name:{key:self.model['mapping'][name][key] for key in ('zero_ticks','ticks_per_radian')} for name in NAMES[:5]},
               'anchor_source_sequence':reader.anchor_source_sequence,'anchor_raw':dict(reader.anchor_raw),
               'anchor_follower':dict(reader.anchor_follower),'held_goal_anchor':dict(reader.held_goal_anchor),
               'anchor_basis':dict(reader.anchor_basis),'gains':dict(reader.mapping['gains']),
               'signs':dict(reader.mapping['signs']),'directions_verified':dict(reader.mapping['directions_verified']),
               'active_axes':list(reader.active_axes),'held_axes':list(reader.held_axes),
               'absolute_zero_verified':False,'tcp_calibrated':False,'model_mapping_verified':False,
               'pda_basis':'manufacturer PRTV PDA raw encoder; follower coordinates checked by native owner'}
        if len(json.dumps(value,allow_nan=False).encode())>8192:raise Fault('PROVENANCE_LIMIT','Runtime provenance exceeds its bounded JSON budget')
        return value

    def _validate_owner_ticks(self,ticks):
        # An episode restart creates a NEW owner anchor. The immutable batch
        # mapping stays unchanged, but opposite ends can exceed this new scope.
        if self._owner_anchor is None:raise Fault('ANCHOR_MISSING','Current native operation anchor is required')
        for name in NAMES:
            maximum=4095
            if abs(ticks[name]-self._owner_anchor[name])>maximum:
                raise Fault('NATIVE_OPERATION_ENVELOPE','Target exceeds this native operation anchor; no silent re-anchor')

    def _packet(self):
        try:packet=json.loads((Path(self.config['leader_input'])/'input.json').read_text(encoding='utf-8'))
        except (OSError,ValueError,TypeError) as error:raise Fault('SOURCE_EXPIRED','Physical leader input is missing or invalid') from error
        # Validate without importing a simulation normalization context.
        if self.leader_reader:self.leader_reader.validate_packet(packet)
        else:
            now=time.monotonic();received=packet.get('received_monotonic');expires=packet.get('expires_monotonic')
            raw=packet.get('native_positions')
            if packet.get('profile_hash')!=self.profile_hash:raise Fault('PROFILE_MISMATCH','Leader profile identity changed')
            if not finite(received) or not finite(expires) or not received<=now<expires or expires-received>.8+1e-6:
                raise Fault('SOURCE_EXPIRED','Physical leader input expired')
            if not isinstance(raw,dict) or set(raw)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in raw.values()) or type(packet.get('sequence')) is not int or packet['sequence']<0:
                raise Fault('INVALID_LEADER_INPUT','Six native main encoder values required')
        return packet

    @staticmethod
    def _ticks(state,field='position'):
        rows=state.get('joints',[])
        if len(rows)!=6 or {row['name'] for row in rows}!=set(NAMES):raise Fault('RECEIPT_MISSING','Six actual owner encoder rows required')
        value={row['name']:row.get(field) for row in rows}
        if any(type(v) is not int or not 0<=v<=4095 for v in value.values()):raise Fault('RECEIPT_MISSING','Actual finite hardware encoder/goal values required')
        return value

    def _begin(self,deadline,*,reuse=False,expected_generation=None):
        self._assert_config_frozen()
        self._packet();state=self.state(motion=True)
        with self._lock:
            if self._closing or not self._source_active:raise Fault('STOPPED','Physical input has been explicitly stopped')
            if self._preempting or expected_generation is not None and expected_generation!=self._generation:
                raise Fault('STOPPED','An earlier native start/target was invalidated by priority stop')
            if self._error:raise Fault(self._error['code'],self._error['message'])
            if reuse and (not self._held_reusable or self.leader_reader is None):raise Fault('NOT_BOUND','Verified original owned holding is required for reuse')
            self._generation+=1;generation=self._generation;event=threading.Event();self._pump_stop=event
            self._native_attempted=True
            begin_key=uuid.uuid4().hex;self._begin_key=begin_key
        args={'session_id':self.session,'revision':state['revision'],'idempotency_key':begin_key,
              'speed':100,'acceleration':5,'input_ttl_s':.8}
        if reuse:args['reuse_holds_only']=True
        operation=self.owner.call('native_teleop_begin',args)
        if not isinstance(operation,dict) or not isinstance(operation.get('id'),str) or operation.get('kind')!='native_teleop':
            raise Fault('TELEOP_START_UNCONFIRMED','Native owner did not return an actual teleoperation operation')
        with self._lock:self.operation=operation
        while time.monotonic()<deadline:
            if event.is_set():raise Fault('STOPPED','Native teleoperation startup was cancelled')
            packet=self._packet();state=self.state(motion=True);current=state.get('operation') or {}
            if current.get('id')!=operation['id'] or current.get('kind')!='native_teleop':raise Fault('CONTROL_CHANGED','Native teleoperation operation changed')
            if current.get('status')!='running':raise Fault('TELEOP_START_UNCONFIRMED','Native owner stopped before READY')
            if current.get('stream_stage')=='READY':
                anchor=current.get('anchor_ticks')
                if not isinstance(anchor,dict) or set(anchor)!=set(NAMES):raise Fault('RECEIPT_MISSING','Actual READY measured anchor is required')
                with self._lock:
                    if self._closing or event.is_set():raise Fault('STOPPED','Late READY cannot restart physical following')
                    self._assert_config_frozen()
                    stationary=self._ticks(state,'goal')
                    checked=checked_mapping(self.config['physical_mapping'])
                    held=[name for name in NAMES if name not in checked['active_axes']]
                    if held:
                        issued=current.get('sent_ticks')
                        if not isinstance(issued,dict) or set(issued)!=set(NAMES) or any(type(v) is not int for v in issued.values()):
                            raise Fault('RECEIPT_MISSING','Selected-axis READY requires the owning operation issued-goal proof')
                        if any(abs(issued[name]-stationary[name])>1 for name in NAMES):
                            raise Fault('CONTROL_CHANGED','READY goal registers no longer match this native operation')
                    if not reuse:
                        self.leader_reader=RelativeNativeLeader(self.config['physical_mapping'],self.model,anchor,self.profile_hash,self.config['gripper_ticks'],self.config['limits'],
                                                               held_goal_ticks={name:stationary[name] for name in held} if held else None)
                        self.leader_reader.anchor(packet)
                    self.leader_reader.validate_relative_ticks(stationary)
                    self._owner_anchor=dict(anchor)
                    self._validate_owner_ticks(stationary)
                    if not reuse:self._runtime_provenance=self._freeze_provenance(packet)
                    else:self._assert_config_frozen()
                    self._desired_ticks=stationary;self._sequence=0;self._held_reusable=False
                self._update_once(generation,event)
                thread=threading.Thread(target=self._pump,args=(generation,event),daemon=True,name='native-relative-input-watchdog')
                with self._lock:self._pump_thread=thread
                thread.start()
                return {'started':True,'managed':True,'operation_id':operation['id'],'stream_stage':'READY',
                        'anchor_ticks':dict(self.leader_reader.anchor_follower),'mapping_scope':'relative_encoder_bench',
                        'active_axes':list(self.leader_reader.active_axes),'held_axes':list(self.leader_reader.held_axes),
                        'runtime_provenance':copy.deepcopy(self._runtime_provenance),
                        'relative_anchor_preserved':reuse}
            if current.get('stream_stage')!='ACQUIRING':raise Fault('TELEOP_START_UNCONFIRMED','Unknown native startup stage')
            time.sleep(min(.02,max(0,deadline-time.monotonic())))
        raise Fault('TELEOP_START_UNCONFIRMED','Native acquisition did not confirm READY within the bounded startup deadline')

    def start_teleop(self,timeout_s):
        timeout_s=bounded_timeout(timeout_s)
        self._assert_config_frozen()
        if not self.session:raise Fault('NOT_BOUND','Connect the physical owner before teleoperation')
        with self._lock:entry_generation=self._generation
        self.state(motion=True)
        if not self.config.get('teleop_source'):raise Fault('TELEOP_UNAVAILABLE','A managed local read-only main input is required')
        with self._lock:
            if self._closing or self._preempting or entry_generation!=self._generation or self._source_active:raise Fault('BUSY','Physical source has already been started, cancelled or explicitly closed')
            self._starting_config_digest=runtime_config_digest(self.config)
        deadline=time.monotonic()+timeout_s
        if self.teleop_source is None:
            self.teleop_source=self.source_factory(self.config['teleop_source'],input_root=self.config['leader_input'],profile_hash=self.profile_hash)
        try:
            source=self.teleop_source.start(min(timeout_s,max(.1,timeout_s/2)))
            if not isinstance(source,dict) or source.get('started') is not True or source.get('managed') is not True:raise Fault('TELEOP_START_UNCONFIRMED','Actual managed main input receipt required')
            instance=source.get('source_instance')
            if not isinstance(instance,str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',instance):
                raise Fault('TELEOP_START_UNCONFIRMED','The actual managed source instance is required for this physical batch')
            with self._lock:
                if self._closing or self._preempting or entry_generation!=self._generation:raise Fault('STOPPED','A late main input receipt cannot revive a stopped start')
                self._source_active=True
                self._source_receipt={key:copy.deepcopy(source.get(key)) for key in
                    ('source_instance','pid','launcher_pid','process_identity_verified')}
            result=self._begin(deadline,expected_generation=entry_generation)
            return {**result,'source_receipt':source}
        except BaseException:
            with self._lock:self._pump_stop.set();self._source_active=False;self._closing=True;attempted=self._native_attempted
            if attempted:
                try:self.hold()
                except Exception:pass  # Peer retains full stop/reconciliation responsibility.
            try:self.teleop_source.stop(min(2,timeout_s))
            except Exception:pass
            raise

    def _update_once(self,generation,event):
        self._assert_config_frozen()
        self._packet()
        # Only updates serialize. HOLD never waits for this lock or an HTTP read.
        with self._update_lock:
            with self._lock:
                if event.is_set() or self._closing or self._preempting or generation!=self._generation or not self._source_active or not self.operation:
                    raise Fault('STOPPED','Cancelled input cannot send another native target')
                operation_id=self.operation['id'];target=dict(self._desired_ticks);sequence=self._sequence;self._sequence+=1
                self._validate_owner_ticks(target)
            self._packet()
            result=self.owner.call('native_teleop_update',{'session_id':self.session,'operation_id':operation_id,
                                                        'sequence':sequence,'target_ticks':target})
            with self._lock:
                if event.is_set() or generation!=self._generation:raise Fault('STOPPED','A late native acknowledgement cannot revive a stopped stream')
            return result

    def _pump(self,generation,event):
        while not event.wait(.1):
            try:self._update_once(generation,event)
            except Exception as error:
                with self._lock:
                    if event.is_set() or generation!=self._generation:return
                    begin_key=self._begin_key
                    self._error={'code':getattr(error,'code',type(error).__name__),'message':str(error),'at':time.time()}
                    event.set()
                try:
                    with self._lock:
                        if generation!=self._generation:return
                    stopped=self.owner.call('stop',{'session_id':self.session,**({'cancel_teleop_key':begin_key} if begin_key else {})})
                    with self._lock:
                        if generation==self._generation and self._error:self._error['stop_result']=copy.deepcopy(stopped)
                except Exception as stop_error:
                    with self._lock:
                        if generation==self._generation and self._error:self._error['stop_error']=str(stop_error)
                return

    def leader(self):
        self._assert_config_frozen()
        with self._lock:error=copy.deepcopy(self._error);reader=self.leader_reader;active=self._source_active;closing=self._closing
        if error:raise Fault(error['code'],error['message'])
        if not active or closing or reader is None:raise Fault('TELEOP_UNAVAILABLE','READY physical main/follower anchors are required')
        deadline=time.monotonic()+.2
        while True:
            packet=self._packet()
            if packet['sequence']>reader.last_sequence:
                mapped=reader.map(packet)
                with self._lock:provenance=copy.deepcopy(self._runtime_provenance)
                provenance.update(schema='so101.native_relative_frame.v1',source_sequence=mapped['sequence'],
                    native_positions=dict(mapped['native_positions']),received_monotonic=mapped['received_monotonic'],
                    expires_monotonic=mapped['expires_monotonic'])
                if len(json.dumps(provenance,allow_nan=False).encode())>8192:raise Fault('PROVENANCE_LIMIT','Frame provenance exceeds its bounded JSON budget')
                return {'positions':mapped['positions'],'provenance':provenance}
            if time.monotonic()>=deadline:raise Fault('STALE_SEQUENCE','No new physical main input arrived')
            time.sleep(.005)

    def send(self,target):
        self._assert_config_frozen()
        with self._lock:entry_generation=self._generation
        state=self.state(motion=True)
        if set(target)!={name+'.pos' for name in NAMES} or any(not finite(v) for v in target.values()):raise Fault('INVALID_TARGET','Six finite named physical targets required')
        for name,value in target.items():
            low,high=self.config['limits'][name]
            if not low<=value<=high:raise Fault('LIMIT','Physical target outside commissioned limits')
        with self._lock:
            if self._closing or self._preempting or entry_generation!=self._generation or not self._source_active:raise Fault('STOPPED','Physical input is not active or its target was cancelled; do not auto-resume after full stop')
            operation=self.operation;reader=self.leader_reader;reuse=self._held_reusable
        if reader is None:raise Fault('NOT_BOUND','Original physical relative anchor is missing')
        ticks={name:round(self.model['mapping'][name]['zero_ticks']+math.radians(target[name+'.pos'])*self.model['mapping'][name]['ticks_per_radian']) for name in NAMES[:5]}
        low,high=self.config['gripper_ticks'];ticks['gripper']=round(low+target['gripper.pos']/100*(high-low))
        reader.validate_relative_ticks(ticks);self._packet()
        # The accepted one-tick number round-trip is not permission to move an
        # inactive axis. Canonicalise it back to the immutable issued goal.
        for name in reader.held_axes:ticks[name]=reader.anchor_follower[name]
        self._validate_owner_ticks(ticks)
        if operation is None:
            if not reuse:raise Fault('NOT_BOUND','Native stream is not READY and has no verified held-only reuse')
            self._begin(time.monotonic()+5,reuse=True,expected_generation=entry_generation)
        else:
            live=state.get('operation') or {}
            if live.get('id')!=operation['id'] or live.get('kind')!='native_teleop' or live.get('status')!='running' or live.get('stream_stage')!='READY':
                raise Fault('CONTROL_CHANGED','Current native READY stream is no longer owned')
        self._validate_owner_ticks(ticks)
        with self._lock:self._desired_ticks=dict(ticks);generation=self._generation;event=self._pump_stop
        self._update_once(generation,event)
        # Read actual hardware goal registers after acceptance. Do not return the
        # queued/requested vector or treat measured feedback as sent action.
        actual=self.state(motion=True)
        live=actual.get('operation') or {}
        if event.is_set() or live.get('id')!=self.operation['id'] or live.get('status')!='running':
            raise Fault('CONTROL_CHANGED','Native stream changed before its actual sent receipt')
        sent=self.goals(actual)
        with self._lock:self.sent=dict(sent)
        return dict(sent)

    def preempt_stop(self,*,close=False):
        """Reach native priority stop before the ROS peer waits for issue_lock.

        This hook is a cancellation fence, not the final holding proof. A pending
        BEGIN key is cancelled even if no actual native operation has yet begun.
        """
        with self._lock:
            self._preempting=True;self._pump_stop.set();self._generation+=1;self._held_reusable=False
            if close:self._closing=True;self._source_active=False
            attempted=self._native_attempted;begin_key=self._begin_key;session=self.session
        if not attempted:return {'preempted':True,'native_stop_attempted':False}
        if not session or not begin_key:raise Fault('STOP_UNCONFIRMED','Native acquisition attempt has no cancellation identity')
        result=self.owner.call('stop',{'session_id':session,'cancel_teleop_key':begin_key})
        return {'preempted':True,'native_stop_attempted':True,'native_result':result}

    def hold(self):
        if not self.session:raise Fault('NOT_BOUND','No adopted native owner session')
        with self._lock:self._preempting=True;self._pump_stop.set();thread=self._pump_thread;begin_key=self._begin_key
        # Priority owner stop is deliberately independent of update/HTTP locks.
        result=self.owner.call('stop',{'session_id':self.session,**({'cancel_teleop_key':begin_key} if begin_key else {})})
        if result.get('hold_confirmed') is not True or result.get('action_queue_empty') is not True:
            raise Fault('STOP_UNCONFIRMED','Native owner must confirm actual holding and an empty action queue')
        with self._lock:
            self.operation=None;self._held_reusable=self.leader_reader is not None and self._source_active and not self._closing and self._error is None
            self._preempting=False
        if thread and thread is not threading.current_thread():thread.join(.1)
        return {'holding':True,'action_queue_empty':True,'native_result':result}

    def stop_teleop(self,timeout_s):
        timeout_s=bounded_timeout(timeout_s)
        with self._lock:self._closing=True;self._source_active=False;self._pump_stop.set();self._held_reusable=False
        if self.teleop_source is None:return {'stopped':True,'managed':True,'process_started':False}
        return self.teleop_source.stop(timeout_s)

    def primitive(self,p):raise Fault('UNSUPPORTED','Relative physical teleoperation does not execute tasks or Cartesian primitives')
