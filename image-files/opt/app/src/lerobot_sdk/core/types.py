from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from collections.abc import Mapping
import math
import re
import numpy as np
from numpy.typing import NDArray
from lerobot_sdk.core.errors import ContractMismatch, ConfigurationError

JOINT_NAMES = ('shoulder_pan.pos', 'shoulder_lift.pos', 'elbow_flex.pos',
               'wrist_flex.pos', 'wrist_roll.pos', 'gripper.pos')
DEGREE_UNITS = MappingProxyType(dict(zip(JOINT_NAMES, ('deg',)*5+('normalized_0_100',), strict=True)))
NORMALIZED_UNITS = MappingProxyType(dict(zip(JOINT_NAMES, ('normalized_-100_100',)*5+('normalized_0_100',), strict=True)))

def position_domain(positions,units):
    if not 0<=positions['gripper.pos']<=100:raise ContractMismatch('Normalized gripper must be in [0,100]')
    if units==NORMALIZED_UNITS and any(not -100<=positions[k]<=100 for k in JOINT_NAMES[:5]):
        raise ContractMismatch('Normalized body position must be in [-100,100]')

def finite(value, name, *, positive=False, nonnegative=False):
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, float, np.integer, np.floating)):
        raise ContractMismatch(f'{name} must be a finite number, not {type(value).__name__}')
    value = float(value)
    if not math.isfinite(value) or (positive and value <= 0) or (nonnegative and value < 0):
        raise ContractMismatch(f'Invalid {name}: {value}')
    return value

def integer(value, name, *, minimum=0):
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractMismatch(f'{name} must be an integer >= {minimum}')
    return value

def calibration(value):
    if not isinstance(value, str) or not re.fullmatch(r'sha256:[0-9a-f]{64}', value):
        raise ContractMismatch('calibration_hash must be sha256:<64 lowercase hexadecimal digits>')
    return value

def named(values, name, *, units=False, positive=False):
    if not isinstance(values, Mapping) or set(values) != set(JOINT_NAMES):
        raise ContractMismatch(f'{name} requires exactly the six canonical joint names')
    result = {k:values[k] if units else finite(values[k], f'{name}.{k}', positive=positive) for k in JOINT_NAMES}
    if units and result not in (DEGREE_UNITS, NORMALIZED_UNITS):
        raise ContractMismatch('Body units must consistently be deg or normalized_-100_100; gripper must be normalized_0_100')
    return MappingProxyType(result)

def immutable_array(values, dtype, ndim):
    if not isinstance(values, np.ndarray) or values.dtype != np.dtype(dtype) or values.ndim != ndim:
        raise ContractMismatch(f'Expected {np.dtype(dtype)} ndarray with {ndim} dimensions')
    # Immutable bytes backing prevents setflags(write=True), unlike a readonly view.
    return np.frombuffer(values.tobytes(order='C'), dtype=dtype).reshape(values.shape)

@dataclass(frozen=True)
class JointTarget:
    positions: Mapping[str, float]
    units: Mapping[str, str]
    calibration_hash: str
    def __post_init__(self):
        object.__setattr__(self, 'positions', named(self.positions, 'positions'))
        object.__setattr__(self, 'units', named(self.units, 'units', units=True))
        calibration(self.calibration_hash)
        position_domain(self.positions,self.units)

@dataclass(frozen=True)
class ArmState:
    sequence: int
    captured_monotonic_ns: int
    calibration_hash: str
    positions: Mapping[str, float]
    units: Mapping[str, str]
    age_ms: float
    def __post_init__(self):
        integer(self.sequence, 'sequence'); integer(self.captured_monotonic_ns, 'captured_monotonic_ns', minimum=1)
        calibration(self.calibration_hash); object.__setattr__(self,'age_ms',finite(self.age_ms, 'age_ms', nonnegative=True))
        object.__setattr__(self, 'positions', named(self.positions, 'positions'))
        object.__setattr__(self, 'units', named(self.units, 'units', units=True))
        position_domain(self.positions,self.units)

@dataclass(frozen=True)
class ImageFrame:
    rgb: NDArray[np.uint8]
    captured_monotonic_ns: int
    age_ms: float
    camera_key: str = ''
    def __post_init__(self):
        a = immutable_array(self.rgb, np.uint8, 3)
        if a.shape[2] != 3 or min(a.shape[:2]) <= 0:
            raise ContractMismatch('RGB must have nonempty shape H x W x 3')
        object.__setattr__(self, 'rgb', a)
        integer(self.captured_monotonic_ns, 'image timestamp', minimum=1)
        object.__setattr__(self,'age_ms',finite(self.age_ms, 'image age_ms', nonnegative=True))
        if not isinstance(self.camera_key, str): raise ContractMismatch('camera_key must be str')
    @property
    def width(self): return self.rgb.shape[1]
    @property
    def height(self): return self.rgb.shape[0]
    @property
    def encoding(self): return 'RGB'

@dataclass(frozen=True)
class Observation:
    arm: ArmState
    images: Mapping[str, ImageFrame]
    task: str
    source_metadata: Mapping[str,object]|None = None
    def __post_init__(self):
        if not isinstance(self.arm, ArmState) or not isinstance(self.task, str) or not self.task.strip():
            raise ContractMismatch('Observation requires ArmState and nonempty task text')
        if not isinstance(self.images, Mapping): raise ContractMismatch('images must be a mapping')
        for key, frame in self.images.items():
            if not isinstance(key, str) or not key or not isinstance(frame, ImageFrame):
                raise ContractMismatch('Invalid camera entry')
            if frame.camera_key and frame.camera_key != key: raise ContractMismatch('camera_key mismatch')
        object.__setattr__(self, 'images', MappingProxyType(dict(self.images)))
        from .provenance import bounded_metadata
        object.__setattr__(self,'source_metadata',bounded_metadata(self.source_metadata))

@dataclass(frozen=True)
class ActionChunk:
    source_sequence: int
    names: tuple[str, ...]
    units: tuple[str, ...]
    values: NDArray[np.float32]  # K × 6，已完成模型后处理
    period_s: float
    max_observation_age_s: float
    def __post_init__(self):
        integer(self.source_sequence, 'source_sequence')
        if not isinstance(self.names,(tuple,list)) or tuple(self.names) != JOINT_NAMES: raise ContractMismatch('Chunk names must use canonical order')
        if not isinstance(self.units,(tuple,list)) or len(self.units) != 6: raise ContractMismatch('Chunk requires six units')
        named(dict(zip(JOINT_NAMES, self.units, strict=True)), 'chunk units', units=True)
        a = immutable_array(self.values, np.float32, 2)
        if a.shape[0] == 0 or a.shape[1] != 6 or not np.isfinite(a).all():
            raise ContractMismatch('Chunk must be finite, nonempty float32[K,6]')
        if np.any(a[:,-1]<0) or np.any(a[:,-1]>100):raise ContractMismatch('Chunk gripper outside [0,100]')
        if self.units[0]=='normalized_-100_100' and (np.any(a[:,:5]<-100) or np.any(a[:,:5]>100)):
            raise ContractMismatch('Chunk normalized body outside [-100,100]')
        object.__setattr__(self, 'values', a)
        object.__setattr__(self, 'names', tuple(self.names)); object.__setattr__(self, 'units', tuple(self.units))
        object.__setattr__(self,'period_s',finite(self.period_s, 'period_s', positive=True))
        object.__setattr__(self,'max_observation_age_s',finite(self.max_observation_age_s, 'max_observation_age_s', positive=True))

class SessionState(str, Enum):
    DISCONNECTED='DISCONNECTED'; IDLE='IDLE'; EXECUTING='EXECUTING'
    RECORDING='RECORDING'; INFERENCING='INFERENCING'; HOLDING='HOLDING'
    FAULT='FAULT'; E_STOP='E_STOP'

@dataclass(frozen=True)
class SessionStatus:
    state: SessionState
    epoch: int
    control_owner: str | None
    fault: str | None

@dataclass(frozen=True)
class ActionReceipt:
    requested_target: JointTarget
    sent_target: JointTarget
    epoch: int
    sent_monotonic_ns: int
    # Deliberately no measured position or implicit "arrived" flag.

@dataclass(frozen=True)
class ActionResult:
    requested_target: JointTarget
    last_receipt: ActionReceipt
    final_state: ArmState
    stable_frames: int

@dataclass(frozen=True)
class StopResult:
    state: SessionState
    epoch: int
    holding: bool
    measured_state: ArmState | None
    reason: str

@dataclass(frozen=True)
class RobotConfig:
    robot_id: str
    calibration_hash: str
    units: Mapping[str, str]
    limits: Mapping[str, tuple[float, float]]
    max_velocity: Mapping[str, float]
    max_step: Mapping[str, float]
    tolerance: Mapping[str, float]
    gripper_open: float
    gripper_closed: float
    release_pose: JointTarget | None = None
    fps: float = 30
    stable_frames: int = 3
    action_timeout_s: float = 10
    io_timeout_s: float = .2
    max_observation_age_s: float = .25
    max_sensor_skew_s: float = .2
    required_cameras: tuple[str, ...] = ()
    event_capacity: int = 32
    close_policy: str = 'hold'
    def __post_init__(self):
        if not isinstance(self.robot_id, str) or not self.robot_id.strip(): raise ConfigurationError('robot_id required')
        calibration(self.calibration_hash)
        object.__setattr__(self, 'units', named(self.units, 'units', units=True))
        if not isinstance(self.limits,Mapping) or set(self.limits) != set(JOINT_NAMES): raise ConfigurationError('All joint limits required')
        limits={}
        for k in JOINT_NAMES:
            pair=self.limits[k]
            if not isinstance(pair,(tuple,list)) or len(pair)!=2: raise ConfigurationError('Limit must be (min,max)')
            lo,hi=(finite(x,'limit') for x in pair)
            if lo>=hi: raise ConfigurationError('Invalid joint limit order')
            limits[k]=(lo,hi)
        if limits['gripper.pos'][0]<0 or limits['gripper.pos'][1]>100: raise ConfigurationError('Gripper limits must stay in [0,100]')
        if self.units==NORMALIZED_UNITS and any(lo < -100 or hi > 100 for k,(lo,hi) in limits.items() if k!='gripper.pos'):
            raise ConfigurationError('Normalized body limits must stay in [-100,100]')
        object.__setattr__(self,'limits',MappingProxyType(limits))
        for field in ('max_velocity','max_step','tolerance'):
            object.__setattr__(self,field,named(getattr(self,field),field,positive=True))
        for field in ('fps','action_timeout_s','io_timeout_s','max_observation_age_s','max_sensor_skew_s'):
            finite(getattr(self,field),field,positive=True)
        integer(self.stable_frames,'stable_frames',minimum=1);integer(self.event_capacity,'event_capacity',minimum=1)
        if self.close_policy!='hold': raise ConfigurationError('Stage one only supports explicit hold-on-close')
        if not isinstance(self.required_cameras,(tuple,list)):raise ConfigurationError('Camera keys must be a sequence')
        cameras=tuple(self.required_cameras)
        if len(cameras)!=len(set(cameras)) or any(not isinstance(k,str) or not k for k in cameras):raise ConfigurationError('Invalid camera keys')
        object.__setattr__(self,'required_cameras',cameras)
        for v in (self.gripper_open,self.gripper_closed):
            if not limits['gripper.pos'][0]<=finite(v,'gripper target')<=limits['gripper.pos'][1]:raise ConfigurationError('Gripper endpoint outside limits')
        if self.release_pose is not None:self.validate_target(self.release_pose)
    def validate_target(self, target):
        if not isinstance(target,JointTarget):raise ContractMismatch('Expected JointTarget, not an unlabelled array')
        if target.calibration_hash!=self.calibration_hash or target.units!=self.units:raise ContractMismatch('Calibration hash or units mismatch')
        if any(not self.limits[k][0]<=v<=self.limits[k][1] for k,v in target.positions.items()):raise ContractMismatch('Target outside configured limits')
