from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from collections.abc import Mapping
import copy
import math
import re
from lerobot_sdk.core.types import named, calibration, integer, finite
from lerobot_sdk.core.errors import ConfigurationError

SEEED_COMMIT = '4aaff99be4a1d81568c08c8f0296b41b40c99ec4'
DATA_SCHEMA = 'so101-sdk.sent-action.v1'


# 2026-10-29：数据集里的 6 个关节可以按机器人自己的定义写（名字、单位），不一定是 SO101 的槽位。
# SDK 内部（控制、校验、录制）仍用 SO101 的 6 个槽位；写数据集时按 from_internal 的线性关系
# （数据集值 = scale × 内部值 + offset）换回机器人自己的数值。没有 dataset_schema 时与以前完全一样。
JOINT_SCHEMA_UNITS=('rad','deg','m','normalized_0_100','normalized_-100_100')
# 2026-11-03：数据集的 action 列怎么来（dataset_schema 可选的 action 键，不写就是 sent_target）：
#   sent_target             实际发给从臂的目标（SO101 主臂带从臂，LeRobot 的原本含义）
#   next_observation_state  下一帧的实测状态。数据源没有下发的关节指令时用（Bobac3：机器人由平台控制，
#                           采集容器只读数据）。录制时每帧先压住，等下一帧到了再写，所以每条最后一帧丢掉。
#   source_action           数据源随观测一起给的、这一帧实际下发的目标（观测的 source_metadata['source_action']，
#                           由数据源按观测编号配好对）。没有配上的帧跳过不写，连续太多帧配不上就判这一条失败。
ACTION_RULES=('sent_target','next_observation_state','source_action','leader_action')


def joint_schema(value):
    if value is None:return None
    # 2026-11-05：只写 action 的 schema = 标准 SO101 关节（so101_follower，关节名、单位和数值都不换），只换 action 的来源
    if isinstance(value,Mapping) and set(value)=={'action'}:
        if value['action'] not in ACTION_RULES:
            raise ConfigurationError('dataset_schema action must be one of '+', '.join(ACTION_RULES))
        return {'action':value['action']}
    required={'robot_type','joint_names','units','from_internal'}
    if not isinstance(value,Mapping) or not required<=set(value)<=required|{'action'}:
        raise ConfigurationError('dataset_schema needs exactly robot_type, joint_names, units, from_internal (and optionally action)')
    if 'action' in value and value['action'] not in ACTION_RULES:
        raise ConfigurationError('dataset_schema action must be one of '+', '.join(ACTION_RULES))
    robot_type=value['robot_type']
    if not isinstance(robot_type,str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}',robot_type) or robot_type=='so101_follower':
        raise ConfigurationError('dataset_schema robot_type must be a lowercase identifier other than so101_follower')
    names=value['joint_names'];units=value['units'];linear=value['from_internal']
    if (not isinstance(names,(list,tuple)) or len(names)!=6 or len(set(names))!=6 or
        not all(isinstance(n,str) and re.fullmatch(r'[A-Za-z][A-Za-z0-9_.]{0,63}',n) for n in names)):
        raise ConfigurationError('dataset_schema joint_names must be six unique identifiers')
    if not isinstance(units,(list,tuple)) or len(units)!=6 or any(u not in JOINT_SCHEMA_UNITS for u in units):
        raise ConfigurationError('dataset_schema units must be six of '+', '.join(JOINT_SCHEMA_UNITS))
    pairs=[]
    if not isinstance(linear,(list,tuple)) or len(linear)!=6:raise ConfigurationError('dataset_schema from_internal needs six [scale, offset]')
    for pair in linear:
        if (not isinstance(pair,(list,tuple)) or len(pair)!=2 or
            any(isinstance(v,bool) or not isinstance(v,(int,float)) or not math.isfinite(v) for v in pair) or pair[0]==0):
            raise ConfigurationError('dataset_schema from_internal entries must be finite [scale!=0, offset]')
        pairs.append([float(pair[0]),float(pair[1])])
    result={'robot_type':robot_type,'joint_names':list(names),'units':list(units),'from_internal':pairs}
    if 'action' in value:result['action']=value['action']   # 不写 action 的 schema 和以前完全一样（登记 hash 不变）
    return result


def manifest_joint_contract_ok(robot_type,names,units):
    """manifest 里 robot_type / joint_names / units 是否为登记过的机器人自定义关节（SO101 由调用方另行检查）。"""
    try:joint_schema({'robot_type':robot_type,'joint_names':names,'units':units,'from_internal':[[1,0]]*6})
    except ConfigurationError:return False
    return True


class CollectionState(str, Enum):
    CREATED='CREATED'
    STARTING='STARTING'
    RECORDING='RECORDING'
    SAVING='SAVING'
    REDOING='REDOING'
    RESETTING='RESETTING'
    FINALIZING='FINALIZING'
    LOCAL_READY='LOCAL_READY'
    RECOVERABLE='RECOVERABLE'


@dataclass(frozen=True)
class CollectionConfig:
    name: str
    description: str
    root: Path
    fps: int
    task: str
    robot_id: str
    calibration_hash: str
    units: Mapping[str, str]
    camera_shapes: Mapping[str, tuple[int, int, int]]
    min_free_bytes: int = 256 * 1024 * 1024
    max_duration_s: float = 300
    max_episode_frames: int = 18000
    leader_timeout_s: float = .2
    dataset_schema: Mapping | None = None

    def __post_init__(self):
        if not isinstance(self.name,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}',self.name):
            raise ConfigurationError('Task name must be a safe local identifier')
        if not isinstance(self.description,str) or not isinstance(self.task,str) or not self.task.strip():
            raise ConfigurationError('Description and nonempty task text required')
        if not isinstance(self.robot_id,str) or not self.robot_id.strip():
            raise ConfigurationError('robot_id required')
        object.__setattr__(self,'root',Path(self.root).expanduser().resolve())
        integer(self.fps,'fps',minimum=1)
        integer(self.min_free_bytes,'min_free_bytes')
        integer(self.max_episode_frames,'max_episode_frames',minimum=1)
        finite(self.max_duration_s,'max_duration_s',positive=True)
        finite(self.leader_timeout_s,'leader_timeout_s',positive=True)
        calibration(self.calibration_hash)
        object.__setattr__(self,'units',named(self.units,'units',units=True))
        if not isinstance(self.camera_shapes,Mapping) or not self.camera_shapes:
            raise ConfigurationError('At least one camera is required')
        shapes={}
        for key,shape in self.camera_shapes.items():
            if not isinstance(key,str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*',key):
                raise ConfigurationError('Camera key must be a safe identifier')
            if not isinstance(shape,(tuple,list)) or len(shape)!=3 or shape[2]!=3:
                raise ConfigurationError('Camera shape must be H,W,3')
            for n in shape:integer(n,'camera dimension',minimum=1)
            if shape[0]%2 or shape[1]%2:
                raise ConfigurationError('H.264 yuv420p requires even image dimensions')
            shapes[key]=tuple(shape)
        object.__setattr__(self,'camera_shapes',MappingProxyType(shapes))
        object.__setattr__(self,'dataset_schema',joint_schema(self.dataset_schema))

    def to_dict(self):
        return {**self.__dict__,'root':str(self.root),'units':dict(self.units),
                'camera_shapes':dict(self.camera_shapes),'dataset_schema':copy.deepcopy(self.dataset_schema)}


@dataclass(frozen=True)
class EpisodeInfo:
    episode_index: int
    frames: int
    task: str
    content_hash: str


@dataclass(frozen=True)
class CollectionStatus:
    task_id: str
    state: CollectionState
    committed_episodes: int
    current_frames: int
    error: str | None


@dataclass(frozen=True)
class DatasetRef:
    root: Path
    dataset_id: str
    version: str
    content_hash: str


@dataclass(frozen=True)
class DatasetInspection:
    ref: DatasetRef
    episodes: int
    frames: int
    files: int
    bytes: int
    schema: str
    camera_keys: tuple[str,...]
    native_readable: bool


@dataclass(frozen=True)
class TransferResult:
    storage_ref: str
    content_hash: str


@dataclass(frozen=True)
class TransferProgress:
    """Latest bounded progress snapshot; totals include manifest bytes."""
    completed_bytes: int
    total_bytes: int
    phase: str
