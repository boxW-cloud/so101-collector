"""Typed local training and artifact contracts; no platform or network fields."""
from dataclasses import dataclass, field, fields
from pathlib import Path
from enum import Enum
from types import MappingProxyType
from collections.abc import Mapping
import json
from lerobot_sdk.core.errors import ConfigurationError
from lerobot_sdk.core.types import integer


def freeze(value):
    if isinstance(value,Mapping):return MappingProxyType({str(k):freeze(v) for k,v in value.items()})
    if isinstance(value,(list,tuple)):return tuple(freeze(v) for v in value)
    return value


def plain(value):
    if isinstance(value,Mapping):return {k:plain(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [plain(v) for v in value]
    if isinstance(value,Path):return str(value)
    return value


@dataclass(frozen=True)
class TrainConfig:
    dataset_dir: Path
    policy_type: str = 'act'
    batch_size: int = 8
    steps: int = 10000  # total step to reach, including the resumed step counter
    seed: int = 42
    device: str = 'cpu'
    output_dir: Path | None = None  # a new attempt directory, never an existing run
    base_model_dir: Path | None = None
    policy_params: Mapping = field(default_factory=dict)
    scheduler_params: Mapping | None = None
    save_freq: int = 1000
    num_workers: int = 0
    cpu_threads: int = 2

    def __post_init__(self):
        if self.dataset_dir is None:raise ConfigurationError('dataset_dir required')
        for key in ('batch_size','steps','save_freq','cpu_threads'):integer(getattr(self,key),key,minimum=1)
        integer(self.seed,'seed');integer(self.num_workers,'num_workers')
        if self.num_workers!=0:raise ConfigurationError('Managed resume currently supports num_workers=0 only')
        if self.policy_type not in ('act','pi05'):raise ConfigurationError('Only ACT and pi05 adapters are supported')
        if self.policy_type=='pi05' and self.base_model_dir is None:raise ConfigurationError('pi05 requires a locally prepared, six-axis bound pretrained base')
        if not isinstance(self.device,str) or self.device not in ('cpu','cuda') and not (self.device.startswith('cuda:') and self.device[5:].isdigit()):
            raise ConfigurationError('Explicit cpu or cuda device required')
        for key in ('dataset_dir','output_dir','base_model_dir'):
            value=getattr(self,key)
            if value is not None:object.__setattr__(self,key,Path(value).expanduser().resolve())
        if not isinstance(self.policy_params,Mapping):raise ConfigurationError('policy_params must be a mapping')
        reserved={'device','push_to_hub','repo_id','pretrained_path','pretrained_revision','input_features','output_features','use_peft','use_amp'}
        if reserved.intersection(self.policy_params):raise ConfigurationError('Device, paths, features and publishing flags are managed by the local adapter')
        try:json.dumps(plain(self.policy_params),allow_nan=False)
        except (ValueError,TypeError) as exc:raise ConfigurationError('Policy parameters must contain finite JSON values') from exc
        object.__setattr__(self,'policy_params',freeze(self.policy_params))
        if self.scheduler_params is not None:
            if not isinstance(self.scheduler_params,Mapping):raise ConfigurationError('scheduler_params must be a mapping')
            try:json.dumps(plain(self.scheduler_params),allow_nan=False)
            except (TypeError,ValueError) as exc:raise ConfigurationError('Scheduler parameters must contain finite JSON values') from exc
            object.__setattr__(self,'scheduler_params',freeze(self.scheduler_params))

    def to_dict(self):return {f.name:plain(getattr(self,f.name)) for f in fields(self)}


@dataclass(frozen=True)
class ValidatedTrainConfig:
    config: TrainConfig
    dataset_hash: str
    native_config_json: str
    base_model_hash: str | None = None


@dataclass(frozen=True)
class PolicyInfo:
    policy_type: str
    available: bool
    trainable: bool
    reason: str | None = None


class JobState(str,Enum):
    STARTING='STARTING';RUNNING='RUNNING';CANCELLING='CANCELLING'
    SUCCEEDED='SUCCEEDED';CANCELLED='CANCELLED';FAILED='FAILED'


@dataclass(frozen=True)
class TrainingStatus:
    job_id: str
    state: JobState
    step: int
    target_steps: int
    checkpoint_dir: Path | None
    error: str | None


@dataclass(frozen=True)
class TrainingEvent:
    sequence: int
    kind: str
    detail: Mapping
    def __post_init__(self):object.__setattr__(self,'detail',freeze(self.detail))


@dataclass(frozen=True)
class TrainResult:
    job_id: str
    output_dir: Path
    model_dir: Path
    checkpoint_dir: Path
    step: int
    dataset_hash: str
    resumed_from: Path | None


@dataclass(frozen=True)
class ModelManifest:
    model_dir: Path
    policy_type: str
    content_hash: str
    dataset_hash: str
    step: int
    contract: Mapping
    input_features: Mapping
    output_features: Mapping
    def __post_init__(self):
        for name in ('contract','input_features','output_features'):object.__setattr__(self,name,freeze(getattr(self,name)))


@dataclass(frozen=True)
class DeploymentRef:
    model_dir: Path
    content_hash: str


@dataclass(frozen=True)
class LoadReport:
    device: str
    step_shape: tuple[int,...]
    chunk_shape: tuple[int,...]
    finite: bool
