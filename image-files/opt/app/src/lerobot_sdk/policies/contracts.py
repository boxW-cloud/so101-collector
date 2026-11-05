from dataclasses import dataclass
from enum import Enum
from lerobot_sdk.core.types import ActionChunk,finite,integer
from lerobot_sdk.core.errors import ConfigurationError


@dataclass(frozen=True)
class PolicyCapabilities:
    policy_type: str
    model_version: str
    supports_step: bool
    supports_chunk: bool
    relative_actions: bool
    memory_enabled: bool
    rtc_enabled: bool
    chunk_size: int
    period_s: float


@dataclass(frozen=True)
class ActionEnvelope:
    session_id: str
    epoch: int
    model_version: str
    chunk_sequence: int
    first_action_index: int
    captured_monotonic_ns: int
    generated_monotonic_ns: int
    chunk: ActionChunk


@dataclass(frozen=True)
class RunConfig:
    mode: str = 'chunk'
    fps: float | None = None
    max_steps: int = 300
    max_duration_s: float = 60
    max_observation_age_s: float = .25
    inference_timeout_s: float = 5
    queue_capacity: int = 100
    low_watermark: int = 2
    stop_timeout_s: float = 5
    metrics_capacity: int = 1024

    def __post_init__(self):
        if self.mode not in ('step','chunk'):raise ConfigurationError('mode must be step or chunk')
        for key in ('max_steps','queue_capacity','metrics_capacity'):integer(getattr(self,key),key,minimum=1)
        integer(self.low_watermark,'low_watermark')
        if self.low_watermark>=self.queue_capacity:raise ConfigurationError('low_watermark must be below queue capacity')
        for key in ('max_duration_s','max_observation_age_s','inference_timeout_s','stop_timeout_s'):
            finite(getattr(self,key),key,positive=True)
        if self.fps is not None:finite(self.fps,'fps',positive=True)


class RunState(str,Enum):
    STARTING='STARTING';RUNNING='RUNNING';STOPPING='STOPPING'
    STOPPED='STOPPED';SUCCEEDED='SUCCEEDED';FAULT='FAULT'


@dataclass(frozen=True)
class RunStatus:
    run_id: str
    state: RunState
    steps: int
    queue_size: int
    inference_pending: bool
    holding: bool
    rejected_actions: int
    queue_underflows: int
    period_p50_ms: float
    period_p95_ms: float
    period_p99_ms: float
    error: str | None


@dataclass(frozen=True)
class RunResult:
    run_id: str
    state: RunState
    steps: int
    reason: str
    holding: bool
    inference_pending_at_stop: bool
