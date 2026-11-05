"""Optional per-worker CUDA allocator cap and reproducibility policy."""
import math
import os
from lerobot_sdk.core.errors import ConfigurationError, ContractMismatch


def runtime_options(device,environ=None):
    env=os.environ if environ is None else environ
    deterministic=env.get('SO101_TRAINING_DETERMINISTIC','0')
    if deterministic not in ('0','1'):raise ConfigurationError('SO101_TRAINING_DETERMINISTIC must be 0 or 1')
    value=env.get('SO101_CUDA_MEMORY_FRACTION')
    try:fraction=float(value) if value is not None else None
    except ValueError as exc:raise ConfigurationError('Invalid CUDA memory fraction') from exc
    if fraction is not None and (not math.isfinite(fraction) or not 0<fraction<=1):
        raise ConfigurationError('CUDA memory fraction must be in (0,1]')
    if fraction is not None and not device.startswith('cuda'):raise ConfigurationError('CUDA cap was set for a CPU job')
    return {'deterministic':deterministic=='1','cuda_memory_fraction':fraction}


def configure_runtime(torch,device,environ=None):
    options=runtime_options(device,environ)
    if device.startswith('cuda'):
        if not torch.cuda.is_available():raise ConfigurationError('CUDA training requested but unavailable; no CPU fallback')
        target=torch.device('cuda:0' if device=='cuda' else device);torch.cuda.set_device(target)
        if options['cuda_memory_fraction'] is not None:
            torch.cuda.set_per_process_memory_fraction(options['cuda_memory_fraction'],target)
    if options['deterministic']:
        torch.use_deterministic_algorithms(True)
        torch.backends.cudnn.benchmark=False
        torch.backends.cudnn.deterministic=True
    return options


def verify_runtime_options(previous,current):
    if previous is not None and previous!=current:raise ContractMismatch('Full resume requires the same worker runtime policy')


def configure_native_runtime(config,device,environ=None):
    # Native train() sets cudnn.benchmark=True unless this config flag is true.
    # Set the native flag, not only the process global that it overwrites.
    config.cudnn_deterministic=runtime_options(device,environ)['deterministic']
