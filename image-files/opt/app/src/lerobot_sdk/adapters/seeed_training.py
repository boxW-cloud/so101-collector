"""Configuration and metadata adapters over the fixed Seeed package, not training algorithms."""
from pathlib import Path
import importlib.util
import json
from lerobot_sdk.core.errors import ConfigurationError, ContractMismatch
from lerobot_sdk.learning.contracts import plain
from lerobot_sdk.adapters.seeed_dataset import upstream

# These are local, tested training adapters, not every model installed upstream.
SUPPORTED_TRAINING_POLICIES = ('act','pi05')


def training_available(policy_type='act'):
    if policy_type not in SUPPORTED_TRAINING_POLICIES:
        raise ConfigurationError(f'No managed training adapter for {policy_type}')
    upstream()
    if importlib.util.find_spec('accelerate') is None:raise ConfigurationError('Install the training extra')
    # Presence alone misses a partially installed accelerate (e.g. absent
    # psutil). Import the entry dependency before advertising training support.
    from accelerate import Accelerator  # noqa: F401
    from lerobot.policies.factory import get_policy_class
    from lerobot.configs.policies import PreTrainedConfig
    cls=get_policy_class(policy_type)
    if policy_type=='pi05':
        from transformers import AutoTokenizer
    PreTrainedConfig.get_choice_class(policy_type)
    return cls


def discover_policies():
    """Only expose installed AND supported policies; validation retains errors."""
    from lerobot_sdk.learning.contracts import PolicyInfo
    from lerobot_sdk.core.errors import SDKError
    result=[]
    for name in SUPPORTED_TRAINING_POLICIES:
        try:training_available(name)
        except (SDKError, ImportError):continue
        result.append(PolicyInfo(name,True,True))
    return tuple(result)


def validate_policy_features(policy, dataset_manifest, dataset_dir):
    """Bind config features before a worker exists; no model/weights allocation."""
    from lerobot_sdk.core.types import JOINT_NAMES
    from lerobot.utils.feature_utils import dataset_to_policy_features
    from lerobot.configs.types import FeatureType
    info=json.loads((Path(dataset_dir)/'meta/info.json').read_text(encoding='utf-8'))
    features=info['features']
    names=list(JOINT_NAMES)
    for key in ('observation.state','action'):
        ft=features.get(key,{})
        if ft.get('dtype')!='float32' or list(ft.get('shape',[]))!=[6] or ft.get('names')!=names:
            raise ContractMismatch(f'{key} requires canonical float32[6] named joints')
    cameras=dataset_manifest['camera_shapes']
    if not cameras:raise ConfigurationError('ACT SO101 training requires at least one RGB camera')
    expected={'observation.state','action',*('observation.images.'+k for k in cameras)}
    actual={'observation.state','action',*(k for k in features if k.startswith('observation.images.'))}
    if actual!=expected:raise ContractMismatch('Dataset camera features and manifest differ')
    shapes=set()
    for name,shape in cameras.items():
        key='observation.images.'+name;ft=features[key]
        if list(ft['shape'])!=list(shape) or ft.get('names')!=['height','width','channels'] or len(shape)!=3 or shape[-1]!=3:
            raise ContractMismatch(f'{key} must match the declared HWC RGB camera shape')
        shapes.add(tuple(shape))
    if policy.type=='act' and len(shapes)>1:raise ConfigurationError('Native ACT requires all camera shapes to match')
    # A set is valid for membership validation, never for camera/token ordering.
    parsed=dataset_to_policy_features({k:v for k,v in features.items() if k in expected})
    inputs={k:v for k,v in parsed.items() if v.type is not FeatureType.ACTION}
    outputs={'action':parsed['action']}
    for attr,value in (('input_features',inputs),('output_features',outputs)):
        previous=getattr(policy,attr)
        if previous and (set(previous)!=set(value) or any(previous[k].type!=v.type or tuple(previous[k].shape)!=tuple(v.shape) for k,v in value.items())):
            raise ContractMismatch(f'Saved policy {attr} differ from the dataset')
        # ACT concatenates images in input_features insertion order. Preserve a
        # loaded policy's order after validating it, especially on full resume.
        setattr(policy,attr,dict(previous) if previous else value)
    policy.validate_features()


def validate_act_parameters(policy, steps):
    """Policy-specific supplement to the native ACT registered configuration."""
    from lerobot_sdk.core.types import finite,integer
    from lerobot.configs.types import NormalizationMode
    from torchvision.models import get_model_builder
    if policy.type!='act':raise ConfigurationError('Managed adapter/config policy type mismatch')
    for name in ('n_obs_steps','n_heads','dim_model','chunk_size','n_action_steps','n_encoder_layers','n_decoder_layers','n_vae_encoder_layers','dim_feedforward','latent_dim'):
        integer(getattr(policy,name),name,minimum=1)
    if policy.dim_model%policy.n_heads:raise ConfigurationError('ACT dim_model must be divisible by n_heads')
    if policy.dim_model%4:raise ConfigurationError('ACT image positional encoding requires dim_model divisible by 4')
    if not 0<=finite(policy.dropout,'ACT dropout')<1:raise ConfigurationError('ACT dropout must be in [0,1)')
    for name in ('optimizer_lr','optimizer_lr_backbone'):finite(getattr(policy,name),name,positive=True)
    for name in ('optimizer_weight_decay','kl_weight'):finite(getattr(policy,name),name,nonnegative=True)
    if policy.temporal_ensemble_coeff is not None:finite(policy.temporal_ensemble_coeff,'temporal_ensemble_coeff',nonnegative=True)
    # GLU halves the intermediate width but native ACT linear2 expects the full
    # width. Reject this native-but-incompatible option before allocating a model.
    if policy.feedforward_activation not in ('relu','gelu'):
        raise ConfigurationError('Managed ACT supports relu/gelu feedforward activation')
    try:get_model_builder(policy.vision_backbone)
    except ValueError as exc:raise ConfigurationError('Unknown torchvision ACT backbone') from exc
    if policy.replace_final_stride_with_dilation and policy.vision_backbone in ('resnet18','resnet34'):
        raise ConfigurationError('BasicBlock resnet18/resnet34 do not support dilated final stride')
    if not {'STATE','VISUAL','ACTION'}<=set(policy.normalization_mapping):
        raise ConfigurationError('ACT normalization modes must include STATE/VISUAL/ACTION')
    for value in policy.normalization_mapping.values():
        try:NormalizationMode(value)
        except ValueError as exc:raise ConfigurationError('Unknown native normalization mode') from exc


def validate_device(device):
    import re
    if not isinstance(device,str) or not re.fullmatch(r'cpu|cuda(?::[0-9]+)?',device):
        raise ConfigurationError('Device must be cpu or an explicit cuda device')
    import torch
    if device.startswith('cuda'):
        if not torch.cuda.is_available():raise ConfigurationError('Requested CUDA is unavailable; no silent CPU fallback')
        index=int(device.split(':')[1]) if ':' in device else 0
        if index>=torch.cuda.device_count():raise ConfigurationError('CUDA device index is unavailable')


class PolicyMetadataView:
    """Policy features exclude diagnostic action columns; source dataset is never edited."""
    def __init__(self,metadata):self.metadata=metadata
    def __getattr__(self,name):return getattr(self.metadata,name)
    @property
    def features(self):
        return {k:v for k,v in self.metadata.features.items() if k in ('action','observation.state') or k.startswith('observation.images.')}


def build_native(config,dataset_manifest,output_dir,checkpoint=None):
    training_available(config.policy_type);validate_device(config.device)
    import draccus
    from lerobot.configs.train import TrainPipelineConfig
    from lerobot.configs.default import DatasetConfig
    from lerobot.policies.factory import make_policy_config
    from lerobot.configs.policies import PreTrainedConfig
    from lerobot.optim import LRSchedulerConfig
    if checkpoint is not None:
        cfg=TrainPipelineConfig.from_pretrained(Path(checkpoint)/'pretrained_model',local_files_only=True)
        cfg.resume=True
        cfg.checkpoint_path=Path(checkpoint)
        cfg.policy.pretrained_path=Path(checkpoint)/'pretrained_model'
        cfg.policy.device=config.device
    else:
        if config.base_model_dir is not None:
            from lerobot_sdk.policies.models import ModelManager
            base=ModelManager().inspect(config.base_model_dir)
            if base.policy_type!=config.policy_type:raise ContractMismatch('Base model policy type differs from requested adapter')
            for key in ('joint_names','units','camera_shapes'):
                if plain(base.contract[key])!=plain(dataset_manifest[key]):raise ContractMismatch('Base model and dataset contract differ')
            if config.policy_params and config.policy_type=='act':raise ConfigurationError('Base model architecture cannot be overridden; use its saved config')
            policy=PreTrainedConfig.from_pretrained(config.base_model_dir,local_files_only=True)
            policy.device=config.device;policy.pretrained_path=config.base_model_dir
            if config.policy_type=='pi05':
                from lerobot_sdk.adapters.pi05_training import apply_training_overrides
                apply_training_overrides(policy,plain(config.policy_params))
        else:
            params=plain(config.policy_params)
            params.setdefault('pretrained_backbone_weights',None)  # explicitly local, no implicit ImageNet download
            if params['pretrained_backbone_weights'] is not None:
                raise ConfigurationError('Use a local base_model_dir; implicit backbone downloads are disabled')
            try:policy=make_policy_config(config.policy_type,device=config.device,push_to_hub=False,**params)
            except (TypeError,ValueError,NotImplementedError) as exc:raise ConfigurationError(str(exc)) from exc
        policy.push_to_hub=False
        scheduler=None
        if config.scheduler_params is not None:
            try:scheduler=draccus.decode(LRSchedulerConfig,plain(config.scheduler_params))
            except Exception as exc:raise ConfigurationError(f'Invalid native scheduler config: {exc}') from exc
        cfg=TrainPipelineConfig(dataset=DatasetConfig(repo_id='local/'+dataset_manifest['dataset_id'],root=str(config.dataset_dir),video_backend='pyav'),
            policy=policy,use_policy_training_preset=scheduler is None,
            optimizer=policy.get_optimizer_preset(),scheduler=scheduler)
    cfg.output_dir=Path(output_dir)
    cfg.steps=config.steps;cfg.batch_size=config.batch_size;cfg.seed=config.seed
    cfg.num_workers=0;cfg.persistent_workers=False;cfg.env_eval_freq=0;cfg.eval_steps=0
    cfg.log_freq=1;cfg.save_freq=config.save_freq;cfg.save_checkpoint=True
    cfg.dataset.root=str(config.dataset_dir);cfg.dataset.video_backend='pyav'
    cfg.wandb.enable=False;cfg.policy.push_to_hub=False;cfg.save_checkpoint_to_hub=False
    from lerobot_sdk.learning.runtime import configure_native_runtime
    configure_native_runtime(cfg,config.device)
    if config.policy_type=='pi05':
        from lerobot_sdk.adapters.pi05_training import validate_pi05_parameters,validate_tokenizer,validate_pi05_statistics
        validate_pi05_parameters(cfg.policy)
        validate_tokenizer(Path(cfg.policy.pretrained_path)/'tokenizer')
        validate_pi05_statistics(cfg.policy,config.dataset_dir)
    else:validate_act_parameters(cfg.policy,config.steps)
    validate_policy_features(cfg.policy,dataset_manifest,config.dataset_dir)
    if cfg.scheduler is not None:
        warmup=cfg.scheduler.num_warmup_steps
        if warmup is not None:
            from lerobot_sdk.core.types import integer
            integer(warmup,'scheduler num_warmup_steps')
    # Build just the native optimizer/scheduler on one scalar to validate their
    # own parameters and optional dependencies, not the training model.
    import torch
    try:
        optimizer=cfg.optimizer.build([torch.nn.Parameter(torch.zeros(()))])
        if cfg.scheduler is not None:cfg.scheduler.build(optimizer,num_training_steps=config.steps)
    except Exception as exc:raise ConfigurationError(f'Invalid native optimizer/scheduler: {exc}') from exc
    return cfg
