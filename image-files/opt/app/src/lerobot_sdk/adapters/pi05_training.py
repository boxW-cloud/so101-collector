"""SO101 pi05 training adaptation over the pinned Seeed package, no model algorithms."""
from pathlib import Path
from functools import partial
import json
import shutil
import threading
import uuid
from lerobot_sdk.data.filesystem import read_json,sha_file,inventory,digest,atomic_json,disk_check,remove_owned_tree,publish_directory
from lerobot_sdk.core.errors import ConfigurationError,ContractMismatch
from lerobot_sdk.core.types import integer,finite,calibration,JOINT_NAMES
from lerobot_sdk.core.locking import DeviceLock

_PROCESSOR_LOCK=threading.Lock()
TRAINING_OVERRIDES={'freeze_vision_encoder','train_expert_only','gradient_checkpointing',
    'optimizer_lr','optimizer_betas','optimizer_eps','optimizer_weight_decay','optimizer_grad_clip_norm',
    'scheduler_warmup_steps','scheduler_decay_steps','scheduler_decay_lr'}


class TrainableUpdateAudit:
    """Record an actual trainable tensor before/after updates, not loss alone."""
    def __init__(self,policy):
        parameters={name:value for name,value in policy.named_parameters() if value.requires_grad}
        if not parameters:raise ConfigurationError('Training policy has no trainable parameters')
        self.count=sum(value.numel() for value in parameters.values())
        self.name=next((name for name in parameters if name.endswith('action_out_proj.weight')),next(iter(parameters)))
        self.before=parameters[self.name].detach().cpu().clone()

    def result(self,policy):
        import torch
        after=dict(policy.named_parameters())[self.name].detach().cpu()
        return {'parameter_name':self.name,'trainable_parameter_count':self.count,
                'changed':not torch.equal(self.before,after),
                'max_abs_delta':float((after.float()-self.before.float()).abs().max())}


def apply_training_overrides(policy,values):
    if set(values)-TRAINING_OVERRIDES:raise ConfigurationError('pi05 base overrides are limited to optimizer/scheduler and freeze/checkpointing settings')
    for key,value in values.items():setattr(policy,key,value)
    validate_pi05_parameters(policy)


def validate_pi05_parameters(policy):
    if policy.type!='pi05':raise ConfigurationError('Expected pi05 configuration')
    for key in ('chunk_size','n_action_steps','num_inference_steps','tokenizer_max_length','n_obs_steps'):
        integer(getattr(policy,key),key,minimum=1)
    if policy.n_obs_steps!=1 or policy.n_action_steps>policy.chunk_size:
        raise ConfigurationError('Managed pi05 requires one observation and n_action_steps <= chunk_size')
    if policy.max_state_dim!=32 or policy.max_action_dim!=32:
        raise ConfigurationError('Keep pretrained internal padding at 32; external SO101 features are six-dimensional')
    if policy.dtype not in ('float32','bfloat16'):raise ConfigurationError('Unsupported pi05 dtype')
    for key in ('freeze_vision_encoder','train_expert_only','gradient_checkpointing','compile_model','use_relative_actions','use_visual_memory','use_proprioceptive_memory'):
        if type(getattr(policy,key)) is not bool:raise ConfigurationError(f'{key} must be boolean')
    if (policy.compile_model or policy.use_visual_memory or policy.use_proprioceptive_memory or policy.use_relative_actions
        or policy.rtc_training_max_delay or policy.empty_cameras or (policy.rtc_config and policy.rtc_config.enabled)):
        raise ConfigurationError('Managed pi05 training currently supports absolute actions, real cameras, no MEM/RTC/compile')
    if len(policy.image_resolution)!=2:raise ConfigurationError('Invalid image resolution')
    for size in policy.image_resolution:integer(size,'image resolution',minimum=1)
    for key in ('optimizer_lr','optimizer_eps','optimizer_grad_clip_norm','time_sampling_beta_alpha','time_sampling_beta_beta','min_period','max_period'):
        finite(getattr(policy,key),key,positive=True)
    finite(policy.optimizer_weight_decay,'optimizer_weight_decay',nonnegative=True)
    if len(policy.optimizer_betas)!=2 or any(not 0<=finite(x,'optimizer beta')<1 for x in policy.optimizer_betas):
        raise ConfigurationError('Optimizer betas must be in [0,1)')
    integer(policy.scheduler_warmup_steps,'scheduler_warmup_steps')
    integer(policy.scheduler_decay_steps,'scheduler_decay_steps',minimum=1)
    finite(policy.scheduler_decay_lr,'scheduler_decay_lr',nonnegative=True)


def validate_tokenizer(directory):
    root=Path(directory).expanduser().absolute()
    if (not root.is_dir() or root.is_symlink() or not (root/'tokenizer_config.json').is_file()
        or not any((root/name).is_file() for name in ('tokenizer.json','tokenizer.model'))):
        raise ConfigurationError('Provide a complete authorized local PaliGemma tokenizer directory')
    inventory(root)  # Reject symlinked content; caller materializes authorized HF snapshots first.
    try:
        from transformers import AutoTokenizer
        tokenizer=AutoTokenizer.from_pretrained(root,local_files_only=True,trust_remote_code=False)
        sample=tokenizer('Task: move the object; Action:',add_special_tokens=True)
        ids=sample['input_ids']
        if not ids or any(type(x) is not int or x<0 or x>=257152 for x in ids):raise ValueError('Token IDs exceed pi05 embedding range')
        # PaliGemma extends Gemma with location/segmentation tokens. Its image
        # marker is index 257152; text embedding indices remain below that.
        if (tokenizer.bos_token_id!=2 or tokenizer.eos_token_id!=1 or tokenizer.vocab_size!=257152
            or tokenizer.convert_tokens_to_ids('<image>')!=257152):
            raise ValueError('Tokenizer is not compatible with the pinned PaliGemma vocabulary')
        return tokenizer
    except Exception as exc:raise ConfigurationError(f'Authorized local tokenizer failed validation: {type(exc).__name__}: {exc}') from exc


def validate_pi05_statistics(policy,dataset_dir):
    import numpy as np
    stats=read_json(Path(dataset_dir)/'meta/stats.json')
    for feature,kind in [('observation.state','STATE'),('action','ACTION')]:
        mode=policy.normalization_mapping.get(kind)
        mode=getattr(mode,'value',mode)
        fields={'QUANTILES':('q01','q99'),'MEAN_STD':('mean','std')}.get(mode)
        if fields is None:raise ConfigurationError('pi05 state/actions require explicit QUANTILES or MEAN_STD normalization')
        arrays=[]
        for key in fields:
            value=np.asarray(stats.get(feature,{}).get(key,[]),dtype=float)
            if value.shape!=(6,) or not np.isfinite(value).all():raise ContractMismatch(f'Missing finite six-axis statistics: {feature}/{key}')
            arrays.append(value)
        if mode=='QUANTILES' and np.any(arrays[1]<arrays[0]):raise ContractMismatch('Reversed quantiles')
        if mode=='MEAN_STD' and np.any(arrays[1]<0):raise ContractMismatch('Negative standard deviation')


def load_training_policy(config):
    from lerobot_sdk.adapters.policy_runtime import load_native_policy
    config.action_feature_names=list(JOINT_NAMES)
    policy=load_native_policy('pi05',config.pretrained_path,config)
    return policy.to(config.device)


def load_training_processors(native,**kwargs):
    path=Path(kwargs['pretrained_path'])/'tokenizer'
    kwargs['preprocessor_overrides']={**kwargs.get('preprocessor_overrides',{}),'tokenizer_processor':{'tokenizer_name':str(path)}}
    return native(**kwargs)


def bundle_tokenizer(source_model,target_model):
    source=Path(source_model)/'tokenizer';target=Path(target_model)/'tokenizer'
    if not source.is_dir():raise ContractMismatch('pi05 checkpoint source has no bundled tokenizer')
    if source.resolve()==target.resolve() or (Path(target_model)/'manifest.json').exists():
        raise ContractMismatch('Cannot replace tokenizer in a source or already sealed model')
    before=inventory(source)
    # Native save_checkpoint already serializes the tokenizer. Canonicalize the
    # unsealed owned output to the verified source bytes before computing its seal.
    if target.exists():
        inventory(target)
        remove_owned_tree(target,Path(target_model))
    shutil.copytree(source,target)
    if inventory(target)!=before or inventory(source)!=before:raise ContractMismatch('Tokenizer changed while saving checkpoint')


def prepare_pi05_base(source_dir,dataset_dir,target_dir,tokenizer_dir,*,expected_weights_hash,
                      chunk_size=8,n_action_steps=8,num_inference_steps=10,dtype='bfloat16'):
    """Bind public pretrained weights to an explicit six-axis dataset; no training claim.

    Core algorithms/processors are imported from Seeed. Source weights remain
    byte-identical; 32-wide internal padding stays unchanged. Features/stats come
    from the finalized data; the first actual model load remains strict.
    """
    tokenizer=validate_tokenizer(tokenizer_dir)  # Fail before allocating models/outputs.
    calibration(expected_weights_hash)
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()
    from lerobot_sdk.data.manager import DatasetManager
    from lerobot_sdk.policies.models import seal_model,ModelManager
    from lerobot_sdk.adapters.seeed_training import validate_policy_features
    from lerobot.configs.policies import PreTrainedConfig
    from lerobot.configs.train import TrainPipelineConfig
    from lerobot.configs.default import DatasetConfig
    from lerobot.policies.pi05 import processor_pi05
    source=Path(source_dir).expanduser().resolve();target=Path(target_dir).expanduser().absolute()
    if target.is_symlink():raise ContractMismatch('Linked target is forbidden')
    target=target.resolve();tokenizer_root=Path(tokenizer_dir).resolve()
    datasets=DatasetManager();ref=datasets.inspect(dataset_dir).ref;metadata=read_json(ref.root/'manifest.json')
    for origin in (source,ref.root,tokenizer_root):
        if target.is_relative_to(origin) or origin.is_relative_to(target):raise ContractMismatch('Base preparation paths overlap')
    source_files=inventory(source);tokenizer_files=inventory(tokenizer_root)
    if sha_file(source/'model.safetensors')!=expected_weights_hash:raise ContractMismatch('Pretrained weights hash differs')
    config=PreTrainedConfig.from_pretrained(source,local_files_only=True)
    if config.type!='pi05':raise ConfigurationError('Source is not pi05')
    config.input_features={};config.output_features={}
    config.empty_cameras=0;config.use_relative_actions=False;config.action_feature_names=list(JOINT_NAMES)
    config.chunk_size=chunk_size;config.n_action_steps=n_action_steps;config.num_inference_steps=num_inference_steps
    config.dtype=dtype;config.device='cpu';config.pretrained_path=None;config.push_to_hub=False
    config.train_expert_only=True;config.freeze_vision_encoder=True;config.gradient_checkpointing=True
    validate_pi05_parameters(config);validate_policy_features(config,metadata,ref.root);validate_pi05_statistics(config,ref.root)
    target.parent.mkdir(parents=True,exist_ok=True);lock=DeviceLock('model-deploy:'+str(target));lock.acquire()
    work=target.parent/('.'+target.name+'.preparing-'+uuid.uuid4().hex)
    try:
        if target.exists():raise ContractMismatch('Prepared base destination already exists')
        disk_check(target.parent,sum(v['bytes'] for v in source_files.values())+512*1024*1024)
        work.mkdir();shutil.copyfile(source/'model.safetensors',work/'model.safetensors')
        if sha_file(work/'model.safetensors')!=expected_weights_hash:raise ContractMismatch('Copied weights differ')
        shutil.copytree(tokenizer_root,work/'tokenizer')
        config.save_pretrained(work)
        pipeline=TrainPipelineConfig(dataset=DatasetConfig(repo_id='local/'+ref.dataset_id,root=str(ref.root)),policy=config)
        pipeline.save_pretrained(work)
        # The fixed Seeed factory hardcodes tokenizer_name; inject an already-authorized
        # tokenizer object only while calling that original factory. No tokenizer logic copied.
        with _PROCESSOR_LOCK:
            original=processor_pi05.TokenizerProcessorStep
            try:
                processor_pi05.TokenizerProcessorStep=partial(original,tokenizer=tokenizer)
                pre,post=processor_pi05.make_pi05_pre_post_processors(config,read_json(ref.root/'meta/stats.json'))
            finally:processor_pi05.TokenizerProcessorStep=original
        pre.save_pretrained(work,config_filename='policy_preprocessor.json')
        post.save_pretrained(work,config_filename='policy_postprocessor.json')
        provenance={'step':0,'job_id':None,'seed':None,'artifact_role':'dataset-bound-pretrained-base',
                    'pretrained_source_hash':digest(source_files),'pretrained_weights_hash':expected_weights_hash,
                    'tokenizer_hash':digest(tokenizer_files),'training_performed':False}
        atomic_json(work/'base-binding.json',{'source_features':read_json(source/'config.json').get('input_features'),
                    'bound_features':read_json(work/'config.json'),
                    'dataset_hash':ref.content_hash,**provenance})
        seal_model(work,metadata,provenance);ModelManager().inspect(work)
        if inventory(source)!=source_files or inventory(tokenizer_root)!=tokenizer_files:raise ContractMismatch('Source changed during binding')
        datasets.inspect(ref);publish_directory(work,target)
        return ModelManager().inspect(target)
    finally:
        try:remove_owned_tree(work,target.parent)
        finally:lock.release()
