"""Managed process entry point. Delegates training/resume algorithms to Seeed."""
from pathlib import Path
import argparse
import hashlib
import json
import math
import os
import platform
import sys
import traceback
from lerobot_sdk.data.filesystem import atomic_json,read_json,remove_owned_tree
from lerobot_sdk.learning.contracts import TrainConfig
from lerobot_sdk.data.manager import DatasetManager
from lerobot_sdk.policies.models import ModelManager,seal_model,seal_checkpoint,inspect_checkpoint
from lerobot_sdk.adapters.seeed_training import build_native,PolicyMetadataView


class CancelledAtCheckpoint(Exception):pass


def run(attempt):
    attempt=Path(attempt).resolve();request=read_json(attempt/'request.json')
    config=TrainConfig(**request['config']);job_id=request['job_id']
    atomic_json(attempt/'worker.json',{'pid':os.getpid(),'job_id':job_id})
    progress={'step':0,'checkpoint':None,'event':0}

    def event(kind,**detail):
        progress['event']+=1
        with (attempt/'events.jsonl').open('a',encoding='utf-8') as stream:
            stream.write(json.dumps({'sequence':progress['event'],'kind':kind,'detail':detail},allow_nan=False)+'\n')
            stream.flush()
        atomic_json(attempt/'progress.json',{'step':progress['step'],'checkpoint_dir':progress['checkpoint'],'state':'RUNNING'})

    def cancel_requested():return (attempt/'cancel.request').exists()
    resumed=Path(request['checkpoint']) if request.get('checkpoint') else None
    audit={}
    parameter_audit=None
    trained_policy=None
    try:
        if cancel_requested():raise CancelledAtCheckpoint()
        import torch
        from lerobot_sdk.learning.runtime import configure_runtime,verify_runtime_options
        runtime_policy=configure_runtime(torch,config.device)
        if resumed:verify_runtime_options(inspect_checkpoint(resumed).get('runtime',{}).get('worker_policy'),runtime_policy)
        execution_device={}
        torch.set_num_threads(config.cpu_threads)
        torch.set_num_interop_threads(1)
        import lerobot.scripts.lerobot_train as native
        from lerobot.utils.random_utils import save_rng_state
        from lerobot.optim import save_optimizer_state,save_scheduler_state
        from safetensors.torch import load_file
        ref=DatasetManager().inspect(config.dataset_dir).ref
        if ref.content_hash!=request['dataset_hash']:raise ValueError('Dataset changed after configuration validation')
        dataset_manifest=read_json(ref.root/'manifest.json')
        from lerobot_sdk.learning.environment import current_training_environment,verify_environment
        environment,image=current_training_environment()
        if request.get('environment_hash'):
            verify_environment(attempt/'environment.json',environment)
            if environment['content_hash']!=request['environment_hash']:
                raise ValueError('Training environment changed between parent validation and worker start')
        if image!=request.get('image_provenance'):
            raise ValueError('Image provenance changed between parent and worker')
        if resumed:inspect_checkpoint(resumed)
        base_hash=request.get('base_model_hash')
        if base_hash and ModelManager().inspect(config.base_model_dir).content_hash!=base_hash:
            raise ValueError('Base model changed after validation')
        cfg=build_native(config,dataset_manifest,attempt/'native',resumed)
        if resumed:
            # The native validator intentionally reads config_path from argv.
            sys.argv=['lerobot-sdk-worker','--config_path='+str(resumed/'pretrained_model/train_config.json')]
        else:sys.argv=['lerobot-sdk-worker']
        originals={name:getattr(native,name) for name in ('make_policy','update_policy','save_checkpoint',
                   'update_last_checkpoint','resume_before_prepare','resume_after_prepare','make_dataloaders','make_pre_post_processors')}

        def dataloaders(*args,**kwargs):
            loaders=originals['make_dataloaders'](*args,**kwargs)
            # A new DataLoader iterator otherwise consumes global Torch RNG for
            # its base seed even with num_workers=0, perturbing resumed ACT VAE/
            # dropout draws. Keep that bookkeeping RNG separate from model RNG.
            for loader in loaders:
                if loader is not None:loader.generator=torch.Generator().manual_seed(config.seed)
            return loaders

        def policy_factory(*args,**kwargs):
            nonlocal parameter_audit,trained_policy
            if config.policy_type=='pi05':
                from lerobot_sdk.adapters.pi05_training import load_training_policy,TrainableUpdateAudit
                if kwargs.get('defer_weight_load'):raise ValueError('Managed pi05 does not support sharded/deferred weight loading')
                policy_config=kwargs.get('cfg') if 'cfg' in kwargs else args[0]
                trained_policy=load_training_policy(policy_config)
                parameter_audit=TrainableUpdateAudit(trained_policy)
                return trained_policy
            kwargs['ds_meta']=PolicyMetadataView(kwargs['ds_meta'])
            return originals['make_policy'](*args,**kwargs)

        def processor_factory(*args,**kwargs):
            if config.policy_type=='pi05':
                from lerobot_sdk.adapters.pi05_training import load_training_processors
                return load_training_processors(originals['make_pre_post_processors'],**kwargs)
            return originals['make_pre_post_processors'](*args,**kwargs)

        def compare_tensors(a,b):
            left=load_file(str(a));right=load_file(str(b))
            return left.keys()==right.keys() and all(torch.equal(left[k],right[k]) for k in left)

        def before_resume(native_config):
            step=originals['resume_before_prepare'](native_config)
            audit_dir=attempt/'resume-audit';audit_dir.mkdir(exist_ok=True)
            save_rng_state(audit_dir)
            audit.update(loaded_step=step,rng_restored=compare_tensors(audit_dir/'rng_state.safetensors',resumed/'training_state/rng_state.safetensors'))
            if not audit['rng_restored']:raise ValueError('Native RNG restore verification failed')
            progress['step']=step;progress['checkpoint']=str(resumed)
            return step

        def after_resume(native_config,accelerator,policy,optimizer,scheduler):
            originals['resume_after_prepare'](native_config,accelerator,policy,optimizer,scheduler)
            audit_dir=attempt/'resume-audit'
            save_optimizer_state(optimizer,audit_dir)
            audit['optimizer_restored']=compare_tensors(audit_dir/'optimizer_state.safetensors',resumed/'training_state/optimizer_state.safetensors')
            audit['optimizer_groups_restored']=read_json(audit_dir/'optimizer_param_groups.json')==read_json(resumed/'training_state/optimizer_param_groups.json')
            if scheduler is not None:
                save_scheduler_state(scheduler,audit_dir)
                audit['scheduler_restored']=read_json(audit_dir/'scheduler_state.json')==read_json(resumed/'training_state/scheduler_state.json')
            else:audit['scheduler_restored']='not configured'
            expected=load_file(str(resumed/'pretrained_model/model.safetensors'))
            actual=accelerator.unwrap_model(policy).state_dict()
            audit['model_weights_restored']=expected.keys()==actual.keys() and all(torch.equal(expected[k],actual[k].detach().cpu()) for k in expected)
            if any(value is False for value in audit.values()):raise ValueError('Native full-state resume verification failed')
            atomic_json(attempt/'resume-audit.json',audit)
            event('resume_restored',**audit)
            remove_owned_tree(audit_dir,attempt)

        def update(*args,**kwargs):
            if cancel_requested() and (progress['step']==0 or progress['checkpoint'] and read_json(Path(progress['checkpoint'])/'manifest.json')['step']==progress['step']):
                raise CancelledAtCheckpoint()
            if not execution_device and os.environ.get('SO101_TRAINING_AUDIT_FIRST_STEP')=='1':
                policy=kwargs.get('policy') if 'policy' in kwargs else args[1]
                batch=kwargs.get('batch') if 'batch' in kwargs else args[2]
                def fingerprint(mapping):
                    digest=hashlib.sha256()
                    for key,value in sorted(mapping.items()):
                        if torch.is_tensor(value):
                            digest.update(json.dumps([key,list(value.shape),str(value.dtype)]).encode())
                            digest.update(value.detach().cpu().contiguous().reshape(-1).view(torch.uint8).numpy().tobytes())
                    return digest.hexdigest()
                event('first_step_audit',model_hash=fingerprint(policy.state_dict()),batch_hash=fingerprint(batch),
                      cpu_rng_hash=hashlib.sha256(torch.get_rng_state().numpy().tobytes()).hexdigest(),
                      cuda_rng_hash=hashlib.sha256(torch.cuda.get_rng_state().numpy().tobytes()).hexdigest() if config.device.startswith('cuda') else None,
                      camera_order=list(policy.config.image_features),cudnn_deterministic=torch.backends.cudnn.deterministic,
                      cudnn_benchmark=torch.backends.cudnn.benchmark,matmul_allow_tf32=torch.backends.cuda.matmul.allow_tf32)
            result=originals['update_policy'](*args,**kwargs)
            if not execution_device:
                policy=kwargs.get('policy') if 'policy' in kwargs else args[1]
                devices=sorted({str(p.device) for p in policy.parameters()})
                if config.device.startswith('cuda') and (not devices or any(not d.startswith('cuda:') for d in devices)):
                    raise ValueError('CUDA requested but actual policy parameters are not on CUDA')
                execution_device.update(parameter_devices=devices,cuda_used=any(d.startswith('cuda:') for d in devices))
                if execution_device['cuda_used']:
                    torch.cuda.synchronize()
                    execution_device.update(gpu_name=torch.cuda.get_device_name(),peak_allocated_bytes=torch.cuda.max_memory_allocated(),
                                            peak_reserved_bytes=torch.cuda.max_memory_reserved())
                event('execution_device_verified',**execution_device)
            progress['step']+=1
            metrics=result[0].to_dict()
            loss=float(metrics['loss'])
            if not math.isfinite(loss):raise ValueError('Non-finite native loss')
            event('training_step',step=progress['step'],loss=loss,lr=float(metrics['lr']))
            return result

        def save(*args,**kwargs):
            originals['save_checkpoint'](*args,**kwargs)
            directory=Path(kwargs['checkpoint_dir']);step=kwargs['step']
            if config.policy_type=='pi05':
                from lerobot_sdk.adapters.pi05_training import bundle_tokenizer
                bundle_tokenizer(cfg.policy.pretrained_path,directory/'pretrained_model')
            runtime={'python':platform.python_version(),'platform':platform.platform(),'torch':torch.__version__,
                     'cuda':torch.version.cuda,'device':config.device,'cpu_threads':config.cpu_threads,
                     'environment_hash':environment['content_hash'],'environment':environment,
                     'image_provenance':image,'worker_policy':runtime_policy,'execution_device':dict(execution_device)}
            if parameter_audit is not None:
                change=parameter_audit.result(trained_policy)
                runtime['parameter_update']=change
                event('parameter_update_verified',**change)
            seal_model(directory/'pretrained_model',dataset_manifest,{'step':step,'job_id':job_id,'seed':config.seed,'runtime':runtime,
                       'base_model_hash':base_hash,'resumed_from':str(resumed) if resumed else None,
                       'resume_checkpoint_hash':read_json(resumed/'manifest.json')['content_hash'] if resumed else None})
            seal_checkpoint(directory,{'step':step,'dataset_hash':ref.content_hash,'job_id':job_id,
                            'scheduler_present':kwargs.get('scheduler') is not None,'train_spec':config.to_dict(),'runtime':runtime,
                            'loader_rng':'isolated-generator-v1'})
            progress['checkpoint']=str(directory)
            event('checkpoint_ready',step=step,checkpoint_dir=str(directory))
            if cancel_requested():raise CancelledAtCheckpoint()

        def latest(directory):
            try:originals['update_last_checkpoint'](directory)
            except OSError as exc:
                if os.name!='nt' or getattr(exc,'winerror',None)!=1314:raise
                atomic_json(Path(directory).parent/'last.json',{'checkpoint':Path(directory).name})
                event('checkpoint_pointer_fallback',reason='Windows symlink privilege unavailable; explicit checkpoint path remains authoritative')

        native.make_policy=policy_factory;native.update_policy=update;native.save_checkpoint=save
        native.make_pre_post_processors=processor_factory
        native.make_dataloaders=dataloaders
        native.update_last_checkpoint=latest
        native.resume_before_prepare=before_resume;native.resume_after_prepare=after_resume
        if cancel_requested():raise CancelledAtCheckpoint()
        event('training_started',resumed_from=str(resumed) if resumed else None)
        native.train(cfg)
        if progress['step']!=config.steps or not progress['checkpoint']:raise ValueError('Native training did not reach a complete final checkpoint')
        inspect_checkpoint(progress['checkpoint'])
        if DatasetManager().inspect(ref).ref.content_hash!=request['dataset_hash']:raise ValueError('Source dataset changed during training')
        if base_hash and ModelManager().inspect(config.base_model_dir).content_hash!=base_hash:
            raise ValueError('Base model changed during training')
        result={'state':'SUCCEEDED','step':progress['step'],'checkpoint_dir':progress['checkpoint'],
                'model_dir':str(Path(progress['checkpoint'])/'pretrained_model'),'dataset_hash':ref.content_hash,'job_id':job_id,
                'output_dir':str(attempt),'resumed_from':str(resumed) if resumed else None}
        atomic_json(attempt/'result.json',result)
        return 0
    except CancelledAtCheckpoint:
        # Only sealed checkpoints are offered for resume, never partial files.
        if progress['checkpoint']:inspect_checkpoint(progress['checkpoint'])
        event('cancelled',checkpoint_dir=progress['checkpoint'])
        atomic_json(attempt/'result.json',{'state':'CANCELLED','step':progress['step'],'checkpoint_dir':progress['checkpoint'],'job_id':job_id})
        return 0
    except BaseException as exc:
        traceback.print_exc()
        atomic_json(attempt/'result.json',{'state':'FAILED','step':progress['step'],'checkpoint_dir':progress['checkpoint'],
                    'error':f'{type(exc).__name__}: {exc}','job_id':job_id})
        return 1


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--attempt',required=True)
    raise SystemExit(run(parser.parse_args().attempt))
