"""Spark managed pi05 acceptance using authorized local tokenizer and original Seeed code."""
import argparse,json,time
from pathlib import Path
from dataclasses import asdict,replace
from lerobot_sdk import Trainer,TrainConfig,DatasetManager,ModelManager,OperationCancelled
from lerobot_sdk.adapters.pi05_training import prepare_pi05_base
from lerobot_sdk._dataset_fs import atomic_json,read_json
from lerobot_sdk.models import inspect_checkpoint

HASH='sha256:5646297f89d76d2505bec177be78a53af232eb3a05a2e204597e95ee862b3804'
WEIGHTS='sha256:0eb11ca9587678c1d2ef8cf32807c29f8ce53a2bfdfc1aa4a4c96f16fca59b0f'


def main():
    p=argparse.ArgumentParser();p.add_argument('--phase',choices=('prepare','train','resume','load'),required=True)
    p.add_argument('--dataset',type=Path,default=Path('/data'));p.add_argument('--output',type=Path,default=Path('/output'))
    p.add_argument('--base',type=Path,default=Path('/base'));p.add_argument('--source',type=Path,default=Path('/raw-base'))
    p.add_argument('--tokenizer',type=Path,default=Path('/tokenizer'));p.add_argument('--checkpoint',type=Path,default=Path('/checkpoint'))
    a=p.parse_args();a.output.mkdir(exist_ok=True,parents=True)
    report_path=a.output/(a.phase+'-report.json');assert not report_path.exists(),'Never overwrite acceptance result'
    source=DatasetManager().inspect(a.dataset).ref;assert source.content_hash==HASH
    if a.phase=='prepare':
        model=prepare_pi05_base(a.source,a.dataset,a.output/'bound-base',a.tokenizer,expected_weights_hash=WEIGHTS)
        result={'phase':'prepare','passed':True,'model_dir':str(model.model_dir),'model_hash':model.content_hash,'training_performed':False}
    elif a.phase in ('train','resume'):
        trainer=Trainer(a.output/'jobs')
        cfg=TrainConfig(dataset_dir=a.dataset,base_model_dir=a.base,policy_type='pi05',device='cuda:0',
            steps=6 if a.phase=='train' else inspect_checkpoint(a.checkpoint)['step']+1,
            batch_size=1,save_freq=1,seed=42,output_dir=a.output/'attempt',
            policy_params={'train_expert_only':True,'freeze_vision_encoder':True,'gradient_checkpointing':True},
            scheduler_params={'type':'constant_with_warmup','num_warmup_steps':1})
        job=trainer.start(cfg) if a.phase=='train' else trainer.resume(a.checkpoint,cfg)
        atomic_json(a.output/'job-identity.json',{'job_id':job.id,'phase':a.phase,'output':str(job.output_dir)})
        deadline=time.monotonic()+1800
        try:
            while job.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):
                status=job.status()
                if a.phase=='train' and status.step>=2 and status.checkpoint_dir is not None:
                    job.cancel(timeout_s=180);break
                if time.monotonic()>deadline:raise TimeoutError('Training acceptance deadline exceeded')
                time.sleep(.1)
            status=job.status();checkpoint=status.checkpoint_dir
            if a.phase=='train':
                assert status.state.value=='CANCELLED','Cancellation was not exercised before completion'
                try:job.wait(1)
                except OperationCancelled:pass
                else:raise AssertionError('Cancelled job wait did not raise')
            else:job.wait(10)
            saved=inspect_checkpoint(checkpoint)
            assert saved['runtime']['execution_device']['cuda_used']
            assert saved['runtime']['parameter_update']['changed'],'No measured trainable parameter update'
            audit=read_json(job.output_dir/'resume-audit.json') if a.phase=='resume' else None
            if audit:assert all(audit[k] is True for k in ('rng_restored','optimizer_restored','optimizer_groups_restored','scheduler_restored','model_weights_restored'))
            result={'phase':a.phase,'passed':True,'state':status.state.value,'step':status.step,
                    'checkpoint_dir':str(checkpoint),'checkpoint_hash':saved['content_hash'],'resume_audit':audit,
                    'parameter_update':saved['runtime']['parameter_update']}
        finally:
            if job.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):job.cancel(timeout_s=60,force=True)
    else:
        import torch
        from lerobot_sdk.adapters.seeed_dataset import upstream
        model_dir=a.checkpoint/'pretrained_model';models=ModelManager();manifest=models.inspect(model_dir)
        dataset_cls,*_=upstream();dataset=dataset_cls(repo_id='local/'+source.dataset_id,root=a.dataset,video_backend='pyav')
        sample=dataset[0];task=sample['task'];assert isinstance(task,str) and task.strip()
        observation={k:v for k,v in sample.items() if k=='observation.state' or k.startswith('observation.images.')}
        observation['task']=task
        # Adapter-level golden check uses the same saved native processor and model.
        with models.load(model_dir,device='cuda:0') as handle,torch.inference_mode():
            handle._policy.reset();handle._pre.reset();handle._post.reset()
            batch=handle._pre(observation)
            chunk=handle._post(handle._policy.predict_action_chunk(batch))
            handle._policy.reset();step=handle._post(handle._policy.select_action(batch))
            assert chunk.shape[-1]==6 and tuple(step.shape)==(1,6)
            assert torch.isfinite(chunk).all() and torch.isfinite(step).all()
            result={'phase':'load','passed':True,'model_hash':manifest.content_hash,'warmup':asdict(handle.load_report),
                    'task':task,'step_shape':list(step.shape),'chunk_shape':list(chunk.shape),
                    'finite':True,'actual_step':step.cpu().tolist(),'sample_index':0,'scope':'model outputs only; no robot action'}
    assert DatasetManager().inspect(source).ref==source
    result['dataset_hash']=HASH;atomic_json(report_path,result);print(json.dumps(result))


if __name__=='__main__':main()
