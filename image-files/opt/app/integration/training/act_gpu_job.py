"""One owned CUDA ACT job. Shared data is read-only; cancellation targets this job only."""
import argparse
from dataclasses import asdict
import json
from pathlib import Path
import time
import traceback
from lerobot_sdk import Trainer,TrainConfig,DatasetManager,ModelManager,OperationCancelled
from lerobot_sdk._dataset_fs import atomic_json,read_json
from lerobot_sdk.models import inspect_checkpoint

DATA_HASH='sha256:5646297f89d76d2505bec177be78a53af232eb3a05a2e204597e95ee862b3804'
PARAMS={'pretrained_backbone_weights':None,'chunk_size':8,'n_action_steps':8,'dim_model':32,'n_heads':4,
        'dim_feedforward':64,'n_encoder_layers':1,'n_decoder_layers':1,'n_vae_encoder_layers':1,'latent_dim':8}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--mode',choices=['train','resume','compare'],default='train')
    parser.add_argument('--steps',type=int,required=True);parser.add_argument('--seed',type=int,default=42)
    args=parser.parse_args();out=Path('/output');started=time.time();job=None
    def state(phase,**fields):atomic_json(out/'job-state.json',{'phase':phase,'updated_unix':time.time(),'started_unix':started,**fields})
    try:
        state('PREFLIGHT')
        import torch
        from lerobot_sdk.training_runtime import configure_runtime
        options=configure_runtime(torch,'cuda:0');torch.set_num_threads(2)
        matrix=torch.ones((32,32),device='cuda:0');checksum=float((matrix@matrix).sum().item());del matrix
        assert checksum==32768.0
        gpu={'cuda_available':torch.cuda.is_available(),'device_count':torch.cuda.device_count(),
             'device_name':torch.cuda.get_device_name(0),'kernel_checksum':checksum,'runtime_policy':options}
        atomic_json(out/'gpu-preflight.json',gpu)
        manager=DatasetManager();source=manager.inspect('/data')
        assert source.ref.content_hash==DATA_HASH and source.frames==408
        trainer=Trainer(out/'jobs')
        cfg=TrainConfig(dataset_dir=Path('/data'),output_dir=out/'attempt',steps=args.steps,batch_size=2,
                        seed=args.seed,device='cuda:0',cpu_threads=2,save_freq=4,policy_params=PARAMS,
                        scheduler_params={'type':'constant_with_warmup','num_warmup_steps':1})
        if args.mode=='resume':job=trainer.resume('/resume',cfg)
        else:job=trainer.start(cfg)
        atomic_json(out/'job-identity.json',{'job_id':job.id,'mode':args.mode,'config':cfg.to_dict(),'started_unix':started})
        while True:
            current=job.status();state(current.state.value,job_id=job.id,step=current.step)
            if current.state.value in ('SUCCEEDED','FAILED','CANCELLED'):break
            if (out/'cancel.trigger').exists():
                requested=time.time();current=job.cancel(timeout_s=120)
                atomic_json(out/'cancel-result.json',{'requested_unix':requested,'confirmed_unix':time.time(),
                            'status':{**asdict(current),'checkpoint_dir':str(current.checkpoint_dir) if current.checkpoint_dir else None}})
                break
            if time.time()-started>1200:raise TimeoutError('Owned training validation exceeded walltime')
            time.sleep(.2)
        current=job.status()
        if current.state.value=='CANCELLED':
            assert current.checkpoint_dir is not None
            checkpoint=inspect_checkpoint(current.checkpoint_dir)
            assert checkpoint['runtime']['execution_device']['cuda_used']
            report={'state':'CANCELLED','job_id':job.id,'step':current.step,'checkpoint_dir':str(current.checkpoint_dir),
                    'dataset_hash':DATA_HASH,'finished_unix':time.time(),'started_unix':started}
        else:
            result=job.wait(30);models=ModelManager();manifest=models.inspect(result.model_dir)
            with models.load(result.model_dir,device='cuda:0') as handle:load=asdict(handle.load_report)
            saved=read_json(result.model_dir/'manifest.json')
            report={'state':'SUCCEEDED','job_id':job.id,'step':result.step,'checkpoint_dir':str(result.checkpoint_dir),
                    'model_dir':str(result.model_dir),'model_hash':manifest.content_hash,'load':load,
                    'dataset_hash':DATA_HASH,'finished_unix':time.time(),'started_unix':started,
                    'runtime':read_json(result.checkpoint_dir/'manifest.json')['runtime']}
            assert report['runtime']['execution_device']['cuda_used']
            if args.mode=='compare':
                from safetensors.torch import load_file
                left=load_file(str(result.model_dir/'model.safetensors'));right=load_file('/resume/pretrained_model/model.safetensors')
                assert left.keys()==right.keys()
                max_abs=max(float((left[k].float()-right[k].float()).abs().max()) for k in left)
                exact=all(torch.equal(left[k],right[k]) for k in left)
                close=all(torch.allclose(left[k],right[k],atol=1e-6,rtol=1e-5) for k in left)
                report['resume_comparison']={'bitwise_equal':exact,'allclose':close,'max_abs':max_abs,'tensors':len(left),
                                             'atol':1e-6,'rtol':1e-5,'checkpoint_hash':inspect_checkpoint('/resume')['content_hash']}
                if not close:raise ValueError(f'Resumed and continuous CUDA weights diverge: {max_abs}')
        assert manager.inspect(source.ref).ref.content_hash==DATA_HASH
        atomic_json(out/'job-report.json',report);state(report['state'],job_id=job.id,step=report['step'])
    except BaseException as exc:
        traceback.print_exc()
        if job is not None and job.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):
            try:job.cancel(timeout_s=30,force=True)
            except Exception:pass
        state('FAILED',error=f'{type(exc).__name__}: {exc}')
        raise


if __name__=='__main__':main()
