"""Pretrained pi05 numerical forward probe with explicit synthetic tokens.

This does NOT verify the gated tokenizer, semantic task conditioning, SO101
mapping, PolicySession end-to-end behavior, or hardware execution.
"""
from pathlib import Path
import argparse,json,time


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--model-dir',required=True,type=Path)
    parser.add_argument('--report',required=True,type=Path)
    parser.add_argument('--inference-steps',type=int,default=None)
    args=parser.parse_args();started=time.monotonic()
    report={'passed':False,'scope':'pretrained pi05 raw numerical forward with synthetic token IDs',
            'tokenizer_verified':False,'so101_mapping_verified':False,'hardware_connected':False}
    try:
        available=int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines() if line.startswith('MemAvailable:')))*1024
        report['available_memory_bytes']=available
        if available<30*1024**3:raise RuntimeError('Need at least 30 GiB available memory; shared workloads were not altered')
        import torch
        from lerobot.configs.policies import PreTrainedConfig
        from lerobot.policies.pi05.modeling_pi05 import PI05Policy
        from lerobot.utils.constants import OBS_LANGUAGE_TOKENS,OBS_LANGUAGE_ATTENTION_MASK
        torch.set_num_threads(4)
        if not torch.cuda.is_available():raise RuntimeError('CUDA runtime unavailable')
        cfg=PreTrainedConfig.from_pretrained(args.model_dir,local_files_only=True)
        cfg.device='cuda';cfg.dtype='bfloat16';cfg.compile_model=False
        if args.inference_steps is not None:
            if args.inference_steps<1:raise ValueError('inference-steps must be positive')
            cfg.num_inference_steps=args.inference_steps
        class VerifiedPI05(PI05Policy):
            _loaded=False
            def load_state_dict(self,state_dict,strict=True,assign=False):
                result=super().load_state_dict(state_dict,strict=True,assign=assign)
                self._loaded=True;return result
        torch.cuda.reset_peak_memory_stats()
        policy=VerifiedPI05.from_pretrained(args.model_dir,config=cfg,local_files_only=True,strict=True)
        if not policy._loaded:raise RuntimeError('Native loader returned without loading weights')
        policy.to('cuda').eval()
        batch={'observation.state':torch.zeros(1,32,device='cuda'),
               OBS_LANGUAGE_TOKENS:torch.full((1,8),2,dtype=torch.long,device='cuda'),
               OBS_LANGUAGE_ATTENTION_MASK:torch.ones(1,8,dtype=torch.bool,device='cuda')}
        for name,feature in cfg.image_features.items():batch[name]=torch.zeros((1,*feature.shape),device='cuda')
        torch.manual_seed(42);torch.cuda.synchronize();inference_start=time.perf_counter()
        with torch.inference_mode():output=policy.predict_action_chunk(batch)
        torch.cuda.synchronize()
        report.update(weights_loaded=True,shape=list(output.shape),finite=bool(torch.isfinite(output).all()),
                      inference_seconds=time.perf_counter()-inference_start,peak_cuda_bytes=torch.cuda.max_memory_allocated(),
                      torch=torch.__version__,cuda=torch.version.cuda,num_inference_steps=cfg.num_inference_steps)
        report['passed']=report['shape']==[1,cfg.chunk_size,32] and report['finite']
        del output,policy,batch
    except Exception as exc:
        report['error']=f'{type(exc).__name__}: {exc}'
        import traceback;traceback.print_exc()
    report['elapsed_seconds']=time.monotonic()-started
    args.report.write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report));raise SystemExit(0 if report['passed'] else 1)


if __name__=='__main__':main()
