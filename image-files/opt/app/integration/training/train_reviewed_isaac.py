"""Train ACT only from the reviewed immutable Isaac export. Never connects robots."""
import argparse,json
from pathlib import Path
from dataclasses import asdict
from lerobot_sdk import Trainer,TrainConfig,DatasetManager,ModelManager
HASH='sha256:5646297f89d76d2505bec177be78a53af232eb3a05a2e204597e95ee862b3804'
p=argparse.ArgumentParser();p.add_argument('--dataset',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
a.output.mkdir(exist_ok=False,parents=True)
ref=DatasetManager().inspect(a.dataset).ref
if ref.content_hash!=HASH:raise ValueError('Not the final reviewed dataset')
m=json.loads((ref.root/'manifest.json').read_text());assert m['episodes']==10 and sum(m['episode_lengths'])==408
params={'pretrained_backbone_weights':None,'chunk_size':8,'n_action_steps':8,'dim_model':32,'n_heads':4,'dim_feedforward':64,'n_encoder_layers':1,'n_decoder_layers':1,'n_vae_encoder_layers':1,'latent_dim':8}
t=Trainer(a.output/'jobs');cfg=TrainConfig(dataset_dir=a.dataset,output_dir=a.output/'act50',steps=50,batch_size=2,seed=42,device='cpu',cpu_threads=2,policy_params=params,save_freq=25)
v=t.validate_config(cfg)
(a.output/'preflight.json').write_text(json.dumps({'dataset_hash':v.dataset_hash,'native_config':json.loads(v.native_config_json)},indent=2))
job=t.start(v);result=job.wait(1800)
models=ModelManager();manifest=models.inspect(result.model_dir)
import torch;torch.set_num_threads(2)
with models.load(result.model_dir,device='cpu') as handle:load=asdict(handle.load_report)
assert DatasetManager().inspect(ref).ref.content_hash==HASH
report={'passed':True,'dataset_hash':HASH,'dataset_frames':408,'result':asdict(result),'model_hash':manifest.content_hash,'load':load,'scope':'50-step functional ACT on scripted simulation data, no task success claim'}
(a.output/'training-result.json').write_text(json.dumps(report,default=str,indent=2));print(json.dumps(report,default=str))
