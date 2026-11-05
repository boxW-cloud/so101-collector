"""Frozen registry -> existing managed Trainer, with real metrics/artifacts."""
import json,time
from pathlib import Path
from .store import Fault

def validate_training(platform,p,a):
    if p.get('image_id')!=platform.config.get('image_id') or not p.get('image_id'):raise Fault('IMAGE_MISMATCH','Requested image differs from deployment image provenance')
    agent=platform.store.get('agents',p['agent_id'],p['agent_revision'],a)
    if agent['project']!=p['project']:raise Fault('FORBIDDEN','Agent project differs',403)
    if agent['body'].get('seeed_commit')!='4aaff99be4a1d81568c08c8f0296b41b40c99ec4':raise Fault('AGENT_VERSION_MISMATCH','Agent must identify the installed frozen Seeed package')
    refs=p['datasets']
    if not isinstance(refs,list) or not refs:raise Fault('DATASET_COUNT','At least one immutable dataset reference is required')
    if any(not isinstance(ref,dict) or set(ref)!={'dataset_id','version','content_hash'} or
           any(not isinstance(value,str) or not value for value in ref.values()) for ref in refs):
        raise Fault('INVALID_DATASET_REF','Each dataset reference requires dataset_id, version and content_hash strings')
    if len(refs)>1:raise Fault('MULTI_DATASET_UNSUPPORTED','Multiple dataset references are recognized but execution requires an explicitly merged immutable version')
    ref=refs[0];data=platform.store.get('datasets',ref['dataset_id'],ref['version'],a)
    if data['project']!=p['project'] or data['body']['content_hash']!=ref['content_hash']:raise Fault('DATASET_VERSION_MISMATCH','Dataset project/hash differs')
    if data['body']['format']=='gr00t-v2.1':raise Fault('FORMAT_MISMATCH','ACT/pi05 use the LeRobot V3 version, not GR00T conversion')
    resources=p.get('resources',{});limits=platform.config.get('resources',{});cpu=resources.get('cpu_threads',1);mem=resources.get('memory_mb',1024);gpu=resources.get('gpu',False)
    if type(cpu)!=int or not 1<=cpu<=limits.get('cpu_threads',2) or type(mem)!=int or not 128<=mem<=limits.get('memory_mb',2048) or type(gpu)!=bool or gpu and not limits.get('gpu'):raise Fault('RESOURCE_LIMIT','Requested resource quota unavailable')
    if resources.keys()-{'cpu_threads','memory_mb','gpu'}:raise Fault('INVALID_RESOURCE','Unknown resource field')
    params=p.get('parameters',{})
    if params.keys()-{'steps','batch_size','seed','save_freq','policy_params','scheduler_params'}:raise Fault('INVALID_PARAMETERS','Unknown managed training parameter')
    for name,lo,hi in [('steps',1,1000000),('batch_size',1,32),('save_freq',1,1000000),('seed',0,2**31-1)]:
        if name in params and (type(params[name])!=int or not lo<=params[name]<=hi):raise Fault('INVALID_PARAMETERS',name+' outside managed bounds')
    base=None
    if agent['body']['policy_type']=='pi05':
        b=p['base_model'];base=platform.store.get('models',b['model_id'],b['version'],a)['body']
        if base['content_hash']!=b['content_hash']:raise Fault('BASE_MISMATCH','Base model hash differs')
    if p.get('resume_operation_id'):
        prior=platform.store.job(p['resume_operation_id'],a);before=prior['payload']
        if prior['kind']!='training.start' or prior['project']!=p['project'] or prior['state'] not in ('SUCCEEDED','CANCELLED') or not (prior.get('result') or {}).get('checkpoint_dir'):raise Fault('RESUME_NOT_READY','Completed/cancelled training checkpoint required')
        for field in ('image_id','agent_id','agent_revision','datasets','base_model'):
            if before.get(field)!=p.get(field):raise Fault('RESUME_BINDING_MISMATCH','Resume must retain '+field)
    return {**p,'resolved_dataset':data['body'],'resolved_agent':agent['body'],'resolved_base':base,'resources':{'cpu_threads':cpu,'memory_mb':mem,'gpu':gpu}}

def run_training(platform,key,p,a,cancel):
    from lerobot_sdk import Trainer,TrainConfig,DatasetManager,ModelManager
    from dataclasses import asdict
    data=p['resolved_dataset'];checked=DatasetManager().inspect(Path(data['root']))
    if checked.ref.content_hash!=data['content_hash']:raise Fault('HASH_MISMATCH','Registered immutable dataset changed')
    out=platform.root/'jobs'/key;out.mkdir(parents=True,exist_ok=True)
    params=dict(p.get('parameters',{}));policy=p['resolved_agent']['policy_type']
    params.setdefault('steps',4);params.setdefault('batch_size',1);params.setdefault('save_freq',1)
    if policy=='act':params.setdefault('policy_params',{'pretrained_backbone_weights':None,'chunk_size':8,'n_action_steps':8,'dim_model':32,'n_heads':4,'dim_feedforward':64,'n_encoder_layers':1,'n_decoder_layers':1,'n_vae_encoder_layers':1,'latent_dim':8})
    base=p['resolved_base'];cfg=TrainConfig(dataset_dir=checked.ref.root,policy_type=policy,base_model_dir=Path(base['path']) if base else None,
        output_dir=out/'attempt',device='cuda:0' if p['resources']['gpu'] else 'cpu',cpu_threads=p['resources']['cpu_threads'],**params)
    # Public references and effective parameters, never private resolved paths.
    effective=cfg.to_dict()
    fields={'training_run_id':key,
            'agent':{'agent_id':p['agent_id'],'agent_revision':p['agent_revision'],'policy_type':policy},
            'datasets':p['datasets'],'base_model':p.get('base_model'),'image_id':p['image_id'],
            'parameters':{name:effective[name] for name in ('steps','batch_size','seed','save_freq','policy_params','scheduler_params')},
            'resources_requested':p['resources'],
            'resource_guarantees':{'cpu_threads_bounded':True,'per_job_memory':False,'gpu_isolation':False,
                'parallel_heavy_jobs':False,'requested_memory_enforced':False,
                'container_memory_limit_mb':platform.config.get('resources',{}).get('memory_mb')},
            'metrics_ref':f'/v2/operations/{key}/metrics','logs_ref':f'/v2/operations/{key}/logs',
            'model_version':None}
    fields=json.loads(json.dumps(fields))  # Snapshot bindings independently of caller mutations.
    trainer=Trainer(out/'trainer')
    if p.get('resume_operation_id'):
        prior=platform.store.job(p['resume_operation_id'],a)
        if prior['state'] not in ('SUCCEEDED','CANCELLED') or not prior.get('result',{}).get('checkpoint_dir'):raise Fault('RESUME_NOT_READY','Verified prior checkpoint required')
        job=trainer.resume(prior['result']['checkpoint_dir'],cfg)
    else:job=trainer.start(trainer.validate_config(cfg))
    last=-1
    try:
        while True:
            st=job.status()
            if st.step!=last:platform.store.event(key,'training_progress',{'step':st.step,'target_steps':st.target_steps,'state':st.state.value});last=st.step
            if st.state.value in ('SUCCEEDED','FAILED','CANCELLED'):break
            if cancel.is_set():
                job.cancel(timeout_s=30);st=job.status()
                return {**fields,'state':'CANCELLED','step':st.step,'checkpoint_dir':str(st.checkpoint_dir) if st.checkpoint_dir else None,'dataset_hash':data['content_hash']}
            time.sleep(.2)
        if st.state.value=='CANCELLED':return {**fields,'state':'CANCELLED','step':st.step,'checkpoint_dir':str(st.checkpoint_dir) if st.checkpoint_dir else None,'dataset_hash':data['content_hash']}
        result=job.wait(5);model=ModelManager().inspect(result.model_dir)
        body={'model_id':key,'version':'v1','policy_type':policy,'path':str(result.model_dir),'content_hash':model.content_hash,'dataset_hash':data['content_hash'],'image_id':p['image_id']}
        platform.store.register('models',key,'v1',p['project'],a,body)
        return {**json.loads(json.dumps(asdict(result),default=str)),**fields,'model':body,'model_version':body['version'],'resource_enforcement':'single-heavy-job; CPU threads bounded; container aggregate memory limit, not per-user GPU partition'}
    finally:
        if job.status().state.value not in ('SUCCEEDED','FAILED','CANCELLED'):job.cancel(timeout_s=30,force=True)
