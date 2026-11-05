"""Immutable model artifacts; deploy prepares files, load never operates a robot."""
from pathlib import Path
import os
import shutil
import threading
import uuid
from lerobot_sdk.data.filesystem import atomic_json,read_json,inventory,digest,remove_owned_tree,sha_file,disk_check
from lerobot_sdk.data.contracts import SEEED_COMMIT
from lerobot_sdk.learning.contracts import ModelManifest,DeploymentRef,LoadReport,plain
from lerobot_sdk.core.errors import SDKError,ContractMismatch,ConfigurationError,StateError,BusyError
from lerobot_sdk.core.async_operation import background
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.types import JOINT_NAMES,named,calibration,integer


def _verify_seal(root,kind):
    root=Path(root).absolute()
    if root.is_symlink():raise ContractMismatch('Use the explicit artifact path, not a symlink')
    data=read_json(root/'manifest.json')
    if data.get('manifest_version')!=1 or data.get('kind')!=kind or data.get('status')!='LOCAL_READY' or data.get('lerobot_commit')!=SEEED_COMMIT:
        raise ContractMismatch('Unsupported or incomplete artifact')
    if digest({k:v for k,v in data.items() if k!='content_hash'})!=data.get('content_hash'):
        raise ContractMismatch('Artifact manifest hash differs')
    if inventory(root)!=data['files']:raise ContractMismatch('Artifact files are missing, extra or changed')
    return data


def _write_seal(root,data):
    data={**data,'status':'LOCAL_READY','lerobot_commit':SEEED_COMMIT,'files':inventory(root)}
    data['content_hash']=digest(data)
    atomic_json(Path(root)/'manifest.json',data)
    return data


def check_model_files(root):
    root=Path(root)
    required=['config.json','train_config.json','model.safetensors','policy_preprocessor.json','policy_postprocessor.json']
    if any(not (root/name).is_file() for name in required):raise ContractMismatch('Model requires config, weights and both saved processors')
    config=read_json(root/'config.json')
    if config.get('type') not in ('act','pi05') or set(config['output_features'])!={'action'} or config['output_features']['action']['shape']!=[6]:
        raise ContractMismatch('Expected ACT/pi05 with exactly one six-dimensional action feature; base 32D models are not SO101 artifacts')
    if config.get('pretrained_backbone_weights') is not None:
        raise ContractMismatch('Self-contained models must not trigger backbone downloads')
    if config.get('use_relative_actions') and tuple(config.get('action_feature_names') or ())!=JOINT_NAMES:
        raise ContractMismatch('Relative action models must declare the canonical joint names')
    from safetensors import safe_open
    for name in ('policy_preprocessor.json','policy_postprocessor.json'):
        processor=read_json(root/name)
        if not processor.get('steps'):raise ContractMismatch('Empty processor pipeline')
        states=0
        for step in processor['steps']:
            filename=step.get('state_file')
            if filename:
                states+=1
                if Path(filename).name!=filename or not filename.endswith('.safetensors') or not (root/filename).is_file():
                    raise ContractMismatch('Processor state is missing or not a local safetensors file')
                with safe_open(root/filename,framework='np') as stream:
                    if not list(stream.keys()):raise ContractMismatch('Empty normalization state')
        if not states:raise ContractMismatch('Missing processor normalization statistics')
    with safe_open(root/'model.safetensors',framework='np') as stream:
        if not list(stream.keys()):raise ContractMismatch('Empty model weights')
    if config['type']=='pi05':
        if not (root/'tokenizer/tokenizer_config.json').is_file() or not any((root/'tokenizer'/name).is_file() for name in ('tokenizer.json','tokenizer.model')):
            raise ContractMismatch('pi05 artifacts must bundle the authorized tokenizer locally')
    return config


def seal_model(root,dataset_manifest,provenance):
    config=check_model_files(root)
    contract={k:dataset_manifest[k] for k in ('joint_names','units','camera_keys','camera_shapes','calibration_hash','robot_id','fps')}
    return _write_seal(root,{'manifest_version':1,'kind':'lerobot-sdk-model-v1','policy_type':config['type'],
        'dataset_hash':dataset_manifest['content_hash'],'dataset_schema':dataset_manifest['schema'],
        'contract':contract,'input_features':config['input_features'],'output_features':config['output_features'],
        **provenance})


def seal_checkpoint(root,metadata):
    required=['optimizer_param_groups.json','optimizer_state.safetensors','rng_state.safetensors','training_step.json']
    state=Path(root)/'training_state'
    if metadata['scheduler_present']:required.append('scheduler_state.json')
    if any(not (state/name).is_file() for name in required):raise ContractMismatch('Incomplete training state cannot be resumable')
    return _write_seal(root,{'manifest_version':1,'kind':'lerobot-sdk-checkpoint-v1',**metadata})


def inspect_checkpoint(root):
    root=Path(root).resolve()
    manifest=_verify_seal(root,'lerobot-sdk-checkpoint-v1')
    model=ModelManager().inspect(root/'pretrained_model')
    state=root/'training_state'
    if read_json(state/'training_step.json')['step']!=manifest['step'] or model.step!=manifest['step']:
        raise ContractMismatch('Checkpoint step differs from model/training metadata')
    for filename in ('optimizer_param_groups.json','optimizer_state.safetensors','rng_state.safetensors'):
        if not (state/filename).is_file():raise ContractMismatch('Missing optimizer or RNG state')
    if manifest['scheduler_present'] and not (state/'scheduler_state.json').is_file():raise ContractMismatch('Missing scheduler state')
    if model.dataset_hash!=manifest['dataset_hash']:raise ContractMismatch('Checkpoint and model data provenance differ')
    return manifest


class ModelManager:
    def inspect(self,model_dir):
        try:
            root=Path(model_dir).absolute()
            data=_verify_seal(root,'lerobot-sdk-model-v1')
            if data.get('policy_type') not in ('act','pi05'):raise ContractMismatch('Unsupported model adapter')
            config=check_model_files(root)
            contract=data['contract']
            if contract['joint_names']!=list(JOINT_NAMES) or len(contract['units'])!=6:raise ContractMismatch('Joint contract mismatch')
            named(dict(zip(JOINT_NAMES,contract['units'],strict=True)),'model units',units=True)
            calibration(contract['calibration_hash'])
            integer(contract['fps'],'model fps',minimum=1)
            if set(contract['camera_keys'])!=set(contract['camera_shapes']) or len(contract['camera_keys'])!=len(set(contract['camera_keys'])):
                raise ContractMismatch('Camera keys and shapes differ')
            if config['input_features']!=data['input_features'] or config['output_features']!=data['output_features']:
                raise ContractMismatch('Model feature contract mismatch')
            expected={'observation.state',*('observation.images.'+k for k in contract['camera_keys'])}
            padding_keys=set()
            if config['type']=='pi05':
                count=config.get('empty_cameras',0);integer(count,'empty_cameras')
                padding_keys={f'observation.images.empty_camera_{i}' for i in range(count)}
                for key in padding_keys:
                    if config['input_features'].get(key,{}).get('shape')!=[3,*config['image_resolution']]:
                        raise ContractMismatch('Declared pi05 padding camera shape differs')
                expected|=padding_keys
            if set(config['input_features'])!=expected:raise ContractMismatch('Camera/state feature names differ')
            if config['input_features']['observation.state']['shape']!=[6]:raise ContractMismatch('State dimension differs')
            for key,shape in contract['camera_shapes'].items():
                if config['input_features']['observation.images.'+key]['shape']!=[3,shape[0],shape[1]]:raise ContractMismatch('Camera shape differs')
            if config['type']!=data['policy_type']:raise ContractMismatch('Policy type differs from manifest')
            return ModelManifest(root.resolve(),config['type'],data['content_hash'],data['dataset_hash'],data['step'],
                                 contract,data['input_features'],data['output_features'])
        except SDKError:raise
        except Exception as exc:raise ContractMismatch(f'Model inspection failed: {exc}') from exc

    def deploy(self,source_dir,target_dir):
        return background(lambda:self._deploy(source_dir,target_dir))

    def _deploy(self,source_dir,target_dir):
        source=self.inspect(source_dir)
        target=Path(target_dir).expanduser().absolute()
        if target.is_symlink():raise ContractMismatch('Linked model destination is forbidden')
        target=target.resolve()
        if target==source.model_dir or target.is_relative_to(source.model_dir) or source.model_dir.is_relative_to(target):
            raise ContractMismatch('Model source and destination overlap')
        target.parent.mkdir(parents=True,exist_ok=True)
        lock=DeviceLock('model-deploy:'+str(target));lock.acquire()
        temp=target.parent/('.'+target.name+'.staging-'+uuid.uuid4().hex)
        try:
            if target.exists():
                existing=self.inspect(target)
                if existing.content_hash!=source.content_hash:raise ContractMismatch('Different model already exists at destination')
                return DeploymentRef(target,existing.content_hash)
            data=read_json(source.model_dir/'manifest.json')
            disk_check(target.parent,sum(x['bytes'] for x in data['files'].values())+1024*1024)
            temp.mkdir()
            for name,item in data['files'].items():
                origin=source.model_dir/name
                if origin.is_symlink() or not origin.resolve().is_relative_to(source.model_dir):raise ContractMismatch('Model source path changed')
                destination=temp/name;destination.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(origin,destination)
                if sha_file(destination)!=item['sha256']:raise ContractMismatch('Model copy hash differs')
            atomic_json(temp/'manifest.json',data)
            self.inspect(temp);self.inspect(source.model_dir)
            if target.exists():raise ContractMismatch('Destination appeared during model deployment')
            os.rename(temp,target)
            return DeploymentRef(target,source.content_hash)
        finally:
            remove_owned_tree(temp,target.parent);lock.release()

    def load(self,model_dir,device='cpu'):
        manifest=self.inspect(model_dir)
        from lerobot_sdk.adapters.seeed_training import validate_device
        from lerobot_sdk.adapters.seeed_dataset import upstream
        validate_device(device);upstream()
        from lerobot_sdk.adapters.policy_runtime import load_native_policy
        from lerobot.configs.policies import PreTrainedConfig
        from lerobot.policies.factory import make_pre_post_processors
        config=PreTrainedConfig.from_pretrained(manifest.model_dir,local_files_only=True)
        config.device=device;config.push_to_hub=False
        rtc=getattr(config,'rtc_config',None)
        if rtc is not None and rtc.enabled:raise ConfigurationError('RTC prefix scheduling is not supported by this runner')
        try:
            policy=load_native_policy(manifest.policy_type,manifest.model_dir,config)
            overrides={'device_processor':{'device':device}}
            if manifest.policy_type=='pi05':overrides['tokenizer_processor']={'tokenizer_name':str(manifest.model_dir/'tokenizer')}
            pre,post=make_pre_post_processors(policy_cfg=config,pretrained_path=str(manifest.model_dir),
                preprocessor_overrides=overrides,
                postprocessor_overrides={'device_processor':{'device':'cpu'}})
            handle=ModelHandle(manifest,policy,pre,post,device)
            try:handle._warmup()
            except Exception:handle.close();raise
            return handle
        except SDKError:raise
        except Exception as exc:raise ContractMismatch(f'Model/processor load or warmup failed: {exc}') from exc


class ModelHandle:
    """Own model resources. Every PolicySession receives independent native state."""
    def __init__(self,manifest,policy,pre,post,device):
        self.manifest=manifest;self._policy=policy;self._pre=pre;self._post=post
        self._device=device;self._lock=threading.RLock();self.load_report=None
        self._sessions=set()

    @property
    def is_loaded(self):return self._policy is not None

    def _warmup(self):
        import torch
        with self._lock,torch.inference_mode():
            p=self._policy;p.eval();p.reset();self._pre.reset();self._post.reset()
            observation={'observation.state':torch.zeros(6),'task':'Local model loading check'}
            for key,shape in self.manifest.contract['camera_shapes'].items():
                observation['observation.images.'+key]=torch.zeros(3,shape[0],shape[1])
            batch=self._pre(observation)
            chunk=self._post(p.predict_action_chunk(batch))
            p.reset()
            step=self._post(p.select_action(batch))
            finite=bool(torch.isfinite(chunk).all() and torch.isfinite(step).all())
            if tuple(chunk.shape)!=(1,p.config.chunk_size,6) or tuple(step.shape)!=(1,6) or not finite:
                raise ContractMismatch('Loaded model warmup shape or finite check failed')
            self.load_report=LoadReport(self._device,tuple(step.shape),tuple(chunk.shape),finite)
            p.reset();self._pre.reset();self._post.reset()

    def close(self):
        with self._lock:
            if self._sessions:raise BusyError('Close active PolicySessions before unloading their model')
            self._policy=self._pre=self._post=None

    def create_session(self,task,contract):
        from lerobot_sdk.policies.session import PolicySession
        with self._lock:
            if not self.is_loaded:raise StateError('Model is closed')
            PolicySession.validate_contract(self,task,contract)
            runtime=ModelManager().load(self.manifest.model_dir,device=self._device)
            try:session=PolicySession(self,runtime,task,contract)
            except Exception:runtime.close();raise
            self._sessions.add(session)
            return session

    def _release_session(self,session):
        with self._lock:self._sessions.discard(session)

    def __enter__(self):
        if not self.is_loaded:raise StateError('Model is closed')
        return self

    def __exit__(self,*exc):self.close()
