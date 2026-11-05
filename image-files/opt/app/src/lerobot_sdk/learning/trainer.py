"""Owned local processes for native ACT training; no shell-built commands or device I/O."""
from pathlib import Path
from dataclasses import replace
import json
import os
import signal
import subprocess
import sys
import threading
import uuid
from lerobot_sdk.learning.contracts import (TrainConfig,ValidatedTrainConfig,PolicyInfo,TrainingStatus,TrainingEvent,
                              TrainResult,JobState,plain)
from lerobot_sdk.data.manager import DatasetManager
from lerobot_sdk.policies.models import inspect_checkpoint
from lerobot_sdk.data.filesystem import read_json,atomic_json
from lerobot_sdk.core.errors import SDKError,ConfigurationError,ContractMismatch,OperationFailed,OperationCancelled,WaitTimeout,StateError
from lerobot_sdk.core.types import finite,integer
from lerobot_sdk.adapters.seeed_training import training_available,build_native,discover_policies


class Trainer:
    def __init__(self,work_dir):self.work_dir=Path(work_dir).expanduser().resolve()

    def list_policies(self):
        return list(discover_policies())

    def validate_config(self,config):
        return self._validate(config)

    def _validate(self,config,checkpoint=None):
        if not isinstance(config,TrainConfig):raise ConfigurationError('TrainConfig required')
        try:
            ref=DatasetManager().inspect(config.dataset_dir).ref
            manifest=read_json(ref.root/'manifest.json')
            if config.output_dir and config.output_dir.exists():raise ConfigurationError('Each attempt requires a new output directory')
            if config.output_dir and (config.output_dir.is_relative_to(ref.root) or ref.root.is_relative_to(config.output_dir)):
                raise ConfigurationError('Output and immutable dataset cannot overlap')
            proposed=config.output_dir or self.work_dir/'validation-only'
            for source in (ref.root,config.base_model_dir,checkpoint):
                if source is not None and (proposed.is_relative_to(source) or Path(source).is_relative_to(proposed)):
                    raise ConfigurationError('Training output cannot overlap dataset/model/checkpoint')
            cfg=build_native(config,manifest,config.output_dir or self.work_dir/'validation-only',checkpoint)
            # Environment locking is an opt-in caller concern, not a new
            # platform/image field in the public TrainConfig.
            if os.environ.get('SO101_TRAINING_ENV_LOCK') or os.environ.get('SO101_IMAGE_PROVENANCE'):
                from lerobot_sdk.learning.environment import current_training_environment
                current_training_environment()
            base_hash=None
            if config.base_model_dir is not None and checkpoint is None:
                from lerobot_sdk.policies.models import ModelManager
                base_hash=ModelManager().inspect(config.base_model_dir).content_hash
            return ValidatedTrainConfig(config,ref.content_hash,json.dumps(cfg.to_dict(),sort_keys=True),base_hash)
        except SDKError:raise
        except Exception as exc:raise ConfigurationError(str(exc)) from exc

    def start(self,config):
        validated=self.validate_config(config.config if isinstance(config,ValidatedTrainConfig) else config)
        if isinstance(config,ValidatedTrainConfig) and config.dataset_hash!=validated.dataset_hash:
            raise ContractMismatch('Dataset changed after validation')
        if isinstance(config,ValidatedTrainConfig) and config.base_model_hash!=validated.base_model_hash:
            raise ContractMismatch('Base model changed after validation')
        return self._launch(validated)

    def resume(self,checkpoint,config):
        source=Path(checkpoint).expanduser().resolve()
        saved=inspect_checkpoint(source)
        from lerobot_sdk.learning.runtime import runtime_options,verify_runtime_options
        candidate_config=config.config if isinstance(config,ValidatedTrainConfig) else config
        verify_runtime_options(saved.get('runtime',{}).get('worker_policy'),runtime_options(candidate_config.device))
        from lerobot_sdk.learning.environment import verify_resume_environment
        verify_resume_environment(saved.get('runtime',{}))
        if saved.get('loader_rng')!='isolated-generator-v1':raise ContractMismatch('Checkpoint loader RNG contract is not supported')
        validated=self._validate(config.config if isinstance(config,ValidatedTrainConfig) else config,source)
        candidate=validated.config
        if validated.dataset_hash!=saved['dataset_hash']:raise ContractMismatch('Resume requires the identical dataset version')
        if candidate.steps<=saved['step']:raise ConfigurationError('Resume target steps must exceed saved step')
        prior=saved['train_spec']
        for key in ('policy_type','batch_size','seed','device','policy_params','scheduler_params','num_workers','cpu_threads','base_model_dir'):
            if candidate.to_dict()[key]!=prior[key]:raise ConfigurationError(f'Full resume cannot change {key}')
        if candidate.output_dir and (candidate.output_dir.is_relative_to(source) or source.is_relative_to(candidate.output_dir)):
            raise ConfigurationError('Resume output cannot overlap the source checkpoint')
        return self._launch(validated,source)

    def _launch(self,validated,checkpoint=None):
        config=validated.config
        from lerobot_sdk.learning.environment import current_training_environment
        environment,image=current_training_environment()
        job_id=uuid.uuid4().hex
        attempt=config.output_dir or self.work_dir/job_id
        if attempt.is_relative_to(config.dataset_dir) or config.dataset_dir.is_relative_to(attempt):raise ConfigurationError('Attempt and dataset trees overlap')
        for source in (config.base_model_dir,checkpoint):
            if source is not None and (attempt.is_relative_to(source) or Path(source).is_relative_to(attempt)):
                raise ConfigurationError('New attempt cannot overlap source model/checkpoint')
        attempt.parent.mkdir(parents=True,exist_ok=True)
        try:attempt.mkdir()
        except FileExistsError as exc:raise StateError('Output already exists; never overwrite a training attempt') from exc
        atomic_json(attempt/'request.json',{'job_id':job_id,'config':config.to_dict(),'dataset_hash':validated.dataset_hash,
                    'base_model_hash':validated.base_model_hash,'checkpoint':str(checkpoint) if checkpoint else None,
                    'environment_hash':environment['content_hash'],'image_provenance':image})
        atomic_json(attempt/'environment.json',environment)
        env=os.environ.copy()
        for key in list(env):
            if key in ('RANK','WORLD_SIZE','LOCAL_RANK','LOCAL_WORLD_SIZE','MASTER_ADDR','MASTER_PORT') or key.startswith(('ACCELERATE_','FSDP_')):
                env.pop(key,None)
        env.update(HF_HUB_OFFLINE='1',TRANSFORMERS_OFFLINE='1',WANDB_MODE='disabled',PYTHONUNBUFFERED='1',
                   OMP_NUM_THREADS=str(config.cpu_threads),MKL_NUM_THREADS=str(config.cpu_threads),PYTHONIOENCODING='utf-8')
        command=[sys.executable,'-m','lerobot_sdk._training_worker','--attempt',str(attempt)]
        try:
            with (attempt/'worker.log').open('wb') as log:
                process=subprocess.Popen(command,stdout=log,stderr=log,env=env,
                    creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0,start_new_session=os.name!='nt')
        except OSError as exc:raise OperationFailed(f'Could not launch owned training process: {exc}') from exc
        atomic_json(attempt/'launch.json',{'job_id':job_id,'pid':process.pid,'command':command})
        return TrainingJob(job_id,config,attempt,process,validated.dataset_hash,checkpoint)


class TrainingJob:
    def __init__(self,job_id,config,attempt,process,dataset_hash,checkpoint):
        self.id=job_id;self.config=config;self.output_dir=attempt;self._process=process
        self._dataset_hash=dataset_hash;self._checkpoint=checkpoint;self._lock=threading.RLock()
        self._done=threading.Event();self._state=JobState.STARTING;self._error=None;self._result=None
        self._step=0;self._last_checkpoint=None;self._forced=False
        threading.Thread(target=self._monitor,name='TrainingJob-'+job_id,daemon=True).start()

    def _progress(self):
        path=self.output_dir/'progress.json'
        if path.exists():
            data=read_json(path);self._step=data['step']
            self._last_checkpoint=Path(data['checkpoint_dir']) if data.get('checkpoint_dir') else None
            if self._state==JobState.STARTING:self._state=JobState.RUNNING

    def status(self):
        with self._lock:
            self._progress()
            return TrainingStatus(self.id,self._state,self._step,self.config.steps,self._last_checkpoint,self._error)

    def _monitor(self):
        code=self._process.wait()
        with self._lock:
            try:
                self._progress()
                if self._forced:
                    self._state=JobState.CANCELLED
                    if self._last_checkpoint:inspect_checkpoint(self._last_checkpoint)
                else:
                    result=read_json(self.output_dir/'result.json')
                    if result['job_id']!=self.id:raise OperationFailed('Training result job identity mismatch')
                    self._step=result['step'];self._last_checkpoint=Path(result['checkpoint_dir']) if result.get('checkpoint_dir') else None
                    if code!=0 or result['state']=='FAILED':raise OperationFailed(result.get('error','Native training failed'))
                    self._state=JobState(result['state'])
                    if self._state==JobState.SUCCEEDED:
                        inspect_checkpoint(self._last_checkpoint)
                        self._result=TrainResult(self.id,self.output_dir,Path(result['model_dir']),self._last_checkpoint,
                                                 self._step,self._dataset_hash,self._checkpoint)
            except Exception as exc:self._state=JobState.FAILED;self._error=str(exc)
            finally:self._done.set()

    def wait(self,timeout_s=None):
        if timeout_s is not None:timeout_s=finite(timeout_s,'wait timeout',nonnegative=True)
        if not self._done.wait(timeout_s):raise WaitTimeout('Training wait expired; the job continues')
        if self._state==JobState.CANCELLED:raise OperationCancelled('Training cancelled after process exit; inspect status for a verified checkpoint')
        if self._state!=JobState.SUCCEEDED:raise OperationFailed(self._error or 'Training failed')
        return self._result

    def cancel(self,timeout_s=30,*,force=False):
        timeout_s=finite(timeout_s,'cancel wait',nonnegative=True)
        with self._lock:
            if self._done.is_set():return self.status()
            self._state=JobState.CANCELLING
            atomic_json(self.output_dir/'cancel.request',{'job_id':self.id})
        if not self._done.wait(timeout_s):
            if not force:raise WaitTimeout('Cancellation pending until a checkpoint boundary; force was not requested')
            with self._lock:
                if not self._done.is_set():
                    self._forced=True
                    if os.name=='nt':
                        subprocess.run(['taskkill','/PID',str(self._process.pid),'/T','/F'],capture_output=True,check=False,
                                       creationflags=subprocess.CREATE_NO_WINDOW)
                    else:os.killpg(self._process.pid,signal.SIGKILL)
            if not self._done.wait(30):raise OperationFailed('Owned process exit not confirmed')
        return self.status()

    def iter_events(self,*,after_sequence=0,follow=False):
        offset=0
        while True:
            path=self.output_dir/'events.jsonl'
            if path.exists():
                with path.open('r',encoding='utf-8') as stream:
                    stream.seek(offset)
                    while True:
                        start=stream.tell();line=stream.readline()
                        if not line or not line.endswith('\n'):offset=start;break
                        offset=stream.tell();data=json.loads(line)
                        if data['sequence']>after_sequence:yield TrainingEvent(data['sequence'],data['kind'],data['detail'])
            if not follow or self._done.is_set():break
            self._done.wait(.1)
