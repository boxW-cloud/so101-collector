"""Initialize either delivery service inside its container, without starting a task."""
from pathlib import Path,PurePosixPath
import hashlib
import json
import os
import re
import secrets
import sys
from filelock import FileLock


def _write(path,value):
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    tmp.chmod(0o600);tmp.replace(path)


def prepare(env=None,root=None,shared=None):
    env=dict(os.environ if env is None else env)
    role=env.get('SO101_SERVICE_ROLE','')
    if role not in ('collection','training'):raise ValueError('SO101_SERVICE_ROLE must be collection or training')
    image_id=env.get('SO101_IMAGE_ID','')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',image_id):
        raise ValueError('SO101_IMAGE_ID is required: pass the actual image ID from docker image inspect')
    port=int(env.get('SO101_PORT','8840' if role=='collection' else '8841'))
    if not 1<=port<=65535:raise ValueError('SO101_PORT must be 1..65535')
    gpu=env.get('SO101_GPU','false')
    if gpu not in ('true','false'):raise ValueError('SO101_GPU must be true or false')
    project=env.get('SO101_PROJECT','lab')
    if not re.fullmatch('[A-Za-z0-9][A-Za-z0-9_.-]{0,127}',project):raise ValueError('Invalid SO101_PROJECT')
    delivery_mode=env.get('SO101_COLLECTION_DELIVERY_MODE','directory')
    output_root=env.get('SO101_COLLECTION_OUTPUT_ROOT','/delivery')
    if role=='collection':
        if delivery_mode not in ('directory','http'):raise ValueError('Collection delivery mode must be directory or http')
        if delivery_mode=='directory' and (not output_root or not (Path(output_root).is_absolute() or PurePosixPath(output_root).is_absolute())):
            raise ValueError('Collection output directory must be absolute')
        if delivery_mode=='http' and not env.get('SO101_TRAINING_URL'):
            raise ValueError('HTTP delivery requires explicit SO101_TRAINING_URL')
        if env.get('SO101_TRAINING_URL'):
            from .collection_destinations import normalize_url
            try:normalize_url(env['SO101_TRAINING_URL'])
            except Exception as error:raise ValueError('Invalid configured HTTP delivery URL') from error
    requested=env.get('SO101_API_TOKEN')
    if requested is not None and not re.fullmatch('[A-Za-z0-9_-]{24,256}',requested):raise ValueError('API token requires 24..256 URL-safe characters')
    bootstrap=[]
    if env.get('SO101_BOOTSTRAP_JSON'):
        bootstrap=json.loads(Path(env['SO101_BOOTSTRAP_JSON']).read_text(encoding='utf-8'))
        if not isinstance(bootstrap,list):raise ValueError('SO101_BOOTSTRAP_JSON must contain a list of registered resources')
    root=Path(root or env.get('SO101_DATA_ROOT','/data')).resolve()
    shared=Path(shared or env.get('SO101_SHARED_ROOT','/shared')).resolve()
    root.mkdir(parents=True,exist_ok=True);shared.mkdir(parents=True,exist_ok=True)
    system=root/'system';system.mkdir(exist_ok=True)
    with FileLock(str(shared/'init.lock'),timeout=15):
        token_path=shared/'access-token'
        if token_path.exists():
            token=token_path.read_text(encoding='ascii').strip()
            if not re.fullmatch('[A-Za-z0-9_-]{24,256}',token):raise ValueError('Existing credential is invalid; not silently replacing it')
            if requested and requested!=token:raise ValueError('Token differs from existing shared credential; not rotating it implicitly')
        else:
            token=requested or secrets.token_urlsafe(36)
            tmp=shared/'access-token.tmp';tmp.write_text(token,encoding='ascii');tmp.chmod(0o600);tmp.replace(token_path)
        ros_key=shared/'ros-key'
        if not ros_key.exists():
            tmp=shared/'ros-key.tmp';tmp.write_text(secrets.token_urlsafe(36),encoding='ascii');tmp.chmod(0o600);tmp.replace(ros_key)
    with FileLock(str(system/'init.lock'),timeout=15):
        identity=system/'deployment.json'
        if identity.exists():
            old=json.loads(identity.read_text(encoding='utf-8'))
            if old['role']!=role or old['project']!=project:raise ValueError('Data volume belongs to a different service role/project')
        _write(identity,{'role':role,'project':project})
        config={'role':role,'root':str(root),'port':port,'bind':env.get('SO101_BIND','0.0.0.0'),'image_id':image_id,
            'resources':{'cpu_threads':2,'memory_mb':4096,'gpu':gpu=='true'},
            'import_roots':['/exchange'],'import_sources':{},'model_roots':['/models',str(root/'jobs')],'bootstrap':bootstrap}
        if role=='collection':
            config['collection_contract_only']=True
            config['sinks']={}
            if delivery_mode=='directory':
                config['sinks']['local']={'kind':'directory','directory':output_root,'project':project,'principal':'operator'}
            if env.get('SO101_TRAINING_URL'):
                config['sinks']['training']={'url':env['SO101_TRAINING_URL'],
                    'token_file':str(token_path),'project':project,'principal':'operator'}
            config['default_upload_sink']='local' if delivery_mode=='directory' else 'training'
            config['converter']={'version':'nvidia-51d4c89f-framealign-language-v1',
                'path':'/opt/converters/nvidia/scripts/lerobot_conversion/convert_v3_to_v2.py',
                'sha256':'sha256:94bc600b613be43133aabcddde64ca3188048a09cd84e293afe4d5a79614e24c',
                'runtime_id':image_id}
        else:
            for policy in ('act','pi05'):
                if not any(x.get('kind')=='agents' and x.get('id')==policy for x in bootstrap):
                    config['bootstrap'].append({'kind':'agents','id':policy,'revision':'seeed-4aaff99','project':project,
                        'body':{'policy_type':policy,'seeed_commit':'4aaff99be4a1d81568c08c8f0296b41b40c99ec4'}})
        accounts=[{'name':'operator','projects':[project],'roles':['admin','operator','trainer'],
                   'token_sha256':hashlib.sha256(token.encode()).hexdigest()}]
        _write(system/'service.json',config);_write(system/'accounts.json',accounts)
    return system/'service.json',system/'accounts.json'


def main():
    config,accounts=prepare()
    print('Initialized service; API credential is in /shared/access-token. No robot connected, no collection or training started.',flush=True)
    os.execv(sys.executable,[sys.executable,'-m','deployment_runtime.http','--config',str(config),'--accounts',str(accounts)])

if __name__=='__main__':main()
