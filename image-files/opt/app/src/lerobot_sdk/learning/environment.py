"""Training environment locks and caller-verified image provenance.

No Docker calls or platform fields in TrainConfig. A caller may bind a lock via
SO101_TRAINING_ENV_LOCK and a launcher-generated image record via
SO101_IMAGE_PROVENANCE. Image identity is a caller attestation, not a hardware
attestation performed by the SDK.
"""
import argparse
import hashlib
import importlib.metadata as metadata
import json
import os
from pathlib import Path
import platform
import re
import sys
from urllib.parse import urlsplit, urlunsplit
from lerobot_sdk.core.errors import ConfigurationError, ContractMismatch


def digest(value):
    data=json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
    return 'sha256:'+hashlib.sha256(data).hexdigest()


def source_hash(directory):
    root=Path(directory)
    return digest({p.relative_to(root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest()
                   for p in sorted(root.rglob('*.py')) if '__pycache__' not in p.parts})


def capture_environment():
    packages={}
    for dist in metadata.distributions():
        name=re.sub(r'[-_.]+','-',dist.metadata['Name']).lower()
        item={'version':dist.version}
        raw=dist.read_text('direct_url.json')
        if raw:
            source=json.loads(raw)
            if source.get('vcs_info'):
                url=urlsplit(source.get('url',''))
                # Never record credentials, signed query parameters or fragments.
                item['vcs_url']=urlunsplit((url.scheme,url.hostname or '',url.path,'',''))
                item['vcs_info']=source['vcs_info']
            if source.get('archive_info'):
                item['archive_info']=source['archive_info']
        if name in packages and packages[name]!=item:
            raise ConfigurationError(f'Ambiguous installed distributions for {name}')
        packages[name]=item
    import torch
    import lerobot
    payload={'schema':'so101.training-environment.v1','python':platform.python_version(),
             'implementation':platform.python_implementation(),'system':platform.system(),
             'machine':platform.machine(),'packages':packages,'torch_cuda_build':torch.version.cuda,
             'sdk_source_hash':source_hash(Path(__file__).resolve().parents[1]),
             'lerobot_source_hash':source_hash(Path(lerobot.__file__).parent)}
    payload['content_hash']=digest(payload)
    return payload


def verify_environment(expected, actual=None):
    if isinstance(expected,(str,Path)):
        expected=json.loads(Path(expected).read_text(encoding='utf-8'))
    if expected.get('schema')!='so101.training-environment.v1' or digest({k:v for k,v in expected.items() if k!='content_hash'})!=expected.get('content_hash'):
        raise ContractMismatch('Training environment lock is invalid or modified')
    actual=capture_environment() if actual is None else actual
    if digest({k:v for k,v in actual.items() if k!='content_hash'})!=actual.get('content_hash'):
        raise ContractMismatch('Current environment fingerprint is invalid')
    if actual['content_hash']!=expected['content_hash']:
        changed=[k for k in expected if k!='content_hash' and expected.get(k)!=actual.get(k)]
        raise ContractMismatch('Training environment differs from lock: '+', '.join(changed))
    return actual


def image_provenance():
    path=os.environ.get('SO101_IMAGE_PROVENANCE')
    if not path:return None
    record=json.loads(Path(path).read_text(encoding='utf-8'))
    if record.get('schema')!='so101.docker-image-provenance.v1':
        raise ConfigurationError('Unknown image provenance schema')
    for name in ('image_id','oci_manifest_digest'):
        value=record.get(name)
        if value is not None and not re.fullmatch(r'sha256:[0-9a-f]{64}',value):
            raise ConfigurationError(f'Invalid image provenance {name}')
    if not record.get('image_id'):raise ConfigurationError('Image provenance requires the inspected image ID')
    return record


def current_training_environment():
    current=capture_environment()
    lock=os.environ.get('SO101_TRAINING_ENV_LOCK')
    if lock:verify_environment(lock,current)
    return current,image_provenance()


def verify_resume_environment(runtime):
    """New sealed checkpoints require their recorded runtime for full resume.

    Legacy checkpoints without this record retain their previous behavior;
    their missing provenance is not retroactively invented.
    """
    expected=runtime.get('environment_hash')
    if not expected:return
    current,image=current_training_environment()
    if current['content_hash']!=expected:
        raise ContractMismatch('Full resume requires the recorded training environment')
    prior=runtime.get('image_provenance')
    if prior and (not image or image['image_id']!=prior['image_id']):
        raise ContractMismatch('Full resume requires the recorded image identity')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['capture','verify'])
    parser.add_argument('path',type=Path)
    args=parser.parse_args()
    if args.action=='capture':
        snapshot=capture_environment()
        args.path.parent.mkdir(parents=True,exist_ok=True)
        with args.path.open('x',encoding='utf-8') as stream:json.dump(snapshot,stream,ensure_ascii=False,indent=2)
    else:snapshot=verify_environment(args.path)
    print(json.dumps({'environment_hash':snapshot['content_hash'],'packages':len(snapshot['packages'])}))


if __name__=='__main__':main()
