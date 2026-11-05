"""Local transaction primitives. Never traverse links or publish partial trees."""
from pathlib import Path
import hashlib
import json
import os
import shutil
import uuid
import time
from lerobot_sdk.core.errors import ContractMismatch, OperationFailed


def canonical(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def digest(value):
    return 'sha256:'+hashlib.sha256(canonical(value)).hexdigest()


def sha_file(path):
    with Path(path).open('rb') as stream:
        return 'sha256:'+hashlib.file_digest(stream,'sha256').hexdigest()


def atomic_json(path,value):
    path=Path(path)
    temp=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    try:
        with temp.open('xb') as stream:
            stream.write(canonical(value));stream.flush();os.fsync(stream.fileno())
        # Windows readers may briefly hold the destination without FILE_SHARE_DELETE.
        # Preserve atomic replacement and the previous valid file; do not truncate
        # the destination or convert a persistent permission error into success.
        for attempt in range(12):
            try:
                os.replace(temp,path)
                break
            except PermissionError as exc:
                if os.name!='nt' or getattr(exc,'winerror',None) not in (5,32,33) or attempt==11:raise
                time.sleep(.005*(attempt+1))
        if os.name!='nt':
            fd=os.open(path.parent,os.O_RDONLY)
            try:os.fsync(fd)
            finally:os.close(fd)
    finally:
        temp.unlink(missing_ok=True)


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError,ValueError) as exc:
        raise ContractMismatch(f'Cannot read metadata: {Path(path).name}') from exc


def publish_directory(source,target):
    """Atomic rename with bounded Windows sharing-violation retry, never fallback copy."""
    source=Path(source);target=Path(target)
    for attempt in range(20):
        if target.exists() or target.is_symlink():
            raise ContractMismatch('Publication destination already exists')
        try:
            os.rename(source,target)
            return
        except PermissionError as exc:
            if os.name!='nt' or getattr(exc,'winerror',None) not in (5,32,33) or attempt==19:raise
            time.sleep(.02*(attempt+1))


def inventory(root):
    root=Path(root)
    if root.is_symlink():raise ContractMismatch('Dataset root cannot be a symlink')
    files={}
    for p in sorted(root.rglob('*')):
        if p.is_symlink():raise ContractMismatch('Dataset trees cannot contain symlinks')
        if p.is_file() and p.relative_to(root).as_posix()!='manifest.json':
            files[p.relative_to(root).as_posix()]={'bytes':p.stat().st_size,'sha256':sha_file(p)}
    return files


def remove_owned_tree(path,parent):
    path=Path(path);parent=Path(parent).resolve()
    if path.is_symlink() or path.resolve()==parent or not path.resolve().is_relative_to(parent):
        raise ContractMismatch('Refusing cleanup outside owned task directory')
    if path.exists():shutil.rmtree(path)


def disk_check(path,reserve):
    if shutil.disk_usage(path).free<reserve:
        raise OperationFailed(f'Insufficient free space: reserve={reserve} bytes')
