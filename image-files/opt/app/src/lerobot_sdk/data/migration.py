"""Versioned local conversions. Converters are trusted installed Python functions."""
import inspect
import json
import os
from pathlib import Path
import shutil
import uuid
from lerobot_sdk.data.filesystem import atomic_json, canonical, digest, read_json, sha_file, remove_owned_tree, publish_directory
from lerobot_sdk.core.async_operation import background
from lerobot_sdk.data.contracts import DatasetRef
from lerobot_sdk.data.manager import seal_dataset
from lerobot_sdk.core.errors import ContractMismatch
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.types import calibration


def validate_lineage(root, manifest):
    lineage=manifest['migration']
    if lineage!=read_json(root/'meta/sdk-migration.json') or lineage.get('lineage_version')!=1:
        raise ContractMismatch('Migration record differs or is unsupported')
    source=lineage['source']; program=lineage['converter']
    for key in ('dataset_id','version','schema','lerobot_commit'):
        if not isinstance(source[key],str) or not source[key]:raise ContractMismatch('Missing source identity')
    calibration(source['content_hash']);calibration(program['sha256'])
    if source['version']==manifest['version']:raise ContractMismatch('Migration must create a new version')
    if not isinstance(lineage['parameters'],dict) or digest(lineage['parameters'])!=lineage['parameters_hash']:
        raise ContractMismatch('Migration parameters hash differs')
    for key in ('module','qualname','filename'):
        if not isinstance(program[key],str) or not program[key]:raise ContractMismatch('Missing converter identity')


def migrate(manager,ref,target_dir,converter,*,parameters=None):
    # Copy JSON inputs before the background thread; never retain caller-owned mutable parameters.
    params=json.loads(canonical({} if parameters is None else parameters))
    if not isinstance(params,dict):raise ContractMismatch('Conversion parameters must be a JSON object')
    if not inspect.isfunction(converter) or not inspect.getsourcefile(converter):
        raise ContractMismatch('Converter must be a Python function with a source file')
    program_path=Path(inspect.getsourcefile(converter)).resolve()
    program_hash=sha_file(program_path)

    def convert():
        source=manager.inspect(ref).ref
        target=Path(target_dir).expanduser().absolute()
        if target.is_symlink():raise ContractMismatch('Linked migration target is forbidden')
        target=target.resolve()
        if target==source.root or target.is_relative_to(source.root) or source.root.is_relative_to(target):
            raise ContractMismatch('Source and migration target cannot overlap')
        target.parent.mkdir(parents=True,exist_ok=True)
        lock=DeviceLock('dataset-publish:'+str(target));lock.acquire()
        work=target.parent/('.'+target.name+'.staging-'+uuid.uuid4().hex)
        try:
            if target.exists():raise ContractMismatch('Migration destination already exists')
            manager._copy(source,work)
            metadata=read_json(work/'manifest.json')
            (work/'manifest.json').unlink()  # A conversion workspace is never LOCAL_READY.
            (work/'meta/sdk-migration.json').unlink(missing_ok=True)
            if sha_file(program_path)!=program_hash:raise ContractMismatch('Converter source changed')
            updates=converter(work,json.loads(canonical(params)))
            if not isinstance(updates,dict):raise ContractMismatch('Converter must return metadata updates as a dict')
            allowed={'fps','episodes','episode_lengths','camera_keys','camera_shapes','task_labels','source_episode_hashes'}
            if set(updates)-allowed:raise ContractMismatch('Converter attempted to replace protected identity/contract fields')
            if (work/'manifest.json').exists():raise ContractMismatch('Converter must not publish a manifest')
            if sha_file(program_path)!=program_hash:raise ContractMismatch('Converter source changed during conversion')
            lineage={'lineage_version':1,
                'source':{k:metadata[k] for k in ('dataset_id','version','content_hash','schema','lerobot_commit')},
                'converter':{'module':converter.__module__,'qualname':converter.__qualname__,
                             'filename':program_path.name,'sha256':program_hash},
                'parameters':params,'parameters_hash':digest(params)}
            for key in ('content_hash','files','migration'):metadata.pop(key,None)
            metadata.update(updates,version=uuid.uuid4().hex,migration=lineage)
            atomic_json(work/'meta/sdk-migration.json',lineage)
            sealed=seal_dataset(work,metadata)
            manager.inspect(sealed);manager.inspect(source)
            if target.exists():raise ContractMismatch('Destination appeared during conversion')
            publish_directory(work,target)
            return DatasetRef(target,sealed.dataset_id,sealed.version,sealed.content_hash)
        finally:
            try:remove_owned_tree(work,target.parent)
            finally:lock.release()
    return background(convert)
