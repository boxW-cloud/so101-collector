"""Whole-dataset verification and atomic local publication, independent of robots."""
from pathlib import Path
import os
import shutil
import uuid
from lerobot_sdk.data.filesystem import atomic_json,read_json,inventory,digest,remove_owned_tree,disk_check,sha_file,publish_directory
from lerobot_sdk.data.contracts import DatasetRef,DatasetInspection,TransferResult,DATA_SCHEMA,SEEED_COMMIT,manifest_joint_contract_ok
from lerobot_sdk.core.async_operation import background
from lerobot_sdk.core.errors import ContractMismatch,StateError,SDKError,OperationFailed
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.types import JOINT_NAMES,named,calibration,integer


def manifest_payload(metadata,files):
    return {**metadata,'manifest_version':1,'schema':DATA_SCHEMA,'format':'lerobot-dataset-v3',
            'status':'LOCAL_READY','lerobot_commit':SEEED_COMMIT,'files':files}


def seal_dataset(root,metadata):
    from lerobot_sdk.adapters.seeed_dataset import validate_native
    validate_native(root,metadata)
    files=inventory(root)
    payload=manifest_payload(metadata,files)
    payload['content_hash']=digest(payload)
    atomic_json(Path(root)/'manifest.json',payload)
    return DatasetRef(Path(root),payload['dataset_id'],payload['version'],payload['content_hash'])


class DatasetManager:
    def inspect(self,ref):
        try:return self._inspect(ref)
        except SDKError:raise
        except Exception as exc:raise ContractMismatch(f'Dataset inspection failed: {exc}') from exc

    def _inspect(self,ref):
        from lerobot_sdk.adapters.seeed_dataset import validate_native
        root=Path(ref.root if isinstance(ref,DatasetRef) else ref).absolute()
        if root.is_symlink():raise ContractMismatch('Linked dataset root is not supported')
        root=root.resolve()
        if not (root/'manifest.json').is_file():raise StateError('Dataset is not finalized; no manifest')
        manifest=read_json(root/'manifest.json')
        if manifest.get('status')!='LOCAL_READY' or manifest.get('schema')!=DATA_SCHEMA:
            raise StateError('Only supported finalized dataset snapshots are accepted')
        if manifest.get('format')!='lerobot-dataset-v3' or manifest.get('manifest_version')!=1 or manifest.get('lerobot_commit')!=SEEED_COMMIT:
            raise ContractMismatch('Unsupported format or upstream version')
        if manifest.get('joint_names')==list(JOINT_NAMES):
            if not isinstance(manifest.get('units'),list) or len(manifest['units'])!=6:
                raise ContractMismatch('Invalid manifest joint contract')
            named(dict(zip(JOINT_NAMES,manifest['units'],strict=True)),'manifest units',units=True)
        elif not manifest_joint_contract_ok(manifest.get('robot_type'),manifest.get('joint_names'),manifest.get('units')):
            raise ContractMismatch('Invalid manifest joint contract')
        calibration(manifest['calibration_hash'])
        integer(manifest['fps'],'fps',minimum=1)
        if (manifest['episodes']!=len(manifest['episode_lengths']) or
            len(manifest['camera_keys'])!=len(set(manifest['camera_keys'])) or
            set(manifest['camera_keys'])!=set(manifest['camera_shapes'])):
            raise ContractMismatch('Manifest counts or camera keys differ')
        for length in manifest['episode_lengths']:integer(length,'episode length',minimum=1)
        expected=manifest['content_hash'];payload={k:v for k,v in manifest.items() if k!='content_hash'}
        if digest(payload)!=expected:raise ContractMismatch('Manifest digest differs')
        if isinstance(ref,DatasetRef) and (ref.content_hash!=expected or ref.dataset_id!=manifest['dataset_id'] or ref.version!=manifest['version']):
            raise ContractMismatch('DatasetRef identity differs')
        files=inventory(root)
        if files!=manifest['files']:raise ContractMismatch('Missing, changed or unexpected dataset files')
        if 'migration' in manifest:
            from lerobot_sdk.data.migration import validate_lineage
            validate_lineage(root,manifest)
        result=validate_native(root,manifest)
        result_ref=DatasetRef(root,manifest['dataset_id'],manifest['version'],expected)
        return DatasetInspection(result_ref,result['episodes'],result['frames'],len(files),
                                 sum(v['bytes'] for v in files.values()),manifest['schema'],tuple(manifest['camera_keys']),True)

    def export(self,ref,target_dir):
        return background(lambda:self._copy(ref,target_dir))

    def copy_to(self,ref,target_dir):
        return background(lambda:self._copy(ref,target_dir))

    def _copy(self,ref,target_dir):
        source=self.inspect(ref).ref
        target=Path(target_dir).expanduser().absolute()
        if target.is_symlink():raise ContractMismatch('Linked export target is forbidden')
        target=target.resolve()
        if target==source.root or target.is_relative_to(source.root) or source.root.is_relative_to(target):
            raise ContractMismatch('Source and destination trees cannot overlap')
        target.parent.mkdir(parents=True,exist_ok=True)
        lock=DeviceLock('dataset-publish:'+str(target));lock.acquire()
        temp=target.parent/('.'+target.name+'.staging-'+uuid.uuid4().hex)
        try:
            if target.exists():
                existing=self.inspect(target).ref
                if existing.content_hash!=source.content_hash:raise ContractMismatch('Destination contains different data')
                return existing
            info=self.inspect(source)
            disk_check(target.parent,info.bytes+1024*1024)
            temp.mkdir()
            manifest=read_json(source.root/'manifest.json')
            for name,entry in manifest['files'].items():
                src=source.root/name;dst=temp/name
                if src.is_symlink() or not src.resolve().is_relative_to(source.root):raise ContractMismatch('Source path changed')
                dst.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(src,dst)
                if sha_file(dst)!=entry['sha256']:raise ContractMismatch('Copied content differs')
            atomic_json(temp/'manifest.json',manifest)
            self.inspect(temp)
            # Recheck the source after copying; no torn snapshot can become READY.
            self.inspect(source)
            if target.exists():raise ContractMismatch('Destination appeared while publishing')
            publish_directory(temp,target)
            return DatasetRef(target,source.dataset_id,source.version,source.content_hash)
        finally:
            remove_owned_tree(temp,target.parent)
            lock.release()

    def upload(self,ref,sink):
        """Optional sink; plugin owns storage, credentials and retry policy."""
        from lerobot_sdk.data.transfer import upload
        return upload(self,ref,sink)

    def migrate(self,ref,target_dir,converter,*,parameters=None):
        """Run a trusted converter on a private copy and publish a new version."""
        from lerobot_sdk.data.migration import migrate
        return migrate(self,ref,target_dir,converter,parameters=parameters)
