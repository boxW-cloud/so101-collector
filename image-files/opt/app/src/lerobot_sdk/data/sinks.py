"""Optional filesystem sink and retry decorator. Core SDK has no cloud credentials."""
from pathlib import Path
import os
import time
from lerobot_sdk.data.filesystem import atomic_json, read_json, sha_file, publish_directory
from lerobot_sdk.data.contracts import TransferProgress, TransferResult
from lerobot_sdk.data.manager import DatasetManager
from lerobot_sdk.core.errors import ContractMismatch
from lerobot_sdk.core.locking import DeviceLock
from lerobot_sdk.core.types import integer, finite


class RetryingDatasetSink:
    """Retry only transport/storage OSError; a resumable inner sink owns offsets."""
    def __init__(self,sink,*,max_attempts=3,delay_s=.25):
        integer(max_attempts,'max_attempts',minimum=1)
        finite(delay_s,'delay_s',nonnegative=True)
        self.sink=sink;self.max_attempts=max_attempts;self.delay_s=delay_s

    def upload(self,snapshot,progress):
        latest=None
        def report(value):
            nonlocal latest
            latest=value;progress(value)
        for attempt in range(self.max_attempts):
            try:return self.sink.upload(snapshot,report)
            except OSError:
                if attempt+1==self.max_attempts:raise
                if latest is not None:progress(TransferProgress(latest.completed_bytes,latest.total_bytes,'retrying'))
                time.sleep(self.delay_s)


class LocalDirectorySink:
    """Resumable local/mounted-directory receiver; no implicit network connection.

    Partial bytes remain in a hash-bound private directory until retry succeeds.
    Each existing prefix is compared to the source before reuse. A final native
    inspection precedes atomic publication; conflicting destinations are never replaced.
    """
    def __init__(self,target_dir,*,chunk_bytes=1024*1024,inspector=None):
        integer(chunk_bytes,'chunk_bytes',minimum=1)
        self.target_dir=Path(target_dir).expanduser().absolute();self.chunk_bytes=chunk_bytes
        self.inspector=inspector

    def upload(self,snapshot,progress):
        manager=self.inspector or DatasetManager();info=manager.inspect(snapshot);source=info.ref
        if self.target_dir.is_symlink():raise ContractMismatch('Linked receiver target is forbidden')
        target=self.target_dir.resolve()
        if target==source.root or target.is_relative_to(source.root) or source.root.is_relative_to(target):
            raise ContractMismatch('Source and receiver cannot overlap')
        target.parent.mkdir(parents=True,exist_ok=True)
        lock=DeviceLock('dataset-publish:'+str(target));lock.acquire()
        stage=target.parent/('.'+target.name+'.upload-'+source.content_hash.removeprefix('sha256:'))
        total=info.bytes+(source.root/'manifest.json').stat().st_size
        owned=False
        try:
            if target.exists():
                existing=manager.inspect(target)
                if existing.ref.content_hash!=source.content_hash:raise ContractMismatch('Receiver contains different data')
                progress(TransferProgress(total,total,'verifying'))
                return TransferResult(str(target),source.content_hash)
            identity={'content_hash':source.content_hash,'dataset_id':source.dataset_id,'version':source.version}
            if stage.exists() or stage.is_symlink():
                if stage.is_symlink() or any(p.is_symlink() for p in stage.rglob('*')):
                    raise ContractMismatch('Linked partial transfer is forbidden')
                if read_json(stage/'.transfer.json')!=identity:raise ContractMismatch('Partial transfer belongs to another version')
            else:
                stage.mkdir();atomic_json(stage/'.transfer.json',identity)
            owned=True
            manifest=read_json(source.root/'manifest.json')
            completed=0
            for name,entry in manifest['files'].items():
                src=source.root/name;dst=stage/name
                if not dst.resolve().is_relative_to(stage):raise ContractMismatch('Receiver path escaped staging')
                dst.parent.mkdir(parents=True,exist_ok=True)
                offset=dst.stat().st_size if dst.exists() else 0
                if offset>entry['bytes']:raise ContractMismatch('Partial file is larger than source')
                with src.open('rb') as incoming:
                    if offset:
                        with dst.open('rb') as partial:
                            remaining=offset
                            while remaining:
                                n=min(remaining,self.chunk_bytes)
                                if incoming.read(n)!=partial.read(n):raise ContractMismatch('Partial transfer prefix differs')
                                remaining-=n
                        completed+=offset
                        progress(TransferProgress(completed,total,'resuming'))
                    with dst.open('ab') as outgoing:
                        while block:=incoming.read(self.chunk_bytes):
                            outgoing.write(block);outgoing.flush();os.fsync(outgoing.fileno())
                            completed+=len(block)
                            progress(TransferProgress(completed,total,'transferring'))
                if sha_file(dst)!=entry['sha256']:raise ContractMismatch('Transferred file hash differs')
            progress(TransferProgress(completed,total,'verifying'))
            manager.inspect(source)
            # The bookkeeping file is not dataset content. Restore it on failure for an explicit retry.
            (stage/'.transfer.json').unlink()
            atomic_json(stage/'manifest.json',manifest)
            manager.inspect(stage)
            if target.exists():raise ContractMismatch('Receiver appeared during transfer')
            publish_directory(stage,target)
            return TransferResult(str(target),source.content_hash)
        finally:
            try:
                if owned and stage.exists() and not stage.is_symlink():
                    # Do not touch an unrecognised tree after failing the identity check.
                    marker=stage/'.transfer.json'
                    if ('identity' in locals() and (not marker.exists() or read_json(marker)==identity)
                        and not any(p.is_symlink() for p in stage.rglob('*'))):
                        (stage/'manifest.json').unlink(missing_ok=True)
                        atomic_json(marker,identity)
            finally:lock.release()
