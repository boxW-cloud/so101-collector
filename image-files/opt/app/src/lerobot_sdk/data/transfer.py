"""Transfer adapter contract with bounded observable progress; no network policy."""
import inspect
from typing import Protocol, Callable
from lerobot_sdk.core.async_operation import Operation, background
from lerobot_sdk.data.contracts import DatasetRef, TransferProgress, TransferResult
from lerobot_sdk.core.errors import ContractMismatch


class DatasetSink(Protocol):
    def upload(self, snapshot: DatasetRef, progress: Callable[[TransferProgress], None]) -> TransferResult: ...


def upload(manager,ref,sink):
    operation=Operation()
    def transfer():
        inspected=manager.inspect(ref); checked=inspected.ref
        total=inspected.bytes+(checked.root/'manifest.json').stat().st_size
        operation._set_progress(TransferProgress(0,total,'starting'))
        def progress(value):
            if (not isinstance(value,TransferProgress) or type(value.completed_bytes) is not int or
                type(value.total_bytes) is not int or value.total_bytes!=total or
                not 0<=value.completed_bytes<=total or value.phase not in ('transferring','resuming','verifying','retrying')):
                raise ContractMismatch('Invalid sink progress')
            operation._set_progress(value)
        # Compatibility is selected before calling; a TypeError inside a sink must not cause a second upload.
        try:inspect.signature(sink.upload).bind(checked,progress)
        except TypeError:result=sink.upload(checked)
        else:result=sink.upload(checked,progress)
        if (not isinstance(result,TransferResult) or result.content_hash!=checked.content_hash or
            not isinstance(result.storage_ref,str) or not result.storage_ref.strip()):
            raise ContractMismatch('Sink must confirm a storage reference and the uploaded content hash')
        manager.inspect(checked)
        operation._set_progress(TransferProgress(total,total,'complete'))
        return result
    return background(transfer,operation=operation)
