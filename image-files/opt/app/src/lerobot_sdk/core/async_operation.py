"""Non-cancellable transactions: wait timeout never cancels a disk commit."""
import threading
import uuid
from lerobot_sdk.core.operation import OperationState, OperationStatus
from lerobot_sdk.core.errors import SDKError, OperationFailed, WaitTimeout
from lerobot_sdk.core.types import finite


class Operation:
    def __init__(self):
        self.id = uuid.uuid4().hex
        self._done = threading.Event()
        self._lock = threading.Lock()
        self._result = self._error = None
        self._progress = None

    def progress(self):
        """Latest transfer progress, or None for operations without progress."""
        with self._lock:
            return self._progress

    def _set_progress(self, value):
        with self._lock:
            if not self._done.is_set():
                self._progress = value

    def status(self):
        with self._lock:
            state = (OperationState.FAILED if self._error else OperationState.SUCCEEDED) if self._done.is_set() else OperationState.RUNNING
            return OperationStatus(self.id, state, str(self._error) if self._error else None)

    def wait(self, timeout_s=None):
        if timeout_s is not None:
            timeout_s = finite(timeout_s, 'wait timeout', nonnegative=True)
        if not self._done.wait(timeout_s):
            raise WaitTimeout('Wait expired; transaction continues')
        if self._error:
            raise self._error
        return self._result

    def _finish(self, result=None, error=None):
        with self._lock:
            if self._done.is_set():
                return
            self._result = result
            self._error = error if error is None or isinstance(error, SDKError) else OperationFailed(str(error))
            self._done.set()


def background(function, *, operation=None):
    operation = operation if operation is not None else Operation()
    def run():
        try:
            operation._finish(function())
        except Exception as exc:
            operation._finish(error=exc)
    threading.Thread(target=run, name='DatasetTransaction', daemon=True).start()
    return operation
