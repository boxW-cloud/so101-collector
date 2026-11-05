from pathlib import Path
import hashlib
import os
from lerobot_sdk.core.errors import BusyError

class DeviceLock:
    def __init__(self,resource_id,root=None):
        root=Path(root) if root else Path.home()/'.cache/lerobot_sdk/locks'
        root.mkdir(parents=True,exist_ok=True)
        self.path=root/(hashlib.sha256(resource_id.encode()).hexdigest()+'.lock');self.file=None
    def acquire(self):
        f=self.path.open('a+b')
        if self.path.stat().st_size==0:f.write(b'0');f.flush()
        f.seek(0)
        try:
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(f.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as e:f.close();raise BusyError('Resource already owned by another session/process') from e
        self.file=f
    def release(self):
        if self.file is None:return
        try:
            self.file.seek(0)
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(self.file,fcntl.LOCK_UN)
        finally:self.file.close();self.file=None
