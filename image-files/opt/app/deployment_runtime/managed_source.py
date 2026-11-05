"""A registered, owned read-only leader process with observed startup evidence.

Only the fixed native producer is accepted. Its present implementation is a
Windows COM16 bench reader; this is not a generic Linux serial driver. A device
deployment must provide its trusted configuration and local input receiver.
"""
import ipaddress
import importlib
import json
import math
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit
from filelock import FileLock, Timeout as LockTimeout

from .store import Fault

ALLOWED_MODULES={'integration.native_collection_producer','integration.leader.native_collection_producer'}
CONFIG_FIELDS={'module','root','reference','profile','receiver_url','seconds'}


def _windows_process_rows():
    """Read Windows parent relationships; never terminate by a process name."""
    import ctypes
    from ctypes import wintypes
    class Entry(ctypes.Structure):
        _fields_=[('dwSize',wintypes.DWORD),('cntUsage',wintypes.DWORD),('th32ProcessID',wintypes.DWORD),
                  ('th32DefaultHeapID',ctypes.c_size_t),('th32ModuleID',wintypes.DWORD),('cntThreads',wintypes.DWORD),
                  ('th32ParentProcessID',wintypes.DWORD),('pcPriClassBase',wintypes.LONG),
                  ('dwFlags',wintypes.DWORD),('szExeFile',wintypes.WCHAR*260)]
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.CreateToolhelp32Snapshot.argtypes=[wintypes.DWORD,wintypes.DWORD];kernel.CreateToolhelp32Snapshot.restype=wintypes.HANDLE
    kernel.Process32FirstW.argtypes=[wintypes.HANDLE,ctypes.POINTER(Entry)];kernel.Process32FirstW.restype=wintypes.BOOL
    kernel.Process32NextW.argtypes=kernel.Process32FirstW.argtypes;kernel.Process32NextW.restype=wintypes.BOOL
    kernel.CloseHandle.argtypes=[wintypes.HANDLE];kernel.CloseHandle.restype=wintypes.BOOL
    handle=kernel.CreateToolhelp32Snapshot(2,0)
    if handle==ctypes.c_void_p(-1).value:raise ctypes.WinError(ctypes.get_last_error())
    rows={};entry=Entry();entry.dwSize=ctypes.sizeof(entry)
    try:
        found=kernel.Process32FirstW(handle,ctypes.byref(entry))
        if not found and ctypes.get_last_error()!=18:raise ctypes.WinError(ctypes.get_last_error())
        while found:
            rows[int(entry.th32ProcessID)]=int(entry.th32ParentProcessID)
            found=kernel.Process32NextW(handle,ctypes.byref(entry))
            if not found and ctypes.get_last_error()!=18:raise ctypes.WinError(ctypes.get_last_error())
        return rows
    finally:kernel.CloseHandle(handle)


def _windows_process_identity(pid,handle=None):
    """PID plus exact creation time prevents adopting a reused Windows PID.

    The retained Popen handle can inspect a launcher after it exits. An absent
    PID returns None; access/probe failures raise rather than mean 'stopped'.
    """
    import ctypes
    from ctypes import wintypes
    if type(pid) is not int or pid<=0:raise ValueError('Positive process ID required')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];kernel.OpenProcess.restype=wintypes.HANDLE
    kernel.GetProcessTimes.argtypes=[wintypes.HANDLE,*([ctypes.POINTER(wintypes.FILETIME)]*4)];kernel.GetProcessTimes.restype=wintypes.BOOL
    kernel.WaitForSingleObject.argtypes=[wintypes.HANDLE,wintypes.DWORD];kernel.WaitForSingleObject.restype=wintypes.DWORD
    kernel.CloseHandle.argtypes=[wintypes.HANDLE];kernel.CloseHandle.restype=wintypes.BOOL
    owned_handle=handle is None
    if owned_handle:
        handle=kernel.OpenProcess(0x1000|0x100000,False,pid)
        if not handle:
            error=ctypes.get_last_error()
            if error in (87,1168):return None
            raise ctypes.WinError(error)
    else:handle=int(handle)
    created,exited,cpu_kernel,cpu_user=(wintypes.FILETIME() for _ in range(4))
    try:
        if not kernel.GetProcessTimes(handle,ctypes.byref(created),ctypes.byref(exited),ctypes.byref(cpu_kernel),ctypes.byref(cpu_user)):
            raise ctypes.WinError(ctypes.get_last_error())
        birth=(created.dwHighDateTime<<32)|created.dwLowDateTime
        exit_tick=(exited.dwHighDateTime<<32)|exited.dwLowDateTime
        wait=kernel.WaitForSingleObject(handle,0)
        if wait not in (0,258):raise ctypes.WinError(ctypes.get_last_error())
        return {'pid':pid,'parent_pid':_windows_process_rows().get(pid) if owned_handle and wait==258 else None,
                'created_filetime':birth,'created_unix':birth/10_000_000-11644473600,
                'exit_unix':exit_tick/10_000_000-11644473600 if exit_tick else None,'alive':wait==258}
    finally:
        if owned_handle:kernel.CloseHandle(handle)


def bounded_timeout(value):
    if type(value) not in (int,float) or not math.isfinite(value) or not 0<value<=10:
        raise Fault('INVALID_TELEOP_REQUEST','Teleoperation timeout must be finite and within (0, 10] seconds')
    return float(value)


class ManagedTeleopSource:
    def __init__(self,config,*,input_root,profile_hash):
        if not isinstance(config,dict) or set(config)-CONFIG_FIELDS:
            raise Fault('TELEOP_CONFIGURATION','Only registered producer module and structured arguments are allowed')
        if config.get('module') not in ALLOWED_MODULES:
            raise Fault('TELEOP_CONFIGURATION','Producer module is not allowlisted')
        self.config=dict(config);self.root=self._absolute('root')
        self.reference=self._absolute('reference');self.profile=self._absolute('profile')
        self.input_root=Path(input_root).resolve();self.profile_hash=profile_hash
        url=config.get('receiver_url','');parsed=urlsplit(url)
        try:local=parsed.hostname in ('localhost','host.docker.internal') or ipaddress.ip_address(parsed.hostname).is_private
        except (ValueError,TypeError):local=False
        if parsed.scheme!='http' or not local or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('','/'):
            raise Fault('TELEOP_CONFIGURATION','A registered private HTTP input receiver is required')
        try:port=parsed.port
        except ValueError:raise Fault('TELEOP_CONFIGURATION','Invalid receiver port')
        if port is None or not 1<=port<=65535:raise Fault('TELEOP_CONFIGURATION','Explicit receiver port required')
        seconds=config.get('seconds',1200)
        if type(seconds) is not int or not 1<=seconds<=86400:raise Fault('TELEOP_CONFIGURATION','Producer lifetime must be 1..86400 seconds')
        self.seconds=seconds;self.receiver_url=url.rstrip('/');self.process=None;self.log=None
        self.started_at=None;self.started_unix=None;self.previous_sequence=-1;self.instance=None;self.lock=threading.RLock();self.source_lock=None
        self.launcher_identity=None;self.worker_identity=None;self.child_identities={}

    def _absolute(self,name):
        value=self.config.get(name)
        if not isinstance(value,str) or not Path(value).is_absolute():raise Fault('TELEOP_CONFIGURATION','Registered absolute '+name+' path required')
        path=Path(value).resolve()
        if name!='root' and not path.is_file():raise Fault('TELEOP_CONFIGURATION','Registered '+name+' file missing')
        return path

    def _packet(self):
        try:
            packet=json.loads((self.input_root/'input.json').read_text())
            now=time.monotonic();received=packet['received_monotonic'];expires=packet['expires_monotonic']
            if type(packet['sequence']) is not int or type(received) not in (int,float) or type(expires) not in (int,float):return None
            if not math.isfinite(received) or not math.isfinite(expires) or not received<=now<expires:return None
            if packet.get('profile_hash')!=self.profile_hash:return None
            positions=packet['native_positions']
            if set(positions)!=set('123456') or any(type(v) is not int or not 0<=v<=4095 for v in positions.values()):return None
            return packet
        except (OSError,ValueError,KeyError,TypeError):return None

    def _receipt(self,packet):
        return {'started':True,'managed':True,'source_instance':self.instance,'pid':self.worker_identity['pid'],
                'launcher_pid':self.process.pid,'process_identity_verified':self.worker_identity['verification'],
                'sequence':packet['sequence'],'profile_hash':packet['profile_hash'],
                'received_monotonic':packet['received_monotonic'],'expires_monotonic':packet['expires_monotonic']}

    def _unlock(self):
        if self.source_lock:self.source_lock.release();self.source_lock=None

    def _previous_sequence(self):
        # Expired or future-dated old packets still identify the old input.
        try:
            sequence=json.loads((self.input_root/'input.json').read_text())['sequence']
            return sequence if type(sequence) is int else -1
        except (OSError,ValueError,KeyError,TypeError):return -1

    def _launcher_record(self):
        if sys.platform!='win32' or self.process is None:return None
        record=_windows_process_identity(self.process.pid,getattr(self.process,'_handle',None))
        if record:self.launcher_identity=record
        return record or self.launcher_identity

    def _identify_worker(self,status):
        if status.get('source_instance')!=self.instance or type(status.get('pid')) is not int:return False
        pid=status['pid']
        if self.worker_identity and self.worker_identity['pid']!=pid:return False
        if pid==self.process.pid:
            if self.process.poll() is not None:return False
            self.worker_identity={'pid':pid,'verification':'popen_pid'}
            return True
        if sys.platform!='win32' or status.get('parent_pid')!=self.process.pid:return False
        parent=self._launcher_record();worker=_windows_process_identity(pid)
        if not parent or not worker or not worker['alive'] or worker['parent_pid']!=self.process.pid:return False
        if self.worker_identity and self.worker_identity.get('created_filetime')!=worker['created_filetime']:return False
        birth=worker['created_unix'];stamp=status.get('updated_unix')
        if (type(stamp) not in (int,float) or not math.isfinite(stamp) or
                not self.started_unix-.1<=birth<=stamp+.1 or birth<parent['created_unix'] or
                (parent.get('exit_unix') is not None and birth>parent['exit_unix'])):return False
        self.worker_identity={**worker,'verification':'windows_child_lineage_and_birth'}
        self.child_identities[pid]=dict(worker)
        return True

    def _owned_streaming(self):
        try:
            status=json.loads((self.root/'producer-status.json').read_text())
            stamp=status['updated_unix'];frames=status['frames']
            return (self._identify_worker(status)
                    and status.get('state')=='STREAMING' and type(frames) is int and frames>0
                    and type(stamp) in (int,float) and math.isfinite(stamp)
                    and self.started_unix<=stamp<=time.time() and time.time()-stamp<2.5)
        except (OSError,ValueError,KeyError,TypeError):return False

    def _children_alive(self):
        """Also find launcher children not yet able to publish their marker."""
        if sys.platform!='win32':return False
        parent=self._launcher_record()
        if parent:
            for pid,ppid in _windows_process_rows().items():
                if ppid!=self.process.pid:continue
                child=_windows_process_identity(pid)
                if (child and child['created_unix']>=max(parent['created_unix'],self.started_unix-.1) and
                        (parent.get('exit_unix') is None or child['created_unix']<=parent['exit_unix'])):
                    self.child_identities[pid]=child
        alive=False
        for pid,old in self.child_identities.items():
            current=_windows_process_identity(pid)
            # A newly reused PID is not our old worker and is never killed.
            if current and current['created_filetime']==old['created_filetime'] and current['alive']:alive=True
        return alive

    def start(self,timeout_s):
        timeout_s=bounded_timeout(timeout_s)
        with self.lock:
            if self.process and (self.process.poll() is None or self._children_alive()):
                packet=self._packet()
                if packet and self._owned_streaming() and packet['received_monotonic']>=self.started_at and packet['sequence']>self.previous_sequence:return self._receipt(packet)
                raise Fault('TELEOP_START_UNCONFIRMED','Owned producer has no fresh input')
            self.root.mkdir(parents=True,exist_ok=True)
            self.source_lock=FileLock(str(self.root/'managed-source.lock'))
            try:self.source_lock.acquire(timeout=0)
            except LockTimeout:
                self.source_lock=None
                raise Fault('SOURCE_BUSY','Another managed producer already owns this input source')
            status=self.root/'producer-status.json'
            if status.exists():
                try:old=json.loads(status.read_text())
                except (OSError,ValueError):old={}
                if old.get('state') in ('STARTING','STREAMING') and 0<=time.time()-old.get('updated_unix',0)<2:
                    self._unlock()
                    raise Fault('SOURCE_BUSY','Another producer reports active input; do not take it over')
            self.previous_sequence=self._previous_sequence()
            self.started_at=time.monotonic();self.started_unix=time.time();self.instance=uuid.uuid4().hex
            self.launcher_identity=None;self.worker_identity=None;self.child_identities={}
            # This flag belongs to the allowlisted producer, not arbitrary data.
            (self.root/'producer-stop.request').unlink(missing_ok=True)
            if self.log:self.log.close()
            self.log=(self.root/('managed-source-'+self.instance+'.log')).open('ab')
            command=[sys.executable,'-m','deployment_runtime.managed_source','--source-instance',self.instance,
                     '--producer-module',self.config['module'],'--','--root',str(self.root),'--reference',str(self.reference),
                     '--profile',str(self.profile),'--url',self.receiver_url,'--seconds',str(self.seconds)]
            options={'shell':False,'stdin':subprocess.DEVNULL,'stdout':self.log,'stderr':subprocess.STDOUT}
            if sys.platform=='win32':options['creationflags']=subprocess.CREATE_NO_WINDOW
            try:self.process=subprocess.Popen(command,**options)
            except Exception as exc:
                self.log.close();self.log=None;self._unlock()
                raise Fault('TELEOP_START_FAILED','Registered input process could not start: '+type(exc).__name__) from exc
            end=time.monotonic()+timeout_s
            while time.monotonic()<end:
                if self.process.poll() is not None and sys.platform!='win32':break
                packet=self._packet()
                if packet and self._owned_streaming() and packet['received_monotonic']>=self.started_at and packet['sequence']>self.previous_sequence:return self._receipt(packet)
                time.sleep(min(.01,max(0,end-time.monotonic())))
            self.stop(min(1,timeout_s))
            raise Fault('TELEOP_START_UNCONFIRMED','Newly started producer did not provide fresh input; source log retained')

    def stop(self,timeout_s):
        timeout_s=bounded_timeout(timeout_s)
        with self.lock:
            if self.process is None:
                self._unlock()
                return {'stopped':True,'managed':True,'process_started':False}
            (self.root/'producer-stop.request').write_text(self.instance or '',encoding='ascii')
            end=time.monotonic()+timeout_s
            try:
                # A marker can arrive after startup failed. Track a proven child
                # even if its launcher has already exited; no STREAMING claim.
                try:self._identify_worker(json.loads((self.root/'producer-status.json').read_text()))
                except (OSError,ValueError,KeyError,TypeError):pass
                if self.process.poll() is None:
                    try:self.process.wait(timeout=timeout_s/2)
                    except subprocess.TimeoutExpired:
                        # Terminating only a venv launcher would orphan its COM
                        # owner. Leave every live child to the normal stop flag.
                        if not self._children_alive():
                            self.process.terminate()
                            try:self.process.wait(timeout=max(0,(end-time.monotonic())/2))
                            except subprocess.TimeoutExpired:
                                self.process.kill()
                while time.monotonic()<end:
                    if self.process.poll() is not None and not self._children_alive():break
                    time.sleep(min(.02,max(0,end-time.monotonic())))
                if self.process.poll() is None or self._children_alive():
                    raise Fault('TELEOP_STOP_UNCONFIRMED','Launcher or verified producer is still active; stop flag and ownership retained')
            except (OSError,ValueError,KeyError,TypeError) as exc:
                raise Fault('TELEOP_STOP_UNCONFIRMED','Process identity/exit could not be verified; ownership retained') from exc
            if self.log:self.log.close();self.log=None
            self._unlock()
            return {'stopped':True,'managed':True,'source_instance':self.instance,'exit_code':self.process.poll()}


def run_registered_producer(module_name,source_instance,args):
    """Child-side identity marker without changing the read-only driver itself."""
    if module_name not in ALLOWED_MODULES or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',source_instance):
        raise Fault('TELEOP_CONFIGURATION','Invalid registered producer identity')
    module=importlib.import_module(module_name)
    original=module.publish_status
    def publish(root,status):
        return original(root,{**status,'source_instance':source_instance,'pid':os.getpid(),'parent_pid':os.getppid()})
    module.publish_status=publish;old_args=sys.argv
    try:
        sys.argv=[module_name,*args]
        module.main()
    finally:sys.argv=old_args;module.publish_status=original


if __name__=='__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--producer-module',required=True);parser.add_argument('--source-instance',required=True)
    parser.add_argument('producer_args',nargs=argparse.REMAINDER)
    arguments=parser.parse_args()
    tail=arguments.producer_args[1:] if arguments.producer_args[:1]==['--'] else arguments.producer_args
    run_registered_producer(arguments.producer_module,arguments.source_instance,tail)
