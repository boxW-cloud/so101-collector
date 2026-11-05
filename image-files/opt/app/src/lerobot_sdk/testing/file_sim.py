"""Same-host file IPC test adapter. Not a public remote-control protocol.

The SDK and Isaac harness share Linux's monotonic clock and a private directory.
Do not use this across machines, trust domains, or physical robot controllers.
"""
from pathlib import Path
import json,time,uuid
import numpy as np
from lerobot_sdk.control.backend import BackendSnapshot
from lerobot_sdk.core.types import ImageFrame
from lerobot_sdk.core.errors import BusyError,ContractMismatch,DeviceDisconnected,OperationFailed

class FileSimulationBackend:
    kind='simulation'
    def __init__(self,directory,*,calibration_hash):
        self.directory=Path(directory).resolve()
        self.resource_id='isaac-test:'+str(self.directory)
        self.client=uuid.uuid4().hex;self._seq=0;self._generation=0;self.expected_hash=calibration_hash
    def _rpc(self,op,timeout_s,**values):
        self._seq+=1
        request={'id':self.client+':'+str(self._seq),'client':self.client,'sequence':self._seq,
                 'generation':self._generation,'deadline_ns':time.monotonic_ns()+int(timeout_s*1e9),
                 'operation':op,'calibration_hash':self.expected_hash,**values}
        temp=self.directory/(self.client+'.tmp')
        temp.write_text(json.dumps(request),encoding='utf-8');temp.replace(self.directory/'request.json')
        end=time.monotonic()+timeout_s
        while time.monotonic()<end:
            try:
                response=json.loads((self.directory/'response.json').read_text())
                if response.get('id')==request['id']:
                    if response.get('error'):
                        error=BusyError if response.get('code')=='busy' else OperationFailed
                        raise error(response['error'])
                    return response
            except (FileNotFoundError,json.JSONDecodeError):pass
            time.sleep(.002)
        raise TimeoutError(f'Isaac {op} missed the IO deadline')
    def connect(self,timeout_s):
        if not (self.directory/'ready.json').exists():raise DeviceDisconnected('Isaac test harness not ready')
        data=json.loads((self.directory/'ready.json').read_text())
        if data['calibration_hash']!=self.expected_hash:raise ContractMismatch('Simulator contract mismatch')
        self._rpc('connect',timeout_s)
    def sample(self,timeout_s):
        response=self._rpc('sample',timeout_s)
        images={}
        for key,info in response['images'].items():
            filename=info['filename']
            if Path(filename).name!=filename:raise ContractMismatch('Image must be a local basename')
            rgb=np.load(self.directory/filename,allow_pickle=False)
            images[key]=ImageFrame(rgb,info['captured_monotonic_ns'],0,key)
        return BackendSnapshot(response['captured_monotonic_ns'],response['positions'],images)
    def send(self,positions,timeout_s):return self._rpc('send',timeout_s,positions=dict(positions))['sent']
    def hold(self,positions,timeout_s):
        self._generation+=1;self._rpc('hold',timeout_s)
    def close(self,*,hold,timeout_s):
        if not hold:raise ContractMismatch('Simulation close must hold')
        self._generation+=1;self._rpc('close',timeout_s)
