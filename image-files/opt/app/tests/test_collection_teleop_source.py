"""Owned teleoperation startup never treats a preexisting input file as startup."""
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from deployment_runtime.ros_wire import Peer, request


class Adapter:
    def __init__(self): self.calls=[]
    def describe(self): return {'kind':'physical','source':'test_only'}
    def connect(self): self.calls.append('connect')
    def start_teleop(self, timeout_s):
        self.calls.append(('start',timeout_s))
        return {'started':True,'managed':True,'source_instance':'new-input','sequence':2}
    def stop_teleop(self, timeout_s):
        self.calls.append(('stop',timeout_s))
        return {'stopped':True,'managed':True}
    def hold(self): self.calls.append('hold');return {'holding':True,'action_queue_empty':True}
    def calibration(self,*args): return {'fresh':True}


def call(peer, operation, owner='a', payload=None):
    return peer.call(request(operation,payload or {},owner,peer.epoch,'test-key'))


def test_peer_requires_current_owner_and_non_calibration_lease_for_source_start():
    a=Adapter();p=Peer(a,'test-key')
    assert not call(p,'teleop.start')['ok']
    assert call(p,'connect')['ok']
    assert not call(p,'teleop.start',owner='other')['ok']
    assert call(p,'teleop.start',payload={'timeout_s':1})['result']['started']
    assert a.calls==['connect',('start',1)]
    assert call(p,'teleop.stop',payload={'timeout_s':1})['result']['stopped']
    assert call(p,'close')['result']['holding']
    assert call(p,'calibration.connect')['ok']
    assert call(p,'teleop.start')['error']['code']=='READ_ONLY_LEASE'


def test_source_without_registered_start_capability_fails_explicitly():
    a=SimpleNamespace(connect=lambda:None,hold=lambda:{'holding':True,'action_queue_empty':True})
    p=Peer(a,'test-key');assert call(p,'connect')['ok']
    assert call(p,'teleop.start')['error']['code']=='TELEOP_UNAVAILABLE'


def test_peer_rejects_fake_start_receipt_and_client_command_fields():
    a=Adapter();p=Peer(a,'test-key');call(p,'connect')
    assert call(p,'teleop.start',payload={'command':'echo trusted?','timeout_s':1})['error']['code']=='INVALID_TELEOP_REQUEST'
    assert a.calls==['connect']
    a.start_teleop=lambda timeout_s:{'started':False,'managed':True}
    assert call(p,'teleop.start')['error']['code']=='TELEOP_START_UNCONFIRMED'


def test_peer_watchdog_stops_owned_source_before_hold():
    a=Adapter();p=Peer(a,'test-key');call(p,'connect');call(p,'teleop.start')
    p.until=time.monotonic()-1;p.tick()
    assert not p.owner and p.holding
    assert a.calls[-2:]==[('stop',2.0),'hold']


def test_episode_pause_hold_preserves_owned_read_only_source_for_automatic_next():
    a=Adapter();p=Peer(a,'test-key');call(p,'connect');call(p,'teleop.start')
    paused=call(p,'hold')
    assert paused['ok'] and paused['result']['holding'] and paused['result']['action_queue_empty']
    assert p.owner=='a' and p.teleop_active
    assert not any(isinstance(entry,tuple) and entry[0]=='stop' for entry in a.calls)
    assert call(p,'close')['ok']
    assert not p.teleop_active and a.calls[-2:]==[('stop',2.0),'hold']


def test_unconfirmed_source_stop_still_holds_robot_and_retains_fault():
    a=Adapter();p=Peer(a,'test-key');call(p,'connect');call(p,'teleop.start')
    a.stop_teleop=lambda timeout_s:{'stopped':False}
    reply=call(p,'close')
    assert not reply['ok'] and reply['error']['code']=='STOP_UNCONFIRMED'
    assert a.calls[-1]=='hold' and p.fault=='STOP_UNCONFIRMED'


def source_config(tmp_path):
    profile=tmp_path/'profile.json';profile.write_text(json.dumps({'profile_hash':'profile-a'}))
    reference=tmp_path/'reference.json';reference.write_text('{}')
    return {'module':'integration.native_collection_producer','root':str(tmp_path),
            'reference':str(reference),'profile':str(profile),'receiver_url':'http://127.0.0.1:8795','seconds':120}


class Process:
    pid=42
    def __init__(self):self.done=False
    def poll(self):return 0 if self.done else None
    def terminate(self):self.done=True
    def kill(self):self.done=True
    def wait(self,timeout=None):self.done=True;return 0


def test_managed_start_needs_new_sample_after_process_start(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    cfg=source_config(tmp_path);proc=Process();launched=[]
    def popen(command,**kwargs):launched.append((command,kwargs));return proc
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',popen)
    start=time.monotonic()
    old={'sequence':1,'profile_hash':'profile-a','received_monotonic':start-1,
         'expires_monotonic':start+60,'native_positions':dict.fromkeys('123456',2048)}
    (tmp_path/'input.json').write_text(json.dumps(old))
    source=ManagedTeleopSource(cfg,input_root=tmp_path,profile_hash='profile-a')
    with pytest.raises(Exception,match='fresh'):source.start(.05)
    assert proc.done and len(launched)==1 and launched[0][1]['shell'] is False
    assert launched[0][0][1:3]==['-m','deployment_runtime.managed_source']
    assert 'integration.native_collection_producer' in launched[0][0]


def test_managed_source_returns_actual_new_input_and_stops_only_its_child(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    cfg=source_config(tmp_path);proc=Process()
    def popen(command,**kwargs):
        now=time.monotonic();(tmp_path/'input.json').write_text(json.dumps({'sequence':3,'profile_hash':'profile-a',
          'received_monotonic':now,'expires_monotonic':now+5,'native_positions':dict.fromkeys('123456',2048)}))
        (tmp_path/'producer-status.json').write_text(json.dumps({'state':'STREAMING','frames':1,'updated_unix':time.time(),
          'source_instance':source.instance,'pid':proc.pid}))
        return proc
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',popen)
    source=ManagedTeleopSource(cfg,input_root=tmp_path,profile_hash='profile-a')
    started=source.start(.2)
    assert started['started'] and started['sequence']==3 and started['managed'] and started['source_instance']
    stopped=source.stop(.01);assert stopped['stopped'] and proc.done
    assert (tmp_path/'input.json').exists(), 'Source shutdown must preserve existing data'


@pytest.mark.parametrize('change',[{'module':'os'},{'command':['whoami']},{'python':'bad.exe'},
                                 {'receiver_url':'http://attacker.example:8795'},{'seconds':0}])
def test_registered_source_still_enforces_module_and_argument_allowlist(tmp_path,change):
    from deployment_runtime.managed_source import ManagedTeleopSource
    with pytest.raises(Exception):ManagedTeleopSource({**source_config(tmp_path),**change},input_root=tmp_path,profile_hash='profile-a')


def test_native_adapter_default_has_no_automatic_source_capability():
    from deployment_runtime.native_adapter import NativeOwnerAdapter
    adapter=NativeOwnerAdapter({'robot_id':'r','calibration_hash':'h'},owner=object())
    with pytest.raises(Exception,match='configured'):adapter.start_teleop(.1)


def test_ros_backend_exposes_owned_start_and_stop_calls():
    from deployment_runtime.ros_transport import RosBackend
    backend=RosBackend.__new__(RosBackend);seen=[]
    backend.call=lambda op,payload,timeout:seen.append((op,payload,timeout)) or {'started':True,'stopped':True,'managed':True}
    assert backend.start_teleop(.5)['started'] and backend.stop_teleop(.5)['stopped']
    assert [entry[0] for entry in seen]==['teleop.start','teleop.stop']


def test_future_dated_preexisting_packet_is_not_a_new_process_sample(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    cfg=source_config(tmp_path);proc=Process();now=time.monotonic()
    (tmp_path/'input.json').write_text(json.dumps({'sequence':4,'profile_hash':'profile-a','received_monotonic':now+.01,
       'expires_monotonic':now+2,'native_positions':dict.fromkeys('123456',2048)}))
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',lambda *args,**kwargs:proc)
    source=ManagedTeleopSource(cfg,input_root=tmp_path,profile_hash='profile-a')
    with pytest.raises(Exception,match='fresh'):source.start(.06)


def test_process_launch_failure_closes_log_and_reports_same_start_failure(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    def failed(*args,**kwargs):raise OSError('executable unavailable')
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',failed)
    source=ManagedTeleopSource(source_config(tmp_path),input_root=tmp_path,profile_hash='profile-a')
    with pytest.raises(Exception) as error:source.start(.05)
    assert getattr(error.value,'code',None)=='TELEOP_START_FAILED' and source.log is None


def test_independent_adapters_cannot_spawn_two_processes_for_same_source(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    cfg=source_config(tmp_path);proc=Process();launched=[]
    def popen(*args,**kwargs):
        launched.append(args);now=time.monotonic();(tmp_path/'input.json').write_text(json.dumps({'sequence':5,'profile_hash':'profile-a',
           'received_monotonic':now,'expires_monotonic':now+2,'native_positions':dict.fromkeys('123456',2048)}))
        (tmp_path/'producer-status.json').write_text(json.dumps({'state':'STREAMING','frames':1,'updated_unix':time.time(),
          'source_instance':first.instance,'pid':proc.pid}))
        return proc
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',popen)
    first=ManagedTeleopSource(cfg,input_root=tmp_path,profile_hash='profile-a')
    second=ManagedTeleopSource(cfg,input_root=tmp_path,profile_hash='profile-a')
    first.start(.1)
    try:
        with pytest.raises(Exception) as error:second.start(.1)
        assert getattr(error.value,'code',None)=='SOURCE_BUSY' and len(launched)==1
    finally:first.stop(.1)


def test_other_process_new_input_is_not_owned_process_startup_evidence(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import ManagedTeleopSource
    proc=Process()
    def popen(*args,**kwargs):
        now=time.monotonic();(tmp_path/'input.json').write_text(json.dumps({'sequence':9,'profile_hash':'profile-a',
           'received_monotonic':now,'expires_monotonic':now+1,'native_positions':dict.fromkeys('123456',2048)}))
        return proc
    monkeypatch.setattr('deployment_runtime.managed_source.subprocess.Popen',popen)
    source=ManagedTeleopSource(source_config(tmp_path),input_root=tmp_path,profile_hash='profile-a')
    with pytest.raises(Exception,match='fresh'):source.start(.06)
    assert proc.done


def test_fixed_launcher_adds_process_identity_to_actual_producer_status(tmp_path,monkeypatch):
    from deployment_runtime.managed_source import run_registered_producer
    from types import SimpleNamespace
    status={}
    module=SimpleNamespace(publish_status=lambda root,value:status.update(value) or True)
    module.main=lambda:module.publish_status(tmp_path,{'state':'STREAMING','frames':1,'updated_unix':time.time()})
    monkeypatch.setattr('deployment_runtime.managed_source.importlib.import_module',lambda name:module)
    run_registered_producer('integration.native_collection_producer','test-instance',[])
    assert status['state']=='STREAMING' and status['frames']==1 and status['source_instance']=='test-instance'
    assert type(status['pid']) is int and status['pid']>0


def test_fresh_fixture_inspect_proves_empty_peer_queue_without_hold_or_connection():
    from deployment_runtime.ros_wire import FixtureAdapter
    adapter=FixtureAdapter();peer=Peer(adapter,'test-key')
    result=call(peer,'inspect')['result']
    assert result['owner'] is None and result['fault'] is None
    assert result['action_queue_empty'] is True and result['holding'] is False
    assert not adapter.robot.connected and not adapter.sent


def test_get_initial_collection_status_is_idle_using_actual_fresh_peer_inspect(tmp_path):
    import asyncio
    import hashlib
    from aiohttp.test_utils import TestClient,TestServer
    from deployment_runtime.collection_contract import CollectionContract
    from deployment_runtime.http import make_app
    from deployment_runtime.ros_wire import FixtureAdapter
    from deployment_runtime.service import Platform
    from deployment_runtime.store import Principal
    adapter=FixtureAdapter();peer=Peer(adapter,'test-key');robot_id=adapter.config.robot_id
    cfg=dict(adapter.config.__dict__)
    for key in ('units','limits','max_velocity','max_step','tolerance'):cfg[key]=dict(cfg[key])
    cfg['release_pose']=None
    platform=Platform({'role':'collection','root':str(tmp_path)},sessions=SimpleNamespace(close=lambda:[]))
    principal=Principal('alice',('lab',),('admin','operator'))
    platform.register('robots',{'project':'lab','id':robot_id,'revision':'v1','body':{'robot_id':robot_id,
        'kind':'simulation','provenance':'test_fixture','scene_id':'desk','scene_revision':'v1','sdk_config':cfg,
        'camera_shapes':{'front':[12,16,3]},'capabilities':['collection','teleop'],'ros':{'namespace':'/test','domain_id':93}}},principal)
    uploads=SimpleNamespace(start=lambda:None,stop=lambda **kwargs:None)
    contract=CollectionContract(platform,uploads=uploads,target_probe=lambda *args:{'peer':call(peer,'inspect')['result']})
    platform.collection=contract
    async def inspect_status():
        accounts=[{'name':'alice','projects':['lab'],'roles':['admin','operator'],
                   'token_sha256':hashlib.sha256(b'secret').hexdigest()}]
        async with TestClient(TestServer(make_app(platform,accounts))) as client:
            response=await client.get('/collection/status?project=lab&robot_id='+robot_id+'&robot_revision=v1',
                                      headers={'Authorization':'Bearer secret'})
            assert response.status==200,await response.text()
            assert (await response.json())['result']['state']=='IDLE'
    try:
        asyncio.run(inspect_status())
        assert not adapter.robot.connected and not adapter.sent and peer.owner is None
    finally:platform.close()


def test_unverified_external_physical_queue_is_not_declared_empty():
    peer=Peer(Adapter(),'test-key')
    result=call(peer,'inspect')['result']
    assert result['holding'] is False and result['action_queue_empty'] is False


class ReadOnlyOwner:
    def __init__(self,operation=None,connected=True):self.calls=[];self.operation=operation;self.connected=connected
    def call(self,operation,payload):
        self.calls.append(operation)
        assert operation=='state', 'Initial queue query must never send connect or actuator commands'
        return {'mode':'real','connected':self.connected,'device_id':'actual-device','profile_id':'actual-profile',
                'session_id':'actual-session','sampled_at':time.time(),'fault':None,'operation':self.operation}


def physical_adapter(owner):
    from deployment_runtime.native_adapter import NativeOwnerAdapter
    return NativeOwnerAdapter({'robot_id':'physical','calibration_hash':'h','device_id':'actual-device',
            'expected_profile_id':'actual-profile'},owner=owner)


def test_physical_initial_inspect_checks_actual_queue_without_claiming_torque():
    owner=ReadOnlyOwner();peer=Peer(physical_adapter(owner),'test-key')
    result=call(peer,'inspect')['result']
    assert result['holding'] is False and result['owner'] is None and result['action_queue_empty'] is True
    assert result['queue_scope']=='peer_and_native_owner' and owner.calls==['state']
    owner.operation={'id':'external','status':'running'}
    assert call(peer,'inspect')['result']['action_queue_empty'] is False
    owner.operation={'id':'external','status':'queued'}
    assert call(peer,'inspect')['result']['action_queue_empty'] is False


def test_physical_initial_inspect_fails_closed_on_disconnected_external_owner():
    owner=ReadOnlyOwner(connected=False);peer=Peer(physical_adapter(owner),'test-key')
    response=call(peer,'inspect')
    assert response['ok'] is False and response['error']['code']=='DEVICE_MISMATCH'
    assert owner.calls==['state'] and peer.owner is None and not peer.holding


def test_missing_native_operation_field_is_unknown_not_an_empty_queue():
    owner=ReadOnlyOwner();original=owner.call
    def incomplete(operation,payload):
        state=original(operation,payload);state.pop('operation');return state
    owner.call=incomplete;peer=Peer(physical_adapter(owner),'test-key')
    response=call(peer,'inspect')
    assert response['ok'] is False and response['error']['code']=='QUEUE_STATUS_UNAVAILABLE'
    assert owner.calls==['state'] and peer.owner is None and not peer.holding


def test_slow_external_queue_read_does_not_hold_peer_lock_or_block_watchdog():
    import threading
    adapter=Adapter();entered=threading.Event();release=threading.Event()
    def queue_status():entered.set();release.wait(2);return {'action_queue_empty':True}
    adapter.queue_status=queue_status;peer=Peer(adapter,'test-key');call(peer,'connect')
    worker=threading.Thread(target=lambda:call(peer,'inspect'));worker.start()
    assert entered.wait(1)
    peer.until=time.monotonic()-1
    watchdog=threading.Thread(target=peer.tick);watchdog.start();watchdog.join(.2)
    try:assert not watchdog.is_alive() and peer.owner is None and peer.holding
    finally:release.set();worker.join(2);watchdog.join(2)
