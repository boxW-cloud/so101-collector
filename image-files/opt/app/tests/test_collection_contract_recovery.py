"""Business restart reconciliation against actual durable SDK capture journals.

The native writer and SQLite are real; device motion/ROS remain simulation seams.
"""
import json
import threading
import time
from pathlib import Path

import pytest

from deployment_runtime.collection_contract import CollectionContract
from deployment_runtime.service import Platform
from deployment_runtime.sessions import SessionRuntime
from deployment_runtime.store import Fault, Principal
from lerobot_sdk.testing import FakeRobot, simulation_config

pytest.importorskip('lerobot')


class RecordedOutbox:
    def __init__(self):self.rows={}
    def enqueue(self,key,payload):
        if key in self.rows:assert self.rows[key]['payload']==payload
        self.rows[key]={'operation_id':key,'payload':payload,'state':'PENDING'}
        return self.rows[key]
    def start(self):pass
    def stop(self,**kwargs):pass


class CaptureBackend(FakeRobot):
    def leader(self,timeout_s):return {'positions':dict(self.positions)}


class SoftwareRuntime(SessionRuntime):
    def reconcile(self,robot,**kwargs):
        return {'holding':True,'action_queue_empty':True,'safe_to_release':True}


@pytest.fixture
def recovery_system(tmp_path):
    from lerobot_sdk.adapters.seeed_dataset import upstream
    upstream()
    actor=Principal('alice',('lab',),('admin','operator'))
    config=simulation_config('recovery-sim',required_cameras=('front',),max_observation_age_s=2)
    sdk=dict(config.__dict__);sdk['release_pose']=None
    for name in ('units','limits','max_velocity','max_step','tolerance'):sdk[name]=dict(sdk[name])
    service_config={'role':'collection','root':str(tmp_path),'collection_min_free_bytes':1}
    instances=[]
    def create():
        runtime=SoftwareRuntime(tmp_path/'capture',lambda robot:CaptureBackend(resource_id=str(tmp_path),cameras=('front',)))
        platform=Platform(service_config,sessions=runtime)
        platform.register('robots',{'project':'lab','id':'recovery-sim','revision':'v1','body':{
            'robot_id':'recovery-sim','kind':'simulation','scene_id':'desk','scene_revision':'v1',
            'sdk_config':sdk,'camera_shapes':{'front':[12,16,3]},'capabilities':['collection','teleop'],
            'ros':{'namespace':'/recovery','domain_id':193}}},actor)
        outbox=RecordedOutbox()
        contract=CollectionContract(platform,uploads=outbox,target_probe=lambda *args:{'peer':{'owner':None,'fault':None,'holding':True,'action_queue_empty':True}})
        platform.collection=contract;instances.append(platform)
        return contract,runtime,outbox,platform
    payload={'project':'lab','robot_id':'recovery-sim','robot_revision':'v1','mode':'simulation_teleop',
             'name':'preserve','calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'}
    yield create,actor,payload
    for platform in instances:
        if not platform.closing:platform.close()


def restart_without_business_stop(contract,runtime,platform):
    """Quiesce fake devices, but keep the crash-window business row unchanged."""
    for key in list(runtime.live):runtime.suspend_capture(key)
    contract.closed=True;contract.db.close()
    platform.close()


def test_stopped_episode_submission_never_restarts_target_and_is_idempotent(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor)
    key=first['session_id']
    c.stop(key,{'project':'lab'},'stop',actor)
    path=p.root/'capture/tasks'/key/'task.json'
    before=json.loads(path.read_text());files=before['current']['files']
    def forbidden(*args,**kwargs):raise AssertionError('Stopped submission must never touch the target')
    r.command=r.start=r.reconcile=forbidden
    params={'project':'lab','recording_generation':1}
    result=c.complete(key,first['episode_id'],params,'submit-stopped',actor)
    assert result['state']=='IDLE' and result['next_recording_started'] is False
    assert result['committed_episodes']==1 and result['preserved_current_frames']==0
    assert not r.live and len(box.rows)==1
    after=json.loads(path.read_text())
    assert after['current'] is None and after['commits'][0]['files']==files
    assert c.complete(key,first['episode_id'],params,'submit-stopped',actor)==result
    with pytest.raises(Fault,match='Only current'):
        c.complete(key,first['episode_id'],params,'different-submit',actor)
    assert len(box.rows)==1


def wait_automatic_finish(contract,key,actor,timeout=30):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        row=contract._read(key,actor)
        if row.get('finish',{}).get('state') in ('QUEUED_UPLOAD','EMPTY'):
            return row
        time.sleep(.03)
    raise AssertionError('Finish did not auto-submit: '+str(contract._read(key,actor).get('finish')))


def test_finish_running_stops_and_submits_nonempty_without_next(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor);key=first['session_id']
    assert r.status(key)['current_frames']>=1
    raw={f:f.read_bytes() for f in (r.root/'tasks'/key).rglob('*.npz')}
    def no_resume(*args,**kwargs):raise AssertionError('Finish may never restart movement')
    r.resume_capture=no_resume
    result=c.finish(key,{'project':'lab'},'finish',actor)
    assert result['state']=='FINISHING'
    assert c.finish(key,{'project':'lab'},'finish',actor)==result
    row=wait_automatic_finish(c,key,actor)
    assert row['internal_state']=='STOPPED' and row['finish']['state']=='QUEUED_UPLOAD'
    assert len(row['collection_episodes'])==1 and len(box.rows)==1 and not r.live
    assert row['stop_result']['preserved_current_frames']==0
    assert all(f.read_bytes()==data for f,data in raw.items())


def test_finish_historical_reconcile_runs_without_extra_operator_confirmation(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor);key=first['session_id']
    assert r.status(key)['current_frames']>=1
    restart_without_business_stop(c,r,p)
    c,r,box,p=create()
    assert c._read(key,actor)['internal_state']=='NEEDS_RECONCILE'
    c.finish(key,{'project':'lab'},'end-after-restart',actor)
    row=wait_automatic_finish(c,key,actor)
    assert row['finish']['state']=='QUEUED_UPLOAD' and not r.live and len(box.rows)==1


def test_finish_recovers_enqueue_failure_without_container_restart(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor);key=first['session_id']
    original=box.enqueue;attempts=[]
    def fail_once(op,data):
        attempts.append(op)
        if len(attempts)==1:raise OSError('Transient outbox storage contention')
        return original(op,data)
    box.enqueue=fail_once
    c.finish(key,{'project':'lab'},'finish',actor)
    row=wait_automatic_finish(c,key,actor)
    assert len(box.rows)==1 and len(row['collection_episodes'])==1
    assert len(attempts)>=2 and len(set(attempts))==1


def test_finish_intent_restart_submits_preserved_data_without_resuming(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor);key=first['session_id']
    c.stop(key,{'project':'lab'},'stop',actor)
    row=c._read(key,actor);row['finish']={'state':'FINALIZING','request_id':'finish-before-crash','attempts':1};c._write(row)
    c.closed=True;c.db.close();p.close()
    restored,r,box,p=create();row=wait_automatic_finish(restored,key,actor)
    assert len(row['collection_episodes'])==1 and len(box.rows)==1 and not r.live
    with pytest.raises(Fault,match='ended'):
        restored.resume(key,{'project':'lab','workspace_confirmed':True},'resume-after-finish',actor)




def test_restart_repairs_commit_after_native_save_before_upload_insert(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor)
    def crash(key):raise Fault('CRASH_AFTER_NATIVE_SAVE','Injected before business outbox')
    r.capture_checkpoint=crash
    with pytest.raises(Fault,match='Injected'):
        c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',actor)
    journal=json.loads((p.root/'capture/tasks'/first['session_id']/'task.json').read_text())
    assert len(journal['commits'])==1 and journal['commits'][0]['frames']>=1
    intent=c._read(first['session_id'],actor)['commit_intent']
    restart_without_business_stop(c,r,p)
    restored,_,repaired,_=create()
    job=repaired.rows[intent['upload_operation']]
    assert job['payload']['episode_id']==first['episode_id']
    assert job['payload']['recording_generation']==1
    assert job['payload']['commits']==journal['commits']
    row=restored._read(first['session_id'],actor)
    assert row['episode_index']==1 and row['episode_id']!=first['episode_id']
    with pytest.raises(Fault):restored.status({'project':'lab','robot_id':'recovery-sim','robot_revision':'v1'},actor)


def test_restart_preserves_existing_outbox_and_normalizes_stale_current_identity(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor)
    saved=r.command(first['session_id'],'save',{})
    row=c._read(first['session_id'],actor)
    row['commit_intent']={'upload_operation':'repair-operation','episode_id':row['episode_id'],
                         'episode_index':row['episode_index'],'recording_generation':1}
    c._write(row)
    identity=c._queue_commit(row,r.capture_checkpoint(first['session_id']),saved)
    # This is the exact durable boundary before complete advances index/id.
    assert row['episode_index']==0 and row['episode_id']==first['episode_id']
    restart_without_business_stop(c,r,p)
    restored,runtime,repaired,_=create()
    assert list(repaired.rows)==[identity['upload_operation']]
    row=restored._read(first['session_id'],actor)
    assert row['episode_index']==1 and row['episode_id']!=first['episode_id']
    stopped=restored.stop(first['session_id'],{'project':'lab','workspace_confirmed':True},'recovery-stop',actor)
    assert stopped['state']=='IDLE'
    resumed=restored.resume(first['session_id'],{'project':'lab','workspace_confirmed':True},'resume',actor)
    assert resumed['state']=='RUNNING' and resumed['current_frames']>=1
    assert resumed['episode_index']==1 and resumed['episode_id']==row['episode_id']
    assert len(runtime.capture_checkpoint(first['session_id'])['commits'])==1


def test_restart_resume_keeps_same_unsubmitted_raw_files(recovery_system):
    create,actor,payload=recovery_system
    c,r,box,p=create();first=c.start(payload,'start',actor)
    restart_without_business_stop(c,r,p)
    path=p.root/'capture/tasks'/first['session_id']/'task.json'
    current=json.loads(path.read_text())['current']
    originals={item['path']:(path.parent/item['path']).read_bytes() for item in current['files']}
    restored,runtime,_,_=create()
    restored.stop(first['session_id'],{'project':'lab','workspace_confirmed':True},'stop-after-restart',actor)
    result=restored.resume(first['session_id'],{'project':'lab','workspace_confirmed':True},'resume',actor)
    assert result['session_id']==first['session_id'] and result['episode_id']==first['episode_id']
    assert result['recording_generation']==2 and result['current_frames']>len(originals)
    for name,data in originals.items():assert (path.parent/name).read_bytes()==data
    assert runtime.capture_checkpoint(first['session_id'])['commits']==[]


@pytest.mark.parametrize('peer',[{}, {'peer':{}}, {'peer':{'fault':None}}])
def test_partial_target_query_is_error_never_idle(recovery_system,peer):
    create,actor,_=recovery_system
    contract,_,_,_=create()
    contract.probe=lambda *args:peer
    with pytest.raises(Fault):
        contract.status({'project':'lab','robot_id':'recovery-sim','robot_revision':'v1'},actor)


def test_shutdown_cancels_start_before_native_import_finishes(recovery_system,monkeypatch):
    import lerobot_sdk.adapters.seeed_dataset as native
    create,actor,payload=recovery_system
    contract,runtime,_,platform=create()
    entered=threading.Event();release=threading.Event();errors=[];backends=[]
    original_upstream=native.upstream;factory=runtime.factory
    def blocked_import():
        entered.set();assert release.wait(20)
        return original_upstream()
    def track_backend(robot):
        value=factory(robot);backends.append(value);return value
    monkeypatch.setattr(native,'upstream',blocked_import)
    runtime.factory=track_backend
    def start():
        try:contract.start(payload,'delayed-start',actor)
        except Exception as exc:errors.append(exc)
    pending=threading.Thread(target=start)
    pending.start()
    assert entered.wait(10)
    closing=threading.Thread(target=platform.close)
    closing.start()
    try:
        deadline=time.monotonic()+5
        while not platform.closing and time.monotonic()<deadline:time.sleep(.01)
        assert platform.closing
    finally:release.set()
    pending.join(15);closing.join(15)
    assert not pending.is_alive() and not closing.is_alive()
    assert errors
    assert not any(call[0]=='connect' for backend in backends for call in backend.calls), 'Startup acquired a device after service shutdown began'
