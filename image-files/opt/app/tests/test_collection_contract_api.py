"""Business lifecycle tests use real SQLite; the device seam is software-only."""
import hashlib
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from deployment_runtime.store import Principal, Fault


class CaptureEngine:
    def __init__(self, root):
        self.root=Path(root);self.live={};self.discards=[];self.stopped=[]
    def start(self,key,robot,payload,**kwargs):
        self.live[key]={'state':'RECORDING','current_frames':2,'committed_episodes':0,'episode_id':'first-'+key,
                        'commits':[], 'mode':payload['mode']}
        return self.status(key)
    def status(self,key):
        if key not in self.live:raise Fault('NOT_ACTIVE','Stopped')
        return {k:v for k,v in self.live[key].items() if k!='commits'}
    def command(self,key,operation,payload):
        v=self.live[key]
        if operation=='save':
            v['commits'].append({'episode_index':len(v['commits']),'frames':v['current_frames'],'task':'test','files':[],'content_hash':'sha256:'+'a'*64})
            v.update(state='RESETTING',committed_episodes=len(v['commits']),current_frames=0)
            return {'episode_index':len(v['commits'])-1,'frames':2}
        if operation=='redo':self.discards.append(v['episode_id']);v.update(state='RESETTING',current_frames=0)
        if operation=='next':v.update(state='RECORDING',current_frames=1,episode_id='next-'+str(len(v['commits']))+'-'+key)
        return self.status(key)
    def capture_checkpoint(self,key):return {'task_directory':str(self.root/'tasks'/key),'commits':list(self.live[key]['commits'])}
    def set_episode_id(self,key,value):self.live[key]['episode_id']=value
    def suspend_capture(self,key):
        self.stopped.append(key);v=self.live.pop(key)
        return {'holding':True,'action_queue_empty':True,'preserved_current_frames':v['current_frames'],'state':'STOPPED'}
    def close(self):return []


def test_finish_empty_stops_without_creating_empty_episode(system):
    c,e,b,p,a,payload=system
    first=c.start(payload,'finish-empty-start',a);key=first['session_id']
    e.live[key]['current_frames']=0
    ack=c.finish(key,{'project':'lab'},'finish-empty',a)
    assert ack['state']=='FINISHING'
    end=time.monotonic()+3
    while c._read(key,a).get('finish',{}).get('state')!='EMPTY' and time.monotonic()<end:time.sleep(.01)
    row=c._read(key,a)
    assert row['finish']['state']=='EMPTY' and row['internal_state']=='STOPPED',row['finish']
    assert not e.live and not b.rows


def test_finish_never_reports_idle_until_hold_is_proven(system):
    c,e,b,p,a,payload=system
    first=c.start(payload,'finish-hold-start',a);key=first['session_id']
    def unconfirmed(*args,**kwargs):return {'holding':False,'action_queue_empty':False,'preserved_current_frames':2}
    e.suspend_capture=unconfirmed
    c.finish(key,{'project':'lab'},'finish-hold',a)
    end=time.monotonic()+3
    while c._read(key,a).get('finish',{}).get('state')!='RETRY_WAIT' and time.monotonic()<end:time.sleep(.01)
    view=c.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},a)
    assert view['state']=='FINISHING' and view['finish_state']=='RETRY_WAIT'
    assert not b.rows and key in e.live


class Outbox:
    def __init__(self):self.rows={}
    def enqueue(self,key,payload):self.rows[key]={'operation_id':key,'payload':payload,'state':'PENDING'};return self.rows[key]
    def get(self,key):return self.rows[key]
    def start(self):pass
    def stop(self,**kwargs):pass


@pytest.fixture
def system(tmp_path):
    import importlib.util
    assert importlib.util.find_spec('deployment_runtime.collection_contract'), 'Complete collection contract is missing'
    from deployment_runtime.service import Platform
    from deployment_runtime.collection_contract import CollectionContract
    from lerobot_sdk.testing import simulation_config
    a=Principal('alice',('lab',),('admin','operator'))
    cfg=simulation_config('sim');sdk=dict(cfg.__dict__)
    sdk['release_pose']=None
    for name in ('units','limits','max_velocity','max_step','tolerance'):sdk[name]=dict(sdk[name])
    engine=CaptureEngine(tmp_path/'capture');platform=Platform({'role':'collection','root':str(tmp_path)},sessions=engine)
    platform.register('robots',{'project':'lab','id':'sim','revision':'v1','body':{
        'robot_id':'sim','kind':'simulation','scene_id':'desk','scene_revision':'v1','sdk_config':sdk,
        'camera_shapes':{'front':[12,16,3]},'capabilities':['collection','teleop'],
        'ros':{'namespace':'/test','domain_id':93}}},a)
    box=Outbox()
    contract=CollectionContract(platform,uploads=box,target_probe=lambda *args:{'peer':{'owner':None,'fault':None,'holding':True,'action_queue_empty':True}})
    platform.collection=contract
    payload={'project':'lab','robot_id':'sim','robot_revision':'v1','mode':'simulation_teleop','name':'test','calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'}
    yield contract,engine,box,platform,a,payload
    contract.close();platform.close()


def test_complete_commits_outbox_and_automatically_records_next(system):
    c,e,b,p,a,payload=system
    first=c.start(payload,'start',a)
    assert first['state']=='RUNNING' and first['current_frames']>0
    done=c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)
    assert done['committed_episode']['episode_index']==0
    assert done['episode_index']==1 and done['episode_id']!=first['episode_id'] and done['state']=='RUNNING'
    assert done['upload_operation'] in b.rows
    frozen=b.rows[done['upload_operation']]['payload']
    assert len(frozen['commits'])==len(frozen['collection_episodes'])==1
    assert frozen['collection_episodes'][0]['episode_id']==first['episode_id']
    assert c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)==done
    assert len(b.rows)==1


def test_redo_increments_generation_and_restarts_only_unsubmitted(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    redone=c.redo(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'redo',a)
    assert redone['episode_id']==first['episode_id'] and redone['recording_generation']==2
    assert redone['state']=='RUNNING' and redone['current_frames']>0 and not b.rows
    with pytest.raises(Fault,match='generation'):
        c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'late',a)


def test_stop_is_independent_of_pending_upload_and_preserves_current(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)
    stopped=c.stop(first['session_id'],{'project':'lab'},'stop',a)
    assert stopped['state']=='IDLE' and stopped['preserved_current_frames']==1
    assert next(iter(b.rows.values()))['state']=='PENDING'
    assert not p.store.claims()
    assert c.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},a)['state']=='IDLE'


def test_unknown_fault_is_api_error_never_false_idle(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    e.live[first['session_id']]['state']='RECOVERABLE'
    with pytest.raises(Fault):c.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},a)


def test_owner_conflict_and_same_key_altered_payload(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    with pytest.raises(Fault,match='conflict'):c.start({**payload,'name':'altered'},'start',a)
    other=Principal('bob',('lab',),('operator',))
    with pytest.raises(Fault):c.stop(first['session_id'],{'project':'lab'},'stop',other)
    with pytest.raises(Fault):c.start(payload,'other',other)


def test_new_rest_routes_and_query_errors(system):
    import asyncio
    from aiohttp.test_utils import TestClient,TestServer
    from deployment_runtime.http import make_app
    c,e,b,p,a,payload=system;p.collection=c
    accounts=[{'name':a.name,'projects':['lab'],'roles':['admin','operator'],'token_sha256':hashlib.sha256(b'secret').hexdigest()}]
    async def run():
        async with TestClient(TestServer(make_app(p,accounts))) as client:
            headers={'Authorization':'Bearer secret'}
            response=await client.post('/collection/sessions',headers=headers,json={**payload,'request_id':'rest-start'})
            assert response.status==201
            first=(await response.json())['result']
            response=await client.get('/collection/status?project=lab&robot_id=sim&robot_revision=v1',headers=headers)
            assert response.status==200 and (await response.json())['result']['state']=='RUNNING'
            response=await client.post('/collection/sessions/'+first['session_id']+'/stop',headers=headers,json={'project':'lab','request_id':'rest-stop'})
            assert response.status==200 and (await response.json())['result']['state']=='IDLE'
            response=await client.get('/collection/status?project=other&robot_id=sim&robot_revision=v1',headers=headers)
            assert response.status==403 and not (await response.json())['ok']
    asyncio.run(run())


def test_submitted_episode_next_failure_cannot_be_submitted_again(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    original=e.command
    def fail_next(key,op,body):
        if op=='next':raise Fault('SOURCE_STALE','Producer stopped')
        return original(key,op,body)
    e.command=fail_next
    with pytest.raises(Fault) as failure:
        c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)
    assert failure.value.code=='NEXT_EPISODE_FAILED'
    assert len(b.rows)==1 and len(e.live[first['session_id']]['commits'])==1
    with pytest.raises(Fault) as repeated:
        c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)
    assert repeated.value.details['committed_episode']['episode_id']==first['episode_id']


def test_idle_query_offline_returns_error_and_preserves_state(system):
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    c.stop(first['session_id'],{'project':'lab'},'stop',a)
    def offline(*args):raise TimeoutError('No peer')
    c.probe=offline
    with pytest.raises(Fault) as error:c.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},a)
    assert error.value.code=='TARGET_STATUS_UNAVAILABLE'


def test_disk_full_refuses_new_capture_without_claim(system,monkeypatch):
    c,e,b,p,a,payload=system
    import lerobot_sdk.data.filesystem as fs
    from lerobot_sdk.core.errors import OperationFailed
    monkeypatch.setattr(fs,'disk_check',lambda *args:(_ for _ in ()).throw(OperationFailed('Disk reserve exhausted')))
    with pytest.raises(Fault,match='Disk'):c.start(payload,'disk-full',a)
    assert not p.store.claims() and not e.live


@pytest.mark.parametrize('invalid',[{}, {'peer':{}}, {'peer':{'owner':None,'fault':None,'holding':True}}])
def test_incomplete_probe_never_means_idle(system,invalid):
    c,e,b,p,a,payload=system;c.probe=lambda *args:invalid
    with pytest.raises(Fault) as error:c.status({'project':'lab','robot_id':'sim','robot_revision':'v1'},a)
    assert error.value.code=='TARGET_STATUS_UNAVAILABLE'


def test_bad_commit_recovery_alert_does_not_block_other_uploads(system,monkeypatch):
    from deployment_runtime.collection_contract import CollectionContract
    c,e,b,p,a,payload=system;first=c.start(payload,'start',a)
    c.complete(first['session_id'],first['episode_id'],{'project':'lab','recording_generation':1},'complete',a)
    c.stop(first['session_id'],{'project':'lab'},'stop',a)
    row=c._read(first['session_id'],a)
    row.update(commit_intent={'upload_operation':'bad'},internal_state='NEEDS_RECONCILE')
    c._write(row);c.close()
    def corrupt(self,row):raise Fault('HASH_MISMATCH','Preserved raw file corrupt')
    monkeypatch.setattr(CollectionContract,'_repair_commit',corrupt)
    restored=CollectionContract(p,uploads=b,target_probe=c.probe);p.collection=restored
    assert len(b.rows)==1
    saved=restored._read(first['session_id'],a)
    assert saved['recovery_alert'] and saved['internal_state']=='NEEDS_RECONCILE'
    assert 'corrupt' in saved['error']
