"""Tests the console adapter, not a simulated successful recording."""
import asyncio,copy,importlib.util
from pathlib import Path
import pytest
from aiohttp.test_utils import TestClient,TestServer

class Backend:
    def __init__(self):self.calls=[];self.result={'state':'IDLE','robot_id':'sim'};self.failure=None;self.fail_once=False
    async def get(self,path):
        self.calls.append(('GET',path,None))
        if path.startswith('/v2/robots?'):
            return {'items':[{'id':'sim','revision':'v2','created':2,'body':{'robot_id':'sim','kind':'simulation','provenance':'test_fixture','camera_shapes':{'front':[12,16,3]},'capabilities':['collection'], 'calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'}}],'next_offset':None}
        if path.startswith('/collection/status'):
            if self.failure:raise self.failure
            return copy.deepcopy(self.result)
        if '/media' in path:return {}
        if '/datasets?' in path:return {'items':[],'next_offset':None}
        return {'items':[],'next_offset':None}
    async def post(self,path,body):
        self.calls.append(('POST',path,copy.deepcopy(body)))
        if self.fail_once:
            self.fail_once=False
            from collection_console.server import ConsoleError
            raise ConsoleError('UPSTREAM_OUTCOME_UNKNOWN','Reply unavailable',503)
        if path=='/collection/sessions':
            self.result={'state':'RUNNING','session_id':'s','episode_id':'e','recording_generation':1,'episode_index':0,'current_frames':1,'committed_episodes':0}
        elif path.endswith('/complete'):
            self.result={**self.result,'episode_id':'e2','recording_generation':1,'episode_index':1,'committed_episodes':1}
        elif path.endswith('/finish'):self.result={'state':'FINISHING','session_id':'s','finish_state':'QUEUED','automatic_upload':True}
        return copy.deepcopy(self.result)

def model(tmp_path,backend):
    assert importlib.util.find_spec('collection_console.server'),'Console implementation missing'
    from collection_console.server import Console
    return Console({'project':'lab','state_file':str(tmp_path/'console.sqlite'),'aliases':{'sim':'g1_1'},'preferred_revisions':{'sim':'v2'}},backend)


def test_console_end_calls_finish_once_without_extra_confirmation_or_complete(tmp_path):
    async def run():
        b=Backend();c=model(tmp_path,b)
        await c.command({'action':'start','request_id':'start','robot_key':'sim@v2','mode':'simulation_teleop','name':'demo'})
        await c.command({'action':'stop','request_id':'stop','robot_key':'sim@v2','mode':'simulation_teleop','session_id':'s'})
        state=await c.state('sim@v2','simulation_teleop')
        assert state['status']['state']=='FINISHING' and state['resume_session'] is None
        posted=[call for call in b.calls if call[0]=='POST']
        assert posted[-1][1]=='/collection/sessions/s/finish'
        assert 'workspace_confirmed' not in posted[-1][2]
        assert not any(path.endswith('/complete') for method,path,_ in b.calls if method=='POST')
        c.close()
    asyncio.run(run())

def test_page_state_does_not_start_capture_or_expose_secrets(tmp_path):
    async def run():
        b=Backend();c=model(tmp_path,b);v=await c.state('sim@v2','simulation_teleop')
        assert v['status']['state']=='IDLE' and v['robots'][0]['label']=='g1_1'
        assert not any(method=='POST' for method,_,_ in b.calls)
        assert 'token' not in str(v).lower() and 'key_file' not in str(v)
        c.close()
    asyncio.run(run())

def test_unknown_status_never_becomes_idle(tmp_path):
    async def run():
        from collection_console.server import ConsoleError
        b=Backend();b.failure=ConsoleError('TARGET_STATUS_UNAVAILABLE','Offline',503)
        c=model(tmp_path,b);v=await c.state('sim@v2','simulation_teleop')
        assert v['status'] is None and v['status_error']['code']=='TARGET_STATUS_UNAVAILABLE'
        c.close()
    asyncio.run(run())

def test_retry_uses_original_frozen_payload_and_stale_episode_is_rejected(tmp_path):
    async def run():
        from collection_console.server import ConsoleError
        b=Backend();c=model(tmp_path,b)
        p={'action':'start','request_id':'start','robot_key':'sim@v2','mode':'simulation_teleop','name':'demo'}
        started=await c.command(p);assert started['state']=='RUNNING'
        b.fail_once=True
        complete={'action':'complete','request_id':'save','robot_key':'sim@v2','mode':'simulation_teleop','session_id':'s','episode_id':'e','recording_generation':1}
        with pytest.raises(ConsoleError):await c.command(complete)
        b.result.update(episode_id='changed',recording_generation=4)
        await c.command(complete)
        requests=[(path,payload) for method,path,payload in b.calls if method=='POST' and path.endswith('/complete')]
        assert requests[0]==requests[1] and requests[1][1]['recording_generation']==1
        with pytest.raises(ConsoleError):await c.command({**complete,'recording_generation':2})
        c.close()
    asyncio.run(run())

def test_refresh_keeps_finish_intent_and_does_not_require_submit(tmp_path):
    async def run():
        b=Backend();c=model(tmp_path,b)
        await c.command({'action':'start','request_id':'start','robot_key':'sim@v2','mode':'simulation_teleop','name':'demo'})
        await c.command({'action':'stop','request_id':'stop','robot_key':'sim@v2','mode':'simulation_teleop','session_id':'s'})
        c.close();c=model(tmp_path,b);v=await c.state('sim@v2','simulation_teleop')
        assert v['known_session']['session_id']=='s' and v['status']['finish_state']=='QUEUED'
        assert v['resume_session'] is None
        assert not any(path.endswith('/complete') for method,path,_ in b.calls if method=='POST')
        c.close()
    asyncio.run(run())

def test_same_origin_csrf_guards_and_no_page_close_stop(tmp_path):
    async def run():
        from collection_console.server import make_app
        b=Backend();c=model(tmp_path,b)
        config={'bind':'127.0.0.1','allowed_hosts':['127.0.0.1','localhost']}
        async with TestClient(TestServer(make_app(config,c))) as client:
            response=await client.get('/api/state?robot_key=sim@v2&mode=simulation_teleop');assert response.status==200
            data=(await response.json())['result'];csrf=data['csrf_token'];origin=str(client.make_url('/')).rstrip('/')
            body={'action':'start','request_id':'click','robot_key':'sim@v2','mode':'simulation_teleop','name':'demo'}
            assert (await client.post('/api/command',json=body)).status==403
            assert (await client.post('/api/command',json=body,headers={'Origin':'https://evil.invalid','X-CSRF-Token':csrf})).status==403
            response=await client.post('/api/command',json=body,headers={'Origin':origin,'X-CSRF-Token':csrf});assert response.status==200
        assert not any(path.endswith(('/stop','/finish')) for method,path,_ in b.calls if method=='POST')
    asyncio.run(run())


class RealArmBackend(Backend):
    """登记了仿真目标、本机直连真实从臂、以及一个非直连真机（Windows 原生那套）。"""
    def __init__(self):
        super().__init__();self.operations={}
    async def get(self,path):
        if path.startswith('/v2/robots?'):
            self.calls.append(('GET',path,None))
            sim={'robot_id':'sim','kind':'simulation','provenance':'test_fixture','camera_shapes':{'front':[12,16,3]},'capabilities':['collection'],'calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'}
            real={'robot_id':'real','kind':'physical','provenance':'physical_direct_follower','commissioned':True,'camera_shapes':{'wrist':[480,640,3]},'capabilities':['collection','teleop'],'calibration_revision':'baseline'}
            native={'robot_id':'native','kind':'physical','provenance':'physical_native_relative_teleop','commissioned':True,'camera_shapes':{'front':[12,16,3]},'capabilities':['collection'],'calibration_revision':'baseline'}
            return {'items':[{'id':'sim','revision':'v2','created':1,'body':sim},{'id':'real','revision':'direct-1','created':2,'body':real},
                             {'id':'native','revision':'n1','created':3,'body':native}],'next_offset':None}
        if path.startswith('/v2/operations/'):
            self.calls.append(('GET',path,None))
            key=path.rsplit('/',1)[1]
            return {'state':'SUCCEEDED','result':{'holding':True,'action_queue_empty':True} if key.startswith('stop') else {}}
        return await super().get(path)
    async def post(self,path,body):
        if path=='/v2/commands':
            self.calls.append(('POST',path,copy.deepcopy(body)))
            return {'operation_id':('stop-' if body['operation']=='verifications.stop' else 'align-')+body['request_id']}
        return await super().post(path,body)


def test_console_lists_targets_without_alignment(tmp_path):
    """v10r15：工作台没有对齐（对齐由操作者自己负责，平台不管）。"""
    async def run():
        from collection_console.server import Console,ConsoleError
        b=RealArmBackend()
        c=Console({'project':'lab','state_file':str(tmp_path/'console.sqlite'),'physical_enabled':True,'robot_ids':['sim','real','native'],
                   'preferred_revisions':{'sim':'v2','real':'direct-1','native':'n1'},'aliases':{'real':'Seeed 真机'}},b)
        state=await c.state('real@direct-1','physical_teleop')
        views={r['id']:r for r in state['robots']}
        assert views['real']['available'] and views['real']['label']=='Seeed 真机' and views['native']['available']
        assert not any('alignable' in view for view in views.values())
        assert state['selection']['robot_key']=='real@direct-1' and not state['notice']
        with pytest.raises(ConsoleError) as error:
            await c.command({'action':'align','request_id':'a1','robot_key':'real@direct-1','mode':'physical_teleop','workspace_confirmed':True})
        assert error.value.code=='INVALID_ACTION'
        assert not [body for method,path,body in b.calls if method=='POST' and path=='/v2/commands']
        c.close()
    asyncio.run(run())
