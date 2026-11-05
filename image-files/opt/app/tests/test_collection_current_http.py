"""Only the v10 HTTP changes: empty bodies, minimal ACK and old shape dispatch."""
import asyncio
import hashlib
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from deployment_runtime.http import make_app


class Current:
    def __init__(self): self.calls=[]
    def submit(self, action, body, actor):
        self.calls.append((action,body,actor.name))
        return {'accepted':True,'message':'已接收，后台执行中'}


class Minimal:
    def __init__(self): self.calls=[]
    def submit(self, action, body, actor):
        self.calls.append((action,body,actor.name))
        return {'accepted':True,'request_id':body['request_id'],'operation_id':'legacy-op'}


class Legacy:
    def _read(self,key,actor):return {'api_contract':'minimal.v10'}
    def finish(self,*args):raise AssertionError('v8 may not bypass the frozen v10 upload destination')


def test_empty_post_and_v9_v8_dispatch_have_distinct_ack():
    async def run():
        current=Current();minimal=Minimal();legacy=Legacy()
        platform=SimpleNamespace(role='collection',get_collection=lambda:legacy,
            get_minimal_collection=lambda:minimal,get_current_collection=lambda:current,close=lambda:None,
            run_legacy_collection=lambda call,*args:call(*args))
        account={'name':'alice','projects':['lab'],'roles':['operator'],'token_sha256':hashlib.sha256(b'test').hexdigest()}
        auth={'Authorization':'Bearer test'}
        async with TestClient(TestServer(make_app(platform,[account]))) as client:
            assert (await client.post('/collection/redo')).status==401
            r=await client.post('/collection/align',headers=auth,json={})       # v10r15：没有对齐接口
            assert r.status==404 and not current.calls
            for action in ('redo','complete','finish'):
                for kwargs in ({},{'json':{}}):
                    r=await client.post('/collection/'+action,headers=auth,**kwargs)
                    assert r.status==202,(action,await r.text())
                    assert (await r.json())=={'ok':True,'result':{'accepted':True,'message':'已接收，后台执行中'}}
                    assert current.calls[-1]==(action,{},'alice')
            finish={'upload_url':'/xxx/action-composer/data/artifacts'}
            r=await client.post('/collection/finish',headers=auth,json=finish)
            assert r.status==202 and current.calls[-1]==('finish',finish,'alice')
            body={'id':'demo-1','name':'demo','is_sim':True}
            r=await client.post('/collection/start',headers=auth,json=body)
            assert r.status==202 and current.calls[-1]==('start',body,'alice')
            r=await client.post('/collection/start',headers=auth)
            assert r.status==400
            for invalid in ('null','[]','false','{"name": NaN}'):
                r=await client.post('/collection/start',headers=auth,data=invalid)
                assert r.status==400
            for action in ('start','redo','complete','finish'):
                r=await client.post('/collection/'+action,headers=auth,json={'request_id':'old-'+action})
                assert r.status==202 and (await r.json())['result']['operation_id']=='legacy-op'
                assert minimal.calls[-1][0]==action
            r=await client.post('/collection/sessions/s1/finish',headers=auth,json={'project':'lab','request_id':'v8-finish'})
            assert r.status==409 and (await r.json())['error']['code']=='API_VERSION_MISMATCH'
    asyncio.run(run())


def test_current_schema_empty_post_keeps_legacy_schemas():
    from deployment_runtime.api_contract import openapi
    schema=openapi();paths=schema['paths']
    assert 'CollectionCurrentAck' in schema['components']['schemas']
    assert paths['/collection/start']['post']['requestBody']['required'] is True
    for action in ('redo','complete','finish'):
        assert paths['/collection/'+action]['post']['requestBody']['required'] is False
    assert '/collection/align' not in paths
    start=schema['components']['schemas']['CollectionCurrentStartRequest']
    assert set(start['required'])=={'id','name'} and 'dataset_id' not in start['properties']
    finish=schema['components']['schemas']['CollectionCurrentFinishRequest']
    assert finish['required']==[] and set(finish['properties'])=={'upload_url'}
    assert '/collection/status' in paths and '/collection/operations/{id}' in paths
    assert '/collection/sessions' in paths


from test_collection_current_api import system
from test_collection_minimal_api import system as minimal_system


def test_abandon_route_is_public_short_and_takes_no_parameters(system):
    from deployment_runtime.collection_access import _ROUTES
    assert any(pattern.fullmatch('/collection/abandon') and methods==('POST',) for pattern,methods in _ROUTES)
    api,legacy,platform,contract,engine,actor=system
    platform.current_collection=api;platform.minimal_collection=legacy
    async def run():
        account={'name':actor.name,'projects':['lab'],'roles':['operator','admin'],
            'token_sha256':hashlib.sha256(b'test').hexdigest()}
        async with TestClient(TestServer(make_app(platform,[account]))) as client:
            headers={'Authorization':'Bearer test'}
            response=await client.post('/collection/abandon',headers=headers)
            assert response.status==202 and await response.json()=={'ok':True,'result':{'accepted':True,'message':'已接收，后台执行中'}}
            response=await client.post('/collection/abandon',headers=headers,json={'id':'x'})
            assert response.status==400 and (await response.json())['error']['code']=='INVALID_FIELDS'
            schema=await (await client.get('/openapi.json',headers=headers)).json()
            assert '/collection/abandon' in schema['paths']
    asyncio.run(run())
    for _ in range(500):
        if not getattr(platform,'collection_abandoning',False):break
        __import__('time').sleep(.01)


def test_legacy_start_cannot_enter_a_queued_current_command(system,monkeypatch):
    import pytest
    from deployment_runtime.store import Fault
    api,_,platform,_,_,actor=system
    monkeypatch.setattr(api,'_kick',lambda *args:None)
    api.submit('start',{'id':'queued-current','name':'x'},actor)
    with pytest.raises(Fault) as error:
        platform.run_legacy_collection(lambda:'must-not-run')
    assert error.value.code=='BUSY'


def test_legacy_slow_start_does_not_block_new_http_admission_thread(system):
    import threading,time,pytest
    from deployment_runtime.store import Fault
    api,_,platform,_,_,actor=system
    entered=threading.Event();release=threading.Event()
    def legacy():entered.set();assert release.wait(3)
    worker=threading.Thread(target=platform.run_legacy_collection,args=(legacy,))
    worker.start()
    try:
        assert entered.wait(1)
        tick=time.monotonic()
        with pytest.raises(Fault) as error:api.submit('start',{'id':'blocked-current','name':'x'},actor)
        assert error.value.code=='BUSY' and time.monotonic()-tick<.5
    finally:release.set();worker.join(3)


def test_real_http_current_batch_uses_only_empty_requests_and_sends_after_end(system):
    from test_collection_current_api import latest
    api,legacy,platform,contract,engine,actor=system
    platform.current_collection=api;platform.minimal_collection=legacy
    async def run():
        account={'name':actor.name,'projects':['lab'],'roles':['operator','admin'],
            'token_sha256':hashlib.sha256(b'test').hexdigest()}
        async with TestClient(TestServer(make_app(platform,[account]))) as client:
            headers={'Authorization':'Bearer test'}
            async def send(action,body=None):
                opts={} if body is None else {'json':body}
                response=await client.post('/collection/'+action,headers=headers,**opts)
                ack=await response.json()
                assert response.status==202 and ack=={'ok':True,'result':{'accepted':True,'message':'已接收，后台执行中'}},ack
                # Test instrumentation reads the internal journal; no frontend query is made.
                for _ in range(500):
                    operation=latest(api)
                    if operation['state'] not in ('QUEUED','RUNNING'):
                        assert operation['state']=='SUCCEEDED',operation
                        return operation['result']
                    await asyncio.sleep(.01)
                raise AssertionError('Internal worker did not settle')
            first=await send('start',{'id':'http-batch-1','name':'HTTP current collection'})
            assert first['session_id']=='http-batch-1' and 'is_sim' not in first
            redone=await send('redo')
            assert first['episode_id']!=redone['episode_id'] and redone['episode_index']==0
            second=await send('complete')
            assert second['episode_index']==1 and not contract.uploads.rows
            engine.live[first['session_id']]['current_frames']=0
            ended=await send('finish',{'upload_url':'/xxx/action-composer/data/artifacts/'})
            assert ended['holding'] and ended['action_queue_empty'] and not engine.live
            assert len(contract.uploads.rows)==1
            assert next(iter(contract.uploads.rows.values()))['payload']['destination']=={
                'protocol':'so101.sync.v2','upload_url':'/xxx/action-composer/data/artifacts'}
    asyncio.run(run())
