"""A v10 start must be queryable through the no-parameter status endpoint."""
import asyncio
import hashlib
from types import SimpleNamespace
import pytest
from aiohttp.test_utils import TestClient,TestServer
from deployment_runtime.http import make_app
from deployment_runtime.store import Fault
from test_collection_current_api import system,wait
from test_collection_minimal_api import system as minimal_system

@pytest.mark.parametrize('query',['','?session_id=status-current','?is_sim=true'])
def test_running_current_collection_is_queryable_without_api_version_mismatch(system,query):
    api,legacy,platform,contract,engine,actor=system
    platform.current_collection=api;platform.minimal_collection=legacy
    api.submit('start',{'id':'status-current','name':'status regression'},actor)
    assert wait(api)['state']=='SUCCEEDED'
    async def run():
        account={'name':actor.name,'projects':['lab'],'roles':['operator','admin'],
                 'token_sha256':hashlib.sha256(b'test').hexdigest()}
        async with TestClient(TestServer(make_app(platform,[account]))) as client:
            r=await client.get('/collection/status'+query,headers={'Authorization':'Bearer test'})
            body=await r.json()
            assert r.status==200,body
            assert body['result']['state']=='RUNNING'
            assert body['result']['session_id']=='status-current'
    asyncio.run(run())

@pytest.mark.parametrize('code,expected,fallback',[
    ('API_VERSION_MISMATCH',200,True),('FORBIDDEN',403,False)])
def test_only_an_api_version_mismatch_can_fall_back_to_legacy_status(code,expected,fallback):
    calls=[]
    class Current:
        def status(self,query,actor):
            calls.append('current');raise Fault(code,'fixture',403 if code=='FORBIDDEN' else 409)
    class Legacy:
        def status(self,query,actor):calls.append('legacy');return {'state':'RUNNING','session_id':'v9'}
    platform=SimpleNamespace(role='collection',get_collection=lambda:Legacy(),
        get_minimal_collection=lambda:Legacy(),get_current_collection=lambda:Current(),close=lambda:None)
    async def run():
        account={'name':'alice','projects':['lab'],'roles':['operator'],'token_sha256':hashlib.sha256(b'test').hexdigest()}
        async with TestClient(TestServer(make_app(platform,[account]))) as client:
            r=await client.get('/collection/status',headers={'Authorization':'Bearer test'})
            assert r.status==expected,await r.text()
    asyncio.run(run())
    assert calls==(['current','legacy'] if fallback else ['current'])
