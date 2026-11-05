"""Only collection REST anonymous access and browser CORS; no device I/O."""
import asyncio,hashlib
from types import SimpleNamespace
import pytest
from aiohttp.test_utils import TestClient,TestServer
from deployment_runtime.http import make_app
from deployment_runtime.store import Fault


class Collection:
    def __init__(self):self.calls=[]
    def submit(self,action,body,actor):
        self.calls.append((action,body,actor))
        if body.get('name')=='reject':raise Fault('INVALID_NAME','Rejected input',400)
        return {'accepted':True,'message':'已接收，后台执行中'}
    def status(self,body,actor):return {'state':'IDLE','project':body.get('project','lab')}


def setup(role='collection',mode='none'):
    service=Collection()
    platform=SimpleNamespace(role=role,config={'collection_rest_auth':mode,'collection_project':'lab'},
        get_collection=lambda:service,get_minimal_collection=lambda:service,get_current_collection=lambda:service,
        close=lambda:None)
    accounts=[{'name':'operator','projects':['lab'],'roles':['admin','operator','trainer'],
        'token_sha256':hashlib.sha256(b'test-secret').hexdigest()}]
    return platform,accounts,service


def test_collection_rest_needs_no_token_and_uses_fixed_operator():
    async def run():
        platform,accounts,service=setup()
        async with TestClient(TestServer(make_app(platform,accounts))) as c:
            for headers in ({},{'Authorization':'Bearer stale-token'},{'Cookie':'anything=not-auth'}):
                r=await c.post('/collection/start',headers=headers,json={'name':'test','is_sim':True})
                assert r.status==202,await r.text()
                assert set((await r.json())['result'])=={'accepted','message'}
                actor=service.calls[-1][2]
                assert actor.name=='operator' and actor.projects==('lab',) and actor.roles==('operator',)
            for action in ('redo','complete','finish'):
                r=await c.post('/collection/'+action)
                assert r.status==202 and service.calls[-1][1]=={}
            r=await c.get('/collection/status?project=lab')
            assert r.status==200
            assert (await c.get('/openapi.json')).status==200
    asyncio.run(run())


def test_browser_preflight_and_actual_errors_are_cors_readable_without_credentials():
    async def run():
        platform,accounts,service=setup()
        origin='http://192.168.0.50:3000'
        async with TestClient(TestServer(make_app(platform,accounts))) as c:
            r=await c.options('/collection/start',headers={'Origin':origin,
                'Access-Control-Request-Method':'POST','Access-Control-Request-Headers':'content-type'})
            assert r.status==204,await r.text()
            assert r.headers['Access-Control-Allow-Origin']=='*'
            assert 'POST' in r.headers['Access-Control-Allow-Methods']
            assert 'content-type' in r.headers['Access-Control-Allow-Headers'].lower()
            assert 'Access-Control-Allow-Credentials' not in r.headers and not service.calls
            for name,status in (('ok',202),('reject',400)):
                r=await c.post('/collection/start',headers={'Origin':origin},json={'name':name})
                assert r.status==status and r.headers['Access-Control-Allow-Origin']=='*'
                assert 'Set-Cookie' not in r.headers
            r=await c.post('/collection/start',headers={'Origin':origin},data='not-json')
            assert r.status==400 and r.headers['Access-Control-Allow-Origin']=='*'
            r=await c.options('/collection/start',headers={'Origin':origin,'Access-Control-Request-Method':'DELETE'})
            assert r.status==405
    asyncio.run(run())


def test_management_and_training_authentication_are_preserved():
    async def run():
        for role in ('collection','training'):
            platform,accounts,_=setup(role)
            async with TestClient(TestServer(make_app(platform,accounts))) as c:
                assert (await c.get('/v2/identity')).status==401
                r=await c.get('/v2/identity',headers={'Authorization':'Bearer test-secret'})
                assert r.status==200 and (await r.json())['result']['principal']=='operator'
                if role=='training':
                    assert (await c.post('/collection/start',json={'name':'x'})).status==401
                    assert (await c.get('/openapi.json')).status==401
                else:
                    r=await c.options('/v2/identity',headers={'Origin':'http://client','Access-Control-Request-Method':'GET'})
                    assert r.status==401 and 'Access-Control-Allow-Origin' not in r.headers
    asyncio.run(run())


def test_bearer_mode_remains_default_and_schema_matches_deployment():
    async def run():
        for mode in ('none','bearer'):
            platform,accounts,_=setup(mode=mode)
            async with TestClient(TestServer(make_app(platform,accounts))) as c:
                r=await c.post('/collection/start',json={'name':'x'})
                assert r.status==(202 if mode=='none' else 401)
                r=await c.get('/openapi.json',headers={'Authorization':'Bearer test-secret'})
                spec=await r.json()
                assert spec['paths']['/collection/start']['post'].get('security')==([] if mode=='none' else None)
                assert spec['paths']['/v2/identity']['get'].get('security') is None
    asyncio.run(run())


def test_ambiguous_identity_fails_at_configuration_not_by_selecting_first_account():
    platform,accounts,_=setup()
    accounts.append({**accounts[0],'name':'other'})
    with pytest.raises((ValueError,Fault)):make_app(platform,accounts)


def test_explicit_operator_resolution_keeps_project_and_never_grants_admin():
    platform,accounts,_=setup()
    platform.config['collection_rest_operator']='operator'
    accounts.append({**accounts[0],'name':'other'})
    from deployment_runtime.collection_access import CollectionAccess
    access=CollectionAccess(platform,accounts)
    assert access.principal.name=='operator' and access.principal.roles==('operator',)
    with pytest.raises(Fault):access.principal.require('other','operator')
    with pytest.raises(Fault):access.principal.require('lab','admin')


def test_cors_covers_method_and_internal_failure_without_opening_future_routes():
    async def run():
        platform,accounts,service=setup()
        def failure(*args):raise RuntimeError('private diagnostic')
        service.submit=failure
        async with TestClient(TestServer(make_app(platform,accounts))) as c:
            r=await c.get('/collection/start',headers={'Origin':'http://client'})
            assert r.status==405 and r.headers['Access-Control-Allow-Origin']=='*'
            r=await c.post('/collection/redo',headers={'Origin':'http://client'})
            assert r.status==500 and r.headers['Access-Control-Allow-Origin']=='*'
            assert 'private diagnostic' not in await r.text()
            r=await c.post('/collection/admin/reset',headers={'Origin':'http://client'})
            assert r.status==401 and 'Access-Control-Allow-Origin' not in r.headers
            r=await c.options('/collection/start',headers={'Origin':'http://client',
                'Access-Control-Request-Method':'POST','Access-Control-Request-Private-Network':'true'})
            assert r.status==204 and r.headers['Access-Control-Allow-Private-Network']=='true'
    asyncio.run(run())


def test_environment_mode_and_explicit_operator_config(monkeypatch):
    from deployment_runtime.collection_access import CollectionAccess
    platform,accounts,_=setup()
    platform.config={}
    monkeypatch.setenv('SO101_COLLECTION_REST_AUTH','none')
    monkeypatch.setenv('SO101_COLLECTION_REST_OPERATOR','operator')
    monkeypatch.setenv('SO101_COLLECTION_PROJECT','lab')
    access=CollectionAccess(platform,accounts)
    assert access.enabled and access.principal.name=='operator'
    platform.role='training'
    assert not CollectionAccess(platform,accounts).enabled
    platform.role='collection';platform.config={'collection_rest_auth':'bearer'}
    assert not CollectionAccess(platform,accounts).enabled
