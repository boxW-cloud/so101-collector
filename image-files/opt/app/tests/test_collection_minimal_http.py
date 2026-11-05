"""HTTP version dispatch, authentication and asynchronous response contract."""
import asyncio
import hashlib
from types import SimpleNamespace

from aiohttp.test_utils import TestClient, TestServer

from deployment_runtime.http import make_app


class Minimal:
    def __init__(self): self.calls = []
    def submit(self, action, body, actor):
        self.calls.append((action, body, actor.name))
        return {'accepted': True, 'request_id': body['request_id'], 'operation_id': 'op-1'}
    def status(self, query, actor): return {'state': 'IDLE', 'query': dict(query)}
    def operation(self, key, actor): return {'operation_id': key, 'state': 'SUCCEEDED', 'result': {'episode_id': 'attempt-2'}}


class Legacy:
    def status(self, query, actor): return {'project': query['project'], 'state': 'IDLE'}
    def _read(self, key, actor): return {'api_contract': 'minimal.v9'}
    def redo(self, *args): raise AssertionError('v8 must not mutate v9 recording attempts')


def test_minimal_http_is_202_authenticated_and_legacy_dispatch_is_preserved():
    async def run():
        minimal = Minimal(); legacy = Legacy()
        platform = SimpleNamespace(role='collection', get_collection=lambda: legacy,
            get_minimal_collection=lambda: minimal, get_current_collection=lambda: None, close=lambda: None,
            run_legacy_collection=lambda call,*args:call(*args))
        account = {'name': 'alice', 'projects': ['lab'], 'roles': ['operator'],
            'token_sha256': hashlib.sha256(b'test').hexdigest()}
        headers = {'Authorization': 'Bearer test'}
        async with TestClient(TestServer(make_app(platform, [account]))) as client:
            assert (await client.post('/collection/start', json={'request_id': 'r'})).status == 401
            for action in ('start', 'redo', 'complete', 'finish'):
                body = {'request_id': action + '-1'}
                response = await client.post('/collection/' + action, json=body, headers=headers)
                assert response.status == 202, (action, await response.text())
                assert (await response.json()) == {'ok': True, 'result': {
                    'accepted': True, 'request_id': action + '-1', 'operation_id': 'op-1'}}
                assert minimal.calls[-1] == (action, body, 'alice')
            response = await client.get('/collection/status?session_id=s-1', headers=headers)
            assert response.status == 200 and (await response.json())['result'] == {'state': 'IDLE', 'query': {'session_id': 's-1'}}
            response = await client.get('/collection/operations/op-1', headers=headers)
            assert response.status == 200 and (await response.json())['result']['result']['episode_id'] == 'attempt-2'
            response = await client.get('/collection/status?project=lab', headers=headers)
            assert response.status == 200 and (await response.json())['result']['project'] == 'lab'
            response = await client.post('/collection/align', json={'project': 'lab', 'request_id': 'old'}, headers=headers)
            assert response.status == 404                                   # v10r15：没有对齐接口
            response = await client.post('/collection/sessions/s1/episodes/e1/redo',
                json={'project': 'lab', 'request_id': 'old-redo', 'recording_generation': 1}, headers=headers)
            assert response.status == 409
            assert (await response.json())['error']['code'] == 'API_VERSION_MISMATCH'
    asyncio.run(run())


def test_openapi_includes_minimal_default_and_preserves_legacy_start():
    from deployment_runtime.api_contract import openapi
    doc = openapi(); paths = doc['paths']
    assert '201' in paths['/collection/sessions']['post']['responses']
    assert '202' in paths['/collection/start']['post']['responses']
    assert '/collection/operations/{id}' in paths
    assert 'minimal' in str(doc).lower()


def test_actual_http_minimal_cycle_has_no_frontend_project_or_generation(tmp_path):
    from deployment_runtime.service import Platform
    from deployment_runtime.collection_contract import CollectionContract
    from deployment_runtime.store import Principal
    from test_collection_minimal_api import Engine
    from test_collection_contract_api import Outbox
    from lerobot_sdk.testing import simulation_config

    async def run():
        actor = Principal('alice', ('lab',), ('operator', 'admin'))
        engine = Engine(tmp_path / 'capture')
        platform = Platform({'role': 'collection', 'root': str(tmp_path), 'collection_project': 'lab',
            'collection_targets': {'lab': {'simulation_teleop': {'robot_id': 'sim', 'robot_revision': 'v1'}}}}, sessions=engine)
        sdk = dict(simulation_config('sim').__dict__); sdk['release_pose'] = None
        for key in ('units', 'limits', 'max_velocity', 'max_step', 'tolerance'): sdk[key] = dict(sdk[key])
        platform.register('robots', {'project': 'lab', 'id': 'sim', 'revision': 'v1', 'body': {
            'robot_id': 'sim', 'kind': 'simulation', 'sdk_config': sdk, 'camera_shapes': {'front': [12, 16, 3]},
            'scene_id': 'desk', 'scene_revision': 'v1', 'capabilities': ['teleop', 'collection'],
            'ros': {'namespace': '/minimal_http', 'domain_id': 95}}}, actor)
        platform.collection = CollectionContract(platform, uploads=Outbox(), target_probe=lambda *args: {
            'peer': {'owner': None, 'fault': None, 'holding': True, 'action_queue_empty': True}})
        account = {'name': 'alice', 'projects': ['lab'], 'roles': ['operator', 'admin'],
            'token_sha256': hashlib.sha256(b'test').hexdigest()}
        async with TestClient(TestServer(make_app(platform, [account]))) as client:
            headers = {'Authorization': 'Bearer test'}
            # v10r12：一台容器只采一台机器人，is_sim 传了也不起作用（不报错，结果里也没有）
            for query in ('', '?is_sim=true', '?is_sim=1'):
                response = await client.get('/collection/status' + query, headers=headers)
                value = (await response.json())['result']
                assert response.status == 200 and value['target_type'] == 'simulation' and 'is_sim' not in value
            async def post(action, body):
                response = await client.post('/collection/' + action, json=body, headers=headers)
                value = await response.json(); assert response.status == 202, value
                key = value['result']['operation_id']
                for _ in range(400):
                    response = await client.get('/collection/operations/' + key, headers=headers)
                    value = (await response.json())['result']
                    if value['state'] not in ('QUEUED', 'RUNNING'):
                        assert value['state'] == 'SUCCEEDED', value
                        return value['result']
                    await asyncio.sleep(.01)
                raise AssertionError('Operation did not settle')
            # 唯一绑定的是仿真：不传 is_sim 也采仿真；v10r15 起开始前不用对齐
            first = await post('start', {'is_sim': True, 'name': 'desk demo', 'request_id': 'start'})
            redone = await post('redo', {'session_id': first['session_id'], 'episode_id': first['episode_id'], 'request_id': 'redo'})
            assert first['episode_index'] == redone['episode_index'] == 0
            assert first['episode_id'] != redone['episode_id']
            assert not {'project', 'mode', 'recording_generation'} & set(redone)
            response = await client.post('/collection/complete', json={'session_id': first['session_id'],
                'episode_id': first['episode_id'], 'request_id': 'stale'}, headers=headers)
            assert (await response.json())['error']['code'] == 'STALE_EPISODE'
            second = await post('complete', {'session_id': first['session_id'], 'episode_id': redone['episode_id'], 'request_id': 'complete'})
            assert second['episode_index'] == 1 and second['episode_id'] != redone['episode_id']
            assert 'recording_generation' not in second['committed_episode']
            engine.live[first['session_id']]['current_frames'] = 0
            end = await post('finish', {'session_id': first['session_id'], 'request_id': 'finish'})
            assert end['holding'] is True and end['action_queue_empty'] is True
            assert not engine.live and not platform.store.claims()
    asyncio.run(run())
