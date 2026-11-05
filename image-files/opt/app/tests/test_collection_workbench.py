"""No hardware: assert operator orchestration against an injected platform."""
import copy
import json
import threading
import time
from pathlib import Path

import pytest

from collection_workbench.client import WorkbenchError
from collection_workbench.controller import Workbench, public


class Platform:
    token = 'shared-platform-secret'

    def __init__(self):
        self.calls = []
        self.jobs = {}
        self.verifications = {}
        self.collections = {}
        self.datasets = {}
        self.calibration = None
        self.release_calls = 0
        self.delay_start = None
        self.alignment_error = False
        self.catalog = {
            'robot': {'robot_id': 'sim', 'config_revision': 'cfg-2', 'device_config_revision': 'device-1', 'scene': 'scene-1'},
            'leader_robot': {'robot_id': 'leader', 'config_revision': 'leader-cfg', 'device_config_revision': 'leader-device'},
            'sources': ['com16_leader', 'scripted_test'], 'ros': {'domain_id': 93, 'namespace': '/sim', 'camera_topics': {'front': {'type': 'jpeg'}}},
            'calibration_revisions': [{'revision': 'baseline'}], 'converters': [], 'sinks': ['receiver']}

    def get(self, path):
        if path == '/v1/catalog':
            return copy.deepcopy(self.catalog)
        _, _, kind, ident = path.split('/')
        table = {'operations': self.jobs, 'collections': self.collections, 'verifications': self.verifications, 'datasets': self.datasets}[kind]
        if ident not in table:
            raise WorkbenchError('NOT_FOUND', 'Not registered yet')
        result = copy.deepcopy(table[ident])
        if kind == 'operations' and self.delay_start and not self.delay_start.is_set() and result.get('is_start'):
            result['status'] = 'RUNNING'
        return result

    def job(self, operation, result, **ids):
        ident = 'op' + str(len(self.jobs))
        self.jobs[ident] = {'status': 'SUCCEEDED', 'result': result, 'is_start': operation.endswith('.start')}
        return {'status': 'QUEUED', 'operation_id': ident, **ids}

    def command(self, op, p, rid):
        self.calls.append((op, copy.deepcopy(p), rid))
        if op == 'robot.claim':
            return p | {'epoch': 12, 'control_token': 'private-control-secret'}
        if op == 'robot.release':
            self.release_calls += 1
            return {'released_assignment': True}
        if op == 'simulation.verify.start':
            if self.alignment_error and p['control_parameters']['initial_alignment'] != 'explicit_move':
                raise WorkbenchError('ALIGNMENT_REQUIRED', 'Alignment needed', {'leader_target': {'elbow': 20}})
            ident = 'verify' + str(len(self.verifications))
            self.verifications[ident] = {'session_id': ident, 'state': 'RUNNING', 'source': p['source'],
                                        'binding': {'calibration_revision': p['calibration_revision']},
                                        'feedback_status': {'state': 'RUNNING', 'sent_target': {'elbow': 20}, 'feedback': {'elbow': 19}}}
            return self.job(op, {'session_id': ident}, session_id=ident)
        if op == 'simulation.verify.stop':
            self.verifications[p['session_id']]['state'] = 'STOPPED'
            return self.job(op, {'holding': True, 'action_queue_empty': True})
        if op == 'collection.start':
            ident = 'collect' + str(len(self.collections))
            self.collections[ident] = {'session_id': ident, 'dataset_id': ident, 'episode_id': 'ep1', 'state': 'RECORDING', 'source': p['source'], 'control_token': 'private-control-secret'}
            return self.job(op, {'session_id': ident, 'dataset_id': ident}, session_id=ident, dataset_id=ident)
        if op in ('collection.save', 'collection.redo'):
            value = self.collections[p['session_id']]
            assert p['episode_id'] == value['episode_id']
            value['state'] = 'RESETTING'
            return self.job(op, {'frames': 10, 'duration_s': 2, 'committed_episodes': 1})
        if op == 'collection.next':
            self.collections[p['session_id']].update(state='RECORDING', episode_id='ep2')
            return self.job(op, {'state': 'RECORDING'})
        if op == 'collection.stop':
            ident = p['session_id']
            self.collections[ident]['state'] = 'LOCAL_READY'
            self.datasets[ident] = {'dataset_id': ident, 'version': 'immutable-v1', 'local_state': 'LOCAL_READY', 'upload_state': 'READY'}
            return self.job(op, {'dataset_id': ident, 'sealed_state': 'LOCAL_READY', 'upload_state': 'READY'})
        if op == 'calibration.start':
            self.calibration = {'calibration_id': 'cal', 'state': 'WAITING_SAMPLES', 'points': {}}
            return {'calibration_id': 'cal', 'operation_id': 'manual-cal', 'status': 'WAITING_SAMPLES'}
        if op == 'calibration.status':
            return copy.deepcopy(self.calibration)
        if op == 'calibration.sample':
            self.calibration['points'][str(p['motor_id'])] = {p['point']: {'encoder': 1234}}
            return {'status': 'WAITING_SAMPLES'}
        if op in ('calibration.finish', 'calibration.cancel'):
            self.calibration['state'] = 'SUCCEEDED' if op.endswith('finish') else 'CANCELLED'
            return {'state': self.calibration['state']}
        if op in ('dataset.inspect', 'dataset.export', 'dataset.upload'):
            return self.job(op, {'dataset_id': p['dataset_id'], 'verified': True})
        if op == 'dataset.convert':
            assert p['source_version'] == 'immutable-v1'
            self.datasets['converted'] = {'dataset_id': 'converted', 'version': 'converted-v1', 'local_state': 'LOCAL_READY'}
            return self.job(op, {'dataset_id': 'converted', 'version': 'converted-v1'})
        raise AssertionError(op)


@pytest.fixture
def workbench(tmp_path):
    platform = Platform()
    config = {'state_file': str(tmp_path / 'private-state.json'), 'live_root': str(tmp_path), 'input_file': str(tmp_path / 'input.json'), 'operation_timeout_s': 3}
    live = {'source': {'id': 'com16_leader', 'fresh': True, 'age_s': .02}, 'telemetry': {}}
    wb = Workbench(config, platform, leader_reader=lambda: copy.deepcopy(live))
    wb.refresh()
    yield wb, platform, live
    wb.close()


def done(wb, command, payload=None, rid=None):
    rid = rid or 'req' + str(len(wb.context['requests']))
    wb.submit(command, payload or {}, rid)
    until = time.monotonic() + 4
    while time.monotonic() < until:
        value = wb.context['requests'][rid]
        if value['status'] in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            return value
        time.sleep(.01)
    raise AssertionError('Operator command did not complete')


def enter(wb):
    result = done(wb, 'enter_teaching', {'calibration_revision': 'baseline'})
    assert result['status'] == 'SUCCEEDED', result


def test_get_and_restart_never_claim_start_or_expose_credentials(workbench):
    wb, api, _ = workbench
    assert api.calls == []
    assert wb.state()['ready']
    enter(wb)
    raw = json.dumps(wb.state())
    assert 'private-control-secret' not in raw and 'shared-platform-secret' not in raw
    assert 'control_token' not in raw
    calls = len(api.calls)
    clone = Workbench(wb.config, api, leader_reader=wb.leader_reader)
    assert clone.state()['restart_reconciliation']
    clone.refresh()
    assert len(api.calls) == calls
    assert clone.state()['verification']['state'] == 'RUNNING'
    clone.close()


def test_duplicate_id_and_changed_content(workbench):
    wb, api, _ = workbench
    result = done(wb, 'enter_teaching', {'calibration_revision': 'baseline'}, 'same-id')
    calls = len(api.calls)
    duplicate = wb.submit('enter_teaching', {'calibration_revision': 'baseline'}, 'same-id')
    assert duplicate['status'] == result['status'] and len(api.calls) == calls
    with pytest.raises(WorkbenchError, match='different content'):
        wb.submit('align_enter', {'calibration_revision': 'baseline'}, 'same-id')


def test_real_source_required_and_alignment_explicit(workbench):
    wb, api, live = workbench
    live['source']['fresh'] = False
    assert done(wb, 'enter_teaching', {'calibration_revision': 'baseline'})['error']['code'] == 'SOURCE_STALE'
    assert not api.calls
    live['source']['fresh'] = True
    api.alignment_error = True
    error = done(wb, 'enter_teaching', {'calibration_revision': 'baseline'})
    assert error['error']['details']['leader_target'] == {'elbow': 20}
    assert done(wb, 'align_enter', {'calibration_revision': 'baseline'})['status'] == 'SUCCEEDED'
    starts = [p for op, p, _ in api.calls if op == 'simulation.verify.start']
    assert all(p['source'] == 'com16_leader' for p in starts)
    assert starts[-1]['control_parameters']['initial_alignment'] == 'explicit_move'


def test_collection_transition_full_sequence_and_version_contract(workbench):
    wb, api, _ = workbench
    assert done(wb, 'start_collection', {'name': 'demo', 'calibration_revision': 'baseline'})['error']['code'] == 'VERIFICATION_REQUIRED'
    enter(wb)
    assert done(wb, 'start_collection', {'name': 'demo', 'calibration_revision': 'baseline', 'fps': 5})['status'] == 'SUCCEEDED'
    ops = [x[0] for x in api.calls]
    assert ops.index('simulation.verify.stop') < ops.index('collection.start')
    request = next(p for op, p, _ in api.calls if op == 'collection.start')
    assert request['ros'] == api.catalog['ros']
    assert request['calibration_revision'] == 'baseline' and request['device_config_revision'] == 'device-1'
    wb.refresh()
    assert wb.state()['ready'] and wb.state()['datasets'][0]['local_state'] == 'NOT_SEALED'
    assert done(wb, 'redo_episode', {'confirm': True})['status'] == 'SUCCEEDED'
    assert done(wb, 'next_episode')['status'] == 'SUCCEEDED'
    assert done(wb, 'save_episode')['result']['frames'] == 10
    assert done(wb, 'stop', {'save_current': False})['result']['sealed_state'] == 'LOCAL_READY'
    assert wb.context['claim'] is not None
    assert api.release_calls == 0
    for command in ('inspect', 'export', 'upload'):
        assert done(wb, command, {'dataset_id': 'collect0', 'sink_id': 'receiver'})['status'] == 'SUCCEEDED'
    assert done(wb, 'convert', {'dataset_id': 'collect0', 'target_format': 'gr00t-v2.1', 'converter_version': 'frozen'})['status'] == 'SUCCEEDED'
    assert 'converted' in wb.context['dataset_ids']


def test_stop_priority_while_start_is_pending(workbench):
    wb, api, _ = workbench
    api.delay_start = threading.Event()
    wb.submit('enter_teaching', {'calibration_revision': 'baseline'}, 'start')
    until = time.monotonic() + 2
    while not api.jobs and time.monotonic() < until:
        time.sleep(.01)
    wb.submit('stop', {}, 'priority-stop')
    with pytest.raises(WorkbenchError, match='current operation'):
        wb.submit('calibration_start', {}, 'blocked')
    api.delay_start.set()
    until = time.monotonic() + 3
    while wb.context['requests']['priority-stop']['status'] not in ('SUCCEEDED', 'FAILED') and time.monotonic() < until:
        time.sleep(.01)
    assert wb.context['requests']['priority-stop']['result']['holding']
    assert api.verifications['verify0']['state'] == 'STOPPED'


def test_calibration_independent_target_manual_samples_and_no_auto_revision_switch(workbench):
    wb, api, _ = workbench
    enter(wb)
    start = done(wb, 'calibration_start')
    assert start['status'] == 'SUCCEEDED' and start['result']['status'] == 'WAITING_SAMPLES'
    assert wb.context['claim']['robot_id'] == 'leader'
    assert done(wb, 'calibration_sample', {'motor_id': 2, 'point': 'center'})['status'] == 'SUCCEEDED'
    assert done(wb, 'calibration_finish')['status'] == 'SUCCEEDED'
    assert api.verifications['verify0']['binding']['calibration_revision'] == 'baseline'
    with pytest.raises(WorkbenchError, match='Physical follower'):
        wb.submit('teleoperation.start', {}, 'physical')


def test_interrupted_start_reconciles_without_replay(workbench):
    wb, api, _ = workbench
    wb.context['requests']['pending'] = {'request_id': 'pending', 'command': 'enter_teaching', 'status': 'RUNNING', 'digest': 'abc'}
    wb.persist()
    clone = Workbench(wb.config, api, leader_reader=wb.leader_reader)
    clone.refresh()
    assert clone.context['requests']['pending']['status'] == 'INTERRUPTED'
    assert not api.calls
    clone.close()


def test_late_or_missing_relay_never_fresh(workbench):
    wb, _, _ = workbench
    input_path = Path(wb.config['input_file'])
    now = time.monotonic()
    input_path.write_text(json.dumps({'sequence': 1, 'expires_monotonic': now + .5, 'received_monotonic': now, 'native_positions': {'1': 1234}}))
    assert wb.read_leader()['source']['fresh'] is False
    live = Path(wb.config['live_root']) / 'status.json'
    live.write_text(json.dumps({'updated_unix': time.time()}))
    assert wb.read_leader()['source']['fresh'] is True
    input_path.write_text(json.dumps({'sequence': 1, 'expires_monotonic': now - 1, 'received_monotonic': now - 2}))
    assert wb.read_leader()['source']['fresh'] is False


def test_recursive_secret_redaction():
    assert public({'control_token': 'x', 'binding': {'signature': 'y', 'positions': [1, 2]}}) == {'binding': {'positions': [1, 2]}}


def test_alignment_phase_and_units_are_visible_and_old_verification_not_reused(workbench):
    wb, api, _ = workbench
    enter(wb)
    api.verifications['verify0']['binding']['contract'] = {'units': {'elbow': 'deg'}}
    api.verifications['verify0']['feedback_status'].update(state='ALIGNING', alignment={'phase': 'ALIGNING', 'completed': False})
    wb.refresh()
    state = wb.state()
    assert state['telemetry']['alignment']['state'] == 'ALIGNING'
    assert state['telemetry']['units'] == {'elbow': 'deg'}
    assert done(wb, 'start_collection', {'name': 'test', 'calibration_revision': 'baseline'})['error']['code'] == 'VERIFICATION_REQUIRED'
    api.verifications['verify0']['feedback_status']['state'] = 'RUNNING'
    assert done(wb, 'start_collection', {'name': 'test', 'calibration_revision': 'baseline'})['status'] == 'SUCCEEDED'
    wb.refresh()
    assert 'sent' not in wb.state()['telemetry']


def test_http_same_origin_csrf_and_state_no_mutations(workbench):
    pytest.importorskip('aiohttp')
    import asyncio
    from aiohttp.test_utils import TestClient, TestServer
    from collection_workbench.server import make_app
    wb, api, _ = workbench

    async def exercise():
        app = make_app({**wb.config, 'allowed_hosts': ['127.0.0.1:18880']}, wb)
        async with TestClient(TestServer(app, port=18880)) as client:
            response = await client.get('/api/state')
            state = await response.json()
            assert state['csrf_token'] and not api.calls
            response = await client.post('/api/command', json={'command': 'enter_teaching', 'request_id': 'http-enter', 'payload': {'calibration_revision': 'baseline'}})
            assert response.status == 403
            response = await client.post('/api/command', headers={'Origin': 'http://127.0.0.1:18880', 'X-CSRF-Token': state['csrf_token']},
                                         json={'command': 'enter_teaching', 'request_id': 'http-enter', 'payload': {'calibration_revision': 'baseline'}})
            assert response.status == 202
            response = await client.get('/api/state', headers={'Origin': 'https://untrusted.example'})
            assert response.status == 403
    asyncio.run(exercise())
