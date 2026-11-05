"""v9 admission/queue tests: real journals and registered targets, no device I/O."""
import copy
import threading
import time

import pytest

from deployment_runtime.collection_contract import CollectionContract
from deployment_runtime.service import Platform
from deployment_runtime.store import Fault, Principal, canonical, digest
from lerobot_sdk.testing import simulation_config
from test_collection_contract_api import CaptureEngine, Outbox


class Engine(CaptureEngine):
    def __init__(self, root):
        super().__init__(root); self.calls = []; self.gate = None; self.cancelled = set()
    def start(self, key, robot, payload, *, verification=False):
        self.calls.append(('start', key, copy.deepcopy(payload), verification, robot['revision']))
        self.live[key] = {'state': 'STARTING', 'current_frames': 0, 'committed_episodes': 0, 'verification': verification}
        while self.gate is not None and not self.gate.wait(.01):
            if key in self.cancelled: raise Fault('STOPPED', 'Start cancelled')
        if key in self.cancelled: raise Fault('STOPPED', 'Start cancelled')
        if verification:
            self.live[key].update(state='RUNNING', verification=True)
            return {'state': 'RUNNING', 'recording': False}
        return super().start(key, robot, payload)
    def command(self, key, operation, payload):
        if self.live[key].get('verification'):
            assert operation == 'stop'; self.live.pop(key)
            return {'holding': True, 'action_queue_empty': True, 'recording': False}
        return super().command(key, operation, payload)
    def cancel_start(self, key): self.cancelled.add(key)
    def close(self):
        if self.gate: self.gate.set()
        return []


@pytest.fixture
def system(tmp_path):
    from deployment_runtime.collection_minimal import MinimalCollectionAPI
    actor = Principal('alice', ('lab',), ('operator', 'admin'))
    engine = Engine(tmp_path / 'capture')
    platform = Platform({'role': 'collection', 'root': str(tmp_path), 'collection_project': 'lab',
        'collection_targets': {'lab': {'default_mode': 'simulation_teleop',
            'simulation_teleop': {'robot_id': 'sim', 'robot_revision': 'v1'}}}}, sessions=engine)
    cfg = simulation_config('sim'); sdk = dict(cfg.__dict__); sdk['release_pose'] = None
    for key in ('units', 'limits', 'max_velocity', 'max_step', 'tolerance'): sdk[key] = dict(sdk[key])
    platform.register('robots', {'project': 'lab', 'id': 'sim', 'revision': 'v1', 'body': {
        'robot_id': 'sim', 'kind': 'simulation', 'sdk_config': sdk, 'camera_shapes': {'front': [12, 16, 3]},
        'scene_id': 'desk', 'scene_revision': 'v1', 'calibration_revision': 'baseline',
        'capabilities': ['teleop', 'collection'], 'ros': {'namespace': '/minimal', 'domain_id': 95}}}, actor)
    contract = CollectionContract(platform, uploads=Outbox(), target_probe=lambda *args: {
        'peer': {'owner': None, 'fault': None, 'holding': True, 'action_queue_empty': True}})
    platform.collection = contract
    api = MinimalCollectionAPI(platform)
    yield api, platform, contract, engine, actor
    if engine.gate: engine.gate.set()
    api.close(); platform.close()


def wait(api, ack, actor):
    end = time.monotonic() + 4
    while time.monotonic() < end:
        operation = api.operation(ack['operation_id'], actor)
        if operation['state'] not in ('QUEUED', 'RUNNING'): return operation
        time.sleep(.01)
    raise AssertionError('Local command did not finish: ' + repr(operation))


def started(api, actor):
    # v10r15：开始采集前不用对齐（对齐由操作者自己负责，平台不管）
    operation = wait(api, api.submit('start', {'is_sim': True, 'name': 'demonstration',
        'request_id': 'start-' + str(time.monotonic_ns())}, actor), actor)
    assert operation['state'] == 'SUCCEEDED', operation
    return operation['result']


def test_ack_is_durable_and_does_not_wait_for_device_io(system):
    api, _, _, engine, actor = system; engine.gate = threading.Event()
    body = {'is_sim': True, 'name': 'slow', 'request_id': 'slow-start'}
    tick = time.monotonic(); ack = api.submit('start', body, actor)
    assert time.monotonic() - tick < .5
    assert set(ack) == {'accepted', 'request_id', 'operation_id'} and ack['accepted'] is True
    assert api.submit('start', body, actor) == ack
    with pytest.raises(Fault) as error: api.submit('start', {**body, 'fps': 6}, actor)
    assert error.value.code == 'IDEMPOTENCY_CONFLICT'
    engine.gate.set()
    operation = wait(api, ack, actor)
    assert operation['state'] == 'SUCCEEDED' and 'is_sim' not in operation['result']
    assert not any('workspace_confirmed' in call[2] for call in engine.calls)


def test_there_is_no_align_action(system):
    api, _, _, engine, actor = system
    with pytest.raises(Fault) as error: api.submit('align', {'request_id': 'align'}, actor)
    assert error.value.code == 'INVALID_ACTION' and not engine.calls


@pytest.mark.parametrize('field', ['project', 'robot_id', 'robot_revision', 'recording_generation',
    'config_revision', 'calibration_revision', 'scene_id', 'scene_revision', 'workspace_confirmed'])
def test_frontend_cannot_supply_server_owned_fields(system, field):
    api, _, _, engine, actor = system
    with pytest.raises(Fault) as error:
        api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'forbidden-field', field: 'value'}, actor)
    assert error.value.code == 'INVALID_FIELDS' and not engine.calls


def test_the_one_configured_target_is_used_and_is_sim_is_ignored(system):
    # v10r12：一台容器只采一台机器人。is_sim 不管传什么（包括不合法的值）都不起作用，目标就是唯一绑定的仿真。
    api, platform, _, engine, actor = system
    ack = api.submit('start', {'is_sim': 'true', 'name': 'x', 'request_id': 'start-any-is-sim'}, actor)
    assert wait(api, ack, actor)['state'] == 'SUCCEEDED' and engine.calls[-1][4] == 'v1'
    # 没有绑定任何目标的容器（例如真机没准备好）明确报错，不会改采别的
    targets = platform.config['collection_targets']
    platform.config['collection_targets'] = {}
    try:
        with pytest.raises(Fault) as error: api.submit('start', {'name': 'x', 'request_id': 'nothing-bound'}, actor)
        assert error.value.code == 'COLLECTION_TARGET_NOT_CONFIGURED'
    finally:
        platform.config['collection_targets'] = targets


def test_target_status_ignores_finished_legacy_session_identity(system):
    api, _, contract, engine, actor = system
    old = contract.start({'project': 'lab', 'robot_id': 'sim', 'robot_revision': 'v1', 'mode': 'simulation_teleop',
        'name': 'old', 'calibration_revision': 'baseline', 'scene_id': 'desk', 'scene_revision': 'v1'}, 'old-start', actor)
    engine.live[old['session_id']]['current_frames'] = 0
    contract.finish(old['session_id'], {'project': 'lab'}, 'old-finish', actor)
    end = time.monotonic() + 3
    while contract._read(old['session_id'], actor).get('finish', {}).get('state') != 'EMPTY':
        assert time.monotonic() < end
        time.sleep(.01)
    result = api.status({'is_sim': True}, actor)
    assert result['state'] == 'IDLE' and result['target_type'] == 'simulation' and 'is_sim' not in result
    assert 'session_id' not in result and 'finished' not in result


def test_start_needs_no_alignment(system):
    api, _, _, engine, actor = system
    result = wait(api, api.submit('start', {'is_sim': True, 'name': 'x' * 180, 'request_id': 'valid-start'}, actor), actor)
    assert result['state'] == 'SUCCEEDED' and 'is_sim' not in result['result']
    assert 'project' not in result['result'] and 'recording_generation' not in result['result']
    assert engine.calls[-1][2]['name'] == 'x' * 180


def test_redo_rotates_episode_and_rejects_old_identity(system):
    api, _, contract, _, actor = system; first = started(api, actor)
    redo = wait(api, api.submit('redo', {'is_sim': True, 'session_id': first['session_id'],
        'episode_id': first['episode_id'], 'request_id': 'redo'}, actor), actor)
    assert redo['state'] == 'SUCCEEDED' and redo['result']['episode_id'] != first['episode_id']
    assert redo['result']['episode_index'] == first['episode_index']
    assert contract._read(first['session_id'], actor)['api_contract'] == 'minimal.v9'
    with pytest.raises(Fault) as error:
        api.submit('complete', {'is_sim': True, 'session_id': first['session_id'],
            'episode_id': first['episode_id'], 'request_id': 'late-complete'}, actor)
    assert error.value.code == 'STALE_EPISODE'


def test_legacy_session_cannot_be_changed_by_minimal_api(system):
    api, _, contract, _, actor = system
    old = contract.start({'project': 'lab', 'robot_id': 'sim', 'robot_revision': 'v1', 'mode': 'simulation_teleop',
        'name': 'old', 'calibration_revision': 'baseline', 'scene_id': 'desk', 'scene_revision': 'v1'}, 'old', actor)
    with pytest.raises(Fault) as error:
        api.submit('finish', {'is_sim': True, 'session_id': old['session_id'], 'request_id': 'old-finish'}, actor)
    assert error.value.code == 'API_VERSION_MISMATCH'


def test_operations_are_owner_protected_and_only_one_robot_mutation_is_queued(system, monkeypatch):
    api, _, _, _, actor = system; monkeypatch.setattr(api, '_kick', lambda *args: None)
    ack = api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'queued'}, actor)
    with pytest.raises(Fault) as error: api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'another'}, actor)
    assert error.value.code == 'BUSY'
    with pytest.raises(Fault) as error:
        api.operation(ack['operation_id'], Principal('bob', ('lab',), ('operator',)))
    assert error.value.code == 'FORBIDDEN'


def test_restart_interrupts_a_queued_start_without_replay(system, monkeypatch):
    from deployment_runtime.collection_minimal import MinimalCollectionAPI
    api, platform, _, engine, actor = system
    monkeypatch.setattr(api, '_kick', lambda *args: None)
    ack = api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'restart-queued'}, actor)
    api.close(); replacement = MinimalCollectionAPI(platform)
    try:
        assert replacement.operation(ack['operation_id'], actor)['state'] == 'INTERRUPTED' and not engine.calls
    finally: replacement.close()


def test_finish_empty_batch_reuses_durable_close_and_does_not_fake_hold(system):
    api, _, contract, engine, actor = system; first = started(api, actor)
    engine.live[first['session_id']]['current_frames'] = 0
    ack = api.submit('finish', {'is_sim': True, 'session_id': first['session_id'], 'request_id': 'end'}, actor)
    operation = wait(api, ack, actor)
    assert operation['state'] == 'SUCCEEDED', operation
    assert contract._read(first['session_id'], actor)['finish']['state'] == 'EMPTY'
    assert operation['result']['holding'] is True and operation['result']['action_queue_empty'] is True


def test_registered_ros_subset_can_be_asserted_on_start(system):
    api, _, _, engine, actor = system
    operation = wait(api, api.submit('start', {'is_sim': True, 'name': 'x', 'ros': {'domain_id': 95},
        'request_id': 'ros-start'}, actor), actor)
    assert operation['state'] == 'SUCCEEDED', operation
    assert engine.calls[0][2].get('workspace_confirmed') is None


def test_finish_requests_are_bounded_and_same_request_retry_is_stable(system, monkeypatch):
    api, _, _, _, actor = system; first = started(api, actor)
    monkeypatch.setattr(api, '_kick', lambda *args: None)
    body = {'session_id': first['session_id'], 'request_id': 'finish-one'}
    ack = api.submit('finish', body, actor)
    assert api.submit('finish', body, actor) == ack
    with pytest.raises(Fault) as error: api.submit('finish', {**body, 'request_id': 'finish-two'}, actor)
    assert error.value.code == 'BUSY'


def test_status_for_closed_session_does_not_return_a_newer_session(system):
    api, _, _, engine, actor = system; first = started(api, actor)
    engine.live[first['session_id']]['current_frames'] = 0
    assert wait(api, api.submit('finish', {'session_id': first['session_id'], 'request_id': 'close-first'}, actor), actor)['state'] == 'SUCCEEDED'
    second = started(api, actor)
    assert second['session_id'] != first['session_id']
    historical = api.status({'session_id': first['session_id']}, actor)
    assert historical['session_id'] == first['session_id'] and historical['active'] is False
    assert api.status({'session_id': second['session_id']}, actor)['session_id'] == second['session_id']


def test_minimal_close_cancels_owned_pending_start_and_joins_before_database_close(system):
    api, platform, _, engine, actor = system; engine.gate = threading.Event()
    platform.config['collection_shutdown_timeout_s'] = .5
    api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'closing-start'}, actor)
    until = time.monotonic() + 2
    while not engine.calls and time.monotonic() < until: time.sleep(.005)
    assert engine.calls
    api.close()
    assert engine.calls[0][1] in engine.cancelled
    assert not any(worker.is_alive() for worker in api.workers.values())


def test_queued_commands_use_frozen_revision_and_do_not_follow_binding_changes(system, monkeypatch):
    api, platform, _, engine, actor = system
    kick = api._kick; monkeypatch.setattr(api, '_kick', lambda *args: None)
    ack = api.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'frozen-revision'}, actor)
    platform.config['collection_targets']['lab']['simulation_teleop']['robot_revision'] = 'v2'
    monkeypatch.setattr(api, '_kick', kick); kick(ack['operation_id'])
    assert wait(api, ack, actor)['state'] == 'SUCCEEDED'
    assert engine.calls[0][4] == 'v1'


def test_queued_episode_change_rejects_late_identity_instead_of_following_current(system, monkeypatch):
    api, _, contract, _, actor = system; first = started(api, actor)
    kick = api._kick; monkeypatch.setattr(api, '_kick', lambda *args: None)
    ack = api.submit('complete', {'session_id': first['session_id'], 'episode_id': first['episode_id'],
        'request_id': 'queued-old-episode'}, actor)
    row = contract._read(first['session_id'], actor)
    contract.minimal_redo(row['session_id'], row['episode_id'],
        {'project': row['project'], 'recording_generation': row['recording_generation']}, 'external-redo', actor)
    monkeypatch.setattr(api, '_kick', kick); kick(ack['operation_id'])
    operation = wait(api, ack, actor)
    assert operation['state'] == 'FAILED' and operation['error']['code'] == 'STALE_EPISODE'


def test_finish_can_cancel_start_that_has_not_created_a_session(system, monkeypatch):
    api, _, _, engine, actor = system
    kick = api._kick; monkeypatch.setattr(api, '_kick', lambda *args: None)
    start = api.submit('start', {'is_sim': True, 'name': 'cancelled', 'request_id': 'queued-start'}, actor)
    end = api.submit('finish', {'operation_id': start['operation_id'], 'request_id': 'end-queued-start'}, actor)
    monkeypatch.setattr(api, '_kick', kick); kick(end['operation_id'])
    operation = wait(api, end, actor)
    assert operation['state'] == 'SUCCEEDED' and operation['result']['cancelled_before_start'] is True
    assert 'holding' not in operation['result'] and 'action_queue_empty' not in operation['result']
    kick(start['operation_id']); assert wait(api, start, actor)['state'] == 'FAILED'
    assert not engine.calls  # The cancelled start never reached the device.


def test_session_status_finish_intent_has_priority_over_running_recording(system):
    api, _, contract, _, actor = system; first = started(api, actor)
    row = contract._read(first['session_id'], actor)
    row['finish'] = {'state': 'QUEUED'}; contract._write(row)
    status = api.status({'session_id': first['session_id']}, actor)
    assert status['state'] == 'FINISHING' and status['finish_state'] == 'QUEUED'
    assert status['session_id'] == first['session_id']


def test_session_status_preserves_reconciliation_fault_instead_of_waiting_forever(system):
    api, _, contract, _, actor = system; first = started(api, actor)
    row = contract._read(first['session_id'], actor)
    row.update(internal_state='NEEDS_RECONCILE', error='Recorder stopped unexpectedly'); contract._write(row)
    status = api.status({'session_id': first['session_id']}, actor)
    assert status['state'] == 'NEEDS_RECONCILE' and status['error'] == row['error']
