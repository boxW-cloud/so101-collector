"""v10 current-container commands; device I/O is a software seam."""
import json
import threading
import time
import uuid

import pytest

from deployment_runtime.store import Fault, Principal
from test_collection_minimal_api import system as minimal_system


@pytest.fixture
def system(minimal_system):
    from deployment_runtime.collection_current import CurrentCollectionAPI
    legacy, platform, contract, engine, actor = minimal_system
    api = CurrentCollectionAPI(platform)
    yield api, legacy, platform, contract, engine, actor
    if engine.gate: engine.gate.set()
    api.close()


def latest(api):
    with api.lock:
        key = api.db.execute('SELECT id FROM commands ORDER BY created DESC LIMIT 1').fetchone()['id']
    return api._entry(key)


def wait(api):
    end = time.monotonic() + 5
    while time.monotonic() < end:
        item = latest(api)
        if item['state'] not in ('QUEUED', 'RUNNING'): return item
        time.sleep(.01)
    raise AssertionError(item)


URL = '/xxx/action-composer/data/artifacts'


def new_id():
    return 'batch-' + uuid.uuid4().hex[:12]


def start(api, actor, batch_id=None):
    # v10r15：开始前不用对齐（对齐由操作者自己负责，平台不管）
    api.submit('start', {'id': batch_id or new_id(), 'name': 'test', 'is_sim': True}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    return result['result']


def test_start_returns_only_acceptance_without_waiting_for_device(system):
    api, _, _, _, engine, actor = system
    engine.gate = threading.Event()
    tick = time.monotonic()
    ack = api.submit('start', {'id': new_id(), 'name': 'slow'}, actor)
    assert time.monotonic() - tick < .5
    assert ack == {'accepted': True, 'message': '已接收，后台执行中'}
    engine.gate.set()
    assert wait(api)['state'] == 'SUCCEEDED'


def test_there_is_no_align_action(system):
    api, _, _, _, engine, actor = system
    with pytest.raises(Fault) as error: api.submit('align', {}, actor)
    assert error.value.code == 'INVALID_ACTION' and not engine.calls


def test_no_external_identifiers_and_start_requires_id_and_name(system):
    api, _, _, _, engine, actor = system
    for body in ({'request_id': 'x'}, {'session_id': 'x'}):
        with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x', **body}, actor)
        assert error.value.code == 'INVALID_FIELDS'
    for body in ({'name': 'test'}, {'id': new_id()}, {'id': new_id(), 'name': 'x', 'dataset_id': 'old-r6-field'}):
        with pytest.raises(Fault) as error: api.submit('start', body, actor)
        assert error.value.code == 'INVALID_FIELDS'
    for bad in ('', '-leading-dash', 'has space', 'a' * 129, 7):
        with pytest.raises(Fault) as error: api.submit('start', {'id': bad, 'name': 'test'}, actor)
        assert error.value.code == 'INVALID_ID'
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': ' '}, actor)
    assert error.value.code == 'INVALID_NAME'
    assert not engine.calls


def test_start_needs_no_alignment(system):
    # 8910：没有对齐。直接开始，不检查对齐凭据、不自动对齐
    api, _, _, _, engine, actor = system
    batch = new_id()
    api.submit('start', {'id': batch, 'name': 'no-align'}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    assert result['result']['session_id'] == batch
    assert [call[3] for call in engine.calls] == [False]              # 只有开始，没有对齐


def wait_abandoned(platform):
    end = time.monotonic() + 15
    while getattr(platform, 'collection_abandoning', False) and time.monotonic() < end:
        time.sleep(.01)
    assert not getattr(platform, 'collection_abandoning', False)


def test_abandon_takes_no_parameters_and_is_harmless_when_idle(system):
    api, _, platform, contract, engine, actor = system
    with pytest.raises(Fault) as error: api.abandon({'id': 'x'}, actor)
    assert error.value.code == 'INVALID_FIELDS'
    assert api.abandon({}, actor) == {'accepted': True, 'message': '已接收，后台执行中'}
    wait_abandoned(platform)
    start(api, actor)                                               # 之后照常开始新批次


def test_abandon_cancels_a_recording_batch_without_uploading_it(system):
    api, _, platform, contract, engine, actor = system
    batch = new_id()
    start(api, actor, batch)
    api.submit('complete', {}, actor)
    assert wait(api)['state'] == 'SUCCEEDED'                        # 已经完成一条
    api.abandon({}, actor)
    wait_abandoned(platform)
    row = contract._read(batch, actor)
    assert row['finish']['state'] == 'ABANDONED' and row['internal_state'] == 'STOPPED'
    assert not engine.live and not platform.store.claims() and not contract.uploads.rows
    with pytest.raises(Fault) as error: api.submit('finish', {}, actor)
    assert error.value.code == 'NO_CURRENT_COLLECTION'
    with pytest.raises(Fault) as error: api.submit('start', {'id': batch, 'name': 'again'}, actor)
    assert error.value.code == 'ID_ALREADY_USED'                    # 放弃的 id 不能再用
    start(api, actor)                                               # 新 id 照常开始
    from pathlib import Path
    assert json.loads((Path(platform.root) / 'collection-abandon-last.json').read_text())['abandoned_batches'] == [batch]


def test_abandon_cancels_a_stuck_start_and_its_worker_cannot_overwrite_it(system):
    api, _, platform, contract, engine, actor = system
    engine.gate = threading.Event()                                 # 设备启动卡住
    batch = new_id()
    api.submit('start', {'id': batch, 'name': 'stuck'}, actor)
    end = time.monotonic() + 3
    while batch not in engine.live and time.monotonic() < end:
        time.sleep(.01)
    api.abandon({}, actor)
    wait_abandoned(platform)
    command = latest(api)
    assert command['action'] == 'start' and command['state'] == 'INTERRUPTED'
    assert 'ABANDONED' in json.dumps(command['error'])
    assert not platform.store.claims() and not engine.live
    assert contract._read(batch, actor)['finish']['state'] == 'ABANDONED'
    engine.gate = None
    start(api, actor)


def test_abandon_clears_a_batch_left_behind_by_a_restart(system):
    api, _, platform, contract, engine, actor = system
    batch = new_id()
    start(api, actor, batch)
    engine.live.clear()                                             # 容器重启：录制进程没了，占用待核对
    with platform.store.lock:
        platform.store.db.execute("UPDATE claims SET state='RECONCILE_REQUIRED'")
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'blocked'}, actor)
    assert error.value.code == 'BUSY'
    api.abandon({}, actor)
    wait_abandoned(platform)
    assert not platform.store.claims() and contract._read(batch, actor)['finish']['state'] == 'ABANDONED'
    start(api, actor)


def test_commands_wait_while_abandoning(system):
    api, _, platform, contract, engine, actor = system
    platform.collection_abandoning = True
    try:
        with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
        assert error.value.code == 'BUSY'
    finally:
        platform.collection_abandoning = False


def test_minimal_start_uses_the_one_configured_target_and_ignores_is_sim(system):
    # v10r12：一台容器只采一台机器人，目标就是部署配置里唯一绑定的那个（这里是仿真）。
    # 老前端还传 is_sim 也不起作用：传 false 照样采仿真，不会报错。
    api, _, platform, contract, engine, actor = system
    batch = new_id()
    api.submit('start', {'id': batch, 'name': 'minimal', 'is_sim': False}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    assert result['result']['session_id'] == batch and 'is_sim' not in result['result']
    assert contract._read(batch, actor)['target_type'] == 'simulation'


def test_upstream_id_becomes_session_and_dataset_id_and_cannot_be_reused(system):
    api, _, platform, contract, engine, actor = system
    batch = new_id()
    first = start(api, actor, batch)
    assert first['session_id'] == batch and first['dataset_id'] == batch
    row = contract._read(batch, actor)
    assert row['session_id'] == row['dataset_id'] == batch
    assert platform.store.job(batch, actor)['kind'] == 'collections.start'
    engine.live[batch]['current_frames'] = 0
    api.submit('finish', {'upload_url': URL}, actor)
    assert wait(api)['state'] == 'SUCCEEDED'
    with pytest.raises(Fault) as error: api.submit('start', {'id': batch, 'name': 'again'}, actor)
    assert error.value.code == 'ID_ALREADY_USED' and error.value.status == 409


def test_finish_upload_url_is_optional_but_validated(system):
    api, _, _, _, engine, actor = system
    start(api, actor)
    for body in ({'extra': 1}, {'upload_url': URL, 'extra': 1}):
        with pytest.raises(Fault) as error: api.submit('finish', body, actor)
        assert error.value.code == 'INVALID_FIELDS'
    for bad in ('http://192.168.1.16/api/complete', 'relative/artifacts', '/', '/data/../etc', '/data/.hidden',
                '/data/-rf', '/data/with space', '/' + 'a' * 1100):
        with pytest.raises(Fault) as error: api.submit('finish', {'upload_url': bad}, actor)
        assert error.value.code == 'INVALID_UPLOAD_URL'
    assert engine.live   # 参数错误不会结束采集


def test_empty_redo_freezes_current_attempt_and_generates_new_id(system):
    api, _, _, contract, _, actor = system
    before = start(api, actor)
    api.submit('redo', {}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    assert result['result']['episode_id'] != before['episode_id']
    assert result['result']['episode_index'] == 0
    assert contract._read(before['session_id'], actor)['api_contract'] == 'minimal.v10'


def test_no_current_session_is_an_immediate_error(system):
    api, _, _, _, _, actor = system
    for action, body in (('redo', {}), ('complete', {}), ('finish', {'upload_url': URL})):
        with pytest.raises(Fault) as error: api.submit(action, body, actor)
        assert error.value.code == 'NO_CURRENT_COLLECTION'


def test_stopped_legacy_history_is_preserved_without_becoming_v10_current(system):
    api,_,platform,contract,engine,actor=system
    payload={'project':'lab','robot_id':'sim','robot_revision':'v1','mode':'simulation_teleop',
        'name':'historical','calibration_revision':'baseline','scene_id':'desk','scene_revision':'v1'}
    first=contract.start(payload,'old-start',actor)
    second=contract.complete(first['session_id'],first['episode_id'],
        {'project':'lab','recording_generation':1},'old-complete',actor)
    contract.stop(first['session_id'],{'project':'lab'},'old-stop',actor)
    before=contract._read(first['session_id'],actor)
    assert before['collection_episodes'] and before['stop_result']['preserved_current_frames']>0
    with pytest.raises(Fault) as error:api.submit('redo',{},actor)
    assert error.value.code=='NO_CURRENT_COLLECTION'
    assert contract._read(first['session_id'],actor)==before


def test_other_owner_cannot_operate_current_session(system):
    api, _, _, _, _, actor = system
    start(api, actor)
    other = Principal('bob', ('lab',), ('operator',))
    with pytest.raises(Fault) as error: api.submit('redo', {}, other)
    assert error.value.code == 'FORBIDDEN'


def test_legacy_and_current_pending_commands_share_admission(system, monkeypatch):
    api, legacy, _, _, _, actor = system
    monkeypatch.setattr(legacy, '_kick', lambda *args: None)
    legacy.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'v9-pending'}, actor)
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    assert error.value.code == 'BUSY'


def test_current_pending_blocks_legacy_and_double_click(system, monkeypatch):
    api, legacy, _, _, _, actor = system
    monkeypatch.setattr(api, '_kick', lambda *args: None)
    api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    with pytest.raises(Fault) as error:
        legacy.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'v9-pending'}, actor)
    assert error.value.code == 'BUSY'
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    assert error.value.code == 'BUSY'


def test_restart_interrupts_a_pending_start_without_replay(system, monkeypatch):
    from deployment_runtime.collection_current import CurrentCollectionAPI
    api, _, platform, _, engine, actor = system
    monkeypatch.setattr(api, '_kick', lambda *args: None)
    api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    api.close()
    replacement = CurrentCollectionAPI(platform)
    try:
        assert latest(replacement)['state'] == 'INTERRUPTED'
        assert not engine.calls
    finally: replacement.close()


def test_finish_cancels_pending_start_without_exposing_ids(system):
    api, _, _, contract, engine, actor = system
    engine.gate = threading.Event()
    api.submit('start', {'id': new_id(), 'name': 'test', 'is_sim': True}, actor)
    end = time.monotonic() + 3
    while len(engine.calls) < 1:
        assert time.monotonic() < end
        time.sleep(.01)
    ack = api.submit('finish', {'upload_url': URL}, actor)
    assert set(ack) == {'accepted', 'message'}
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    assert engine.cancelled
    assert not engine.live


def test_complete_local_then_finish_uses_frozen_destination(system):
    api, _, platform, contract, engine, actor = system
    first = start(api, actor)
    api.submit('complete', {}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED', result
    assert result['result']['episode_index'] == 1
    assert not contract.uploads.rows
    engine.live[first['session_id']]['current_frames'] = 0
    ack = api.submit('finish', {'upload_url': URL}, actor)
    assert set(ack) == {'accepted', 'message'}
    assert wait(api)['state'] == 'SUCCEEDED'
    jobs = list(contract.uploads.rows.values())
    assert len(jobs) == 1 and jobs[0]['payload']['destination'] == {'protocol': 'so101.sync.v2', 'upload_url': URL}
    assert contract._read(first['session_id'], actor)['finish']['state'] == 'QUEUED_UPLOAD'
    assert not platform.store.claims()


def test_queued_start_can_be_finished_before_any_session_exists(system, monkeypatch):
    api, _, _, contract, engine, actor = system
    original = api._kick
    monkeypatch.setattr(api, '_kick', lambda key: None)
    api.submit('start', {'id': new_id(), 'name': 'queued', 'is_sim': True}, actor)
    queued = latest(api)
    assert queued['state'] == 'QUEUED'
    monkeypatch.setattr(api, '_kick', original)
    api.submit('finish', {'upload_url': URL}, actor)
    result = wait(api)
    assert result['state'] == 'SUCCEEDED' and result['result']['cancelled_before_start']
    assert not engine.calls and not engine.live
    with contract.lock: assert contract.db.execute('SELECT count(*) FROM sessions').fetchone()[0] == 0


def test_accepted_redo_never_rebinds_to_a_new_attempt(system, monkeypatch):
    api, _, _, contract, engine, actor = system
    first = start(api, actor)
    monkeypatch.setattr(api, '_kick', lambda key: None)
    api.submit('redo', {}, actor)
    accepted = latest(api)
    row = contract._read(first['session_id'], actor)
    row['episode_id'] = 'internally-changed-attempt'
    row['recording_generation'] += 1
    contract._write(row)
    api._work(accepted['id'])
    result = latest(api)
    assert result['state'] == 'FAILED' and result['error']['code'] == 'STALE_EPISODE'
    assert not engine.discards


def test_stopped_unfinished_batch_is_still_current(system):
    api, legacy, _, contract, engine, actor = system
    first = start(api, actor)
    engine.live[first['session_id']]['current_frames'] = 0
    contract.stop(first['session_id'], {'project': 'lab'}, 'safe-stop', actor)
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    assert error.value.code == 'BUSY'
    with pytest.raises(Fault) as error:
        legacy.submit('start', {'is_sim': True, 'name': 'x', 'request_id': 'v9-after-stop'}, actor)
    assert error.value.code == 'BUSY'
    api.submit('finish', {'upload_url': URL}, actor)
    assert wait(api)['state'] == 'SUCCEEDED'
    start(api, actor)                                               # 结束后照常开始新批次


def test_current_cannot_adopt_a_legacy_active_batch(system):
    from test_collection_minimal_api import started
    api, legacy, _, _, _, actor = system
    started(legacy, actor)
    with pytest.raises(Fault) as error: api.submit('redo', {}, actor)
    assert error.value.code == 'API_VERSION_MISMATCH'
    with pytest.raises(Fault) as error: api.submit('start', {'id': new_id(), 'name': 'x'}, actor)
    assert error.value.code == 'BUSY'


def test_restart_resumes_only_accepted_finish_intent(system, monkeypatch):
    from deployment_runtime.collection_current import CurrentCollectionAPI
    api, _, platform, contract, engine, actor = system
    first = start(api, actor)
    engine.live[first['session_id']]['current_frames'] = 0
    monkeypatch.setattr(api, '_kick', lambda key: None)
    api.submit('finish', {'upload_url': '/srv/frozen/artifacts'}, actor)
    api.close()
    replacement = CurrentCollectionAPI(platform)
    try:
        assert wait(replacement)['state'] == 'SUCCEEDED'
        assert not engine.live
        row = contract._read(first['session_id'], actor)
        assert row['finish']['destination']['upload_url'] == '/srv/frozen/artifacts'
    finally: replacement.close()


@pytest.mark.parametrize('body', [{}, {'upload_url': None}, {'upload_url': ''}])
def test_finish_without_upload_url_uses_default_dir(system, body):
    # 不带（或 null / 空字符串）upload_url：冻结为 None，推送时按 .env 的部署根目录拼默认目录
    api, _, _, contract, engine, actor = system
    first = start(api, actor)
    api.submit('complete', {}, actor)                              # 先提交一条，结束时才有数据要推送
    assert wait(api)['state'] == 'SUCCEEDED'
    engine.live[first['session_id']]['current_frames'] = 0
    ack = api.submit('finish', body, actor)
    assert set(ack) == {'accepted', 'message'}
    assert wait(api)['state'] == 'SUCCEEDED'
    jobs = list(contract.uploads.rows.values())
    assert len(jobs) == 1 and jobs[0]['payload']['destination'] == {'protocol': 'so101.sync.v2', 'upload_url': None}
    assert contract._read(first['session_id'], actor)['finish']['state'] == 'QUEUED_UPLOAD'


@pytest.mark.parametrize('value', [0, False, [], {}])
def test_explicit_upload_url_must_be_text(system, value):
    api, _, _, _, _, actor = system
    with pytest.raises(Fault) as error: api.submit('finish', {'upload_url': value}, actor)
    assert error.value.code == 'INVALID_UPLOAD_URL'
