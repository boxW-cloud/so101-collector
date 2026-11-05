"""v10 batch delivery boundaries; SQLite/runtime seams, no hardware I/O."""
import copy
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from deployment_runtime.collection_contract import CollectionContract
from deployment_runtime.store import Fault, Principal
from test_collection_contract_api import system, Outbox


def complete(c, row, actor, request):
    return c.current_complete(row['session_id'], row['episode_id'],
        {'project': 'lab', 'recording_generation': row['recording_generation']}, request, actor)


def finished(c, key, actor):
    end = time.monotonic() + 3
    while time.monotonic() < end:
        row = c._read(key, actor)
        if row.get('finish', {}).get('state') in ('QUEUED_UPLOAD', 'EMPTY'): return row
        time.sleep(.01)
    raise AssertionError(row)


def test_complete_waits_for_finish_and_restore_cannot_send_early(system):
    c, e, box, p, a, payload = system
    first = c.current_start(payload, 'start', a)
    next_row = complete(c, first, a, 'complete')
    assert len(e.live[first['session_id']]['commits']) == 1
    assert next_row['episode_index'] == 1 and not box.rows
    c._restore_outbox()
    assert not box.rows
    assert c._read(first['session_id'], a)['api_contract'] == 'minimal.v10'


def test_finish_freezes_one_target_for_all_committed_episodes(system):
    from deployment_runtime.collection_destinations import resolve_destination
    c, e, box, p, a, payload = system
    p.config['sinks'] = {'training': {'url': 'http://old:8911', 'principal': 'alice', 'token_file': '/private/old'}}
    first = c.current_start(payload, 'start', a)
    second = complete(c, first, a, 'complete-1')
    third = complete(c, second, a, 'complete-2')
    assert not box.rows
    destination = resolve_destination(p, 'http://new:8911/prefix', a, 'lab')
    e.live[first['session_id']]['current_frames'] = 0
    c.current_finish(first['session_id'], {'project': 'lab'}, 'finish', a, destination=destination)
    row = finished(c, first['session_id'], a)
    assert row['internal_state'] == 'STOPPED' and not p.store.claims()
    assert len(box.rows) == 2
    assert all(v['payload']['destination'] == destination for v in box.rows.values())
    assert 'token_file' not in destination and destination['url'] == 'http://new:8911/prefix'
    p.config['sinks']['training']['url'] = 'http://changed:8911'
    c._restore_outbox()
    assert all(v['payload']['destination'] == destination for v in box.rows.values())
    with pytest.raises(Fault) as error:
        c.current_finish(first['session_id'], {'project': 'lab'}, 'other-finish', a,
            destination=resolve_destination(p, 'http://other:8911', a, 'lab'))
    assert error.value.code == 'UPLOAD_DESTINATION_FROZEN'


def test_v9_complete_keeps_automatic_per_episode_enqueue(system):
    c, e, box, p, a, payload = system
    first = c.minimal_start(payload, 'old-start', a)
    c.minimal_complete(first['session_id'], first['episode_id'],
        {'project': 'lab', 'recording_generation': 1}, 'old-complete', a)
    assert len(box.rows) == 1
    assert 'destination' not in next(iter(box.rows.values()))['payload']


def test_legacy_directory_commit_freezes_target_across_restart(system,tmp_path):
    c,e,box,p,a,payload=system
    directory=str(tmp_path/'delivery')
    p.config.update(default_upload_sink='local',sinks={'local':{
        'kind':'directory','directory':directory,'principal':a.name,'project':'lab'}})
    first=c.minimal_start(payload,'directory-start',a)
    c.minimal_complete(first['session_id'],first['episode_id'],
        {'project':'lab','recording_generation':1},'directory-complete',a)
    queued=copy.deepcopy(next(iter(box.rows.values()))['payload'])
    assert queued['destination']['protocol']=='so101.directory.v1'
    assert queued['destination']['directory']==directory
    p.config['sinks']['local']['directory']=str(tmp_path/'different')
    c._restore_outbox()
    assert next(iter(box.rows.values()))['payload']==queued


def test_restart_before_finish_keeps_committed_batch_local(system):
    c, e, box, p, a, payload = system
    first = c.current_start(payload, 'start', a)
    complete(c, first, a, 'complete')
    c.close()
    replacement = CollectionContract(p, uploads=box)
    p.collection = replacement
    try:
        assert not box.rows
        assert len(replacement._read(first['session_id'], a)['collection_episodes']) == 1
        assert not e.live
    finally: replacement.close()


def test_finish_final_nonempty_episode_joins_same_frozen_batch(system, monkeypatch):
    from deployment_runtime.collection_destinations import resolve_destination
    c, e, box, p, a, payload = system
    first = c.current_start(payload, 'start', a)
    complete(c, first, a, 'complete')
    key = first['session_id']
    prior = copy.deepcopy(e.live[key]['commits'])
    def save_stopped(row, episode_id, packet, rid):
        info = {'episode_index': len(prior), 'frames': row['stop_result']['preserved_current_frames'],
            'task': 'test', 'files': [], 'content_hash': 'sha256:' + 'b' * 64}
        row['commit_intent'] = {'stopped': True}
        c._queue_commit(row, {'task_directory': str(e.root / 'tasks' / key), 'commits': [*prior, info]}, info)
    monkeypatch.setattr(c, '_complete_stopped', save_stopped)
    destination = resolve_destination(p, 'http://receiver:8911', a, 'lab')
    c.current_finish(key, {'project': 'lab'}, 'finish', a, destination=destination)
    row = finished(c, key, a)
    assert row['stop_result']['preserved_current_frames'] == 0
    assert len(box.rows) == 2 and len(row['collection_episodes']) == 2
    assert all(item['payload']['destination'] == destination for item in box.rows.values())


def test_missing_default_destination_does_not_block_stop_or_local_commits(system):
    c, e, box, p, a, payload = system
    first = c.current_start(payload, 'start', a)
    complete(c, first, a, 'complete')
    e.live[first['session_id']]['current_frames'] = 0
    c.current_finish(first['session_id'], {'project': 'lab'}, 'finish', a)
    row = finished(c, first['session_id'], a)
    assert row['internal_state'] == 'STOPPED' and not e.live and not p.store.claims()
    assert len(row['collection_episodes']) == 1 and len(box.rows) == 1
    assert next(iter(box.rows.values()))['payload']['destination']['configuration_error'] == 'SINK_NOT_CONFIGURED'


def test_deferred_finish_restarts_after_partial_enqueue_without_changing_target(system, monkeypatch):
    from deployment_runtime.collection_destinations import resolve_destination
    c, e, box, p, a, payload = system
    first = c.current_start(payload, 'start', a)
    second = complete(c, first, a, 'complete-1')
    complete(c, second, a, 'complete-2')
    e.live[first['session_id']]['current_frames'] = 0
    destination = resolve_destination(p, 'http://offline:8911', a, 'lab')
    monkeypatch.setattr(c, '_launch_finish', lambda key: None)
    c.current_finish(first['session_id'], {'project': 'lab'}, 'finish', a, destination=destination)
    saved_enqueue = box.enqueue
    count = 0
    def interrupted_enqueue(key, payload):
        nonlocal count
        count += 1
        if count == 2:
            c.finish_exit.set()
            raise RuntimeError('simulated process exit after first durable enqueue')
        return saved_enqueue(key, payload)
    box.enqueue = interrupted_enqueue
    c._finish_session(first['session_id'])
    assert len(box.rows) == 1
    box.enqueue = saved_enqueue
    c.close()
    replacement = CollectionContract(p, uploads=box, target_probe=lambda *args: {
        'peer': {'owner': None, 'fault': None, 'holding': True, 'action_queue_empty': True}})
    p.collection = replacement
    try:
        row = finished(replacement, first['session_id'], a)
        assert len(box.rows) == 2 and row['finish']['destination'] == destination
        assert all(v['payload']['destination'] == destination for v in box.rows.values())
    finally: replacement.close()


def test_destination_auth_is_bound_to_exact_registered_url(system, tmp_path):
    from deployment_runtime.collection_destinations import resolve_destination
    c, e, box, p, a, payload = system
    p.config['sinks'] = {'training': {'url': 'http://registered:8911', 'token_file': str(tmp_path / 'secret'),
        'principals': {'alice': {'principal': 'alice', 'project': 'lab', 'token_file': str(tmp_path / 'alice')}}}}
    bound = resolve_destination(p, 'http://registered:8911/', a, 'lab')
    assert bound['token_file'] == str(tmp_path / 'alice')
    anonymous = resolve_destination(p, 'http://unregistered:8911', a, 'lab')
    assert anonymous['authentication'] == 'none' and 'token_file' not in anonymous
    p.config['sinks']['training']['url'] = 'http://new-default:8911'
    assert bound['url'] == 'http://registered:8911'


@pytest.mark.parametrize('url', ['', 'http://user:pass@host', 'ftp://host', 'http://host?q=x', 'http://host/#x', 'http://host/with space', 'http://host/v2/transfers'])
def test_invalid_destination_is_rejected_without_network(system, url):
    from deployment_runtime.collection_destinations import resolve_destination
    *_, p, a, payload = system
    with pytest.raises(Fault) as error: resolve_destination(p, url, a, 'lab')
    assert error.value.code == 'INVALID_UPLOAD_URL'


@pytest.fixture
def receiver(tmp_path):
    calls = []; uploaded = bytearray(); offline = threading.Event(); redirect = threading.Event()
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def reply(self, value):
            raw = json.dumps({'ok': True, 'result': value}).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
        def do_GET(self):
            calls.append((self.command, self.path, self.headers.get('Authorization')))
            if offline.is_set(): self.send_error(503); return
            if redirect.is_set():
                self.send_response(302); self.send_header('Location', '/should-not-follow'); self.end_headers(); return
            self.reply({'principal': 'alice', 'projects': ['lab']})
        def do_POST(self):
            calls.append((self.command, self.path, self.headers.get('Authorization')))
            body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            if self.path.endswith('/v2/transfers'):
                assert body['project'] == 'lab'
                self.reply({'transfer_id': 't1', 'state': 'UPLOADING', 'offsets': {'data.bin': len(uploaded)}})
            else:
                self.reply({'id': 'dataset', 'revision': 'v1', 'owner': 'alice', 'project': 'lab',
                    'body': {'content_hash': 'sha256:' + 'a' * 64}})
        def do_PUT(self):
            calls.append((self.command, self.path, self.headers.get('Authorization')))
            uploaded.extend(self.rfile.read(int(self.headers.get('Content-Length', 0))))
            self.reply({'completed_bytes': len(uploaded), 'total_bytes': 12})
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
    file = tmp_path / 'data.bin'; file.write_bytes(b'actual bytes')
    source = {'project': 'lab', 'body': {'content_hash': 'sha256:' + 'a' * 64,
        'manifest': {'files': {'data.bin': {}}, 'content_hash': 'sha256:' + 'a' * 64}}}
    platform = SimpleNamespace(config={}, datasets=SimpleNamespace(file=lambda *args: file),
        store=SimpleNamespace(get=lambda *args: source, event=lambda *args: None))
    yield platform, 'http://127.0.0.1:' + str(server.server_port) + '/prefix', calls, uploaded, offline, redirect
    server.shutdown(); server.server_close(); thread.join(2)


@pytest.mark.parametrize('registered', [False, True])
def test_actual_http_receiver_uses_only_exact_destination_credentials(receiver, tmp_path, registered):
    from deployment_runtime.collection_destinations import resolve_destination, push_destination
    platform, url, calls, uploaded, offline, redirect = receiver
    actor = Principal('alice', ('lab',), ('operator',))
    token = tmp_path / 'token'; token.write_text('registered-only-token')
    platform.config['sinks'] = {'training': {'url': url if registered else 'http://different-host:8911',
        'principal': 'alice', 'token_file': str(token)}}
    destination = resolve_destination(platform, url, actor, 'lab')
    platform.config['sinks']['training']['url'] = 'http://changed-after-accept:8911'
    result = push_destination(platform, {'project': 'lab', 'dataset_id': 'dataset', 'version': 'v1',
        'content_hash': 'sha256:' + 'a' * 64}, actor, 'operation', destination)
    assert result['upload_verified'] is True and uploaded == b'actual bytes'
    assert len(calls) == 4 and all(call[1].startswith('/prefix/v2/') for call in calls)
    assert all(call[2] == ('Bearer registered-only-token' if registered else None) for call in calls)


def test_authenticated_receiver_redirect_is_not_followed(receiver, tmp_path):
    from deployment_runtime.collection_destinations import resolve_destination, push_destination
    platform, url, calls, uploaded, offline, redirect = receiver
    actor = Principal('alice', ('lab',), ('operator',)); token = tmp_path / 'token'; token.write_text('private')
    platform.config['sinks'] = {'training': {'url': url, 'principal': 'alice', 'token_file': str(token)}}
    destination = resolve_destination(platform, None, actor, 'lab'); redirect.set()
    with pytest.raises(Fault) as error:
        push_destination(platform, {'project': 'lab', 'dataset_id': 'dataset', 'version': 'v1',
            'content_hash': 'sha256:' + 'a' * 64}, actor, 'operation', destination)
    assert error.value.code == 'UPLOAD_REDIRECT_REJECTED'
    assert len(calls) == 1 and not uploaded


def test_offline_delivery_retries_after_worker_restart_with_same_destination(receiver, tmp_path):
    from deployment_runtime.collection_destinations import resolve_destination, push_destination
    from deployment_runtime.collection_uploads import DurableUploads
    platform, url, calls, uploaded, offline, redirect = receiver
    actor = Principal('alice', ('lab',), ('operator',)); destination = resolve_destination(platform, url, actor, 'lab')
    payload = {'session_id': 's1', 'project': 'lab', 'operator': 'alice', 'robot_id': 'sim',
        'target_type': 'simulation', 'mode': 'simulation_teleop', 'episode_id': 'e1', 'episode_index': 0,
        'recording_generation': 1, 'manifest_version': 1, 'upload_operation': 'op1', 'destination': destination}
    observed = []
    def execute(job):
        observed.append(copy.deepcopy(job['payload']['destination']))
        result = push_destination(platform, {'project': 'lab', 'dataset_id': 'dataset', 'version': 'v1',
            'content_hash': 'sha256:' + 'a' * 64}, actor, 'op1', job['payload']['destination'])
        return {**result, 'owner': 'alice', 'project': 'lab', 'session_id': 's1', 'episode_id': 'e1',
            'recording_generation': 1, 'upload_operation': 'op1'}
    box = DurableUploads(tmp_path / 'retry-outbox', execute, retry_delay_s=.05)
    box.enqueue('op1', payload); box.bind_manifest('op1', {'dataset_id': 'dataset', 'version': 'v1', 'content_hash': 'sha256:' + 'a' * 64})
    offline.set(); box.start()
    try:
        end = time.monotonic() + 3
        while box.get('op1')['state'] != 'RETRY_WAIT':
            assert time.monotonic() < end; time.sleep(.01)
    finally: assert box.stop()
    offline.clear(); platform.config['sinks'] = {'training': {'url': 'http://different-after-restart:8911'}}
    recovered = DurableUploads(tmp_path / 'retry-outbox', execute, retry_delay_s=.05); recovered.start()
    try:
        end = time.monotonic() + 3
        while recovered.get('op1')['state'] != 'READY':
            assert time.monotonic() < end; time.sleep(.01)
        assert len(observed) >= 2 and all(value == destination for value in observed)
        assert uploaded == b'actual bytes' and all(call[2] is None for call in calls)
    finally: assert recovered.stop()
