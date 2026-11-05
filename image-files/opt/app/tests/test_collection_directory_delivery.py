"""Single-container publication through the real native filesystem sink."""
import copy
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from deployment_runtime.collection_destinations import resolve_destination, push_destination
from deployment_runtime.collection_uploads import DurableUploads
from deployment_runtime.store import Fault, Principal, Store


def platform_config(root, owner='student', project='lab'):
    return SimpleNamespace(config={
        'default_upload_sink': 'delivery',
        'sinks': {'delivery': {'kind': 'directory', 'directory': str(root),
                               'principal': owner, 'project': project}},
    })


def test_container_defaults_to_local_delivery_without_receiver_url(tmp_path):
    from deployment_runtime.container_init import prepare
    env = {'SO101_SERVICE_ROLE': 'collection', 'SO101_IMAGE_ID': 'sha256:' + 'a' * 64,
           'SO101_COLLECTION_OUTPUT_ROOT': str(tmp_path / 'mounted-delivery')}
    config, _ = prepare(env, tmp_path / 'data', tmp_path / 'shared')
    value = json.loads(config.read_text())
    sink = value['sinks'][value['default_upload_sink']]
    assert sink == {'kind': 'directory', 'directory': str(tmp_path / 'mounted-delivery'),
                    'principal': 'operator', 'project': 'lab'}
    assert all('url' not in item and 'token_file' not in item for item in value['sinks'].values())


def test_directory_destination_is_frozen_and_has_no_credentials(tmp_path, monkeypatch):
    import urllib.request
    monkeypatch.setattr(urllib.request, 'build_opener', lambda *args: pytest.fail('No receiver needed'))
    root = tmp_path / 'delivery'
    platform = platform_config(root)
    actor = Principal('student', ('lab',), ('operator',))
    destination = resolve_destination(platform, None, actor, 'lab')
    assert destination == {'protocol': 'so101.directory.v1', 'directory': str(root),
        'sink_id': 'delivery', 'authentication': 'none', 'principal': 'student', 'project': 'lab'}
    platform.config['sinks']['delivery']['directory'] = str(tmp_path / 'changed')
    assert destination['directory'] == str(root)
    assert not root.exists()  # Acceptance freezes a target without filesystem publication.


def test_explicit_http_url_overrides_directory_without_borrowing_credentials(tmp_path):
    platform = platform_config(tmp_path / 'delivery')
    platform.config['sinks']['unrelated'] = {'url': 'http://another:8911',
        'principal': 'student', 'project': 'lab', 'token_file': '/private/access-token'}
    actor = Principal('student', ('lab',), ('operator',))
    value = resolve_destination(platform, 'http://backend:8911/prefix/', actor, 'lab')
    assert value == {'protocol': 'so101.transfer.v2', 'url': 'http://backend:8911/prefix',
        'authentication': 'none', 'principal': 'student', 'project': 'lab'}


@pytest.mark.parametrize('field,value', [('principal', 'another'), ('project', 'other')])
def test_local_sink_rejects_wrong_owner_or_project_before_publication(tmp_path, field, value):
    platform = platform_config(tmp_path / 'delivery')
    platform.config['sinks']['delivery'][field] = value
    actor = Principal('student', ('lab',), ('operator',))
    with pytest.raises(Fault):
        resolve_destination(platform, None, actor, 'lab')
    assert not (tmp_path / 'delivery').exists()


@pytest.fixture
def native_delivery(tmp_path):
    pytest.importorskip('lerobot')
    from test_collection_uploads import raw_task, identities
    from deployment_runtime.collection_snapshots import prepare_snapshot
    from deployment_runtime.datasets import Datasets
    directory, commits = raw_task(tmp_path)
    identity = {**identities(1)[0], 'collection_episodes': identities(1)}
    snapshot = prepare_snapshot(directory, commits[:1], identity, tmp_path / 'snapshots')
    root = Path(snapshot['dataset_root'])
    actor = Principal('student', ('lab',), ('operator',))
    store = Store(tmp_path / 'registry.sqlite3')
    datasets = Datasets(store, tmp_path / 'registered-datasets')
    registered = datasets.register_local(root, snapshot['content_hash'], actor, 'lab', [tmp_path / 'snapshots'])
    platform = platform_config(tmp_path / 'delivery')
    platform.store = store
    platform.datasets = datasets
    request = {'project': 'lab', 'dataset_id': registered['id'], 'version': registered['revision'],
               'content_hash': snapshot['content_hash'], 'sink_id': 'delivery'}
    source = Path(registered['body']['root'])
    original = {p.relative_to(source).as_posix(): p.read_bytes() for p in source.rglob('*') if p.is_file()}
    yield platform, actor, request, source, original, identity
    store.close()


def assert_source_unchanged(source, original):
    assert {p.relative_to(source).as_posix(): p.read_bytes()
            for p in source.rglob('*') if p.is_file()} == original


def test_real_native_directory_publication_is_idempotent_and_verified(native_delivery, monkeypatch):
    import urllib.request
    from lerobot_sdk import DatasetManager
    platform, actor, request, source, original, _ = native_delivery
    monkeypatch.setattr(urllib.request, 'build_opener', lambda *args: pytest.fail('Directory delivery used network'))
    destination = resolve_destination(platform, None, actor, 'lab')
    target = Path(destination['directory']) / 'lab' / actor.name / request['dataset_id'] / request['version']
    first = push_destination(platform, request, actor, 'upload-0', destination)
    assert first['business_complete'] is True and first['upload_verified'] is True
    assert first['upload_state'] == 'READY'
    assert first['content_hash'] == request['content_hash']
    assert first['target_dataset_id'] == request['dataset_id'] and first['target_version'] == request['version']
    assert first['storage_ref']
    assert DatasetManager().inspect(target).ref.content_hash == request['content_hash']
    published = {p.relative_to(target).as_posix(): p.read_bytes() for p in target.rglob('*') if p.is_file()}
    assert published == original
    second = push_destination(platform, request, actor, 'upload-0', destination)
    assert second == first
    assert_source_unchanged(source, original)


def test_legacy_registered_sink_entry_uses_directory_without_network(native_delivery,monkeypatch):
    import urllib.request
    from deployment_runtime.transfers import push
    platform,actor,request,source,original,_=native_delivery
    monkeypatch.setattr(urllib.request,'build_opener',lambda *args:pytest.fail('Legacy entry used HTTP'))
    result=push(platform,request,actor,'legacy-directory-upload')
    assert result['delivery_kind']=='directory' and result['upload_verified']
    assert_source_unchanged(source,original)


def test_existing_conflicting_directory_is_never_overwritten(native_delivery):
    from lerobot_sdk import SDKError
    platform, actor, request, source, original, _ = native_delivery
    destination = resolve_destination(platform, None, actor, 'lab')
    target = Path(destination['directory']) / 'lab' / actor.name / request['dataset_id'] / request['version']
    target.mkdir(parents=True)
    (target / 'keep.txt').write_bytes(b'unrelated data')
    with pytest.raises((Fault, SDKError)):
        push_destination(platform, request, actor, 'upload-0', destination)
    assert list(target.iterdir()) == [target / 'keep.txt']
    assert (target / 'keep.txt').read_bytes() == b'unrelated data'
    assert_source_unchanged(source, original)


@pytest.mark.parametrize('change', ['hash', 'principal', 'project'])
def test_frozen_directory_request_cannot_substitute_hash_or_identity(native_delivery, change):
    platform, actor, request, source, original, _ = native_delivery
    destination = resolve_destination(platform, None, actor, 'lab')
    if change == 'hash': request = {**request, 'content_hash': 'sha256:' + 'f' * 64}
    else: destination[change] = 'another'
    with pytest.raises(Fault):
        push_destination(platform, request, actor, 'upload-0', destination)
    assert not Path(destination['directory']).exists()
    assert_source_unchanged(source, original)


def test_shared_read_access_cannot_change_published_owner(native_delivery):
    platform, actor, request, source, original, _ = native_delivery
    reader = Principal('reader', ('lab',), ('operator',))
    platform.store.grant('datasets', request['dataset_id'], request['version'], actor, reader.name)
    platform.config['sinks']['delivery']['principal'] = reader.name
    destination = resolve_destination(platform, None, reader, 'lab')
    with pytest.raises(Fault):
        push_destination(platform, request, reader, 'upload-reader', destination)
    assert not Path(destination['directory']).exists()
    assert_source_unchanged(source, original)


@pytest.mark.parametrize('linked_part', ['root', 'project'])
def test_directory_publication_does_not_follow_symlink_ancestors(native_delivery, tmp_path, linked_part):
    from lerobot_sdk import SDKError
    platform, actor, request, source, original, _ = native_delivery
    destination = resolve_destination(platform, None, actor, 'lab')
    delivery = Path(destination['directory'])
    outside = tmp_path / 'outside'; outside.mkdir()
    link = delivery if linked_part == 'root' else delivery / 'lab'
    link.parent.mkdir(parents=True, exist_ok=True)
    try: link.symlink_to(outside, target_is_directory=True)
    except OSError as exc: pytest.skip('Directory symlinks unavailable: ' + str(exc))
    with pytest.raises((Fault, SDKError)):
        push_destination(platform, request, actor, 'upload-0', destination)
    assert not list(outside.iterdir())
    assert_source_unchanged(source, original)


def test_interrupted_local_delivery_retries_after_worker_restart_with_frozen_target(native_delivery, tmp_path, monkeypatch):
    from lerobot_sdk.dataset_sinks import LocalDirectorySink
    from lerobot_sdk import DatasetManager
    platform, actor, request, source, original, identity = native_delivery
    destination = resolve_destination(platform, None, actor, 'lab')
    payload = {**identity, 'sink_id': 'delivery', 'destination': destination}
    operation_id = payload['upload_operation']
    manifest = {'dataset_id': request['dataset_id'], 'version': request['version'], 'content_hash': request['content_hash']}
    original_upload = LocalDirectorySink.upload
    fail = True
    def interrupted(sink, snapshot, progress):
        def report(value):
            progress(value)
            if fail and value.phase == 'transferring': raise OSError('interrupted mounted delivery')
        return original_upload(sink, snapshot, report)
    monkeypatch.setattr(LocalDirectorySink, 'upload', interrupted)
    observed = []
    def execute(job):
        observed.append(copy.deepcopy(job['payload']['destination']))
        result = push_destination(platform, request, actor, operation_id, job['payload']['destination'])
        return {**result, 'owner': actor.name, 'project': 'lab', 'session_id': payload['session_id'],
            'episode_id': payload['episode_id'], 'recording_generation': payload['recording_generation'],
            'upload_operation': operation_id}
    def wait_for(box, state):
        end = time.monotonic() + 30
        while box.get(operation_id)['state'] != state:
            assert time.monotonic() < end, box.get(operation_id)
            time.sleep(.02)
    box = DurableUploads(tmp_path / 'outbox', execute, retry_delay_s=.1, max_retry_delay_s=.1)
    box.enqueue(operation_id, payload); box.bind_manifest(operation_id, manifest); box.start()
    try:
        wait_for(box, 'RETRY_WAIT')
        assert 'interrupted mounted delivery' in box.get(operation_id)['error']
    finally: assert box.stop()
    assert list(Path(destination['directory']).rglob('.' + request['version'] + '.upload-*'))
    fail = False
    platform.config['sinks']['delivery']['directory'] = str(tmp_path / 'must-not-use-new-default')
    restored = DurableUploads(tmp_path / 'outbox', execute, retry_delay_s=.1, max_retry_delay_s=.1)
    restored.start()
    try:
        wait_for(restored, 'READY')
        job = restored.get(operation_id)
        assert job['receipt']['business_complete'] and job['receipt']['upload_verified']
        assert job['receipt']['content_hash'] == manifest['content_hash']
        assert job['attempts'] >= 2 and job['error'] is None
        assert all(value == destination for value in observed)
        target = Path(destination['directory']) / 'lab' / actor.name / request['dataset_id'] / request['version']
        assert DatasetManager().inspect(target).ref.content_hash == request['content_hash']
        assert not (tmp_path / 'must-not-use-new-default').exists()
    finally: assert restored.stop()
    assert_source_unchanged(source, original)
