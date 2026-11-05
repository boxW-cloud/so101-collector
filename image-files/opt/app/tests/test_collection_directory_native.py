"""Actual two-camera native recording and local delivery; devices are software only."""
import json
import time
from pathlib import Path

import pytest

pytest.importorskip('lerobot', reason='Native collection dependencies required')


def until(predicate, timeout=45):
    end = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < end, 'Native directory delivery did not finish'
        time.sleep(.02)


def test_native_record_redo_finish_publishes_local_versions_and_restart_is_idle(tmp_path, monkeypatch):
    import urllib.request
    from dataclasses import replace
    from deployment_runtime.collection_contract import CollectionContract
    from deployment_runtime.container_init import prepare
    from deployment_runtime.service import Platform
    from deployment_runtime.sessions import SessionRuntime
    from deployment_runtime.store import Principal
    from lerobot_sdk import DatasetManager
    from lerobot_sdk.testing import FakeRobot, simulation_config

    monkeypatch.setattr(urllib.request, 'build_opener',
        lambda *args: pytest.fail('Single-container capture must not contact an HTTP receiver'))
    root = tmp_path / 'collector'
    delivery = tmp_path / 'mounted-delivery'
    config_file, _ = prepare({'SO101_SERVICE_ROLE': 'collection', 'SO101_IMAGE_ID': 'sha256:' + 'a' * 64,
        'SO101_COLLECTION_OUTPUT_ROOT': str(delivery)}, root, tmp_path / 'shared')
    config = json.loads(config_file.read_text())
    cfg = simulation_config('directory-native-sim', required_cameras=('front', 'wrist'),
                            max_observation_age_s=3)
    sdk = {**cfg.__dict__, 'release_pose': None}
    for name in ('units', 'limits', 'max_velocity', 'max_step', 'tolerance'):
        sdk[name] = dict(sdk[name])

    class SoftwareTeleop(FakeRobot):
        def leader(self, timeout_s):
            self.source_sequence = getattr(self, 'source_sequence', 0) + 1
            return {'positions': dict(self.positions), 'provenance': {
                'source_kind': 'simulation', 'source': 'local-directory-test-device',
                'source_sequence': self.source_sequence}}

        def sample(self, timeout_s):
            snapshot = super().sample(timeout_s)
            return replace(snapshot, source_metadata={'clock_mapping': {
                'method': 'software-fixture', 'source_capture_ns': snapshot.captured_monotonic_ns,
                'mapped_lower_bound_ns': snapshot.captured_monotonic_ns, 'exact_hardware_sync': False}})

    devices = []
    def factory(robot):
        device = SoftwareTeleop(str(tmp_path), cameras=('front', 'wrist'))
        devices.append(device)
        return device
    runtime = SessionRuntime(root / 'capture', factory)
    collector = Platform(config, sessions=runtime)
    actor = Principal('operator', ('lab',), ('operator', 'admin', 'trainer'))
    collector.register('robots', {'project': 'lab', 'id': cfg.robot_id, 'revision': 'v1', 'body': {
        'robot_id': cfg.robot_id, 'kind': 'simulation', 'sdk_config': sdk,
        'camera_shapes': {'front': [12, 16, 3], 'wrist': [12, 16, 3]},
        'scene_id': 'desk', 'scene_revision': 'v1', 'capabilities': ['collection', 'teleop'],
        'ros': {'domain_id': 117, 'namespace': '/directory_native_test'}}}, actor)
    probe = lambda *args: {'peer': {'owner': None, 'fault': None,
                                  'holding': True, 'action_queue_empty': True}}
    contract = CollectionContract(collector, target_probe=probe)
    collector.collection = contract
    reopened = None
    try:
        first = contract.start({'project': 'lab', 'robot_id': cfg.robot_id, 'robot_revision': 'v1',
            'mode': 'simulation_teleop', 'name': 'native directory collection', 'fps': 20,
            'calibration_revision': 'baseline', 'scene_id': 'desk', 'scene_revision': 'v1'},
            'directory-start', actor)
        key = first['session_id']
        until(lambda: runtime.status(key)['current_frames'] >= 2)
        second = contract.complete(key, first['episode_id'], {'project': 'lab', 'recording_generation': 1},
                                   'directory-complete', actor)
        assert second['episode_index'] == 1 and second['state'] == 'RUNNING'
        until(lambda: runtime.status(key)['current_frames'] >= 2)
        task = runtime.live[key]['task']
        discarded = task._episode_dir
        discarded_frames = list(discarded.glob('*.npz'))
        assert len(discarded_frames) >= 2
        discarded_relative = discarded.relative_to(task.directory).as_posix() + '/'
        repeated = contract.redo(key, second['episode_id'], {'project': 'lab', 'recording_generation': 1},
                                 'directory-redo', actor)
        assert repeated['episode_index'] == 1 and repeated['recording_generation'] == 2
        assert repeated['episode_id'] == second['episode_id']
        assert not discarded.exists()
        until(lambda: runtime.status(key)['current_frames'] >= 2)
        retained = {p: p.read_bytes() for p in task.directory.rglob('*.npz')}
        result = contract.finish(key, {'project': 'lab'}, 'directory-finish', actor)
        assert result['automatic_upload'] is True
        until(lambda: contract._read(key, actor).get('finish', {}).get('state') == 'QUEUED_UPLOAD')
        row = contract._read(key, actor)
        operations = [item['upload_operation'] for item in row['collection_episodes']]
        assert len(operations) == 2
        until(lambda: all(contract.uploads.get(op)['state'] == 'READY' for op in operations))
        assert row['holding_confirmed'] is True and row['internal_state'] == 'STOPPED'
        assert row['stop_result']['holding'] is True and row['stop_result']['action_queue_empty'] is True
        assert not collector.store.claims() and not runtime.live
        assert devices[0].holds and not devices[0].connected
        assert all(p.read_bytes() == value for p, value in retained.items())
        published = sorted(delivery.glob('lab/operator/' + key + '/*'))
        assert len(published) == 2
        inspected = [DatasetManager().inspect(path) for path in published]
        assert sorted(item.episodes for item in inspected) == [1, 2]
        latest = next(item for item in inspected if item.episodes == 2)
        manifest = json.loads((latest.ref.root / 'manifest.json').read_text())
        assert [ep['episode_id'] for ep in manifest['collection_episodes']] == [first['episode_id'], second['episode_id']]
        assert [ep['recording_generation'] for ep in manifest['collection_episodes']] == [1, 2]
        assert manifest['camera_keys'] == ['front', 'wrist']
        assert all(len(list((path / 'videos').rglob('*.mp4'))) == 2 for path in published)
        assert all(list((path / 'data').rglob('*.parquet')) for path in published)
        # Finish commits the stopped episode through a reopened SDK task; the
        # original live task object intentionally keeps its older snapshot.
        commits = json.loads((task.directory / 'task.json').read_text())['commits']
        assert len(commits) == 2 and all(not frame['path'].startswith(discarded_relative)
            for commit in commits for frame in commit['files'])
        attempts = {}
        for operation in operations:
            job = contract.uploads.get(operation)
            frozen = job['payload']['destination']
            assert frozen['protocol'] == 'so101.directory.v1' and frozen['directory'] == str(delivery)
            target = Path(job['receipt']['target_directory'])
            assert DatasetManager().inspect(target).ref.content_hash == job['manifest']['content_hash']
            assert job['receipt']['content_hash'] == job['manifest']['content_hash']
            source = Path(job['prepared_root'])
            assert source.is_dir() and (source / 'manifest.json').read_bytes() == (target / 'manifest.json').read_bytes()
            attempts[operation] = job['attempts']
        before_restart = {p: (p.read_bytes(), p.stat().st_mtime_ns)
                          for p in delivery.rglob('*') if p.is_file()}
        collector.close()

        def no_new_device(robot):
            pytest.fail('Reopening a finished collector must not acquire or replay a robot')
        def no_new_delivery(self, job):
            pytest.fail('READY directory delivery must not execute again after restart')
        monkeypatch.setattr(CollectionContract, '_upload', no_new_delivery)
        reopened = Platform(config, sessions=SessionRuntime(root / 'capture', no_new_device))
        restored = CollectionContract(reopened, target_probe=probe)
        reopened.collection = restored
        assert restored.status({'project': 'lab', 'robot_id': cfg.robot_id, 'robot_revision': 'v1'}, actor)['state'] == 'IDLE'
        assert not reopened.store.claims() and not restored.uploads.pending()
        assert all(restored.uploads.get(op)['state'] == 'READY' and
                   restored.uploads.get(op)['attempts'] == attempts[op] for op in operations)
        assert {p: (p.read_bytes(), p.stat().st_mtime_ns)
                for p in delivery.rglob('*') if p.is_file()} == before_restart
    finally:
        if reopened is not None: reopened.close()
        if not collector.closing: collector.close()
