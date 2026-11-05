"""v10r9 端到端：真实 SDK 采集/封存 → 真实 rsync 守护进程（账号密码）→ 逐文件校验 → 完成标记。

两种去向各跑一遍：finish 不带参数（<部署根目录>/action-composer/data/artifacts），finish 带 upload_url。

只有机器人是软件替身（FakeRobot）。rsync 守护进程在本机回环地址上真实运行，不访问外部网络。
需要在采集镜像里运行（要有 lerobot、rsync）。
"""
import hashlib
import json
import shutil
import socket
import subprocess
import time
from collections import Counter
from pathlib import Path

import pytest

pytest.importorskip('lerobot', reason='Run native sync acceptance in the collection image')
if shutil.which('rsync') is None:
    pytest.skip('rsync is required', allow_module_level=True)


def until(predicate, timeout=90, message='expected state not reached'):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.02)
    raise AssertionError(message)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def command(api, actor, action, body):
    assert api.submit(action, body, actor) == {'accepted': True, 'message': '已接收，后台执行中'}
    with api.lock:
        key = api.db.execute('SELECT id FROM commands ORDER BY created DESC LIMIT 1').fetchone()['id']
    entry = until(lambda: (value if (value := api._entry(key, actor))['state'] not in ('QUEUED', 'RUNNING') else None))
    assert entry['state'] == 'SUCCEEDED', entry
    return entry['result']


@pytest.fixture
def rsync_daemon(tmp_path):
    """本机 rsync 守护进程：模块 incoming，账号 robot，密码 right-password。"""
    root = tmp_path / 'daemon'
    (root / 'incoming').mkdir(parents=True)
    secrets = root / 'rsyncd.secrets'
    secrets.write_text('robot:right-password\n')
    secrets.chmod(0o600)
    port = free_port()
    conf = root / 'rsyncd.conf'
    conf.write_text(f'''pid file = {root}/rsyncd.pid
log file = {root}/rsyncd.log
use chroot = no
[incoming]
    path = {root}/incoming
    read only = no
    auth users = robot
    secrets file = {secrets}
    strict modes = yes
''')
    process = subprocess.Popen(['rsync', '--daemon', '--no-detach', '--config=' + str(conf),
                                '--address=127.0.0.1', f'--port={port}'])
    try:
        until(lambda: _listening(port), timeout=10, message='rsync daemon did not start')
        yield {'port': port, 'incoming': root / 'incoming'}
    finally:
        process.terminate()
        process.wait(5)


def _listening(port):
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=.2):
            return True
    except OSError:
        return False


@pytest.mark.parametrize('explicit_url', [False, True], ids=['default-dir', 'upload-url'])
def test_native_batch_is_pushed_verified_and_marked_ready_by_the_container(tmp_path, monkeypatch, rsync_daemon, explicit_url):
    from dataclasses import replace
    from deployment_runtime import collection_sync
    from deployment_runtime.collection_contract import CollectionContract
    from deployment_runtime.collection_current import CurrentCollectionAPI
    from deployment_runtime.collection_sync import CollectionSync
    from deployment_runtime.service import Platform
    from deployment_runtime.sessions import SessionRuntime
    from deployment_runtime.store import Fault, Principal
    from lerobot_sdk import DatasetManager
    from lerobot_sdk.testing import FakeRobot, simulation_config

    # ---- 同步配置：和 apply-env.py 生成的一样，文件名相对配置目录
    config_dir = tmp_path / 'config'
    config_dir.mkdir()
    password = config_dir / 'sync.password'
    password.write_text('wrong-password\n')          # 先故意写错
    password.chmod(0o600)
    (config_dir / 'upload.json').write_text(json.dumps({
        '_说明': '测试配置',
        'sync': {'protocol': 'rsync', 'host': '127.0.0.1', 'port': rsync_daemon['port'], 'user': 'robot',
                 'password_file': 'sync.password',
                 'deploy_root': '/incoming/deploy'}}))         # rsync 守护进程：第一段是模块名

    attempts = Counter()
    holder = {}

    def make_sync(platform):
        sync = CollectionSync(Path(platform.root) / 'rsync-uploads', config_dir / 'upload.json',
                              retry_base_s=.05, retry_max_s=.2)
        real = sync.transfer

        def counted(task, config):
            attempts['tries'] += 1
            # 推送时录制必须已经结束：没有活动会话、没有占用的机器人
            assert not holder['platform'].sessions.live and not holder['platform'].store.claims()
            result = real(task, config)
            attempts['succeeded'] += 1
            return result
        sync.transfer = counted
        holder['sync'] = sync
        return sync

    monkeypatch.setattr(collection_sync, 'create_for_platform', make_sync)

    root = tmp_path / 'collector'
    cfg = simulation_config('sync-native-software', required_cameras=('front', 'wrist'), max_observation_age_s=3)
    sdk = {**cfg.__dict__, 'release_pose': None}
    for name in ('units', 'limits', 'max_velocity', 'max_step', 'tolerance'):
        sdk[name] = dict(sdk[name])
    config = {'role': 'collection', 'root': str(root), 'collection_project': 'lab',
              'collection_targets': {'lab': {'default_mode': 'simulation_teleop',
                  'simulation_teleop': {'robot_id': cfg.robot_id, 'robot_revision': 'v1'}}}}
    actor = Principal('operator', ('lab',), ('operator', 'admin', 'trainer'))
    devices = []

    class SoftwareTeleop(FakeRobot):
        def leader(self, timeout_s):
            self.source_sequence = getattr(self, 'source_sequence', 0) + 1
            return {'positions': dict(self.positions), 'provenance': {
                'source_kind': 'simulation', 'source': 'native-sync-software-fixture',
                'source_sequence': self.source_sequence}}

        def sample(self, timeout_s):
            snapshot = super().sample(timeout_s)
            return replace(snapshot, source_metadata={'clock_mapping': {
                'method': 'software-fixture', 'source_capture_ns': snapshot.captured_monotonic_ns,
                'mapped_lower_bound_ns': snapshot.captured_monotonic_ns, 'exact_hardware_sync': False}})

    def factory(robot):
        device = SoftwareTeleop(str(tmp_path), cameras=('front', 'wrist'))
        devices.append(device)
        return device

    runtime = SessionRuntime(root / 'capture', factory)
    collector = Platform(config, sessions=runtime)
    holder['platform'] = collector
    collector.register('robots', {'project': 'lab', 'id': cfg.robot_id, 'revision': 'v1', 'body': {
        'robot_id': cfg.robot_id, 'kind': 'simulation', 'sdk_config': sdk,
        'camera_shapes': {'front': [12, 16, 3], 'wrist': [12, 16, 3]},
        'scene_id': 'desk', 'scene_revision': 'v1', 'calibration_revision': 'baseline',
        'capabilities': ['collection', 'teleop'],
        'ros': {'domain_id': 119, 'namespace': '/sync_native_test'}}}, actor)
    probe = lambda *args: {'peer': {'owner': None, 'fault': None, 'holding': True, 'action_queue_empty': True}}
    contract = CollectionContract(collector, target_probe=probe)
    collector.collection = contract
    api = CurrentCollectionAPI(collector)
    collector.current_collection = api
    reopened = None
    batch = 'platform-batch-0001'
    if explicit_url:
        upload_url = '/incoming/action-composer/data/artifacts'
        artifacts = rsync_daemon['incoming'] / 'action-composer' / 'data' / 'artifacts'
    else:
        upload_url = '/incoming/deploy/action-composer/data/artifacts/data_set'   # <部署根目录>/action-composer/data/artifacts/data_set
        artifacts = rsync_daemon['incoming'] / 'deploy' / 'action-composer' / 'data' / 'artifacts' / 'data_set'
    try:
        # ---- 前端只发最小参数：start 只有 id + name；finish 空对象（或只带服务器目录）
        first = command(api, actor, 'start', {'id': batch, 'name': 'native sync batch', 'fps': 20})
        assert first['session_id'] == batch and 'is_sim' not in first
        until(lambda: runtime.status(batch)['current_frames'] >= 2)
        command(api, actor, 'complete', {})
        until(lambda: runtime.status(batch)['current_frames'] >= 2)
        command(api, actor, 'redo', {})
        until(lambda: runtime.status(batch)['current_frames'] >= 2)
        assert attempts['tries'] == 0   # 结束前不推送
        started = time.perf_counter()
        api.submit('finish', {'upload_url': upload_url} if explicit_url else {}, actor)
        assert time.perf_counter() - started < .5                  # 短接口：不等停止、封存和推送
        until(lambda: contract._read(batch, actor).get('finish', {}).get('state') == 'QUEUED_UPLOAD')

        # ---- 1. 密码错：rsync 退出码 5，任务停在 TRANSFER_RETRY，错误写明原因且不含密码
        sync = holder['sync']
        failed = until(lambda: (t := _task(sync, batch)) and t['state'] == 'TRANSFER_RETRY' and '退出码 5' in t['error'] and t)
        assert '账号或密码' in failed['error'] and 'right-password' not in failed['error'] and 'wrong-password' not in failed['error']
        probe_args = ['--config', str(config_dir / 'upload.json'), '--probe', '--dir', '/incoming']
        assert collection_sync.main(probe_args) == 1           # 配置检查命令也能发现密码错
        assert collection_sync.main(probe_args[:-2]) == 1      # 不写 --dir：默认测部署根目录下的 artifacts

        # ---- 2. 改对密码（不重启容器）：服务器上的 upload_url 目录还不存在 → 继续重试
        password.write_text('right-password\n')
        assert collection_sync.main(probe_args) == 0
        missing = until(lambda: (t := _task(sync, batch)) and t['state'] == 'TRANSFER_RETRY'
                        and '退出码 5' not in (t['error'] or '') and t)
        assert '退出码' in missing['error'] and not artifacts.exists()

        # ---- 3. 服务器上建好目录：容器下一次重试自动成功（默认目录只需 artifacts 存在，data_set 由容器自己建）
        (artifacts if explicit_url else artifacts.parent).mkdir(parents=True)
        done = until(lambda: (t := _task(sync, batch)) and t['state'] == 'READY' and t)
        assert attempts['succeeded'] == 1 and done['error'] is None

        # ---- 远端 = <upload_url>/<id>/，与本地封存快照逐字节一致，没有断点续传残留；完成标记 .ready.json 在文件夹里
        source = Path(done['source_root'])
        assert source.is_relative_to(root / 'collection-snapshots')
        remote = artifacts / batch
        local_files = {p.relative_to(source).as_posix(): p.read_bytes() for p in source.rglob('*') if p.is_file()}
        remote_files = {p.relative_to(remote).as_posix(): p.read_bytes() for p in remote.rglob('*')
                        if p.is_file() and p.name != '.ready.json'}
        assert remote_files == local_files and 'manifest.json' in remote_files
        assert not list(remote.rglob('.rsync-partial'))
        # 远端去掉完成标记 .ready.json 后就是能直接读的 LeRobot 数据集（严格校验不允许清单外的文件，消费方要忽略它）
        import shutil
        checked = tmp_path / 'remote-without-marker'
        shutil.copytree(remote, checked, ignore=shutil.ignore_patterns('.ready.json'))
        info = DatasetManager().inspect(checked)
        assert info.episodes == 2 and info.native_readable and info.ref.dataset_id == batch
        marker = json.loads((remote / '.ready.json').read_text())
        assert marker == {**marker, 'schema': 'so101.collection-sync.v1', 'id': batch, 'folder': batch,
                          'format': 'lerobot_v3', 'content_hash': info.ref.content_hash,
                          'manifest_sha256': hashlib.sha256((source / 'manifest.json').read_bytes()).hexdigest(),
                          'file_count': len(local_files), 'total_bytes': sum(map(len, local_files.values()))}
        assert sorted(p.name for p in artifacts.iterdir()) == [batch]             # 目录下只有这一批的文件夹
        assert sorted(p.name for p in remote.iterdir() if p.name.startswith('.')) == ['.ready.json']   # 没有临时文件残留
        assert done['result']['folder'] == f'{upload_url}/{batch}'
        assert done['result']['ready_file'] == f'{upload_url}/{batch}/.ready.json'

        # ---- 外层 outbox 和数据集列表也显示同步完成
        operation = contract._read(batch, actor)['last_upload_operation']
        until(lambda: contract.uploads.get(operation)['state'] == 'READY')
        listed = collector.store.list('datasets', actor, project='lab')['items']
        fields = [contract.dataset_fields(v) for v in listed if v.get('id') == batch]
        assert fields and fields[-1]['delivery_kind'] == 'sync' and fields[-1]['upload_state'] == 'READY'
        assert fields[-1]['upload_url'] == (upload_url if explicit_url else None)
        assert fields[-1]['sync_folder'] == f'{upload_url}/{batch}'
        assert fields[-1]['ready_file'] == f'{upload_url}/{batch}/.ready.json'

        # ---- 同一个 id 不能再开始第二批
        with pytest.raises(Fault) as error:
            api.submit('start', {'id': batch, 'name': 'again'}, actor)
        assert error.value.code == 'ID_ALREADY_USED'

        # ---- 重启：READY 的任务不会再推送
        collector.close()
        assert all(not device.connected and device.holds for device in devices)

        def no_new_device(robot):
            pytest.fail('Finished jobs must not acquire devices after restart')
        reopened = Platform(config, sessions=SessionRuntime(root / 'capture', no_new_device))
        holder['platform'] = reopened
        restored = CollectionContract(reopened, target_probe=probe)
        reopened.collection = restored
        time.sleep(.5)
        assert attempts['succeeded'] == 1 and holder['sync'].get(batch)['state'] == 'READY'
        assert {p.relative_to(source).as_posix(): p.read_bytes() for p in source.rglob('*') if p.is_file()} == local_files
    finally:
        if reopened is not None:
            reopened.close()
        if not collector.closing:
            collector.close()


def _task(sync, key):
    try:
        return sync.get(key)
    except KeyError:
        return None
