"""collection_sync 单元测试（v10r8）：配置检查、服务器目录、rsync 四步命令、校验解析、完成标记、持久化队列。

队列测试里推送用替身（可控地成功/失败），其余（SQLite 队列、重试、重启恢复、配置每次重读）都是正式代码。
真实 rsync 传输见 test_collection_sync_native.py。
"""
import hashlib
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import pytest

from deployment_runtime import collection_sync as cs


# ------------------------------------------------------------------ 工具

def secret(path, text='x\n', mode=0o600):
    path.write_text(text)
    path.chmod(mode)
    return path


def write_config(directory, sync=None):
    directory.mkdir(parents=True, exist_ok=True)
    value = {'sync': sync if sync is not None else {
        'protocol': 'rsync', 'host': '192.168.1.16', 'user': 'robot', 'password_file': 'sync.password'}}
    if not (directory / 'sync.password').exists():
        secret(directory / 'sync.password', 'pw\n')
    (directory / 'upload.json').write_text(json.dumps(value, ensure_ascii=False))
    return directory / 'upload.json'


def sealed(root, name='snap'):
    """一个最小“封存快照”：manifest.json 里列出文件和大小（与 SDK 清单同形）。"""
    path = root / name
    (path / 'data').mkdir(parents=True)
    (path / 'data' / 'a.parquet').write_bytes(b'parquet-bytes')
    (path / 'videos').mkdir()
    (path / 'videos' / 'b.mp4').write_bytes(b'video-bytes-long')
    files = {p.relative_to(path).as_posix(): {'bytes': p.stat().st_size,
             'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in path.rglob('*') if p.is_file()}
    (path / 'manifest.json').write_text(json.dumps({'files': files, 'content_hash': 'sha256:' + 'a' * 64}))
    return path


def fake_inspect(calls):
    def inspect(path):
        calls.append(str(path))
        path = Path(path)
        manifest = path / 'manifest.json'
        files = json.loads(manifest.read_text())['files']
        return {'content_hash': 'sha256:' + 'a' * 64,
                'manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
                'file_count': len(files) + 1,
                'total_bytes': sum(v['bytes'] for v in files.values()) + manifest.stat().st_size}
    return inspect


def until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(.01)
    raise AssertionError('state not reached')


DIR = '/xxx/action-composer/data/artifacts'


# ------------------------------------------------------------------ 1. 配置文件

def test_missing_config_is_a_readable_retryable_error(tmp_path):
    with pytest.raises(cs.SyncConfigError, match='未配置同步服务器'):
        cs.load_config(tmp_path / 'upload.json')
    (tmp_path / 'upload.json').write_text('{not json')
    with pytest.raises(cs.SyncConfigError, match='不是合法的 JSON'):
        cs.load_config(tmp_path / 'upload.json')


def test_ssh_password_config_resolves_relative_files_and_ignores_comment_keys(tmp_path):
    secret(tmp_path / 'sync.password', 'pw\n')
    (tmp_path / 'known_hosts').write_text('192.168.1.16 ssh-ed25519 AAAA\n')
    path = write_config(tmp_path, {'_说明': '注释', 'host': '192.168.1.16', 'user': 'robot',
                                   'password_file': 'sync.password', 'known_hosts_file': 'known_hosts'})
    assert cs.load_config(path) == {'sync': {
        'protocol': 'ssh', 'host': '192.168.1.16', 'port': 22, 'user': 'robot',
        'password_file': str(tmp_path / 'sync.password'), 'identity_file': None,
        'known_hosts_file': str(tmp_path / 'known_hosts'), 'bwlimit_kbps': 0, 'deploy_root': None}}


@pytest.mark.parametrize('sync, message', [
    ({'protocol': 'ftp'}, 'sync.protocol'),
    ({'host': 'bad host', 'user': 'u', 'password_file': 'sync.password'}, 'sync.host'),
    ({'host': 'h', 'user': 'u', 'port': 70000, 'password_file': 'sync.password'}, 'sync.port'),
    ({'host': 'h', 'user': 'u', 'accept_new_host_key': True}, '二选一'),
    ({'host': 'h', 'user': 'u', 'password_file': 'sync.password', 'identity_file': 'sync.password',
      'accept_new_host_key': True}, '二选一'),
    ({'host': 'h', 'user': 'u', 'password_file': 'sync.password'}, 'known_hosts_file'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u'}, 'password_file'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'missing.txt'}, '不存在'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'sync.password', 'remote_root': '/x'}, 'COLLECTION_SYNC_DEPLOY_ROOT'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'sync.password', 'typo': 1}, 'typo'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'sync.password',
      'deploy_root': 'home/unitree/deploy'}, 'COLLECTION_SYNC_DEPLOY_ROOT'),
    ({'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'sync.password',
      'deploy_root': '/home/../deploy'}, 'COLLECTION_SYNC_DEPLOY_ROOT'),
])
def test_invalid_sync_config_names_the_problem(tmp_path, sync, message):
    with pytest.raises(cs.SyncConfigError, match=message):
        cs.load_config(write_config(tmp_path, sync))


def test_r7_platform_section_is_rejected_with_a_clear_message(tmp_path):
    path = write_config(tmp_path)
    value = json.loads(path.read_text())
    value['platform'] = {'host': '192.168.1.16:8000'}
    path.write_text(json.dumps(value))
    with pytest.raises(cs.SyncConfigError, match='不再通知平台'):
        cs.load_config(path)


def test_secret_files_must_not_be_group_or_world_readable(tmp_path):
    secret(tmp_path / 'sync.password', 'pw\n', 0o644)
    with pytest.raises(cs.SyncConfigError, match='chmod 600'):
        cs.load_config(write_config(tmp_path))


# ------------------------------------------------------------------ 2. upload_url：服务器目录

def test_remote_dir_is_normalized():
    assert cs.normalize_remote_dir(DIR) == DIR
    assert cs.normalize_remote_dir(DIR + '/') == DIR
    assert cs.normalize_remote_dir('//xxx//data/') == '/xxx/data'
    assert cs.normalize_remote_dir('/data/so101_v2.1+raw') == '/data/so101_v2.1+raw'


@pytest.mark.parametrize('bad', [None, '', 'relative/dir', '/', '//', '/data/../etc', '/data/./x', '/data/.hidden',
                                 '/data/-rf', '/data/with space', '/data/a:b', 'http://192.168.1.16/x', '/' + 'a' * 1100])
def test_remote_dir_rejects_unsafe_values(bad):
    with pytest.raises(ValueError):
        cs.normalize_remote_dir(bad)


def test_default_dir_is_deploy_root_plus_artifacts(tmp_path):
    path = write_config(tmp_path, {'protocol': 'rsync', 'host': 'h', 'user': 'u', 'password_file': 'sync.password',
                                   'deploy_root': '/home/unitree/deploy/'})
    config = cs.load_config(path)
    assert config['sync']['deploy_root'] == '/home/unitree/deploy'
    assert cs.resolve_remote_dir('', config) == '/home/unitree/deploy/action-composer/data/artifacts/data_set'
    assert cs.resolve_remote_dir(DIR, config) == DIR                # 请求带了 upload_url 就用请求的
    config['sync']['deploy_root'] = None
    assert cs.resolve_remote_dir(DIR, config) == DIR
    with pytest.raises(cs.SyncConfigError, match='COLLECTION_SYNC_DEPLOY_ROOT'):
        cs.resolve_remote_dir('', config)


# ------------------------------------------------------------------ 3. rsync 四步命令

def _config(**changes):
    value = {'protocol': 'ssh', 'host': '192.168.1.16', 'port': 2222, 'user': 'robot',
             'password_file': '/config/sync.password', 'identity_file': None,
             'known_hosts_file': None, 'bwlimit_kbps': 0, 'deploy_root': None}
    value.update(changes)
    return {'sync': value}


def test_ssh_password_steps_push_data_verify_manifest_then_marker(tmp_path):
    steps, env = cs.build_sync_steps(_config(), '/data/snap/', DIR, 'batch-1', tmp_path / 'batch-1.ready.json',
                                     known_hosts_store=tmp_path / 'kh')
    names = [name for name, _ in steps]
    assert names == ['推送数据', '逐文件校验', '推送 manifest.json', '写完成标记']
    push, verify, manifest, marker = (argv for _, argv in steps)
    assert push[-2:] == ['/data/snap/', f'robot@192.168.1.16:{DIR}/batch-1/']
    assert '--exclude=/manifest.json' in push and '--partial-dir=.rsync-partial' in push
    assert {'-rtc', '--dry-run', '--itemize-changes', '--exclude=/manifest.json'} <= set(verify)
    assert verify[-1] == push[-1]
    assert manifest[-2:] == ['/data/snap/manifest.json', f'robot@192.168.1.16:{DIR}/batch-1/manifest.json']
    assert marker[-2:] == [str(tmp_path / 'batch-1.ready.json'), f'robot@192.168.1.16:{DIR}/batch-1/.ready.json']
    ssh = push[push.index('-e') + 1]
    assert '-p 2222' in ssh and 'StrictHostKeyChecking=accept-new' in ssh and f'UserKnownHostsFile={tmp_path / "kh"}' in ssh
    assert 'BatchMode=no' in ssh and 'PubkeyAuthentication=no' in ssh
    assert env['SSH_ASKPASS'].endswith('sync_askpass.sh') and env['SSH_ASKPASS_REQUIRE'] == 'force'
    assert env['SO101_SYNC_PASSWORD_FILE'] == '/config/sync.password'
    assert all('pw' not in ' '.join(argv) for _, argv in steps)


def test_prepare_step_creates_only_the_last_directory_level(tmp_path):
    steps, _ = cs.build_sync_steps(_config(), '/data/snap/', DIR + '/data_set', 'batch-1', tmp_path / 'r',
                                   known_hosts_store=tmp_path / 'kh', empty_dir=tmp_path / 'empty')
    assert [name for name, _ in steps] == ['准备目录', '推送数据', '逐文件校验', '推送 manifest.json', '写完成标记']
    prepare = steps[0][1]
    assert '-d' in prepare and '-r' not in prepare and '--mkpath' not in prepare and '--delete' not in prepare
    assert prepare[-2:] == [str(tmp_path / 'empty') + '/', f'robot@192.168.1.16:{DIR}/data_set/']


def test_ssh_key_with_known_hosts_and_bwlimit(tmp_path):
    config = _config(password_file=None, identity_file='/config/id_ed25519', known_hosts_file='/config/known_hosts',
                     bwlimit_kbps=500)
    steps, env = cs.build_sync_steps(config, '/data/snap', DIR, 'b', tmp_path / 'r', known_hosts_store=tmp_path / 'kh')
    push = steps[0][1]
    ssh = push[push.index('-e') + 1]
    assert '-i /config/id_ed25519' in ssh and 'BatchMode=yes' in ssh and 'StrictHostKeyChecking=yes' in ssh
    assert '--bwlimit=500' in push and 'SSH_ASKPASS' not in env


def test_rsync_daemon_steps_use_module_path_and_password_file(tmp_path):
    config = _config(protocol='rsync', port=873)
    steps, _ = cs.build_sync_steps(config, '/data/snap', '/incoming/artifacts', 'batch-1', tmp_path / 'r',
                                   known_hosts_store=tmp_path / 'kh')
    push, marker = steps[0][1], steps[3][1]
    assert '--password-file=/config/sync.password' in push and '-e' not in push
    assert push[-1] == 'rsync://robot@192.168.1.16:873/incoming/artifacts/batch-1/'
    assert marker[-1] == 'rsync://robot@192.168.1.16:873/incoming/artifacts/batch-1/.ready.json'


def test_differences_lists_only_files_that_would_be_sent_or_created():
    output = '\n'.join([
        '.d..t...... ./',
        '>f+++++++++ data/chunk-000/file-000.parquet',
        '>fc.t...... videos/front.mp4',
        'cd+++++++++ meta/episodes/',
        '.f..t...... meta/info.json',
        'some informational line',
    ])
    assert cs.differences(output) == ['data/chunk-000/file-000.parquet', 'videos/front.mp4', 'meta/episodes/']
    assert cs.differences('') == []


def test_askpass_script_prints_only_the_password_file_first_line(tmp_path):
    assert os.access(cs.ASKPASS_SCRIPT, os.X_OK), 'sync_askpass.sh must be executable in the image'
    password = secret(tmp_path / 'pw', 's3cret pass\nignored\n')
    out = subprocess.run([str(cs.ASKPASS_SCRIPT), 'robot@host password:'], capture_output=True, text=True,
                         env={'PATH': '/usr/bin:/bin', 'SO101_SYNC_PASSWORD_FILE': str(password)}, check=True)
    assert out.stdout == 's3cret pass\n'


def test_rsync_failure_message_has_step_hint_and_output_tail():
    with pytest.raises(cs.TransferError, match='推送数据失败.*退出码 10.*连不上同步服务器.*boom'):
        cs.RsyncRunner().run(['sh', '-c', 'echo boom >&2; exit 10'], {'PATH': '/usr/bin:/bin'}, step='推送数据')
    assert cs.RsyncRunner().run(['sh', '-c', 'echo listed'], {'PATH': '/usr/bin:/bin'}) == 'listed\n'


def test_runner_cancel_stops_a_running_transfer():
    runner = cs.RsyncRunner()
    threading.Timer(.3, runner.cancel).start()
    started = time.monotonic()
    with pytest.raises(InterruptedError):
        runner.run(['sleep', '30'], {'PATH': '/usr/bin:/bin'})
    assert time.monotonic() - started < 5


def test_transfer_fails_when_verification_finds_differences_and_writes_marker_last(tmp_path):
    write_config(tmp_path / 'config')
    queue = cs.CollectionSync(tmp_path / 'queue', tmp_path / 'config' / 'upload.json',
                              inspect=fake_inspect([]), retry_base_s=.02, retry_max_s=.05)
    source = sealed(tmp_path)
    task = queue.submit('batch-1', source, DIR)
    ran = []

    class Runner:
        mismatch = True

        def run(self, argv, env, *, step):
            ran.append(step)
            if step == '逐文件校验' and self.mismatch:
                return '>fc.t...... videos/b.mp4\n'
            return ''
    queue.runner = Runner()
    config = cs.load_config(tmp_path / 'config' / 'upload.json')
    with pytest.raises(cs.TransferError, match='1 个文件与本地不一致'):
        queue.transfer(task, config)
    assert ran == ['准备目录', '推送数据', '逐文件校验']         # 校验不过：不发 manifest、不写完成标记
    queue.runner.mismatch = False
    result = queue.transfer(task, config)
    assert ran[3:] == ['准备目录', '推送数据', '逐文件校验', '推送 manifest.json', '写完成标记']
    assert result['folder'] == f'{DIR}/batch-1' and result['ready_file'] == f'{DIR}/batch-1/.ready.json'
    marker = json.loads((tmp_path / 'queue' / 'ready' / 'batch-1.ready.json').read_text())
    assert marker['schema'] == 'so101.collection-sync.v1' and marker['id'] == 'batch-1' and marker['folder'] == 'batch-1'
    assert marker['content_hash'] == task['content_hash'] and marker['file_count'] == task['file_count']


# ------------------------------------------------------------------ 4. 持久化队列

class FakeTransfer:
    def __init__(self):
        self.calls = 0
        self.errors = []

    def __call__(self, task, config):
        self.calls += 1
        if self.errors:
            raise self.errors.pop(0)
        folder = task['remote_dir'] + '/' + task['id']
        return {'protocol': 'rsync', 'host': config['sync']['host'], 'port': 873, 'folder': folder,
                'ready_file': folder + '/.ready.json', 'target': config['sync']['host'] + ':' + folder}


def make_queue(tmp_path, transfer, inspected=None):
    return cs.CollectionSync(tmp_path / 'queue', tmp_path / 'config' / 'upload.json', transfer=transfer,
                             inspect=fake_inspect(inspected if inspected is not None else []),
                             retry_base_s=.02, retry_max_s=.05)


def test_submit_is_idempotent_and_inspects_only_once(tmp_path):
    inspected = []
    queue = make_queue(tmp_path, FakeTransfer(), inspected)
    source = sealed(tmp_path)
    first = queue.submit('batch-1', source, DIR + '/')
    for _ in range(5):
        assert queue.submit('batch-1', source, DIR)['id'] == 'batch-1'
    assert len(inspected) == 1 and first['state'] == 'QUEUED' and first['remote_dir'] == DIR
    with pytest.raises(ValueError):
        queue.submit('batch-1', source, '/other/dir')            # 同一批次的目录不能改
    with pytest.raises(ValueError):
        queue.submit('bad id', source, DIR)


def test_missing_config_and_failures_keep_retrying_until_ready(tmp_path):
    transfer = FakeTransfer()
    transfer.errors = [cs.TransferError('推送数据失败（rsync 退出码 255）'), cs.TransferError('逐文件校验发现 1 个文件不一致')]
    queue = make_queue(tmp_path, transfer)
    queue.submit('batch-1', sealed(tmp_path), DIR)
    queue.start()
    try:
        waiting = until(lambda: (t := queue.get('batch-1'))['state'] == 'TRANSFER_RETRY' and t)
        assert '未配置同步服务器' in waiting['error'] and transfer.calls == 0
        write_config(tmp_path / 'config')                         # 配好配置文件，不重启
        done = until(lambda: (t := queue.get('batch-1'))['state'] == 'READY' and t)
        assert transfer.calls == 3 and done['error'] is None      # 两次失败后自己重试成功
        assert done['result']['ready_file'] == f'{DIR}/batch-1/.ready.json'
        assert queue.public_status('batch-1')['upload_url'] == DIR and queue.can_cleanup('batch-1')
        time.sleep(.2)
        assert transfer.calls == 3                                # READY 后不再推送
    finally:
        assert queue.stop()


def test_batch_without_upload_url_waits_for_deploy_root_then_uses_default_dir(tmp_path):
    transfer = FakeTransfer()
    queue = make_queue(tmp_path, transfer)
    source = sealed(tmp_path)
    write_config(tmp_path / 'config')                              # 配了服务器，但没配部署根目录
    first = queue.submit('batch-2', source)
    assert first['remote_dir'] == '' and queue.submit('batch-2', source, None)['id'] == 'batch-2'
    with pytest.raises(ValueError):
        queue.submit('batch-2', source, DIR)                       # 已按默认目录登记，不能再改成别的目录
    queue.start()
    try:
        waiting = until(lambda: (t := queue.get('batch-2'))['state'] == 'TRANSFER_RETRY' and t)
        assert 'COLLECTION_SYNC_DEPLOY_ROOT' in waiting['error'] and transfer.calls == 0
        write_config(tmp_path / 'config', {'protocol': 'rsync', 'host': '192.168.1.16', 'user': 'robot',
                                           'password_file': 'sync.password', 'deploy_root': '/home/unitree/deploy'})
        done = until(lambda: (t := queue.get('batch-2'))['state'] == 'READY' and t)
        folder = '/home/unitree/deploy/action-composer/data/artifacts/data_set/batch-2'
        assert transfer.calls == 1 and done['remote_dir'] == ''     # 队列里仍记“默认目录”，实际目录在推送时拼
        assert done['result']['folder'] == folder and done['result']['ready_file'] == folder + '/.ready.json'
        status = queue.public_status('batch-2')
        assert status['upload_url'] is None and status['uses_default_dir'] and status['folder'] == folder
    finally:
        assert queue.stop()


def test_restart_resumes_unfinished_and_never_repeats_ready(tmp_path):
    write_config(tmp_path / 'config')
    blocked = threading.Event()
    release = threading.Event()

    def slow(task, config):
        blocked.set()
        release.wait(5)
        raise InterruptedError('同步已取消（容器正在停止）')
    queue = make_queue(tmp_path, slow)
    queue.submit('batch-1', sealed(tmp_path, 'one'), DIR)
    queue.start()
    until(blocked.is_set)
    release.set()
    assert queue.stop()
    assert queue.get('batch-1')['state'] != 'READY'
    transfer = FakeTransfer()
    restarted = make_queue(tmp_path, transfer)                    # 模拟容器重启：新实例接管同一持久化队列
    restarted.start()
    try:
        until(lambda: restarted.get('batch-1')['state'] == 'READY')
        assert transfer.calls == 1
    finally:
        assert restarted.stop()
    again = make_queue(tmp_path, transfer)
    again.start()
    try:
        time.sleep(.2)
        assert transfer.calls == 1                                # 已 READY 的不会重复推送
    finally:
        assert again.stop()


def test_changed_snapshot_is_never_pushed(tmp_path):
    write_config(tmp_path / 'config')
    transfer = FakeTransfer()
    source = sealed(tmp_path)
    queue = make_queue(tmp_path, transfer)
    queue.submit('batch-1', source, DIR)
    (source / 'videos' / 'b.mp4').write_bytes(b'changed!')
    queue.start()
    try:
        task = until(lambda: (t := queue.get('batch-1'))['state'] == 'TRANSFER_RETRY' and t)
        assert '已被改动' in task['error'] and transfer.calls == 0
    finally:
        assert queue.stop()


def test_only_one_worker_per_queue(tmp_path):
    first, second = make_queue(tmp_path, FakeTransfer()), make_queue(tmp_path, FakeTransfer())
    assert first.start() is True
    try:
        assert second.start() is False                           # 第二个实例不推送，但不报错
    finally:
        assert first.stop() and second.stop()
    assert second.start() is True
    assert second.stop()
