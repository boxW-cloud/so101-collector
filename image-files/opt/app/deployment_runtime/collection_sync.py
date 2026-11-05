"""采集容器后台同步模块（v10r9）：结束采集后把已封存的数据集推到同步服务器，由容器自己保证成功。

一个采集批次 = 一个同步任务，任务 ID 就是开始采集时上游下发的 id。

  结束采集（POST /collection/finish）
    服务器上的目录 <upload_url>：
      - 请求没带 upload_url（默认）：<部署根目录>/action-composer/data/artifacts/data_set，
        部署根目录来自 .env 的 COLLECTION_SYNC_DEPLOY_ROOT（经 apply-env.py 写进配置的 deploy_root）；
      - 请求带了 upload_url（绝对路径）：以请求为准。
    → 封存快照：视频 / Parquet / 元数据全部写完、不再变化（collection_snapshots 负责）
    → submit()：首次登记时完整校验一次快照，写进持久化队列（/data/rsync-uploads）
    → 准备目录：<upload_url> 的最后一级（如 data_set）不存在就建（只建这一级，上一级必须已存在）
    → 推送数据：rsync 到 <upload_url>/<id>/，断点续传；manifest.json 先不发
    → 逐文件校验：按内容 checksum 比对远端与本地，有任何不一致就算失败
    → 推送 manifest.json
    → 完成标记：最后写 <upload_url>/<id>/.ready.json（在这一批的文件夹里，固定文件名）——
      服务器端 / 前端看到它，才表示这一批完整、已校验
    → READY
  任何一步失败（断网、密码错、目录不存在或不可写、校验不一致）都按退避自动重试，直到成功；
  容器重启后从头再跑一遍这几步，已传完的文件 rsync 会直接跳过。

同步服务器地址、端口、账号、密码/私钥、部署根目录只来自配置文件 /config/upload.json（由 .env 经 apply-env.py 生成），
采集请求最多只决定服务器上的目录。配置每次尝试都重新读取，改完不用重启容器；
默认目录在每次推送时才按当前配置拼出来，所以部署根目录后补或改了，等待中的批次会用上新值。
本模块从不删除本地数据，也不删除服务器上的任何文件。
"""
import hashlib
import json
import os
import re
import shlex
import signal
import sqlite3
import stat
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path


# ------------------------------------------------------------------ 常量

SYNC_PROTOCOL = 'so101.sync.v2'           # 冻结在批次 finish.destination 里的协议名（v2：upload_url 是服务器目录或空）
DEFAULT_ARTIFACTS_SUBDIR = 'action-composer/data/artifacts/data_set'   # 不带 upload_url 时，部署根目录下的固定子目录
READY_SCHEMA = 'so101.collection-sync.v1' # 完成标记 <id>/.ready.json 的格式名
READY_NAME = '.ready.json'                  # 完成标记在每批文件夹里的文件名（最后发送）
DEFAULT_CONFIG_PATH = '/config/upload.json'
QUEUE_FILE = 'sync-queue-v2.sqlite3'       # 与 r7 的 sync-queue.sqlite3（含通知阶段）分开
RETRY_BASE_S = 5                           # 第一次重试等 5 秒，之后每次翻倍
RETRY_MAX_S = 120                          # 最多等 2 分钟：网络恢复后 2 分钟内自动继续
RSYNC_IO_TIMEOUT_S = 120                   # rsync 超过 120 秒没有数据流动就断开，进入重试
SSH_CONNECT_TIMEOUT_S = 15
MAX_REMOTE_DIR = 1024
ERROR_TAIL_CHARS = 600                     # 错误里只保留 rsync 输出的最后一段
ASKPASS_SCRIPT = Path(__file__).with_name('sync_askpass.sh')

_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z')
_HOST = re.compile(r'[A-Za-z0-9][A-Za-z0-9.-]{0,252}\Z')
_USER = re.compile(r'[A-Za-z_][A-Za-z0-9_.-]{0,63}\Z')
_PATH_PART = re.compile(r'[A-Za-z0-9_+][A-Za-z0-9_.+-]*\Z')   # 不以 . 或 - 开头：没有 ..、隐藏目录或像参数的路径
_ITEMIZED = re.compile(r'([<>ch.*])([fdLDS])\S{9} (.+)')        # rsync --itemize-changes 的一行


class SyncConfigError(Exception):
    """配置缺失或写错。任务留在队列里，修正配置后下一次重试自动继续。"""


class TransferError(Exception):
    """推送或校验没有成功。整批稍后重试。"""


# ------------------------------------------------------------------ 1. 配置文件

_REMOVED_KEYS = {
    'platform': 'v10r8 不再通知平台，请删掉 "platform" 段',
    'remote_root': 'remote_root 已不用：服务器目录改用 deploy_root（.env 的 COLLECTION_SYNC_DEPLOY_ROOT），请删掉 remote_root',
}


def load_config(path):
    """读取并检查 upload.json，返回规范化后的配置。只检查密钥文件，不读取其内容。"""
    path = Path(path)
    if not path.is_file():
        raise SyncConfigError(f'未配置同步服务器：找不到 {path}')
    try:
        raw = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, UnicodeError, ValueError) as exc:
        raise SyncConfigError(f'{path} 不是合法的 JSON：{exc}') from None
    if not isinstance(raw, dict) or not isinstance(raw.get('sync'), dict):
        raise SyncConfigError(f'{path} 缺少 "sync" 段')
    _known_keys(raw, {'sync'}, '配置文件')
    return {'sync': _sync_section(raw['sync'], path.parent)}


def _known_keys(section, allowed, label):
    # 以下划线开头的键当注释用（JSON 不支持注释），直接忽略。
    for key in section:
        if key in _REMOVED_KEYS and key not in allowed:
            raise SyncConfigError(_REMOVED_KEYS[key])
    unknown = sorted(k for k in section if not str(k).startswith('_') and k not in allowed)
    if unknown:
        raise SyncConfigError(f'{label} 里有不认识的字段：{", ".join(unknown)}')


def _sync_section(raw, base):
    _known_keys(raw, {'protocol', 'host', 'port', 'user', 'password_file', 'identity_file',
                      'known_hosts_file', 'accept_new_host_key', 'bwlimit_kbps', 'deploy_root'}, '"sync" 段')
    protocol = raw.get('protocol', 'ssh')
    if protocol not in ('ssh', 'rsync'):
        raise SyncConfigError('sync.protocol 只能是 "ssh"（经 SSH 加密传输）或 "rsync"（rsync 守护进程，明文传输）')
    host = raw.get('host')
    if not isinstance(host, str) or not _HOST.fullmatch(host):
        raise SyncConfigError('sync.host 必须是同步服务器的 IP 或主机名')
    port = raw.get('port', 22 if protocol == 'ssh' else 873)
    if type(port) is not int or not 1 <= port <= 65535:
        raise SyncConfigError('sync.port 必须是 1-65535 的整数')
    user = raw.get('user')
    if not isinstance(user, str) or not _USER.fullmatch(user):
        raise SyncConfigError('sync.user 必须是同步服务器上的账号名')
    password = _secret_file(raw['password_file'], base, 'sync.password_file') if raw.get('password_file') else None
    identity = _secret_file(raw['identity_file'], base, 'sync.identity_file') if raw.get('identity_file') else None
    accept_new = raw.get('accept_new_host_key', False)
    if type(accept_new) is not bool:
        raise SyncConfigError('sync.accept_new_host_key 只能是 true 或 false')
    known_hosts = None
    if protocol == 'ssh':
        if bool(password) == bool(identity):
            raise SyncConfigError('SSH 方式需要 password_file（账号密码）或 identity_file（私钥），二选一')
        if raw.get('known_hosts_file'):
            known_hosts = _public_file(raw['known_hosts_file'], base, 'sync.known_hosts_file')
        elif not accept_new:
            raise SyncConfigError('SSH 方式需要 known_hosts_file（在宿主机执行 ssh-keyscan -p 端口 服务器IP > config/known_hosts 生成），'
                                  '或者设置 accept_new_host_key: true（首次连接时记住服务器指纹）')
    else:
        if identity:
            raise SyncConfigError('rsync 守护进程方式不使用私钥，请改用 password_file')
        if not password:
            raise SyncConfigError('rsync 守护进程方式需要 password_file（账号对应的密码）')
    bwlimit = raw.get('bwlimit_kbps', 0)
    if type(bwlimit) is not int or bwlimit < 0:
        raise SyncConfigError('sync.bwlimit_kbps 必须是 >= 0 的整数（0 表示不限速）')
    deploy_root = raw.get('deploy_root') or None     # 可以不配：只有不带 upload_url 的批次才需要它
    if deploy_root is not None:
        try:
            deploy_root = normalize_remote_dir(deploy_root)
        except ValueError:
            raise SyncConfigError('sync.deploy_root（.env 的 COLLECTION_SYNC_DEPLOY_ROOT）必须是服务器上的绝对路径，'
                                  '例如 /home/unitree/deploy') from None
    return {'protocol': protocol, 'host': host, 'port': port, 'user': user,
            'password_file': password, 'identity_file': identity,
            'known_hosts_file': known_hosts, 'bwlimit_kbps': bwlimit, 'deploy_root': deploy_root}


def resolve_remote_dir(remote_dir, config):
    """本批在服务器上的目录：请求带了 upload_url 就用它，否则 <部署根目录>/action-composer/data/artifacts/data_set。"""
    if remote_dir:
        return remote_dir
    deploy_root = config['sync']['deploy_root']
    if not deploy_root:
        raise SyncConfigError('未配置部署根目录：结束采集没带 upload_url 时，数据放到 <部署根目录>/'
                              f'{DEFAULT_ARTIFACTS_SUBDIR}；请在 .env 填 COLLECTION_SYNC_DEPLOY_ROOT 后执行 ./apply-env.py')
    return deploy_root + '/' + DEFAULT_ARTIFACTS_SUBDIR


def _resolve(value, base, label):
    if not isinstance(value, str) or not value or any(ord(c) < 32 for c in value):
        raise SyncConfigError(f'{label} 必须是文件名')
    path = Path(value)
    return path if path.is_absolute() else Path(base) / path   # 相对路径按配置目录解析


def _secret_file(value, base, label):
    # 密码、私钥：只允许属主读写。rsync 和 ssh 自己也会拒绝权限过宽的文件。
    path = _resolve(value, base, label)
    info = _regular_file(path, label)
    if os.name != 'nt' and info.st_mode & 0o077:
        raise SyncConfigError(f'{label} 权限过宽（{oct(info.st_mode & 0o777)}），请在宿主机对 {path.name} 执行 chmod 600')
    return str(path)


def _public_file(value, base, label):
    path = _resolve(value, base, label)
    info = _regular_file(path, label)
    if os.name != 'nt' and info.st_mode & 0o022:
        raise SyncConfigError(f'{label} 不能让其他人可写，请执行 chmod 644 {path.name}')
    return str(path)


def _regular_file(path, label):
    try:
        info = path.stat()
    except OSError:
        raise SyncConfigError(f'{label} 指向的文件不存在：{path}') from None
    if not stat.S_ISREG(info.st_mode) or info.st_size == 0:
        raise SyncConfigError(f'{label} 必须是非空的普通文件：{path}')
    return info


# ------------------------------------------------------------------ 2. upload_url：服务器上的目录

def normalize_remote_dir(value):
    """检查 finish 的 upload_url：同步服务器上已存在的绝对目录，例如 /xxx/action-composer/data/artifacts。

    数据会放到 <upload_url>/<id>/，完成标记是 <upload_url>/<id>/.ready.json。
    rsync 守护进程方式下，第一段是 rsync 模块名，例如 /incoming/artifacts。
    """
    if (not isinstance(value, str) or not value or len(value) > MAX_REMOTE_DIR
            or any(c.isspace() or ord(c) < 32 for c in value)):
        raise ValueError('upload_url must be an absolute directory on the sync server, e.g. /xxx/action-composer/data/artifacts')
    if not value.startswith('/'):
        raise ValueError('upload_url must be an absolute directory starting with /, e.g. /xxx/action-composer/data/artifacts')
    parts = [part for part in value.split('/') if part]
    if not parts:
        raise ValueError('upload_url must not be the root directory /')
    if any(not _PATH_PART.fullmatch(part) for part in parts):
        raise ValueError('each upload_url path segment may contain only letters, digits, "_", "+", "." and "-", '
                         'and must not start with "." or "-"')
    return '/' + '/'.join(parts)


# ------------------------------------------------------------------ 3. rsync 命令

def _login(config, known_hosts_store):
    """返回 (登录相关的 rsync 参数, 环境变量, 把远端路径变成 rsync 目标的函数)。密码只经文件传递。"""
    sync = config['sync']
    env = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'HOME': os.environ.get('HOME', '/tmp')}
    if sync['protocol'] == 'rsync':
        args = ['--password-file=' + sync['password_file']]
        return args, env, lambda path: f'rsync://{sync["user"]}@{sync["host"]}:{sync["port"]}/{path.lstrip("/")}'
    ssh = ['ssh', '-F', '/dev/null', '-T', '-p', str(sync['port']),
           '-o', f'ConnectTimeout={SSH_CONNECT_TIMEOUT_S}', '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=3',
           '-o', 'ForwardAgent=no', '-o', 'ClearAllForwardings=yes', '-o', 'PermitLocalCommand=no',
           '-o', 'IdentityAgent=none', '-o', 'GlobalKnownHostsFile=/dev/null']
    if sync['known_hosts_file']:
        ssh += ['-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=' + sync['known_hosts_file']]
    else:
        # accept_new_host_key：首次连接记住服务器指纹（存在持久化队列目录），之后指纹变了就拒绝连接。
        ssh += ['-o', 'StrictHostKeyChecking=accept-new', '-o', 'UserKnownHostsFile=' + str(known_hosts_store)]
    if sync['identity_file']:
        ssh += ['-i', sync['identity_file'], '-o', 'BatchMode=yes', '-o', 'IdentitiesOnly=yes',
                '-o', 'PasswordAuthentication=no']
    else:
        # 账号密码登录：OpenSSH 通过 SSH_ASKPASS 调用 sync_askpass.sh，从只读挂载的密码文件读密码。
        ssh += ['-o', 'BatchMode=no', '-o', 'PubkeyAuthentication=no',
                '-o', 'PreferredAuthentications=password,keyboard-interactive', '-o', 'NumberOfPasswordPrompts=1']
        env.update(SSH_ASKPASS=str(ASKPASS_SCRIPT), SSH_ASKPASS_REQUIRE='force',
                   SO101_SYNC_PASSWORD_FILE=sync['password_file'])
    return ['-e', shlex.join(ssh)], env, lambda path: f'{sync["user"]}@{sync["host"]}:{path}'


def build_sync_steps(config, source_root, remote_dir, item_id, ready_file, *, known_hosts_store, empty_dir=None):
    """一批数据依次执行的 rsync 命令，返回 ([(步骤名, argv), ...], env)。

    给了 empty_dir（本地空目录）时，最前面多一步“准备目录”：把空目录同步到 <remote_dir>/，
    rsync 会在 <remote_dir> 不存在时建出这一级（例如 data_set），上一级不存在则失败、照常重试——
    不会在服务器上建出一串目录。
    """
    login, env, remote = _login(config, known_hosts_store)
    sync = config['sync']
    common = ['rsync', f'--timeout={RSYNC_IO_TIMEOUT_S}'] + (
        [f'--bwlimit={sync["bwlimit_kbps"]}'] if sync['bwlimit_kbps'] else []) + login
    folder = remote_dir.rstrip('/') + '/' + item_id
    source = str(source_root).rstrip('/') + '/'
    prepare = [] if empty_dir is None else [
        # -d 只同步目录本身、不递归；源是空目录，所以只会建出缺的最后一级，不改动已有内容
        ('准备目录', common + ['-d', '--', str(empty_dir).rstrip('/') + '/', remote(remote_dir.rstrip('/')) + '/'])]
    return prepare + [
        # -r 递归，-t 保留修改时间（重试时已传完的文件直接跳过）；--partial-dir：中断的大文件下次接着传。
        ('推送数据', common + ['-rt', '--partial', '--partial-dir=.rsync-partial', '--exclude=/manifest.json',
                            '--', source, remote(folder) + '/']),
        # -c 按内容 checksum 比较，--dry-run 只比较不传输；输出里列出的文件就是与本地不一致的。
        ('逐文件校验', common + ['-rtc', '--dry-run', '--itemize-changes', '--exclude=/manifest.json',
                             '--', source, remote(folder) + '/']),
        # manifest.json 最后发：目录里有 manifest 时，其余文件一定已经到齐并校验过。
        ('推送 manifest.json', common + ['-tc', '--', source + 'manifest.json', remote(folder) + '/manifest.json']),
        # 完成标记最后写，放在这一批的文件夹里（rsync 先写临时文件再改名，服务器端不会读到半个文件）。
        ('写完成标记', common + ['-t', '--', str(ready_file), remote(folder + '/' + READY_NAME)]),
    ], env


def differences(itemized_output):
    """从校验步骤的 --itemize-changes 输出里挑出“需要传输/新建”的条目，即与本地不一致的文件。"""
    found = []
    for line in itemized_output.splitlines():
        match = _ITEMIZED.fullmatch(line.strip())
        if match and match.group(1) in '<>c':
            found.append(match.group(3))
    return found


_RSYNC_HINTS = {
    3: '服务器上的目录不存在或不可访问（upload_url 目录需要事先建好）',
    5: '登录 rsync 服务失败（账号或密码不对，或该账号不能使用这个模块）',
    10: '连不上同步服务器（地址或端口不对，或网络不通）',
    11: '服务器上写文件失败（目录不存在、没有写权限或磁盘满）',
    12: 'rsync 数据流中断（常见于传输中途断网，或服务器上没有安装 rsync）',
    23: '部分文件没有传成功（常见于目录不存在或没有写权限）',
    30: '传输超时（网络卡住）',
    35: '等待 rsync 服务响应超时',
    127: '服务器上没有安装 rsync',
    255: 'SSH 连接或登录失败（地址、端口、账号、密码/私钥或服务器指纹不对，或网络不通）',
}


class RsyncRunner:
    """执行 rsync 子进程；cancel() 会结束整个进程组（rsync 和它启动的 ssh）。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._process = None
        self._cancelled = threading.Event()

    def reset(self):
        self._cancelled.clear()

    def cancel(self):
        self._cancelled.set()
        with self._lock:
            process = self._process
        _terminate(process)

    def run(self, argv, env, *, step='rsync'):
        """运行一条 rsync 命令，返回它的输出文本；失败抛 TransferError（带原因提示和输出末尾）。"""
        # 输出写临时文件而不是管道：大数据集传很久，管道写满会卡死，也避免占内存。
        with tempfile.TemporaryFile() as output:
            with self._lock:
                if self._cancelled.is_set():
                    raise InterruptedError('同步已取消（容器正在停止）')
                process = self._process = subprocess.Popen(
                    argv, env=env, stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT,
                    start_new_session=(os.name == 'posix'))
            try:
                while process.poll() is None:
                    if self._cancelled.wait(.2):
                        raise InterruptedError('同步已取消（容器正在停止）')
            finally:
                _terminate(process)
                with self._lock:
                    self._process = None
            output.seek(0)
            text = output.read().decode('utf-8', errors='replace')
        if process.returncode != 0:
            hint = _RSYNC_HINTS.get(process.returncode, '见 rsync 输出')
            raise TransferError(f'{step}失败（rsync 退出码 {process.returncode}）：{hint}。'
                                f'rsync 输出：{text.strip()[-ERROR_TAIL_CHARS:]}')
        return text


def _terminate(process):
    if process is None or process.poll() is not None:
        return
    try:
        if os.name == 'posix':
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        process.wait(timeout=3)
    except ProcessLookupError:
        process.wait()
    except subprocess.TimeoutExpired:
        try:
            if os.name == 'posix':
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            pass
        process.wait(timeout=3)


# ------------------------------------------------------------------ 4. 封存快照检查

def inspect_sealed_snapshot(path):
    """完整校验一次封存快照：逐个文件算 sha256、读每行 Parquet、解码视频。只在首次登记时做。"""
    from lerobot_sdk import DatasetManager
    path = Path(path)
    info = DatasetManager().inspect(path)
    manifest = path / 'manifest.json'
    return {'content_hash': info.ref.content_hash,
            'manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
            'file_count': info.files + 1, 'total_bytes': info.bytes + manifest.stat().st_size}


def quick_source_check(task):
    """每次推送前后的轻量检查：manifest 字节没变、清单里每个文件都在且大小没变。"""
    root = Path(task['source_root'])
    manifest = root / 'manifest.json'
    try:
        raw = manifest.read_bytes()
        files = json.loads(raw)['files']
        sizes_ok = all((root / name).stat().st_size == entry['bytes'] for name, entry in files.items())
    except (OSError, ValueError, KeyError, TypeError):
        raise TransferError('本地封存快照不完整或已被改动，停止推送') from None
    if hashlib.sha256(raw).hexdigest() != task['manifest_sha256'] or not sizes_ok or len(files) + 1 != task['file_count']:
        raise TransferError('本地封存快照已被改动，停止推送')


# ------------------------------------------------------------------ 5. 持久化队列与后台 worker

_COLUMNS = ('id', 'source_root', 'remote_dir', 'content_hash', 'manifest_sha256', 'file_count', 'total_bytes',
            'state', 'attempts', 'next_attempt_at', 'result', 'error', 'created_at', 'updated_at')


class CollectionSync:
    """持久化同步队列 + 单个后台 worker（一台机器同时只推一个数据集）。

    state：QUEUED（排队）/ UPLOADING（推送或校验中）/ TRANSFER_RETRY（失败，等待重试）/ READY（已推送、
    已校验、已写完成标记）。没有终止的失败状态：只要还没 READY，就一直按退避重试。
    """

    def __init__(self, root, config_path=DEFAULT_CONFIG_PATH, *, transfer=None, inspect=None,
                 retry_base_s=RETRY_BASE_S, retry_max_s=RETRY_MAX_S):
        self.root = Path(root).absolute()
        self.root.mkdir(parents=True, exist_ok=True)
        if os.name == 'posix':
            self.root.chmod(0o700)
        self.config_path = Path(config_path)
        self.path = self.root / QUEUE_FILE
        self.known_hosts_store = self.root / 'known_hosts'
        self.ready_dir = self.root / 'ready'        # 本地保留一份每批的完成标记
        self.runner = RsyncRunner()
        self.transfer = transfer or self._rsync_transfer
        self.inspect = inspect or inspect_sealed_snapshot
        self.retry_base_s, self.retry_max_s = retry_base_s, retry_max_s
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread = None
        self._lifecycle = threading.Lock()
        self._worker_lock = None
        with self._db() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS sync_tasks (
                id TEXT PRIMARY KEY,            -- 上游下发的采集 id
                source_root TEXT NOT NULL,      -- 已封存快照目录（/data/collection-snapshots/...）
                remote_dir TEXT NOT NULL,       -- finish 的 upload_url；空字符串 = 推送时用默认目录
                content_hash TEXT NOT NULL, manifest_sha256 TEXT NOT NULL,
                file_count INTEGER NOT NULL, total_bytes INTEGER NOT NULL,
                state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0, result TEXT, error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL)''')
        if os.name == 'posix':
            self.path.chmod(0o600)

    @contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                yield db
        finally:
            db.close()

    # ---- 登记与查询

    def submit(self, item_id, source_root, remote_dir=None):
        """登记一个已封存批次（可重复调用）。只有第一次登记时才做完整校验。

        remote_dir 为空表示结束采集没带 upload_url：推送时再按当前配置拼默认目录。
        """
        if not isinstance(item_id, str) or not _ID.fullmatch(item_id):
            raise ValueError('Invalid collection id')
        remote_dir = normalize_remote_dir(remote_dir) if remote_dir else ''
        source = str(Path(source_root).resolve())
        existing = self._find(item_id)
        if existing is None:
            summary = self.inspect(source)
            now = time.time()
            with self._db() as db:
                db.execute('BEGIN IMMEDIATE')
                if db.execute('SELECT 1 FROM sync_tasks WHERE id=?', (item_id,)).fetchone() is None:
                    db.execute('INSERT INTO sync_tasks(id,source_root,remote_dir,content_hash,manifest_sha256,file_count,'
                               'total_bytes,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                               (item_id, source, remote_dir, summary['content_hash'], summary['manifest_sha256'],
                                summary['file_count'], summary['total_bytes'], 'QUEUED', now, now))
            self._wake.set()
            existing = self.get(item_id)
        if existing['source_root'] != source or existing['remote_dir'] != remote_dir:
            raise ValueError('This collection id is already bound to other data or another upload_url')
        return existing

    def get(self, item_id):
        task = self._find(item_id)
        if task is None:
            raise KeyError(item_id)
        return task

    def _find(self, item_id):
        with self._db() as db:
            row = db.execute('SELECT * FROM sync_tasks WHERE id=?', (item_id,)).fetchone()
        if row is None:
            return None
        task = dict(row)
        task['result'] = json.loads(task['result']) if task['result'] else None
        return task

    def public_status(self, item_id):
        """给接口/工作台看的状态，不含任何凭据。"""
        task = self.get(item_id)
        result = task['result'] or {}
        return {'state': task['state'], 'attempts': task['attempts'], 'error': task['error'],
                'sync_target': result.get('target'), 'upload_url': task['remote_dir'] or None,
                'uses_default_dir': not task['remote_dir'], 'folder': result.get('folder'),
                'ready_file': result.get('ready_file'), 'updated_at': task['updated_at']}

    def can_cleanup(self, item_id):
        """只有已推送、已校验、已写完成标记（READY）才允许清理本地数据。本版不自动清理。"""
        return self.get(item_id)['state'] == 'READY'

    # ---- 后台 worker

    def start(self):
        """启动后台 worker。同一队列已有 worker 在跑时不再启动第二个（返回 False），采集接口照常可用。"""
        from filelock import FileLock, Timeout
        with self._lifecycle:
            if self._thread and self._thread.is_alive():
                return True
            lock = FileLock(str(self.root / '.worker.lock'))
            try:
                lock.acquire(timeout=0)    # 同一队列只允许一个 worker，避免两个进程同时推同一份数据
            except Timeout:
                return False
            self._worker_lock = lock
            self._stop.clear()
            self.runner.reset()
            # 容器重启：推到一半的回到 QUEUED，下次从头再跑四步（已传完的文件 rsync 直接跳过）。
            with self._db() as db:
                db.execute("UPDATE sync_tasks SET state='QUEUED', next_attempt_at=0 WHERE state!='READY'")
            self._thread = threading.Thread(target=self._run, name='CollectionSync', daemon=True)
            self._thread.start()
            return True

    def stop(self, timeout_s=5):
        with self._lifecycle:
            self._stop.set()
            self._wake.set()
            self.runner.cancel()
            if self._thread:
                self._thread.join(timeout_s)
            stopped = not (self._thread and self._thread.is_alive())
            if stopped and self._worker_lock is not None:
                self._worker_lock.release()
                self._worker_lock = None
            return stopped

    def _run(self):
        while not self._stop.is_set():
            task = self._take()
            if task is None:
                self._wake.wait(1)
                self._wake.clear()
                continue
            try:
                self._process(task)
            except Exception as exc:
                self._retry_later(task, exc)

    def _take(self):
        """取一个到期的任务（按登记先后），并标记为推送中。"""
        with self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute("SELECT id FROM sync_tasks WHERE state!='READY' AND next_attempt_at<=? "
                             "ORDER BY created_at, id LIMIT 1", (time.time(),)).fetchone()
            if row is None:
                return None
            db.execute("UPDATE sync_tasks SET state='UPLOADING', attempts=attempts+1, updated_at=? WHERE id=?",
                       (time.time(), row['id']))
        return self.get(row['id'])

    def _process(self, task):
        config = load_config(self.config_path)         # 每次重新读，改配置不用重启
        # 没带 upload_url 的批次在这里才拼默认目录：部署根目录后补或改了，下一次重试就用上
        task = {**task, 'remote_dir': resolve_remote_dir(task['remote_dir'], config)}
        quick_source_check(task)
        result = self.transfer(task, config)
        quick_source_check(task)
        self._update(task['id'], state='READY', error=None, result=json.dumps(result, ensure_ascii=False))

    def _retry_later(self, task, exc):
        with self._db() as db:
            db.execute("UPDATE sync_tasks SET state='TRANSFER_RETRY', error=?, next_attempt_at=?, updated_at=? "
                       "WHERE id=? AND state!='READY'",
                       (str(exc)[:2000], time.time() + self._backoff(task['attempts']), time.time(), task['id']))

    def _backoff(self, attempts):
        return min(self.retry_max_s, self.retry_base_s * 2 ** max(0, min(attempts, 20) - 1))

    def _update(self, item_id, **fields):
        fields['updated_at'] = time.time()
        assert set(fields) <= set(_COLUMNS)
        with self._db() as db:
            db.execute('UPDATE sync_tasks SET ' + ', '.join(k + '=?' for k in fields) + ' WHERE id=?',
                       (*fields.values(), item_id))

    def ready_marker(self, task):
        """生成本批的完成标记（本地留存一份，再推到服务器）。"""
        self.ready_dir.mkdir(exist_ok=True)
        path = self.ready_dir / (task['id'] + '.ready.json')
        value = {'schema': READY_SCHEMA, 'id': task['id'], 'folder': task['id'], 'format': 'lerobot_v3',
                 'content_hash': task['content_hash'], 'manifest_sha256': task['manifest_sha256'],
                 'file_count': task['file_count'], 'total_bytes': task['total_bytes'],
                 'completed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
        temporary = path.with_name('.' + path.name + '.tmp')
        temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        temporary.replace(path)
        return path

    def _rsync_transfer(self, task, config):
        ready = self.ready_marker(task)
        empty = self.root / 'empty-dir'           # 准备目录用的本地空目录
        empty.mkdir(exist_ok=True)
        for leftover in empty.iterdir():
            raise TransferError(f'本地空目录 {empty} 里不应该有文件：{leftover.name}')
        steps, env = build_sync_steps(config, task['source_root'], task['remote_dir'], task['id'], ready,
                                      known_hosts_store=self.known_hosts_store, empty_dir=empty)
        for name, argv in steps:
            output = self.runner.run(argv, env, step=name)
            if name == '逐文件校验':
                mismatched = differences(output)
                if mismatched:
                    raise TransferError(f'逐文件校验发现 {len(mismatched)} 个文件与本地不一致'
                                        f'（例如 {mismatched[0]}），稍后重新推送')
        sync = config['sync']
        folder = task['remote_dir'].rstrip('/') + '/' + task['id']
        return {'protocol': sync['protocol'], 'host': sync['host'], 'port': sync['port'],
                'folder': folder, 'ready_file': folder + '/' + READY_NAME, 'target': f'{sync["host"]}:{folder}',
                'verified_files': task['file_count']}


def create_for_platform(platform):
    """采集服务启动时调用。配置文件此时可以还不存在：只有真正推送时才读取。"""
    root = os.environ.get('SO101_SYNC_QUEUE_ROOT') or str(Path(platform.root) / 'rsync-uploads')
    config = os.environ.get('SO101_UPLOAD_CONFIG') or DEFAULT_CONFIG_PATH
    return CollectionSync(root, config)


# ------------------------------------------------------------------ 6. 配置检查命令

def main(argv=None):
    """检查配置：docker exec <容器> /opt/runtime/bin/python -m deployment_runtime.collection_sync [--probe [--dir 目录]]

    不带参数：只检查 upload.json 和密钥文件，打印规范化后的配置（不打印密码内容）。
    --probe：实际登录同步服务器并列出 --dir 目录，只读不写。不写 --dir 时：配了部署根目录就列默认目录
    <部署根目录>/action-composer/data/artifacts/data_set，否则 SSH 列账号主目录（rsync 守护进程方式必须写 --dir）。
    """
    import argparse
    parser = argparse.ArgumentParser(description='检查采集容器的同步服务器配置')
    parser.add_argument('--config', default=os.environ.get('SO101_UPLOAD_CONFIG') or DEFAULT_CONFIG_PATH)
    parser.add_argument('--probe', action='store_true', help='实际登录同步服务器并列出目录（只读）')
    parser.add_argument('--dir', default='', help='要列出的服务器目录，默认是部署根目录下的 action-composer/data/artifacts/data_set')
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except SyncConfigError as exc:
        print('配置有问题：' + str(exc))
        return 2
    print(json.dumps(config, ensure_ascii=False, indent=2))
    if config['sync']['deploy_root']:
        print('不带 upload_url 的批次将放到：' + resolve_remote_dir('', config) + '/<id>/')
    else:
        print('未配置部署根目录（COLLECTION_SYNC_DEPLOY_ROOT）：不带 upload_url 的批次会等待，直到配置好。')
    if not args.probe:
        print('配置格式正确。加 --probe [--dir 服务器目录] 可实际测试登录。')
        return 0
    if not args.dir and config['sync']['deploy_root']:
        args.dir = resolve_remote_dir('', config)
    if not args.dir and config['sync']['protocol'] == 'rsync':
        # 守护进程列模块名不需要登录，测不出密码对不对，所以必须指定一个模块目录
        print('rsync 守护进程方式请用 --dir /模块名[/子目录] 指定要检查的目录')
        return 2
    try:
        directory = normalize_remote_dir(args.dir) + '/' if args.dir else ''
    except ValueError as exc:
        print('--dir 不合法：' + str(exc))
        return 2
    queue_root = Path(os.environ.get('SO101_SYNC_QUEUE_ROOT') or '/data/rsync-uploads')
    login, env, remote = _login(config, queue_root / 'known_hosts')
    target = remote(directory)
    argv = ['rsync', '--list-only', f'--timeout={RSYNC_IO_TIMEOUT_S}', *login, '--', target]
    try:
        listing = RsyncRunner().run(argv, env, step='登录并列出目录')
    except TransferError as exc:
        print(str(exc))
        return 1
    print(listing.rstrip()[-2000:])
    print(f'登录成功，目录可访问：{target}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
