"""真实机器人（Bobac3 等）→ 采集：从 Perceptor 的 Redis 采集队列（capture/v5）取数据，录制、打包、上传都沿用现有流程。

v10r15（2026-11-05）：SO101（仿真 so101-sim 和真机 so101）也走这里，采集容器只采集、不再控制从臂：
  - 观测：Perceptor 推到观测队列（六个关节，度 / 夹爪 0~100，和 LeRobot 相同，units=lerobot；图片 front / wrist）。
  - action：主臂节点 so101-leader 推到动作队列的 teleop/action/v1（source=teleop_leader，input_observation_id=null），
    每帧观测配“时间戳 ≥ 观测状态采样时刻（sources.<state_source>.sampled_at）的第一条主臂 action”，
    相差超过 max_action_skew_s 就跳过这一帧（action=teleop → 数据集规则 leader_action）。同一台主机上两边都是 CLOCK_MONOTONIC。
  - 供数开关：开始采集时由采集容器自己打开（SET collection_enabled 1 NX；已经开着就报错，不和别的采集抢），
    结束时删掉。容器异常退出没来得及删的，下次启动时按 switch-owner.json 删掉（所有 Redis 数据源都这样，含 Bobac3）。
  - 对齐由操作者自己负责，采集容器不检查、不移动从臂。

只在 8910 开始采集时才从队列取数据：设备进程收到 connect（开始采集）时开始消费，会话结束（结束 / 放弃 /
租约过期）时停止。其他时候不碰队列。开始时先丢掉队列里的积压：没人消费时队列会涨到上限（1000 条），
Perceptor 随即停止出数，积压里的图片也早已过期（TTL 60 秒）。

和本机直连的真实从臂（direct_follower）一样，走"仿真那套流程、数据标为真机"的路子，区别是：
  1. 不控制机器人：send / hold 不下发任何东西，机器人由平台（DM / Perceptor）控制。
  2. 每帧的关节和图片来自 Redis：观测队列给关节读数和图片 key，收到后马上按 key 取图。
  3. 数据集的 action 取下一帧的实测状态（2026-11-03；之前是复制当前状态的占位）：机器人由平台控制，
     采集容器拿不到下发的关节指令。规则写在登记的 dataset_schema（action=next_observation_state），
     SDK 录制时每帧先压住，等下一帧到了再写，所以每条最后一帧丢掉。录制时 SDK 拿到的"主臂"仍是当前状态。
  4. 动作队列的事件按 event_id 去重、按 process_id + action_id 合并成一条动作记录，
     存在 /data/redis-capture/<会话>/ 下，不进数据集。
  5. 2026-11-03 起动作队列还可以有"每帧成对的实际下发目标"（schema_version=teleop/action/v1，见 TELEOP_SCHEMA）：
     .env 的 COLLECTION_REDIS_ACTION=paired 时，数据集的 action 用它（按 process_id + input_observation_id 对上观测），
     没配上的帧跳过；默认 next_state 时仍是上面第 3 条。
关节：数据集里写的是机器人自己的关节名和数值（默认 Perceptor 的 joint1~joint6，弧度），robot_type 是机器人类型
（默认 bobac3）——由登记里的 dataset_schema 决定（2026-10-29 起，之前写的是 SO101 的名字）。
SDK 内部的控制和校验仍按 SO101 的 6 个槽位走：前 5 个关节弧度换成度，第 6 个按 (弧度 + 2π) / 4π × 100
放进夹爪槽位（0~100）；写数据集时 SDK 按 dataset_schema 的线性关系换回弧度，manifest 的 joint_mapping 记录了这层换算。

命令行（在采集容器里）：
  python -m deployment_runtime.redis_capture prepare    容器启动时由 supervisor 调用：登记目标、写设备进程配置
  python -m deployment_runtime.redis_capture serve --config /data/isaac-console/redis-capture/peer.json   设备进程
  python -m deployment_runtime.redis_capture peek       只读：开关、队列长度、最新一条观测（LRANGE，不消费）
"""
import base64
import bisect
import hashlib
import io
import json
import math
import os
import re
import socket
import threading
import time
from collections import Counter, OrderedDict
from pathlib import Path

from .store import Fault, canonical

PROVENANCE = 'perceptor_capture_v5'
SCHEMA = 'capture/v5'
# 每帧成对的实际下发目标（2026-11-03，草案）。和观测一样推到动作队列 collection_action_queue：
#   {"schema_version": "teleop/action/v1", "process_id": 与观测相同, "action_id": 唯一编号,
#    "input_observation_id": 与它成对的观测的 observation_id, "timestamp": 实际下发时刻（与观测 timestamp 同一个时钟，秒）,
#    "positions": {关节名: 值}（关节名、单位和观测的 state 相同）, "requested_positions": 可选，同上}
# 下发失败、结果不明时不要发。time_basis、clock_anchor 等其他字段原样存档。
TELEOP_SCHEMA = 'teleop/action/v1'
# .env 的值 → 数据集的 action 规则；teleop = 主臂节点的 action 按时间配对（SO101）
ACTION_MODES = {'next_state': 'next_observation_state', 'paired': 'source_action', 'teleop': 'leader_action'}
PAIRING_CAPACITY = 4096            # 最多记住这么多条还没被取走的成对目标（旧的先丢）
LEADER_SOURCE = 'teleop_leader'    # 主臂节点 action 的 source
LEADER_CAPACITY = 1200             # 主臂 action 最多留这么多条（30 Hz 约 40 秒）
LEADER_KEEP_S = 2.0                # 配对时丢掉比观测早这么多秒以上的主臂 action
SWITCH_KEY = 'collection_enabled'  # 供数开关：值为字符串 1 时 Perceptor 和主臂节点推数据，删掉就停
UNITS = ('rad', 'lerobot')         # 观测 state 的单位：rad = Bobac3（六个关节弧度）；lerobot = SO101（五个关节度、夹爪 0~100）
# 所有 Redis 数据源都登记成 physical（外部机器人数据源）：SO101 仿真也一样，仿真只是另一台机器人（robot_id / robot_type 区分），
# 采集容器不再驱动 Isaac，不需要 Isaac 场景（simulation 目标要求 scene_id）。
MAPPING_RULE = 'perceptor-joints-to-sdk-slots-v2'      # 只是 SDK 内部槽位的换算；数据集里是机器人自己的关节
GRIPPER_SPAN = 4 * math.pi                              # 第 6 个关节在夹爪槽位里覆盖 [-2π, 2π]
PROFILE_SCHEMA = 'so101.redis-capture.profile.v1'
NAMES = ('shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll', 'gripper')
CONFIG_PATH = Path(os.environ.get('SO101_REDIS_CAPTURE_CONFIG', '/config/redis-capture.json'))
RUNTIME_DIR = Path('/data/isaac-console/redis-capture')

DEFAULTS = {
    'robot_id': 'bobac3-capture',
    'robot_type': 'bobac3',            # 写进数据集的 robot_type
    'namespace': '/so101_redis_capture',
    'port': 6379,
    'db': 0,
    'observation_queue': 'collection_observation_queue',
    'action_queue': 'collection_action_queue',
    'fps': 4,                          # 8910 开始采集不带 fps 时的录制帧率（Bobac3 实测出数约 4.8 Hz）
    'joints': ['joint1', 'joint2', 'joint3', 'joint4', 'joint5', 'joint6'],
    'cameras': {'front': [400, 640]},  # 名字 = Perceptor 观测里 images 的 key；尺寸 [高, 宽]
    # 下面三个等待都必须短于设备进程的 2 秒租约（每次请求续 2 秒，超时就判会话结束）：
    # 2026-10-29 实测 sample_wait=2.0 时，送达停顿约 2 秒就会先撞上租约到期。
    # Perceptor 的采样时间很均匀（间隔最长 0.43 秒），但送达有时会晚约 1 秒、随后一起到，所以录制中最多等 1.4 秒。
    'connect_wait_s': 1.5,             # 开始采集时等第一条新观测的时间（供数没开就报错）
    'sample_wait_s': 1.4,              # 录制中等下一条新观测的时间，超过就判供数中断
    'stop_wait_s': 0.5,                # 停止（保存一条 / 结束采集）时等新观测的时间，见 RedisCaptureAdapter.sample
    'io_timeout_s': 5.0,               # SDK 每次请求设备进程的超时，要大于上面两个
    'action': 'next_state',            # 数据集的 action：next_state = 下一帧实测状态；paired = 动作队列里成对的实际下发目标
    'action_wait_s': 0.3,              # paired / teleop：拿到观测后最多再等这么久等它的 action（和 sample_wait_s 加起来要在租约内）
    'max_action_skew_s': 0.1,          # teleop：主臂 action 比观测状态晚超过这么多秒就不算配上
    'units': 'rad',                    # 观测 state 的单位，见 UNITS
    'state_source': 'arm_state',       # 观测 sources 里关节状态那一项的名字（取它的 sampled_at 配主臂 action）
    'camera_store': '/tmp/so101-camera',   # 写给 8910 视频接口看（只在采集期间有画面）；SO101 不写（8910 的画面就是 Perceptor 图片的来源）
    'session_root': '/data/redis-capture',
}


def log(message):
    """设备进程的诊断日志（容器里 /data/isaac-console/retest-redis.log）。只记少量关键事件。"""
    print(time.strftime('%H:%M:%S') + f'.{int(time.time() * 1000) % 1000:03d} [redis-capture] {message}', flush=True)


def canonical_hash(value):
    return 'sha256:' + hashlib.sha256(canonical(value).encode()).hexdigest()


def is_redis_capture(robot):
    return isinstance(robot, dict) and robot.get('kind') == 'physical' and robot.get('provenance') == PROVENANCE


class SupplyBusy(RuntimeError):
    """开始采集时供数开关已经是开着的：可能有别的采集在用，不抢。"""


# 供数开关是谁打开的：采集容器打开时写这个文件，正常关闭时删掉。容器异常退出留下的，下次启动（prepare）时据此删开关。
def switch_owner_path(config):
    return Path(config.get('switch_owner') or RUNTIME_DIR / 'switch-owner.json')


def write_switch_owner(config):
    try:
        _write_json(switch_owner_path(config), {'host': config['host'], 'port': config['port'], 'db': config['db'],
                                                'key': SWITCH_KEY, 'set_at': time.time()})
    except OSError as error:
        log(f'记不下供数开关的打开记录（容器异常退出后要手动 DEL collection_enabled）：{error}')


def clear_switch_owner(config):
    try:
        switch_owner_path(config).unlink(missing_ok=True)
    except OSError:
        pass


def release_stale_switch(config, client_factory=None):
    """容器启动时：上次是本容器打开、没来得及关的供数开关，删掉。返回做了什么（写进 prepare 的结果）。"""
    path = switch_owner_path(config)
    if not path.exists():
        return 'none'
    try:
        client = (client_factory or client_factory_of(config))()
        try:
            client.call('DEL', SWITCH_KEY)
        finally:
            client.close()
        path.unlink(missing_ok=True)
        return 'deleted_stale_switch'
    except Exception as error:
        return f'stale_switch_left: {error}'


# ══════════ 1. Redis：最小 RESP 客户端（镜像里没有 redis 包） ══════════

class RedisClient:
    def __init__(self, host, port=6379, db=0, password=None, timeout=5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.file = self.sock.makefile('rb')
        if password:
            self.call('AUTH', password)
        if db:
            self.call('SELECT', str(db))

    def _send(self, commands):
        payload = b''
        for args in commands:
            parts = [a.encode() if isinstance(a, str) else a for a in args]
            payload += b'*%d\r\n' % len(parts) + b''.join(b'$%d\r\n%s\r\n' % (len(p), p) for p in parts)
        self.sock.sendall(payload)

    def call(self, *args):
        self._send([args])
        return self._read()

    def call_many(self, commands):
        """一次发出多条命令再依次读回复（丢积压时用，免得一条一条等往返）。"""
        if not commands:
            return []
        self._send(commands)
        return [self._read() for _ in commands]

    def _read(self):
        line = self.file.readline()
        if not line:
            raise ConnectionError('Redis 连接断开')
        kind, rest = line[:1], line[1:-2]
        if kind == b'+':
            return rest.decode()
        if kind == b'-':
            raise RuntimeError('Redis 报错：' + rest.decode())
        if kind == b':':
            return int(rest)
        if kind == b'$':
            n = int(rest)
            return None if n < 0 else self.file.read(n + 2)[:-2]
        if kind == b'*':
            n = int(rest)
            return None if n < 0 else [self._read() for _ in range(n)]
        raise RuntimeError('无法识别的 Redis 回复')

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def client_factory_of(config):
    password = None
    if config.get('password_file'):
        password = Path(config['password_file']).read_text(encoding='utf-8').strip() or None
    return lambda: RedisClient(config['host'], config['port'], config['db'], password)


# ══════════ 2. 换算 ══════════

def to_slots(state, joints, units='rad'):
    """Perceptor 的 6 个关节 → SDK 内部的 6 个槽位。读数缺失、不是有限数或超出范围时返回 None。
    units=rad（Bobac3）：弧度，±2π 以内，换算见文件开头；units=lerobot（SO101）：度 / 夹爪 0~100，原样放进槽位。"""
    if not isinstance(state, dict):
        return None
    out = {}
    for source, name in zip(joints, NAMES):
        value = state.get(source)
        if type(value) not in (int, float) or not math.isfinite(value):
            return None
        if units == 'lerobot':
            if (name == 'gripper' and not -1e-6 <= value <= 100 + 1e-6) or (name != 'gripper' and abs(value) > 360):
                return None
            out[name + '.pos'] = float(value)
            continue
        if abs(value) > 2 * math.pi:
            return None
        if name == 'gripper':
            out[name + '.pos'] = (value + GRIPPER_SPAN / 2) / GRIPPER_SPAN * 100
        else:
            out[name + '.pos'] = math.degrees(value)
    return out


def dataset_schema_of(config):
    """数据集里的关节：机器人自己的名字和弧度。from_internal 是 to_slots 的逆运算（SDK 写数据集时用）。
    action（2026-11-03）：默认 next_state（Bobac3 由平台控制，采集容器拿不到下发的关节指令，取下一帧的实测状态）；
    paired 时用动作队列里和观测成对的实际下发目标。
    units=lerobot（SO101，2026-11-05）：数据集就是标准 SO101 格式（so101_follower，关节名、单位不换），只写 action 规则。"""
    if config.get('units') == 'lerobot':
        return {'action': ACTION_MODES[config.get('action', 'teleop')]}
    return {'robot_type': config['robot_type'], 'joint_names': list(config['joints']), 'units': ['rad'] * 6,
            'from_internal': [[math.pi / 180, 0.0]] * 5 + [[GRIPPER_SPAN / 100, -GRIPPER_SPAN / 2]],
            'action': ACTION_MODES[config.get('action', 'next_state')]}


def decode_image(raw):
    import numpy as np
    from PIL import Image
    with Image.open(io.BytesIO(raw)) as image:
        return np.asarray(image.convert('RGB'), dtype=np.uint8)


def encode(rgb, fmt):
    from PIL import Image
    stream = io.BytesIO()
    if fmt == 'PNG':
        Image.fromarray(rgb).save(stream, format='PNG', compress_level=1)
    else:
        Image.fromarray(rgb).save(stream, format='JPEG', quality=80)
    return stream.getvalue()


# ══════════ 3. 消费：只在一次采集会话期间运行 ══════════

class Observation:
    __slots__ = ('sequence', 'received_ns', 'captured_ns', 'positions', 'images', 'image_ns', 'evidence')

    def __init__(self, **values):
        for key, value in values.items():
            setattr(self, key, value)


class Consumer:
    """开始时丢掉积压，然后两个线程分别 BRPOP 观测队列和动作队列，直到 stop()。"""

    def __init__(self, config, client_factory, *, frame_store=None):
        self.config = config
        self.client_factory = client_factory
        self.frame_store = frame_store
        self.cond = threading.Condition()
        self.latest = None
        self.sequence = 0
        self.error = None
        self.last_problem = None
        self.stop_event = threading.Event()
        self.threads = []
        self.stats = Counter()
        self.gap_counts = {}            # process_id -> [第一次看到的 data_gap_count, 最近一次]
        self.actions = {}
        self.seen_events = set()
        self.teleop = OrderedDict()      # (process_id, input_observation_id) -> 成对的实际下发目标（teleop/action/v1）
        self.leader, self.leader_ts = [], []   # 主臂 action 和它们的 timestamp，按 timestamp 排序（teleop 按时间配对）
        self.switch_owned = False        # 供数开关是本次采集打开的：停止时要删掉
        self.started = None
        self.first_ts = self.last_ts = None
        self.last_process = None
        self.max_interval = 0.0          # 相邻两条观测（同一 Perceptor 进程）的最长间隔，秒
        self.offsets = {}                # process_id -> 本机 monotonic ns − Perceptor monotonic ns 的最小值（见 _local_time）
        self.last_captured = 0
        self.max_delay = 0.0             # 观测从 Perceptor 采样到本机收到的最长延迟（相对最快那条），秒
        self.directory = None
        self._events_file = None
        self.supply_switch = None        # 开始时读到的 collection_enabled（只用于报错说明）

    def start(self):
        """在后台开始：丢积压、再起两个 BRPOP 线程。立刻返回——连不上 Redis 等问题由第一次取观测报出来。"""
        self.started = time.time()
        name = time.strftime('%Y%m%d-%H%M%S') + '-' + os.urandom(3).hex()
        self.directory = Path(self.config['session_root']) / name
        self.directory.mkdir(parents=True, exist_ok=True)
        self._events_file = (self.directory / 'action_events.jsonl').open('a', encoding='utf-8')
        starter = threading.Thread(target=self._start, daemon=True, name='redis-capture-start')
        starter.start()
        self.threads.append(starter)

    def _start(self):
        try:
            client = self.client_factory()
            try:
                for queue, label in ((self.config['observation_queue'], 'observation'), (self.config['action_queue'], 'action')):
                    backlog = client.call('LLEN', queue)
                    client.call_many([('RPOP', queue)] * backlog)     # 从旧的一端取走开始时已在队列里的条数
                    self.stats['drained_' + label] = backlog
                # 2026-11-05：供数开关由采集容器自己打开（NX：已经开着说明别的采集在用，不抢）
                if client.call('SET', SWITCH_KEY, '1', 'NX') is None:
                    self.supply_switch = client.call('GET', SWITCH_KEY)
                    raise SupplyBusy('供数开关 collection_enabled 已经是开着的（值 '
                                     f'{(self.supply_switch or b"").decode(errors="replace")!r}），可能有别的采集在用；'
                                     f'确认没有之后，在 Redis {self.config["host"]}:{self.config["port"]}/{self.config["db"]} '
                                     '执行 DEL collection_enabled 再开始')
                self.switch_owned, self.supply_switch = True, b'1'
                write_switch_owner(self.config)
            finally:
                client.close()
        except SupplyBusy as error:
            return self._fail(error)
        except Exception as error:
            return self._fail(ConnectionError(f'连不上 Redis {self.config["host"]}:{self.config["port"]}：{error}'))
        log(f'开始消费：丢掉积压 观测 {self.stats["drained_observation"]} 条、动作 {self.stats["drained_action"]} 条，'
            f'已打开供数开关 collection_enabled')
        for target in (self._observations, self._actions):
            if self.stop_event.is_set():
                return
            thread = threading.Thread(target=target, daemon=True, name='redis-capture-' + target.__name__.strip('_'))
            thread.start()
            self.threads.append(thread)

    def alive(self):
        return self.error is None and not self.stop_event.is_set()

    def stop(self):
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self._release_switch()           # 先关供数，Perceptor 和主臂节点就不再往队列里推
        for thread in self.threads:
            thread.join(3)
        with self.cond:
            self.cond.notify_all()
        try:
            self._events_file.close()
            (self.directory / 'actions_merged.json').write_text(
                json.dumps(list(self.actions.values()), ensure_ascii=False, indent=1), encoding='utf-8')
            (self.directory / 'summary.json').write_text(json.dumps(self.summary(), ensure_ascii=False, indent=1),
                                                         encoding='utf-8')
        except Exception:
            pass

    def _release_switch(self):
        """关掉本次采集打开的供数开关。Redis 连不上就留着 switch-owner.json，下次容器启动时再删。"""
        if not self.switch_owned:
            return
        try:
            client = self.client_factory()
            try:
                client.call('DEL', SWITCH_KEY)
            finally:
                client.close()
            self.switch_owned = False
            clear_switch_owner(self.config)
            log('已关闭供数开关 collection_enabled')
        except Exception as error:
            log(f'关闭供数开关失败（下次启动时再删）：{error}')

    def summary(self):
        span = (self.last_ts - self.first_ts) if self.first_ts is not None and self.last_ts is not None else 0
        return {'started_at': self.started, 'stopped_at': time.time(), 'stats': dict(self.stats),
                'observation_rate_hz': round((self.stats['observations'] - 1) / span, 2) if span > 0 else None,
                'max_interval_s': round(self.max_interval, 3), 'max_delivery_delay_s': round(self.max_delay, 3),
                'data_gap_count': self.gap_counts, 'actions': len(self.actions),
                'error': None if self.error is None else str(self.error), 'last_problem': self.last_problem}

    def next_observation(self, after_sequence, timeout):
        with self.cond:
            ready = self.cond.wait_for(lambda: self.error is not None or self.stop_event.is_set()
                                       or (self.latest is not None and self.latest.sequence > after_sequence), timeout)
            if self.error is not None:
                raise Fault('SOURCE_FAILED', f'Redis 消费出错：{self.error}')
            if self.stop_event.is_set():
                raise Fault('SOURCE_STOPPED', 'Redis 消费已停止')
            if not ready:
                if self.last_problem:
                    hint = f'；最近的问题：{self.last_problem}'
                elif self.latest is None and self.supply_switch != b'1':
                    hint = '：还没打开供数开关 collection_enabled（连 Redis 慢了？）'
                else:
                    hint = '（供数开关已打开，Perceptor 在出数吗？）'
                raise Fault('SOURCE_STALLED', f'{timeout:g} 秒没有收到新观测{hint}')
            return self.latest

    def _fail(self, error):
        with self.cond:
            self.error = error
            self.cond.notify_all()

    def _observations(self):
        try:
            client = self.client_factory()
        except Exception as error:
            return self._fail(error)
        try:
            while not self.stop_event.is_set():
                reply = client.call('BRPOP', self.config['observation_queue'], '1')
                if reply is None:
                    continue
                received = time.monotonic_ns()
                try:
                    message = json.loads(reply[1].decode('utf-8'))
                except ValueError:
                    self.stats['bad_json'] += 1
                    continue
                observation = self._convert(client, message, received)
                if observation is None:
                    continue
                with self.cond:
                    self.sequence += 1
                    observation.sequence = self.sequence
                    self.latest = observation
                    self.cond.notify_all()
                self._publish_video(observation)
        except Exception as error:
            if not self.stop_event.is_set():
                self._fail(error)
        finally:
            client.close()

    def _convert(self, client, message, received):
        if not isinstance(message, dict) or message.get('schema_version') != SCHEMA:
            self.stats['bad_schema'] += 1
            self.last_problem = '观测不是 capture/v5'
            return None
        units = self.config.get('units', 'rad')
        positions = to_slots(message.get('state'), self.config['joints'], units)
        if positions is None:
            self.stats['bad_state'] += 1
            self.last_problem = (f'观测的 state 里缺少有效的 {", ".join(self.config["joints"])}'
                                 + ('（要是有限数：五个关节 ±360°，夹爪 0~100）' if units == 'lerobot' else '（要是有限数且在 ±2π 以内）'))
            return None
        images, image_ns = {}, {}
        sources = message.get('sources') if isinstance(message.get('sources'), dict) else {}
        state_source = sources.get(self.config.get('state_source', 'arm_state'))
        state_sampled_at = state_source.get('sampled_at') if isinstance(state_source, dict) else None
        if type(state_sampled_at) not in (int, float) or not math.isfinite(state_sampled_at):
            state_sampled_at = None
        process_id = message.get('process_id')
        captured = self._local_time(process_id, message.get('timestamp'), received)
        for name, (height, width) in self.config['cameras'].items():
            key = (message.get('images') or {}).get(name)
            raw = client.call('GET', key) if isinstance(key, str) else None
            if raw is None:
                self.stats['image_missing'] += 1
                self.last_problem = f'相机 {name} 的图片取不到（没有 key 或已过期）'
                return None
            try:
                rgb = decode_image(raw)
            except Exception:
                self.stats['image_undecodable'] += 1
                self.last_problem = f'相机 {name} 的图片解不开'
                return None
            if rgb.shape != (height, width, 3):
                self.stats['camera_shape_mismatch'] += 1
                self.last_problem = f'相机 {name} 的图片是 {rgb.shape[0]}x{rgb.shape[1]}，和配置的 {height}x{width} 不一致'
                return None
            images[name] = (rgb, raw)
            # 图片比观测早采到多少，就把时间往前推多少（只用 Perceptor 一台机器上的时间差，不跨机器比时钟）
            lag = 0.0
            source = sources.get(name)
            if isinstance(source, dict) and type(source.get('sampled_at')) in (int, float) and type(message.get('timestamp')) in (int, float):
                lag = min(1.0, max(0.0, message['timestamp'] - source['sampled_at']))
            image_ns[name] = captured - int(lag * 1e9)
        gap = message.get('data_gap_count')
        if isinstance(process_id, str) and type(gap) is int:
            seen = self.gap_counts.setdefault(process_id, [gap, gap])
            seen[1] = gap
        if type(message.get('timestamp')) in (int, float):
            if self.first_ts is None:
                self.first_ts = message['timestamp']
            elif self.last_process == process_id:
                interval = message['timestamp'] - self.last_ts
                self.max_interval = max(self.max_interval, interval)
            self.last_ts = message['timestamp']
            self.last_process = process_id
        self.stats['observations'] += 1
        evidence = {'source': PROVENANCE, 'mapping': MAPPING_RULE, 'process_id': process_id,
                    'observation_id': message.get('observation_id'), 'perceptor_timestamp': message.get('timestamp'),
                    'received_monotonic_ns': received, 'captured_monotonic_ns': captured,
                    'clock': 'perceptor_timestamp + min(received - timestamp) per process',
                    'config_hash': message.get('config_hash'), 'data_gap_count': gap,
                    'action_id': message.get('action_id'), 'association_quality': message.get('association_quality'),
                    'image_keys': {name: (message.get('images') or {}).get(name) for name in images},
                    'raw_state': {k: message['state'][k] for k in self.config['joints']},
                    'state_sampled_at': state_sampled_at,   # 关节状态的采样时刻（Perceptor 的时钟），teleop 按它配主臂 action
                    # Preserve raw per-camera identity and clock evidence; no new timestamp gates.
                    'source_clock': {'time_basis': message.get('time_basis'), 'clock_anchor': message.get('clock_anchor')},
                    'frame_sources': {name: {**(sources.get(name) if isinstance(sources.get(name),dict) else {}),
                        'kind':'redis_capture','process_id':process_id,'observation_id':message.get('observation_id'),
                        'image_key':(message.get('images') or {}).get(name)} for name in images}}
        return Observation(sequence=0, received_ns=received, captured_ns=captured, positions=positions, images=images,
                           image_ns=image_ns, evidence=evidence)

    def _local_time(self, process_id, timestamp, received):
        """Perceptor 采样时刻换到本机 monotonic 时间。

        Perceptor 的时间是它那台机器上的进程 monotonic 秒，不能直接比。两台机器的 monotonic 只差一个常数，
        送达延迟有快有慢：取"本机收到 − Perceptor 采样"的最小值当常数（同一 Perceptor 进程内），
        这样送达晚到、一起到的观测仍然按真实采样间隔打时间。结果不晚于本机收到的时刻，并且严格递增。
        """
        if type(timestamp) not in (int, float) or not math.isfinite(timestamp) or not isinstance(process_id, str):
            captured = received
        else:
            stamp = int(timestamp * 1e9)
            offset = min(self.offsets.get(process_id, received - stamp), received - stamp)
            self.offsets[process_id] = offset
            captured = stamp + offset
            self.max_delay = max(self.max_delay, (received - captured) / 1e9)
        captured = min(received, max(captured, self.last_captured + 1))
        self.last_captured = captured
        return captured

    def _publish_video(self, observation):
        if self.frame_store is None:
            return
        for name, (rgb, raw) in observation.images.items():
            try:
                jpeg = raw if raw[:2] == b'\xff\xd8' else encode(rgb, 'JPEG')
                self.frame_store.write(name, rgb, jpeg, captured_monotonic_ns=observation.image_ns[name],
                                       captured_unix_ns=time.time_ns(), sequence=observation.sequence)
            except Exception:
                self.stats['video_write_failed'] += 1

    def _actions(self):
        try:
            client = self.client_factory()
        except Exception as error:
            return self._fail(error)
        fields = ('type', 'parameters', 'request_context', 'task_id', 'trace_id', 'request_origin', 'send_status',
                  'input_observation_id', 't_request', 't_send', 't_received', 't_dispatch', 't_accepted', 't_return',
                  't_end', 'motion_result', 'completion_evidence')
        try:
            while not self.stop_event.is_set():
                reply = client.call('BRPOP', self.config['action_queue'], '1')
                if reply is None:
                    continue
                try:
                    event = json.loads(reply[1].decode('utf-8'))
                except ValueError:
                    self.stats['bad_action_json'] += 1
                    continue
                self._events_file.write(json.dumps(event, ensure_ascii=False) + '\n')
                self._events_file.flush()
                if isinstance(event, dict) and event.get('schema_version') == TELEOP_SCHEMA:
                    if event.get('source') == LEADER_SOURCE and event.get('input_observation_id') is None:
                        self._keep_leader(event)      # 主臂节点的 action：按时间留着，等 sample 按时间配对
                    else:
                        self._keep_teleop(event)      # 每帧成对的实际下发目标：留着等 sample 配对，不进下面的指令合并
                    continue
                event_id, action_id, process_id = event.get('event_id'), event.get('action_id'), event.get('process_id')
                if not all(isinstance(x, str) for x in (event_id, action_id, process_id)):
                    self.stats['bad_action_event'] += 1
                    continue
                if event_id in self.seen_events:
                    self.stats['duplicate_action_event'] += 1
                    continue
                self.seen_events.add(event_id)
                self.stats['action_events'] += 1
                record = self.actions.setdefault(process_id + '/' + action_id,
                                                 {'process_id': process_id, 'action_id': action_id, 'events': []})
                record['events'].append(event.get('event'))
                for field in fields:                  # 后到的事件带到该阶段为止的累计字段，用最新值
                    if field in event:
                        record[field] = event[field]
        except Exception as error:
            if not self.stop_event.is_set():
                self._fail(error)
        finally:
            client.close()

    def _keep_teleop(self, event):
        """teleop/action/v1：检查必需字段，按 (process_id, input_observation_id) 记下来；同一个观测只认第一条。"""
        keys = ('process_id', 'action_id', 'input_observation_id')
        if not all(isinstance(event.get(k), str) and event[k] for k in keys) \
                or not isinstance(event.get('positions'), dict) or type(event.get('timestamp')) not in (int, float) \
                or ('requested_positions' in event and event['requested_positions'] is not None
                    and not isinstance(event['requested_positions'], dict)):
            self.stats['bad_teleop_action'] += 1
            self.last_problem = '动作队列里有缺字段的 teleop/action/v1 消息'
            return
        key = (event['process_id'], event['input_observation_id'])
        with self.cond:
            if key in self.teleop:
                self.stats['duplicate_teleop_action'] += 1
                return
            self.teleop[key] = event
            while len(self.teleop) > PAIRING_CAPACITY:
                self.teleop.popitem(last=False)
                self.stats['teleop_action_dropped'] += 1
            self.stats['teleop_actions'] += 1
            self.cond.notify_all()

    def paired_action(self, process_id, observation_id, timeout):
        """等这条观测的成对目标，最多 timeout 秒；拿到后从表里取走。没有就返回 None。"""
        key = (process_id, observation_id)
        with self.cond:
            self.cond.wait_for(lambda: key in self.teleop or self.stop_event.is_set() or self.error is not None, timeout)
            return self.teleop.pop(key, None)

    def _keep_leader(self, event):
        """主臂节点的 action（teleop/action/v1，source=teleop_leader）：检查字段，按 timestamp 排好留着。"""
        stamp = event.get('timestamp')
        if (type(stamp) not in (int, float) or not math.isfinite(stamp) or not isinstance(event.get('positions'), dict)
                or not all(isinstance(event.get(k), str) and event[k] for k in ('process_id', 'action_id'))):
            self.stats['bad_leader_action'] += 1
            self.last_problem = '动作队列里有缺字段的主臂 action（teleop/action/v1）'
            return
        expected = self.config.get('robot_type')
        if self.config.get('units') == 'lerobot' and event.get('robot_type') not in (None, expected):
            self.stats['leader_robot_type_mismatch'] += 1
            self.last_problem = f'主臂 action 的 robot_type 是 {event.get("robot_type")!r}，这台采集容器采的是 {expected!r}'
            return
        with self.cond:
            index = bisect.bisect_right(self.leader_ts, stamp)
            self.leader_ts.insert(index, stamp)
            self.leader.insert(index, event)
            if len(self.leader) > LEADER_CAPACITY:
                del self.leader[0], self.leader_ts[0]
                self.stats['leader_action_dropped'] += 1
            self.stats['leader_actions'] += 1
            self.cond.notify_all()

    def leader_action_after(self, stamp, timeout):
        """第一条 timestamp ≥ stamp 的主臂 action，最多等 timeout 秒；没有返回 None。比 stamp 早 LEADER_KEEP_S 以上的顺手丢掉。"""
        def first():
            index = bisect.bisect_left(self.leader_ts, stamp)
            return self.leader[index] if index < len(self.leader) else None
        with self.cond:
            self.cond.wait_for(lambda: first() is not None or self.stop_event.is_set() or self.error is not None, timeout)
            event = first()
            old = bisect.bisect_left(self.leader_ts, stamp - LEADER_KEEP_S)
            if old:
                del self.leader[:old], self.leader_ts[:old]
            return event


# ══════════ 4. 设备进程适配器：和 IsaacAdapter / DirectFollowerAdapter 同一个接口 ══════════

class RedisCaptureAdapter:
    kind = 'physical'
    source = PROVENANCE

    def __init__(self, config, *, client_factory=None, frame_store=None):
        self.config = config
        self.client_factory = client_factory or client_factory_of(config)
        if frame_store is None and config.get('camera_store'):
            from .camera_service import FrameStore
            frame_store = FrameStore(config['camera_store'])
        self.frame_store = frame_store
        self.consumer = None
        self.returned = 0
        self.session_id = None
        self.closing = False
        self.after_hold = False         # hold 之后、下一次 send 之前：SDK 在确认停止
        self.lock = threading.RLock()

    def describe(self):
        return {'kind': self.kind, 'source': self.source, 'robot_id': self.config['robot_id'],
                'calibration_hash': self.config['calibration_hash'], 'capabilities': ['collection', 'teleop'],
                'commissioned': True}

    def queue_status(self):
        """本适配器从不给机器人下发动作，没有在排队的动作。"""
        return {'action_queue_empty': True, 'session_id': self.session_id or 'not-connected',
                'device_id': f'redis:{self.config["host"]}:{self.config["port"]}/{self.config["db"]}',
                'profile_id': self.config['calibration_hash'], 'sampled_at': time.time()}

    # —— 开始采集：在后台开始消费（先丢积压），立刻返回
    def connect(self):
        """不在这里等数据、也不在这里报错：connect 失败时采集接口拿不到设备会话的 epoch，清理会失败，批次会卡住
        （2026-10-29 实测：供数关着时开始采集，批次一直停在 FINISHING，只能放弃）。没有数据由第一次 sample 报出来，
        这时会话已经建立，失败后正常关闭。"""
        with self.lock:
            if self.consumer is None or not self.consumer.alive():
                self._stop_consumer()
                consumer = Consumer(self.config, self.client_factory, frame_store=self.frame_store)
                consumer.start()
                self.consumer, self.returned = consumer, 0
            self.session_id = os.urandom(8).hex()
            self.after_hold = False

    def heartbeat(self):
        pass

    def _require(self):
        consumer = self.consumer
        if consumer is None:
            raise Fault('NOT_CONNECTED', '还没有开始采集')
        return consumer

    # —— 读：等一条比上次新的观测，用本机收到它的时刻打时间（同一条观测不会返回两次）
    def sample(self):
        """录制中：没有新观测就报供数中断，从不把旧观测当新的。

        例外是停止确认：SDK 停止（保存一条、结束采集）时要在 hold 前后各拿一次新读数。平台可能先关供数再结束采集，
        这时等 stop_wait_s 还没有新观测，就把最后一条观测换上本机当前时间交给 SDK 完成停止，并在来源信息里标明
        restamped_after_hold。录制恢复（下一次 send）后不再这样做。
        """
        consumer = self._require()
        restamped = False
        started = time.monotonic()
        if self.returned == 0:
            wait = self.config['connect_wait_s']           # 开始采集后的第一条：含丢积压的时间
        else:
            wait = self.config['stop_wait_s'] if self.after_hold else self.config['sample_wait_s']
        try:
            observation = consumer.next_observation(self.returned, wait)
        except Fault as error:
            if not self.after_hold or error.code != 'SOURCE_STALLED' or consumer.latest is None:
                log(f'sample 失败（等了 {time.monotonic() - started:.2f}s，after_hold={self.after_hold}）：{error}')
                raise
            observation, restamped = consumer.latest, True
            consumer.stats['restamped_after_hold'] += 1
            log(f'sample 停止确认用最后一条观测（等了 {time.monotonic() - started:.2f}s 没有新观测）')
        waited = time.monotonic() - started
        if waited > 0.6 and not restamped:
            log(f'sample 等了 {waited:.2f}s 才有新观测（after_hold={self.after_hold}）')
        self.returned = observation.sequence
        shift = time.monotonic_ns() - observation.captured_ns if restamped else 0
        images = {name: {'png': base64.b64encode(encode(rgb, 'PNG')).decode(),
                         'captured_monotonic_ns': observation.image_ns[name] + shift,
                         'source_frame': {**observation.evidence['frame_sources'][name],
                            'source_clock':observation.evidence['source_clock']}}
                  for name, (rgb, _) in observation.images.items()}
        evidence = {**observation.evidence, 'restamped_after_hold': True} if restamped else observation.evidence
        result = {'positions': dict(observation.positions), 'captured_monotonic_ns': observation.captured_ns + shift,
                  'images': images, 'clock_mapping': evidence}
        if self.config.get('action') == 'paired' and not restamped:
            paired = self._paired(consumer, observation)
            if paired is not None:
                result['source_action'] = paired      # SDK 用它写这一帧的 action；没有就跳过这一帧
        elif self.config.get('action') == 'teleop' and not restamped:
            leader = self._leader(consumer, observation)
            if leader is not None:
                result['source_action'] = leader      # 主臂指令（按时间配对）；没有就跳过这一帧
        return result

    def _leader(self, consumer, observation):
        """teleop：这一帧观测配“状态采样之后的第一条主臂 action”，换成 SDK 的 6 个槽位。没配上或不合法返回 None。
        两边的时间都是同一台主机的 CLOCK_MONOTONIC 秒（Perceptor 的 sampled_at、主臂节点的 timestamp），直接比较。"""
        evidence = observation.evidence
        stamp = evidence.get('state_sampled_at')
        if type(stamp) not in (int, float):
            stamp = evidence.get('perceptor_timestamp')            # 观测里没有状态源的 sampled_at：退一步用观测的时间
        if type(stamp) not in (int, float):
            consumer.stats['action_unpairable_observation'] += 1
            consumer.last_problem = '观测没有状态采样时刻，配不了主臂 action'
            return None
        event = consumer.leader_action_after(stamp, self.config['action_wait_s'])
        if event is None:
            consumer.stats['action_missing'] += 1
            if not consumer.stats['leader_robot_type_mismatch']:      # 收到的主臂 action 都是别的机器人的：保留那条更具体的说明
                consumer.last_problem = ('没有收到主臂 action（主臂节点 so101-leader 在运行吗？）' if not consumer.stats['leader_actions']
                                         else '主臂 action 断了：观测之后没有新的主臂 action')
            return None
        delay = event['timestamp'] - stamp
        if delay > self.config['max_action_skew_s']:
            consumer.stats['action_too_late'] += 1
            consumer.last_problem = f'主臂 action 比观测晚 {delay:.3f} 秒，超过 {self.config["max_action_skew_s"]} 秒'
            return None
        positions = to_slots(event['positions'], self.config['joints'], self.config.get('units', 'rad'))
        if positions is None:
            consumer.stats['action_invalid'] += 1
            consumer.last_problem = '主臂 action 的关节值不对（关节名、单位要和观测的 state 相同）'
            return None
        consumer.stats['action_paired'] += 1
        return {'positions': positions, 'requested_positions': positions, 'action_id': event['action_id'],
                'process_id': event['process_id'], 'seq': event.get('seq'), 'leader_serial': event.get('leader_serial'),
                'action_timestamp': event['timestamp'], 'state_sampled_at': stamp, 'delay_s': round(delay, 4)}

    def _paired(self, consumer, observation):
        """按观测编号取它的成对目标（teleop/action/v1），换成 SDK 的 6 个槽位。没配上或不合法返回 None。"""
        evidence = observation.evidence
        process_id, observation_id = evidence.get('process_id'), evidence.get('observation_id')
        if not isinstance(process_id, str) or not isinstance(observation_id, str):
            consumer.stats['action_unpairable_observation'] += 1
            consumer.last_problem = '观测没有 process_id / observation_id，配不了 action'
            return None
        event = consumer.paired_action(process_id, observation_id, self.config['action_wait_s'])
        if event is None:
            consumer.stats['action_missing'] += 1
            return None
        units = self.config.get('units', 'rad')
        positions = to_slots(event['positions'], self.config['joints'], units)
        requested = to_slots(event['requested_positions'], self.config['joints'], units) \
            if event.get('requested_positions') else positions
        stamp = evidence.get('perceptor_timestamp')
        delay = event['timestamp'] - stamp if type(stamp) in (int, float) else None
        if positions is None or requested is None or (delay is not None and delay < 0):
            consumer.stats['action_invalid'] += 1
            consumer.last_problem = ('成对目标的关节值不对（关节名、单位要和观测的 state 相同）' if positions is None or requested is None
                                     else '成对目标的下发时刻早于它的观测（应该先观测、再下发）')
            return None
        consumer.stats['action_paired'] += 1
        return {'positions': positions, 'requested_positions': requested, 'action_id': event['action_id'],
                'observation_id': observation_id, 'process_id': process_id,
                'delay_s': None if delay is None else round(delay, 4)}

    def leader(self):
        """"主臂"目标的占位：当前状态（采集接口实际用的是 CurrentStateLeader，不走这里）。数据集的 action 见 dataset_schema_of。"""
        latest = self._require().latest
        if latest is None:
            raise Fault('NOT_CONNECTED', '还没有收到观测')
        return {'positions': dict(latest.positions)}

    # —— 写：不下发任何东西，原样回执
    def send(self, positions):
        if not isinstance(positions, dict) or set(positions) != {n + '.pos' for n in NAMES} or any(
                type(v) not in (int, float) or not math.isfinite(v) for v in positions.values()):
            raise Fault('INVALID_TARGET', '需要 6 个有限的关节值')
        if self.after_hold:
            log('send：录制恢复')
        self.after_hold = False
        return dict(positions)

    def preempt_stop(self, *, close):
        self.closing = close

    def hold(self):
        """停止：不下发任何东西（机器人不归采集容器控制）。结束会话时停止消费。"""
        with self.lock:
            log(f'hold（结束会话={self.closing}）')
            self.after_hold = True
            if self.closing:
                self.closing = False
                self._stop_consumer()
        return {'holding': True, 'action_queue_empty': True, 'controlled': False}

    def close(self):
        with self.lock:
            self._stop_consumer()

    def _stop_consumer(self):
        if self.consumer is not None:
            self.consumer.stop()
            log(f'停止消费：{json.dumps(self.consumer.summary()["stats"], ensure_ascii=False)}')
            self.consumer = None

    def reset(self):
        raise Fault('WRONG_TARGET', '真实机器人数据源没有"重置场景"')

    def calibration(self, *a):
        raise Fault('WRONG_TARGET', 'Redis 数据源不做标定')

    def primitive(self, *a):
        raise Fault('UNSUPPORTED', 'Redis 数据源只采集，不控制机器人')


class CurrentStateLeader:
    """采集接口（API 进程）一侧的"主臂"占位：设备会话最近一次实测状态，不发设备请求，也不下发任何东西。
    它只是让 SDK 的逐帧流程走得下去；数据集的 action 按 dataset_schema 的规则另写（v10r13 起是下一帧的实测状态）。

    sessions 对 Redis 目标用它代替 RemoteLeader：RemoteLeader 每帧都要发一次 leader 请求，和一直在等新观测的
    sample 请求抢同一把锁，2026-10-29 实测录制只有约 2.4 帧/秒（标称 4）。
    """
    is_connected = True
    resource_id = 'redis-capture:measured-state'

    def __init__(self, session):
        self.session = session

    def get_action(self, *, timeout_s):
        from lerobot_sdk import JointTarget
        arm = self.session.get_arm_data()
        return JointTarget(dict(arm.positions), arm.units, arm.calibration_hash)


# ══════════ 5. 部署：登记目标 → 写设备进程配置和采集目标绑定 ══════════

def load_config(path=CONFIG_PATH):
    """/config/redis-capture.json（宿主机 apply-env.py 根据 .env 生成）。没有这个文件或 enabled 不是 true 就不启用。"""
    path = Path(path)
    if not path.exists():
        return None
    raw = json.loads(path.read_text(encoding='utf-8'))
    if raw.get('enabled') is not True:
        return None
    config = {**DEFAULTS, **{k: v for k, v in raw.items() if not k.startswith('_')}}
    if not isinstance(config.get('host'), str) or not config['host']:
        raise ValueError('redis-capture.json 缺少 host')
    if config['units'] not in UNITS:
        raise ValueError('redis-capture.json 的 units 只能是 ' + ' 或 '.join(UNITS))
    if config['units'] == 'lerobot':
        # SO101：robot_type 是 Perceptor / 主臂节点用的机器人型号（so-101-follower_sim 等），只用来核对主臂 action；
        # 数据集是标准 SO101 格式（so101_follower）
        if not isinstance(config['robot_type'], str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}', config['robot_type']):
            raise ValueError('redis-capture.json 的 robot_type 只能用小写字母、数字、- 和 _')
    elif not isinstance(config['robot_type'], str) or not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', config['robot_type']) \
            or config['robot_type'] == 'so101_follower':
        raise ValueError('redis-capture.json 的 robot_type 只能用小写字母、数字和下划线，且不能是 so101_follower')
    if not isinstance(config['state_source'], str) or not config['state_source']:
        raise ValueError('redis-capture.json 的 state_source 不能为空')
    if type(config['max_action_skew_s']) not in (int, float) or not 0.01 <= config['max_action_skew_s'] <= 1.0:
        raise ValueError('redis-capture.json 的 max_action_skew_s 要在 0.01~1 秒之间')
    if config.get('camera_store') is not None and not isinstance(config['camera_store'], str):
        raise ValueError('redis-capture.json 的 camera_store 要么不写（null），要么是目录')
    for key in ('port', 'db', 'fps'):
        if type(config[key]) is not int or config[key] < 0:
            raise ValueError(f'redis-capture.json 的 {key} 必须是非负整数')
    if not 1 <= config['fps'] <= 10:
        raise ValueError('redis-capture.json 的 fps 要在 1~10 之间')
    for key in ('connect_wait_s', 'sample_wait_s', 'stop_wait_s'):
        if type(config[key]) not in (int, float) or not 0.1 <= config[key] <= 1.5:
            raise ValueError(f'redis-capture.json 的 {key} 要在 0.1~1.5 秒之间（设备进程租约只有 2 秒）')
    if config['action'] not in ACTION_MODES:
        raise ValueError('redis-capture.json 的 action 只能是 ' + ' 或 '.join(ACTION_MODES))
    if type(config['action_wait_s']) not in (int, float) or not 0.05 <= config['action_wait_s'] <= 0.5 \
            or config['sample_wait_s'] + config['action_wait_s'] > 1.8:
        raise ValueError('redis-capture.json 的 action_wait_s 要在 0.05~0.5 秒之间，并且和 sample_wait_s 加起来不超过 1.8 秒（租约 2 秒）')
    joints = config['joints']
    if not isinstance(joints, list) or len(joints) != 6 or not all(isinstance(j, str) and j for j in joints) or len(set(joints)) != 6:
        raise ValueError('redis-capture.json 的 joints 必须是 6 个不重复的关节名')
    cameras = config['cameras']
    if not isinstance(cameras, dict) or not cameras:
        raise ValueError('redis-capture.json 至少要配一个相机')
    for name, shape in cameras.items():
        if not isinstance(name, str) or not name.isidentifier() or not name.islower():
            raise ValueError(f'相机名字 {name!r} 只能是小写字母、数字和下划线')
        if not isinstance(shape, list) or len(shape) != 2 or not all(type(v) is int and v > 0 for v in shape):
            raise ValueError(f'相机 {name} 的尺寸要写成 [高, 宽]')
    if config.get('password_file') and not Path(config['password_file']).is_absolute():
        config['password_file'] = str(path.parent / config['password_file'])
    return config


def build_registration(config, ros_base):
    """同样的配置总是得到同样的 revision。"""
    names = [n + '.pos' for n in NAMES]
    cameras = {name: config['cameras'][name] for name in sorted(config['cameras'])}
    dataset_schema = dataset_schema_of(config)
    profile = {'schema': PROFILE_SCHEMA, 'rule': MAPPING_RULE, 'robot_id': config['robot_id'],
               'joints': config['joints'], 'cameras': cameras, 'dataset_schema': dataset_schema}
    if config['units'] != 'rad':                           # Bobac3（rad）的登记内容和以前一样，revision 不变
        profile['units'] = config['units']
    calibration_hash = canonical_hash(profile)
    limits = {n: [-360.0, 360.0] for n in names[:5]}
    limits['gripper.pos'] = [0.0, 100.0]
    sdk_config = {
        'robot_id': config['robot_id'], 'calibration_hash': calibration_hash,
        'units': {n: ('normalized_0_100' if n == 'gripper.pos' else 'deg') for n in names},
        'limits': limits,
        'max_step': {n: 360.0 for n in names},          # 不限步长：action 是占位，什么也不下发
        'max_velocity': {n: 1e6 for n in names},
        'tolerance': {n: 1.0 for n in names}, 'gripper_open': 80.0, 'gripper_closed': 20.0, 'release_pose': None,
        'fps': 10, 'stable_frames': 2, 'action_timeout_s': 45, 'io_timeout_s': config['io_timeout_s'],
        # 时间按 Perceptor 采样时刻打，送达晚到的观测"年龄"会大一些：放宽到 3 秒（等待最多 1.4 秒 + 送达延迟）
        'max_observation_age_s': 3.0, 'max_sensor_skew_s': 1.0,
        'required_cameras': list(cameras), 'event_capacity': 64, 'close_policy': 'hold'}
    ros = {k: ros_base[k] for k in ('domain_id', 'key_file', 'dds_config_uri', 'dds_config_sha256',
                                    'rmw_implementation') if k in ros_base}
    ros['namespace'] = config['namespace']
    body = {'robot_id': config['robot_id'], 'kind': 'physical', 'provenance': PROVENANCE, 'commissioned': True,
            'capabilities': ['collection', 'teleop'], 'calibration_revision': 'baseline',
            'camera_shapes': {name: [h, w, 3] for name, (h, w) in cameras.items()},
            'require_runtime_provenance': False, 'fps': config['fps'], 'redis_capture_profile_hash': calibration_hash,
            'dataset_schema': dataset_schema,           # 数据集的关节和 action 规则（sessions 传给 CollectionConfig）
            'sdk_config': sdk_config, 'ros': ros}
    revision = 'redis-' + canonical_hash(body)[7:19]
    adapter = {key: config[key] for key in ('robot_id', 'robot_type', 'units', 'host', 'port', 'db',
                                            'observation_queue', 'action_queue', 'joints', 'state_source',
                                            'connect_wait_s', 'sample_wait_s', 'stop_wait_s', 'camera_store',
                                            'session_root', 'action', 'action_wait_s', 'max_action_skew_s')}
    adapter.update(calibration_hash=calibration_hash, cameras=cameras, password_file=config.get('password_file'))
    # ROS JointState 里夹爪槽位换回角度：槽位 0~100 对应 -360°~360°，正好还原第 6 个关节的原始角度
    peer = {**ros, 'gripper_angle_deg': [-360.0, 360.0], 'diagnostic_timing': True, 'adapter': adapter}
    return body, revision, peer


def prepare(*, data_root='/data', defaults_targets='/opt/app/collection-project-targets.json',
            config_path=CONFIG_PATH, store=None, project='lab'):
    """容器启动时调用（supervisor 里，API 启动之前）：登记目标（同样配置只登记一次），写设备进程配置和采集目标绑定。"""
    from .ros_contract import complete
    from .store import Principal, Store
    from lerobot_sdk import RobotConfig

    runtime = Path(data_root) / 'isaac-console/redis-capture'
    targets_out = runtime / 'collection-project-targets.json'
    peer_out = runtime / 'peer.json'
    config = load_config(config_path)
    if config is None:
        for stale in (targets_out, peer_out):            # 关掉以后，下次启动不再加载
            stale.unlink(missing_ok=True)
        return {'enabled': False}
    runtime.mkdir(parents=True, exist_ok=True)
    stale_switch = release_stale_switch({**config, 'switch_owner': str(runtime / 'switch-owner.json')})
    isaac_peer = json.loads((Path(data_root) / 'isaac-console/peer.json').read_text(encoding='utf-8'))
    body, revision, peer = build_registration(config, isaac_peer)
    RobotConfig(**body['sdk_config'])                     # 与 Platform.register 相同的检查
    from lerobot_sdk.data.contracts import joint_schema   # 需要挂进容器的 SDK 覆盖文件（支持 dataset_schema）
    joint_schema(body['dataset_schema'])
    body = {**body, 'ros': complete(body['ros'], tuple(body['camera_shapes']))}
    peer = {**peer, **body['ros']}
    own_store = store is None
    store = store or Store(Path(data_root) / 'registry.sqlite')
    try:
        actor = Principal('operator', (project,), ('admin', 'operator'))
        try:
            store.get('robots', body['robot_id'], revision, actor)
            registered = 'existing'
        except Fault:
            store.register('robots', body['robot_id'], revision, project, actor, body)
            registered = 'new'
    finally:
        if own_store:
            store.close()
    # 一台容器只采一台机器人（.env 的 COLLECTION_ROBOT），绑定里只有这个 Redis 数据源
    mode = 'physical_teleop'
    targets = json.loads(Path(defaults_targets).read_text(encoding='utf-8'))
    targets[project] = {'default_mode': mode, mode: {'robot_id': body['robot_id'], 'robot_revision': revision}}
    peer['adapter']['switch_owner'] = str(runtime / 'switch-owner.json')
    _write_json(peer_out, peer, mode=0o600)
    _write_json(targets_out, targets)
    return {'enabled': True, 'registered': registered, 'robot_id': body['robot_id'], 'revision': revision,
            'redis': f'{config["host"]}:{config["port"]}/{config["db"]}', 'mode': mode, 'stale_switch': stale_switch,
            'peer_config': str(peer_out), 'targets': str(targets_out)}


def _write_json(path, value, mode=0o644):
    temporary = Path(path).with_name('.' + Path(path).name + '.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.chmod(mode)
    temporary.replace(path)


# ══════════ 6. 命令行 ══════════

def peek(config=None):
    """只读：开关、两个队列的长度、最新一条观测的概要。用 LRANGE / LINDEX，不取走任何数据。"""
    config = config or load_config()
    if config is None:
        print('没有启用 Redis 数据源（/config/redis-capture.json 不存在或 enabled 不是 true）')
        return 2
    client = client_factory_of(config)()
    try:
        enabled = client.call('GET', 'collection_enabled')
        print(f'Redis {config["host"]}:{config["port"]}/{config["db"]}  collection_enabled={enabled.decode() if enabled else "(未设置)"}')
        for queue in (config['observation_queue'], config['action_queue']):
            print(f'  {queue}: {client.call("LLEN", queue)} 条')
        newest = client.call('LINDEX', config['observation_queue'], '0')
        if newest:
            message = json.loads(newest.decode('utf-8'))
            print('  最新观测：', json.dumps({k: message.get(k) for k in ('observation_id', 'timestamp', 'data_gap_count', 'state', 'images')},
                                          ensure_ascii=False))
    finally:
        client.close()
    return 0


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='真实机器人 Redis 采集队列（capture/v5）数据源')
    parser.add_argument('command', choices=('prepare', 'serve', 'peek'))
    parser.add_argument('--config', help='serve：设备进程配置（peer.json）')
    args = parser.parse_args(argv)
    if args.command == 'prepare':
        try:
            print(json.dumps(prepare(), ensure_ascii=False), flush=True)
        except Exception as error:      # 准备失败不影响仿真采集照常启动
            print(json.dumps({'enabled': True, 'registered': False, 'reason': str(error)}, ensure_ascii=False), flush=True)
        return 0
    if args.command == 'peek':
        return peek()
    if not args.config:
        parser.error('serve 需要 --config')
    from .ros_transport import serve_peer
    config = json.loads(Path(args.config).read_text(encoding='utf-8'))
    serve_peer(RedisCaptureAdapter(config['adapter']), config)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
