"""SO101 主臂节点：用飞特 SDK 只读主臂 6 个舵机，每读一次就打包成一条 action，发到 ROS 话题 /so101/action；
供数开关开着时，把同一条 action 原样推进 Redis 的 collection_action_queue。

2026-11-04 v2：消息改成和 Bobac3 动作队列同一种格式（teleop/action/v1），发 ROS 之前就打包好，
ROS 上的和 Redis 里的是同一个 JSON 字符串。从臂节点（遥控器）订阅 /so101/action 跟随；
采集容器只从 Redis 取。方案见《SO101遥操作采集链路方案-20261104》。

主臂（Seeed SO101，飞特 STS3215 舵机，ID 1~6）插在本机 USB 上，按控制板的 USB 序列号找它：
/dev/serial/by-id/usb-1a86_USB_Single_Serial_<序列号>-if00（ttyACM 编号会变，序列号不会）。
本节点只读不写：从不写任何舵机寄存器，也不改主臂的通电状态。

每读一轮 6 个舵机的当前位置（寄存器 56），发一条 std_msgs/String，内容是 JSON：
  {
    "schema_version": "teleop/action/v1",
    "robot_type": "so-101-follower_sim",   和 Perceptor 观测 profile 的 robot_type 相同（真机 so-101-follower）
    "process_id": "so101-leader:<会话>",    本次进程启动时生成
    "action_id": "<会话>:000123",          全局唯一，消费方按它去重
    "seq": 123,                            每读一轮加 1（重连不清零，进程重启从 1 开始）
    "source": "teleop_leader",
    "leader_serial": "5A7C124026",         主臂控制板的 USB 序列号
    "time_basis": "host_monotonic",
    "timestamp": 1384898.025301,           这一轮读数的中间时刻，主机 CLOCK_MONOTONIC 秒；
                                           和 Perceptor 观测的 timestamp、sources.*.sampled_at 是同一个时钟
    "sampled_unix": 1793780012.345678,     同一时刻的 Unix 时间（秒），跨主机时对时用
    "input_observation_id": null,          遥操作的 action 不是根据观测算的；消费方按时间配对
    "positions": {"shoulder_pan": 度, …, "gripper": 0~100},   关节名、单位和 Perceptor 观测的 state 相同
    "units": {"shoulder_pan": "deg", …, "gripper": "normalized_0_100"},
    "raw": {"shoulder_pan": 舵机刻度 0~4095, …},
    "rate_hz": 30
  }
舵机里的限位（寄存器 9 / 11）每次连上主臂时读一遍，写在日志里（leader_connected）。

换算和 LeRobot 相同：五个臂关节 = (读数 - 限位中点) × 360 / 4095 度；夹爪 = 读数（先夹进限位）在限位行程里的百分比。

读失败（拔线、没上电、丢帧）时这一轮不发消息、进程也不退出：关串口，1 秒后重连。
Redis 连不上时只跳过推送（日志记一次），ROS 照常发。

命令行：
  python3 leader_node.py run   --serial 5A7C124026 [--rate 30] [--topic /so101/action] [--robot-type so-101-follower_sim]
                               [--redis-host 192.168.0.215 --redis-port 6379 --redis-db 0] [--host-dev /hostdev]
  python3 leader_node.py check --serial 5A7C124026 [--host-dev /hostdev]     只读一轮并打印，不发 ROS、不推 Redis
"""
import argparse
import json
import os
import signal
import socket
import threading
import time
import uuid
from pathlib import Path

SCHEMA = 'teleop/action/v1'
NAMES = ('shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll', 'gripper')
UNITS = {name: ('normalized_0_100' if name == 'gripper' else 'deg') for name in NAMES}
DEFAULT_ROBOT_TYPE = 'so-101-follower_sim'
ACTION_QUEUE = 'collection_action_queue'
SWITCH_KEY = 'collection_enabled'          # Redis 里的供数开关：值为字符串 1 时开，删掉就关（和 Perceptor 同一个）
SWITCH_CHECK_S = 0.5                       # 多久看一次开关
QUEUE_MAX = 1000                           # 队列最多留这么多条（和 Perceptor profile 的 queue_max_messages 相同）

# 飞特 STS3215 寄存器：(地址, 字节数)
MIN_POSITION_LIMIT = (9, 2)
MAX_POSITION_LIMIT = (11, 2)
PRESENT_POSITION = (56, 2)
SERVO_MODEL = 777                  # STS3215 的型号编号（ping 返回）
BAUDRATE = 1_000_000
READ_TRIES = 3                     # CH34x 偶尔丢帧，单个寄存器最多读 3 次
RETRY_S = 1.0                      # 读失败后等多久再重连
DEFAULT_RATE_HZ = 30.0
DEFAULT_TOPIC = '/so101/action'
DEFAULT_HOST_DEV = '/hostdev'      # 容器里把宿主机 /dev 只读挂在这里（拔插后 ttyACM 编号变了也能找到）


class LeaderError(Exception):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def serial_path(host_dev, serial):
    return str(Path(host_dev) / 'serial/by-id' / f'usb-1a86_USB_Single_Serial_{serial}-if00')


def to_units(name, ticks, lo, hi):
    """舵机刻度 → LeRobot 单位。公式和采集容器 direct_follower.to_units 相同。"""
    if name == 'gripper':
        return (min(hi, max(lo, ticks)) - lo) / (hi - lo) * 100
    return (ticks - (lo + hi) / 2) * 360 / 4095


def _monotonic_ns():
    return time.monotonic_ns()


# ══════════ 1. 主臂总线：只读 ══════════

class LeaderBus:
    """一块控制板上的 6 个舵机。只有读操作，没有任何写寄存器的方法。"""

    def __init__(self, path, *, sdk=None):
        if sdk is None:
            import scservo_sdk as sdk
        real = os.path.realpath(path)
        if not os.path.exists(real):
            raise LeaderError('DEVICE_NOT_CONNECTED', f'主臂控制板没有接上（找不到 {path}）')
        self.port = sdk.PortHandler(real)
        self.packet = sdk.PacketHandler(0)
        if not self.port.openPort() or not self.port.setBaudRate(BAUDRATE):
            raise LeaderError('DEVICE_OPEN_FAILED', f'主臂串口打不开：{real}')
        self.port.ser.write_timeout = .5

    def read(self, motor, register):
        address, size = register
        for _ in range(READ_TRIES):
            try:
                self.port.ser.reset_input_buffer()          # 丢掉迟到的旧应答，否则之后每次都读到“上一条”
                data, comm, _error = self.packet.readTxRx(self.port, motor, address, size)
            except Exception as exc:                        # 拔线时 pyserial 抛 SerialException
                raise LeaderError('DEVICE_IO', f'读 ID{motor} 地址 {address} 失败：{exc}') from exc
            if not comm and len(data) == size:
                return int.from_bytes(bytes(data), 'little')
            time.sleep(.005)
        raise LeaderError('DEVICE_IO', f'读 ID{motor} 地址 {address} 失败（连续 {READ_TRIES} 次没有有效应答）')

    def limits(self):
        """核对 6 个舵机的型号，读出各自的限位。返回 {关节名: (下限, 上限)}。"""
        result = {}
        for motor, name in enumerate(NAMES, 1):
            try:
                self.port.ser.reset_input_buffer()
                model, comm, _error = self.packet.ping(self.port, motor)
            except Exception as exc:
                raise LeaderError('DEVICE_IO', f'ID{motor} 读不到：{exc}') from exc
            if comm:
                raise LeaderError('DEVICE_IO', f'ID{motor} 无应答（主臂没上电？）')
            if model != SERVO_MODEL:
                raise LeaderError('DEVICE_IDENTITY', f'ID{motor} 型号是 {model}，应为 {SERVO_MODEL}（STS3215）')
            lo, hi = self.read(motor, MIN_POSITION_LIMIT), self.read(motor, MAX_POSITION_LIMIT)
            if not 0 <= lo < hi <= 4095:
                raise LeaderError('DEVICE_IDENTITY', f'ID{motor}（{name}）的限位不对：[{lo}, {hi}]')
            result[name] = (lo, hi)
        return result

    def positions(self):
        return {name: self.read(motor, PRESENT_POSITION) for motor, name in enumerate(NAMES, 1)}

    def close(self):
        try:
            if self.port.is_open:
                self.port.closePort()
        except Exception:
            pass


# ══════════ 2. 读一轮 → 一条消息 ══════════

class Leader:
    def __init__(self, *, serial, rate_hz=DEFAULT_RATE_HZ, robot_type=DEFAULT_ROBOT_TYPE, host_dev=DEFAULT_HOST_DEV,
                 bus_factory=None, log=print):
        if not isinstance(serial, str) or not serial.isalnum():
            raise ValueError('serial 必须是主臂控制板的 USB 序列号（字母和数字）')
        if not 1 <= rate_hz <= 100:
            raise ValueError('发布频率必须在 1~100 Hz 之间')
        if not isinstance(robot_type, str) or not robot_type:
            raise ValueError('robot_type 不能为空')
        self.serial, self.rate_hz, self.robot_type = serial, float(rate_hz), robot_type
        self.path = serial_path(host_dev, serial)
        self.bus_factory = bus_factory or (lambda path: LeaderBus(path))
        self.log = log
        self.session_id = uuid.uuid4().hex
        self.process_id = 'so101-leader:' + self.session_id
        self.bus = None
        self.joint_limits = None
        self.seq = 0
        self.problem = None

    def _report(self, problem):
        if problem != self.problem:                         # 状态变了才打日志，免得每秒刷屏
            self.log(json.dumps({'event': 'leader', 'ok': problem is None, 'message': problem,
                                 'serial': self.serial}, ensure_ascii=False))
            self.problem = problem

    def step(self):
        """读一轮。成功返回打包好的 action（teleop/action/v1）；失败关串口、返回 None（调用方等一会儿再试）。"""
        try:
            if self.bus is None:
                self.bus = self.bus_factory(self.path)
                self.joint_limits = self.bus.limits()
                self.log(json.dumps({'event': 'leader_connected', 'serial': self.serial, 'path': self.path,
                                     'limits': {k: list(v) for k, v in self.joint_limits.items()}}, ensure_ascii=False))
            started = _monotonic_ns()
            raw = self.bus.positions()
            finished = _monotonic_ns()
            positions = {name: round(to_units(name, raw[name], *self.joint_limits[name]), 4) for name in NAMES}
            self.seq += 1
            self._report(None)
            return {'schema_version': SCHEMA, 'robot_type': self.robot_type, 'process_id': self.process_id,
                    'action_id': f'{self.session_id}:{self.seq:06d}', 'seq': self.seq, 'source': 'teleop_leader',
                    'leader_serial': self.serial, 'time_basis': 'host_monotonic',
                    'timestamp': round((started + finished) // 2 / 1e9, 6), 'sampled_unix': round(time.time(), 6),
                    'input_observation_id': None, 'positions': positions, 'units': dict(UNITS), 'raw': raw,
                    'rate_hz': self.rate_hz}
        except Exception as exc:                            # 拔线、读失败……都不退出
            self._report(f"{getattr(exc, 'code', type(exc).__name__)}: {exc}")
            self.close()
            return None

    def close(self):
        if self.bus is not None:
            self.bus.close()
        self.bus = None

    def run(self, publish, stop, *, clock=time.monotonic):
        """按固定频率读和发，直到 stop（threading.Event）被设置。跟不上时不补发，从当前时刻重新排。"""
        period = 1.0 / self.rate_hz
        next_at = clock()
        try:
            while not stop.is_set():
                message = self.step()
                if message is None:
                    stop.wait(RETRY_S)
                    next_at = clock()
                    continue
                publish(message)
                next_at = max(next_at + period, clock())
                stop.wait(max(0.0, next_at - clock()))
        finally:
            self.close()


# ══════════ 3. Redis：供数开关开着时把 action 推进动作队列 ══════════

class RedisError(Exception):
    pass


class RedisLink:
    """最小的 Redis 客户端（RESP 协议），只用 GET / LPUSH / LTRIM / SELECT。不需要装 redis 库。"""

    def __init__(self, host, port=6379, db=0, timeout=1.0, connect=socket.create_connection):
        self.host, self.port, self.db, self.timeout, self.connect = host, int(port), int(db), timeout, connect
        self.sock = self.reader = None

    def _open(self):
        self.sock = self.connect((self.host, self.port), self.timeout)
        self.sock.settimeout(self.timeout)
        self.reader = self.sock.makefile('rb')
        if self.db:
            self.call('SELECT', str(self.db))

    def close(self):
        for item in (self.reader, self.sock):
            try:
                if item is not None:
                    item.close()
            except Exception:
                pass
        self.sock = self.reader = None

    @staticmethod
    def encode(*parts):
        out = [b'*%d\r\n' % len(parts)]
        for part in parts:
            data = part if isinstance(part, bytes) else str(part).encode('utf-8')
            out.append(b'$%d\r\n%s\r\n' % (len(data), data))
        return b''.join(out)

    def _reply(self):
        line = self.reader.readline()
        if not line.endswith(b'\r\n'):
            raise RedisError('Redis 连接断了')
        kind, rest = line[:1], line[1:-2]
        if kind == b'+':
            return rest.decode()
        if kind == b'-':
            raise RedisError(rest.decode(errors='replace'))
        if kind == b':':
            return int(rest)
        if kind == b'$':
            size = int(rest)
            if size < 0:
                return None
            data = self.reader.read(size + 2)
            if len(data) != size + 2:
                raise RedisError('Redis 连接断了')
            return data[:-2]
        if kind == b'*':
            return [self._reply() for _ in range(int(rest))]
        raise RedisError('Redis 回复看不懂')

    def calls(self, *commands):
        """一次发出几条命令，按顺序取回各自的回复。出错就关掉连接，下次再连。"""
        try:
            if self.sock is None:
                self._open()
            self.sock.sendall(b''.join(self.encode(*command) for command in commands))
            return [self._reply() for _ in commands]
        except Exception:
            self.close()
            raise

    def call(self, *parts):
        return self.calls(parts)[0]


class ActionSink:
    """看 Redis 的供数开关：开着就把 action 的 JSON 字符串原样 LPUSH 进动作队列（再 LTRIM 到 queue_max 条）。
    开关每 check_s 秒看一次。Redis 连不上只记一次日志、跳过，1 秒后再试，不影响 ROS 发布。"""

    def __init__(self, link, *, queue=ACTION_QUEUE, switch=SWITCH_KEY, queue_max=QUEUE_MAX, check_s=SWITCH_CHECK_S,
                 clock=time.monotonic, log=print):
        self.link, self.queue, self.switch, self.queue_max, self.check_s = link, queue, switch, int(queue_max), check_s
        self.clock, self.log = clock, log
        self.enabled, self.next_check, self.retry_at = False, 0.0, 0.0
        self.pushed, self.problem = 0, None

    def _report(self, problem):
        if problem != self.problem:
            self.log(json.dumps({'event': 'redis', 'ok': problem is None, 'message': problem,
                                 'enabled': self.enabled, 'pushed': self.pushed}, ensure_ascii=False))
            self.problem = problem

    def offer(self, text):
        """返回 True 表示这条推进了队列。"""
        now = self.clock()
        if now < self.retry_at:
            return False
        try:
            if now >= self.next_check:
                was, self.enabled = self.enabled, self.link.call('GET', self.switch) == b'1'
                self.next_check = now + self.check_s
                if was != self.enabled:
                    self.log(json.dumps({'event': 'supply', 'enabled': self.enabled, 'pushed': self.pushed}))
            if not self.enabled:
                self._report(None)
                return False
            self.link.calls(('LPUSH', self.queue, text), ('LTRIM', self.queue, 0, self.queue_max - 1))
            self.pushed += 1
            self._report(None)
            return True
        except Exception as exc:
            self.enabled, self.retry_at = False, now + RETRY_S
            self._report(f'{type(exc).__name__}: {exc}')
            return False

    def close(self):
        self.link.close()


# ══════════ 4. ROS 发布 ══════════

def ros_publisher(topic):
    """建一个只发不收的 ROS 节点。返回 (publish(JSON 字符串), close())。"""
    import rclpy
    from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
    from std_msgs.msg import String
    try:
        from rclpy.signals import SignalHandlerOptions
        rclpy.init(signal_handler_options=SignalHandlerOptions.NO)   # 停止信号由本程序自己处理
    except ImportError:
        rclpy.init()
    node = rclpy.create_node('so101_leader')
    qos = QoSProfile(depth=10, reliability=ReliabilityPolicy.RELIABLE, durability=DurabilityPolicy.VOLATILE,
                     history=HistoryPolicy.KEEP_LAST)
    publisher = node.create_publisher(String, topic, qos)

    def publish(text):
        message = String()
        message.data = text
        publisher.publish(message)

    def close():
        node.destroy_node()
        rclpy.try_shutdown()

    return publish, close


def main(argv=None):
    parser = argparse.ArgumentParser(description='SO101 主臂节点：只读主臂，打包成 action 发到 ROS，供数开关开着时推进 Redis')
    parser.add_argument('command', choices=('run', 'check'))
    parser.add_argument('--serial', required=True, help='主臂控制板的 USB 序列号，例如 5A7C124026')
    parser.add_argument('--rate', type=float, default=DEFAULT_RATE_HZ, help='发布频率（Hz），默认 30')
    parser.add_argument('--topic', default=DEFAULT_TOPIC, help='发布的话题，默认 /so101/action')
    parser.add_argument('--robot-type', default=DEFAULT_ROBOT_TYPE, help='写进 action 的 robot_type，默认 so-101-follower_sim')
    parser.add_argument('--redis-host', default='', help='Redis 地址；不填就只发 ROS、不推 Redis')
    parser.add_argument('--redis-port', type=int, default=6379)
    parser.add_argument('--redis-db', type=int, default=0)
    parser.add_argument('--queue-max', type=int, default=QUEUE_MAX, help='动作队列最多留多少条')
    parser.add_argument('--host-dev', default=DEFAULT_HOST_DEV, help='宿主机 /dev 在容器里的位置')
    args = parser.parse_args(argv)
    log = lambda text: print(text, flush=True)                              # noqa: E731
    leader = Leader(serial=args.serial, rate_hz=args.rate, robot_type=args.robot_type, host_dev=args.host_dev, log=log)

    if args.command == 'check':
        message = leader.step()
        leader.close()
        if message is None:
            raise SystemExit(1)
        print(json.dumps(message, ensure_ascii=False, indent=2))
        return

    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    ros_publish, close = ros_publisher(args.topic)
    sink = ActionSink(RedisLink(args.redis_host, args.redis_port, args.redis_db), queue_max=args.queue_max,
                      log=log) if args.redis_host else None

    def publish(message):
        text = json.dumps(message, ensure_ascii=False)                      # 只打包一次：ROS 和 Redis 里是同一个字符串
        ros_publish(text)
        if sink is not None:
            sink.offer(text)

    print(json.dumps({'event': 'leader_node_started', 'topic': args.topic, 'rate_hz': leader.rate_hz,
                      'serial': leader.serial, 'process_id': leader.process_id, 'robot_type': leader.robot_type,
                      'redis': f'{args.redis_host}:{args.redis_port}/{args.redis_db}' if sink else None,
                      'ros_domain_id': os.environ.get('ROS_DOMAIN_ID')}, ensure_ascii=False), flush=True)
    try:
        leader.run(publish, stop)
    finally:
        if sink is not None:
            sink.close()
        close()


if __name__ == '__main__':
    main()
