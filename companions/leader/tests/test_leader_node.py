"""主臂节点测试：用假的飞特 SDK，不需要真主臂，也不需要 ROS。"""
import json
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import leader_node  # noqa: E402
from leader_node import NAMES, Leader, LeaderBus, LeaderError, to_units  # noqa: E402

LIMITS = {'shoulder_pan': (700, 3400), 'shoulder_lift': (800, 3300), 'elbow_flex': (900, 3100),
          'wrist_flex': (850, 3200), 'wrist_roll': (0, 4095), 'gripper': (2000, 3500)}


class FakeSerial:
    write_timeout = None

    def reset_input_buffer(self):
        pass


class FakePort:
    def __init__(self, path):
        self.path, self.is_open, self.ser = path, False, FakeSerial()

    def openPort(self):
        self.is_open = True
        return True

    def setBaudRate(self, baud):
        self.baud = baud
        return True

    def closePort(self):
        self.is_open = False


class FakeArm:
    """6 个舵机的寄存器。记下每一次调用，好检查从来没有写。"""

    def __init__(self):
        self.registers = {}
        self.models = dict.fromkeys(range(1, 7), 777)
        for motor, name in enumerate(NAMES, 1):
            lo, hi = LIMITS[name]
            self.registers[motor] = {9: lo, 11: hi, 56: (lo + hi) // 2}
        self.calls, self.drop_next, self.broken = [], 0, False

    def sdk(self):
        arm = self

        class PacketHandler:
            def __init__(self, end):
                assert end == 0

            def readTxRx(self, port, motor, address, size):
                arm.calls.append(('read', motor, address))
                if arm.broken:
                    raise OSError('device disconnected')
                if arm.drop_next:
                    arm.drop_next -= 1
                    return [], -3001, 0
                return list(arm.registers[motor][address].to_bytes(size, 'little')), 0, 0

            def ping(self, port, motor):
                arm.calls.append(('ping', motor))
                if arm.broken:
                    raise OSError('device disconnected')
                return arm.models[motor], 0, 0

            def __getattr__(self, name):                   # 任何写操作都算失败
                raise AssertionError(f'leader node must never call {name}')

        class Sdk:
            pass

        sdk = Sdk()
        sdk.PortHandler = FakePort
        sdk.PacketHandler = PacketHandler
        return sdk


def make_leader(tmp_path, arm, **kw):
    dev = tmp_path / 'dev'
    (dev / 'serial/by-id').mkdir(parents=True, exist_ok=True)
    (dev / 'ttyACM1').write_text('')
    link = dev / 'serial/by-id/usb-1a86_USB_Single_Serial_5A7C124026-if00'
    if not link.exists():
        link.symlink_to('../../ttyACM1')
    opened = []

    def factory(path):
        opened.append(path)
        return LeaderBus(path, sdk=arm.sdk())

    logs = []
    leader = Leader(serial='5A7C124026', host_dev=str(dev), bus_factory=factory, log=logs.append, **kw)
    return leader, opened, logs


def test_units_match_lerobot_formula():
    assert to_units('shoulder_pan', 2050, 700, 3400) == 0
    assert to_units('elbow_flex', 2000 + 4095 // 4, 900, 3100) == pytest.approx((4095 // 4 - 0) * 360 / 4095)
    assert to_units('gripper', 2000, 2000, 3500) == 0
    assert to_units('gripper', 3500, 2000, 3500) == 100
    assert to_units('gripper', 1500, 2000, 3500) == 0             # 越过限位先夹回来
    assert to_units('gripper', 2750, 2000, 3500) == 50


def test_each_step_is_one_action_in_the_queue_format(tmp_path):
    arm = FakeArm()
    leader, opened, logs = make_leader(tmp_path, arm)
    first, second = leader.step(), leader.step()
    assert len(opened) == 1                                      # 连一次，之后一直用同一个串口
    assert (first['seq'], second['seq']) == (1, 2)
    assert first['schema_version'] == 'teleop/action/v1' and first['source'] == 'teleop_leader'
    assert first['robot_type'] == 'so-101-follower_sim' and first['leader_serial'] == '5A7C124026'
    assert first['process_id'] == second['process_id'] == 'so101-leader:' + leader.session_id
    assert first['action_id'] == leader.session_id + ':000001' and second['action_id'].endswith(':000002')
    assert first['input_observation_id'] is None and first['time_basis'] == 'host_monotonic'
    assert list(first['positions']) == list(NAMES)              # 关节名、顺序和 Perceptor 观测的 state 相同
    assert first['positions']['shoulder_pan'] == 0               # 读数在限位中点 → 0 度
    assert first['positions']['gripper'] == 50
    assert first['units']['gripper'] == 'normalized_0_100' and first['units']['elbow_flex'] == 'deg'
    assert first['raw']['shoulder_pan'] == 2050
    assert 0 < first['timestamp'] < second['timestamp'] and first['sampled_unix'] > 1.7e9
    json.dumps(first)                                            # 能直接发成 JSON
    assert any('leader_connected' in line and '[2000, 3500]' in line for line in logs)   # 限位写在日志里


def test_robot_type_is_configurable(tmp_path):
    leader, _, _ = make_leader(tmp_path, FakeArm(), robot_type='so-101-follower')
    assert leader.step()['robot_type'] == 'so-101-follower'
    with pytest.raises(ValueError):
        Leader(serial='5A7C124026', robot_type='', host_dev=str(tmp_path))


class FakeLink:
    """假的 Redis 连接：记下命令；GET 返回 switch 的值；broken 时抛错。"""

    def __init__(self):
        self.switch, self.broken, self.commands, self.queue = None, False, [], []

    def calls(self, *commands):
        if self.broken:
            raise OSError('connection refused')
        replies = []
        for command in commands:
            self.commands.append(command)
            if command[0] == 'GET':
                replies.append(self.switch)
            elif command[0] == 'LPUSH':
                self.queue.insert(0, command[2])
                replies.append(len(self.queue))
            elif command[0] == 'LTRIM':
                del self.queue[command[3] + 1:]
                replies.append('OK')
        return replies

    def call(self, *parts):
        return self.calls(parts)[0]

    def close(self):
        pass


def test_actions_go_to_redis_only_while_the_switch_is_on():
    link, now, logs = FakeLink(), [0.0], []
    sink = leader_node.ActionSink(link, queue_max=3, clock=lambda: now[0], log=logs.append)
    assert sink.offer('a1') is False and link.queue == []       # 开关没开：不推
    link.switch = b'1'
    assert sink.offer('a2') is False                             # 0.5 秒内不重新看开关
    now[0] = .6
    assert sink.offer('a3') is True and sink.offer('a4') is True
    assert link.queue == ['a4', 'a3']                            # LPUSH：新的在左边，消费方从右边 BRPOP
    assert ('LPUSH', 'collection_action_queue', 'a3') in link.commands
    for i in range(5):
        sink.offer(f'b{i}')
    assert link.queue == ['b4', 'b3', 'b2']                      # LTRIM 到 queue_max 条
    link.switch = None                                           # 关开关（DEL）
    now[0] = 1.2
    assert sink.offer('c1') is False and link.queue[0] == 'b4'
    assert [json.loads(line)['enabled'] for line in logs if '"supply"' in line] == [True, False]


def test_redis_failure_is_skipped_and_retried():
    link, now, logs = FakeLink(), [0.0], []
    link.switch = b'1'
    sink = leader_node.ActionSink(link, clock=lambda: now[0], log=logs.append)
    assert sink.offer('a1') is True
    link.broken = True
    assert sink.offer('a2') is False                             # 连不上：跳过，不抛
    link.broken = False
    now[0] = .5
    assert sink.offer('a3') is False                             # 1 秒内不重试
    now[0] = 1.1
    assert sink.offer('a4') is True and link.queue == ['a4', 'a1']
    assert sum('"ok": false' in line for line in logs) == 1


def test_redis_link_speaks_resp():
    import io

    class Sock:
        def __init__(self):
            self.sent = b''

        def settimeout(self, value):
            pass

        def sendall(self, data):
            self.sent += data

        def makefile(self, mode):
            return io.BytesIO(b'+OK\r\n$1\r\n1\r\n:3\r\n+OK\r\n$-1\r\n')

        def close(self):
            pass

    sock = Sock()
    link = leader_node.RedisLink('192.168.0.215', 6379, db=2, connect=lambda address, timeout: sock)
    assert link.call('GET', 'collection_enabled') == b'1'       # 连上时先 SELECT 2（回 +OK）
    assert link.calls(('LPUSH', 'q', '{"a": 1}'), ('LTRIM', 'q', 0, 999)) == [3, 'OK']
    assert link.call('GET', 'missing') is None
    assert sock.sent.startswith(b'*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n*2\r\n$3\r\nGET\r\n$18\r\ncollection_enabled\r\n')
    assert b'*3\r\n$5\r\nLPUSH\r\n$1\r\nq\r\n$8\r\n{"a": 1}\r\n*4\r\n$5\r\nLTRIM\r\n' in sock.sent


def test_never_writes_to_the_leader(tmp_path):
    arm = FakeArm()
    leader, _, _ = make_leader(tmp_path, arm)
    for _ in range(5):
        assert leader.step() is not None
    assert {call[0] for call in arm.calls} == {'read', 'ping'}
    assert {call[2] for call in arm.calls if call[0] == 'read'} == {9, 11, 56}


def test_dropped_frame_is_retried(tmp_path):
    arm = FakeArm()
    leader, _, _ = make_leader(tmp_path, arm)
    leader.step()
    arm.drop_next = 2                                            # 丢两帧，第三次读到
    assert leader.step()['seq'] == 2


def test_failure_closes_and_reconnects_without_resetting_seq(tmp_path):
    arm = FakeArm()
    leader, opened, logs = make_leader(tmp_path, arm)
    assert leader.step()['seq'] == 1
    arm.broken = True
    assert leader.step() is None and leader.bus is None          # 读位置时断了
    assert leader.step() is None                                 # 重连时 ping 不到
    failures = len([line for line in logs if '"ok": false' in line])
    assert leader.step() is None                                 # 和上一次同样的错误：不再记日志
    assert len([line for line in logs if '"ok": false' in line]) == failures
    arm.broken = False
    assert leader.step()['seq'] == 2                             # 编号接着上次往下走
    assert len(opened) == 4                                      # 每次失败后重新打开串口
    assert '"ok": true' in logs[-1]


def test_wrong_servo_model_is_refused(tmp_path):
    arm = FakeArm()
    arm.models[3] = 1234
    leader, _, logs = make_leader(tmp_path, arm)
    assert leader.step() is None
    assert 'DEVICE_IDENTITY' in logs[-1]


def test_missing_board_is_reported(tmp_path):
    leader = Leader(serial='5A7C999999', host_dev=str(tmp_path), log=lambda text: None)
    with pytest.raises(LeaderError) as error:
        LeaderBus(leader.path, sdk=FakeArm().sdk())
    assert error.value.code == 'DEVICE_NOT_CONNECTED'


def test_bad_settings_are_rejected(tmp_path):
    with pytest.raises(ValueError):
        Leader(serial='../ttyACM0', host_dev=str(tmp_path))
    with pytest.raises(ValueError):
        Leader(serial='5A7C124026', rate_hz=500, host_dev=str(tmp_path))


def test_run_publishes_at_the_set_rate_and_waits_after_failures(tmp_path, monkeypatch):
    arm = FakeArm()
    leader, _, _ = make_leader(tmp_path, arm, rate_hz=20)
    now = [100.0]
    waits, published = [], []

    class Stop:
        def is_set(self):
            return len(waits) >= 6

        def wait(self, seconds):
            waits.append(round(seconds, 6))
            now[0] += seconds

    def publish(message):
        published.append(message['seq'])
        if message['seq'] == 2:
            arm.broken = True                                    # 下一轮读失败
        now[0] += .01                                            # 每轮读和发用掉 10 毫秒

    def heal_after_retry(seconds):
        arm.broken = False

    stop = Stop()
    original_wait = stop.wait

    def wait(seconds):
        if seconds == leader_node.RETRY_S:
            heal_after_retry(seconds)
        original_wait(seconds)

    stop.wait = wait
    leader.run(publish, stop, clock=lambda: now[0])
    assert published == [1, 2, 3, 4, 5]
    assert waits[0] == pytest.approx(.04) and waits[1] == pytest.approx(.04)   # 50 ms 一轮，扣掉 10 ms
    assert waits[2] == leader_node.RETRY_S                       # 失败后等 1 秒再连
    assert leader.bus is None                                    # 退出时关了串口


def test_check_command_prints_one_reading(tmp_path, monkeypatch, capsys):
    arm = FakeArm()
    monkeypatch.setattr(leader_node, 'LeaderBus', lambda path: LeaderBus(path, sdk=arm.sdk()))
    leader, _, _ = make_leader(tmp_path, arm)                    # 只为建好假的 /dev
    leader_node.main(['check', '--serial', '5A7C124026', '--host-dev', str(tmp_path / 'dev')])
    printed = capsys.readouterr().out
    reading = json.loads(printed[printed.index('{\n'):])
    assert reading['seq'] == 1 and reading['positions']['gripper'] == 50


def test_stop_event_ends_run(tmp_path):
    arm = FakeArm()
    leader, _, _ = make_leader(tmp_path, arm, rate_hz=100)
    stop, published = threading.Event(), []

    def publish(message):
        published.append(message)
        if len(published) == 3:
            stop.set()

    leader.run(publish, stop)
    assert len(published) == 3 and leader.bus is None
