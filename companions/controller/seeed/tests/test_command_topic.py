"""单命令 Topic（seeed_ros.CommandTopic）：命令 {action, params: {request_id, ...}} → 内部请求 → 一条终态回执
{action, params: {request_id, status: SUCCEEDED|FAILED, [error], [result]}}。接模拟舵机总线，不需要 ROS。"""
import json
import time
import types

import pytest

import seeed_ros
import seeed_ros_client
from seeed_ros import CommandTopic, Follower
from sim_seeed import SimBus, connect
from test_seeed_ros import STAND, RealClock, clock, make  # noqa: F401  clock 是 fixture

POSE_KEYS = ('x', 'y', 'z', 'roll', 'pitch', 'yaw')


@pytest.fixture(autouse=True)
def direct(monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)


def command(action, params=None, request_id='abc123', **override):
    return {'action': action, 'params': {'request_id': request_id, **(params or {})}, **override}


def ok(action, request_id='abc123'):
    return {'action': action, 'params': {'request_id': request_id, 'status': 'SUCCEEDED'}}


class Topic:
    """CommandTopic 加一个收回执的列表：send / poll 返回这一次新发出的回执。"""

    def __init__(self, follower):
        self.sent = []
        self.topic = CommandTopic(follower, self.sent.append)

    def send(self, packet):
        before = len(self.sent)
        self.topic.handle(packet if isinstance(packet, str) else json.dumps(packet))
        return self.sent[before:]

    def poll(self):
        before = len(self.sent)
        self.topic.poll()
        return self.sent[before:]


@pytest.fixture
def topic(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path, pose={**STAND, 'gripper': 2429})
    value = Topic(follower)
    value.bus = bus
    return value


# ══════════ 1. 七个原动作：受理时不回，结束时回一条，带回 request_id ══════════

def test_arm_moveto_back_to_the_inspected_pose(topic):
    [inspect] = topic.send(command('ARM_INSPECT', request_id='look'))
    assert inspect['action'] == 'ARM_INSPECT' and inspect['params']['request_id'] == 'look'
    assert inspect['params']['status'] == 'SUCCEEDED' and 'error' not in inspect['params']
    tool = inspect['params']['result']['native']['tool']
    assert topic.send(command('MOVE_GRASP', {'direction': 'up', 'distance': .01}, 'up-1')) == []    # 受理时不回
    assert topic.poll() == [ok('MOVE_GRASP', 'up-1')]
    target = dict(zip(POSE_KEYS, tool['position_m'] + tool['orientation_rpy']))
    assert topic.send(command('ARM_MOVETO', {'speed': 0.5, 'target_position': target})) == []
    assert topic.poll() == [ok('ARM_MOVETO')]        # {"request_id": "abc123", "status": "SUCCEEDED"}
    assert topic.poll() == [] and topic.topic.waiting == {}


def test_a_failed_action_reports_its_error(topic, tmp_path):
    (tmp_path / 'config.json').write_text('{}', encoding='utf-8')            # 没示教胸前位
    assert topic.send(command('HOLD_ARM', request_id='no-chest')) == []
    [final] = topic.poll()
    assert final['params']['request_id'] == 'no-chest' and final['params']['status'] == 'FAILED'
    assert final['params']['error']['code'] == 'POSE_NOT_CONFIGURED' and final['params']['error']['message']
    assert not topic.bus.writes


def test_same_request_id_is_answered_not_executed_twice(topic):
    topic.send(command('LOOSEN', request_id='once'))
    assert topic.poll() == [ok('LOOSEN', 'once')]
    writes = len(topic.bus.writes)
    assert topic.send(command('LOOSEN', request_id='once')) == [ok('LOOSEN', 'once')]   # 已结束：立刻回原结果
    assert len(topic.bus.writes) == writes
    [conflict] = topic.send(command('GRASP', request_id='once'))
    assert conflict['params']['status'] == 'FAILED' and conflict['params']['error']['code'] == 'IDEMPOTENCY_CONFLICT'
    assert len(topic.bus.writes) == writes


def test_any_non_empty_request_id_works_and_is_echoed(topic):
    odd = '平台/调用:2026-10-22#7'                                          # 不符合内部编号规则：内部换成哈希
    topic.send(command('GRASP', request_id=odd))
    assert topic.poll() == [ok('GRASP', odd)]


# ══════════ 2. 格式或参数不对：立刻回一条 FAILED，臂不动 ══════════

@pytest.mark.parametrize('packet, code', [
    ('{not json', 'INVALID_JSON'),
    ('[1, 2]', 'INVALID_ENVELOPE'),
    ({'action': 'ARM_INSPECT'}, 'INVALID_ENVELOPE'),
    ({'params': {'request_id': 'abc123'}}, 'INVALID_ENVELOPE'),
    ({'action': 'ARM_INSPECT', 'params': {}}, 'INVALID_ID'),          # 没带 request_id
    (command('ARM_INSPECT', request_id=''), 'INVALID_ID'),
    (command('ARM_INSPECT', request_id=7), 'INVALID_ID'),
    (command(7), 'UNKNOWN_ACTION'),
    (command('FLY'), 'UNKNOWN_ACTION'),
    ({'action': 'LOOSEN', 'params': '{}'}, 'INVALID_ARGUMENT'),         # params 不能再编码成字符串
    (command('ARM_MOVETO', {'speed': .5, 'position': {'x': 1.2}}), 'INVALID_ARGUMENT'),
    (command('ARM_MOVETO', {'target_position': {'x': .18, 'y': .02, 'z': .11}, 'speed': .5}), 'INVALID_ARGUMENT'),
    (command('MOVE_GRASP', {'direction': 'up', 'distance': '0.02'}), 'INVALID_ARGUMENT'),
    (command('MOVE_GRASP', {'direction': 'up', 'distance': .005}), 'INVALID_ARGUMENT'),
    (command('HOLD_ARM', {'side': 'left'}), 'TARGET_NOT_CONFIGURED'),
    (command('ARM_INSPECT', {'verbose': True}), 'INVALID_ARGUMENT'),
])
def test_bad_commands_fail_at_once_without_motion(topic, packet, code):
    [reply] = topic.send(packet)
    assert set(reply) == {'action', 'params'} and set(reply['params']) == {'request_id', 'status', 'error'}
    assert reply['params']['status'] == 'FAILED' and reply['params']['error']['code'] == code, reply
    assert not topic.bus.writes and topic.topic.follower.active is None


def test_failed_reply_echoes_the_request_id_it_could_read(topic):
    [reply] = topic.send(command('FLY', request_id='req-009'))
    assert reply['action'] == 'FLY' and reply['params']['request_id'] == 'req-009'


def test_extra_fields_are_ignored(topic):
    [reply] = topic.send({**command('ARM_INSPECT'), 'id': 2, 'sendMode': 0})
    assert reply['params']['status'] == 'SUCCEEDED' and set(reply) == {'action', 'params'}


# ══════════ 3. 查询、停止、示教：立刻回一条，数据在 params.result ══════════

def test_control_commands_answer_once(topic):
    [catalog] = topic.send(command('ARM_CATALOG', request_id='c1'))
    assert catalog['params']['status'] == 'SUCCEEDED' and set(catalog['params']['result']['actions']) == set(seeed_ros.CATALOG)
    [teach] = topic.send(command('ARM_TEACH_PARK', request_id='t1'))
    assert teach['params']['status'] == 'SUCCEEDED' and teach['params']['result']['saved']
    [stop] = topic.send(command('ARM_STOP', request_id='s1'))
    assert stop['params']['request_id'] == 's1' and stop['params']['result']['state'] == 'STOPPED'
    assert not topic.bus.writes and topic.poll() == []


# ══════════ 4. 真实线程：执行中不回、重发不重复执行，结束回一条；第二个动作立刻以 BUSY 失败 ══════════

def test_one_reply_at_the_end_resend_is_harmless_and_a_second_action_is_busy(monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'gripper_open': 2429 + 600}), encoding='utf-8')   # 约 3 秒行程
    follower = Follower(connect(monkeypatch, None, SimBus(RealClock())), tmp_path)
    topic = Topic(follower)
    try:
        assert topic.send(command('LOOSEN', request_id='slow-1')) == []
        time.sleep(.3)
        assert topic.send(command('LOOSEN', request_id='slow-1')) == []          # 执行中重发：不回、不重复执行
        [busy] = topic.send(command('GRASP', request_id='slow-2'))
        assert busy['params']['request_id'] == 'slow-2' and busy['params']['error']['code'] == 'BUSY'
        replies, end = [], time.monotonic() + 30
        while time.monotonic() < end and not replies:
            replies += topic.poll()
            time.sleep(.05)
        assert replies == [ok('LOOSEN', 'slow-1')]
    finally:
        follower.close()


# ══════════ 5. 命令行客户端拼出来的命令 ══════════

def cli(**values):
    base = dict.fromkeys(('operation', 'name', 'side', 'pose', 'speed', 'direction', 'distance', 'params'))
    return types.SimpleNamespace(**{**base, **values})


def test_cli_builds_commands_the_node_accepts():
    build = seeed_ros_client.build
    pose = [.18, .02, .11, 2.63, -.03, 1.6]
    assert build(cli(operation='ARM_MOVETO', pose=pose, speed=.5)) == (
        'ARM_MOVETO', {'target_position': dict(zip(POSE_KEYS, pose)), 'speed': .5})
    assert build(cli(operation='teach', name='chest')) == ('ARM_TEACH_CHEST', {})
    for bad in (cli(operation='MOVE_GRASP', direction='up'), cli(operation='ARM_MOVETO'), cli(operation='teach'),
                cli(operation='LOOSEN', params='[]')):
        with pytest.raises(ValueError):
            build(bad)
    packet = seeed_ros_client.command('ARM_MOVETO', build(cli(operation='ARM_MOVETO', pose=pose))[1], 'x1')
    assert packet['action'] == 'ARM_MOVETO' and packet['params']['request_id'] == 'x1'
    assert seeed_ros_client.command('GRASP', {})['params']['request_id'].startswith('cli-')
    assert CommandTopic._convert(packet)['operation'] == 'execute'
