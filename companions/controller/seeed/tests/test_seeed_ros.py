"""不接硬件的检查：seeed_ros.Follower 接在 sim_seeed.py 的模拟舵机总线上，按规范逐条发 JSON 请求。

ROS 收发层（run_node）没有真的 rclpy 可用，最后一个测试用桩模块顶替它，只检查接线是否正确。
"""
import itertools
import json
import sys
import threading
import time
import types

import pytest

import seeed_arm
import seeed_ros
from conftest import Clock
from seeed_arm import JOINTS, MARGIN
from seeed_ros import DEVICE_ID, Follower, envelope
from sim_seeed import FOLDED, SimBus, connect

STAND = {'shoulder_pan': 2080, 'shoulder_lift': 1360, 'elbow_flex': 2490, 'wrist_flex': 3020, 'wrist_roll': 1125}
OPEN, CLOSED = JOINTS['gripper'].hi - MARGIN, JOINTS['gripper'].lo + MARGIN
counter = itertools.count()


class WallClock(Clock):
    """假时钟再加一个墙上时间（信封的 issued_at / expires_at 用）。hook 在每次 sleep 时被调用。"""
    hook = None

    def time(self):
        return 1_790_000_000 + self.t

    def sleep(self, seconds):
        super().sleep(seconds)
        if self.hook:
            self.hook(self.t)


@pytest.fixture
def clock(monkeypatch):
    c = WallClock()
    monkeypatch.setattr(seeed_arm, 'time', c)
    monkeypatch.setattr(seeed_ros, 'time', c)
    monkeypatch.setattr(seeed_ros, 'LEASE_ACTIVE', 1e9)      # 测试里没有人发心跳；租约到期另有专门的测试
    return c


def make(clock, monkeypatch, tmp_path, **faults):
    bus = SimBus(clock, **faults)
    return Follower(connect(monkeypatch, clock, bus), tmp_path, background=False), bus


def ask(follower, operation, payload, request_id=None, on_action_topic=None):
    request_id = request_id or f'{operation}-{next(counter)}'
    text = json.dumps(envelope(operation, payload, request_id))
    return follower.handle(text, operation == 'execute' if on_action_topic is None else on_action_topic)


def ready(follower):
    """规范的调用顺序：inspect → bind → confirm_workspace。返回 epoch。"""
    session = ask(follower, 'inspect', {})['result']['native']['session_id']
    epoch = ask(follower, 'bind', {'device_id': DEVICE_ID, 'controller_session_id': session,
                                   'adopt_existing_session': True})['result']['epoch']
    assert ask(follower, 'confirm_workspace', {'epoch': epoch, 'secured': True, 'clear': True})['ok']
    return epoch


def act(follower, epoch, action_id, parameters=None, request_id=None):
    """发一个动作，返回 (受理回执, 最终结果)。测试里动作是同步执行的。"""
    request_id = request_id or f'act-{next(counter)}'
    payload = {'epoch': epoch, 'action_id': action_id, 'actionType': 'ExecutionAction',
               'actionCategory': 'BuildinAction', 'part_id': 2,
               'parameters': {'side': 'right'} if parameters is None else parameters}
    accepted = ask(follower, 'execute', payload, request_id)
    final = ask(follower, 'job', {'request_id': request_id})
    return accepted, final.get('result')


def goal_writes(bus, motor=None):
    return [m for m, addr in bus.writes if addr == 42 and motor in (None, m)]


# ══════════ 信封格式 ══════════

def test_envelope_rules(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    good = envelope('inspect', {}, 'env-1')
    assert follower.handle(json.dumps(good), False)['ok']

    def error(packet, on_action_topic=False):
        return follower.handle(json.dumps(packet), on_action_topic)['error']['code']
    assert len(good) == 8
    assert error({**good, 'signature': 'legacy'}) == 'INVALID_ENVELOPE'
    assert error({**good, 'extra': 1}) == 'INVALID_ENVELOPE'
    assert error({**good, 'robot_id': 'other-robot'}) == 'WRONG_TARGET'
    assert error(envelope('inspect', {}, 'env-2', now=clock.time() - 60)) == 'EXPIRED'
    assert error(good, on_action_topic=True) == 'WRONG_TOPIC'
    reply = follower.handle('{not json', False)
    assert reply['error']['code'] == 'INVALID_JSON' and 'request_id' not in reply
    assert bus.writes == []


# ══════════ 查询 ══════════

def test_state_samples_are_numbered_for_the_push_topic(clock, monkeypatch, tmp_path):
    """2026-11-04：真机后端每次采样也带编号和读数时刻（没有更上游的源头，source 为空），推送钩子每采一次调用一次。"""
    follower, bus = make(clock, monkeypatch, tmp_path)
    pushed = []
    follower.on_sample = pushed.append
    a, b = follower.sample(), follower.sample()
    assert b['seq'] == a['seq'] + 1 and pushed == [a, b] and 'source' not in a
    assert isinstance(a['sampled_mono_ns'], int) and b['sampled_mono_ns'] >= a['sampled_mono_ns']
    native = follower.state(b)['native']
    assert native['sample_seq'] == b['seq'] and native['sampled_unix'] == b['at'] and native['source'] is None
    follower.on_sample = lambda snap: 1 / 0                                  # 推送出错不影响采样
    assert follower.sample()['seq'] == b['seq'] + 1
    assert bus.writes == []                                                  # 只读


# ══════════ 跟随主臂（/so101/action，2026-11-04）══════════

def action_message(seq=1, **positions):
    values = dict.fromkeys(('shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll'), 0.0)
    values['gripper'] = 50.0
    values.update(positions)
    return json.dumps({'schema_version': 'teleop/action/v1', 'process_id': 'so101-leader:abc', 'seq': seq,
                       'action_id': f'abc:{seq:06d}', 'positions': values})


def test_follow_writes_leader_targets_in_the_observation_frame(clock, monkeypatch, tmp_path):
    """主臂 action（度 / 0~100）→ 从臂目标 ticks，用的是 Perceptor 读观测时同一张 JOINTS 表，跟到位后观测 = action。"""
    follower, bus = make(clock, monkeypatch, tmp_path)
    assert follower.offer_action(action_message(shoulder_pan=10.0, gripper=25.0))
    assert follower.follow_step() is True and follower.following
    pan, grip = JOINTS['shoulder_pan'], JOINTS['gripper']
    assert bus.peek(pan.id, 42) == round(10.0 * 4095 / 360 + (pan.lo + pan.hi) / 2)
    assert bus.peek(grip.id, 42) == round(grip.lo + .25 * (grip.hi - grip.lo))
    observed = (bus.peek(pan.id, 42) - (pan.lo + pan.hi) / 2) * 360 / 4095           # Perceptor 的 encoder_mapping
    assert observed == pytest.approx(10.0, abs=.1)
    assert all(bus.mem[j.id][40] == 1 for j in JOINTS.values())                      # 六个关节都上电
    # 跟随不加安全限制：速度 0（全速）、加速度 0（不限）、扭矩 100%
    assert all(bus.peek(j.id, 46) == 0 and bus.mem[j.id][41] == 0 and bus.peek(j.id, 48) == 1000
               for j in JOINTS.values())
    follow = follower.state()['native']['follow']
    assert follow['following'] is True and follow['action_seq'] == 1 and follow['action_process_id'] == 'so101-leader:abc'
    writes = len(bus.writes)
    assert follower.follow_step() is False and len(bus.writes) == writes            # 没有新 action：不重复写
    assert follower.offer_action(action_message(2, shoulder_pan=1e6))                # 越界：只夹在限位寄存器以内，不留余量
    assert follower.follow_step() and bus.peek(pan.id, 42) == pan.hi


def test_follow_skips_power_on_checks_and_primitives_get_their_limits_back(clock, monkeypatch, tmp_path):
    """舵机 70°C：跟随照样上电跟随（不查温度）；之后的原动作恢复自己的扭矩上限和加速度。"""
    follower, bus = make(clock, monkeypatch, tmp_path, temps=lambda t: 70)
    assert follower.offer_action(action_message(shoulder_pan=3.0))
    assert follower.follow_step() and all(bus.mem[j.id][40] == 1 for j in JOINTS.values())
    assert follower.unlimited and bus.peek(JOINTS['gripper'].id, 48) == 1000
    follower._restore_primitive_limits()                                             # 原动作开始前调用（_run_job）
    assert not follower.unlimited
    assert all(bus.peek(j.id, 48) == j.torque_cap and bus.mem[j.id][41] == seeed_arm.DEFAULT_ACC
               for j in JOINTS.values())
    writes = len(bus.writes)
    follower._restore_primitive_limits()                                             # 没跟随过：不再写
    assert len(bus.writes) == writes


def test_power_on_with_limits_is_unchanged(clock, monkeypatch, tmp_path):
    """原动作照旧：上电写扭矩上限、默认速度和加速度，过热拒绝。"""
    follower, bus = make(clock, monkeypatch, tmp_path)
    follower.arm.torque_on(['shoulder_pan'])
    pan = JOINTS['shoulder_pan']
    assert bus.peek(pan.id, 48) == pan.torque_cap and bus.peek(pan.id, 46) == seeed_arm.DEFAULT_SPEED
    assert bus.mem[pan.id][41] == seeed_arm.DEFAULT_ACC
    (tmp_path / 'hot').mkdir()
    hot, _ = make(clock, monkeypatch, tmp_path / 'hot', temps=lambda t: 70)
    with pytest.raises(seeed_arm.ArmError, match='温度'):
        hot.arm.torque_on(['elbow_flex'])


def test_follow_holds_when_actions_stop_or_a_primitive_runs(clock, monkeypatch, tmp_path, capsys):
    follower, bus = make(clock, monkeypatch, tmp_path)
    follower.offer_action(action_message(shoulder_pan=5.0))
    assert follower.follow_step() and follower.following
    follower.offer_action(action_message(2, shoulder_pan=6.0))
    follower.active = 'act-1'                                                        # 原动作在执行：让给它
    writes = len(bus.writes)
    assert follower.follow_step() is False and len(bus.writes) == writes and follower.following is False
    follower.active = None
    follower.offer_action(action_message(3, shoulder_pan=7.0))
    assert follower.follow_step() is True and follower.following                     # 动作完了接着跟
    clock.t += seeed_ros.FOLLOW_STALE_S + .1                                         # 主臂停了
    assert follower.follow_step() is False and follower.following is False
    out = capsys.readouterr().out
    assert 'action act-1 running' in out and 'action stale' in out and out.count('"state": "following"') == 2


def test_follow_ignores_messages_that_are_not_leader_actions(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    good = json.loads(action_message())
    bad = ['not json', '[1, 2]', json.dumps({**good, 'schema_version': 'capture/v5'}),
           json.dumps({**good, 'positions': {'shoulder_pan': 1.0}}),
           json.dumps({**good, 'positions': {**good['positions'], 'gripper': True}}),
           action_message(gripper=float('nan'))]
    assert not any(follower.offer_action(text) for text in bad)
    assert follower.follow_target is None and follower.follow_step() is False and bus.writes == []
    monkeypatch.setattr(seeed_ros, 'FOLLOW', False)                                  # SO101_FOLLOW=0
    assert follower.offer_action(action_message()) and follower.follow_step() is False and bus.writes == []


def test_inspect_and_catalog_match_the_documented_shape(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    state = ask(follower, 'inspect', {})['result']
    assert state['robot_id'] == 'so101-follower-seeed' and state['device_id'] == DEVICE_ID
    assert state['bound'] is False and state['active_request_id'] is None
    native = state['native']
    assert native['connected'] and native['feedback_fresh'] and native['fault'] is None
    assert native['phase'] == 'CONNECTED_UNARMED' and len(native['joints']) == 6
    assert native['joints'][5] == {'id': 6, 'name': 'gripper', 'position': 2429, 'goal': 2429, 'torque': False,
                                   'temperature': 30, 'voltage_raw': 0, 'status': 0}
    catalog = ask(follower, 'catalog', {})['result']
    assert set(catalog['actions']) == {'ARM_MOVETO', 'MOVE_GRASP', 'ARM_RELEASE', 'LOOSEN', 'GRASP', 'HOLD_ARM',
                                       'HOLD_GRASP'}
    assert catalog['part_ids_by_side'] == {'right': [2]}
    assert all({'name', 'definition', 'part_id', 'parameters', 'failure_message', 'actionType', 'actionCategory'}
               <= set(spec) for spec in catalog['actions'].values())
    assert bus.writes == []


# ══════════ 绑定、现场确认、心跳、释放 ══════════

def test_control_sequence_is_enforced(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    execute = {'epoch': 1, 'action_id': 'LOOSEN', 'actionType': 'ExecutionAction',
               'actionCategory': 'BuildinAction', 'part_id': 2, 'parameters': {'side': 'right'}}
    assert ask(follower, 'execute', execute)['error']['code'] == 'STALE_CONTROL'            # 没绑定
    bind = {'device_id': DEVICE_ID, 'controller_session_id': 'someone-else', 'adopt_existing_session': True}
    assert ask(follower, 'bind', bind)['error']['code'] == 'BIND_CONFIRMATION_REQUIRED'      # session 不对
    bind['controller_session_id'] = follower.session_id
    bound = ask(follower, 'bind', bind)['result']
    assert bound['bound'] and bound['motion_started'] is False
    epoch = bound['epoch']
    _, job = act(follower, epoch, 'LOOSEN')                                                  # 绑定了但没确认现场
    assert job['state'] == 'FAILED' and job['error_code'] == 'WORKSPACE_NOT_CONFIRMED'
    refused = ask(follower, 'confirm_workspace', {'epoch': epoch, 'secured': True, 'clear': False})
    assert refused['error']['code'] == 'WORKSPACE_NOT_CONFIRMED'
    assert ask(follower, 'confirm_workspace', {'epoch': epoch, 'secured': True, 'clear': True})['result']['confirmed']
    assert ask(follower, 'heartbeat', {'epoch': epoch})['result'] == {'epoch': epoch, 'lease_seconds': 120}
    assert bus.writes == []                                                                  # 到这里臂还没被碰过

    released = ask(follower, 'release_control', {'epoch': epoch})['result']
    assert released == {'released': True, 'torque_changed': False}
    assert ask(follower, 'heartbeat', {'epoch': epoch})['error']['code'] == 'STALE_CONTROL'  # 旧 epoch 作废


def test_idle_lease_expires_without_heartbeat(clock, monkeypatch, tmp_path):
    follower, _ = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    clock.sleep(121)
    assert ask(follower, 'heartbeat', {'epoch': epoch})['error']['code'] == 'LEASE_EXPIRED'


# ══════════ 夹爪：LOOSEN / GRASP ══════════

def test_loosen_walks_the_jaw_to_the_open_endpoint_in_bounded_steps(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    accepted, job = act(follower, ready(follower), 'LOOSEN')
    assert accepted['ok'] and accepted['result']['state'] == 'QUEUED' and accepted['result']['success'] is False
    assert job['state'] == 'SUCCEEDED' and job['success'] and job['completed_commands'] == ['open_gripper']
    assert job['device_result']['status'] == 'reached' and abs(job['device_result']['position'] - OPEN) <= 20
    assert job['object_grasp_verified'] is False
    assert set(goal_writes(bus)) == {6} and len(goal_writes(bus)) >= 10          # 约 1030 ticks，每步最多 100
    assert bus.mem[6][40] == 1 and all(bus.mem[m][40] == 0 for m in range(1, 6))  # 只有夹爪通了电，并保持着


def test_grasp_goes_the_other_way_and_never_claims_an_object(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    _, job = act(follower, ready(follower), 'GRASP')
    assert job['state'] == 'SUCCEEDED' and abs(bus.peek(6, 56) - CLOSED) <= 20
    assert job['object_grasp_verified'] is False


def test_a_jaw_that_stops_short_is_a_failure_and_is_not_pushed_harder(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path, blocked={6: 2700.})       # 张到 2700 就被挡住
    _, job = act(follower, ready(follower), 'LOOSEN')
    assert job['state'] == 'FAILED' and job['error_code'] == 'TARGET_NOT_REACHED'
    assert job['message'] == '打开夹爪失败，请稍后重试' and job['device_result']['status'] == 'settled_short'
    assert job['stop']['hold_confirmed'] and bus.mem[6][40] == 1                 # 仍然通电保持，没有卸力
    assert 0 < bus.peek(6, 42) - 2700 <= 100                 # 目标留在最后那一个子操作上，没有再往前加
    assert len(goal_writes(bus, 6)) == 4                     # 上电 1 次 + 3 个子操作，之后没有任何追加


def test_taught_gripper_endpoint_overrides_the_default(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'gripper_open': 2800}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path)
    _, job = act(follower, ready(follower), 'LOOSEN')
    assert job['state'] == 'SUCCEEDED' and abs(bus.peek(6, 56) - 2800) <= 20 and 'taught' in job['detail']


# ══════════ 幂等 ══════════

def test_same_request_id_is_not_executed_twice(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    _, first = act(follower, epoch, 'LOOSEN', request_id='open-001')
    writes = len(bus.writes)
    accepted, again = act(follower, epoch, 'LOOSEN', request_id='open-001')
    assert accepted['result']['state'] == 'SUCCEEDED' and again == first and len(bus.writes) == writes
    other, _ = act(follower, epoch, 'GRASP', request_id='open-001')              # 同一个 ID 换了内容
    assert other['error']['code'] == 'IDEMPOTENCY_CONFLICT' and len(bus.writes) == writes


# ══════════ 没有实现或没有配置的动作：明确失败，一步不走 ══════════

def test_unreachable_and_unconfigured_actions_fail_before_any_motion(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    pose = dict.fromkeys(('x', 'y', 'z', 'roll', 'pitch', 'yaw'), 0.0)
    expected = [('ARM_MOVETO', {'target_position': pose, 'speed': .1}, 'IK_POSE_NOT_CONVERGED'),   # 全零位姿到不了
                ('ARM_MOVETO', {'target_position': {**pose, 'x': .9}}, 'UNREACHABLE'),             # 比臂展还远
                ('ARM_MOVETO', {'target_position': {'x': .2, 'y': 0, 'z': .2}}, 'INVALID_ARGUMENT'),   # 缺姿态
                ('MOVE_GRASP', {'distance': .9, 'direction': 'up', 'side': 'right'}, 'UNREACHABLE'),
                ('MOVE_GRASP', {'distance': .01, 'direction': 'sideways', 'side': 'right'}, 'INVALID_ARGUMENT'),
                ('ARM_RELEASE', None, 'POSE_NOT_CONFIGURED'),
                ('HOLD_ARM', None, 'POSE_NOT_CONFIGURED'),
                ('HOLD_GRASP', None, 'POSE_NOT_CONFIGURED'),       # 缺胸前位：直接拒绝，不会先闭爪
                ('ARM_RELEASE', {}, 'TARGET_NOT_CONFIGURED'),      # side 默认 both，这台臂只有 right
                ('LOOSEN', {'side': 'left'}, 'TARGET_NOT_CONFIGURED'),
                ('LOOSEN', {'side': 'right', 'force': 9}, 'INVALID_ARGUMENT'),
                ('MOVE_GRASP', {'distance': 5, 'side': 'right'}, 'INVALID_ARGUMENT')]
    for action_id, parameters, code in expected:
        _, job = act(follower, epoch, action_id, parameters)
        assert (job['state'], job['error_code']) == ('FAILED', code), (action_id, job)
        assert job['completed_commands'] == []
    unknown = ask(follower, 'execute', {'epoch': epoch, 'action_id': 'DANCE', 'actionType': 'ExecutionAction',
                                        'actionCategory': 'BuildinAction', 'part_id': 2, 'parameters': {}})
    assert unknown['error']['code'] == 'UNKNOWN_ACTION'
    assert bus.writes == []


# ══════════ 位姿运动：ARM_MOVETO / MOVE_GRASP ══════════

def arm_ticks(bus):
    return {name: bus.peek(JOINTS[name].id, 56) for name in STAND}


def test_arm_moveto_solves_the_pose_and_moves_all_joints_together(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    model = follower.kinematics()
    tool = ask(follower, 'inspect', {})['result']['native']['tool']
    assert tool == model.pose({name: FOLDED[name] for name in STAND})            # 反馈里带当前末端位姿
    goal = model.pose(STAND)                                                     # 一个肯定到得了的位姿
    pose = dict(zip(('x', 'y', 'z', 'roll', 'pitch', 'yaw'), goal['position_m'] + goal['orientation_rpy']))
    _, job = act(follower, epoch, 'ARM_MOVETO', {'target_position': pose})
    assert job['state'] == 'SUCCEEDED' and job['completed_commands'] == ['move_to']
    result = job['device_result']
    assert result['status'] == 'reached' and result['position_only'] is False
    assert result['position_error_mm'] <= 5 and result['orientation_error_rad'] <= .01
    off, twist = model.error(arm_ticks(bus), goal['position_m'], goal['orientation_rpy'])   # 用模拟舵机的真实位置再核一遍
    assert off <= .005 and twist <= .01
    assert all(bus.mem[m][40] == 1 for m in range(1, 6)) and bus.mem[6][40] == 0    # 五个臂关节保持，夹爪没碰
    first_round = goal_writes(bus)[5:8]                  # 上电之后的头几次目标写入
    assert len(set(first_round)) == 3                    # 是几个不同的关节，而不是一个关节走完再走下一个
    again_writes = len(bus.writes)
    _, again = act(follower, epoch, 'ARM_MOVETO', {'target_position': pose})
    assert again['state'] == 'SUCCEEDED' and len(bus.writes) == again_writes     # 已经在目标上：不再发运动


def test_move_grasp_translates_the_tool_along_base_axes(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path, pose={**STAND, 'gripper': 2429})
    epoch = ready(follower)
    model = follower.kinematics()
    for direction, axis, sign in (('up', 2, 1), ('forward', 0, 1), ('left', 1, 1), ('right', 1, -1)):
        before = model.fk(arm_ticks(bus))[0]
        _, job = act(follower, epoch, 'MOVE_GRASP', {'direction': direction, 'distance': .02, 'side': 'right'})
        assert job['state'] == 'SUCCEEDED' and job['device_result']['position_only'] is True, job
        moved = model.fk(arm_ticks(bus))[0] - before
        assert abs(moved[axis] - sign * .02) <= .003 and all(abs(moved[i]) <= .003 for i in range(3) if i != axis)
    assert bus.peek(JOINTS['wrist_roll'].id, 56) == STAND['wrist_roll']          # 腕旋全程没动


def test_pose_motion_refuses_a_path_that_dips_below_the_base(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path, pose={**STAND, 'gripper': 2429})
    _, job = act(follower, ready(follower), 'MOVE_GRASP', {'direction': 'down', 'distance': .25, 'side': 'right'})
    assert job['state'] == 'FAILED' and job['error_code'] == 'PATH_BELOW_BASE' and bus.writes == []


def test_pose_motion_reports_a_miss_instead_of_claiming_success(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path, pose={**STAND, 'gripper': 2429}, blocked={2: 1380.})
    _, job = act(follower, ready(follower), 'MOVE_GRASP', {'direction': 'down', 'distance': .04, 'side': 'right'})
    assert job['state'] == 'FAILED' and job['error_code'] == 'TARGET_NOT_REACHED'    # 肩走了 20 ticks 就被挡住，末端没到
    assert job['device_result']['status'] == 'settled_short' and job['device_result']['position_error_mm'] > 5
    assert job['stop']['hold_confirmed']
    trim = job['device_result']['trim_ticks']                    # 补偿试过了，但每个关节最多多给 TRIM_MAX
    assert trim['shoulder_lift'] == seeed_ros.TRIM_MAX and all(abs(v) <= seeed_ros.TRIM_MAX for v in trim.values())


def test_pose_motion_trims_out_the_lag_of_a_loaded_joint(clock, monkeypatch, tmp_path):
    """真机 2026-10-12：肘关节带着负载，每次都停在离目标约 30 格的地方，末端因此差 1cm。"""
    lagging = dict(pose={**STAND, 'gripper': 2429}, drag={3: 30})
    up = {'direction': 'up', 'distance': .03, 'side': 'right'}
    rounds = seeed_ros.TRIM_ROUNDS
    monkeypatch.setattr(seeed_ros, 'TRIM_ROUNDS', 0)             # 不补偿：重现真机上的失败
    follower, bus = make(clock, monkeypatch, tmp_path / 'a', **lagging)
    _, job = act(follower, ready(follower), 'MOVE_GRASP', up)
    assert job['error_code'] == 'TARGET_NOT_REACHED' and job['device_result']['position_error_mm'] > 5
    monkeypatch.setattr(seeed_ros, 'TRIM_ROUNDS', rounds)        # 补偿：同样的舵机、同样的动作
    follower, bus = make(clock, monkeypatch, tmp_path / 'b', **lagging)
    model = follower.kinematics()
    before = model.fk(arm_ticks(bus))[0]
    _, job = act(follower, ready(follower), 'MOVE_GRASP', up)
    result = job['device_result']
    assert job['state'] == 'SUCCEEDED' and result['position_error_mm'] <= 5, job
    assert list(result['trim_ticks']) == ['elbow_flex'] and 20 <= abs(result['trim_ticks']['elbow_flex']) <= 40
    assert abs(model.fk(arm_ticks(bus))[0][2] - before[2] - .03) <= .005          # 末端真的上去了 3cm
    assert bus.peek(3, 48) == JOINTS['elbow_flex'].torque_cap                     # 扭矩上限寄存器没有被动过


def test_preset_motion_trims_too(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path, drag={2: 20, 3: 30})
    _, job = act(follower, ready(follower), 'HOLD_ARM')
    assert job['state'] == 'SUCCEEDED', job
    assert all(abs(r) <= 8 for r in job['device_result']['residual_ticks'].values())
    assert set(job['device_result']['trim_ticks']) == {'shoulder_lift', 'elbow_flex'}


def test_arm_moveto_speed_maps_to_a_bounded_joint_speed():
    assert [seeed_ros.joint_speed(v) for v in (.1, .5, 2.0)] == [100, 200, 400]


# ══════════ 预设位：ARM_RELEASE / HOLD_ARM / HOLD_GRASP ══════════

def test_hold_arm_goes_to_the_taught_chest_pose_in_route_order(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path)
    _, job = act(follower, ready(follower), 'HOLD_ARM')
    assert job['state'] == 'SUCCEEDED' and job['completed_commands'] == ['hold_arm']
    assert all(abs(bus.peek(JOINTS[name].id, 56) - ticks) <= 30 for name, ticks in STAND.items())
    assert all(bus.mem[m][40] == 1 for m in range(1, 6)) and bus.mem[6][40] == 0     # 五个臂关节保持，夹爪没碰
    moves = goal_writes(bus)[5:]                     # 前 5 次目标写入是五个关节原地上电
    assert moves[0] == 2 and moves[-1] == 4          # 肘要展开：肩先走；腕要低头：最后走
    assert job['device_result']['preset'] == 'chest' and job['device_result']['torque_retained']


def test_arm_release_goes_to_park_without_touching_the_gripper(clock, monkeypatch, tmp_path):
    park = {name: FOLDED[name] for name in STAND}
    park.update(shoulder_lift=910, elbow_flex=3120)              # 折叠位，夹进限位余量内
    (tmp_path / 'config.json').write_text(json.dumps({'park': park}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path, pose={**STAND, 'gripper': 2429})
    _, job = act(follower, ready(follower), 'ARM_RELEASE', {'side': 'right'})
    assert job['state'] == 'SUCCEEDED' and 6 not in goal_writes(bus)
    assert all(abs(bus.peek(JOINTS[name].id, 56) - ticks) <= 30 for name, ticks in park.items())
    assert all(bus.mem[m][40] == 1 for m in range(1, 6))         # 放下之后仍然通电，不是卸力


def test_hold_grasp_closes_first_then_lifts(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path)
    _, job = act(follower, ready(follower), 'HOLD_GRASP')
    assert job['state'] == 'SUCCEEDED' and job['completed_commands'] == ['close_gripper', 'hold_arm']
    order = goal_writes(bus)
    assert order[0] == 6 and order.index(2) > max(i for i, m in enumerate(order) if m == 6)   # 夹爪全部走完才动臂


def test_teach_records_the_current_pose(clock, monkeypatch, tmp_path):
    follower, _ = make(clock, monkeypatch, tmp_path)
    seeed_ros.teach(follower.arm, tmp_path, 'park')
    seeed_ros.teach(follower.arm, tmp_path, 'gripper_closed')
    park = {name: FOLDED[name] for name in STAND}
    park.update(shoulder_lift=JOINTS['shoulder_lift'].lo + MARGIN, elbow_flex=JOINTS['elbow_flex'].hi - MARGIN)
    assert follower.config() == {'park': park, 'gripper_closed': 2429}    # 折到底的肩和肘夹回限位余量之内
    _, job = act(follower, ready(follower), 'ARM_RELEASE', {'side': 'right'})
    assert job['state'] == 'SUCCEEDED'                                    # 所以这样记下来的预设位是走得到的


# ══════════ 停止、租约到期、故障 ══════════

def test_stop_cancels_remaining_steps_and_holds(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    started = clock.t
    clock.hook = lambda t: t - started > 1.5 and follower.cancel.set()        # 动作开始 1.5 秒后有人叫停
    _, job = act(follower, epoch, 'HOLD_ARM')
    clock.hook = None
    assert job['state'] == 'FAILED' and job['error_code'] == 'STOPPED' and job['stop']['hold_confirmed']
    assert bus.peek(3, 56) > 3000                                # 肘还没来得及动：后面的分段都取消了
    assert all(bus.mem[m][40] == 1 for m in range(1, 6))         # 没有卸力
    assert all(abs(bus.peek(m, 42) - bus.peek(m, 56)) <= 8 for m in range(1, 6))    # 目标都收回到了当前位置
    reply = ask(follower, 'stop', {'epoch': epoch})['result']
    assert reply['state'] == 'STOPPED' and reply['hold_confirmed'] and reply['phase'] == 'HOLDING'
    assert ask(follower, 'stop', {'epoch': epoch + 1})['error']['code'] == 'STALE_CONTROL'


def test_action_stops_when_heartbeats_stop(clock, monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'chest': STAND}), encoding='utf-8')
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    monkeypatch.setattr(seeed_ros, 'LEASE_ACTIVE', 2)            # 动作期间租约 2 秒，而测试里没有人发心跳
    _, job = act(follower, epoch, 'HOLD_ARM')
    assert job['state'] == 'FAILED' and job['error_code'] == 'LEASE_EXPIRED' and job['stop']['hold_confirmed']
    assert bus.peek(3, 56) > 3000 and all(bus.mem[m][40] == 1 for m in range(1, 6))


def test_overheated_joint_blocks_motion_and_shows_in_feedback(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path, temps=lambda t: 56)
    native = ask(follower, 'inspect', {})['result']['native']
    assert native['fault']['code'] == 'TEMPERATURE' and native['native_control_available'] is False
    session = native['session_id']
    epoch = ask(follower, 'bind', {'device_id': DEVICE_ID, 'controller_session_id': session,
                                   'adopt_existing_session': True})['result']['epoch']
    confirm = ask(follower, 'confirm_workspace', {'epoch': epoch, 'secured': True, 'clear': True})
    assert confirm['error']['code'] == 'FAULT_LATCHED' and bus.writes == []      # 现场确认绕不过故障


# ══════════ 重启 ══════════

def test_restart_invalidates_the_epoch_and_never_replays(clock, monkeypatch, tmp_path):
    follower, bus = make(clock, monkeypatch, tmp_path)
    epoch = ready(follower)
    _, done = act(follower, epoch, 'LOOSEN', request_id='open-before-restart')
    journal = json.loads((tmp_path / 'journal.json').read_text(encoding='utf-8'))
    journal['jobs']['half-done'] = {'request_id': 'half-done', 'action_id': 'GRASP', 'state': 'RUNNING',
                                    'success': False}
    (tmp_path / 'journal.json').write_text(json.dumps(journal), encoding='utf-8')
    writes = len(bus.writes)

    again = Follower(follower.arm, tmp_path, background=False)
    assert again.epoch > epoch and again.session_id != follower.session_id
    assert ask(again, 'heartbeat', {'epoch': epoch})['error']['code'] == 'STALE_CONTROL'
    assert ask(again, 'job', {'request_id': 'open-before-restart'})['result'] == done
    assert ask(again, 'job', {'request_id': 'half-done'})['result']['state'] == 'INTERRUPTED'
    assert len(bus.writes) == writes                             # 重启本身不发任何写指令


# ══════════ 后台线程模式（真实时间） ══════════

class RealClock:
    """让模拟总线按真实时间走，用来测带后台线程的正常运行方式。"""
    t = property(lambda self: time.monotonic(), lambda self, value: None)


def test_threaded_action_reports_progress_refuses_a_second_one_and_stops(monkeypatch, tmp_path):
    (tmp_path / 'config.json').write_text(json.dumps({'gripper_open': 2429 + 600}), encoding='utf-8')   # 约 3 秒的行程
    bus = SimBus(RealClock())
    follower = Follower(connect(monkeypatch, None, bus), tmp_path)      # 采样、看门狗、执行线程都开着
    try:
        epoch = ready(follower)
        execute = {'epoch': epoch, 'action_id': 'LOOSEN', 'actionType': 'ExecutionAction',
                   'actionCategory': 'BuildinAction', 'part_id': 2, 'parameters': {'side': 'right'}}
        assert ask(follower, 'execute', execute, 'threaded-1')['result']['state'] == 'QUEUED'    # 立即返回受理回执
        time.sleep(.8)
        state = ask(follower, 'inspect', {})['result']
        assert state['active_request_id'] == 'threaded-1' and state['native']['phase'] == 'MOVING'
        assert state['lease_remaining_s'] <= 5                               # 动作期间租约是 5 秒
        busy = ask(follower, 'execute', {**execute, 'action_id': 'GRASP'}, 'threaded-2')
        assert busy['error']['code'] == 'BUSY'                               # 动作不排队
        stopped = ask(follower, 'stop', {'epoch': epoch})['result']
        assert stopped['state'] == 'STOPPED' and stopped['hold_confirmed'] and stopped['phase'] == 'HOLDING'
        job = ask(follower, 'job', {'request_id': 'threaded-1'})['result']
        assert job['state'] == 'FAILED' and job['error_code'] == 'STOPPED'
        assert bus.peek(6, 56) < 2429 + 600 - 100                            # 确实是半路停下的
        assert follower.pop_updates() == [('threaded-1', job, True)] and follower.pop_updates() == []
    finally:
        follower.close()


# ══════════ ROS 收发层（桩） ══════════

def test_ros_node_wiring_with_stub_rclpy(monkeypatch, tmp_path):
    """没有真的 ROS：用桩模块顶替 rclpy / std_msgs，检查只订阅一个命令 Topic、回执都发到一个结果 Topic。"""
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)
    published, callbacks, spins = [], {}, itertools.count()

    class String:
        data = ''

    class Node:
        def create_publisher(self, kind, topic, qos):
            return types.SimpleNamespace(publish=lambda message: published.append((topic, json.loads(message.data))))

        def create_subscription(self, kind, topic, callback, qos):
            callbacks[topic] = (callback, qos)

        def destroy_node(self):
            pass

    def command(action, params, **extra):
        message = String()
        message.data = json.dumps({'action': action, 'params': {'request_id': 'ros-' + action, **params}, **extra})
        callbacks['/so101/command'][0](message)

    def spin_once(node, timeout_sec):
        n = next(spins)
        threading.Event().wait(.02)
        if n == 1:                                               # 第二圈：平台发来一条查询
            command('ARM_INSPECT', {})
        if n == 3:                                               # 缺姿态的 ARM_MOVETO：拒绝
            command('ARM_MOVETO', {'target_position': {'x': .2, 'y': 0, 'z': .2}})
        if n == 5:                                               # 多出来的字段：忽略，照常回
            command('ARM_CATALOG', {}, requestId='old', id=2)

    rclpy = types.SimpleNamespace(init=lambda: None, shutdown=lambda: None, create_node=lambda name, namespace: Node(),
                                  spin_once=spin_once, ok=lambda: next(itertools.count(next(spins))) < 40)
    qos = types.SimpleNamespace(QoSProfile=lambda **kw: kw, ReliabilityPolicy=types.SimpleNamespace(RELIABLE='R'),
                                DurabilityPolicy=types.SimpleNamespace(VOLATILE='V'),
                                HistoryPolicy=types.SimpleNamespace(KEEP_LAST='K'))
    monkeypatch.setitem(sys.modules, 'rclpy', rclpy)
    monkeypatch.setitem(sys.modules, 'rclpy.qos', qos)
    monkeypatch.setitem(sys.modules, 'std_msgs', types.ModuleType('std_msgs'))
    monkeypatch.setitem(sys.modules, 'std_msgs.msg', types.SimpleNamespace(String=String))

    bus_clock = Clock()                                          # 这里用真实时间跑节点循环，只有模拟总线用假时钟
    follower = Follower(connect(monkeypatch, bus_clock, SimBus(bus_clock)), tmp_path, background=False)
    seeed_ros.run_node(follower)

    assert set(callbacks) == {'/so101/command', '/so101/action'}
    assert callbacks['/so101/command'][1] == {'depth': 16, 'reliability': 'R', 'durability': 'V', 'history': 'K'}
    assert {topic for topic, _ in published} == {'/so101/result', '/so101/state'}
    pushed = [value for topic, value in published if topic == '/so101/state']
    assert pushed and all(value['action'] == 'ARM_STATE' and value['params']['status'] == 'SUCCEEDED' for value in pushed)
    seqs = [value['params']['result']['native']['sample_seq'] for value in pushed]
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)  # 每次采样推一条，编号递增不重复
    assert 'request_id' not in pushed[0]['params']               # 主动推送，不是哪条命令的回执
    replies = {value['action']: value for topic, value in published if topic == '/so101/result'}
    inspect = replies['ARM_INSPECT']
    assert set(inspect) == {'action', 'params'} and inspect['params']['status'] == 'SUCCEEDED'
    assert inspect['params']['request_id'] == 'ros-ARM_INSPECT'
    assert inspect['params']['result']['device_id'] == DEVICE_ID
    rejected = replies['ARM_MOVETO']
    assert rejected['params']['status'] == 'FAILED' and rejected['params']['error']['code'] == 'INVALID_ARGUMENT'
    assert replies['ARM_CATALOG']['params']['status'] == 'SUCCEEDED' and set(replies['ARM_CATALOG']) == {'action', 'params'}


def _stub_ros(monkeypatch, subscribers, spins):
    """桩 rclpy：/so101/command 上的订阅者就是 subscribers（节点名列表），rclpy.ok() 只放行 spins 圈。"""
    published, destroyed, counter = [], [], itertools.count()

    class String:
        data = ''

    class Node:
        def create_publisher(self, kind, topic, qos):
            return types.SimpleNamespace(publish=lambda message: published.append(topic))

        def create_subscription(self, kind, topic, callback, qos):
            return object()

        def get_subscriptions_info_by_topic(self, topic):
            assert topic == '/so101/command'
            return [types.SimpleNamespace(node_name=name, node_namespace='/x') for name in subscribers]

        def destroy_node(self):
            destroyed.append(True)

    rclpy = types.SimpleNamespace(init=lambda: None, shutdown=lambda: None, create_node=lambda name, namespace: Node(),
                                  spin_once=lambda node, timeout_sec: None, ok=lambda: next(counter) < spins)
    qos = types.SimpleNamespace(QoSProfile=lambda **kw: kw, ReliabilityPolicy=types.SimpleNamespace(RELIABLE='R'),
                                DurabilityPolicy=types.SimpleNamespace(VOLATILE='V'),
                                HistoryPolicy=types.SimpleNamespace(KEEP_LAST='K'))
    monkeypatch.setitem(sys.modules, 'rclpy', rclpy)
    monkeypatch.setitem(sys.modules, 'rclpy.qos', qos)
    monkeypatch.setitem(sys.modules, 'std_msgs', types.ModuleType('std_msgs'))
    monkeypatch.setitem(sys.modules, 'std_msgs.msg', types.SimpleNamespace(String=String))
    return published, destroyed


def test_ros_node_refuses_a_second_follower_on_the_command_topic(monkeypatch, tmp_path, capsys):
    """/so101/command 上已有另一个从臂节点（so101-controller 或 Isaac 里的仿真节点）：不起第二个，退出码 3。"""
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)
    published, destroyed = _stub_ros(monkeypatch, ['seeed_follower_actions', 'seeed_follower_actions'], spins=5)
    bus_clock = Clock()
    follower = Follower(connect(monkeypatch, bus_clock, SimBus(bus_clock)), tmp_path, background=False)
    with pytest.raises(SystemExit) as stopped:
        seeed_ros.run_node(follower)
    assert stopped.value.code == seeed_ros.DUPLICATE_EXIT == 3
    assert published == [] and destroyed == [True] and follower.on_sample is None
    assert '已有另一个从臂节点' in capsys.readouterr().out


def test_ros_node_ignores_non_follower_subscribers(monkeypatch, tmp_path, capsys):
    """ros2 topic echo 之类的订阅者不算从臂节点，照常启动。"""
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)
    monkeypatch.setattr(seeed_ros, 'COMMAND_GUARD_S', 0)
    published, destroyed = _stub_ros(monkeypatch, ['seeed_follower_actions', '_ros2cli_4242'], spins=3)
    bus_clock = Clock()
    follower = Follower(connect(monkeypatch, bus_clock, SimBus(bus_clock)), tmp_path, background=False)
    seeed_ros.run_node(follower)
    assert destroyed == [True] and 'ROS 节点已启动' in capsys.readouterr().out


def test_ros_node_refuses_leased_mode(monkeypatch, tmp_path):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', False)
    with pytest.raises(SystemExit, match='直连模式'):
        seeed_ros.run_node(None)


# ══════════ local 模式（终端里走同一套协议） ══════════

def test_local_console_runs_the_documented_sequence(monkeypatch, tmp_path, capsys):
    (tmp_path / 'config.json').write_text(json.dumps({'gripper_open': 2429 + 120}), encoding='utf-8')
    bus = SimBus(RealClock())
    follower = Follower(connect(monkeypatch, None, bus), tmp_path)
    typed = iter(['inspect', 'LOOSEN', 'bind', 'confirm', '', 'LOOSEN', 'ARM_MOVETO 0 0 0 0 0 0', 'ARM_MOVETO 1 2',
                  'release', 'q'])
    monkeypatch.setattr('builtins.input', lambda prompt='': next(typed))
    try:
        seeed_ros.local_console(follower)
    finally:
        follower.close()
    out = capsys.readouterr().out
    assert out.count('"STALE_CONTROL"') == 1                     # 绑定之前的那次 LOOSEN 被拒绝
    assert '"SUCCEEDED"' in out and '"IK_POSE_NOT_CONVERGED"' in out and '"released": true' in out
    assert '参数不对' in out                                      # 少给了数，没有发出请求
    assert abs(bus.peek(6, 56) - (2429 + 120)) <= 20 and follower.bound is False
