"""Isaac 仿真从臂后端（sim_arm.IsaacArm）：七个原动作不改代码，在仿真从臂上执行。用假 Isaac（同样的文件协议），真实时间。"""
import json
import time

import pytest

import seeed_arm
import seeed_ros
import sim_arm
from fake_isaac import FakeIsaac
from seeed_arm import JOINTS, MARGIN, ArmError
from seeed_ros import Follower
from seeed_ros import CommandTopic
from sim_arm import IsaacArm, IsaacBusy, clamp_presets, to_ticks, to_units
from test_seeed_ros import STAND, ask

START = {**STAND, 'gripper': 2429}
CHEST = dict(STAND)
PARK = {'shoulder_pan': 2200, 'shoulder_lift': 1500, 'elbow_flex': 2400, 'wrist_flex': 2900, 'wrist_roll': 1200}


def units(pose):
    return {name + '.pos': to_units(name, ticks) for name, ticks in pose.items()}


@pytest.fixture
def isaac(tmp_path, monkeypatch):
    monkeypatch.setattr(sim_arm, 'LOOP_S', .02)
    monkeypatch.setattr(sim_arm, 'IDLE_RELEASE_S', .5)
    fake = FakeIsaac(tmp_path / 'isaac-console', units(START)).start()
    yield fake
    fake.stop()


def arm_of(fake):
    return IsaacArm(fake.ipc, speed_scale=10)


def until(check, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if check():
            return True
        time.sleep(.01)
    return False


# ══════════ 1. 换算、只读 ══════════

def test_units_match_lerobot_formula_and_round_trip():
    for name, j in JOINTS.items():
        for ticks in (j.lo + MARGIN, (j.lo + j.hi) // 2, j.hi - MARGIN):
            assert to_units(name, ticks) == pytest.approx(seeed_arm.lerobot_value(name, ticks))
            assert to_ticks(name, to_units(name, ticks)) == pytest.approx(ticks)


def test_reading_an_idle_sim_arm_never_takes_the_lease(isaac):
    arm = arm_of(isaac)
    positions = arm.positions()
    assert all(abs(positions[n] - START[n]) <= 1 for n in JOINTS)
    assert arm.read(JOINTS['gripper'].id, seeed_arm.PRESENT_TEMPERATURE) == 30
    assert arm.read(JOINTS['shoulder_pan'].id, seeed_ros.PRESENT_VOLTAGE) == 120
    assert not arm.torque_states()['shoulder_pan']
    assert not isaac.ops                                         # 只读：一个请求都没发，不占租约


# ══════════ 2. 拿租约、限速运动、交还租约 ══════════

def test_move_takes_the_lease_moves_slowly_then_gives_it_back(isaac):
    arm = arm_of(isaac)
    arm.torque_on(['shoulder_pan'])
    assert isaac.owner == arm.client
    start = arm.position('shoulder_pan')
    arm.move('shoulder_pan', 150)
    assert until(lambda: abs(arm.position('shoulder_pan') - (start + 150)) <= 3)
    # 目标是一点点推过去的：相邻两次发出的目标，差不超过 速度×周期（再加一格取整）
    pan = [to_ticks('shoulder_pan', sent['shoulder_pan.pos']) for sent in isaac.sent]
    step = seeed_arm.DEFAULT_SPEED * 10 * sim_arm.LOOP_S + 1
    assert len(pan) > 3 and all(abs(b - a) <= step for a, b in zip(pan, pan[1:]))
    assert until(lambda: isaac.owner is None)                   # 空闲后交还租约，仿真采集可以用了
    assert ('close', arm.client) in isaac.ops and isaac.expired == 0
    assert arm.torque_states()['shoulder_pan']                  # 通电标记保留，下次动作直接重新连上
    arm.move('shoulder_pan', -100)
    assert until(lambda: abs(arm.position('shoulder_pan') - (start + 50)) <= 3)
    arm.close()
    assert isaac.owner is None


def test_speed_zero_means_no_limit_like_the_real_servo(isaac):
    """跟随主臂（2026-11-05）：上电 limits=False 不限速，目标一步就发到 Isaac，不再一点点推。"""
    arm = arm_of(isaac)
    arm.torque_on(['shoulder_pan'], limits=False)
    start = arm.position('shoulder_pan')
    sent_before = len(isaac.sent)
    arm.write(JOINTS['shoulder_pan'].id, seeed_arm.GOAL_POSITION, start + 400)
    assert until(lambda: any(abs(to_ticks('shoulder_pan', sent['shoulder_pan.pos']) - (start + 400)) <= 1
                             for sent in isaac.sent[sent_before:]))
    pan = [to_ticks('shoulder_pan', sent['shoulder_pan.pos']) for sent in isaac.sent[sent_before:]]
    # 发出的目标只有两种：上电时的保持位置，和一步到位的最终目标（中间没有一点点推过去的过渡值）
    assert all(abs(p - start) <= 1 or abs(p - (start + 400)) <= 1 for p in pan)
    with pytest.raises(ArmError, match='负数'):
        arm.write(JOINTS['shoulder_pan'].id, seeed_arm.GOAL_VELOCITY, -1)
    arm.write(JOINTS['shoulder_pan'].id, seeed_arm.GOAL_VELOCITY, seeed_arm.DEFAULT_SPEED)   # 原动作照旧限速
    assert arm.speed['shoulder_pan'] == seeed_arm.DEFAULT_SPEED
    arm.close()


def test_busy_lease_is_reported_and_never_stolen(isaac):
    isaac.lease_to('collection-container')
    arm = arm_of(isaac)
    with pytest.raises(IsaacBusy, match='正被别的程序控制'):
        arm.torque_on()
    assert isaac.owner == 'collection-container' and not isaac.sent


def test_targets_outside_the_sim_range_are_clamped_not_rejected(isaac):
    arm = arm_of(isaac)
    arm.torque_on(['shoulder_lift'])
    here = arm.position('shoulder_lift')
    lowest = JOINTS['shoulder_lift'].lo + MARGIN                 # 真机限位余量内，但仿真范围（-100°）外
    assert to_units('shoulder_lift', lowest) < -100
    for _ in range(4):                                          # 仿真在 -100° 处夹住，到不了 lowest：只走有限几步
        delta = min(180, here - lowest)
        if delta < seeed_arm.MIN_STEP:
            break
        arm.move('shoulder_lift', -delta)
        time.sleep(.3)
        here = arm.position('shoulder_lift')
    assert to_units('shoulder_lift', here) == pytest.approx(-100, abs=.5)    # 停在仿真范围的边上
    assert until(lambda: isaac.sent and isaac.sent[-1]['shoulder_lift.pos'] == -100.0)
    assert arm.error is None                                    # Isaac 没有拒绝过任何目标
    arm.close()


def test_same_checks_as_the_real_arm_before_any_request(isaac):
    arm = arm_of(isaac)
    with pytest.raises(ArmError, match='未通电'):
        arm.move('elbow_flex', 50)
    arm.torque_on(['elbow_flex'])
    with pytest.raises(ArmError, match='单次幅度'):
        arm.move('elbow_flex', 400)
    arm.close()


def test_restarted_isaac_is_detected(isaac):
    arm = arm_of(isaac)
    isaac.run = 'run-2'
    time.sleep(.3)
    with pytest.raises(ArmError, match='重启'):
        arm.positions()


def test_lost_lease_is_reported_once_then_reconnects(isaac):
    arm = arm_of(isaac)
    arm.torque_on(['wrist_roll'])
    isaac.lease_to('someone-else')                              # 租约被别人拿走（例如 Isaac 端过期后被抢）
    assert until(lambda: arm.error is not None)
    with pytest.raises(ArmError, match='连接断了'):
        arm.move('wrist_roll', 60)
    isaac.owner = None
    arm.move('wrist_roll', 60)
    assert isaac.owner == arm.client
    arm.close()


# ══════════ 3. 七个原动作在仿真从臂上执行 ══════════

@pytest.fixture
def follower(isaac, tmp_path, monkeypatch):
    monkeypatch.setattr(seeed_ros, 'DIRECT_CONTROL', True)
    monkeypatch.setattr(seeed_ros, 'POLL_S', .01)
    monkeypatch.setattr(seeed_ros, 'SETTLE_STABLE_S', .1)
    data = tmp_path / 'data'
    data.mkdir()
    (data / 'config.json').write_text(json.dumps({
        'chest': CHEST, 'park': PARK, 'gripper_open': JOINTS['gripper'].hi - MARGIN - 50,
        'gripper_closed': JOINTS['gripper'].lo + MARGIN + 50}))
    value = Follower(arm_of(isaac), data, background=False)
    yield value
    value.close()


def execute(follower, action, parameters, rid):
    payload = {'action_id': action, 'actionType': 'ExecutionAction', 'actionCategory': 'BuildinAction',
               'part_id': 2, 'parameters': parameters}
    accepted = ask(follower, 'execute', payload, rid)
    assert accepted['ok'], accepted
    return ask(follower, 'job', {'request_id': rid})['result']


def test_seven_primitives_run_on_the_sim_arm(follower, isaac):
    for i, (action, parameters) in enumerate([
            ('ARM_RELEASE', {'side': 'right'}), ('HOLD_ARM', {'side': 'right'}),
            ('LOOSEN', {'side': 'right'}), ('GRASP', {'side': 'right'}), ('HOLD_GRASP', {'side': 'right'}),
            ('MOVE_GRASP', {'side': 'right', 'direction': 'up', 'distance': .01})]):
        job = execute(follower, action, parameters, f'sim-{i}')
        assert job['state'] == 'SUCCEEDED' and job['success'], (action, job)
    measured = {n: to_ticks(n.removesuffix('.pos'), v) for n, v in isaac.measured.items()}
    tool = ask(follower, 'inspect', {})['result']['native']['tool']
    pose = dict(zip(('x', 'y', 'z', 'roll', 'pitch', 'yaw'), tool['position_m'] + tool['orientation_rpy']))
    execute(follower, 'MOVE_GRASP', {'side': 'right', 'direction': 'down', 'distance': .01}, 'sim-down')
    job = execute(follower, 'ARM_MOVETO', {'target_position': pose, 'speed': .1}, 'sim-moveto')
    assert job['success'], job
    assert abs(measured['gripper.pos'] - (JOINTS['gripper'].lo + MARGIN + 50)) <= 25   # HOLD_GRASP 合上了夹爪
    assert all(op != 'send' or client == follower.arm.client for op, client in isaac.ops)
    assert until(lambda: isaac.owner is None)                   # 动作做完，租约交还


def test_state_samples_carry_sequence_time_and_isaac_source(follower, isaac):
    """2026-11-04：每次采样带编号和读数时刻，仿真还带 Isaac 的源头；/so101/state 的推送钩子每采一次调用一次。"""
    pushed = []
    follower.on_sample = pushed.append
    first, second = follower.sample(), follower.sample()
    assert second['seq'] == first['seq'] + 1 and second['sampled_mono_ns'] >= first['sampled_mono_ns']
    assert [s['seq'] for s in pushed] == [first['seq'], second['seq']]
    assert first['source']['kind'] == 'isaac_status' and isinstance(first['source']['time_ns'], int)
    native = follower.state(second)['native']
    assert native['sample_seq'] == second['seq'] and native['sampled_mono_ns'] == second['sampled_mono_ns']
    assert native['source'] == second['source'] and native['connected'] is True
    inspected = ask(follower, 'inspect', {})['result']['native']           # ARM_INSPECT 也带着这些字段
    assert inspected['sample_seq'] >= second['seq'] and inspected['sampled_unix'] > 0
    # 动作期间位置来自实时采样：源头是 Isaac 采到这次读数的时刻
    follower.arm.torque_on(['shoulder_pan'])
    follower.arm.move('shoulder_pan', 20)
    assert until(lambda: follower.sample()['source']['kind'] == 'isaac_sample')
    assert isinstance(follower.snapshot['source']['time_ns'], int)
    follower.on_sample = None


def test_single_command_topic_drives_the_sim_arm(follower, isaac):
    sent = []
    topic = CommandTopic(follower, sent.append)
    for action, params in [('HOLD_ARM', {}), ('MOVE_GRASP', {'direction': 'up', 'distance': .01})]:
        topic.handle(json.dumps({'action': action, 'params': {'request_id': 'sim-' + action, **params}}))
        topic.poll()
    assert sent == [{'action': 'HOLD_ARM', 'params': {'request_id': 'sim-HOLD_ARM', 'status': 'SUCCEEDED'}},
                    {'action': 'MOVE_GRASP', 'params': {'request_id': 'sim-MOVE_GRASP', 'status': 'SUCCEEDED'}}]
    assert until(lambda: isaac.owner is None)                   # 动作做完，租约交还


def test_presets_copied_from_the_real_arm_are_clamped_into_the_sim_range():
    real = {'chest': {'shoulder_pan': 2077, 'shoulder_lift': 900, 'elbow_flex': 3132, 'wrist_flex': 2865, 'wrist_roll': 983},
            'park': {'shoulder_pan': 2039, 'shoulder_lift': 1099, 'elbow_flex': 2920, 'wrist_flex': 2783, 'wrist_roll': 1013},
            'gripper_open': 3146, 'gripper_closed': 3146}
    from fake_isaac import LIMITS
    limits = {k.removesuffix('.pos'): tuple(v) for k, v in LIMITS.items()}
    sim, changed = clamp_presets(real, limits)
    assert set(changed) == {'chest.shoulder_lift', 'chest.elbow_flex'}   # 胸前位：大臂 -103.6° < -100°，肘 95.5° > 90°
    for key in ('chest', 'park'):
        for name, ticks in sim[key].items():
            lo, hi = limits[name]
            assert lo <= to_units(name, ticks) <= hi
            assert JOINTS[name].lo + MARGIN <= ticks <= JOINTS[name].hi - MARGIN
    assert sim['chest']['shoulder_pan'] == 2077 and sim['gripper_open'] == 3146
