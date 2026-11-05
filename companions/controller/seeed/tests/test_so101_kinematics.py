"""so101_kinematics 的检查：纯计算，不接硬件。"""
import math
import random

import numpy as np
import pytest

import so101_kinematics as sk
from joint_route import ARM_JOINTS
from seeed_arm import JOINTS, MARGIN

FOLDED = {'shoulder_pan': 2080, 'shoulder_lift': 880, 'elbow_flex': 3150, 'wrist_flex': 2900, 'wrist_roll': 1125}
STAND = {'shoulder_pan': 2080, 'shoulder_lift': 1360, 'elbow_flex': 2490, 'wrist_flex': 3020, 'wrist_roll': 1125}


@pytest.fixture(scope='module')
def model():
    return sk.Kinematics({name: (JOINTS[name].lo, JOINTS[name].hi) for name in ARM_JOINTS}, MARGIN)


def test_chain_and_mapping(model):
    assert model.names == ARM_JOINTS                                  # 底座 → 工具：正好是五个臂关节
    middle = {name: (JOINTS[name].lo + JOINTS[name].hi) / 2 for name in ARM_JOINTS}
    assert all(abs(a) < 1e-12 for a in model.ticks_to_angles(middle).values())   # 量程中点 = 0 弧度
    assert .5 < model.reach < .6
    for name in ARM_JOINTS:                                           # 求解范围留在舵机限位余量之内
        m = model.mapping[name]
        lo, hi = (m[k] * sk.TICKS_PER_RADIAN + m['zero_ticks'] for k in ('lower', 'upper'))
        assert JOINTS[name].lo + MARGIN <= lo + 1e-6 and hi - 1e-6 <= JOINTS[name].hi - MARGIN


def test_forward_kinematics_reference_values(model):
    """这几个数是用松甲直连版的 arm_mcp/model.py 对同样的关节角算出来的（两边逐点一致到 1e-16）。"""
    assert model.pose(FOLDED) == {'position_m': [0.13728, 0.00977, -0.01247],
                                  'orientation_rpy': [2.80771, 0.03507, 1.69305]}
    assert model.pose(STAND) == {'position_m': [0.16729, 0.01042, 0.10682],
                                 'orientation_rpy': [2.71603, 0.04417, 1.68941]}


def test_rpy_round_trip_and_rotation_distance():
    rng = random.Random(1)
    for _ in range(200):
        rpy = [rng.uniform(-3, 3), rng.uniform(-1.5, 1.5), rng.uniform(-3, 3)]
        R = sk.rpy_matrix(rpy)
        assert np.allclose(R.T @ R, np.eye(3), atol=1e-12)
        assert sk.rotation_distance(sk.rpy_matrix(sk.matrix_rpy(R)), R) < 1e-9
    assert sk.rotation_distance(sk.rpy_matrix([0, 0, .3]), sk.rpy_matrix([0, 0, .1])) == pytest.approx(.2)
    assert sk.rotation_distance(sk.rpy_matrix([0, 0, math.pi]), np.eye(3)) == pytest.approx(math.pi)   # 180° 附近也稳定


def test_pose_ik_recovers_poses_inside_the_joint_limits(model):
    rng = random.Random(7)
    solved = 0
    while solved < 25:
        ticks = {name: rng.randint(JOINTS[name].lo + MARGIN + 1, JOINTS[name].hi - MARGIN - 1) for name in ARM_JOINTS}
        try:
            model.angles_to_ticks(model.ticks_to_angles(ticks))
        except sk.KinematicsError:
            continue                                                  # 取到了 URDF 限位之外，换一个
        position, R = model.fk(ticks)
        solution = model.solve_pose([float(v) for v in position], sk.matrix_rpy(R), FOLDED)
        # 解不一定是原来那组关节角，但换算成整数 ticks 之后必须同时满足两个容差，并且在限位余量之内
        assert solution['position_error_m'] <= .003 and solution['orientation_error_rad'] <= .01
        off, twist = model.error(solution['ticks'], position, sk.matrix_rpy(R))
        assert off <= .003 and twist <= .01
        assert all(JOINTS[n].lo + MARGIN <= solution['ticks'][n] <= JOINTS[n].hi - MARGIN for n in ARM_JOINTS)
        assert all(type(v) is int for v in solution['ticks'].values())
        solved += 1


def test_pose_ik_fails_loudly_instead_of_dropping_orientation(model):
    with pytest.raises(sk.KinematicsError) as far:
        model.solve_pose([.9, 0, .2], [0, 0, 0], FOLDED)
    assert far.value.code == 'UNREACHABLE'
    goal = model.pose(STAND)
    twisted = [goal['orientation_rpy'][0] + 1.0, *goal['orientation_rpy'][1:]]    # 位置到得了，这个姿态到不了
    with pytest.raises(sk.KinematicsError) as stuck:
        model.solve_pose(goal['position_m'], twisted, FOLDED)
    assert stuck.value.code == 'IK_POSE_NOT_CONVERGED'
    with pytest.raises(sk.KinematicsError) as bad:
        model.solve_pose([0, float('nan'), 0], [0, 0, 0], FOLDED)
    assert bad.value.code == 'INVALID_INPUT'


def test_position_ik_keeps_the_wrist_roll_where_it_is(model):
    here, _ = model.fk(STAND)
    for axis, step in ((2, .02), (0, .03), (1, -.02)):
        target = [float(v) for v in here]
        target[axis] += step
        solution = model.solve_position(target, STAND)
        assert solution['ticks']['wrist_roll'] == STAND['wrist_roll']
        assert solution['position_error_m'] <= .003
        reached, _ = model.fk(solution['ticks'])
        assert abs(reached[axis] - here[axis] - step) <= .003
        assert max(abs(solution['ticks'][n] - STAND[n]) for n in ARM_JOINTS) <= 240      # 小位移优先找附近的解
    with pytest.raises(sk.KinematicsError) as far:
        model.solve_position([0, 0, .9], STAND)
    assert far.value.code == 'UNREACHABLE'
