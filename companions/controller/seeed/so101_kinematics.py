"""SO-101 的正/逆运动学：算末端位姿，以及由 (x, y, z, roll, pitch, yaw) 反解五个臂关节的位置。

算法和判据照搬松甲直连版已经写好的那一套（so101-spark-native-pose6d-20261008/arm_mcp/model.py），
只是整理成一个不依赖 arm-mcp 的文件；两台臂机械尺寸相同，用的是同一份 URDF。

约定：
  · 坐标：base_link → gripper_frame_link（夹爪指尖之间的那个参考点）。位置单位米，角度单位弧度。
  · 姿态：旋转矩阵 = Rz(yaw) · Ry(pitch) · Rx(roll)。
  · 关节角 ↔ ticks：按 LeRobot 的约定，每个关节量程的中点是 0 弧度，一圈 4095 格，方向不翻转。

要知道的限制：
  · 这台臂只有五个自由度，不是任意六维位姿都到得了。求解同时满足位置 ≤3mm 和姿态 ≤0.01rad 才算有解；
    找不到就明确报错，不会悄悄退成"只管位置"。
  · "量程中点 = 0 弧度"是定义出来的映射，没有在真臂上标定过。算出来的位姿在模型里是对的，
    真实末端的位置和姿态精度取决于整臂零位和 TCP 标定，目前没有做。
  · 这里没有碰撞检测。

依赖 numpy（装了 ROS 2 的环境里都有）。
"""
import hashlib
import math
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np

URDF = Path(__file__).with_name('models') / 'so101_new_calib.urdf'
URDF_SHA256 = '651ea00a3efe02a3c984b21a1b52273bd1b320ed635b3eae92fd52e99d0a393b'   # TheRobotStudio/SO-ARM100 @ eecbe3e
ROOT, TOOL = 'base_link', 'gripper_frame_link'
TICKS_PER_RADIAN = 4095 / (2 * math.pi)
POSITION_TOLERANCE = .003      # 米。求解结果换算成整数 ticks 之后，位置误差不超过这么多才算有解
ORIENTATION_TOLERANCE = .01    # 弧度。同上，姿态误差
SEARCH_BUDGET_S = 1.5          # 一次求解最多算这么久


class KinematicsError(Exception):
    """带错误码的求解失败。code：INVALID_INPUT / UNREACHABLE / LIMIT / IK_NOT_CONVERGED / IK_POSE_NOT_CONVERGED。"""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


# ══════════ 1. 旋转的基本运算 ══════════

def rotation(axis, angle):
    """绕 axis 转 angle 弧度的 3×3 旋转矩阵（罗德里格斯公式）。"""
    v = np.asarray(axis, dtype=float)
    x, y, z = v / np.linalg.norm(v)
    K = np.array([[0, -z, y], [z, 0, -x], [-y, x, 0]])
    return np.eye(3) + math.sin(angle) * K + (1 - math.cos(angle)) * (K @ K)


def rpy_matrix(rpy):
    roll, pitch, yaw = rpy
    return rotation([0, 0, 1], yaw) @ rotation([0, 1, 0], pitch) @ rotation([1, 0, 0], roll)


def matrix_rpy(R):
    """rpy_matrix 的逆运算。pitch 接近 ±90° 时 roll 和 yaw 不唯一，这里把 roll 取 0。"""
    if abs(R[2, 0]) > 1 - 1e-9:
        return [0., math.copysign(math.pi / 2, -R[2, 0]), math.atan2(-R[0, 1], R[1, 1])]
    return [math.atan2(R[2, 1], R[2, 2]), math.asin(-R[2, 0]), math.atan2(R[1, 0], R[0, 0])]


def rotation_vector(R):
    """旋转矩阵 → 轴角向量（方向是转轴，长度是转角）。经由四元数最大分量来取，在 0 和 180° 附近都稳定。"""
    R = np.asarray(R, dtype=float)
    candidates = np.array([1 + np.trace(R), 1 + R[0, 0] - R[1, 1] - R[2, 2],
                           1 - R[0, 0] + R[1, 1] - R[2, 2], 1 - R[0, 0] - R[1, 1] + R[2, 2]])
    index = int(np.argmax(candidates))
    component = .5 * math.sqrt(max(0, float(candidates[index])))
    d = 4 * component
    if index == 0:
        q = np.array([component, (R[2, 1] - R[1, 2]) / d, (R[0, 2] - R[2, 0]) / d, (R[1, 0] - R[0, 1]) / d])
    elif index == 1:
        q = np.array([(R[2, 1] - R[1, 2]) / d, component, (R[0, 1] + R[1, 0]) / d, (R[0, 2] + R[2, 0]) / d])
    elif index == 2:
        q = np.array([(R[0, 2] - R[2, 0]) / d, (R[0, 1] + R[1, 0]) / d, component, (R[1, 2] + R[2, 1]) / d])
    else:
        q = np.array([(R[1, 0] - R[0, 1]) / d, (R[0, 2] + R[2, 0]) / d, (R[1, 2] + R[2, 1]) / d, component])
    q /= np.linalg.norm(q)
    if q[0] < 0:
        q = -q
    length = float(np.linalg.norm(q[1:]))
    if length < 1e-12:
        return 2 * q[1:]
    return q[1:] * (2 * math.atan2(length, float(q[0])) / length)


def rotation_distance(A, B):
    """两个姿态之间的夹角（弧度）。直接在旋转上算，不拿欧拉角相减。"""
    return float(np.linalg.norm(rotation_vector(np.asarray(A) @ np.asarray(B).T)))


def _transform(xyz, rpy):
    T = np.eye(4)
    T[:3, :3] = rpy_matrix(rpy)
    T[:3, 3] = xyz
    return T


def _vector(value, label):
    if not isinstance(value, (list, tuple)) or len(value) != 3 or any(
            type(v) not in (int, float) or not math.isfinite(v) for v in value):
        raise KinematicsError('INVALID_INPUT', f'{label}必须是三个有限数值')
    return np.asarray(value, dtype=float)


class Kinematics:

    # ══════════ 2. 读 URDF，建立关节角和 ticks 的对应关系 ══════════

    def __init__(self, limits, margin):
        """limits = {关节名: (最小 ticks, 最大 ticks)}，即舵机的限位；求解出的目标离限位至少 margin。"""
        raw = URDF.read_bytes()
        if hashlib.sha256(raw).hexdigest() != URDF_SHA256:
            raise RuntimeError('URDF 文件和登记的校验和不一致')
        edges = []
        for node in ET.fromstring(raw).findall('joint'):
            origin_node, axis, limit = node.find('origin'), node.find('axis'), node.find('limit')
            edge = {'name': node.attrib['name'], 'type': node.attrib['type'],
                    'parent': node.find('parent').attrib['link'], 'child': node.find('child').attrib['link'],
                    'fixed': _transform([float(v) for v in origin_node.get('xyz', '0 0 0').split()],
                                        [float(v) for v in origin_node.get('rpy', '0 0 0').split()]),
                    'axis': [float(v) for v in axis.get('xyz', '0 0 1').split()] if axis is not None else [0, 0, 1]}
            if limit is not None:
                edge.update(lower=float(limit.attrib['lower']), upper=float(limit.attrib['upper']))
            edges.append(edge)
        # 从工具坐标系往回走到底座，得到底座 → 工具的一条链：五个转动关节加末尾一个固定变换
        by_child = {edge['child']: edge for edge in edges}
        chain, link = [], TOOL
        while link != ROOT:
            chain.append(by_child[link])
            link = by_child[link]['parent']
        self.chain = chain[::-1]
        self.names = [edge['name'] for edge in self.chain if edge['type'] == 'revolute']
        self.mapping = {}
        for edge in self.chain:
            if edge['type'] != 'revolute':
                continue
            lo, hi = limits[edge['name']]
            zero = (lo + hi) / 2                                  # 量程中点 = 0 弧度
            self.mapping[edge['name']] = {
                'zero_ticks': zero,
                'lower': max(edge['lower'], (lo + margin - zero) / TICKS_PER_RADIAN),   # 模型限位和舵机限位取交集
                'upper': min(edge['upper'], (hi - margin - zero) / TICKS_PER_RADIAN)}
        self.reach = sum(np.linalg.norm(edge['fixed'][:3, 3]) for edge in self.chain)   # 连杆总长：到达范围的外边界

    def ticks_to_angles(self, ticks):
        return {name: (ticks[name] - self.mapping[name]['zero_ticks']) / TICKS_PER_RADIAN for name in self.names}

    def angles_to_ticks(self, angles):
        out = {}
        for name in self.names:
            m = self.mapping[name]
            if not m['lower'] - 1e-8 <= angles[name] <= m['upper'] + 1e-8:
                raise KinematicsError('LIMIT', f'{name} 超出模型与舵机限位的共同范围')
            out[name] = round(m['zero_ticks'] + angles[name] * TICKS_PER_RADIAN)
        return out

    # ══════════ 3. 正运动学 ══════════

    def _forward(self, angles):
        """沿链乘过去。返回 (末端位置, 末端旋转, 各关节转轴方向, 各关节转轴上的一点)，后两项给雅可比用。"""
        T, axes, pivots = np.eye(4), [], []
        for edge in self.chain:
            T = T @ edge['fixed']
            if edge['type'] == 'revolute':
                axes.append(T[:3, :3] @ np.asarray(edge['axis']))
                pivots.append(T[:3, 3].copy())
                R = np.eye(4)
                R[:3, :3] = rotation(edge['axis'], angles[edge['name']])
                T = T @ R
        return T[:3, 3], T[:3, :3], np.asarray(axes), np.asarray(pivots)

    def fk(self, ticks):
        """五个臂关节的 ticks → 末端位置（米）和 3×3 旋转矩阵。"""
        position, R, _, _ = self._forward(self.ticks_to_angles(ticks))
        return position, R

    def pose(self, ticks):
        """给人看的末端位姿：{'position_m': [x, y, z], 'orientation_rpy': [roll, pitch, yaw]}。"""
        position, R = self.fk(ticks)
        return {'position_m': [round(float(v), 5) for v in position],
                'orientation_rpy': [round(float(v), 5) for v in matrix_rpy(R)]}

    def error(self, ticks, position, rpy=None):
        """当前 ticks 离目标位姿多远：(位置误差 米, 姿态误差 弧度 或 None)。"""
        here, R = self.fk(ticks)
        return (float(np.linalg.norm(np.asarray(position) - here)),
                None if rpy is None else rotation_distance(rpy_matrix(rpy), R))

    # ══════════ 4. 逆运动学 ══════════

    def _bounds(self, locked):
        """每个关节的搜索范围（弧度），向内收到整数 ticks 上；locked 里的关节固定在给定角度。"""
        lower, upper = [], []
        for name in self.names:
            m = self.mapping[name]
            lo, hi = (locked[name], locked[name]) if name in locked else (m['lower'], m['upper'])
            lo = (math.ceil(m['zero_ticks'] + lo * TICKS_PER_RADIAN - 1e-8) - m['zero_ticks']) / TICKS_PER_RADIAN
            hi = (math.floor(m['zero_ticks'] + hi * TICKS_PER_RADIAN + 1e-8) - m['zero_ticks']) / TICKS_PER_RADIAN
            if lo > hi:
                raise KinematicsError('LIMIT', f'{name} 在模型和舵机限位之间没有可用范围')
            lower.append(lo)
            upper.append(hi)
        return np.asarray(lower), np.asarray(upper)

    def solve_pose(self, position, rpy, current_ticks):
        """位置和姿态一起求解。返回 {'ticks', 'position_error_m', 'orientation_error_rad', 'iterations'}。

        方法：阻尼最小二乘（Levenberg-Marquardt），位置误差和姿态误差按各自的容差归一后一起最小化。
        初值依次用：当前姿态、由目标姿态解析拆出来的几组初值、量程中点、几组肩肘组合、16 组随机值。
        任何一组收敛、并且换算成整数 ticks 后仍满足两个容差，就返回。
        """
        target, wanted = _vector(position, '末端位置（米）'), rpy_matrix(_vector(rpy, '姿态 roll/pitch/yaw（弧度）'))
        if np.linalg.norm(target) > self.reach + POSITION_TOLERANCE:
            raise KinematicsError('UNREACHABLE', '目标超出连杆总长形成的外边界')
        names = self.names
        q0 = self.ticks_to_angles(current_ticks)
        lower, upper = self._bounds({})
        current = np.array([q0[name] for name in names])
        deadline = time.perf_counter() + SEARCH_BUDGET_S
        weight = POSITION_TOLERANCE / ORIENTATION_TOLERANCE       # 让 3mm 和 0.01rad 在代价里一样重
        best = {'score': math.inf, 'position': math.inf, 'orientation': math.inf, 'q': None}
        iterations = 0

        def evaluate(v):
            here, R, axes, pivots = self._forward(dict(zip(names, v)))
            ep, eo = target - here, rotation_vector(wanted @ R.T)
            return (np.concatenate((ep, weight * eo)), np.cross(axes, here - pivots).T, axes.T,
                    float(np.linalg.norm(ep)), float(np.linalg.norm(eo)), eo)

        def accept(v):
            """把连续解取整成 ticks，再验一遍；取整后超差就不算。"""
            if v is None:
                return None
            try:
                ticks = self.angles_to_ticks(dict(zip(names, map(float, v))))
            except KinematicsError:
                return None
            ep, eo = self.error(ticks, target, rpy)
            if ep > POSITION_TOLERANCE or eo > ORIENTATION_TOLERANCE:
                return None
            return {'ticks': ticks, 'position_error_m': ep, 'orientation_error_rad': eo, 'iterations': iterations}

        def optimize(initial, maximum=150):
            nonlocal iterations
            v, damping, stalled = np.clip(initial, lower, upper), 2e-5, 0
            free = np.where(upper - lower > 1e-12)[0]
            for _ in range(maximum):
                if time.perf_counter() >= deadline:
                    break
                iterations += 1
                error, Jp, Jw, ep, eo, eo_vector = evaluate(v)
                score = float(error @ error)
                if score < best['score']:
                    best.update(score=score, position=ep, orientation=eo, q=v.copy())
                if ep <= POSITION_TOLERANCE and eo <= ORIENTATION_TOLERANCE:
                    result = accept(v)
                    if result is not None:
                        return result
                # 姿态误差是轴角向量，它对关节角的导数要乘上 SO(3) 对数映射的雅可比，大角度误差时才准
                x, y, z = eo_vector
                K = np.array([[0, -z, y], [z, 0, -x], [-y, x, 0]])
                coefficient = 1 / 12 if eo < 1e-5 else (1 - .5 * eo / math.tan(.5 * eo)) / (eo * eo)
                J = np.vstack((Jp, weight * ((np.eye(3) + .5 * K + coefficient * (K @ K)) @ Jw)))[:, free]
                step = np.linalg.solve(J.T @ J + damping * np.eye(len(free)), J.T @ error)
                step *= min(1, .25 / max(float(np.max(np.abs(step))), 1e-12))      # 单步每个关节最多 0.25 弧度
                for alpha in (1., .5, .25, .1):                                    # 回溯：步子太大就缩
                    candidate = v.copy()
                    candidate[free] += alpha * step
                    candidate = np.clip(candidate, lower, upper)
                    after = evaluate(candidate)[0]
                    if float(after @ after) < score - 1e-12:
                        v, damping, stalled = candidate, max(1e-8, damping * .7), 0
                        break
                else:
                    damping, stalled = min(.5, damping * 5), stalled + 1
                    if stalled >= 10:
                        break
            return None

        seeds = [current] + self._pose_seeds(target, wanted, lower, upper) + [(lower + upper) / 2]
        for shoulder, elbow in ((.2, .2), (.2, .8), (.8, .2), (.8, .8)):
            v = np.clip(current, lower, upper)
            v[1] = lower[1] + shoulder * (upper[1] - lower[1])
            v[2] = lower[2] + elbow * (upper[2] - lower[2])
            seeds.append(v)
        seeds.extend(np.random.default_rng().uniform(lower, upper) for _ in range(16))
        for initial in seeds:
            if time.perf_counter() >= deadline:
                break
            result = optimize(initial)
            if result is not None:
                return result
        result = accept(best['q'])
        if result is not None:
            return result
        raise KinematicsError('IK_POSE_NOT_CONVERGED',
                              f"没有找到同时满足位置≤{POSITION_TOLERANCE * 1000:.0f}mm、姿态≤{ORIENTATION_TOLERANCE}rad "
                              f"和关节限位的解（最接近的候选差 {best['position'] * 1000:.1f}mm / "
                              f"{best['orientation']:.4f}rad）。五自由度臂到不了任意六维位姿；"
                              '搜索失败也不等于证明不可达')

    def _pose_seeds(self, target, wanted, lower, upper):
        """由目标位姿解析地拆出几组初值，免得数值迭代卡在腕旋的另一个分支上。

        这条链在底座旋转和腕旋之间是三个轴互相平行的弯曲关节（肩、肘、腕），所以可以：
        先由工具轴的方向解出底座旋转和腕旋（各两个分支），再把剩下的问题当成平面两连杆解肩和肘（各两个分支）。
        解出来的只是初值，最后仍然要过完整的数值求解和容差检查。
        """
        fixed = [edge['fixed'] for edge in self.chain]
        A, B = fixed[0][:3, :3], fixed[1][:3, :3]
        middle = fixed[2][:3, :3] @ fixed[3][:3, :3]
        C, D = middle @ fixed[4][:3, :3], fixed[5][:3, :3]
        tool_axis, normal = A.T @ wanted @ D.T[:, 2], B[:, 2]
        cosine = normal[0] * tool_axis[0] + normal[1] * tool_axis[1]
        sine = normal[0] * tool_axis[1] - normal[1] * tool_axis[0]
        magnitude = math.hypot(cosine, sine)
        seeds = []
        if magnitude <= 1e-10:
            return seeds
        ratio = (C[2, 2] - normal[2] * tool_axis[2]) / magnitude
        if abs(ratio) > 1 + 1e-7:
            return seeds

        def wraps(value, index):                         # 同一个角加减整圈后，落在该关节范围内的那些
            return [value + turn * 2 * math.pi for turn in (-1, 0, 1)
                    if lower[index] - 1e-9 <= value + turn * 2 * math.pi <= upper[index] + 1e-9]

        center, offset = math.atan2(sine, cosine), math.acos(float(np.clip(ratio, -1, 1)))
        v1, v2 = fixed[2][:2, 3], (fixed[2][:3, :3] @ fixed[3][:3, 3])[:2]
        l1, l2 = float(np.linalg.norm(v1)), float(np.linalg.norm(v2))
        phase1, phase2 = math.atan2(v1[1], v1[0]), math.atan2(v2[1], v2[0])
        for pan0 in (center - offset, center + offset):
            for pan in wraps(pan0, 0):
                Rpan = rotation([0, 0, 1], pan)
                E = B.T @ Rpan.T @ A.T @ wanted @ D.T
                for roll in wraps(math.atan2(E[2, 0], E[2, 1]) - math.atan2(C[2, 0], C[2, 1]), 4):
                    Rroll = rotation([0, 0, 1], roll)
                    bend = E @ Rroll.T @ C.T
                    total = math.atan2(bend[1, 0], bend[0, 0])           # 肩 + 肘 + 腕 三个弯曲角之和
                    reduced = B.T @ (Rpan.T @ A.T @ (target - fixed[0][:3, 3]) - fixed[1][:3, 3])
                    reduced -= rotation([0, 0, 1], total) @ middle @ (
                        fixed[4][:3, 3] + fixed[4][:3, :3] @ Rroll @ fixed[5][:3, 3])
                    elbow_cos = (float(reduced[:2] @ reduced[:2]) - l1 * l1 - l2 * l2) / (2 * l1 * l2)
                    if abs(elbow_cos) > 1 + 1e-7:
                        continue
                    beta = math.acos(float(np.clip(elbow_cos, -1, 1)))
                    for branch in (beta, -beta):
                        shoulder0 = math.atan2(reduced[1], reduced[0]) - math.atan2(
                            l2 * math.sin(branch), l1 + l2 * math.cos(branch)) - phase1
                        for shoulder in wraps(shoulder0, 1):
                            for elbow in wraps(branch - phase2 + phase1, 2):
                                for wrist in wraps(total - shoulder - elbow, 3):
                                    seeds.append(np.array([pan, shoulder, elbow, wrist, roll]))
        return seeds

    def solve_position(self, position, current_ticks, locked=('wrist_roll',)):
        """只管位置的求解，locked 里的关节保持当前角度不动。返回 {'ticks', 'position_error_m', 'iterations'}。

        只给位置时解有无穷多个，腕旋会被解到很偏的角度（夹爪上的相机被带歪），所以默认把腕旋锁在当前角度。
        先在当前姿态附近找（每个关节 ±240 ticks 以内），找不到再放开到整个量程。
        """
        target = _vector(position, '末端位置（米）')
        if np.linalg.norm(target) > self.reach + POSITION_TOLERANCE:
            raise KinematicsError('UNREACHABLE', '目标超出连杆总长形成的外边界')
        names = self.names
        q0 = self.ticks_to_angles(current_ticks)
        lower, upper = self._bounds({name: q0[name] for name in locked})
        current = np.array([q0[name] for name in names])
        deadline = time.perf_counter() + SEARCH_BUDGET_S
        best = {'error': math.inf, 'q': None}
        iterations = 0

        def position_and_jacobian(v):
            here, _, axes, pivots = self._forward(dict(zip(names, v)))
            return here, np.cross(axes, here - pivots).T

        def accept(v):
            if v is None:
                return None
            try:
                ticks = self.angles_to_ticks(dict(zip(names, map(float, v))))
            except KinematicsError:
                return None
            ep, _ = self.error(ticks, target)
            return None if ep > POSITION_TOLERANCE else {'ticks': ticks, 'position_error_m': ep,
                                                         'iterations': iterations}

        def optimize(initial, lo, hi, maximum):
            nonlocal iterations
            v, damping, stalled = np.clip(initial, lo, hi), 2e-5, 0
            local_best, local_q = math.inf, v.copy()
            for _ in range(maximum):
                if time.perf_counter() > deadline:
                    break
                iterations += 1
                here, J = position_and_jacobian(v)
                error = target - here
                norm = float(np.linalg.norm(error))
                if norm < local_best:
                    local_best, local_q = norm, v.copy()
                if norm < best['error']:
                    best.update(error=norm, q=v.copy())
                if norm < .0007:
                    break
                step = J.T @ np.linalg.solve(J @ J.T + damping * np.eye(3), error)
                step *= min(1, .22 / max(float(np.max(np.abs(step))), 1e-12))
                for alpha in (1., .5, .25, .1):
                    candidate = np.clip(v + alpha * step, lo, hi)
                    if float(np.linalg.norm(target - position_and_jacobian(candidate)[0])) < norm - 1e-9:
                        v, damping, stalled = candidate, max(1e-7, damping * .7), 0
                        break
                else:
                    damping, stalled = min(.5, damping * 5), stalled + 1
                    if stalled >= 12:
                        break
            return local_q

        near = 240 / TICKS_PER_RADIAN
        local_lo, local_hi = np.maximum(lower, current - near), np.minimum(upper, current + near)
        if np.all(local_lo <= local_hi):
            result = accept(optimize(current, local_lo, local_hi, 70))
            if result is not None:
                return result
        seeds = [current, (lower + upper) / 2]
        for shoulder, elbow in ((.2, .2), (.2, .8), (.8, .2), (.8, .8)):
            v = np.clip(current, lower, upper)
            v[1] = lower[1] + shoulder * (upper[1] - lower[1])
            v[2] = lower[2] + elbow * (upper[2] - lower[2])
            seeds.append(v)
        seeds.extend(np.random.default_rng().uniform(lower, upper) for _ in range(12))
        for initial in seeds:
            if time.perf_counter() > deadline:
                break
            result = accept(optimize(initial, lower, upper, 120))
            if result is not None:
                return result
        result = accept(best['q'])
        if result is not None:
            return result
        raise KinematicsError('IK_NOT_CONVERGED',
                              f"没有找到满足关节限位的解（最小残差 {best['error'] * 1000:.1f}mm）；"
                              '腕旋被锁在当前角度。搜索失败不等于证明不可达')
