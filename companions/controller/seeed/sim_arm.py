"""Isaac 仿真从臂：和 seeed_arm.SeeedArm 同样的接口，七个原动作（seeed_ros.py）不改一行就能驱动仿真里的从臂。

单位：对外仍是 ticks，换算和真机完全一样（LeRobot 换算，用真实从臂的限位寄存器 seeed_arm.JOINTS）：
  五个臂关节  角度 = (ticks - (lo+hi)/2) × 360/4095        夹爪  开合度 = (ticks - lo)/(hi - lo) × 100
所以真机上示教的预设位、ARM_MOVETO 的运动学，在仿真里对应同样的关节角。

通信：采集容器里 Isaac 的同机文件接口（<ipc>/request.json ↔ <ipc>/response.json），协议与采集容器的
FileSimulationBackend 相同：connect（拿租约）→ sample（读六个关节）/ send（发六个关节目标）→ close（原地保持、交还租约）。
Isaac 端 2 秒收不到请求就让租约过期并原地保持，所以本进程意外退出时仿真臂不会乱动。

占用租约的时间尽量短：只在动作执行期间连接，目标不再变化、都已走到后 IDLE_RELEASE_S 秒就交还；
空闲时读位置走 Isaac 每秒发布一次的 sim-status.json，不占租约，仿真采集照常可用。
仿真采集正在进行（租约被采集容器占着）时，动作会以“Isaac 仿真从臂正被别的程序控制”失败，不会抢。

舵机速度：每个关节的目标按 GOAL_VELOCITY（ticks/s）一点点推过去，和真舵机一样慢慢走，不会一下跳到终点。
仿真里没有温度、电压，也不能卸力（Isaac 总是保持）：温度读 30°C，电压读 12.0V，卸力只清通电标记。

命令行：
    python sim_arm.py check --ipc /isaac/ipc                       只读：打印仿真从臂当前六个关节（ticks 和角度）
    python sim_arm.py presets --from 真机config.json --to 仿真config.json --ipc /isaac/ipc
        把真机示教的预设复制给仿真实例，超出仿真关节范围的夹到范围内（打印改了哪些）
"""
import json
import os
import threading
import time
import uuid
from pathlib import Path

from seeed_arm import (ACCELERATION, DEFAULT_SPEED, GOAL_POSITION, GOAL_VELOCITY, HOMING_OFFSET, JOINTS, MARGIN,
                       MAX_POSITION_LIMIT, MAX_STEP, MIN_POSITION_LIMIT, MIN_STEP, PRESENT_POSITION,
                       PRESENT_TEMPERATURE, TEMP_LIMIT, TORQUE_ENABLE, TORQUE_LIMIT, ArmError)

PRESENT_VOLTAGE = (62, 1)
LOOP_S = 0.1              # 动作期间：每 0.1 秒推进一次目标、发一次目标、读一次位置（同时给租约续期）
IDLE_RELEASE_S = 3.0      # 目标不再变化、都已走到这么久之后交还租约
RPC_TIMEOUT_S = 2.0       # Isaac 每秒处理约 30 个请求；2 秒没回复算没响应
STATUS_MAX_AGE_S = 5.0    # 空闲时 sim-status.json 最多这么旧（Isaac 约每秒发布一次）
FRESH_S = 0.5             # 连接期间的位置缓存最多这么旧
SIM_TEMPERATURE = 30      # °C
SIM_VOLTAGE_RAW = 120     # 0.1V，即 12.0V
NAME_BY_ID = {j.id: name for name, j in JOINTS.items()}


class IsaacBusy(ArmError):
    """Isaac 的控制租约被别的程序（例如采集容器的仿真采集）占着。"""


def to_units(name, ticks):
    """ticks → Isaac 用的 LeRobot 单位（角度 / 夹爪 0~100）。和 seeed_arm.lerobot_value 同一个公式，但不截断。"""
    j = JOINTS[name]
    if name == 'gripper':
        return (ticks - j.lo) / (j.hi - j.lo) * 100
    return (ticks - (j.lo + j.hi) / 2) * 360 / 4095


def to_ticks(name, value):
    j = JOINTS[name]
    if name == 'gripper':
        return j.lo + value / 100 * (j.hi - j.lo)
    return value * 4095 / 360 + (j.lo + j.hi) / 2


class IsaacArm:

    def __init__(self, ipc_dir, *, contract_file=None, speed_scale=1.0):
        """ipc_dir：Isaac 的 ipc 目录；contract_file 不填就用它上一级的 contract.json。只读检查，不拿租约。"""
        self.ipc = Path(ipc_dir)
        ready = self._json('ready.json')
        if ready is None:
            raise ArmError(f'Isaac 仿真没在运行：找不到 {self.ipc / "ready.json"}')
        contract = json.loads(Path(contract_file or self.ipc.parent / 'contract.json').read_text(encoding='utf-8'))
        if set(contract['names']) != {name + '.pos' for name in JOINTS}:
            raise ArmError(f'Isaac 合约的关节名和 SO101 对不上：{contract["names"]}')
        self.hash, self.run = ready['calibration_hash'], ready.get('generation')
        self.limits = {name: tuple(contract['limits'][name + '.pos']) for name in JOINTS}
        self.port_name = f'isaac:{self.ipc}'
        self.speed_scale = speed_scale
        self.enabled = set()                      # 本次运行里“上电”过的舵机 ID（仿真里只是标记）
        self.client = 'seeed-sim-' + uuid.uuid4().hex[:12]
        self.lock = threading.RLock()             # 保护下面这些状态
        self.rpc_lock = threading.Lock()          # 同一时刻只发一个请求
        self.seq = self.gen = 0
        self.connected, self.thread, self.stop = False, None, threading.Event()
        self.goal, self.ref = {}, {}              # 每个关节：要去的目标、此刻正在发的目标（ticks，浮点）
        self.speed = dict.fromkeys(JOINTS, DEFAULT_SPEED)
        self.regs = {}                            # 其余寄存器的写入值（加速度、扭矩上限），仿真里只记下来
        self.measured, self.measured_at = None, 0.0
        self.passive, self.passive_at = None, 0.0
        # 2026-11-04：读数的源头（Isaac 的步数/时刻），给 seeed_ros 的状态推送用（source_stamp）
        self.measured_source = self.passive_source = self.last_source = None
        self.last_change, self.error = 0.0, None
        self._read_status()                       # 仿真活着、数值正常才算连上

    # ══════════ 1. 和 Isaac 的文件接口 ══════════

    def _json(self, name):
        try:
            return json.loads((self.ipc / name).read_text(encoding='utf-8'))
        except (FileNotFoundError, json.JSONDecodeError):
            return None

    def _rpc(self, operation, **values):
        """一次请求：写 request.json（先写临时文件再改名），等 response.json 里出现同一个 id。"""
        with self.rpc_lock:
            self.seq += 1
            request = {'id': f'{self.client}:{self.seq}', 'client': self.client, 'sequence': self.seq,
                       'generation': self.gen, 'deadline_ns': time.monotonic_ns() + int(RPC_TIMEOUT_S * 1e9),
                       'operation': operation, 'calibration_hash': self.hash, **values}
            temporary = self.ipc / (self.client + '.tmp')
            temporary.write_text(json.dumps(request), encoding='utf-8')
            temporary.replace(self.ipc / 'request.json')
            end = time.monotonic() + RPC_TIMEOUT_S
            while time.monotonic() < end:
                response = self._json('response.json')
                if response and response.get('id') == request['id']:
                    if response.get('error'):
                        if response.get('code') == 'busy':
                            raise IsaacBusy('Isaac 仿真从臂正被别的程序控制（例如采集容器的仿真采集），等它结束再试')
                        raise ArmError(f'Isaac 拒绝 {operation}：{response["error"]}')
                    return response
                time.sleep(.003)
        raise ArmError(f'Isaac {RPC_TIMEOUT_S:.0f} 秒没有回复 {operation}')

    def _read_status(self):
        """空闲时的位置：Isaac 每秒发布一次的 sim-status.json（不占租约）。返回 {关节: ticks}。"""
        status = self._json('sim-status.json')
        if status is None:
            raise ArmError('读不到 Isaac 的 sim-status.json')
        if self.run is not None and status.get('run_generation') not in (None, self.run):
            raise ArmError('Isaac 重启过（运行代次变了），请重启仿真从臂节点')
        age = (time.monotonic_ns() - status['time_ns']) / 1e9
        if not -1 <= age <= STATUS_MAX_AGE_S:
            raise ArmError(f'Isaac 的状态已经 {age:.1f} 秒没有更新，仿真可能卡住了')
        values = {name: to_ticks(name, status['measured'][name + '.pos']) for name in JOINTS}
        with self.lock:
            self.passive, self.passive_at = values, time.monotonic()
            self.passive_source = self.last_source = {'kind': 'isaac_status', 'steps': status.get('steps'),
                                                      'time_ns': status.get('time_ns')}
        return values

    def _sample(self):
        response = self._rpc('sample')
        values = {name: to_ticks(name, response['positions'][name + '.pos']) for name in JOINTS}
        with self.lock:
            self.measured, self.measured_at = values, time.monotonic()
            self.measured_source = self.last_source = {'kind': 'isaac_sample',
                                                       'time_ns': response.get('captured_monotonic_ns')}
        return values

    def _units(self, ticks):
        """发给 Isaac 的目标：换成 LeRobot 单位，并夹在仿真合约的关节范围里（越界的请求 Isaac 会整条拒绝）。"""
        out = {}
        for name in JOINTS:
            lo, hi = self.limits[name]
            out[name + '.pos'] = max(lo, min(hi, to_units(name, ticks[name])))
        return out

    # ══════════ 2. 拿租约、动作期间的后台循环、交还租约 ══════════

    def _connect(self):
        with self.lock:
            if self.connected:
                return
        old = self.thread                         # 上一轮的循环可能正在交还租约：等它交还完再连，顺序不能反
        if old is not None and old.is_alive() and old is not threading.current_thread():
            old.join(RPC_TIMEOUT_S * 3)
        self.gen += 1
        self._rpc('connect')
        values = self._sample()
        with self.lock:
            # Isaac 连上时把目标设为当前位置：这里也从当前位置开始，不会跳
            self.goal = dict(values)
            self.ref = dict(values)
            self.connected, self.error, self.last_change = True, None, time.monotonic()
            self.stop.clear()
            self.thread = threading.Thread(target=self._loop, name='isaac-arm', daemon=True)
            self.thread.start()

    def _loop(self):
        sent, last_rpc = None, time.monotonic()
        while not self.stop.is_set():
            tick = time.monotonic()
            with self.lock:
                for name in JOINTS:
                    # 速度 0 = 不限速（跟随主臂，2026-11-05）：一步直接到目标，和真舵机写 0 是全速一样
                    step = (float('inf') if self.speed[name] == 0
                            else max(1, self.speed[name]) * self.speed_scale * LOOP_S)
                    gap = self.goal[name] - self.ref[name]
                    self.ref[name] += max(-step, min(step, gap))
                moving = any(abs(self.goal[name] - self.ref[name]) > .5 for name in JOINTS)
                quiet = time.monotonic() - self.last_change
                if not moving and quiet > IDLE_RELEASE_S:
                    self.connected = False        # 先标记：之后来的新目标会等交还完成再重新连接
                target = self._units(self.ref)
                connected = self.connected
            try:
                if not connected:
                    self._release()
                    return
                if target != sent:
                    self._rpc('send', positions=target)
                    sent, last_rpc = target, time.monotonic()
                # 运动中和停稳期间读位置（Isaac 每读一次会顺带存两张相机图，所以静止时不读）；否则只发心跳续租约
                if moving or quiet < 1.5 or time.monotonic() - self.measured_at > 1.0:
                    self._sample()
                    last_rpc = time.monotonic()
                elif time.monotonic() - last_rpc > .8:
                    self._rpc('heartbeat')
                    last_rpc = time.monotonic()
            except Exception as error:
                with self.lock:
                    self.error, self.connected = str(error), False
                return
            self.stop.wait(max(0.0, LOOP_S - (time.monotonic() - tick)))

    def _release(self):
        """原地保持并交还租约（Isaac 把目标设为当前位置）。"""
        with self.lock:
            self.connected = False
        self.gen += 1
        self._rpc('close')

    def _ensure(self):
        """要动之前：连上 Isaac（拿租约）。之前的循环因出错退出过，就把原因报出来，不悄悄重连。"""
        with self.lock:
            if self.connected:
                return
            error, self.error = self.error, None
        if error:
            raise ArmError(f'和 Isaac 的连接断了：{error}')
        self._connect()

    # ══════════ 3. 和 SeeedArm 一样的接口 ══════════

    def read(self, motor, reg):
        name = NAME_BY_ID[motor]
        j = JOINTS[name]
        if reg == PRESENT_POSITION:
            return self.position(name)
        if reg == GOAL_POSITION:
            with self.lock:
                goal = self.goal.get(name)
            return round(goal) if goal is not None else self.position(name)
        if reg == TORQUE_ENABLE:
            return int(motor in self.enabled)
        if reg == PRESENT_TEMPERATURE:
            return SIM_TEMPERATURE
        if reg == PRESENT_VOLTAGE:
            return SIM_VOLTAGE_RAW
        if reg == MIN_POSITION_LIMIT:
            return j.lo
        if reg == MAX_POSITION_LIMIT:
            return j.hi
        if reg == HOMING_OFFSET:
            return j.homing
        return self.regs.get((motor, reg[0]), 0)

    def write(self, motor, reg, value):
        name = NAME_BY_ID[motor]
        if reg == GOAL_POSITION:
            self._set_goal(name, value)
        elif reg == TORQUE_ENABLE:
            if value:
                self.torque_on([name])
            else:
                self.enabled.discard(motor)
        elif reg == GOAL_VELOCITY:
            if value < 0:
                raise ArmError('速度不能是负数（0 表示不限速）')
            with self.lock:
                self.speed[name] = value
        elif reg in (ACCELERATION, TORQUE_LIMIT):
            self.regs[(motor, reg[0])] = value
        else:
            raise ArmError(f'仿真从臂不支持写寄存器 {reg[0]}')

    def _set_goal(self, name, value):
        self._ensure()
        with self.lock:
            self.goal[name] = float(value)
            self.last_change = time.monotonic()

    def position(self, name):
        return round(self.positions_float()[name])

    def positions_float(self):
        with self.lock:
            if self.connected and self.measured and time.monotonic() - self.measured_at <= FRESH_S:
                self.last_source = self.measured_source
                return dict(self.measured)
            if self.passive and time.monotonic() - self.passive_at <= .2:
                self.last_source = self.passive_source
                return dict(self.passive)
        return self._read_status()

    def source_stamp(self):
        """最近一次给出的位置来自哪里：Isaac 的 sim-status.json（steps、time_ns，约每 1.5 秒更新一次）或
        动作期间的实时采样（time_ns = Isaac 采到的时刻）。time_ns 是主机 CLOCK_MONOTONIC。"""
        with self.lock:
            return dict(self.last_source) if self.last_source else None

    def positions(self):
        return {name: round(value) for name, value in self.positions_float().items()}

    def temperature(self, name):
        return SIM_TEMPERATURE

    def temps(self):
        return dict.fromkeys(JOINTS, SIM_TEMPERATURE)

    def torque_states(self):
        return {name: j.id in self.enabled for name, j in JOINTS.items()}

    def torque_on(self, joints=None, limits=True):
        """在当前位置“上电”：拿租约，目标设为当前位置，所以不会动。
        limits=False（跟随主臂）：不查温度，速度设为 0（不限速），和 SeeedArm.torque_on 一样。"""
        self._ensure()
        for name in joints or list(JOINTS):
            if limits and self.temperature(name) > TEMP_LIMIT:
                raise ArmError(f'{name} 温度超过门限 {TEMP_LIMIT}°C')
            if not limits:
                with self.lock:
                    self.speed[name] = 0
            here = self.positions_float()[name]
            with self.lock:
                self.goal[name] = self.ref[name] = here
                self.last_change = time.monotonic()
            self.enabled.add(JOINTS[name].id)

    def move(self, name, delta_ticks, speed=DEFAULT_SPEED):
        """相对移动一小步，检查和 SeeedArm.move 完全一样。发完立即返回，不等到位。"""
        j = JOINTS[name]
        if j.id not in self.enabled:
            raise ArmError(f'{name} 未通电，先 torque_on()')
        if not MIN_STEP <= abs(delta_ticks) <= MAX_STEP:
            raise ArmError(f'单次幅度限 {MIN_STEP}~{MAX_STEP} ticks')
        self._ensure()
        pos = self.position(name)
        target = pos + delta_ticks
        if not j.lo + MARGIN <= target <= j.hi - MARGIN:
            raise ArmError(f'{name} 目标 {target} 越出限位余量 [{j.lo + MARGIN}, {j.hi - MARGIN}]')
        self.write(j.id, GOAL_VELOCITY, speed)
        self._set_goal(name, target)
        return {'start': pos, 'target': target}

    def set_pose(self, targets, speed=DEFAULT_SPEED):
        out = {}
        for name, target in targets.items():
            delta = target - self.position(name)
            if abs(delta) > MAX_STEP:
                raise ArmError(f'{name} 距目标 {delta} 超过单步 {MAX_STEP}，请分段')
            if abs(delta) >= MIN_STEP:
                out[name] = self.move(name, delta, speed)
        return out

    def wait_settled(self, name, tol=8, timeout=8):
        goal = self.read(JOINTS[name].id, GOAL_POSITION)
        pos = self.position(name)
        start = time.monotonic()
        while abs(pos - goal) > tol and time.monotonic() - start < timeout:
            time.sleep(.08)
            pos = self.position(name)
        return {'position': pos, 'goal': goal, 'settled': abs(pos - goal) <= tol}

    def release(self, joints=None):
        """仿真里不能卸力（Isaac 总是保持）：只清通电标记。"""
        names = joints or [name for name, on in self.torque_states().items() if on]
        for name in names:
            self.enabled.discard(JOINTS[name].id)
        return names

    def close(self):
        """停掉后台循环，原地保持并交还租约。"""
        self.stop.set()
        thread = self.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(RPC_TIMEOUT_S * 3)
        with self.lock:
            connected = self.connected
        if connected:
            try:
                self._release()
            except Exception:
                pass                                # 交还失败也没关系：2 秒后 Isaac 自己让租约过期并原地保持
        with self.lock:
            self.connected = False

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


# ══════════ 4. 命令行：只读检查、把真机预设复制给仿真 ══════════

def clamp_presets(config, limits):
    """真机预设 → 仿真预设：超出仿真关节范围（limits，LeRobot 单位）的夹进范围，并保持离限位寄存器 MARGIN 以上。

    返回 (新 config, {预设.关节: {'real': 原值, 'sim': 新值}})。"""
    out, changed = json.loads(json.dumps(config)), {}

    def fit(name, ticks):
        lo, hi = limits[name]
        units = max(lo, min(hi, to_units(name, ticks)))
        j = JOINTS[name]
        value = round(max(j.lo + MARGIN, min(j.hi - MARGIN, to_ticks(name, units))))
        # 四舍五入后再确认一次仍在范围内（贴着范围边的，往里挪一格）
        while to_units(name, value) < lo:
            value += 1
        while to_units(name, value) > hi:
            value -= 1
        return value

    for key, value in config.items():
        if isinstance(value, dict):
            for name, ticks in value.items():
                new = fit(name, ticks)
                if new != ticks:
                    out[key][name] = new
                    changed[f'{key}.{name}'] = {'real': ticks, 'sim': new}
        elif key.startswith('gripper_') and isinstance(value, int):
            new = fit('gripper', value)
            if new != value:
                out[key] = new
                changed[key] = {'real': value, 'sim': new}
    return out, changed


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description='Isaac 仿真从臂（只读检查 / 复制预设）')
    parser.add_argument('command', choices=['check', 'presets'])
    parser.add_argument('--ipc', default=os.environ.get('SO101_ISAAC_IPC', '/isaac/ipc'))
    parser.add_argument('--from', dest='source')
    parser.add_argument('--to', dest='target')
    args = parser.parse_args(argv)
    arm = IsaacArm(args.ipc)
    if args.command == 'check':
        positions = arm.positions()
        print(f'Isaac 仿真从臂（{arm.ipc}），只读')
        for name in JOINTS:
            lo, hi = arm.limits[name]
            unit = '%' if name == 'gripper' else '°'
            print(f'  {name:<14} {positions[name]:>5} ticks   {to_units(name, positions[name]):>7.1f}{unit}'
                  f'   仿真范围 {lo:g}~{hi:g}{unit}')
        return 0
    if not args.source or not args.target:
        parser.error('presets 要写 --from 和 --to')
    config = json.loads(Path(args.source).read_text(encoding='utf-8'))
    new, changed = clamp_presets(config, arm.limits)
    target = Path(args.target)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix('.tmp')
    temporary.write_text(json.dumps(new, ensure_ascii=False, indent=1), encoding='utf-8')
    temporary.replace(target)
    print(json.dumps({'written': str(target), 'clamped_to_sim_range': changed}, ensure_ascii=False, indent=1))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
