"""Seeed SO-101 从臂直接控制：从打开串口到让关节运动，全部在这一个文件里。

硬件：CH343 控制板 + 6 个飞特 STS3215 舵机，1 Mbps 串行总线。
依赖：scservo_sdk（pip 包名 feetech-servo-sdk）和 pyserial。不依赖 LeRobot，也不依赖本仓库其他代码。

用法：
    from seeed_arm import SeeedArm
    arm = SeeedArm()                    # 找串口 → 打开 → 核对六个舵机的身份（全程只读）
    arm.positions()                     # {'shoulder_pan': 2080, ...}，单位 ticks
    arm.torque_on()                     # 在当前位置上电保持，不会跳动
    arm.move('shoulder_lift', +60)      # 相对移动，单次 5~180 ticks
    arm.wait_settled('shoulder_lift')   # 等它停稳
    arm.release()                       # 卸力。臂会失去支撑，先用手托稳
    arm.close()                         # 只关串口，不卸力

位置单位 ticks：一圈 4096 格，1 tick ≈ 0.088°。正方向就是编码器读数增大的方向。
直接运行本文件只做只读检查，不上电、不运动：python seeed_arm.py
"""
import time
from typing import NamedTuple

import scservo_sdk as sdk
from serial import SerialException
from serial.tools import list_ports


class ArmError(RuntimeError):
    """任何一步检查不通过都抛这个异常；抛出之后本文件不会再发运动指令。"""


# ══════════ 1. 这台臂的身份和限位 ══════════

USB_ID = (0x1A86, 0x55D3, '5A7C124107')   # 控制板的 (VID, PID, 序列号)
BAUDRATE = 1_000_000


class Joint(NamedTuple):
    id: int           # 舵机在总线上的 ID
    homing: int       # 寄存器 31 的原始值（零位偏移），只用来核对身份
    lo: int           # 寄存器 9：最小位置限位
    hi: int           # 寄存器 11：最大位置限位
    torque_cap: int   # 上电时写进寄存器 48 的扭矩上限，1000 = 100%


JOINTS = {
    'shoulder_pan':  Joint(1, 4083,  731, 3457, 250),
    'shoulder_lift': Joint(2, 1997,  870, 3288, 400),
    'elbow_flex':    Joint(3, 4035,  928, 3162, 350),
    'wrist_flex':    Joint(4, 3966,  808, 3194, 250),
    'wrist_roll':    Joint(5, 4083,    0, 4095, 150),
    'gripper':       Joint(6, 1071, 2006, 3489, 500),
}


# ══════════ 2. 用到的寄存器：(地址, 字节数) ══════════
# 名字和 LeRobot 的 lerobot/motors/feetech/tables.py 相同，方便两边对照。

MIN_POSITION_LIMIT = (9, 2)
MAX_POSITION_LIMIT = (11, 2)
HOMING_OFFSET = (31, 2)
TORQUE_ENABLE = (40, 1)          # 1 = 上电保持，0 = 卸力
ACCELERATION = (41, 1)           # 单位 100 ticks/s²
GOAL_POSITION = (42, 2)          # 目标位置
GOAL_VELOCITY = (46, 2)          # 运动速度 ticks/s；写 0 表示全速：只有跟随主臂（不限速，2026-11-05）时写 0
TORQUE_LIMIT = (48, 2)           # 运行时扭矩上限，断电不保存
PRESENT_POSITION = (56, 2)
PRESENT_TEMPERATURE = (63, 1)    # °C


# ══════════ 3. 运动参数 ══════════

MAX_STEP = 180        # move() 单次最大幅度（约 15.8°）
MIN_STEP = 5          # 比这更小的移动落在舵机死区里，不发
MARGIN = 30           # 任何目标都要离限位至少这么远
DEFAULT_SPEED = 200   # ticks/s
DEFAULT_ACC = 5       # ×100 ticks/s²
TEMP_LIMIT = 55       # °C，超过就拒绝上电和运动（跟随主臂时不查，见 torque_on 的 limits）
FULL_TORQUE = 1000    # 扭矩上限 100%（跟随主臂时用）


def lerobot_value(name, ticks):
    """同一个读数在 LeRobot 里显示成什么：五个臂关节是角度，夹爪是 0~100。

    公式来自 lerobot/motors/motors_bus.py 的 _normalize。前提是 LeRobot 标定文件里的
    range_min / range_max 与舵机寄存器 9 / 11 一致（LeRobot 连接时会自己核对这一点）。
    """
    j = JOINTS[name]
    if name == 'gripper':
        return (min(j.hi, max(j.lo, ticks)) - j.lo) / (j.hi - j.lo) * 100
    return (ticks - (j.lo + j.hi) / 2) * 360 / 4095


class SeeedArm:

    # ══════════ 4. 初始化：找串口 → 打开 → 核对身份（全程只读） ══════════

    def __init__(self, port=None):
        """port 不填就按 USB 序列号自动找。COM 号会随插拔变化，序列号不会。"""
        found = [p.device for p in list_ports.comports()
                 if (p.vid, p.pid, p.serial_number) == USB_ID and port in (None, p.device)]
        if not found:
            raise ArmError(f'没找到 Seeed 臂的控制板（CH343，序列号 {USB_ID[2]}）'
                           + (f'，{port} 不是它' if port else ''))
        self.port_name = found[0]
        self.port = sdk.PortHandler(self.port_name)
        self.packet = sdk.PacketHandler(0)        # 0 = STS 系列的字节序
        if not self.port.openPort() or not self.port.setBaudRate(BAUDRATE):
            raise ArmError('串口打开失败')
        self.port.ser.write_timeout = .5
        self.reopened = 0                         # 串口出错后重开的次数（排查用）
        self.enabled = set()                      # 本次连接里由我们上电的舵机 ID
        try:
            # 六个舵机的零偏和限位必须和登记的完全一样，否则说明换了臂、换了舵机或重新标定过
            for name, j in JOINTS.items():
                seen = (self.read(j.id, HOMING_OFFSET),
                        self.read(j.id, MIN_POSITION_LIMIT),
                        self.read(j.id, MAX_POSITION_LIMIT))
                if seen != (j.homing, j.lo, j.hi):
                    raise ArmError(f'{name} 的身份/标定和登记的不一样：读到 {seen}，应为 {(j.homing, j.lo, j.hi)}')
        except Exception:
            self.port.closePort()
            raise

    # ══════════ 5. 最底层：读一个寄存器、写一个寄存器 ══════════

    def _txrx(self, call, *args):
        """调一次 SDK 收发。

        SDK 发送时如果抛异常（串口写超时、读写出错），它的"串口占用中"标志 is_using 不会复位，此后每次都直接
        返回 comm=-1（PORT_BUSY），不再碰串口，直到进程重启（2026-10-20、10-26 真机都这样卡死过）。
        串口由调用方的锁串行使用，所以每次调用后都把这个标志复位；遇到串口异常就重开一次串口再报失败。
        """
        try:
            # 先丢掉接收缓冲里迟到的旧应答。SDK 的 clearPort() 只排空发送、不清接收：上一次超时的应答若稍后才到，
            # 会被当成这一次的应答，此后每次读到的都是"上一条"的结果（2026-10-12 真机：夹爪 35°C 读成了 >55°C）
            self.port.ser.reset_input_buffer()
            return call(self.port, *args)
        except (SerialException, OSError) as exc:
            self._reopen()
            raise ArmError(f'串口出错，已重开：{type(exc).__name__}: {exc}') from None
        finally:
            self.port.is_using = False

    def _reopen(self):
        """关掉再打开同一个串口。只是重建连接，不给舵机发任何命令，臂不会动。"""
        self.reopened += 1
        try:
            self.port.closePort()
        except Exception:
            pass
        try:
            if self.port.openPort() and self.port.setBaudRate(BAUDRATE):
                self.port.ser.write_timeout = .5
        except Exception:
            pass                                  # 下次读写还会再试

    def read(self, motor, reg):
        addr, size = reg
        comm = error = None
        for _ in range(3):                        # CH34x 偶尔丢帧，最多试 3 次
            try:
                data, comm, error = self._txrx(self.packet.readTxRx, motor, addr, size)
            except ArmError as exc:
                comm, error = str(exc), 0
                time.sleep(.05)
                continue
            if not comm and not error and len(data) == size:
                return int.from_bytes(bytes(data), 'little')
            time.sleep(.05)
        raise ArmError(f'读失败 ID{motor} 地址{addr} comm={comm} error={error}')

    def write(self, motor, reg, value):
        """写完立刻读回来核对；对不上就抛异常，不会带着一个没写进去的值继续往下走。"""
        addr, size = reg
        comm, error = self._txrx(self.packet.writeTxRx, motor, addr, size, list(value.to_bytes(size, 'little')))
        if comm or error:
            raise ArmError(f'写失败 ID{motor} 地址{addr} comm={comm} error={error}')
        time.sleep(.01)
        if self.read(motor, reg) != value:
            raise ArmError(f'写后读回不一致 ID{motor} 地址{addr}')

    # ══════════ 6. 读状态 ══════════

    def position(self, name):
        return self.read(JOINTS[name].id, PRESENT_POSITION)

    def positions(self):
        return {name: self.read(j.id, PRESENT_POSITION) for name, j in JOINTS.items()}

    def temperature(self, name):
        """读一个舵机的温度。读到超过门限时再读两次，取三次里最低的。

        真过热的舵机三次都会读高；只有一次高说明那一帧读错了，不该因此打断动作。
        """
        motor = JOINTS[name].id
        temp = self.read(motor, PRESENT_TEMPERATURE)
        for _ in range(2):
            if temp <= TEMP_LIMIT:
                break
            temp = min(temp, self.read(motor, PRESENT_TEMPERATURE))
        return temp

    def temps(self):
        return {name: self.temperature(name) for name in JOINTS}

    def torque_states(self):
        return {name: bool(self.read(j.id, TORQUE_ENABLE)) for name, j in JOINTS.items()}

    # ══════════ 7. 上电：在当前位置建立保持 ══════════

    def torque_on(self, joints=None, limits=True):
        """joints 不填就是全部六个。每个舵机五步写入，顺序不能换。

        limits=False（2026-11-05，跟随主臂用；用户：安全由人来保证）：不查温度，扭矩 100%，速度、加速度都写 0（不限）。"""
        for name in joints or list(JOINTS):
            j = JOINTS[name]
            pos = self.read(j.id, PRESENT_POSITION)
            if limits:
                temp = self.temperature(name)
                if temp > TEMP_LIMIT:
                    raise ArmError(f'{name} 温度 {temp}°C 超过门限 {TEMP_LIMIT}°C')
            self.write(j.id, TORQUE_LIMIT, j.torque_cap if limits else FULL_TORQUE)   # ① 扭矩上限
            self.write(j.id, GOAL_VELOCITY, DEFAULT_SPEED if limits else 0)           # ② 速度
            self.write(j.id, ACCELERATION, DEFAULT_ACC if limits else 0)              # ③ 加速度
            self.write(j.id, GOAL_POSITION, pos)             # ④ 目标 = 当前位置，所以上电瞬间不会动
            self.write(j.id, TORQUE_ENABLE, 1)               # ⑤ 最后才使能
            self.enabled.add(j.id)

    # ══════════ 8. 运动 ══════════

    def move(self, name, delta_ticks, speed=DEFAULT_SPEED):
        """相对移动一小步。发完指令立即返回，不等到位；要等就接着调 wait_settled()。"""
        j = JOINTS[name]
        if j.id not in self.enabled:
            raise ArmError(f'{name} 未通电，先 torque_on()')
        if not MIN_STEP <= abs(delta_ticks) <= MAX_STEP:
            raise ArmError(f'单次幅度限 {MIN_STEP}~{MAX_STEP} ticks')
        pos = self.read(j.id, PRESENT_POSITION)
        target = pos + delta_ticks
        if not j.lo + MARGIN <= target <= j.hi - MARGIN:
            raise ArmError(f'{name} 目标 {target} 越出限位余量 [{j.lo + MARGIN}, {j.hi - MARGIN}]')
        if self.temperature(name) > TEMP_LIMIT:
            raise ArmError(f'{name} 温度超过门限 {TEMP_LIMIT}°C')
        self.write(j.id, GOAL_VELOCITY, speed)
        self.write(j.id, GOAL_POSITION, target)              # 写入这一刻舵机开始转
        return {'start': pos, 'target': target}

    def set_pose(self, targets, speed=DEFAULT_SPEED):
        """绝对目标 {关节名: ticks}。每个关节离目标都必须在一步之内，否则拒绝，让调用方自己分段。"""
        out = {}
        for name, target in targets.items():
            delta = target - self.position(name)
            if abs(delta) > MAX_STEP:
                raise ArmError(f'{name} 距目标 {delta} 超过单步 {MAX_STEP}，请分段')
            if abs(delta) >= MIN_STEP:
                out[name] = self.move(name, delta, speed)
        return out

    def wait_settled(self, name, tol=8, timeout=8):
        """等到实测位置离目标不超过 tol。超时返回 settled=False：舵机停在死区里差几格是正常的。"""
        motor = JOINTS[name].id
        goal = self.read(motor, GOAL_POSITION)
        pos = self.read(motor, PRESENT_POSITION)
        start = time.monotonic()
        while abs(pos - goal) > tol and time.monotonic() - start < timeout:
            time.sleep(.08)
            pos = self.read(motor, PRESENT_POSITION)
        return {'position': pos, 'goal': goal, 'settled': abs(pos - goal) <= tol}

    # ══════════ 9. 卸力和关闭 ══════════

    def release(self, joints=None):
        """卸力。joints 不填就是所有正在通电的关节。臂会立刻失去支撑，先托稳。"""
        names = joints or [name for name, on in self.torque_states().items() if on]
        for name in names:
            motor = JOINTS[name].id
            self.write(motor, GOAL_POSITION, self.read(motor, PRESENT_POSITION))   # 目标收回到当前位置
            self.write(motor, TORQUE_ENABLE, 0)
            self.enabled.discard(motor)
        return names

    def close(self):
        """只关串口。舵机继续通电保持在原地，要卸力必须显式调 release()。"""
        if self.port.is_open:
            self.port.closePort()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


if __name__ == '__main__':
    with SeeedArm() as arm:               # 只读：不上电、不运动
        pos, temp, on = arm.positions(), arm.temps(), arm.torque_states()
        print(f'已连接 {arm.port_name}，六个舵机身份核对通过')
        for name, j in JOINTS.items():
            unit = '%' if name == 'gripper' else '°'
            print(f'  ID{j.id} {name:<14} {pos[name]:>4} ticks   LeRobot {lerobot_value(name, pos[name]):>7.1f}{unit}'
                  f'   {temp[name]}°C   {"通电" if on[name] else "未通电"}   限位 {j.lo}~{j.hi}')
