"""模拟的 STS 舵机总线，给 Seeed 臂的各个测试共用。不接硬件。"""
from types import SimpleNamespace

import seeed_arm
from seeed_arm import DEFAULT_ACC, DEFAULT_SPEED, JOINTS, USB_ID, SeeedArm

FOLDED = {'shoulder_pan': 2080, 'shoulder_lift': 880, 'elbow_flex': 3150,
          'wrist_flex': 2900, 'wrist_roll': 1125, 'gripper': 2429}


class SimBus:
    """顶替 sdk.PortHandler：解析发来的帧，像 STS 舵机一样应答。

    每个舵机按速度寄存器追目标寄存器；轴的实际位置比它再滞后 40ms。
    """

    def __init__(self, clock, pose=FOLDED, blocked=None, mute_after=None, temps=None, sync_read=True,
                 drag=None, late_reads=()):
        """drag={舵机: 格数}：带负载的纯比例舵机，轴停在离目标还差这么多的地方（真机肘关节约 30）。
        late_reads=(n, ...)：第 n 条读指令的应答迟到——等 SDK 判了超时才出现在接收缓冲里。
        """
        self.clock, self.at = clock, clock.t
        self.blocked, self.mute_after, self.temps = blocked or {}, mute_after, temps or (lambda t: 30)
        self.drag, self.late_reads, self.reads, self.late = drag or {}, set(late_reads), 0, b''
        self.sync_read, self.is_using, self.ser, self.is_open = sync_read, False, self, True
        self.rx, self.polls, self.frames = bytearray(), 0, 0
        self.mem = {m: bytearray(128) for m in range(1, 7)}
        for n, j in JOINTS.items():
            self.poke(j.id, 31, j.homing)
            self.poke(j.id, 9, j.lo)
            self.poke(j.id, 11, j.hi)
            self.poke(j.id, 42, pose[n])
            self.poke(j.id, 46, DEFAULT_SPEED)
            self.mem[j.id][41] = DEFAULT_ACC
        self.ref = {m: float(self.peek(m, 42)) for m in self.mem}
        self.shaft = dict(self.ref)
        self.trace = []     # 每条流式目标帧 (时间, {舵机: 目标}, {舵机: 轴位置})
        self.writes = []    # 每条单舵机写指令 (舵机, 地址)
        self._run(0)

    def peek(self, m, addr):
        return int.from_bytes(self.mem[m][addr:addr + 2], 'little')

    def poke(self, m, addr, value):
        self.mem[m][addr:addr + 2] = value.to_bytes(2, 'little')

    def _run(self, cost):
        self.clock.t += cost
        while self.at < self.clock.t:
            for m in self.mem:
                step = (self.peek(m, 46) or 3400) * .001        # 速度 0 = 全速，和真舵机一样
                self.ref[m] += max(-step, min(step, self.peek(m, 42) - self.ref[m]))
                gap, drag = self.ref[m] - self.shaft[m], self.drag.get(m, 0)
                self.shaft[m] += (gap - max(-drag, min(drag, gap))) * .001 / .04
                self.shaft[m] = min(self.shaft[m], self.blocked.get(m, 4095))
            self.at += .001
        for m in self.mem:
            self.poke(m, 56, round(self.shaft[m]))
            self.mem[m][63] = self.temps(self.clock.t)

    def _reply(self, m, data):
        if self.mute_after is None or self.frames <= self.mute_after:
            body = [m, len(data) + 2, 0, *data]
            self.rx += bytes([0xFF, 0xFF, *body, ~sum(body) & 0xFF])

    # ---- SDK 会用到的 PortHandler 接口 ----
    def openPort(self):
        return True

    def setBaudRate(self, baudrate):
        return baudrate == 1_000_000

    def clearPort(self):
        pass

    def closePort(self):
        self.is_open = False

    def reset_input_buffer(self):
        self.rx.clear()

    def setPacketTimeout(self, length):
        self.polls = 0

    def isPacketTimeout(self):
        self.polls += 1
        if self.polls > 20 and self.late:               # 刚判超时，迟到的应答才到
            self.rx += self.late
            self.late = b''
        return self.polls > 20

    def readPort(self, n):
        out = bytes(self.rx[:n])
        del self.rx[:n]
        return out

    def writePort(self, packet):
        assert packet[:2] == [0xFF, 0xFF] and packet[-1] == ~sum(packet[2:-1]) & 0xFF
        target, inst, params = packet[2], packet[4], packet[5:-1]
        self._run(.0005 if inst == 0x83 else .002)
        if inst == 0x83:                                        # 同步写
            addr, n = params[:2]
            assert target == 0xFE and (len(params) - 2) % (n + 1) == 0
            values = {params[i]: bytes(params[i + 1:i + 1 + n]) for i in range(2, len(params), n + 1)}
            for m, data in values.items():
                assert addr != 46 or int.from_bytes(data, 'little') >= 1
                self.mem[m][addr:addr + n] = data
            if addr == 42:
                self.trace.append((self.clock.t, {m: self.peek(m, 42) for m in values},
                                   {m: self.shaft[m] for m in values}))
        elif inst == 0x82:                                      # 同步读
            self.frames += 1
            for m in params[2:] if self.sync_read else ():
                self._reply(m, self.mem[m][params[0]:params[0] + params[1]])
        elif inst == 0x02:                                      # 读
            self.frames += params == [56, 8]
            self.reads += 1
            self._reply(target, self.mem[target][params[0]:params[0] + params[1]])
            if self.reads in self.late_reads:                   # 这条应答先扣下，等超时之后再放出来
                self.late, self.rx = bytes(self.rx), bytearray()
        elif inst == 0x03:                                      # 写
            self.writes.append((target, params[0]))
            self.mem[target][params[0]:params[0] + len(params) - 1] = bytes(params[1:])
            self._reply(target, b'')
        return len(packet)


def connect(monkeypatch, clock, bus, **kw):
    board = SimpleNamespace(device='COM7', vid=USB_ID[0], pid=USB_ID[1], serial_number=USB_ID[2])
    other = SimpleNamespace(device='COM3', vid=0x1A86, pid=0x7523, serial_number=None)
    monkeypatch.setattr(seeed_arm.list_ports, 'comports', lambda: [other, board])
    monkeypatch.setattr(seeed_arm.sdk, 'PortHandler', lambda name: bus)
    return SeeedArm(**kw)
