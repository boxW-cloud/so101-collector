"""串口出一次错之后不能永久卡在 comm=-1（PORT_BUSY）：2026-10-20、10-26 真机都卡死过，只能重启容器。"""
import pytest
from serial import SerialException, SerialTimeoutException

import seeed_arm
import seeed_ros
from seeed_arm import JOINTS, PRESENT_POSITION, ArmError
from seeed_ros import Follower
from sim_seeed import SimBus, connect
from test_seeed_ros import WallClock


@pytest.fixture
def clock(monkeypatch):
    c = WallClock()
    monkeypatch.setattr(seeed_arm, 'time', c)
    monkeypatch.setattr(seeed_ros, 'time', c)
    return c


def fail_writes(bus, times, error):
    original, left = bus.writePort, [times]

    def write(packet):
        if left[0]:
            left[0] -= 1
            raise error
        return original(packet)

    bus.writePort = write


@pytest.mark.parametrize('error', [SerialTimeoutException('Write timeout'), SerialException('device reports readiness')])
def test_one_serial_error_does_not_leave_the_port_busy(clock, monkeypatch, error):
    bus = SimBus(clock)
    arm = connect(monkeypatch, clock, bus)
    fail_writes(bus, 1, error)
    assert arm.read(JOINTS['shoulder_pan'].id, PRESENT_POSITION) == 2080     # 第 2 次重试就读到了
    assert bus.is_using is False and arm.reopened == 1
    for _ in range(3):
        assert arm.read(JOINTS['shoulder_pan'].id, PRESENT_POSITION) == 2080


def test_a_failed_write_is_reported_and_the_next_write_works(clock, monkeypatch):
    bus = SimBus(clock)
    arm = connect(monkeypatch, clock, bus)
    goal = (42, 2)
    fail_writes(bus, 1, SerialTimeoutException('Write timeout'))
    with pytest.raises(ArmError, match='串口出错，已重开'):
        arm.write(JOINTS['gripper'].id, goal, 2429)
    assert bus.is_using is False
    arm.write(JOINTS['gripper'].id, goal, 2429)


def test_follower_reports_connected_again_after_a_glitch(clock, monkeypatch, tmp_path):
    bus = SimBus(clock)
    follower = Follower(connect(monkeypatch, clock, bus), tmp_path, background=False)
    fail_writes(bus, 3, SerialTimeoutException('Write timeout'))             # 一次 read 的 3 次重试都失败
    assert 'error' in follower.sample()
    assert 'error' not in follower.sample() and bus.is_using is False         # 以前这里永远是 comm=-1
