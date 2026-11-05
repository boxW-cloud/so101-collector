import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
if (Path(__file__).parent.parent / 'deps').is_dir():     # 打包目录里自带依赖，测试也用它
    sys.path.insert(0, str(Path(__file__).parent.parent / 'deps'))


class Clock:
    """假时钟：sleep 不真的等，只把时间往前拨，模拟出来的舵机按这个时间走。"""

    def __init__(self):
        self.t = 100.

    def monotonic(self):
        return self.t

    def sleep(self, seconds):
        self.t += seconds


import pytest


@pytest.fixture(autouse=True)
def soft_limits_on(monkeypatch):
    """这些测试是按"软限位开着"写的。文件里默认关着（使用者 2026-10-12 的要求），测试时统一打开；
    关着的行为由 test_songjia.py 里的 test_soft_limits_off_* 单独测。"""
    try:
        import songjia_arm
    except ImportError:          # 只带 Seeed 文件的打包目录里没有它
        return
    monkeypatch.setattr(songjia_arm, 'SOFT_LIMITS', True)
