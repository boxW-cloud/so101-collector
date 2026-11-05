"""本机数据源的分类。

v10r15（2026-11-05）：采集容器只采集，不再直连真实主从臂：以前这里的 DirectFollowerAdapter（读主臂、写从臂舵机）、
FeetechBus、摄像头读取和 prepare / check / release 命令都删掉了。SO101 真机、仿真和 Bobac3 都从 Redis 采集队列取数据
（redis_capture.py），从臂由从臂节点控制（so101-isaac 里的节点 / so101-controller）。
这里只留下各处判断“本机的数据源”用的两个函数。PROVENANCE 留着，只是为了认出以前登记过的直连真实从臂目标。
"""

PROVENANCE = 'physical_direct_follower'
# Redis 采集队列（redis_capture.py，provenance=perceptor_capture_v5）
LOCAL_SOURCES = (PROVENANCE, 'perceptor_capture_v5')


def is_direct(robot):
    """登记的目标是不是本机的数据源（Redis 采集队列，或以前的直连真实从臂）。是的话，各处按仿真那套流程走，但数据标为真机。"""
    return isinstance(robot, dict) and robot.get('kind') == 'physical' and robot.get('provenance') in LOCAL_SOURCES


def sim_like(robot):
    """仿真目标，或者复用仿真流程的本机数据源。"""
    return isinstance(robot, dict) and (robot.get('kind') == 'simulation' or is_direct(robot))
