"""Seeed SO-101 从臂的 ROS 2 封装，按《真实从臂控制：ROS 2 Topic与JSON接口规范》（协议 so101.follower.v1）实现。

一个进程做完三件事：直连串口控制 Seeed 臂（seeed_arm.SeeedArm）、执行七个元动作、在 ROS 2 上收发 JSON。
旧的松甲桥接要经过"Spark 节点 → SSH 隧道 → Windows 代理 → arm-mcp 服务"；这里中间没有别的服务，
所以臂的 USB 必须插在运行本程序的那台机器上。

Topic（仿真实例这一版）：只有两个——命令 /so101/command、结果 /so101/result，
都是 std_msgs/String，内容是 JSON。命令和回执都是 {action, params}，params 里带 request_id（见第 6 节 CommandTopic）。
命令在节点内部转成规范里的请求，再走同一套检查：request_id 幂等、参数检查、同一时间只跑一个动作。
和规范不同的只有身份：robot_id 默认 so101-follower-seeed，device_id 是 ch343:<USB 序列号>。

七个动作在这台臂上的实际能力：
  LOOSEN / GRASP          执行。夹爪分段走到开/合端点，每段最多 100 ticks；某一段没走到就报失败
  ARM_RELEASE             先用 teach park 记过停放位才执行，否则 POSE_NOT_CONFIGURED
  HOLD_ARM / HOLD_GRASP   先用 teach chest 记过胸前位才执行，否则 POSE_NOT_CONFIGURED（HOLD_GRASP 会先整体预检）
  ARM_MOVETO              执行。由 (x, y, z, roll, pitch, yaw) 反解五个关节（so101_kinematics.py），五个关节同时走过去
  MOVE_GRASP              执行。末端沿底座坐标系 ±X/±Y/±Z 平移指定距离，腕旋保持不动
  后两个用的运动学模型没有在真臂上标定过：到位与否按编码器读数判定，真实末端的位置和姿态精度没有量过。
  臂关节停稳后还差一截时会补偿（见 TRIM_* 常量旁的说明）。

用法：
    python seeed_ros.py teach park|chest|gripper_open|gripper_closed   只读：把当前位置记成预设
    python seeed_ros.py local      不用 ROS，在终端里按同一套协议逐条发请求（Windows 上自测用）
    python seeed_ros.py run        启动 ROS 2 节点（需要 rclpy，一般在 Linux/Spark 上）

⚠ 验证状态：2026-10-12 在真臂上用 local 模式把七个动作都跑成功了（空爪，按编码器读数判定到位）；
  单命令 Topic：2026-10-21 在 Spark 上接 Isaac 仿真从臂，经真实 DDS 验证过。
"""
import argparse
import hashlib
import json
import math
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# 打包交付时，两个第三方依赖（pyserial、feetech-servo-sdk）解包放在本文件旁边的 deps/ 里。把它加进搜索路径，
# 目标机器就不用 pip 安装。开发目录里没有 deps/，那就用当前环境里装好的。
_DEPS = Path(__file__).with_name('deps')
if _DEPS.is_dir():
    sys.path.insert(0, str(_DEPS))

from joint_route import ARM_JOINTS, steps
from seeed_arm import (ACCELERATION, DEFAULT_ACC, DEFAULT_SPEED, FULL_TORQUE, GOAL_POSITION, GOAL_VELOCITY, JOINTS,
                       MARGIN, MAX_STEP, MIN_STEP, PRESENT_POSITION, TORQUE_LIMIT,
                       TEMP_LIMIT, TORQUE_ENABLE, USB_ID, ArmError, SeeedArm)

# ══════════ 1. 身份、Topic 和时间参数 ══════════

SCHEMA = 'so101.follower.v1'
DIRECT_CONTROL = os.environ.get('SO101_DIRECT_CONTROL', '1') == '1'
ROBOT_ID = os.environ.get('SO101_ROBOT_ID', 'so101-follower-seeed')
PRINCIPAL = 'operator'
# 仿真实例（--backend isaac）用环境变量换掉设备身份，免得对外报真机控制板的序列号
DEVICE_ID = os.environ.get('SO101_DEVICE_ID') or f'ch343:{USB_ID[2]}'
PROFILE_ID = os.environ.get('SO101_PROFILE_ID') or f'seeed-sts3215-{USB_ID[2]}'
PART_IDS_BY_SIDE = {'right': (2,)}        # 单臂：right → part_id 2；left/both 没有映射，会明确失败
ID_PATTERN = r'[A-Za-z0-9][A-Za-z0-9_-]{0,79}'
TEACH_PRESETS = ('park', 'chest', 'gripper_open', 'gripper_closed')

NAMESPACE = '/so101_follower'           # 只是 ROS 节点的命名空间；Topic 是下面两个全局名字
COMMAND_TOPIC = '/so101/command'        # 唯一的命令 Topic
RESULT_TOPIC = '/so101/result'          # 唯一的结果 Topic：每条命令一条终态回执
# 2026-11-04：状态主动推送。后台每采一次关节快照就在这里发一条，内容和 ARM_INSPECT 的结果（params.result）相同，
# 另带这次采样的编号和时刻（sample_seq、sampled_mono_ns），订阅方不用再轮询 ARM_INSPECT。ARM_INSPECT 照旧可用。
STATE_TOPIC = '/so101/state'
# 2026-11-04：跟随主臂（本节点是“遥控器”，采集容器不再控制从臂）。主臂节点把每次读数打包成 action
# （teleop/action/v1，positions 是 LeRobot 单位：五个关节度、夹爪 0~100）发到 ACTION_TOPIC。
# 收到新鲜的 action、又没有原动作在执行，从臂就跟着走；action 超过 FOLLOW_STALE_S 秒没来就停在最后的目标上。
# 对齐是操作者自己的事：这里不检查主从相差多少。2026-11-05 起跟随不加任何安全限制（用户：安全由人来保证）：
# 不限速、不限加速度、扭矩 100%、上电不查温度、目标不留限位余量（只夹在舵机限位寄存器 / 仿真关节范围以内，
# 越界的目标舵机和 Isaac 都不接受）。原动作开始时恢复它自己的扭矩上限和加速度。SO101_FOLLOW=0 关掉跟随。
ACTION_TOPIC = '/so101/action'
ACTION_SCHEMA = 'teleop/action/v1'
FOLLOW = os.environ.get('SO101_FOLLOW', '1') == '1'
FOLLOW_STALE_S = 0.5
FOLLOW_PERIOD = 0.05      # 最多每 0.05 秒（20 Hz）写一次目标

MAX_REQUEST_BYTES = 32768
MAX_TTL_S = 10            # 信封有效期上限
# 状态采样周期，也是 /so101/state 的推送周期。可用环境变量 SO101_FEEDBACK_PERIOD（秒）改。
# 仿真读数便宜，默认 0.2 秒；真机每次采样要在串口上读 30 个寄存器，保持原来的 0.5 秒。
FEEDBACK_PERIOD = float(os.environ.get('SO101_FEEDBACK_PERIOD')
                        or (.2 if os.environ.get('SO101_BACKEND') == 'isaac' else .5))
UPDATE_PERIOD = .2        # 多久检查一次执行中动作的状态，变了就发回执
FRESH_S = 1.5             # 反馈超过这么久算不新鲜
LEASE_IDLE = 120          # 空闲时的控制租约（秒）
LEASE_ACTIVE = 5          # 动作执行期间的租约；调用方每 2 秒发一次 heartbeat
PRESENT_VOLTAGE = (62, 1)                 # 单位 0.1V。seeed_arm.py 里没有定义这个寄存器

# ── 动作执行参数（单位 ticks）──
GRIPPER_STEP = 100        # 夹爪每个子操作最多走这么多
GRIPPER_TOLERANCE = 20    # 离端点这么近算到位
GRIPPER_BUDGET = 32       # 最多这么多个子操作
ARM_CHUNK = 120           # 每步最多走这么多。seeed_arm.move() 的上限是 180，留出 60 给"上一步还差的那一截"
ARM_ARRIVED = 8           # 离这一步的目标这么近就不再发
POSE_TOLERANCE = 30       # 全部走完后，每个关节离预设位这么近才算到位
SETTLE_STABLE_S = .4      # 位置这么久不变算停稳
SETTLE_TIMEOUT_S = 8
POLL_S = .08
HOLD_TOLERANCE = 8        # 停止后，关节离"收回的目标"这么近才算保持住了
HOLD_VERIFY_S = 1.5

# ── 停稳后的补偿 ──
# 这台臂的舵机是纯比例控制（寄存器 21/23：P=16、I=0），带着负载时会停在离目标还差一截的地方。
# 2026-10-12 真机：肘关节托着小臂和夹爪，三次都停在离目标 28~32 格处，末端因此差约 1cm。
# 做法：停稳后哪个关节还差，就把它的舵机目标再往前多给一截。扭矩上限寄存器（48）不动。
TRIM_ROUNDS = 3           # 最多补这么多次
TRIM_MAX = 60             # 每个关节的舵机目标最多比真正的目标多给这么多
TRIM_SPEED = 60           # 补偿时的关节速度（ticks/s）
STALL_STEP = 60           # 发了至少这么大的一步却几乎没动，才判为被挡住；更小的步子可能只是落在上面那一截里

# ── 位姿运动（ARM_MOVETO / MOVE_GRASP），判据和松甲直连版相同 ──
ARRIVAL_M = .005          # 走完后按编码器读数算正运动学，末端位置误差不超过这么多才算到位
ARRIVAL_RAD = .01         # 同上，姿态误差（只对 ARM_MOVETO）
BASE_CLEARANCE_M = .005   # 途中末端不许低于底座平面上方这么高；起点本来就更低时，以起点高度为准
PATH_SAMPLES = 20         # 检查途中高度时沿路取这么多个点
POSE_SPEED_RANGE = (60, 400)   # ARM_MOVETO 的 speed 换成关节速度后夹在这个范围里（ticks/s）
SPEED_FLOOR = 30          # 几个关节同时走时，行程短的按比例放慢，但不低于这个速度
DIRECTIONS = {'forward': (0, 1), 'backward': (0, -1), 'left': (1, 1), 'right': (1, -1),
              'up': (2, 1), 'down': (2, -1)}      # 方向 → (底座坐标系的第几个轴, 正负)


class Error(Exception):
    """带错误码的拒绝或失败。code 会原样出现在回执的 error.code / result.error_code 里。"""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


if not re.fullmatch(ID_PATTERN, ROBOT_ID):
    raise ValueError('SO101_ROBOT_ID 不合法')


# ══════════ 2. 请求格式：八字段信封，无密钥 ══════════

def canonical(value):
    """签名用的规范形式：键排序、无多余空格、UTF-8、中文不转义、不允许 NaN。"""
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def envelope(operation, payload, request_id, now=None):
    """Build an unsigned request; timestamps retain stale-request protection."""
    now = time.time() if now is None else now
    return {'schema': SCHEMA, 'request_id': request_id, 'robot_id': ROBOT_ID, 'principal': PRINCIPAL,
            'operation': operation, 'payload': payload, 'issued_at': now, 'expires_at': now + 5}


def normalize_request(packet):
    """Direct mode accepts operation/payload; this controller owns protocol defaults.

    Full envelopes retain sender timestamps and strict verification. Compact
    requests have no sender expiry; supply request_id when retrying one operation.
    """
    if not isinstance(packet, dict) or 'schema' in packet:
        return packet
    if not DIRECT_CONTROL:
        raise Error('INVALID_ENVELOPE', 'Compact requests require direct control mode')
    if not {'operation', 'payload'} <= set(packet) or set(packet) - {'operation', 'payload', 'request_id'}:
        raise Error('INVALID_ENVELOPE', 'Compact request requires operation/payload and optional request_id')
    operation, payload = packet['operation'], packet['payload']
    if not isinstance(operation, str) or not isinstance(payload, dict):
        raise Error('INVALID_PAYLOAD', 'Object payload and string operation required')
    payload = dict(payload)
    if operation == 'execute':
        action_id = payload.get('action_id')
        if not isinstance(action_id, str) or action_id not in CATALOG:
            raise Error('INVALID_ACTION', 'Unknown action_id')
        payload.setdefault('actionType', 'ExecutionAction')
        payload.setdefault('actionCategory', 'BuildinAction')
        payload.setdefault('part_id', 2)
        parameters = payload.setdefault('parameters', {})
        if not isinstance(parameters, dict):
            raise Error('INVALID_ARGUMENT', 'Object parameters required')
        parameters = dict(parameters)
        if action_id == 'ARM_MOVETO' and 'target_position' not in parameters:
            raise Error('INVALID_ARGUMENT', 'ARM_MOVETO requires target_position')
        if action_id != 'ARM_MOVETO':
            parameters.setdefault('side', 'right')
        # Reject invalid input before queueing, including malformed poses.
        payload['parameters'] = check_parameters(action_id, parameters)
        resolve_parts(action_id, payload['parameters'], payload['part_id'])
    request_id = packet.get('request_id', 'request-' + uuid.uuid4().hex)
    return envelope(operation, payload, request_id)


def verify(packet):
    """Validate structure, target, finite JSON values and request lifetime; no authentication."""
    fields = {'schema', 'request_id', 'robot_id', 'principal', 'operation', 'payload', 'issued_at', 'expires_at'}
    if not isinstance(packet, dict) or set(packet) != fields:
        raise Error('INVALID_ENVELOPE', 'Exact eight-field request envelope required')
    try:
        canonical(packet)
    except (ValueError, TypeError):
        raise Error('INVALID_ENVELOPE', 'Finite JSON values required') from None
    if packet['schema'] != SCHEMA or packet['robot_id'] != ROBOT_ID or packet['principal'] != PRINCIPAL:
        raise Error('WRONG_TARGET', 'Unknown schema, physical robot or principal')
    if not isinstance(packet['request_id'], str) or not re.fullmatch(ID_PATTERN, packet['request_id']):
        raise Error('INVALID_ID', 'ASCII request_id required')
    if not isinstance(packet['payload'], dict) or not isinstance(packet['operation'], str):
        raise Error('INVALID_PAYLOAD', 'Object payload and string operation required')
    issued, expires, now = packet['issued_at'], packet['expires_at'], time.time()
    if (any(type(x) not in (int, float) or not math.isfinite(x) for x in (issued, expires))
            or not 0 < expires - issued <= MAX_TTL_S or not issued - 2 <= now < expires):
        raise Error('EXPIRED', f'Request TTL must be at most {MAX_TTL_S} seconds and currently valid')
    return packet


# ══════════ 3. 七个元动作的目录和参数检查 ══════════

def _side(default='right', both=True):
    return {'semantic': 'enum', 'options': ['left', 'right', 'both'] if both else ['left', 'right'],
            'default': default}


CATALOG = {
    'ARM_MOVETO': {
        'name': '移动手臂到目标位置', 'definition': '将指定手臂移动到给定的目标位置和姿态', 'part_id': [1, 2],
        'parameters': {
            'target_position': {'semantic': 'pose_6d', 'unit': 'meter_radian',
                                'default': dict.fromkeys(('x', 'y', 'z', 'roll', 'pitch', 'yaw'), 0.0)},
            'speed': {'semantic': 'velocity', 'unit': 'm/s', 'default': .5, 'range': [.1, 2.0], 'optional': True}},
        'failure_message': '移动手臂到目标位置失败，请稍后重试'},
    'MOVE_GRASP': {
        'name': '相对移动手臂', 'definition': '将指定手臂的末端执行器沿给定方向移动指定距离', 'part_id': [1, 2],
        'parameters': {
            'distance': {'semantic': 'length', 'unit': 'meter', 'default': .1, 'range': [.01, 1.0]},
            'direction': {'semantic': 'enum', 'options': ['forward', 'backward', 'left', 'right', 'up', 'down'],
                          'default': 'forward'},
            'side': _side(both=False)},
        'failure_message': '移动手臂失败，请稍后重试'},
    'ARM_RELEASE': {
        'name': '释放手臂', 'definition': '将指定手臂从当前姿态安全放下；不会打开夹爪', 'part_id': [1, 2, 3],
        'parameters': {'side': _side('both')}, 'failure_message': '释放手臂失败，请稍后重试'},
    'LOOSEN': {
        'name': '打开夹爪', 'definition': '打开指定手臂对应的夹爪', 'part_id': [1, 2],
        'parameters': {'side': _side()}, 'failure_message': '打开夹爪失败，请稍后重试'},
    'GRASP': {
        'name': '闭合夹爪', 'definition': '闭合指定手臂对应的夹爪以抓取物体', 'part_id': [1, 2],
        'parameters': {'side': _side()}, 'failure_message': '闭合夹爪失败，请稍后重试'},
    'HOLD_ARM': {
        'name': '胸前保持手臂', 'definition': '将指定手臂抬到机器人胸前预设位置并持续保持', 'part_id': [1, 2, 3],
        'parameters': {'side': _side()}, 'failure_message': '手臂移动到胸前保持位置失败，请稍后重试'},
    'HOLD_GRASP': {
        'name': '胸前保持抓取', 'definition': '闭合指定夹爪抓取物体，然后将对应手臂抬到胸前预设位置并持续保持',
        'part_id': [1, 2, 3], 'parameters': {'side': _side()}, 'failure_message': '抓取物体并保持到胸前失败，请稍后重试'},
}
for _spec in CATALOG.values():
    _spec.update(actionType='ExecutionAction', actionCategory='BuildinAction')

# 动作 → 依次执行的内部命令。HOLD_GRASP 是"先闭爪、再去胸前"两步
COMMANDS = {'ARM_MOVETO': ['move_to'], 'MOVE_GRASP': ['move'], 'ARM_RELEASE': ['release'], 'LOOSEN': ['open_gripper'],
            'GRASP': ['close_gripper'], 'HOLD_ARM': ['hold_arm'], 'HOLD_GRASP': ['close_gripper', 'hold_arm']}
EXECUTORS = {'LOOSEN': 'bounded_gripper_steps', 'GRASP': 'bounded_gripper_steps',
             'ARM_RELEASE': 'taught_park_pose_joint_steps', 'HOLD_ARM': 'taught_chest_pose_joint_steps',
             'HOLD_GRASP': 'close_gripper_then_taught_chest_pose',
             'ARM_MOVETO': 'constrained_5dof_pose_ik_joint_interpolated',
             'MOVE_GRASP': 'relative_xyz_position_ik_wrist_roll_locked'}
PRESET_OF = {'release': 'park', 'hold_arm': 'chest'}


def joint_speed(speed_m_s):
    """ARM_MOVETO 的 speed（m/s）→ 关节速度（ticks/s）。

    只是按比例换算（默认的 0.5 m/s 对应 seeed_arm 的默认速度 200），没有标定成末端的真实线速度。
    """
    return int(max(POSE_SPEED_RANGE[0], min(POSE_SPEED_RANGE[1], round(100 + (speed_m_s - .1) * 250))))


def _number(value):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise Error('INVALID_ARGUMENT', 'Expected a finite number, not bool/string/NaN/Infinity')
    return float(value)


def check_parameters(action_id, parameters):
    """按目录补默认值并检查类型、范围、枚举；多出来的字段直接拒绝。"""
    spec = CATALOG[action_id]
    if not isinstance(parameters, dict) or set(parameters) - set(spec['parameters']):
        raise Error('INVALID_ARGUMENT', 'Unknown parameters or non-object input')
    out = {}
    for name, field in spec['parameters'].items():
        value = parameters.get(name, field['default'])
        if field['semantic'] == 'pose_6d':
            if not isinstance(value, dict) or set(value) != set(field['default']):
                raise Error('INVALID_ARGUMENT', 'pose_6d requires exactly x/y/z/roll/pitch/yaw')
            value = {k: _number(value[k]) for k in field['default']}
        elif 'options' in field:
            if not isinstance(value, str) or value not in field['options']:
                raise Error('INVALID_ARGUMENT', f'Invalid {name}')
        else:
            value = _number(value)
            lo, hi = field['range']
            if not lo <= value <= hi:
                raise Error('INVALID_ARGUMENT', f'{name} must be in [{lo}, {hi}]')
        out[name] = value
    return out


def resolve_parts(action_id, params, part_id):
    """side → part_id。没有映射的 side 明确失败，不悄悄改成单臂。"""
    if type(part_id) is not int:
        raise Error('INVALID_ARGUMENT', 'part_id must be an integer')
    if action_id == 'ARM_MOVETO':                        # 这个动作没有 side 参数，只看 part_id
        if part_id not in {p for parts in PART_IDS_BY_SIDE.values() for p in parts}:
            raise Error('TARGET_NOT_CONFIGURED', 'ARM_MOVETO needs an explicit configured part_id')
        parts = (part_id,)
    else:
        parts = PART_IDS_BY_SIDE.get(params['side'])
        if not parts:
            raise Error('TARGET_NOT_CONFIGURED', 'Requested side has no configured target; no implicit fallback')
        if parts != (part_id,):
            raise Error('INVALID_ARGUMENT', 'part_id conflicts with side mapping')
    if any(p not in CATALOG[action_id]['part_id'] for p in parts):
        raise Error('INVALID_ARGUMENT', 'Action does not support this part_id')
    return parts


def _monotonic_ns():
    """主机 CLOCK_MONOTONIC，纳秒。测试里 time 被换成只有 monotonic() 的假时钟时，按它换算。"""
    exact = getattr(time, 'monotonic_ns', None)
    return exact() if exact else int(time.monotonic() * 1e9)


# ══════════ 4. 从臂服务：会话、绑定、租约、请求分发 ══════════

class Follower:
    """协议的全部状态和逻辑，不依赖 ROS。ROS 节点和 local 模式都只是把 JSON 递给 handle()。"""

    def __init__(self, arm, data_dir, background=True):
        self.arm = arm
        self.config_path = Path(data_dir) / 'config.json'      # teach 记下的预设位和夹爪端点
        self.journal_path = Path(data_dir) / 'journal.json'    # epoch、已处理的请求、动作结果
        self.lock = threading.RLock()     # 保护本对象的状态和 journal
        self.io = threading.RLock()       # 串口同一时刻只能有一个人用
        self.cancel = threading.Event()
        self.session_id = uuid.uuid4().hex     # 每次进程启动（重新打开串口）换一个；bind 时要原样带回来
        self.revision = 0
        self.bound, self.lease_until, self.workspace_confirmed = False, 0., False
        self.active = None                # 正在执行的动作的 request_id
        self.updates = []                 # 还没对外发布终态的动作 request_id（给 ROS 节点轮询）
        self.snapshot = None              # 最近一次读数：{'at', 'mono', 'joints'} 或 {'at', 'mono', 'error'}
        self.sample_seq = 0               # 采样编号：每采一次加 1（本进程内），推送和 ARM_INSPECT 里都带着
        self.on_sample = None             # 每采一次就调用 on_sample(快照)：ROS 节点用它推 /so101/state
        self.follow_target = None         # 最近一条主臂 action：{'ticks', 'at', 'seq', 'process_id', 'action_id'}
        self.follow_applied = None        # 已经写给舵机的那一条
        self.following = False
        self.follow_problem = None
        self.unlimited = False            # 跟随去掉过扭矩上限和加速度限制：下一个原动作开始前要恢复
        self.issued, self.device_result = False, None
        self.goal = None                  # 预检时为 ARM_MOVETO / MOVE_GRASP 求出的目标，执行时用
        self._kinematics = None
        self.closed = False
        self.data = (json.loads(self.journal_path.read_text(encoding='utf-8')) if self.journal_path.exists()
                     else {'epoch': 0, 'requests': {}, 'jobs': {}})
        self.data['epoch'] += 1           # 重启后旧 epoch 一律失效
        self.epoch = self.data['epoch']
        for job in self.data['jobs'].values():           # 上次没跑完的动作：标记中断，绝不重放
            if job['state'] in ('QUEUED', 'RUNNING'):
                job.update(state='INTERRUPTED', success=False, error_code='AGENT_RESTART',
                           message='Not replayed; inspect before rebinding')
        self._persist()
        self.pool = ThreadPoolExecutor(1) if background else None
        if background:
            threading.Thread(target=self._sampler, daemon=True).start()
            threading.Thread(target=self._watchdog, daemon=True).start()

    def close(self):
        """停掉后台线程并关串口。不卸力：臂继续保持在原地。"""
        self.closed = True
        self.cancel.set()
        if self.pool:
            self.pool.shutdown(wait=True)
        with self.io:
            self.arm.close()

    def _persist(self):
        with self.lock:
            self.journal_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.journal_path.with_suffix('.tmp')
            tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1), encoding='utf-8')
            tmp.replace(self.journal_path)

    def config(self):
        """每次用的时候重新读文件，所以 teach 之后不用重启。"""
        return json.loads(self.config_path.read_text(encoding='utf-8')) if self.config_path.exists() else {}

    def kinematics(self):
        """运动学模型，第一次用到时才加载（需要 numpy）。加载不了只影响位姿类动作，夹爪和预设位照常可用。"""
        if self._kinematics is None:
            try:
                from so101_kinematics import Kinematics
                self._kinematics = Kinematics({name: (JOINTS[name].lo, JOINTS[name].hi) for name in ARM_JOINTS},
                                              MARGIN)
            except Exception as exc:
                raise Error('KINEMATICS_UNAVAILABLE',
                            f'位姿运动需要 numpy 和 models/so101_new_calib.urdf：{exc}') from exc
        return self._kinematics

    # ---- 4.1 读状态 ----
    def sample(self):
        """读一遍六个关节。读失败也记下来（当作断连），不抛出去。

        2026-11-04：每次采样带编号 seq 和读数时刻 sampled_mono_ns（读这六个关节的中间时刻，主机 CLOCK_MONOTONIC，
        同一台机器上的各个容器可以直接比较）；后端能说出数据源头时（仿真：Isaac 的步数和时刻）记在 source 里。
        采完调用 on_sample（ROS 节点用它推 /so101/state）。"""
        started = _monotonic_ns()
        now = {'at': time.time(), 'mono': time.monotonic()}
        try:
            with self.io:
                now['joints'] = [{
                    'id': j.id, 'name': name,
                    'position': self.arm.read(j.id, PRESENT_POSITION), 'goal': self.arm.read(j.id, GOAL_POSITION),
                    'torque': bool(self.arm.read(j.id, TORQUE_ENABLE)),
                    'temperature': self.arm.temperature(name),
                    'voltage_raw': self.arm.read(j.id, PRESENT_VOLTAGE),
                    'status': 0,          # seeed_arm.read() 只在舵机应答里的错误字节为 0 时才算读成功
                } for name, j in JOINTS.items()]
        except Exception as exc:
            now['error'] = str(exc)
        now['sampled_mono_ns'] = (started + _monotonic_ns()) // 2
        stamp = getattr(self.arm, 'source_stamp', None)
        if callable(stamp):
            try:
                now['source'] = stamp()
            except Exception:
                pass
        with self.lock:
            self.sample_seq += 1
            now['seq'] = self.sample_seq
        self.snapshot = now
        hook = self.on_sample
        if hook is not None:
            try:
                hook(now)
            except Exception:
                pass                      # 推送出问题不影响采样和 ARM_INSPECT
        return now

    def _fresh(self):
        snap = self.snapshot
        if snap is None or time.monotonic() - snap['mono'] > FEEDBACK_PERIOD:
            snap = self.sample()          # 后台采样线程没跟上（或没开）时，现读一次
        return snap

    def _fault(self, snap):
        """当前仍然存在的问题。不锁存：温度降下来它自己消失；本程序也没有"清故障"的命令。"""
        hot = [f"{j['name']} {j['temperature']}°C" for j in snap.get('joints', []) if j['temperature'] > TEMP_LIMIT]
        if hot:
            return {'code': 'TEMPERATURE', 'message': f'超过 {TEMP_LIMIT}°C：' + '，'.join(hot), 'at': snap['at']}
        return None

    def state(self, snap=None):
        """inspect 的结果，也是 /so101/state 推送的内容。snap 不给就用最近一次读数（太旧就现读）。"""
        snap = snap or self._fresh()
        age = time.monotonic() - snap['mono']
        sample = {'sample_seq': snap.get('seq'), 'sampled_mono_ns': snap.get('sampled_mono_ns'),
                  'sampled_unix': snap['at'], 'source': snap.get('source')}
        if 'error' in snap:
            native = {'connected': False, 'error': snap['error'], 'feedback_fresh': False, **sample}
        else:
            powered = any(j['torque'] for j in snap['joints'])
            native = {'connected': True, 'session_id': self.session_id, 'revision': self.revision,
                      'phase': 'MOVING' if self.active else 'HOLDING' if powered else 'CONNECTED_UNARMED',
                      'profile_id': PROFILE_ID, 'fault': self._fault(snap), 'feedback_age_s': round(age, 3),
                      'feedback_fresh': age <= FRESH_S, 'consent_active': self.workspace_confirmed,
                      'native_control_available': self._fault(snap) is None, 'joints': snap['joints'], **sample}
            try:                          # 末端当前位姿（按编码器读数算正运动学），给调用方挑目标用
                native['tool'] = self.kinematics().pose({j['name']: j['position'] for j in snap['joints']
                                                         if j['name'] in ARM_JOINTS})
            except Error:
                native['tool'] = None
            target = self.follow_target
            native['follow'] = {'enabled': FOLLOW, 'following': self.following,
                                'action_seq': target and target['seq'], 'action_process_id': target and target['process_id'],
                                'action_age_s': None if target is None else round(time.monotonic() - target['at'], 3)}
        with self.lock:
            return {'robot_id': ROBOT_ID, 'device_id': DEVICE_ID, 'bound': self.bound, 'epoch': self.epoch,
                    'lease_remaining_s': round(max(0., self.lease_until - time.monotonic()), 3),
                    'workspace_confirmed_for_ros': self.workspace_confirmed, 'active_request_id': self.active,
                    'native': native, 'control_mode': 'direct' if DIRECT_CONTROL else 'leased',
                    'requires_bind': not DIRECT_CONTROL, 'requires_epoch': not DIRECT_CONTROL,
                    'requires_heartbeat': not DIRECT_CONTROL, 'requires_workspace_confirmation': not DIRECT_CONTROL}

    # ---- 4.2 跟随主臂（/so101/action）----
    def offer_action(self, text):
        """收到 /so101/action 的一条消息。是有效的 teleop/action/v1 就换算成 ticks、记成最新目标；其他的不理。
        换算和 Perceptor 读观测时的 encoder_mapping 相同（同一张 JOINTS 表），所以从臂跟到位后观测和 action 是同一套数。"""
        try:
            message = json.loads(text)
            positions = message.get('positions')
            if message.get('schema_version') != ACTION_SCHEMA or not isinstance(positions, dict):
                return False
            ticks = {}
            for name, j in JOINTS.items():
                value = positions[name]
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                    return False
                tick = (j.lo + value / 100 * (j.hi - j.lo) if name == 'gripper'
                        else value * 4095 / 360 + (j.lo + j.hi) / 2)
                ticks[name] = int(round(max(j.lo, min(j.hi, tick))))   # 只夹在限位寄存器以内，不留余量
        except (ValueError, KeyError, TypeError, AttributeError):
            return False
        with self.lock:
            self.follow_target = {'ticks': ticks, 'at': time.monotonic(), 'seq': message.get('seq'),
                                  'process_id': message.get('process_id'), 'action_id': message.get('action_id')}
        return True

    def _follow_report(self, state, detail=None):
        if (state, detail) != self.follow_problem:
            target = self.follow_target or {}
            print(json.dumps({'event': 'follow', 'state': state, 'detail': detail, 'action_seq': target.get('seq'),
                              'action_process_id': target.get('process_id')}, ensure_ascii=False), flush=True)
            self.follow_problem = (state, detail)

    def follow_step(self):
        """跟随一步：有新鲜的 action、没有原动作在执行，就把六个关节的目标写过去。返回这一步写了没有。"""
        if not FOLLOW or self.closed:
            return False
        with self.lock:
            target, active = self.follow_target, self.active
        fresh = target is not None and time.monotonic() - target['at'] <= FOLLOW_STALE_S
        if not fresh or active is not None:
            if self.following:
                self.following = False            # 停在最后一个目标上，舵机保持通电
                self._follow_report('held', 'action stale' if not fresh else f'action {active} running')
            return False
        if target is self.follow_applied:
            return False
        try:
            with self.io:
                if self.active is not None:       # 拿到串口之前刚好受理了一个原动作：让给它
                    return False
                if not self.following:
                    self._unlimit_for_follow()
                for name, j in JOINTS.items():
                    self.arm.write(j.id, GOAL_POSITION, target['ticks'][name])
        except Exception as exc:
            self.following = False
            self._follow_report('error', f"{getattr(exc, 'code', type(exc).__name__)}: {exc}")
            return False
        self.follow_applied = target
        if not self.following:
            self.following = True
            self._follow_report('following')
        return True

    def _unlimit_for_follow(self):
        """跟随开始（调用方已拿着串口锁）：没通电的关节上电（不查温度）；已通电的关节也去掉限制：
        扭矩 100%，速度、加速度写 0（不限）。"""
        limp = [name for name, j in JOINTS.items() if j.id not in self.arm.enabled]
        if limp:
            self.arm.torque_on(limp, limits=False)
            self.issued = True
        self.unlimited = True
        for name, j in JOINTS.items():
            if name not in limp:
                self.arm.write(j.id, TORQUE_LIMIT, FULL_TORQUE)
                self.arm.write(j.id, GOAL_VELOCITY, 0)
                self.arm.write(j.id, ACCELERATION, 0)

    def _restore_primitive_limits(self):
        """原动作开始前：跟随去掉过的扭矩上限和加速度改回原动作用的值（速度每一步 move 自己写）。"""
        if not self.unlimited:
            return
        with self.io:
            for j in JOINTS.values():
                if j.id in self.arm.enabled:
                    self.arm.write(j.id, TORQUE_LIMIT, j.torque_cap)
                    self.arm.write(j.id, ACCELERATION, DEFAULT_ACC)
            self.unlimited = False

    def follow_loop(self, stopping):
        while not stopping.is_set() and not self.closed:
            try:
                self.follow_step()
            except Exception as exc:              # 不让跟随线程因为意外退出
                self._follow_report('error', repr(exc))
            stopping.wait(FOLLOW_PERIOD)

    def catalog(self):
        return {'robot_id': ROBOT_ID, 'device_id': DEVICE_ID,
                'part_ids_by_side': {side: list(parts) for side, parts in PART_IDS_BY_SIDE.items()},
                'actions': json.loads(json.dumps(CATALOG)), 'executors': dict(EXECUTORS),
                'configured': sorted(self.config()), 'stop_preserves_torque': True, 'automatic_fault_reset': False,
                'teaching': {'operation': 'teach', 'presets': list(TEACH_PRESETS),
                             'requires_idle': True, 'motion_started': False, 'torque_changed': False},
                'pose': {'frame': 'base_link', 'tool_link': 'gripper_frame_link', 'unit': 'meter_radian',
                         'orientation': 'Rz(yaw) @ Ry(pitch) @ Rx(roll)', 'arrival_position_mm': ARRIVAL_M * 1000,
                         'arrival_orientation_rad': ARRIVAL_RAD, 'path': 'joint_interpolated_not_cartesian_line',
                         'physically_calibrated': False, 'collision_checked': False},
                'mode': 'physical_follower_only',
                'control': {'mode': 'direct' if DIRECT_CONTROL else 'leased',
                            'requires_bind': not DIRECT_CONTROL, 'requires_epoch': not DIRECT_CONTROL,
                            'requires_heartbeat': not DIRECT_CONTROL, 'requires_workspace_confirmation': not DIRECT_CONTROL}}

    def _sampler(self):
        while not self.closed:
            self.sample()
            time.sleep(FEEDBACK_PERIOD)

    def _watchdog(self):
        """动作执行期间租约到期（调用方没按时发 heartbeat）就取消后续分段；执行线程自己会停下并保持。"""
        while not self.closed:
            if not DIRECT_CONTROL and self.active and time.monotonic() >= self.lease_until:
                self.cancel.set()
            time.sleep(.2)

    # ---- 4.2 请求入口 ----
    def handle(self, text, on_action_topic):
        """一条收到的 JSON 文本 → 一条回执（还没签名）。任何情况都返回回执，不抛异常。"""
        request_id = None
        try:
            request = verify(normalize_request(json.loads(text)))
            request_id = request['request_id']
            if on_action_topic != (request['operation'] == 'execute'):
                raise Error('WRONG_TOPIC', 'execute belongs on actions/request; control operations on control/request')
            return {'request_id': request_id, 'ok': True, 'result': self.dispatch(request)}
        except Error as exc:
            reply = {'ok': False, 'error': {'code': exc.code, 'message': str(exc)}}
        except ValueError as exc:                        # JSON 解析不了：没有 request_id 可以关联
            reply = {'ok': False, 'error': {'code': 'INVALID_JSON', 'message': str(exc)}}
        except Exception as exc:                         # 意料之外的错：结果不明，调用方应查询而不是重发
            reply = {'ok': False, 'error': {'code': 'INTERNAL_ERROR', 'message': f'{type(exc).__name__}: {exc}'},
                     'outcome_unknown': True}
        return {'request_id': request_id, **reply} if request_id else reply

    def _check_lease(self, epoch):
        if DIRECT_CONTROL:
            return
        if not self.bound or type(epoch) is not int or epoch != self.epoch:
            raise Error('STALE_CONTROL', 'Bind this physical arm and use the current epoch')
        if time.monotonic() >= self.lease_until:
            raise Error('LEASE_EXPIRED', 'Send authenticated heartbeat every 2 seconds while controlling')

    def dispatch(self, request):
        """已校验格式的请求 → result。拒绝时抛 Error。"""
        request_id, operation, payload = request['request_id'], request['operation'], request['payload']
        if DIRECT_CONTROL and operation in ('execute', 'stop', 'heartbeat', 'release_control', 'confirm_workspace'):
            payload = dict(payload)
            payload.setdefault('epoch', self.epoch)
            request = {**request, 'payload': payload}
        digest_request = request
        if DIRECT_CONTROL:
            digest_request = {**request, 'payload': {k: v for k, v in payload.items() if k != 'epoch'}}
        digest = hashlib.sha256(canonical({k: digest_request[k] for k in ('robot_id', 'principal', 'operation', 'payload')})
                                ).hexdigest()
        # 只读查询：不占 request_id，不需要绑定
        if operation in ('inspect', 'catalog'):
            if payload:
                raise Error('INVALID_FIELDS', 'Empty payload required')
            return self.state() if operation == 'inspect' else self.catalog()
        if operation == 'job':
            job = self.data['jobs'].get(payload.get('request_id')) if set(payload) == {'request_id'} else None
            if job is None:
                raise Error('NOT_FOUND', 'No operation with this ID')
            return dict(job)
        if operation == 'heartbeat':
            if set(payload) != {'epoch'}:
                raise Error('INVALID_FIELDS', 'epoch only')
            with self.lock:
                self._check_lease(payload['epoch'])
                seconds = LEASE_ACTIVE if self.active else LEASE_IDLE
                self.lease_until = time.monotonic() + seconds
                return {'epoch': self.epoch, 'lease_seconds': seconds}
        if operation == 'stop':                          # 停止不查租约：租约过期了也必须能停
            if set(payload) != {'epoch'} or (not DIRECT_CONTROL and (not self.bound or payload['epoch'] != self.epoch)):
                raise Error('STALE_CONTROL', 'Stop requires the bound physical session epoch')
            return self.stop()

        inline = None
        with self.lock:
            previous = self.data['requests'].get(request_id)
            if previous:                                 # 同一个 request_id 再来：内容一样就返回原结果，不再执行
                if previous['digest'] != digest:
                    raise Error('IDEMPOTENCY_CONFLICT', 'Same request_id with different content')
                return dict(self.data['jobs'].get(request_id, previous['result']))
            if operation == 'teach':
                if set(payload) != {'name'} or payload['name'] not in TEACH_PRESETS:
                    raise Error('INVALID_ARGUMENT', 'teach requires name: park/chest/gripper_open/gripper_closed')
                if self.active:
                    raise Error('BUSY', 'Cannot record a preset while an action is active')
                if not DIRECT_CONTROL:
                    self._check_lease(self.epoch)
                with self.io:
                    snap = self.sample()
                    if 'error' in snap:
                        raise Error('NOT_CONNECTED', snap['error'])
                    fault = self._fault(snap)
                    if fault:
                        raise Error('FAULT_LATCHED', fault['message'])
                    result = record_preset(self.arm, self.config_path.parent, payload['name'])
            elif operation == 'bind':
                if (set(payload) != {'device_id', 'controller_session_id', 'adopt_existing_session'}
                        or payload['device_id'] != DEVICE_ID):
                    raise Error('INVALID_BIND', 'Exact registered device and inspected controller session required')
                if self.active:
                    raise Error('BUSY', 'An action is still active')
                snap = self._fresh()
                if 'error' in snap:
                    raise Error('NOT_CONNECTED', snap['error'])
                if payload['controller_session_id'] != self.session_id or payload['adopt_existing_session'] is not True:
                    raise Error('BIND_CONFIRMATION_REQUIRED', 'Explicit adoption of the inspected session is required')
                self.epoch += 1
                self.data['epoch'] = self.epoch
                self.bound, self.workspace_confirmed = True, False       # 新绑定不继承之前的现场确认
                self.lease_until = time.monotonic() + LEASE_IDLE
                result = {'bound': True, 'epoch': self.epoch, 'session_id': self.session_id, 'device_id': DEVICE_ID,
                          'motion_started': False, 'fault_retained': self._fault(snap),
                          'binding_method': 'operator_confirmed_device_and_session'}
            elif operation == 'confirm_workspace':
                if (set(payload) != {'epoch', 'secured', 'clear'} or payload['secured'] is not True
                        or payload['clear'] is not True):
                    raise Error('WORKSPACE_NOT_CONFIRMED', 'Operator must confirm secured base and clear workspace')
                self._check_lease(payload['epoch'])
                if self.active:
                    raise Error('BUSY', 'Workspace confirmation is only accepted while idle')
                fault = self._fault(self._fresh())
                if fault:                                # 现场确认不能用来绕过故障
                    raise Error('FAULT_LATCHED', fault['message'])
                self.workspace_confirmed = True
                result = {'confirmed': True, 'epoch': self.epoch, 'motion_started': False}
            elif operation == 'release_control':
                if set(payload) != {'epoch'}:
                    raise Error('INVALID_FIELDS', 'epoch only')
                self._check_lease(payload['epoch'])
                if self.active:
                    raise Error('BUSY', 'Stop first; never release while action is running')
                self.bound, self.workspace_confirmed, self.lease_until = False, False, 0.
                self.epoch += 1                          # 旧 epoch 立刻作废
                self.data['epoch'] = self.epoch
                result = {'released': True, 'torque_changed': False}
            elif operation == 'execute':
                required = {'epoch', 'action_id', 'actionType', 'actionCategory', 'part_id', 'parameters'}
                if (set(payload) != required or payload['actionType'] != 'ExecutionAction'
                        or payload['actionCategory'] != 'BuildinAction'):
                    raise Error('INVALID_ACTION', 'Exact action fields required')
                if not isinstance(payload['action_id'], str) or payload['action_id'] not in CATALOG:
                    raise Error('UNKNOWN_ACTION', 'Unknown primitive')
                self._check_lease(payload['epoch'])
                if self.active:
                    raise Error('BUSY', 'Physical actions are not queued behind each other')
                self.cancel.clear()
                self.active = request_id
                self.lease_until = time.monotonic() + LEASE_ACTIVE
                result = {'request_id': request_id, 'action_id': payload['action_id'], 'state': 'QUEUED',
                          'success': False, 'accepted_at': time.time()}
                self.data['jobs'][request_id] = result
                self.updates.append(request_id)
                inline = request
            else:
                raise Error('UNSUPPORTED_OPERATION', 'Use inspect, catalog, bind, confirm_workspace, heartbeat, '
                                                     'execute, teach, stop, job or release_control')
            self.revision += 1
            self.data['requests'][request_id] = {'digest': digest, 'result': result}
            self._persist()
            reply = dict(result)
        if inline:                                       # 受理回执先返回，动作在后台线程里跑
            if self.pool:
                self.pool.submit(self._run_job, inline)
            else:
                self._run_job(inline)                    # 测试用：同步执行
        return reply

    # ---- 4.3 停止并保持 ----
    def stop(self):
        """取消后续分段，把正在通电的关节的目标收回到当前位置。不开爪、不卸力。"""
        self.cancel.set()
        deadline = time.monotonic() + 3
        while self.active and self.pool and time.monotonic() < deadline:     # 等执行线程走完手里这一步
            time.sleep(.05)
        hold = self.hold()
        return {'state': 'STOPPED' if hold['error'] is None else 'STOP_UNCONFIRMED', **hold}

    def hold(self):
        """让所有通电的关节停在原地并核对：把目标收回到当前位置，再确认它仍通电并停在那附近。

        正在运动的关节会先冲过一点再回来，所以最多等 HOLD_VERIFY_S 秒，而不是只看一眼。
        夹爪例外：它的目标保留不动，只核对仍在通电。夹着东西时目标本来就在物体"里面"，
        收回目标等于松手；它的扭矩上限为 50%，而且目标离实际位置最多一个子操作（100 ticks）。
        """
        try:
            with self.io:
                held = [j for j in JOINTS.values() if self.arm.read(j.id, TORQUE_ENABLE)]
                goal = {}
                for j in held:
                    if j.id != JOINTS['gripper'].id:
                        goal[j.id] = self.arm.read(j.id, PRESENT_POSITION)
                        self.arm.write(j.id, GOAL_POSITION, goal[j.id])
                deadline = time.monotonic() + HOLD_VERIFY_S
                while True:
                    steady = all(self.arm.read(j.id, TORQUE_ENABLE) and
                                 (j.id not in goal or
                                  abs(self.arm.read(j.id, PRESENT_POSITION) - goal[j.id]) <= HOLD_TOLERANCE)
                                 for j in held)
                    if steady or time.monotonic() >= deadline:
                        break
                    time.sleep(.1)
            return {'hold_confirmed': bool(held) and steady, 'phase': 'HOLDING' if held else 'CONNECTED_UNARMED',
                    'held_joint_ids': [j.id for j in held], 'error': None if steady else '关节没有停在收回的目标附近'}
        except Exception as exc:
            return {'hold_confirmed': False, 'phase': 'UNKNOWN', 'held_joint_ids': [], 'error': str(exc)}

    # ══════════ 5. 执行一个动作 ══════════

    def _run_job(self, request):
        """后台线程里跑完一个已受理的动作，把最终结果写进 journal。"""
        request_id, payload = request['request_id'], request['payload']
        action_id = payload['action_id']
        job = self.data['jobs'][request_id]
        completed, details = [], []
        self.issued, self.device_result = False, None
        with self.lock:
            job['state'] = 'RUNNING'
            self._persist()
        result = {'operation_id': uuid.uuid4().hex, 'request_id': request_id, 'action_id': payload['action_id']}
        try:
            params = check_parameters(action_id, payload['parameters'])
            resolve_parts(action_id, params, payload['part_id'])
            plan = COMMANDS[action_id]
            self._restore_primitive_limits()             # 刚跟随过主臂：先恢复原动作的扭矩上限和加速度
            self._guard(payload['epoch'])
            self._preflight(plan, params)                # 整套动作先检查一遍，通不过就一步都不走
            for command in plan:
                self._guard(payload['epoch'])
                details.append(f'{command}: ' + self._invoke(command, payload['epoch']))
                completed.append(command)
            result.update(state='SUCCEEDED', success=True, error_code=None, message='')
        except (Error, ArmError) as exc:
            result.update(state='FAILED', success=False, error_code=getattr(exc, 'code', 'ARM_ERROR'),
                          message=CATALOG[action_id]['failure_message'])
            details.append(str(exc))
            if self.issued:                              # 已经发过运动指令：停在原地并核对保持
                result['stop'] = self.hold()
                if result['stop']['error'] is not None:
                    result['state'] = 'STOP_UNCONFIRMED'
        except Exception as exc:
            result.update(state='FAILED', success=False, error_code='INTERNAL_ERROR', message=str(exc))
        result.update(completed_commands=completed, detail='; '.join(details), device_result=self.device_result,
                      object_grasp_verified=False,     # 夹爪合上不等于抓住了东西，这里从不声称抓取成功
                      finished_at=time.time())
        with self.lock:
            job.update(result)
            self.active = None
            self.revision += 1
            if time.monotonic() < self.lease_until:      # 动作结束，租约回到空闲时长
                self.lease_until = time.monotonic() + LEASE_IDLE
            self._persist()

    def _guard(self, epoch):
        """每发一步之前都查一遍：控制权还在、没被叫停、反馈新鲜、没有故障、现场确认过。"""
        self._check_lease(epoch)
        if self.cancel.is_set():
            raise Error('STOPPED', 'Action cancelled')
        snap = self._fresh()
        if 'error' in snap:
            raise Error('NOT_CONNECTED', snap['error'])
        fault = self._fault(snap)
        if fault:
            raise Error('FAULT_LATCHED', f"{fault['code']}: {fault['message']}")
        if not DIRECT_CONTROL and not self.workspace_confirmed:
            raise Error('WORKSPACE_NOT_CONFIRMED', 'This ROS binding has not received operator workspace confirmation')

    def _preflight(self, plan, params):
        config = self.config()
        for command in plan:
            if command in ('move_to', 'move'):
                self.goal = self._solve(command, params)
            if command in PRESET_OF:
                preset = config.get(PRESET_OF[command])
                if not preset:
                    raise Error('POSE_NOT_CONFIGURED', f'No taught {PRESET_OF[command]} pose for this arm; '
                                                       'no substitute pose is used')
                for name in ARM_JOINTS:
                    j = JOINTS[name]
                    if not j.lo + MARGIN <= preset[name] <= j.hi - MARGIN:
                        raise Error('POSE_INVALID', f'{PRESET_OF[command]}.{name}={preset[name]} 越出限位余量')

    def _invoke(self, command, epoch):
        """执行一个内部命令，返回一句说明。失败抛 Error。"""
        if command in ('open_gripper', 'close_gripper'):
            return self._gripper(command, epoch)
        if command in ('move_to', 'move'):
            return self._move_pose(epoch)
        return self._go_to(PRESET_OF[command], epoch)

    def _settle(self, name, epoch):
        """等一个关节停稳（位置 SETTLE_STABLE_S 内不变）。期间被叫停或租约到期就立刻抛出。"""
        with self.io:
            last = self.arm.position(name)
        since, deadline = time.monotonic(), time.monotonic() + SETTLE_TIMEOUT_S
        while time.monotonic() < deadline:
            time.sleep(POLL_S)
            if self.cancel.is_set():
                raise Error('STOPPED', 'Action cancelled')
            self._check_lease(epoch)
            with self.io:
                now = self.arm.position(name)
            if abs(now - last) > 2:
                last, since = now, time.monotonic()
            elif time.monotonic() - since >= SETTLE_STABLE_S:
                break
        return last

    def _energize(self, names):
        with self.io:
            limp = [name for name in names if JOINTS[name].id not in self.arm.enabled]
            if limp:
                self.arm.torque_on(limp)          # 在当前位置上电保持，不会跳动
                self.issued = True

    def _gripper(self, command, epoch):
        """夹爪分段走到端点。每段最多 GRIPPER_STEP；某一段没走到就失败。夹爪不做停稳后的补偿。"""
        j, config = JOINTS['gripper'], self.config()
        key = 'gripper_open' if command == 'open_gripper' else 'gripper_closed'
        # 没示教过端点时按 LeRobot 的约定：读数大的一头是张开。方向不对就用 teach 记一次
        target = config.get(key, j.hi - MARGIN if command == 'open_gripper' else j.lo + MARGIN)
        source = 'taught' if key in config else 'default from limit registers'
        self._energize(['gripper'])
        for _ in range(GRIPPER_BUDGET):
            self._guard(epoch)
            with self.io:
                position = self.arm.position('gripper')
            delta = target - position
            if abs(delta) <= GRIPPER_TOLERANCE:
                self.device_result = {'status': 'reached', 'position': position, 'target': target,
                                      'residual_ticks': delta, 'hold_confirmed': True, 'torque_retained': True}
                return f'jaw at {source} endpoint {target} (residual {delta} ticks); object grasp is not inferred'
            step = max(-GRIPPER_STEP, min(GRIPPER_STEP, delta))
            with self.io:
                self.arm.move('gripper', step)
                self.issued = True
            stopped = self._settle('gripper', epoch)
            if abs(position + step - stopped) > GRIPPER_TOLERANCE:
                # 停早了：可能夹到了东西，也可能出了问题。两种都不当成功
                self.device_result = {'status': 'settled_short', 'position': stopped, 'target': target,
                                      'residual_ticks': target - stopped, 'hold_confirmed': True}
                raise Error('TARGET_NOT_REACHED', 'Gripper settled short; endpoint not reached and grasp not verified')
        raise Error('ACTION_BUDGET', 'Bounded gripper action budget exhausted')

    def _go_to(self, preset_name, epoch):
        """五个臂关节走到示教过的预设位：按 joint_route 的顺序，一个关节一小段，每段等停稳。"""
        target = {name: self.config()[preset_name][name] for name in ARM_JOINTS}
        self._energize(ARM_JOINTS)                       # 先全部上电，免得一个关节动时别的关节被带着甩
        with self.io:
            now = {name: self.arm.position(name) for name in ARM_JOINTS}
        for name, goal in steps(now, target, ARM_CHUNK, ARM_ARRIVED):
            self._guard(epoch)
            with self.io:
                start = self.arm.position(name)          # 每步重新读：上一步可能差几格
                delta = max(-MAX_STEP, min(MAX_STEP, goal - start))
                if abs(delta) < MIN_STEP:
                    continue
                self.arm.move(name, delta, DEFAULT_SPEED)
                self.issued = True
            stopped = self._settle(name, epoch)
            if abs(delta) >= STALL_STEP and abs(stopped - start) < MIN_STEP:   # 被挡住或舵机没响应
                raise Error('TARGET_NOT_REACHED', f'{name} did not move towards {goal}')
        trim = dict.fromkeys(ARM_JOINTS, 0)
        for _ in range(TRIM_ROUNDS):                     # 带负载的关节会差一截：补几次
            if not self._trim(target, trim, epoch):
                break
        with self.io:
            final = {name: self.arm.position(name) for name in ARM_JOINTS}
        residual = {name: target[name] - final[name] for name in ARM_JOINTS}
        reached = all(abs(r) <= POSE_TOLERANCE for r in residual.values())
        self.device_result = {'status': 'reached' if reached else 'settled_short', 'preset': preset_name,
                              'position': final, 'residual_ticks': residual,
                              'trim_ticks': {name: extra for name, extra in trim.items() if extra},
                              'hold_confirmed': True, 'torque_retained': True}
        if not reached:
            raise Error('TARGET_NOT_REACHED', f'{preset_name} pose not reached; residual {residual}')
        return f'at taught {preset_name} pose, holding (max residual {max(map(abs, residual.values()))} ticks)'

    # ---- 位姿运动：ARM_MOVETO / MOVE_GRASP ----
    def _solve(self, command, params):
        """预检阶段：读当前关节位置，反解出五个关节的目标，并检查沿途高度。不发任何运动指令。"""
        model = self.kinematics()
        with self.io:
            now = {name: self.arm.position(name) for name in ARM_JOINTS}
        try:
            if command == 'move_to':                     # 绝对位姿：位置和姿态一起求解，求不出就报错，不退成只管位置
                pose = params['target_position']
                position, rpy = [pose['x'], pose['y'], pose['z']], [pose['roll'], pose['pitch'], pose['yaw']]
                solution, speed = model.solve_pose(position, rpy, now), joint_speed(params['speed'])
            else:                                        # 相对平移：在当前末端位置上加一段，腕旋锁住不动
                axis, sign = DIRECTIONS[params['direction']]
                position, rpy = [float(v) for v in model.fk(now)[0]], None
                position[axis] += sign * params['distance']
                solution, speed = model.solve_position(position, now), DEFAULT_SPEED
        except Exception as exc:
            if not hasattr(exc, 'code'):
                raise
            raise Error(exc.code, str(exc)) from exc     # 求解失败的错误码原样带出去
        # 五个关节是在关节空间里走直线过去的，末端并不走直线。沿途取样，只防"钻到底座平面以下"，不是碰撞检测
        floor = min(BASE_CLEARANCE_M, float(model.fk(now)[0][2]))
        for i in range(1, PATH_SAMPLES + 1):
            along = {name: now[name] + (solution['ticks'][name] - now[name]) * i / PATH_SAMPLES for name in ARM_JOINTS}
            height = float(model.fk(along)[0][2])
            if height < floor - 1e-6:
                raise Error('PATH_BELOW_BASE', f'途中末端会降到离底座平面 {height * 1000:.0f}mm 处'
                                               f'（下限 {floor * 1000:.0f}mm），请换起点或目标')
        return {'position': position, 'rpy': rpy, 'ticks': solution['ticks'], 'speed': speed}

    def _move_pose(self, epoch):
        """五个臂关节同时走到求解出的位置，再按编码器读数算正运动学，核对末端到没到。"""
        goal, model = self.goal, self.kinematics()
        self._energize(ARM_JOINTS)

        def measure():
            with self.io:
                ticks = {name: self.arm.position(name) for name in ARM_JOINTS}
            off, twist = model.error(ticks, goal['position'], goal['rpy'])
            return ticks, off, twist, off <= ARRIVAL_M and (twist is None or twist <= ARRIVAL_RAD)

        ticks, off, twist, reached = measure()
        trim = dict.fromkeys(ARM_JOINTS, 0)
        if not reached:                                  # 已经在目标上就不动
            self._move_together(goal['ticks'], goal['speed'], epoch)
            ticks, off, twist, reached = measure()
            for _ in range(TRIM_ROUNDS):                 # 带负载的关节会差一截：补几次，到没到仍按编码器读数判定
                if reached or not self._trim(goal['ticks'], trim, epoch):
                    break
                ticks, off, twist, reached = measure()
        self.device_result = {
            'status': 'reached' if reached else 'settled_short', 'tool': model.pose(ticks),
            'target_position_m': goal['position'], 'target_orientation_rpy': goal['rpy'],
            'position_error_mm': round(off * 1000, 2),
            'orientation_error_rad': None if twist is None else round(twist, 4),
            'position_only': goal['rpy'] is None, 'measurement_basis': 'encoder_FK',
            'trim_ticks': {name: extra for name, extra in trim.items() if extra},
            'hold_confirmed': True, 'torque_retained': True}
        if not reached:                                  # 补完仍然没到：如实报告误差，不当成功
            raise Error('TARGET_NOT_REACHED' if goal['rpy'] is None else 'POSE_NOT_REACHED',
                        f'settled {off * 1000:.1f}mm' + ('' if twist is None else f' / {twist:.4f}rad')
                        + ' from the target')
        return (f'tool at target by encoder FK (error {off * 1000:.1f}mm'
                + ('' if twist is None else f', {twist:.4f}rad') + '); physical TCP accuracy is not calibrated')

    def _move_together(self, target, speed, epoch):
        """五个臂关节同时走到 target（ticks）：行程最长的关节用 speed，其余按比例放慢，大家差不多同时到。

        行程长时分成几段，每段每个关节最多 ARM_CHUNK，等全部停稳再发下一段。
        """
        with self.io:
            start = {name: self.arm.position(name) for name in ARM_JOINTS}
        span = max(abs(target[name] - start[name]) for name in ARM_JOINTS)
        rounds = max(1, -(-span // ARM_CHUNK))
        for i in range(1, rounds + 1):
            self._guard(epoch)
            before = {}
            with self.io:
                for name in ARM_JOINTS:
                    here = self.arm.position(name)
                    waypoint = start[name] + round((target[name] - start[name]) * i / rounds)
                    delta = max(-MAX_STEP, min(MAX_STEP, waypoint - here))
                    if abs(delta) < MIN_STEP:
                        continue
                    pace = max(SPEED_FLOOR, round(speed * abs(target[name] - start[name]) / span))
                    self.arm.move(name, delta, pace)
                    self.issued = True
                    before[name] = (here, delta)
            for name, (here, delta) in before.items():
                moved = abs(self._settle(name, epoch) - here)
                if abs(delta) >= STALL_STEP and moved < MIN_STEP:   # 发了一段像样的行程却没动：被挡住或舵机没响应
                    raise Error('TARGET_NOT_REACHED', f'{name} did not move')

    def _trim(self, target, trim, epoch):
        """停稳后的补偿：哪个关节离 target 还差，就把它的舵机目标再往前多给一截（等于差的那一截）。

        trim 是 {关节: 舵机目标已经比 target 多给了多少}，就地更新；累计不超过 TRIM_MAX，也不越出限位余量。
        返回这一次有没有发出指令——一个都没发，说明没什么可补的了。
        真被东西挡住时这样做会多推一点，但推力仍受扭矩上限寄存器限制；补完还不到就由调用方报失败并原地保持。
        """
        self._guard(epoch)
        moved = []
        with self.io:
            for name in ARM_JOINTS:
                j, here = JOINTS[name], self.arm.position(name)
                short = target[name] - here
                extra = max(-TRIM_MAX, min(TRIM_MAX, trim[name] + short))
                command = max(j.lo + MARGIN, min(j.hi - MARGIN, target[name] + extra))
                step = max(-MAX_STEP, min(MAX_STEP, command - here))
                # 差得不到 MIN_STEP 的落在舵机死区里，不补；已经补到上限的也不再发
                if abs(short) < MIN_STEP or abs(step) < MIN_STEP or command - target[name] == trim[name]:
                    continue
                self.arm.move(name, step, TRIM_SPEED)
                self.issued = True
                trim[name] = here + step - target[name]
                moved.append(name)
        for name in moved:
            self._settle(name, epoch)
        return bool(moved)

    # ---- 给 ROS 节点轮询：哪些动作的状态要发布 ----
    def pop_updates(self):
        """返回 [(request_id, 动作当前结果, 是否终态)]；终态的只返回这一次。"""
        with self.lock:
            out = []
            for request_id in list(self.updates):
                job = dict(self.data['jobs'][request_id])
                final = job['state'] not in ('QUEUED', 'RUNNING')
                out.append((request_id, job, final))
                if final:
                    self.updates.remove(request_id)
            return out


# ══════════ 6. 单命令 Topic：命令 JSON ↔ Follower 的内部请求；ROS 节点只负责收发字符串 ══════════

# 七个原动作（CATALOG）之外，命令 Topic 上还能发的查询、停止、示教。名字沿用 perceptor 机器人配置里的
CONTROL_ACTIONS = {'ARM_INSPECT': 'inspect', 'ARM_CATALOG': 'catalog', 'ARM_STOP': 'stop',
                   'ARM_TEACH_CHEST': 'teach', 'ARM_TEACH_PARK': 'teach',
                   'GRIPPER_TEACH_OPEN': 'teach', 'GRIPPER_TEACH_CLOSED': 'teach'}
TEACH_NAME = {'ARM_TEACH_CHEST': 'chest', 'ARM_TEACH_PARK': 'park',
              'GRIPPER_TEACH_OPEN': 'gripper_open', 'GRIPPER_TEACH_CLOSED': 'gripper_closed'}
MAX_REQUEST_ID = 256


def internal_id(request_id):
    """调用方的 request_id → 内部请求编号。符合内部规则的原样用；否则用它的哈希（同一个 request_id 永远得到同一个编号）。"""
    if re.fullmatch(ID_PATTERN, request_id):
        return request_id
    return 'h-' + hashlib.sha256(request_id.encode('utf-8')).hexdigest()[:40]


class CommandTopic:
    """命令 Topic 上的一条 JSON → Follower.handle() 认的内部请求；动作结束 → 结果 Topic 上的一条 JSON。

    命令：{"action": "ARM_MOVETO", "params": {"request_id": "abc123", ...}}，只有这两个字段（多出来的忽略）。
      params.request_id 必填（非空字符串，最长 256），其余 params 是动作参数（ARM_MOVETO 要给全 x/y/z/roll/pitch/yaw）。
      action 是七个原动作之一时执行动作；是 CONTROL_ACTIONS 里的就转成对应的查询、停止、示教。单臂，部件固定是 2。
    回执：{"action": "ARM_MOVETO", "params": {"request_id": "abc123", "status": "SUCCEEDED"}}，每条命令只回一条：
      七个原动作在结束时回，其他立刻回。status 是 SUCCEEDED 或 FAILED；FAILED 时 params 里加 error {code, message}；
      查询、停止、示教成功时 params 里加 result。
    同一个 request_id 重发同样的命令不会再执行：已结束的立刻回原结果，还在执行的等结束时回；内容不同回 IDEMPOTENCY_CONFLICT。
    """

    def __init__(self, follower, emit):
        self.follower, self.emit = follower, emit
        self.lock = threading.Lock()
        self.waiting = {}            # 还没结束的动作：内部编号 → 调用方的 request_id

    @staticmethod
    def _reply(action, request_id, error=None, result=None):
        params = {'request_id': request_id, 'status': 'SUCCEEDED' if error is None else 'FAILED'}
        if error is not None:
            params['error'] = error
        if result is not None:
            params['result'] = result
        return {'action': action, 'params': params}

    @classmethod
    def _final_reply(cls, job, request_id):
        error = None
        if job['state'] != 'SUCCEEDED':
            error = {'code': job.get('error_code') or job['state'],
                     'message': job.get('detail') or job.get('message') or ''}
        return cls._reply(job['action_id'], request_id, error)

    def handle(self, text):
        """命令 Topic 收到的一条字符串。动作已受理时先不回，等 poll 看到它结束再回。"""
        with self.lock:
            reply = self._handle(text)
            if reply is not None:
                self.emit(reply)

    def poll(self):
        """动作结束就发它唯一的一条回执。"""
        with self.lock:
            for key, job, final in self.follower.pop_updates():
                if final:
                    self.emit(self._final_reply(job, self.waiting.pop(key, key)))

    def _handle(self, text):
        try:
            packet = json.loads(text)
        except (ValueError, TypeError) as exc:
            return self._reply(None, None, {'code': 'INVALID_JSON', 'message': str(exc)})
        if not isinstance(packet, dict):
            return self._reply(None, None, {'code': 'INVALID_ENVELOPE', 'message': 'JSON object required'})
        action = packet.get('action') if isinstance(packet.get('action'), str) else None
        params = packet.get('params')
        request_id = params.get('request_id') if isinstance(params, dict) and isinstance(params.get('request_id'), str) else None
        try:
            compact = self._convert(packet)
        except Error as exc:
            return self._reply(action, request_id, {'code': exc.code, 'message': str(exc)})
        execute = compact['operation'] == 'execute'
        reply = self.follower.handle(json.dumps(compact, ensure_ascii=False), execute)
        if not reply['ok']:
            error = {'code': reply['error']['code'], 'message': reply['error']['message']}
            if reply.get('outcome_unknown'):
                error['outcome_unknown'] = True          # 内部出错、结果不明：用同一个 request_id 重发，或发 ARM_INSPECT 看现场
            return self._reply(action, request_id, error)
        if not execute:
            return self._reply(action, request_id, result=reply['result'])
        job = reply['result']
        if job['state'] in ('QUEUED', 'RUNNING'):
            self.waiting[compact['request_id']] = request_id
            return None                                  # 已受理或还在执行：结束时由 poll 回执
        return self._final_reply(job, request_id)        # 同一个 request_id 重发，动作早已结束：回原结果

    @staticmethod
    def _convert(packet):
        """检查 action、params、params.request_id，换成内部的紧凑请求 {operation, payload, request_id}。不对就抛 Error。"""
        missing = [k for k in ('action', 'params') if k not in packet]
        if missing:
            raise Error('INVALID_ENVELOPE', 'action and params are required; missing ' + ', '.join(missing))
        action, params = packet['action'], packet['params']
        if not isinstance(params, dict):
            raise Error('INVALID_ARGUMENT', 'params must be a JSON object')
        request_id = params.get('request_id')
        if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > MAX_REQUEST_ID:
            raise Error('INVALID_ID', f'params.request_id is required: a non-empty string of at most {MAX_REQUEST_ID} characters')
        if not isinstance(action, str):
            raise Error('UNKNOWN_ACTION', 'action must be an action name')
        key = internal_id(request_id)
        params = {k: v for k, v in params.items() if k != 'request_id'}
        if action in CATALOG:
            payload = {'action_id': action, 'part_id': 2, 'parameters': params}
            return {'operation': 'execute', 'payload': payload, 'request_id': key}
        if action not in CONTROL_ACTIONS:
            raise Error('UNKNOWN_ACTION', 'Unknown action')
        if params:
            raise Error('INVALID_ARGUMENT', f'{action} takes no params besides request_id')
        operation = CONTROL_ACTIONS[action]
        payload = {'name': TEACH_NAME[action]} if operation == 'teach' else {}
        return {'operation': operation, 'payload': payload, 'request_id': key}


FOLLOWER_NODE = 'seeed_follower_actions'
# 2026-11-04：/so101/command 同一时间只能有一个从臂节点在听（so101-controller 和 so101-isaac 里的仿真节点用同一组话题）。
# 启动时最多等这么久让 DDS 发现别的节点；发现已有同名节点在听就退出（退出码 DUPLICATE_EXIT），不和它一起执行同一条命令。
COMMAND_GUARD_S = float(os.environ.get('SO101_COMMAND_GUARD_S') or 5)
DUPLICATE_EXIT = 3


def other_followers(node):
    """/so101/command 上除了自己还有几个从臂节点在听（按节点名认，ros2 topic echo 之类的不算）。
    节点不支持查询（测试桩）时返回 None。"""
    lookup = getattr(node, 'get_subscriptions_info_by_topic', None)
    if lookup is None:
        return None
    return max(0, sum(1 for info in lookup(COMMAND_TOPIC) if info.node_name == FOLLOWER_NODE) - 1)


def run_node(follower, namespace=NAMESPACE):
    """订阅 /so101/command，回执发到 /so101/result。只在直连模式下用（不需要 bind / epoch / 心跳）。"""
    if not DIRECT_CONTROL:
        raise SystemExit('单命令 Topic 只支持直连模式（SO101_DIRECT_CONTROL=1）')
    import rclpy                                         # 放在函数里：没装 ROS 的机器也能用 teach / local / 跑测试
    from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
    from std_msgs.msg import String
    from queue import Empty, Queue

    rclpy.init()
    node = rclpy.create_node(FOLLOWER_NODE, namespace=namespace)
    qos = QoSProfile(depth=16, reliability=ReliabilityPolicy.RELIABLE, durability=DurabilityPolicy.VOLATILE,
                     history=HistoryPolicy.KEEP_LAST)
    publisher = node.create_publisher(String, RESULT_TOPIC, qos)
    state_publisher = node.create_publisher(String, STATE_TOPIC, qos)
    outbox = Queue(128)                                  # 所有发布都从主循环走，rclpy 的发布不保证线程安全
    states = Queue(8)                                    # /so101/state 待发的状态；满了丢最旧的，只保留最新

    def emit(value):
        try:
            outbox.put_nowait(value)
        except Exception:
            pass                                         # 队列满了就丢这条；可以发 ARM_INSPECT 看现场

    def push_state(snap):
        """采样线程每采一次就调用：和 ARM_INSPECT 回执一样的形状 {action, params: {result}}，result 就是 ARM_INSPECT 的结果。"""
        value = {'action': 'ARM_STATE', 'params': {'status': 'SUCCEEDED', 'result': follower.state(snap)}}
        while True:
            try:
                states.put_nowait(value)
                return
            except Exception:
                try:
                    states.get_nowait()                  # 丢掉最旧的一条，再放这条
                except Exception:
                    return

    follower.on_sample = push_state

    commands = CommandTopic(follower, emit)

    def receive(message):
        if len(message.data.encode()) <= MAX_REQUEST_BYTES:
            commands.handle(message.data)

    subscription = node.create_subscription(String, COMMAND_TOPIC, receive, qos)
    deadline = time.monotonic() + COMMAND_GUARD_S
    while True:                                          # 等 DDS 发现：已经有别的从臂节点在听就不起第二个
        others = other_followers(node)
        if others is None or others or time.monotonic() >= deadline:
            break
        time.sleep(.2)
    if others:
        print(f'{COMMAND_TOPIC} 上已有另一个从臂节点（{FOLLOWER_NODE}）在听，本节点不启动。'
              '真机控制器 so101-controller 和 so101-isaac 里的仿真节点同一时间只能开一个。', flush=True)
        follower.on_sample = None
        del subscription
        node.destroy_node()
        rclpy.shutdown()
        raise SystemExit(DUPLICATE_EXIT)
    stopping = threading.Event()

    def receive_action(message):
        if len(message.data.encode()) <= MAX_REQUEST_BYTES:
            follower.offer_action(message.data)

    action_subscription = node.create_subscription(String, ACTION_TOPIC, receive_action, qos)

    def observe():
        while not stopping.is_set():
            commands.poll()
            stopping.wait(UPDATE_PERIOD)

    threading.Thread(target=observe, daemon=True).start()
    threading.Thread(target=follower.follow_loop, args=(stopping,), daemon=True).start()
    print(f'ROS 节点已启动：命令 {COMMAND_TOPIC}，结果 {RESULT_TOPIC}，状态推送 {STATE_TOPIC}（每 {FEEDBACK_PERIOD:g} 秒），'
          f'跟随 {ACTION_TOPIC}（{"开，不限速" if FOLLOW else "关"}），'
          f'robot_id={ROBOT_ID}，device_id={DEVICE_ID}，session={follower.session_id}')
    try:
        while rclpy.ok():
            rclpy.spin_once(node, timeout_sec=.02)
            for queue, target in ((outbox, publisher), (states, state_publisher)):
                while True:
                    try:
                        value = queue.get_nowait()
                    except Empty:
                        break
                    message = String()
                    message.data = json.dumps(value, ensure_ascii=False)
                    target.publish(message)
    finally:
        follower.on_sample = None
        stopping.set()
        del subscription, action_subscription
        node.destroy_node()
        if rclpy.ok():                                   # docker stop 时 ROS 可能已经自己关了，不再关第二次
            rclpy.shutdown()


# ══════════ 7. 命令行 ══════════

def record_preset(arm, data_dir, name):
    """Read positions and atomically save one preset; never write servo registers."""
    if name not in TEACH_PRESETS:
        raise Error('INVALID_ARGUMENT', 'Unknown teaching preset')
    path = Path(data_dir) / 'config.json'
    config = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    positions = arm.positions()
    kept, clamped = {}, {}
    for joint in (['gripper'] if name.startswith('gripper_') else ARM_JOINTS):
        j = JOINTS[joint]
        # 运动指令的目标必须离限位至少 MARGIN，所以贴着限位的读数（比如肘折到底）夹回余量之内再记
        kept[joint] = max(j.lo + MARGIN, min(j.hi - MARGIN, positions[joint]))
        if kept[joint] != positions[joint]:
            clamped[joint] = {'read': positions[joint], 'saved': kept[joint]}
    config[name] = kept['gripper'] if name.startswith('gripper_') else kept
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(config, ensure_ascii=False, indent=1), encoding='utf-8')
    tmp.replace(path)
    return {'name': name, 'value': config[name], 'clamped': clamped,
            'saved': True, 'motion_started': False, 'torque_changed': False}


def teach(arm, data_dir, name):
    result = record_preset(arm, data_dir, name)
    print(f'已记录 {name}:', result['value'])
    if result['clamped']:
        print('限位余量调整：', result['clamped'])


def local_console(follower):
    """不用 ROS，在终端里走同一套协议：每行一条命令，组信封、校验格式、交给 handle()，打印回执。

    为了少打字，epoch 和动作的固定字段会自动补上；发出去的仍是规范里的完整 JSON。
    """
    print('命令：inspect | catalog | bind | confirm | stop | job <request_id> | release | q\n'
          '      LOOSEN | GRASP | ARM_RELEASE | HOLD_ARM | HOLD_GRASP                 （动作会真的驱动臂）\n'
          '      ARM_MOVETO <x> <y> <z> <roll> <pitch> <yaw>     米和弧度；当前位姿见 inspect 里的 native.tool\n'
          '      MOVE_GRASP <forward|backward|left|right|up|down> <距离，米>')
    epoch = None

    def call(operation, payload):
        request_id = f'local-{uuid.uuid4().hex[:12]}'
        reply = follower.handle(json.dumps(envelope(operation, payload, request_id)),
                                operation == 'execute')
        print(json.dumps(reply, ensure_ascii=False, indent=1))
        return reply

    def keep_alive():                                    # 规范要求控制期间每 2 秒发一次 heartbeat
        while not follower.closed:
            if epoch is not None and follower.bound:
                follower.handle(json.dumps(envelope('heartbeat', {'epoch': epoch}, f'hb-{uuid.uuid4().hex[:12]}')), False)
            time.sleep(2)

    threading.Thread(target=keep_alive, daemon=True).start()
    while True:
        try:
            words = input('ros> ').split()
        except (EOFError, KeyboardInterrupt):
            break
        if not words:
            continue
        command = words[0]
        if command == 'q':
            break
        if command in ('inspect', 'catalog'):
            call(command, {})
        elif command == 'bind':
            reply = call('bind', {'device_id': DEVICE_ID, 'controller_session_id': follower.session_id,
                                  'adopt_existing_session': True})
            epoch = reply['result']['epoch'] if reply['ok'] else epoch
        elif command == 'confirm':
            input('请确认：底座已固定、活动范围无人无障碍。回车表示确认，Ctrl+C 取消。')
            call('confirm_workspace', {'epoch': epoch, 'secured': True, 'clear': True})
        elif command == 'stop':
            call('stop', {'epoch': epoch})
        elif command == 'release':
            call('release_control', {'epoch': epoch})
        elif command == 'job' and len(words) == 2:
            call('job', {'request_id': words[1]})
        elif command in CATALOG:
            parameters = {'side': 'right'}
            try:
                if command == 'ARM_MOVETO':
                    x, y, z, roll, pitch, yaw = map(float, words[1:])
                    parameters = {'target_position': {'x': x, 'y': y, 'z': z, 'roll': roll, 'pitch': pitch,
                                                      'yaw': yaw}}
                elif command == 'MOVE_GRASP':
                    parameters.update(direction=words[1], distance=float(words[2]))
            except (IndexError, ValueError):
                print('参数不对：ARM_MOVETO 要 6 个数，MOVE_GRASP 要方向和距离')
                continue
            reply = call('execute', {'epoch': epoch, 'action_id': command, 'actionType': 'ExecutionAction',
                                     'actionCategory': 'BuildinAction', 'part_id': 2, 'parameters': parameters})
            if reply['ok']:                              # 已受理：等到终态，再把最终结果打印一次
                job = follower.data['jobs'][reply['result']['request_id']]
                while job['state'] in ('QUEUED', 'RUNNING'):
                    time.sleep(.3)
                print(json.dumps({'request_id': job['request_id'], 'ok': True, 'result': job},
                                 ensure_ascii=False, indent=1))
        else:
            print('不认识的命令')


def main():
    parser = argparse.ArgumentParser(description='Seeed SO-101 从臂的 ROS 2 封装（so101.follower.v1）')
    parser.add_argument('mode', choices=['run', 'local', 'teach'])
    parser.add_argument('name', nargs='?', choices=['park', 'chest', 'gripper_open', 'gripper_closed'],
                        help='teach 时要记的预设名')
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).with_name('seeed_ros_data'),
                        help='放预设和请求记录的目录')
    parser.add_argument('--port', help='串口名；不填就按 USB 序列号自动找')
    parser.add_argument('--backend', choices=['seeed', 'isaac'], default=os.environ.get('SO101_BACKEND', 'seeed'),
                        help='seeed = 真实从臂（串口，默认）；isaac = Isaac 仿真从臂（采集容器的同机文件接口，见 sim_arm.py）')
    parser.add_argument('--isaac-ipc', default=os.environ.get('SO101_ISAAC_IPC', '/isaac/ipc'),
                        help='--backend isaac 时 Isaac 的 ipc 目录')
    parser.add_argument('--namespace', default=os.environ.get('SO101_NAMESPACE', NAMESPACE),
                        help='ROS 节点的命名空间；Topic 固定为 /so101/command 和 /so101/result')
    args = parser.parse_args()
    if args.mode == 'teach' and not args.name:
        parser.error('teach 后面要跟 park / chest / gripper_open / gripper_closed')
    try:
        if args.backend == 'isaac':
            from sim_arm import IsaacArm                 # 仿真从臂：接口和 SeeedArm 一样，只读检查，不拿租约
            arm = IsaacArm(args.isaac_ipc)
        else:
            arm = SeeedArm(args.port)                    # 找串口 → 打开 → 核对六个舵机的身份，全程只读
    except ArmError as exc:
        raise SystemExit(f'连不上 {"Isaac 仿真从臂" if args.backend == "isaac" else "Seeed 臂"}：{exc}')
    print(f'已连接 {arm.port_name}')
    if args.mode == 'teach':
        try:
            teach(arm, args.data_dir, args.name)
        finally:
            arm.close()
        return
    follower = Follower(arm, args.data_dir)
    try:
        if args.mode == 'local':
            local_console(follower)
        else:
            run_node(follower, args.namespace)
    finally:
        follower.close()                                 # 只关串口，不卸力；要卸力用 seeed_arm 的 release()
        print('已退出。臂保持通电停在原地。')


if __name__ == '__main__':
    main()
