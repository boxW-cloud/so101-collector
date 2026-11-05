"""给从臂节点（seeed_ros.py run）发一条命令并等回执：命令发到 /so101/command，回执从 /so101/result 收。

要和节点在同一个 ROS_DOMAIN_ID。命令和回执都是 {action, params}，params 里带 request_id；每条命令只回一条终态（params.status）。无需密钥文件。

用法（entry.sh 把参数原样转给本程序）：
    python seeed_ros_client.py inspect                       查状态（ARM_INSPECT）
    python seeed_ros_client.py catalog                       动作目录（ARM_CATALOG）
    python seeed_ros_client.py HOLD_ARM                      七个原动作之一，等到最终结果；会真的驱动臂
    python seeed_ros_client.py MOVE_GRASP --direction up --distance 0.02
    python seeed_ros_client.py ARM_MOVETO --pose 0.17 0.01 0.12 2.7 0.04 1.69 --speed 0.5
    python seeed_ros_client.py LOOSEN --params '{"side": "right"}'      直接给 params JSON
    python seeed_ros_client.py stop                          停止并保持（ARM_STOP）
    python seeed_ros_client.py teach chest                   把当前姿态记成预设（ARM_TEACH_CHEST 等）
"""
import argparse
import json
import time
import uuid

from seeed_ros import CATALOG, COMMAND_TOPIC, RESULT_TOPIC, TEACH_NAME

RESEND_S = 1           # 动作要等结束才回执；这么久没回就用同一个 request_id 重发（幂等，不会重复执行）
REPLY_TIMEOUT_S = 15
ACTION_TIMEOUT_S = 120
SHORTCUTS = {'inspect': 'ARM_INSPECT', 'catalog': 'ARM_CATALOG', 'stop': 'ARM_STOP'}
TEACH = {name: action for action, name in TEACH_NAME.items()}


def command(action, params, request_id=None):
    """命令 Topic 上的一条命令：{action, params}，params 里带 request_id。"""
    return {'action': action, 'params': {'request_id': request_id or f'cli-{uuid.uuid4().hex[:12]}', **params}}


def build(args):
    """命令行参数 → (action, params)。参数不够时抛 ValueError，不发命令。"""
    if args.operation == 'teach':
        if args.name not in TEACH:
            raise ValueError('teach 后面要跟 park / chest / gripper_open / gripper_closed')
        return TEACH[args.name], {}
    if args.operation in SHORTCUTS:
        return SHORTCUTS[args.operation], {}
    action = args.operation
    if args.params is not None:
        params = json.loads(args.params)
        if not isinstance(params, dict):
            raise ValueError('--params 必须是 JSON 对象')
        return action, params
    if action == 'ARM_MOVETO':
        if not args.pose:
            raise ValueError('ARM_MOVETO 需要 --pose X Y Z ROLL PITCH YAW（米、弧度）')
        params = {'target_position': dict(zip(('x', 'y', 'z', 'roll', 'pitch', 'yaw'), args.pose))}
        if args.speed is not None:
            params['speed'] = args.speed
        return action, params
    params = {} if args.side is None else {'side': args.side}
    if action == 'MOVE_GRASP':
        if args.direction is None or args.distance is None:
            raise ValueError('MOVE_GRASP 需要 --direction 和 --distance')   # 不用节点的默认值（向前 10 厘米）
        params.update(direction=args.direction, distance=args.distance)
    return action, params


class Client:
    def __init__(self):
        import rclpy                                     # 放在这里：没装 ROS 也能 import 本文件、跑测试
        from rclpy.qos import DurabilityPolicy, HistoryPolicy, QoSProfile, ReliabilityPolicy
        from std_msgs.msg import String
        self.rclpy, self.String = rclpy, String
        rclpy.init()
        self.node = rclpy.create_node('so101_command_client_' + uuid.uuid4().hex[:8])
        qos = QoSProfile(depth=16, reliability=ReliabilityPolicy.RELIABLE, durability=DurabilityPolicy.VOLATILE,
                         history=HistoryPolicy.KEEP_LAST)
        self.publisher = self.node.create_publisher(String, COMMAND_TOPIC, qos)
        self.replies = []
        self.subscription = self.node.create_subscription(String, RESULT_TOPIC, self._receive, qos)

    def _receive(self, message):
        try:
            reply = json.loads(message.data)
        except (ValueError, TypeError):
            return
        if isinstance(reply, dict):
            self.replies.append(reply)

    def call(self, packet, timeout):
        """发一条命令，返回 params.request_id 对得上的那一条回执。没收到回执前每秒用同一个 request_id 重发（不会重复执行）。"""
        request_id = packet['params']['request_id']
        deadline = time.monotonic() + timeout
        sent = 0.
        while time.monotonic() < deadline:
            if self.publisher.get_subscription_count() and time.monotonic() - sent > RESEND_S:
                self.publisher.publish(self.String(data=json.dumps(packet, ensure_ascii=False)))
                sent = time.monotonic()
            self.rclpy.spin_once(self.node, timeout_sec=.05)
            while self.replies:
                reply = self.replies.pop(0)
                params = reply.get('params')
                if isinstance(params, dict) and params.get('request_id') == request_id:
                    return reply
        raise TimeoutError(f'没有等到结果。用同一个 request_id 重发：--request-id {request_id}（不会重复执行）')

    def close(self):
        self.node.destroy_node()
        self.rclpy.shutdown()


def main():
    parser = argparse.ArgumentParser(description='给从臂节点发一条命令（/so101/command → /so101/result）')
    parser.add_argument('operation', choices=['inspect', 'catalog', 'stop', 'teach'] + list(CATALOG))
    parser.add_argument('name', nargs='?', help='teach 时要记的预设：park / chest / gripper_open / gripper_closed')
    parser.add_argument('--side', help='夹爪和预设位动作的 side，不填用节点默认的 right')
    parser.add_argument('--pose', type=float, nargs=6, metavar=('X', 'Y', 'Z', 'ROLL', 'PITCH', 'YAW'),
                        help='ARM_MOVETO 的目标位姿：米和弧度')
    parser.add_argument('--speed', type=float, help='ARM_MOVETO 的 speed（m/s，0.1~2.0）')
    parser.add_argument('--direction', help='MOVE_GRASP 的方向：forward/backward/left/right/up/down')
    parser.add_argument('--distance', type=float, help='MOVE_GRASP 的距离（米，0.01~1.0）')
    parser.add_argument('--params', help='直接给出 params JSON，覆盖上面这些便捷参数')
    parser.add_argument('--request-id', help='重试同一次调用时沿用原来的 request_id')
    args = parser.parse_args()
    try:
        action, params = build(args)
    except ValueError as exc:
        parser.error(str(exc))
    packet = command(action, params, args.request_id)
    client = Client()
    try:
        reply = client.call(packet, ACTION_TIMEOUT_S if action in CATALOG else REPLY_TIMEOUT_S)
        print(json.dumps(reply, ensure_ascii=False, indent=1))
        if reply['params'].get('status') != 'SUCCEEDED':
            raise SystemExit(1)
    finally:
        client.close()


if __name__ == '__main__':
    main()
