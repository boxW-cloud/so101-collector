#!/usr/bin/env python3
"""把 .env 里的配置写成容器读取的 config/ 下的文件：
  上传服务器（COLLECTION_SYNC_*）       → config/upload.json + config/sync.password
  这台采集容器采哪台机器人（COLLECTION_ROBOT，v10r12 起一台容器只采一台）→ config/robot.json
  v10r15 起三种都从 Redis 采集队列取数据（采集容器只采集，不读主臂、不控制从臂）→ config/redis-capture.json
    so101-sim  = Isaac 仿真的 SO101：观测来自 Perceptor，action 来自主臂节点 so101-leader。
                 Redis 地址 COLLECTION_SO101_REDIS_HOST / _PORT / _DB（默认 127.0.0.1:6379 / 0，即本机），帧率 COLLECTION_SO101_FPS（默认 2）
    so101      = 真实 SO101：同上；摄像头（COLLECTION_REAL_CAMERAS）→ config/real-cameras.json，给 8910 视频和 Perceptor 用
    bobac3     = 机器人上 Perceptor 的 Redis 采集队列（COLLECTION_REDIS_*，+ config/redis.password）
  视频画面来源跟着机器人走 → config/video.json（so101-sim 是 Isaac 画面，其他是真实摄像头）

用法：改完 .env 后在本目录执行 ./apply-env.py（./start.sh 启动前也会自动执行）。
容器每次推送前都会重读 config/，所以只改上传服务器配置时不用重启容器。
.env 里没填 COLLECTION_SYNC_HOST 时不生成上传配置，继续使用手写的 config/upload.json（如有）。

生成的文件：
  config/upload.json    服务器地址、端口、账号、传输方式（644）
  config/sync.password  同步账号密码（600，只有 uid 1000 可读）
  config/robot.json     采哪台机器人（644）。改了要重启容器
  config/redis-capture.json  Redis 采集队列的地址、关节、相机、帧率、action 规则（644）。改了要重启容器
                             8910 开始采集时打开供数开关 collection_enabled、从队列取数据，结束就关掉
  config/real-cameras.json   真实 SO101 的摄像头（644）。改了要重启容器
  config/video.json     8910 视频 front / wrist 两路画面来自真实摄像头（real）还是 Isaac 仿真（sim）（644）。立即生效
改了机器人以后执行 ./start.sh：它会在这些配置变了时自动重启容器。
服务器上的目录：<COLLECTION_SYNC_DEPLOY_ROOT>/action-composer/data/artifacts/data_set/<id>/
（结束采集请求里带了 upload_url 时以请求为准）。
"""
import json
import os
import re
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
MARK = '由 .env 自动生成（apply-env.py）：请改 .env 后重新执行 ./apply-env.py，不要手改本文件'


def read_env(path):
    """按 docker compose 的简单规则读 .env：KEY=VALUE，# 开头是注释，值两边的引号会去掉。不执行任何 shell。"""
    values = {}
    for number, line in enumerate(path.read_text(encoding='utf-8').splitlines(), 1):
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            sys.exit(f'.env 第 {number} 行格式不对（应为 KEY=VALUE）：{line}')
        key, value = (part.strip() for part in line.split('=', 1))
        if len(value) >= 2 and value[0] == value[-1] and value[0] in '\'"':
            value = value[1:-1]
        values[key] = value
    return values


def write_file(path, text, mode):
    # 先写临时文件再替换：容器任何时候读到的都是完整文件
    temporary = path.with_name('.' + path.name + '.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
    with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
        stream.write(text)
    os.chmod(temporary, mode)
    os.replace(temporary, path)


def config_dir_of(env):
    config_dir = Path(env.get('COLLECTION_CONFIG_DIR') or 'config')
    if not config_dir.is_absolute():
        config_dir = HERE / config_dir       # 和 docker compose 一样，相对路径按本目录算
    config_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    return config_dir


ROBOTS = ('so101', 'bobac3', 'so101-sim')


def robot_of(env):
    """COLLECTION_ROBOT：这台采集容器采哪台机器人。仿真也是一台机器人（so101-sim）。"""
    value = env.get('COLLECTION_ROBOT', '').strip().lower()
    if value not in ROBOTS:
        sys.exit('.env 要写 COLLECTION_ROBOT=' + '、'.join(ROBOTS) + ' 之一（这台采集容器采哪台机器人）：'
                 'so101 = USB 真实主从臂，bobac3 = Redis 采集队列，so101-sim = Isaac 仿真')
    return value


def write_robot(config_dir, robot):
    target = config_dir / 'robot.json'
    write_file(target, json.dumps({'_说明': MARK, 'robot': robot}, ensure_ascii=False, indent=2) + '\n', 0o644)
    print(f'已生成 {target}：这台采集容器采 {robot}（重启容器后生效）')


def write_real_cameras(env, config_dir, robot):
    """真实 SO101（COLLECTION_ROBOT=so101）的摄像头：容器里的摄像头常驻进程读它们，给 8910 视频（Perceptor 的图片也从这里取）。
    不是 so101 时写 enabled=false。宿主机 /dev 只读挂在容器的 /hostdev。"""
    target = config_dir / 'real-cameras.json'
    enabled = robot == 'so101'
    value = {'_说明': MARK, 'enabled': enabled}
    if enabled:
        cameras = {}
        for item in filter(None, (part.strip() for part in env.get('COLLECTION_REAL_CAMERAS', '').split(','))):
            name, _, path = item.partition('=')
            name, path = name.strip(), path.strip()
            if not re.fullmatch(r'[a-z][a-z0-9_]{0,31}', name) or not path.startswith('/dev/'):
                sys.exit('.env 的 COLLECTION_REAL_CAMERAS 写法：名字=/dev/v4l/by-path/...，多个用逗号分开（名字用小写字母，例如 front、wrist）')
            mapped = '/hostdev/' + path[len('/dev/'):]
            if mapped in cameras.values():
                sys.exit('.env 的 COLLECTION_REAL_CAMERAS 里两个摄像头配成了同一个设备；同型号摄像头请用 /dev/v4l/by-path/...')
            cameras[name] = mapped
        if not cameras:
            sys.exit('.env 的 COLLECTION_REAL_CAMERAS 至少要配一个摄像头')
        value.update(cameras=cameras, width=640, height=480, fps=15, store='/tmp/so101-camera', jpeg_quality=80)
    write_file(target, json.dumps(value, ensure_ascii=False, indent=2) + '\n', 0o644)
    if enabled:
        print(f'已生成 {target}：摄像头 {", ".join(value["cameras"])}（重启容器后生效）')
    return value


# SO101 的 Redis 数据源（v10r15）：观测来自 Perceptor（六个关节：五个度、夹爪 0~100，图片 front / wrist 640x480），
# action 来自主臂节点 so101-leader（teleop/action/v1），按时间配对。robot_type 和 Perceptor / 主臂节点用的型号一致。
SO101_PROFILES = {
    'so101-sim': {'robot_id': 'so101-sim-capture', 'robot_type': 'so-101-follower_sim'},
    'so101': {'robot_id': 'so101-capture', 'robot_type': 'so-101-follower'},
}
SO101_JOINTS = ['shoulder_pan', 'shoulder_lift', 'elbow_flex', 'wrist_flex', 'wrist_roll', 'gripper']


def so101_redis(env, robot):
    value = {**SO101_PROFILES[robot], 'units': 'lerobot', 'joints': list(SO101_JOINTS),
             'cameras': {'front': [480, 640], 'wrist': [480, 640]}, 'action': 'teleop', 'camera_store': None,
             'state_source': 'arm_state', 'host': env.get('COLLECTION_SO101_REDIS_HOST', '') or '127.0.0.1'}
    for key, name, default in (('COLLECTION_SO101_REDIS_PORT', 'port', 6379), ('COLLECTION_SO101_REDIS_DB', 'db', 0),
                               ('COLLECTION_SO101_FPS', 'fps', 2)):
        raw = env.get(key, '') or str(default)
        if not raw.isdigit():
            sys.exit(f'.env 的 {key} 必须是数字')
        value[name] = int(raw)
    return value


def write_redis_capture(env, config_dir, robot):
    """这台采集容器的 Redis 数据源：SO101（so101-sim / so101）或 Bobac3。"""
    target = config_dir / 'redis-capture.json'
    password_path = config_dir / 'redis.password'
    value = {'_说明': MARK, 'enabled': True}
    if robot in SO101_PROFILES:
        value.update(so101_redis(env, robot))
        if password_path.exists():
            password_path.unlink()             # .215 的 Redis 没有密码
        write_file(target, json.dumps(value, ensure_ascii=False, indent=2) + '\n', 0o644)
        print(f'已生成 {target}：Redis {value["host"]}:{value["port"]}/{value["db"]}，目标 {value["robot_id"]}'
              f'（{value["robot_type"]}），观测来自 Perceptor、action 来自主臂节点（按时间配对），{value["fps"]} 帧/秒（重启容器后生效）')
        return value
    if robot == 'bobac3':                       # Bobac3：机器人上 Perceptor 的 Redis
        host = env.get('COLLECTION_REDIS_HOST', '')
        if not host:
            sys.exit('.env 的 COLLECTION_REDIS_HOST 不能为空（机器人上 Perceptor 用的 Redis 地址）')
        value['host'] = host
        for key, name, default in (('COLLECTION_REDIS_PORT', 'port', 6379), ('COLLECTION_REDIS_DB', 'db', 0),
                                   ('COLLECTION_REDIS_FPS', 'fps', 4)):
            raw = env.get(key, '') or str(default)
            if not raw.isdigit():
                sys.exit(f'.env 的 {key} 必须是数字')
            value[name] = int(raw)
        robot_id = env.get('COLLECTION_REDIS_ROBOT_ID', '') or 'bobac3-capture'
        if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', robot_id):
            sys.exit('.env 的 COLLECTION_REDIS_ROBOT_ID 只能用小写字母、数字和 -')
        value['robot_id'] = robot_id
        robot_type = env.get('COLLECTION_REDIS_ROBOT_TYPE', '') or 'bobac3'
        if not re.fullmatch(r'[a-z][a-z0-9_]{0,63}', robot_type) or robot_type == 'so101_follower':
            sys.exit('.env 的 COLLECTION_REDIS_ROBOT_TYPE 是写进数据集的机器人类型：小写字母、数字和下划线，不能是 so101_follower')
        value['robot_type'] = robot_type
        joints = [j.strip() for j in (env.get('COLLECTION_REDIS_JOINTS', '') or 'joint1,joint2,joint3,joint4,joint5,joint6').split(',') if j.strip()]
        if len(joints) != 6 or len(set(joints)) != 6:
            sys.exit('.env 的 COLLECTION_REDIS_JOINTS 要写 6 个不重复的关节名，用逗号分开（就是数据集里的关节名）')
        value['joints'] = joints
        cameras = {}
        for item in filter(None, (part.strip() for part in (env.get('COLLECTION_REDIS_CAMERAS', '') or 'front=400x640').split(','))):
            name, _, size = item.partition('=')
            height, _, width = size.strip().partition('x')
            if not name.strip() or not height.isdigit() or not width.isdigit():
                sys.exit('.env 的 COLLECTION_REDIS_CAMERAS 写法：相机名=高x宽，多个用逗号分开，例如 front=400x640')
            cameras[name.strip()] = [int(height), int(width)]
        value['cameras'] = cameras
        # 数据集的 action：next_state = 下一帧实测状态（默认，Bobac3 现在这样）；
        # paired = 动作队列里每帧成对的实际下发目标（teleop/action/v1，按观测编号配对，配不上的帧跳过）
        action = (env.get('COLLECTION_REDIS_ACTION', '') or 'next_state').strip().lower()
        if action not in ('next_state', 'paired'):
            sys.exit('.env 的 COLLECTION_REDIS_ACTION 只能是 next_state（下一帧实测状态）或 paired（动作队列里成对的实际下发目标）')
        value['action'] = action
        password = env.get('COLLECTION_REDIS_PASSWORD', '')
        if password:
            write_file(password_path, password + '\n', 0o600)
            value['password_file'] = 'redis.password'
        elif password_path.exists():
            password_path.unlink()
    write_file(target, json.dumps(value, ensure_ascii=False, indent=2) + '\n', 0o644)
    print(f'已生成 {target}：Redis {value["host"]}:{value["port"]}/{value["db"]}，目标 {value["robot_id"]}'
          f'（数据集 robot_type {value["robot_type"]}，关节 {",".join(value["joints"])}），'
          f'相机 {", ".join(f"{k}={h}x{w}" for k, (h, w) in value["cameras"].items())}，{value["fps"]} 帧/秒，'
          f'action={value["action"]}（重启容器后生效）')
    return value


def remove_old_configs(config_dir):
    """v10r15 起采集容器不读主臂、不控制从臂：以前的主从臂配置删掉。"""
    for name in ('real-arm.json', 'sim-leader.json'):
        path = config_dir / name
        if path.exists():
            path.unlink()
            print(f'已删除 {path}（v10r15 起不再使用）')


def write_video(config_dir, robot):
    """8910 视频：前端只取 front、wrist 两路，仿真时是 Isaac 画面，其他是真实摄像头。"""
    source = 'sim' if robot == 'so101-sim' else 'real'
    target = config_dir / 'video.json'
    write_file(target, json.dumps({'_说明': MARK, 'source': source}, ensure_ascii=False, indent=2) + '\n', 0o644)
    print(f'已生成 {target}：8910 视频画面来自{"真实摄像头" if source == "real" else " Isaac 仿真"}（立即生效，不用重启容器）')


def main():
    env = read_env(HERE / '.env')
    robot = robot_of(env)
    config_dir = config_dir_of(env)
    write_robot(config_dir, robot)
    write_redis_capture(env, config_dir, robot)
    write_real_cameras(env, config_dir, robot)
    remove_old_configs(config_dir)
    write_video(config_dir, robot)
    host = env.get('COLLECTION_SYNC_HOST', '')
    if not host:
        print('.env 没有填 COLLECTION_SYNC_HOST：不生成，继续使用 config/upload.json（如有）。')
        return 0
    password = env.get('COLLECTION_SYNC_PASSWORD', '')
    if not password:
        sys.exit('.env 的 COLLECTION_SYNC_PASSWORD 为空：账号密码登录需要填写密码')
    protocol = env.get('COLLECTION_SYNC_PROTOCOL') or 'ssh'
    sync = {'protocol': protocol, 'host': host, 'user': env.get('COLLECTION_SYNC_USER', ''),
            'password_file': 'sync.password'}
    port = env.get('COLLECTION_SYNC_PORT', '')
    if port:
        if not port.isdigit():
            sys.exit('.env 的 COLLECTION_SYNC_PORT 必须是数字')
        sync['port'] = int(port)
    if protocol == 'ssh':
        sync['accept_new_host_key'] = True   # 首次连接记住服务器指纹，之后指纹变了会拒绝连接
    deploy_root = env.get('COLLECTION_SYNC_DEPLOY_ROOT', '').rstrip('/')
    if deploy_root:
        if not deploy_root.startswith('/'):
            sys.exit('.env 的 COLLECTION_SYNC_DEPLOY_ROOT 必须是服务器上的绝对路径，例如 /home/lxk/dev2')
        sync['deploy_root'] = deploy_root

    config_dir = config_dir_of(env)
    target = config_dir / 'upload.json'
    if target.exists() and MARK not in target.read_text(encoding='utf-8'):
        backup = target.with_name('upload.json.bak-' + time.strftime('%Y%m%d%H%M%S'))
        target.rename(backup)
        print(f'原来手写的 upload.json 已备份为 {backup.name}')
    write_file(config_dir / 'sync.password', password + '\n', 0o600)
    stale = config_dir / 'platform.token'    # v10r7 的平台 token；v10r8 起不再通知平台
    if stale.exists():
        stale.unlink()
    write_file(target, json.dumps({'_说明': MARK, 'sync': sync}, ensure_ascii=False, indent=2) + '\n', 0o644)
    shown_port = sync.get('port', 22 if protocol == 'ssh' else 873)
    print(f'已生成 {target}：{protocol} {sync["user"]}@{host}:{shown_port}')
    if deploy_root:
        print(f'结束采集不带 upload_url 时，数据放到服务器 {deploy_root}/action-composer/data/artifacts/data_set/<id>/')
    else:
        print('注意：没填 COLLECTION_SYNC_DEPLOY_ROOT，结束采集不带 upload_url 的批次会在本地排队，直到填好并重新执行本脚本')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
