#!/bin/bash
# 主臂节点镜像的入口。run（默认）= 常驻发布；check = 只读一轮并打印；ros = 执行 ros2 命令（例如 ros topic echo）。
set -eo pipefail
export PYTHONPATH=/opt/so101/seeed/deps            # 飞特 SDK（scservo_sdk）和 pyserial，来自基础镜像
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp
export FASTDDS_BUILTIN_TRANSPORTS=UDPv4
export ROS_AUTOMATIC_DISCOVERY_RANGE=SUBNET
export ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-96}"
export ROS_HOME=/tmp/ros
unset ROS_LOCALHOST_ONLY FASTRTPS_DEFAULT_PROFILES_FILE
source /opt/ros/jazzy/setup.bash
case "${1:-run}" in
  ros) shift; exec ros2 "$@" ;;
  *) exec /usr/bin/python3 -u /opt/so101/leader/leader_node.py "$@" ;;
esac
