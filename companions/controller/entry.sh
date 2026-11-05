#!/bin/bash
set -eo pipefail
export PYTHONPATH=/opt/so101/seeed:/opt/so101/seeed/deps
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp
export FASTDDS_BUILTIN_TRANSPORTS=UDPv4
export ROS_AUTOMATIC_DISCOVERY_RANGE=SUBNET
export ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-96}"
export ROS_HOME=/tmp/ros
export SO101_DIRECT_CONTROL="${SO101_DIRECT_CONTROL:-1}"
export SO101_ROBOT_ID="${SO101_ROBOT_ID:-so101-follower-seeed}"
unset ROS_LOCALHOST_ONLY FOLLOWER_AGENT_URL FASTRTPS_DEFAULT_PROFILES_FILE
source /opt/ros/jazzy/setup.bash
DATA="${SO101_RUNTIME:-/data/seeed-20261012}"
if [ "$#" -eq 0 ] || [ "$1" = serve ] || [ "$1" = run ]; then
  [ "$#" -eq 0 ] || shift
  mkdir -p "$DATA"
  if [ ! -f "$DATA/config.json" ]; then cp /opt/so101/seeed/default-config.json "$DATA/config.json"; fi
  exec /usr/bin/python3 -u /opt/so101/seeed/seeed_ros.py run --data-dir "$DATA" --port "${SO101_PORT:-/dev/ttyACM1}" "$@"
fi
case "$1" in
  ros) shift; exec ros2 "$@" ;;
  local) exec /usr/bin/python3 -u /opt/so101/seeed/seeed_ros.py "$@" --data-dir "$DATA" --port "${SO101_PORT:-/dev/ttyACM1}" ;;
  teach-local) shift; exec /usr/bin/python3 -u /opt/so101/seeed/seeed_ros.py teach "$@" --data-dir "$DATA" --port "${SO101_PORT:-/dev/ttyACM1}" ;;
  *) exec /usr/bin/python3 /opt/so101/seeed/seeed_ros_client.py "$@" ;;
esac
