#!/bin/bash
# SO101 仿真从臂的 ROS 节点（七个原动作、ARM_INSPECT、/so101/state 推送），跑在 so101-isaac 容器里。
# 2026-11-04 起取代 so101-controller 的仿真模式：Perceptor → /so101/command → 本节点 → ipc/ 文件 → Isaac。
# 代码和 so101-controller 是同一份（seeed/，SO101_BACKEND=isaac），用 Isaac 自带的 Python 3.11 和 ROS 2 桥里的 Jazzy rclpy。
# ROS 接口不变：ROS_DOMAIN_ID 96，命令 /so101/command，回执 /so101/result，状态推送 /so101/state（std_msgs/String，JSON）。
#
# so101-isaac 的启动命令里有一行 `/sim/follower/run.sh supervise &`：Isaac 起来就起节点，节点退出就重启。
# 在宿主机上：
#   docker exec so101-isaac /sim/follower/run.sh status    在跑就打印 PID
#   docker exec so101-isaac /sim/follower/run.sh stop      停掉，并且不再自动起（要用真机 so101-controller 时先执行这个）
#   docker exec so101-isaac /sim/follower/run.sh start     恢复自动起
#   docker exec so101-isaac /sim/follower/run.sh inspect   命令行查一次状态（其他参数同 so101-controller 的 entry.sh）
# 日志：宿主机 /home/nvidia/so101-collection/data/isaac-console/follower/node.log
set -eo pipefail
HERE=$(cd "$(dirname "$(readlink -f "$0")")" && pwd)
NAME=$(basename "$HERE")
PIDFILE=/tmp/$NAME.pid
OFF=$HERE/disabled                         # 有这个文件就不自动起（stop 建、start 删）
LOG=$HERE/node.log

running() {
  [ -f "$PIDFILE" ] || return 1
  local pid; pid=$(cat "$PIDFILE")
  [ -r "/proc/$pid/cmdline" ] && tr '\0' ' ' < "/proc/$pid/cmdline" | grep -q seeed_ros.py
}

say() { echo "$(date '+%F %T') [run.sh] $*" >> "$LOG"; }

case "${1:-serve}" in
  status)
    if running; then cat "$PIDFILE"; exit 0; fi
    if [ -f "$OFF" ]; then echo "已停用（$OFF）" >&2; fi
    exit 1 ;;
  stop)
    touch "$OFF"
    if running; then
      pid=$(cat "$PIDFILE"); kill -TERM "$pid"
      for _ in $(seq 1 30); do [ -d "/proc/$pid" ] || break; sleep 1; done
      if [ -d "/proc/$pid" ]; then kill -KILL "$pid"; fi
      echo "已停止仿真从臂节点（PID $pid），不再自动起；恢复：run.sh start"
    fi
    rm -f "$PIDFILE"; exit 0 ;;
  start)
    rm -f "$OFF"; echo "已恢复自动起，几秒内节点会起来"; exit 0 ;;
  supervise)
    since=$(date +%s)
    # 等这一轮 Isaac 起来：sim-status.json 每 1~2 秒更新一次
    until [ -f /sim/ipc/sim-status.json ] && [ "$(stat -c %Y /sim/ipc/sim-status.json)" -ge "$since" ]; do sleep 2; done
    say "Isaac 已就绪，开始守护节点"
    while true; do
      if [ -f "$OFF" ] || running; then sleep 5; continue; fi
      if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 5000000 ]; then mv -f "$LOG" "$LOG.1"; fi
      code=0; "$0" serve >> "$LOG" 2>&1 || code=$?
      rm -f "$PIDFILE"
      if [ -f "$OFF" ]; then say "节点已按 stop 停用"
      elif [ "$code" = 3 ]; then say "已有别的从臂节点在听 /so101/command，60 秒后再试"; sleep 60
      else say "节点退出（退出码 $code），10 秒后重启"; sleep 10; fi
    done ;;
esac

I=/workspace/isaaclab/_isaac_sim
source "$I/setup_python_env.sh"
B=/isaac-sim/exts/isaacsim.ros2.bridge/jazzy
# numpy 和 yaml（rclpy 要用）在 Isaac 自带的 pip 包目录里
PRE=$(ls -d /isaac-sim/extscache/omni.kit.pip_archive-*/pip_prebundle | head -1):/isaac-sim/exts/omni.pip.compute/pip_prebundle
export LD_LIBRARY_PATH="$B/lib:${LD_LIBRARY_PATH:-}"
export PYTHONPATH="$B/rclpy:$PRE:$HERE/seeed:$HERE/seeed/deps:${PYTHONPATH:-}"
export RMW_IMPLEMENTATION=rmw_fastrtps_cpp FASTDDS_BUILTIN_TRANSPORTS=UDPv4 ROS_AUTOMATIC_DISCOVERY_RANGE=SUBNET
export ROS_DOMAIN_ID="${ROS_DOMAIN_ID:-96}" ROS_HOME="/tmp/$NAME-ros"
unset ROS_LOCALHOST_ONLY FOLLOWER_AGENT_URL FASTRTPS_DEFAULT_PROFILES_FILE HTTP_PROXY HTTPS_PROXY ALL_PROXY
# 和 so101-controller 仿真模式完全相同的身份：Perceptor 看到的 robot_id、device_id 都不变
export SO101_BACKEND=isaac SO101_ISAAC_IPC=/sim/ipc SO101_NAMESPACE=/so101_follower_sim SO101_ROBOT_ID=so101-follower-isaac
export SO101_DEVICE_ID=isaac:so101-isaac-console SO101_PROFILE_ID=isaac-so101-console SO101_DIRECT_CONTROL=1
export SO101_RUNTIME="$HERE/data"
umask 002                                  # ipc/ 里的文件采集容器（uid 1000）也要能替换
PY="$I/kit/python/bin/python3"
cd /tmp

case "${1:-serve}" in
  serve)
    if running; then echo "仿真从臂节点已在运行（PID $(cat "$PIDFILE")）"; exit 0; fi
    echo $$ > "$PIDFILE"
    exec "$PY" -u "$HERE/seeed/seeed_ros.py" run --data-dir "$HERE/data" ;;
  *)
    exec "$PY" "$HERE/seeed/seeed_ros_client.py" "$@" ;;
esac
