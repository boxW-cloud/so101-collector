#!/usr/bin/env bash
# 启动主臂节点容器 so101-leader（配置在同目录的 leader.json）。--check 只检查，不启动。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CONFIG="$ROOT/leader.json"
NAME=so101-leader

if [[ "${1:-}" != "" && "${1:-}" != "--check" ]] || (( $# > 1 )); then
  echo "用法：$0 [--check]" >&2
  exit 2
fi
value() { jq -er --arg key "$1" '.[$key] | select(. != null) | tostring' "$CONFIG"; }
IMAGE="$(value image)"
SERIAL="$(value leader_serial)"
RATE="$(value rate_hz)"
TOPIC="$(value topic)"
DOMAIN="$(value ros_domain_id)"
ROBOT_TYPE="$(value robot_type)"
REDIS_HOST="$(value redis_host)"
REDIS_PORT="$(value redis_port)"
REDIS_DB="$(value redis_db)"
[[ "$SERIAL" =~ ^[A-Za-z0-9]+$ ]] || { echo "leader_serial 不对：$SERIAL" >&2; exit 1; }
PORT="/dev/serial/by-id/usb-1a86_USB_Single_Serial_${SERIAL}-if00"
[[ -e "$PORT" ]] || { echo "主臂控制板没有接上：$PORT" >&2; exit 1; }
REAL="$(readlink -f "$PORT")"
docker image inspect "$IMAGE" >/dev/null 2>&1 || { echo "镜像不存在：$IMAGE（先运行 build.sh）" >&2; exit 1; }

running() { [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == true ]]; }

# 同一个串口同一时刻只能有一个程序读，否则两边的应答会串在一起。
# 列出占着主臂串口、又不属于本容器的进程（只看得到和当前用户同一身份的进程，采集容器就是这种）。
others() {
  local own="" pid
  if docker container inspect "$NAME" >/dev/null 2>&1; then own="$(docker inspect -f '{{.Id}}' "$NAME")"; fi
  for pid in $(fuser "$REAL" 2>/dev/null || true); do
    if [[ -n "$own" ]] && grep -q "$own" "/proc/$pid/cgroup" 2>/dev/null; then continue; fi
    echo "$pid"
  done
}

echo "image=$IMAGE serial=$SERIAL port=$REAL rate=${RATE}Hz topic=$TOPIC domain=$DOMAIN robot_type=$ROBOT_TYPE redis=$REDIS_HOST:$REDIS_PORT/$REDIS_DB container=$NAME"
BUSY="$(others)"
if [[ -n "$BUSY" ]]; then
  echo "主臂串口 $REAL 正被别的程序占用，不能启动：" >&2
  for pid in $BUSY; do ps -o pid=,args= -p "$pid" >&2 || true; done
  exit 1
fi
if [[ "${1:-}" == "--check" ]]; then
  if running "$NAME"; then echo "Would restart: $NAME"; fi
  echo "检查通过"
  exit 0
fi

if running "$NAME"; then docker stop "$NAME" >/dev/null; fi
if docker container inspect "$NAME" >/dev/null 2>&1; then docker rm "$NAME" >/dev/null; fi

# 宿主机 /dev 只读挂到 /hostdev，按序列号找主臂（拔插后 ttyACM 编号变了也能重连）；只放行 USB 串口这一类设备（主设备号 166）
docker run -d --name "$NAME" --restart unless-stopped --network host \
  --read-only --tmpfs /tmp:rw,size=67108864 --cap-drop ALL --security-opt no-new-privileges \
  -v /dev:/hostdev:ro --device-cgroup-rule 'c 166:* rmw' --group-add "$(stat -Lc %g "$REAL")" \
  -e "ROS_DOMAIN_ID=$DOMAIN" \
  "$IMAGE" run --serial "$SERIAL" --rate "$RATE" --topic "$TOPIC" --robot-type "$ROBOT_TYPE" \
  --redis-host "$REDIS_HOST" --redis-port "$REDIS_PORT" --redis-db "$REDIS_DB" --host-dev /hostdev >/dev/null

for _ in $(seq 1 20); do
  if docker logs "$NAME" 2>&1 | grep -q '"event": "leader_connected"'; then
    echo "已启动：$NAME，话题 $TOPIC（${RATE} Hz，ROS Domain $DOMAIN）"
    echo "查看：docker exec $NAME /opt/so101/leader/entry.sh ros topic echo $TOPIC --once"
    exit 0
  fi
  if ! running "$NAME"; then
    docker logs --tail 30 "$NAME" >&2
    echo "容器启动后退出了" >&2
    exit 1
  fi
  sleep 1
done
docker logs --tail 20 "$NAME" >&2
echo "20 秒内没连上主臂（容器仍在运行，每秒重试一次）" >&2
exit 1
