#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
CONFIG="${SO101_CONTROL_CONFIG:-$ROOT/control.json}"
NAME=so101-controller
OLD_NAMES=(so101-follower-direct so101-follower-sim
  so101-follower-control so101-follower-7524b964358e so101-follower-ros-20260924-v2)

if [[ "${1:-}" != "" && "${1:-}" != "--check" ]] || (( $# > 1 )); then
  echo "Usage: $0 [--check]" >&2
  exit 2
fi
CHECK_ONLY=false
[[ "${1:-}" == "--check" ]] && CHECK_ONLY=true
[[ -f "$CONFIG" ]] || { echo "Configuration not found: $CONFIG" >&2; exit 1; }

required_string() {
  jq -er --arg key "$1" '.[$key] | select(type == "string" and length > 0)' "$CONFIG"
}
data_dir() {
  local path
  path="$(required_string "$1")"
  if [[ "$path" == /* ]]; then
    printf '%s\n' "$path"
  else
    printf '%s/%s\n' "$ROOT" "$path"
  fi
}
IS_SIM="$(jq -er 'if (.is_sim | type) == "boolean" then (.is_sim | tostring) else error("is_sim must be a boolean") end' "$CONFIG")"
IMAGE="$(required_string image)"
PORT="$(required_string serial_port)"
ISAAC="$(required_string isaac_dir)"
REAL_DATA="$(data_dir real_data_dir)"
SIM_DATA="$(data_dir sim_data_dir)"
for path in "$PORT" "$ISAAC"; do
  [[ "$path" == /* ]] || { echo "Expected an absolute path: $path" >&2; exit 1; }
done
IMAGE_USER="$(docker image inspect --format '{{.Config.User}}' "$IMAGE")"
[[ "$IMAGE_USER" =~ ^[1-9][0-9]*:[0-9]+$ ]] || {
  echo "Image must declare a numeric non-root USER: $IMAGE" >&2
  exit 1
}
[[ -f "$ROOT/entry.sh" ]] || { echo "Missing $ROOT/entry.sh" >&2; exit 1; }

if [[ "$IS_SIM" == true ]]; then
  MODE=isaac
  DATA="$SIM_DATA"
  [[ -f "$ISAAC/ipc/ready.json" && -f "$ISAAC/contract.json" ]] || {
    echo "Isaac ready.json or contract.json is missing under $ISAAC" >&2
    exit 1
  }
  [[ -f "$DATA/config.json" || -f "$REAL_DATA/config.json" ]] || {
    echo "No simulation presets or real presets to initialize them" >&2
    exit 1
  }
else
  MODE=seeed
  DATA="$REAL_DATA"
  [[ -e "$PORT" && -f "$DATA/config.json" ]] || {
    echo "Serial port or real presets are missing: $PORT, $DATA/config.json" >&2
    exit 1
  }
  PORT="$(readlink -f "$PORT")"
fi

prepare_data() {
  mkdir -p "$DATA"
  for path in "$DATA" "$DATA/config.json" "$DATA/journal.json" "$DATA/journal.tmp"; do
    [[ -e "$path" ]] || continue
    if [[ "$(stat -Lc '%u:%g' "$path")" != "$IMAGE_USER" ]]; then
      chown "$IMAGE_USER" "$path" || {
        echo "Cannot prepare $path for image user $IMAGE_USER; run start.sh with sudo" >&2
        exit 1
      }
    fi
  done
  chmod u+rwx "$DATA"
  for path in "$DATA/config.json" "$DATA/journal.json" "$DATA/journal.tmp"; do
    [[ -f "$path" ]] && chmod u+rw "$path"
  done
  docker run --rm --network none --cap-drop ALL --security-opt no-new-privileges \
    --read-only --tmpfs /tmp:rw,size=134217728 -v "$DATA:/data" \
    --entrypoint /usr/bin/python3 "$IMAGE" -c \
    'import tempfile; tempfile.NamedTemporaryFile(dir="/data").close()'
}

running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == true ]]
}

echo "is_sim=$IS_SIM backend=$MODE image=$IMAGE data=$DATA container=$NAME"
if [[ "$CHECK_ONLY" == true ]]; then
  for old in "${OLD_NAMES[@]}" "$NAME"; do
    if running "$old"; then echo "Would stop: $old"; fi
  done
  exit 0
fi

prepare_data
if [[ "$IS_SIM" == true && ! -f "$DATA/config.json" ]]; then
  docker run --rm --network none \
    --cap-drop ALL --security-opt no-new-privileges --read-only \
    --tmpfs /tmp:rw,size=134217728 \
    -v "$DATA:/data" -v "$ISAAC/ipc:/isaac/ipc:ro" \
    -v "$ISAAC/contract.json:/isaac/contract.json:ro" \
    -v "$REAL_DATA/config.json:/real/config.json:ro" \
    --entrypoint /usr/bin/python3 "$IMAGE" /opt/so101/seeed/sim_arm.py presets \
    --from /real/config.json --to /data/config.json --ipc /isaac/ipc
fi

for old in "${OLD_NAMES[@]}" "$NAME"; do
  if running "$old"; then docker stop "$old" >/dev/null; fi
done
if docker container inspect "$NAME" >/dev/null 2>&1; then docker rm "$NAME" >/dev/null; fi

COMMON=(--name "$NAME" --network host --ipc host
  --cap-drop ALL --security-opt no-new-privileges --read-only
  --tmpfs /tmp:rw,size=134217728 --stop-timeout 30
  -v "$DATA:/data" -v "$ROOT/entry.sh:/opt/so101/entry.sh:ro"
  -e "SO101_BACKEND=$MODE" -e SO101_RUNTIME=/data -e SO101_DIRECT_CONTROL=1
  -e ROS_DOMAIN_ID=96 -e FASTDDS_BUILTIN_TRANSPORTS=UDPv4
  -e ROS_LOCALHOST_ONLY=0 -e ROS_AUTOMATIC_DISCOVERY_RANGE=SUBNET)
if [[ "$IS_SIM" == true ]]; then
  COMMON+=(-v "$ISAAC/ipc:/isaac/ipc" -v "$ISAAC/contract.json:/isaac/contract.json:ro"
    -e SO101_ISAAC_IPC=/isaac/ipc -e SO101_NAMESPACE=/so101_follower_sim
    -e SO101_ROBOT_ID=so101-follower-isaac
    -e SO101_DEVICE_ID=isaac:so101-isaac-console -e SO101_PROFILE_ID=isaac-so101-console)
else
  COMMON+=(--device "$PORT:$PORT" --group-add "$(stat -Lc %g "$PORT")"
    -e "SO101_PORT=$PORT" -e SO101_NAMESPACE=/so101_follower
    -e SO101_ROBOT_ID=so101-follower-seeed)
fi

docker run -d "${COMMON[@]}" "$IMAGE" >/dev/null
for _ in $(seq 1 30); do
  if docker logs "$NAME" 2>&1 | grep -q 'ROS 节点已启动'; then
    echo "Ready: $NAME ($MODE)"
    echo "Inspect: docker exec $NAME /opt/so101/entry.sh inspect"
    exit 0
  fi
  if ! running "$NAME"; then
    docker logs --tail 30 "$NAME" >&2
    echo "Control container exited during startup" >&2
    exit 1
  fi
  sleep 1
done
docker logs --tail 30 "$NAME" >&2
echo "Timed out waiting for the ROS node" >&2
exit 1
