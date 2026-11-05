#!/bin/bash
# 2026-11-04：重建 so101-isaac，只在启动命令开头多一行：起仿真从臂的 ROS 节点（/sim/follower/run.sh supervise）。
# 镜像、GPU、网络、权限、挂载、环境变量都和 10-28 手工建的 so101-isaac 一样；
# 旧容器改名为 so101-isaac-before-follower-20261104 并停着，留作回滚。
# 回滚：docker stop so101-isaac && docker rm so101-isaac && docker rename so101-isaac-before-follower-20261104 so101-isaac
#       && docker update --restart unless-stopped so101-isaac && docker start so101-isaac
#       然后 bash /home/nvidia/so101-controller/start.sh（control.json is_sim=true）
set -euo pipefail
NAME=so101-isaac
OLD=so101-isaac-before-follower-20261104
IMAGE=so101-runtime-isaac:locked-20260923
C=/home/nvidia/so101-collection/data/isaac-console
SRC=/home/nvidia/Sim-to-Real-SO-101-Workshop/source
GENERATION=be0461213519417581438118fd980892
BACKUP=/home/nvidia/backups/so101-isaac-before-follower-20261104

grep -q "\"$GENERATION\"" "$C/generation.json" || { echo "generation.json 和旧容器的 generation 对不上" >&2; exit 1; }
[ -x "$C/follower/run.sh" ] || { echo "缺少 $C/follower/run.sh" >&2; exit 1; }
docker container inspect "$OLD" >/dev/null 2>&1 && { echo "$OLD 已存在，不重复执行" >&2; exit 1; }

CMD='set -e
# 2026-11-04：仿真从臂的 ROS 节点（/so101/command → /so101/result、/so101/state）跟 Isaac 一起起，后台常驻
if [ -x /sim/follower/run.sh ]; then /sim/follower/run.sh supervise >/dev/null 2>&1 & fi
ISAAC_SIM=/workspace/isaaclab/_isaac_sim
export CARB_APP_PATH="$ISAAC_SIM/kit" ISAAC_PATH="$ISAAC_SIM" EXP_PATH="$ISAAC_SIM/apps"
source "$ISAAC_SIM/setup_python_env.sh"
export PYTHONPATH="/workspace/Sim-to-Real-SO-101-Workshop/source:/sim:$PYTHONPATH"
unset DISPLAY HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
exec "$ISAAC_SIM/python.sh" /sim/isaac_harness.py --ipc-directory /sim/ipc --contract-file /sim/contract.json --generation '"$GENERATION"' --headless --enable_cameras --device cpu --livestream 0 --kit_args=--/app/extensions/registryEnabled=false'

mkdir -p "$BACKUP"
docker inspect "$NAME" > "$BACKUP/inspect-so101-isaac.json"
docker stop -t 30 "$NAME" >/dev/null
docker rename "$NAME" "$OLD"
docker update --restart no "$OLD" >/dev/null
docker run -d --name "$NAME" --gpus '"device=0"' --network host --ipc host --user root \
  --workdir /workspace/Sim-to-Real-SO-101-Workshop --entrypoint bash \
  --cap-drop ALL --cap-add DAC_OVERRIDE --security-opt no-new-privileges --security-opt label=disable \
  --restart unless-stopped -e OMP_NUM_THREADS=4 -e PYTHONUNBUFFERED=1 \
  -v "$C/cache/data:/root/.local/share/ov/data" -v "$C/cache/computecache:/root/.nv/ComputeCache" \
  -v "$C/cache/logs:/root/.nvidia-omniverse/logs" -v "$C/cache/ov:/root/.cache/ov" \
  -v "$SRC:/workspace/Sim-to-Real-SO-101-Workshop/source:ro" -v "$C:/sim" \
  -v "$C/cache/documents:/root/Documents" -v "$C/cache/kit:/isaac-sim/kit/cache" \
  -v "$C/cache/glcache:/root/.cache/nvidia/GLCache" \
  "$IMAGE" -c "$CMD" >/dev/null
echo "已重建 $NAME（旧容器 $OLD 停着留作回滚）"
