#!/usr/bin/env bash
# 启动采集容器（v10r15）。镜像不在本机时从 images/ 下的离线包加载。
# 一台采集容器只采一台机器人：.env 的 COLLECTION_ROBOT=so101|bobac3|so101-sim。so101-sim 时同时启动 Isaac 仿真容器。
# 改了 .env 之后再执行一次本脚本就生效：采哪台机器人等配置变了而容器没被重建时，会自动重启采集容器。
set -euo pipefail
cd "$(dirname "$0")"
IMAGE=so101-collection:20261105-v10r16
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker load -i images/so101-collection-20261105-v10r16.tar.gz
fi
./write-image-env.sh
# 机器人配置只在容器启动时读取，先记下改之前的内容（文件不存在也照常往下走）
config_digest() { { cat config/robot.json config/redis-capture.json config/redis.password config/real-cameras.json 2>/dev/null || true; } | sha256sum; }
before=$(config_digest)
# 把 .env 写成 config/ 下的文件（采哪台机器人、上传服务器、主从臂、Redis、视频来源）
python3 ./apply-env.py
ROBOT=$(python3 -c 'import json; print(json.load(open("config/robot.json"))["robot"])')
# 挂载目录必须存在：读写挂载不存在就建（权限 700）并检查当前用户可写；只读挂载（配置目录、宿主机 /dev）只要求存在
docker compose config --format json | python3 -c 'import json,os,sys; v=json.load(sys.stdin)["services"]["collection"]["volumes"]; rw=[x["source"] for x in v if not x.get("read_only")]; [os.makedirs(p,mode=0o700,exist_ok=True) for p in rw]; bad=[p for p in rw if not os.access(p,os.W_OK)]+[x["source"] for x in v if x.get("read_only") and not os.path.exists(x["source"])]; sys.exit("Mount directory missing or not writable: "+str(bad)) if bad else None'
NAME=$(docker compose config --format json | python3 -c 'import json,sys; print(json.load(sys.stdin)["services"]["collection"]["container_name"])')
PROFILE=()
if [ "$ROBOT" = so101-sim ]; then
  ISAAC=$(docker compose --profile sim config --format json | python3 -c 'import json,sys; s=json.load(sys.stdin)["services"]["isaac"]; print(s["container_name"], s["image"])')
  ISAAC_NAME=${ISAAC%% *}; ISAAC_IMAGE=${ISAAC#* }
  if docker inspect "$ISAAC_NAME" >/dev/null 2>&1 && [ -z "$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$ISAAC_NAME")" ]; then
    echo "本机已经有一个不归本目录管的 $ISAAC_NAME 在用：沿用它，不再启动 Isaac"
  else
    if ! docker image inspect "$ISAAC_IMAGE" >/dev/null 2>&1; then
      docker load -i images/so101-runtime-isaac-locked-20260923.tar.gz
    fi
    # Isaac 和采集容器共用 <数据目录>/isaac-console；先以当前用户建好，免得 docker 以 root 建出采集容器写不了的目录
    DATA=$(docker compose config --format json | python3 -c 'import json,sys; print([x["source"] for x in json.load(sys.stdin)["services"]["collection"]["volumes"] if x["target"]=="/data"][0])')
    for sub in kit data computecache logs documents glcache ov; do mkdir -p -m 700 "$DATA/isaac-console/cache/$sub"; done
    chmod 700 "$DATA/isaac-console"
    SOURCE=$(docker compose --profile sim config --format json | python3 -c 'import json,sys; print([x["source"] for x in json.load(sys.stdin)["services"]["isaac"]["volumes"] if x["target"].endswith("/source")][0])')
    [ -d "$SOURCE" ] || { echo "找不到 Isaac 场景源码目录 $SOURCE（.env 的 ISAAC_SOURCE_DIR）"; exit 1; }
    PROFILE=(--profile sim)
  fi
fi
started=$(docker inspect -f '{{.Id}} {{.State.StartedAt}}' "$NAME" 2>/dev/null || true)
docker compose "${PROFILE[@]}" up -d --no-build
# compose 只在挂载、镜像等变化时重建容器；只改了机器人配置时它不会重启，这里补一次（正在采集的批次会中断，请在空闲时执行）
if [ -n "$started" ] && [ "$before" != "$(config_digest)" ] && [ "$started" = "$(docker inspect -f '{{.Id}} {{.State.StartedAt}}' "$NAME")" ]; then
  echo "机器人配置变了：重启 $NAME 使其生效"
  docker restart "$NAME" >/dev/null
fi
# 不再采仿真时，停掉本目录启动的 Isaac（不删除）
if [ "$ROBOT" != so101-sim ] && [ -n "$(docker compose --profile sim ps -q isaac 2>/dev/null)" ]; then
  echo "这台采集容器现在采 $ROBOT：停掉 Isaac 仿真"
  docker compose --profile sim stop isaac
fi
echo "采集容器 $NAME 采 $ROBOT；8910 就绪后可以用：curl -s http://127.0.0.1:8910/collection/status"
