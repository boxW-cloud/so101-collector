#!/usr/bin/env bash
# 修改 image-files/opt/app/ 后重建应用镜像（离线，不联网）。
set -euo pipefail
cd "$(dirname "$0")"
RUNTIME=so101-collection-runtime:20261015-v10r6-rsync
IMAGE=so101-collection:20261105-v10r16
if ! docker image inspect "$RUNTIME" >/dev/null 2>&1; then
  docker load -i images/so101-collection-20261105-v10r16.tar.gz
fi
docker build --pull=false --network=none -t "$IMAGE" .
./write-image-env.sh
