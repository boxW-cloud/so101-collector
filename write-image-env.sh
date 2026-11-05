#!/usr/bin/env bash
# 把实际镜像 ID 写进 .env（容器启动时校验，防止误用别的镜像）。
set -euo pipefail
cd "$(dirname "$0")"
image_id=$(docker image inspect so101-collection:20261105-v10r15r1 --format '{{.Id}}')
touch .env
sed -i '/^COLLECTION_IMAGE_ID=/d' .env
printf 'COLLECTION_IMAGE_ID=%s\n' "$image_id" >> .env
