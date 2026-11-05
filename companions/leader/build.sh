#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
IMAGE="$(jq -er '.image | select(type == "string" and length > 0)' "$ROOT/leader.json")"
docker build -t "$IMAGE" -f "$ROOT/Dockerfile" "$ROOT"
