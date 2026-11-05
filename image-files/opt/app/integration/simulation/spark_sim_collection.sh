#!/usr/bin/env bash
# Remote driver for the Isaac sim-collection acceptance, run on the Spark host.
# Subcommands: setup-env | collect | replay | tests | shutdown
# The SDK data layer runs in this run's own venv (pinned Seeed commit); the
# Isaac harness runs in the teleop container (course env untouched).
set -euo pipefail
RUN_ID="${RUN_ID:-20260922-a1}"
SDK_RUN=/home/nvidia/lerobot-so101-sdk/collection-validation-$RUN_ID
WS_HOST=/home/nvidia/Sim-to-Real-SO-101-Workshop
RUN_HOST=$WS_HOST/outputs/sdk-collection-$RUN_ID
ENV="${SDK_ENV:-$SDK_RUN/env}"
PY=$ENV/bin/python
export PYTHONPATH=$SDK_RUN/sdk/src:$SDK_RUN/sdk
export PYTHONUNBUFFERED=1
# Isolated HF cache: the host default /home/nvidia/.cache/huggingface is root-owned.
export HF_HOME=$SDK_RUN/hf-cache HF_HUB_OFFLINE=1
cmd=${1:?subcommand required: setup-env|collect|replay|tests|shutdown}
case "$cmd" in
  setup-env)
    python3 -m venv "$ENV"
    export HTTPS_PROXY=http://127.0.0.1:23138 HTTP_PROXY=http://127.0.0.1:23138 NO_PROXY=127.0.0.1,localhost
    "$ENV/bin/pip" install -q --upgrade pip
    "$ENV/bin/pip" install -q pytest pillow numpy pyarrow av \
      "lerobot[dataset] @ git+https://github.com/Seeed-Projects/lerobot.git@4aaff99be4a1d81568c08c8f0296b41b40c99ec4"
    "$PY" -c "from lerobot_sdk.adapters.seeed_dataset import upstream; upstream(); print('pinned Seeed upstream ok')"
    "$PY" -m pytest --version
    ;;
  collect)
    "$PY" "$SDK_RUN/sdk/examples/isaac_collect.py" \
      --ipc-dir "$RUN_HOST/ipc" --contract "$SDK_RUN/sdk/integration/contract.json" \
      --root "${COLLECT_ROOT:-$SDK_RUN/data-full}" --export-dir "${EXPORT_ROOT:-$SDK_RUN/export-full}" \
      --report "${COLLECT_REPORT:-$SDK_RUN/report/collection-report.json}" \
      --episodes 10 --fps 10 --min-frames 40
    ;;
  replay)
    "$PY" "$SDK_RUN/sdk/integration/isaac_replay.py" \
      --dataset-dir "${REPLAY_DATASET:-$SDK_RUN/export-full/exported}" --episode "${REPLAY_EPISODE:-0}" \
      --ipc-dir "$RUN_HOST/ipc" --contract "$SDK_RUN/sdk/integration/contract.json" \
      --out-dir "${REPLAY_OUT:-$SDK_RUN/replay}" --time-scale "${REPLAY_TIME_SCALE:-1}" --substeps "${REPLAY_SUBSTEPS:-1}"
    ;;
  tests)
    "$PY" -m pytest -q \
      "$SDK_RUN/sdk/tests/test_robot.py" \
      "$SDK_RUN/sdk/tests/test_contracts.py" \
      "$SDK_RUN/sdk/tests/test_more_boundaries.py" \
      "$SDK_RUN/sdk/tests/test_collection.py" \
      "$SDK_RUN/sdk/tests/test_collection_contracts.py"
    ;;
  shutdown)
    python3 "$SDK_RUN/sdk/integration/isaac_collection_launcher.py" "$RUN_ID" --shutdown
    ;;
  *) echo "unknown subcommand $cmd" >&2; exit 2;;
esac
