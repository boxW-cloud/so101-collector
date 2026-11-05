#!/usr/bin/env bash
set -eo pipefail
source /opt/ros/jazzy/setup.bash
export PYTHONPATH=/opt/app:/opt/app/src:${PYTHONPATH:-}
export FASTRTPS_DEFAULT_PROFILES_FILE=/data/isaac-console/fastdds.xml FASTDDS_DEFAULT_PROFILES_FILE=/data/isaac-console/fastdds.xml
cd /opt/app
/opt/runtime/bin/python /opt/sim-retest/initialize_defaults.py
exec /opt/runtime/bin/python /opt/sim-retest/simulation_retest_supervisor.py
