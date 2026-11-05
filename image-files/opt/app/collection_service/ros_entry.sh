#!/usr/bin/env bash
set -e
export FASTRTPS_DEFAULT_PROFILES_FILE="$1"
export ROS_DOMAIN_ID="$2"
shift 2
unset DISPLAY VIRTUAL_ENV PYTHONHOME
export ISAAC_SIM=/workspace/isaaclab/_isaac_sim
export CARB_APP_PATH="$ISAAC_SIM/kit" ISAAC_PATH="$ISAAC_SIM" EXP_PATH="$ISAAC_SIM/apps"
source "$ISAAC_SIM/setup_python_env.sh"
export ROS_DISTRO=jazzy RMW_IMPLEMENTATION=rmw_fastrtps_cpp ROS_LOCALHOST_ONLY=1
export LD_LIBRARY_PATH="$ISAAC_SIM/exts/isaacsim.ros2.bridge/jazzy/lib:$LD_LIBRARY_PATH"
export PYTHONPATH="/opt/edge:$ISAAC_SIM/exts/isaacsim.ros2.bridge/jazzy/rclpy:$PYTHONPATH"
exec "$ISAAC_SIM/python.sh" "$@"
