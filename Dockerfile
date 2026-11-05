# v10r15：沿用 r6 的运行基础镜像（已装 rsync、openssh-client），只替换应用代码，离线构建。
# v10r15 起采集容器不读主臂、不控制从臂；舵机驱动 feetech-servo-sdk 1.0.0 和 pyserial 3.5 仍在 /opt/app/vendor（旧的 integration 工具还在用）。
ARG RUNTIME_IMAGE=so101-collection-runtime:20261015-v10r6-rsync
FROM ${RUNTIME_IMAGE}
WORKDIR /opt/app
ENV HF_HUB_OFFLINE="1"
ENV HOME="/tmp/so101-home"
ENV LANG="C.UTF-8"
ENV LC_ALL="C.UTF-8"
ENV OMP_NUM_THREADS="2"
ENV PATH="/opt/runtime/bin:/usr/local/bin:/usr/bin:/bin"
ENV PYTHONDONTWRITEBYTECODE="1"
ENV PYTHONNOUSERSITE="1"
ENV PYTHONPATH="/opt/app:/opt/app/src:/opt/app/vendor"
ENV PYTHONUNBUFFERED="1"
ENV RMW_IMPLEMENTATION="rmw_fastrtps_cpp"
ENV ROS_DISTRO="jazzy"
ENV ROS_DOMAIN_ID="173"
ENV ROS_LOCALHOST_ONLY="1"
ENV ROS_LOG_DIR="/tmp/ros-log"
ENV SO101_COLLECTION_PROJECT="lab"
ENV SO101_COLLECTION_REST_AUTH="none"
ENV SO101_COLLECTION_REST_OPERATOR="operator"
ENV SO101_COLLECTION_TARGETS_JSON="/opt/app/collection-project-targets.json"
ENV SO101_FLOW_PLAN="/data/physical-flow-20261009-v2/launch.json"
ENV SO101_PORT="8910"
ENV SO101_PYTHON="/opt/runtime/bin/python"
ENV SO101_ROS_SETUP="/opt/ros/jazzy/setup.bash"
ENV SO101_SERVICE_ROLE="collection"
ENV SO101_TRAINING_ENV_LOCK="/opt/app/environment-lock.json"
ENV SO101_VIDEO_ROBOT_KEY="so101-isaac-console@isaac-20261007-wifi-v1"
ENV TRANSFORMERS_OFFLINE="1"
ENV WANDB_MODE="disabled"
# 旧接口（8920 测试工作台）结束采集时写本地目录 /delivery
ENV SO101_COLLECTION_DELIVERY_MODE="directory"
ENV SO101_COLLECTION_OUTPUT_ROOT="/delivery"
# v10 结束采集后的同步：配置文件（只读挂载）和持久化队列位置
ENV SO101_UPLOAD_CONFIG="/config/upload.json"
ENV SO101_SYNC_QUEUE_ROOT="/data/rsync-uploads"
COPY --chown=1000:1000 image-files/ /
# Windows source archives do not preserve executable mode bits.
RUN find /opt/app /opt/sim-retest -type f -name '*.sh' -exec chmod 0755 {} +
USER 1000:1000
EXPOSE 8910 8920
HEALTHCHECK --interval=15s --timeout=5s --start-period=45s --retries=3 CMD /opt/runtime/bin/python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8910/healthz',timeout=3); urllib.request.urlopen('http://127.0.0.1:8920/',timeout=1)"
ENTRYPOINT ["/opt/sim-retest/entrypoint.sh"]
