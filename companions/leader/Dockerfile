# SO101 主臂节点：基础镜像自带 ROS 2 Jazzy、Python 3.12 和飞特 SDK（/opt/so101/seeed/deps）
FROM so101-follower:direct-20261026-portfix

USER root
COPY --chown=1000:1000 leader_node.py entry.sh /opt/so101/leader/
RUN chmod 755 /opt/so101/leader/entry.sh && /usr/bin/python3 -m compileall -q /opt/so101/leader
LABEL so101.leader="20261104-v2"
USER 1000:1000
WORKDIR /opt/so101/leader
ENTRYPOINT ["/opt/so101/leader/entry.sh"]
