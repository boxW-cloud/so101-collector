FROM so101-follower:direct-20261026-portfix

USER root
COPY --chown=1000:1000 seeed/ /opt/so101/seeed/
COPY --chown=1000:1000 entry.sh /opt/so101/entry.sh
RUN chmod 755 /opt/so101/entry.sh && /usr/bin/python3 -m compileall -q /opt/so101/seeed
LABEL so101.backends="seeed,isaac" so101.controller="unified-20261104-statepush"
USER 1000:1000
