# The per-node CONNECT proxy.
#
# It runs in the same network namespace as the WARP node (compose sets
# network_mode: service:alztrk-warp-<id>), so every connection it opens leaves
# through that node's tunnel.
FROM docker.io/library/python:3.12-slim

COPY proxy-node.py /usr/local/bin/proxy-node.py
RUN chmod +x /usr/local/bin/proxy-node.py

EXPOSE 8888

ENTRYPOINT ["python3", "/usr/local/bin/proxy-node.py"]
