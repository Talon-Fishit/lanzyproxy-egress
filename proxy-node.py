#!/usr/bin/env python3
"""Per-node CONNECT proxy that always exits over IPv6.

Each WARP node carries its own IPv6 /64 on the CloudflareWARP tunnel
(2606:4700:110:<site>::/64), and the upstream quota is counted per /64.
Cloudflare's WARP IPv4 side is one shared NAT for the whole colo, so a
dual-stack target that happens to be dialled over IPv4 lands every node in the
same bucket.

So: resolve the target with AF_INET6 only and connect over IPv6. If the
target has no AAAA record, fall back to IPv4 — a small number of hosts, and
they share the NAT rather than being wrong.

The drain gate is honoured through /markers/<NODE_ID>: while the marker
exists the listener closes, so the arbiter can take a node out of the pool
instead of letting it serve traffic from a bucket a peer is already using.
"""

import errno
import os
import select
import socket
import sys
import threading
import time

PORT = int(os.environ.get("PROXY_PORT", "8888"))
NODE_ID = os.environ.get("NODE_ID", "?")
MARKER_DIR = os.environ.get("MARKER_DIR", "/markers")
MARKER = os.path.join(MARKER_DIR, NODE_ID)


def log(msg: str) -> None:
    sys.stdout.write(f"{time.strftime('%H:%M:%S')} proxy[{NODE_ID}] {msg}\n")
    sys.stdout.flush()


def resolve(host: str, port: int) -> list:
    """Addresses to try, IPv6 first."""
    out = []
    for family in (socket.AF_INET6, socket.AF_INET):
        try:
            infos = socket.getaddrinfo(host, port, family, socket.SOCK_STREAM)
        except socket.gaierror:
            continue
        for fam, _type, _proto, _canon, sockaddr in infos:
            if fam not in (socket.AF_INET6, socket.AF_INET):
                continue
            if fam == socket.AF_INET6:
                sockaddr = (sockaddr[0], sockaddr[1], 0, 0)
            if sockaddr not in out:
                out.append(sockaddr)
    return out


def pipe(a: socket.socket, b: socket.socket) -> None:
    a.setblocking(False)
    b.setblocking(False)
    poller = select.poll()
    poller.register(a, select.POLLIN)
    poller.register(b, select.POLLIN)
    try:
        while True:
            events = poller.poll(120)
            if not events:
                return
            for fd, _ in events:
                src = a if fd == a.fileno() else b
                dst = b if fd == a.fileno() else a
                try:
                    chunk = src.recv(65536)
                except OSError:
                    return
                if not chunk:
                    return
                try:
                    dst.setblocking(True)
                    dst.sendall(chunk)
                    dst.setblocking(False)
                except OSError:
                    return
    finally:
        poller.close()


def handle(client: socket.socket) -> None:
    try:
        client.settimeout(30)
        buf = b""
        while b"\r\n\r\n" not in buf and len(buf) < 65536:
            chunk = client.recv(4096)
            if not chunk:
                return
            buf += chunk
        if not buf:
            return
        line = buf.split(b"\r\n", 1)[0]
        try:
            method, target, _ = line.split(b" ", 2)
        except ValueError:
            client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return
        if method.upper() != b"CONNECT":
            client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
            return
        try:
            host, port = target.decode().rsplit(":", 1)
            port = int(port)
        except ValueError:
            client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return

        upstream = None
        for sockaddr in resolve(host, port):
            try:
                upstream = socket.socket(socket.AF_INET6 if len(sockaddr) == 4 else socket.AF_INET)
                upstream.settimeout(20)
                upstream.connect(sockaddr)
                break
            except OSError:
                if upstream:
                    upstream.close()
                upstream = None
        if upstream is None:
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return

        upstream.settimeout(120)
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        pipe(client, upstream)
    except OSError:
        pass
    finally:
        for s in (client, upstream):
            if s:
                try:
                    s.close()
                except OSError:
                    pass


def main() -> int:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", PORT))
    srv.listen(64)
    srv.setblocking(False)
    log(f"listening on :{PORT}")

    running = True
    while running:
        # Drain gate: while the arbiter has marked this node, the listener
        # closes so the engine's health check drops it immediately.
        if os.path.exists(MARKER):
            try:
                srv.close()
            except OSError:
                pass
            log("drained")
            while os.path.exists(MARKER):
                time.sleep(5)
            srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            srv.bind(("0.0.0.0", PORT))
            srv.listen(64)
            srv.setblocking(False)
            log("marker cleared, serving again")
            continue

        try:
            r, _, _ = select.select([srv], [], [], 5)
        except (OSError, ValueError):
            continue
        if srv in r:
            try:
                conn, _addr = srv.accept()
            except OSError as exc:
                if exc.errno in (errno.EAGAIN, errno.EWOULDBLOCK):
                    continue
                raise
            threading.Thread(target=handle, args=(conn,), daemon=True).start()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
