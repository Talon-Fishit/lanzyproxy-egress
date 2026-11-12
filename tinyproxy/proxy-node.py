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
from typing import Optional

PORT = int(os.environ.get("PROXY_PORT", "8888"))
NODE_ID = os.environ.get("NODE_ID", "?")
MARKER_DIR = os.environ.get("MARKER_DIR", "/markers")
MARKER = os.path.join(MARKER_DIR, NODE_ID)

# How long a tunnel may sit idle before the relay gives up on it.
IDLE_TIMEOUT_MS = int(os.environ.get("IDLE_TIMEOUT_MS", "300000"))


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


def http_connect(sock: socket.socket, host: str, port: int) -> None:
    """Ask the origin to open a tunnel to host:port."""
    sock.sendall(
        f"CONNECT {host}:{port} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Proxy-Connection: keep-alive\r\n\r\n".encode()
    )


def read_connect_response(sock: socket.socket) -> int:
    """Read the CONNECT response headers and return the status code."""
    buf = b""
    while b"\r\n\r\n" not in buf and len(buf) < 8192:
        chunk = sock.recv(4096)
        if not chunk:
            break
        buf += chunk
    if not buf:
        return 0
    first = buf.split(b"\r\n", 1)[0].split()
    if len(first) >= 2 and first[1].isdigit():
        return int(first[1])
    return 0


def pipe(a: socket.socket, b: socket.socket) -> None:
    """Shuttle bytes both ways until either side closes.

    The sockets are non-blocking, so a send can be short or refuse outright.
    Dropping the connection on a partial write is what makes a client see an
    EOF, so each direction keeps its own buffer and the poller watches for
    writability whenever a buffer still has data to send.
    """
    a.setblocking(False)
    b.setblocking(False)
    peers = {a: b, b: a}
    # pending[sock] holds the bytes still to be written to sock.
    pending = {a: bytearray(), b: bytearray()}
    poller = select.poll()
    for sock in (a, b):
        poller.register(sock, select.POLLIN)

    def flush(sock: socket.socket) -> bool:
        """Write what is pending for this socket. False when it is gone."""
        data = pending[sock]
        while data:
            try:
                sent = sock.send(data)
            except BlockingIOError:
                return True
            except OSError:
                return False
            if sent <= 0:
                return False
            del data[:sent]
        return True

    try:
        while True:
            for sock in (a, b):
                if pending[sock]:
                    poller.modify(sock, select.POLLIN | select.POLLOUT)
                else:
                    poller.modify(sock, select.POLLIN)

            # The poll timeout must be generous: a slow origin can leave the
            # tunnel idle for a while (a 6 MB JSON document takes the server
            # a moment to start writing), and closing on a short idle would
            # show up to the client as an unexpected EOF.
            ready = poller.poll(IDLE_TIMEOUT_MS)
            if not ready:
                continue
            for fd, mask in ready:
                sock = a if fd == a.fileno() else b
                peer = peers[sock]
                if mask & select.POLLOUT:
                    if not flush(sock):
                        return
                if mask & select.POLLIN:
                    try:
                        chunk = sock.recv(65536)
                    except BlockingIOError:
                        continue
                    except OSError:
                        return
                    if not chunk:
                        return
                    try:
                        sent = peer.send(chunk)
                        if sent < len(chunk):
                            pending[peer].extend(chunk[sent:])
                    except BlockingIOError:
                        pending[peer].extend(chunk)
                    except OSError:
                        return
    finally:
        # select.poll has no close(); only the selectors module does.
        del poller


def forward_plain(client: socket.socket, method: bytes, target: bytes, head: bytes) -> None:
    """Forward a plain HTTP request and relay the response."""
    # The target is an absolute URI; take the host and path from it.
    try:
        parts = target.decode().split("://", 1)[1].split("/", 1)
        hostport = parts[0]
        path = "/" + (parts[1] if len(parts) > 1 else "")
    except IndexError:
        client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        return
    host, _, port = hostport.partition(":")
    try:
        port = int(port or 80)
    except ValueError:
        client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        return

    upstream = connect_to(host, port)
    if upstream is not None:
        try:
            # Keep the client's headers byte for byte; dropping the CRLF
            # between them would make the origin reject the request.
            rest = head.split(b"\r\n", 1)[1] if b"\r\n" in head else b""
            upstream.sendall(
                f"{method.decode()} {path} HTTP/1.1\r\nHost: {hostport}\r\n".encode() + rest
            )
            pipe(client, upstream)
            return
        except OSError:
            pass
        finally:
            try:
                upstream.close()
            except OSError:
                pass
    client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")


def resolve(host: str, port: int) -> list:
    """Addresses to try, IPv6 first.

    The upstream quota is counted per IPv6 /64, and WARP's IPv4 side is a
    single shared NAT for the whole colo: dialling over IPv4 puts every node
    in one bucket, so a node that reaches the origin over IPv4 has no quota
    of its own. IPv6 first keeps each node in its own block.
    """
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


def connect_to(host: str, port: int) -> Optional[socket.socket]:
    """Open a connection to host:port, preferring IPv6."""
    for sockaddr in resolve(host, port):
        family = socket.AF_INET6 if len(sockaddr) == 4 else socket.AF_INET
        try:
            sock = socket.socket(family, socket.SOCK_STREAM)
            sock.settimeout(20)
            sock.connect(sockaddr)
            return sock
        except OSError:
            continue
    return None


def handle(client: socket.socket) -> None:
    upstream = None
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
            # A plain HTTP request through the proxy: forward it to the
            # origin and relay the response back. Some clients health-check a
            # proxy this way rather than with CONNECT.
            forward_plain(client, method, target, buf)
            return
        try:
            host, port = target.decode().rsplit(":", 1)
            port = int(port)
        except ValueError:
            client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            return

        upstream = connect_to(host, port)
        if upstream is None:
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            return

        # The node's tunnel is transparent, so this is a plain relay to the
        # origin: connect, tell the client the tunnel is up, and copy bytes.
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
