#!/usr/bin/env python3
"""LanzyProxy — a rotating egress proxy front-end.

Exposes one local SOCKS5 port and one local HTTP CONNECT port in front of a
pool of upstream CONNECT proxies (the WARP-backed nodes in docker-compose.yml).

Every client connection is handed to the next healthy upstream, so successive
requests leave through different egress addresses. Upstreams are health-checked
against a real endpoint and de-duplicated by the IP they actually exit from, so
two nodes that resolve to the same address are not counted as two.

No third-party packages: the standard library only.
"""

from __future__ import annotations

import argparse
import asyncio
import ipaddress
import json
import os
import selectors
import socket
import ssl
import struct
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

LOG_LOCK = threading.Lock()


def log(msg: str) -> None:
    with LOG_LOCK:
        sys.stdout.write(f"{time.strftime('%H:%M:%S')} {msg}\n")
        sys.stdout.flush()


@dataclass
class Upstream:
    """One upstream CONNECT proxy."""

    name: str
    host: str
    port: int
    user: Optional[str] = None
    password: Optional[str] = None

    healthy: bool = False
    exit_ip: Optional[str] = None
    last_check: float = 0.0
    latency_ms: Optional[float] = None
    failures: int = 0
    uses: int = 0

    @property
    def addr(self) -> Tuple[str, int]:
        return (self.host, self.port)

    def label(self) -> str:
        return f"{self.name}={self.host}:{self.port}"


class Pool:
    """The set of upstreams, with rotation and health bookkeeping."""

    def __init__(self, upstreams: List[Upstream]) -> None:
        self.upstreams = upstreams
        self._rr = 0
        self._lock = threading.Lock()

    def pick(self) -> Optional[Upstream]:
        """Next healthy upstream in round-robin order."""
        with self._lock:
            n = len(self.upstreams)
            for i in range(n):
                u = self.upstreams[(self._rr + i) % n]
                if u.healthy:
                    self._rr = (self._rr + i + 1) % n
                    u.uses += 1
                    return u
        return None

    def snapshot(self) -> List[dict]:
        return [
            {
                "name": u.name,
                "endpoint": f"{u.host}:{u.port}",
                "healthy": u.healthy,
                "exit_ip": u.exit_ip,
                "latency_ms": round(u.latency_ms, 1) if u.latency_ms is not None else None,
                "failures": u.failures,
                "uses": u.uses,
            }
            for u in self.upstreams
        ]


# --------------------------------------------------------------------------
# Upstream probing: does it work, and what IP does it exit from?
# --------------------------------------------------------------------------

PROBE_HOST = "api64.ipify.org"
PROBE_PORT = 443


def http_connect(sock: socket.socket, host: str, port: int) -> None:
    """Issue a CONNECT tunnel to host:port over an already-connected socket."""
    req = (
        f"CONNECT {host}:{port} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Proxy-Connection: keep-alive\r\n\r\n"
    )
    sock.sendall(req.encode())


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


def probe(upstream: Upstream, timeout: float = 12.0) -> Tuple[bool, Optional[str], Optional[float]]:
    """Check an upstream and read the IP it exits from."""
    started = time.time()
    try:
        sock = socket.create_connection(upstream.addr, timeout=timeout)
    except OSError:
        return False, None, None
    try:
        sock.settimeout(timeout)
        if upstream.user:
            import base64

            token = base64.b64encode(
                f"{upstream.user}:{upstream.password or ''}".encode()
            ).decode()
            # Some upstreams want the credential in CONNECT; send it up front.
            sock.sendall(
                (
                    f"CONNECT {PROBE_HOST}:{PROBE_PORT} HTTP/1.1\r\n"
                    f"Host: {PROBE_HOST}:{PROBE_PORT}\r\n"
                    f"Proxy-Authorization: Basic {token}\r\n\r\n"
                ).encode()
            )
            status = read_connect_response(sock)
            if status != 200:
                return False, None, None
        else:
            http_connect(sock, PROBE_HOST, PROBE_PORT)
            status = read_connect_response(sock)
            if status != 200:
                return False, None, None

        # The probe target is HTTPS, so the tunnel needs TLS before the request.
        ctx = ssl.create_default_context()
        tls = ctx.wrap_socket(sock, server_hostname=PROBE_HOST)
        tls.sendall(
            f"GET / HTTP/1.1\r\nHost: {PROBE_HOST}\r\nConnection: close\r\n\r\n".encode()
        )
        data = b""
        while len(data) < 512:
            chunk = tls.recv(512)
            if not chunk:
                break
            data += chunk
            if b"\r\n\r\n" in data:
                break
        text = data.decode("utf-8", "replace")
        body = text.split("\r\n\r\n", 1)[1] if "\r\n\r\n" in text else ""
        exit_ip = body.strip().split()[0] if body.strip() else None
        if exit_ip:
            try:
                ipaddress.ip_address(exit_ip)
            except ValueError:
                exit_ip = None
        if not exit_ip:
            return False, None, None
        return True, exit_ip, (time.time() - started) * 1000
    except OSError:
        return False, None, None
    except ssl.SSLError:
        return False, None, None
    finally:
        try:
            sock.close()
        except OSError:
            pass


def health_loop(pool: Pool, interval: float) -> None:
    """Periodically re-probe every upstream."""
    while True:
        seen_ips: Dict[str, str] = {}
        for u in pool.upstreams:
            ok, ip, ms = probe(u)
            u.last_check = time.time()
            if ok and ip:
                # Two nodes that exit from the same address are one block.
                if ip in seen_ips and seen_ips[ip] != u.name:
                    u.healthy = False
                    log(f"{u.label()} exit {ip} duplicates {seen_ips[ip]} -> drained")
                    continue
                seen_ips[ip] = u.name
                u.exit_ip = ip
                u.latency_ms = ms
                if not u.healthy:
                    log(f"{u.label()} healthy, exit {ip} ({ms:.0f}ms)")
                u.healthy = True
                u.failures = 0
            else:
                if u.healthy:
                    log(f"{u.label()} failed")
                u.healthy = False
                u.exit_ip = None
                u.failures += 1
        time.sleep(interval)


# --------------------------------------------------------------------------
# SOCKS5 server
# --------------------------------------------------------------------------

SOCKS_VERSION = 5
SOCKS_CONNECT = 1
SOCKS_NO_AUTH = 0
SOCKS_IPV4 = 1
SOCKS_DOMAIN = 3
SOCKS_IPV6 = 4


def socks5_handshake(sock: socket.socket) -> Tuple[Optional[Tuple[str, int]], str]:
    """Do the SOCKS5 greeting and request. Returns (target, error)."""
    sock.settimeout(30)
    greeting = sock.recv(2)
    if len(greeting) < 2 or greeting[0] != SOCKS_VERSION:
        return None, "not socks5"
    nmethods = greeting[1]
    methods = sock.recv(nmethods) if nmethods else b""
    if SOCKS_NO_AUTH not in methods:
        socks5_select(sock, 0xFF)
        return None, "no acceptable auth method"
    socks5_select(sock, SOCKS_NO_AUTH)

    req = sock.recv(4)
    if len(req) < 4 or req[1] != SOCKS_CONNECT:
        return None, "only CONNECT supported"
    atyp = req[3]
    if atyp == SOCKS_IPV4:
        host = socket.inet_ntoa(sock.recv(4))
    elif atyp == SOCKS_DOMAIN:
        ln = sock.recv(1)[0]
        # The idna codec rejects a non-strict error handler, so decode the
        # bytes as latin-1 and let the resolver deal with the name.
        host = sock.recv(ln).decode("latin-1").strip("\x00")
    elif atyp == SOCKS_IPV6:
        host = socket.inet_ntop(socket.AF_INET6, sock.recv(16))
    else:
        return None, "bad address type"
    port = struct.unpack("!H", sock.recv(2))[0]
    return (host, port), ""


def socks5_select(sock: socket.socket, method: int) -> None:
    """Send the SOCKS5 method-selection reply (two bytes, not a full reply)."""
    try:
        sock.sendall(bytes([SOCKS_VERSION, method]))
    except OSError:
        pass


def socks5_reply(sock: socket.socket, code: int) -> None:
    """Send a SOCKS5 reply with the given status code."""
    try:
        sock.sendall(b"\x05" + bytes([code]) + b"\x00\x01" + b"\x00" * 6)
    except OSError:
        pass


def handle_socks_client(client: socket.socket, pool: Pool) -> None:
    upstream = pool.pick()
    if upstream is None:
        socks5_select(client, 0xFF)
        client.close()
        return
    try:
        target, err = socks5_handshake(client)
    except OSError:
        client.close()
        return
    if target is None:
        socks5_reply(client, 0x07)
        client.close()
        return

    host, port = target
    # A node can go stale between health checks. Try the one we picked, then
    # fall through to the others before giving up, so one dead node does not
    # turn into a failed request.
    candidates = [upstream] + [u for u in pool.upstreams if u is not upstream]
    for attempt, up_node in enumerate(candidates):
        try:
            up = socket.create_connection(up_node.addr, timeout=30)
        except OSError:
            up_node.healthy = False
            continue
        try:
            if up_node.user:
                import base64

                token = base64.b64encode(
                    f"{up_node.user}:{up_node.password or ''}".encode()
                ).decode()
                up.sendall(
                    (
                        f"CONNECT {host}:{port} HTTP/1.1\r\n"
                        f"Host: {host}:{port}\r\n"
                        f"Proxy-Authorization: Basic {token}\r\n\r\n"
                    ).encode()
                )
            else:
                http_connect(up, host, port)
            status = read_connect_response(up)
            if status != 200:
                up_node.healthy = False
                continue
            socks5_reply(client, 0x00)
            relay(client, up)
            return
        except OSError:
            continue
        finally:
            try:
                up.close()
            except OSError:
                pass
    socks5_reply(client, 0x01)
    client.close()


# --------------------------------------------------------------------------
# HTTP CONNECT server
# --------------------------------------------------------------------------

def handle_http_client(client: socket.socket, pool: Pool) -> None:
    client.settimeout(30)
    buf = b""
    while b"\r\n\r\n" not in buf and len(buf) < 65536:
        chunk = client.recv(4096)
        if not chunk:
            break
        buf += chunk
    if not buf:
        client.close()
        return
    lines = buf.split(b"\r\n")
    try:
        method, target, _ = lines[0].split(b" ", 2)
    except ValueError:
        client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        client.close()
        return
    if method.upper() != b"CONNECT":
        client.sendall(b"HTTP/1.1 405 Method Not Allowed\r\n\r\n")
        client.close()
        return
    try:
        host, port = target.decode().rsplit(":", 1)
        port = int(port)
    except ValueError:
        client.sendall(b"HTTP/1.1 400 Bad Request\r\n\r\n")
        client.close()
        return

    upstream = pool.pick()
    if upstream is None:
        client.sendall(b"HTTP/1.1 503 No healthy upstream\r\n\r\n")
        client.close()
        return
    try:
        up = socket.create_connection(upstream.addr, timeout=30)
    except OSError:
        upstream.healthy = False
        client.sendall(b"HTTP/1.1 503 No healthy upstream\r\n\r\n")
        client.close()
        return
    try:
        if upstream.user:
            import base64

            token = base64.b64encode(
                f"{upstream.user}:{upstream.password or ''}".encode()
            ).decode()
            up.sendall(
                (
                    f"CONNECT {host}:{port} HTTP/1.1\r\n"
                    f"Host: {host}:{port}\r\n"
                    f"Proxy-Authorization: Basic {token}\r\n\r\n"
                ).encode()
            )
        else:
            http_connect(up, host, port)
        status = read_connect_response(up)
        if status != 200:
            upstream.healthy = False
            client.sendall(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
            client.close()
            return
        client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
        relay(client, up)
    except OSError:
        pass
    finally:
        for s in (up, client):
            try:
                s.close()
            except OSError:
                pass


# --------------------------------------------------------------------------
# Relay
# --------------------------------------------------------------------------

def relay(a: socket.socket, b: socket.socket) -> None:
    """Shuttle bytes both ways until either side closes."""
    a.setblocking(False)
    b.setblocking(False)
    sel = selectors.DefaultSelector()
    sel.register(a, selectors.EVENT_READ, b)
    sel.register(b, selectors.EVENT_READ, a)
    try:
        while True:
            events = sel.select(timeout=60)
            if not events:
                break
            for key, _ in events:
                src, dst = key.fileobj, key.data
                try:
                    chunk = src.recv(65536)
                except (BlockingIOError, InterruptedError):
                    continue
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
        sel.close()


# --------------------------------------------------------------------------
# Dashboard
# --------------------------------------------------------------------------

def dashboard_handler(path: str, pool: Pool, started: float) -> Tuple[str, bytes]:
    if path in ("/", "/index.html"):
        body = b"""<!doctype html><html><head><meta charset="utf-8">
<title>LanzyProxy</title><style>
body{font:14px/1.5 system-ui,sans-serif;margin:0;background:#0f1419;color:#c9d1d9}
header{padding:16px 24px;background:#161b22;border-bottom:1px solid #30363d}
h1{margin:0;font-size:18px;color:#58a6ff}
main{padding:20px 24px}
table{border-collapse:collapse;width:100%;margin-top:12px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid #21262d;font-size:13px}
th{color:#8b949e;font-weight:600}
.ok{color:#3fb950}.bad{color:#f85149}.dim{color:#6e7681}
</style></head><body>
<header><h1>LanzyProxy</h1></header>
<main><div id="s">loading...</div>
<script>
async function tick(){
 const d = await (await fetch('/api/status')).json();
 const up = d.upstreams.filter(u=>u.healthy).length;
 const ips = new Set(d.upstreams.filter(u=>u.exit_ip).map(u=>u.exit_ip));
 document.getElementById('s').innerHTML =
  '<p>uptime '+Math.floor(d.uptime)+'s &middot; <span class="'+(up?'ok':'bad')+'">'+up+'/'+d.upstreams.length+
  '</span> healthy &middot; <span class="ok">'+ips.size+'</span> distinct exit IPs</p>'+
  '<table><tr><th>node</th><th>endpoint</th><th>exit IP</th><th>latency</th><th>uses</th><th>state</th></tr>'+
  d.upstreams.map(u=>'<tr><td>'+u.name+'</td><td>'+u.endpoint+'</td><td>'+(u.exit_ip||'-')+
  '</td><td>'+(u.latency_ms!==null?u.latency_ms+'ms':'-')+'</td><td>'+u.uses+'</td><td class="'+
  (u.healthy?'ok':'bad')+'">'+(u.healthy?'healthy':'down')+'</td></tr>').join('')+'</table>';
}
tick(); setInterval(tick,3000);
</script></main></body></html>"""
        return "text/html; charset=utf-8", body
    if path == "/api/status":
        payload = {
            "uptime": round(time.time() - started, 1),
            "upstreams": pool.snapshot(),
        }
        return "application/json", json.dumps(payload, indent=2).encode()
    return "text/plain; charset=utf-8", b"not found\n"


def serve_dashboard(port: int, pool: Pool, started: float) -> None:
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", port))
    srv.listen(32)
    while True:
        conn, _ = srv.accept()
        try:
            conn.settimeout(15)
            req = conn.recv(8192)
            path = "/"
            if req.startswith(b"GET "):
                path = req.split(b" ")[1].decode(errors="replace").split("?")[0]
            ctype, body = dashboard_handler(path, pool, started)
            conn.sendall(
                f"HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                f"Connection: close\r\n\r\n".encode()
                + body
            )
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def parse_upstream(spec: str) -> Upstream:
    """Parse host:port, host:port:user:pass, or a URL."""
    name = "node"
    if spec.startswith("http://") or spec.startswith("socks5://"):
        from urllib.parse import urlparse

        u = urlparse(spec)
        return Upstream(
            name=u.hostname or "node",
            host=u.hostname or "",
            port=u.port or (1080 if u.scheme == "socks5" else 8080),
            user=u.username,
            password=u.password,
        )
    parts = spec.split(":")
    if len(parts) >= 4:
        return Upstream("node", parts[0], int(parts[1]), parts[2], parts[3])
    return Upstream("node", parts[0], int(parts[1]))


def main() -> int:
    ap = argparse.ArgumentParser(description="LanzyProxy rotating egress proxy")
    ap.add_argument("--upstreams", required=True, help="comma-separated host:port[:user:pass]")
    ap.add_argument("--socks-port", type=int, default=1080)
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--dashboard-port", type=int, default=8081)
    ap.add_argument("--health-interval", type=float, default=60)
    args = ap.parse_args()

    ups = []
    for spec in args.upstreams.split(","):
        spec = spec.strip()
        if not spec:
            continue
        u = parse_upstream(spec)
        u.name = f"node{len(ups)}"
        ups.append(u)
    if not ups:
        print("no upstreams given", file=sys.stderr)
        return 1

    pool = Pool(ups)
    started = time.time()
    log(f"LanzyProxy starting with {len(ups)} upstream(s)")

    threading.Thread(target=health_loop, args=(pool, args.health_interval), daemon=True).start()
    threading.Thread(target=serve_dashboard, args=(args.dashboard_port, pool, started), daemon=True).start()

    # SOCKS5 listener
    socks = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socks.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    socks.bind(("127.0.0.1", args.socks_port))
    socks.listen(128)

    # HTTP CONNECT listener
    http = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    http.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    http.bind(("127.0.0.1", args.http_port))
    http.listen(128)

    log(f"SOCKS5 on 127.0.0.1:{args.socks_port}")
    log(f"HTTP CONNECT on 127.0.0.1:{args.http_port}")
    log(f"dashboard on http://127.0.0.1:{args.dashboard_port}/")

    socks.setblocking(False)
    http.setblocking(False)
    sel = selectors.DefaultSelector()
    sel.register(socks, selectors.EVENT_READ, "socks")
    sel.register(http, selectors.EVENT_READ, "http")

    while True:
        events = sel.select(timeout=1.0)
        for key, _ in events:
            conn, _addr = key.fileobj.accept()
            threading.Thread(
                target=handle_socks_client if key.data == "socks" else handle_http_client,
                args=(conn, pool),
                daemon=True,
            ).start()


if __name__ == "__main__":
    raise SystemExit(main())
