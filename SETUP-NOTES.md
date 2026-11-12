# LanzyProxy — setup notes on this sandbox

The full stack notes live in the opencode-unlimited-proxy repo. This file
keeps the part that is about this service.

## How it fits in

Six WARP nodes, each with its own CONNECT proxy on ports 18090–18095, behind
a rotator on `127.0.0.1:8082` (HTTP) and `:8080` (SOCKS5). The dashboard on
`:8083` shows which nodes are live and which address each one exits from.

The service is started by `/etc/systemd/system/lanzyproxy.service`. Point the
opencode2api engine at it by putting `"proxies": ["http://127.0.0.1:8082"]` in
the engine's `stack/config.json`. The rotator round-robins per tunnel, and it
skips a node that has been drained.

The quota this works around is counted per **IPv6 /64** and per model. The
engine's own WARP pool reaches about 20 distinct blocks; these six nodes
add more, and can be re-registered for a new address on demand.

**The relay must dial over IPv6.** WARP's IPv4 side is one shared NAT for the
whole colo (`44.242.217.54/32` here), so an IPv4 dial puts every node in the
same bucket. `proxy-node.py` resolves IPv6 first and only falls back to IPv4
for hosts with no AAAA record.

**The relay must not close an idle tunnel quickly.** An earlier version
polled with a 120 ms timeout and closed the tunnel when nothing happened in
that window. A slow origin leaves the tunnel idle while it builds its
response, so the client saw an unexpected EOF in the middle of a large
download — a Go client fetching a 6 MB document got a truncated body while
`curl` and Python, which read more slowly, sometimes did not notice. The
timeout is now `IDLE_TIMEOUT_MS` (300 s by default).

