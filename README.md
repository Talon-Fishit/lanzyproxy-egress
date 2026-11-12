# LanzyProxy

A rotating egress proxy. Every connection is handed to the next node in the
pool, so successive requests leave through different egress addresses.

```
client ──► SOCKS5 :1080 ──┐
         └► HTTP   :8082 ──┼──► LanzyProxy ──► 6 WARP nodes ──► internet
                            └──► dashboard :8083
```

| Port | |
| --- | --- |
| `1080` | SOCKS5 (remote DNS — the proxy resolves the hostname) |
| `8082` | HTTP CONNECT |
| `8083` | dashboard |

Both listeners bind `127.0.0.1` only.

## Quick start

```bash
cd lanzyproxy
export WARP_ACCEPT_TOS=1
docker compose up -d --build        # the WARP node pool
python3 lanzyproxy.py \
  --upstreams "127.0.0.1:18090,127.0.0.1:18091,127.0.0.1:18092,127.0.0.1:18093,127.0.0.1:18094,127.0.0.1:18095" \
  --socks-port 1080 --http-port 8082 --dashboard-port 8083
```

Use it:

```bash
curl --socks5-hostname 127.0.0.1:1080 https://icanhazip.com
curl -x http://127.0.0.1:8082 https://icanhazip.com
```

Or point an app at it:

```bash
export ALL_PROXY=socks5h://127.0.0.1:1080
```

## Why the nodes rotate over IPv6

Each WARP node carries its own IPv6 `/64` on the CloudflareWARP tunnel:

```
2606:4700:110:8647:...  →  exits as 2a09:bac5:b1bb:4aa::/64
2606:4700:110:8c86:...  →  exits as 2a09:bac5:b1bb:26be::/64
2606:4700:110:8ada:...  →  exits as 2a09:bac5:b1ba:26be::/64
```

The IPv4 side of WARP is a **single shared NAT** for the whole colo
(`104.28.201.162`), so any traffic that goes out over IPv4 lands every node in
the same bucket and rotation buys nothing.

Two things had to be fixed to make that work:

1. **The per-node proxy resolves AAAA first.** `tinyproxy` could not be told
   to prefer IPv6, and `/etc/gai.conf` is ignored by musl (Alpine) and was
   unreliable under glibc's RFC 6724 rules — dual-stack targets ended up
   dialled over IPv4 at random. `proxy-node.py` resolves with `AF_INET6` only
   and falls back to IPv4 for hosts with no AAAA record.

2. **The dedupe arbiter has to measure IPv6.** It was comparing the IPv4
   address, so it saw all six nodes as the same and drained five of them.
   `dedupe.py` now measures `api64.ipify.org` (IPv6-only) and groups nodes by
   their **quota block** — `/64` on IPv6, `/32` on IPv4 — because that is what
   the upstream counts against, not the individual address.

## How rotation works

- `lanzyproxy.py` keeps one upstream per connection, round-robin over the
  healthy ones.
- Upstreams are probed every 60s against a real endpoint, which also records
  the exit address.
- Two nodes that resolve to the same exit are treated as one bucket.
- The arbiter (`ofu-dedupe`) rotates a node whose exit collides with a peer's,
  and drains a node that cannot be given its own block.
- If a node fails between health checks, the request is retried on the others
  rather than failing.

## Dashboard

`http://127.0.0.1:8083/` — live view of each node, its exit address, latency
and use count. `/api/status` returns the same as JSON.

## Bringing your own proxies

The WARP pool is optional. Any HTTP or SOCKS5 proxy works:

```bash
python3 lanzyproxy.py \
  --upstreams "user:pass@1.2.3.4:1080,http://5.6.7.8:8080" \
  --socks-port 1080 --http-port 8082 --dashboard-port 8083
```

Upstreams may be `host:port`, `host:port:user:pass`, or a URL. Credentials
are sent as `Proxy-Authorization: Basic` on the CONNECT request.

## Operational notes

- The WARP pool needs `/dev/net/tun` and privileged containers.
- Cloudflare's WARP terms apply: <https://www.cloudflare.com/application/terms/>.
  `WARP_ACCEPT_TOS=1` is the explicit opt-in the node image requires.
- The upstream quota this proxy is good for is counted per egress block and
  resets daily. More blocks need more egress, not more tuning.
- Both listeners bind loopback. If you expose them, put your own
  authentication in front — the proxy has none.

## Layout

| | |
| --- | --- |
| `lanzyproxy.py` | the rotator: SOCKS5, HTTP CONNECT, dashboard, health checks |
| `proxy-node.py` | the per-node CONNECT proxy (IPv6-first) |
| `warp/` | WARP node image (from `alztrk/opencode-ip-rotator`, MIT) |
| `dedupe/dedupe.py` | the arbiter that keeps exits distinct |
| `docker-compose.yml` | the node pool |

`warp/` and `dedupe/` come from
[opencode-unlimited-proxy](https://github.com/ebouther/opencode-unlimited-proxy)
and carry its fixes for measuring IPv6 exits.
