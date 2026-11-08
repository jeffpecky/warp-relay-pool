# 01 — Overview: how it works

One stack, one client URL. N isolated Cloudflare WARP egress addresses are
fronted by a single L7 relay router: it parses each HTTP request, picks a lane,
and forwards through that lane's SOCKS proxy. Every lane is also a WARP client
container with its own egress, so a 429 throttles one egress, not the pool.

Self-hosted Docker/WARP alternative to the Workers/Deno/Vercel relays that
[pi-freeflow](https://github.com/trefeon/pi-freeflow)'s `/freeflow deploy`
creates. Same `x-relay-target`/`x-relay-path` protocol, same default allowlist
(`https://opencode.ai`, `https://api.kilo.ai`,
`https://oai.endpoints.kepler.ai.cloud.ovh.net`), but one URL instead of one
per relay. This repo documents the relay side only.

```
[any HTTP client] ──► http://<router>:3100 ──► lane pick ──► [MicroWARP SOCKS :1080]
                                                  │              (Alpine WireGuard + microsocks,
                                                  │               ~1.8 MiB RAM each)
                                                  ▼
                                          warp-1 .. warp-N  ──► [upstream API, egress <EGRESS-IP>]
```

Reachable two ways:

1. **TryCloudflare Quick Tunnels (`--with-tunnels`):** one `tunnel-0` container
   fronts the router at a public HTTPS URL (`https://<hash>.trycloudflare.com`).
   Zero configuration — no Cloudflare account, domain, or client VPN.
2. **Mesh-VPN Direct (Tailscale):** `http://<tailscale-ip>:3100` published
   directly over an encrypted WireGuard mesh. The wizard binds it to the tailnet
   address.

## Request lifecycle

1. **One listener.** `server.mjs` accepts the request on `BASE_PORT` (container
   `3000`, host `3100`). There is exactly one, for every lane.
2. **Lane pick.** `router.mjs` derives a stickiness key from the session headers
   (falling back to the peer address), then picks the lane already bound to that
   key if it is usable; otherwise it round-robins to the next usable lane.
   "Usable" = not sick, past its probe cooldown, past any 429 cooldown.
3. **Allowlist.** The target origin — from `x-relay-target`, or from an
   absolute request URI — must match `RELAY_ALLOWED_TARGETS`. A wildcard match
   additionally requires `x-relay-auth`. Anything else is `403`.
4. **Replayable buffer.** The request body is buffered up to
   `RELAY_LANE_RETRY_MAX_BODY` (default 1 MiB) so a retry can replay it. A
   larger body streams and is never retried.
5. **Attempt loop.** Forward through the lane's `ProxyAgent` dispatcher
   (`undici`, 5-minute cap, client-disconnect abort wiring). A connect failure,
   a `429`, or a `502`/`503`/`504` on a `GET`/`HEAD` retries on another lane,
   up to `RELAY_LANE_RETRY_MAX` extra attempts.
6. **Stream out.** `cache-control: no-cache`, `x-accel-buffering: no`, upstream
   `content-type` only, `flushHeaders()` before the first body byte, then
   `pipeline(Readable.fromWeb(body), res)` — SSE tokens stream, nothing buffers.

No `x-relay-target` header = health check: the relay fetches
`https://cloudflare.com/cdn-cgi/trace` through its lane and returns the trace
body, a per-egress proof rather than a load-balancer ping.

## Header protocol

| Header | Required | Meaning |
| --- | --- | --- |
| `x-relay-target` | no | Upstream origin, trailing `/` stripped. Absent = health check. |
| `x-relay-path` | no | Path appended to target. Default `/`. |
| `x-relay-auth` | wildcard only | Token for a wildcard/suffix/regex allowlist match. Stripped, never forwarded. |
| `x-relay-lane` | no | Keep-cold only. Pins the lane a probe measures; ignored without `x-keep-cold-probe`. |
| `x-opencode-session` | no | Stickiness key, with `x-session-affinity` / `x-session-id` as fallbacks. |

Minimal forward example:

```bash
curl -s -N http://<router>:3100/v1/models \
  -H "x-relay-target: https://opencode.ai" \
  -H "x-relay-path: /zen/v1/models"
```

## Why one URL (not one port per relay)

An earlier design gave each relay its own listener and left the client to
choose — the pi-freeflow model, where the extension held a relay list and
raced them. That put routing knowledge in every client. One URL moves it into the
router, which can see things a client cannot: a lane's last probe result, its
429 cooldown, and the egress the previous turn actually used.

- **Reasoning-issuer affinity** is preserved by **session stickiness**, not by
  a client-side pin. Reasoning models return encrypted reasoning decryptable
  only by the issuing egress; the router keeps a session on one lane with no
  TTL, so turn 2 reaches the egress that issued turn 1. Conflict with
  failover is resolved in favour of staying alive: a lane that 429s is cooled
  and the session moves to a sibling.
- **Per-request failover.** A `429` or a dead connect retries inside the pool,
  so a single hot egress no longer means a client-visible error.
- **One capability to hand out.** One URL is one secret, one firewall rule, one
  thing to paste. Per-relay URLs made rotation a client-config change.
- **A `CONNECT` tunnel is the exception.** It splices a raw byte pipe for the
  life of the connection, so it occupies one egress for that life. That is the
  protocol, not a routing decision.

## Why per-relay WARP egress

Each `warp-N` (minimal MicroWARP `warp-lite:latest`, `NET_ADMIN`) owns its IPv6
egress `/128`, so upstream per-IP throttling (HTTP 429) hits one lane, not the
pool. IPv4 egress is Cloudflare CGNAT, shared across the pool; an IPv4-only
upstream can therefore throttle several lanes at once, which is what
`GLOBAL_QUOTA_GUARD` and the duplicate-IP note in `get-status.sh` are for.
Rotating one `warp-N` buys a fresh egress address while the client URL keeps
working. That isolation is the whole pool design, and it is why the router is
worth having: only one component needs to know which lanes are healthy.
