# 07 — The single relay URL

One URL fronts the whole pool. There is no per-relay port to track: point a
client at the router and every request is routed to a WARP egress lane,
failing over inside the pool when a lane is throttled or dead.

```
client ──► http://<router>:3100/... ──► lane pick ──► warp-N:1080 SOCKS ──► upstream
```

- **Mesh-VPN:** `http://<tailscale-ip>:3100` (the wizard writes the bind).
- **Tunnels:** the `tunnel-0` Quick-Tunnel URL from `./get-status.sh`.
- **Direct public:** `http://user:pass@<public-ip>:3100` — Basic proxy
  credentials are enforced on that arm.

`./get-status.sh` prints the URL it parsed from `docker-compose.yml`, so it is
always the one this deployment actually wrote.

## Health check

No `x-relay-target` header means "tell me which egress answered":

```sh
curl -s -x http://<router>:3100 https://cloudflare.com/cdn-cgi/trace
```

The body is Cloudflare's trace (`ip=<EGRESS-IP>`, `warp=on`), written by
whichever lane served the request. Repeat it to see the spread across the pool.

## Sending a request

Both entry shapes work and both pick a lane per request.

Explicit target (the pi-freeflow / OMP header protocol):

```sh
curl -s -N http://<router>:3100/inference/v1/chat/completions \
  -H "x-relay-target: https://opencode.ai" \
  -H "x-relay-path: /inference/v1/chat/completions" \
  -H "content-type: application/json" \
  -d '{"model":"<model>","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

Forward-proxy form — an absolute URI needs no header, the relay derives target
and path from the request line:

```sh
curl -s -N -x http://<router>:3100 https://opencode.ai/zen/v1/models
```

A `CONNECT` tunnel (HTTPS through the relay) pins one lane for the life of the
tunnel — that is the one case where the client keeps a single egress, by
construction.

Wildcard/suffix/regex allowlist entries need `x-relay-auth: <token>`; exact
origins do not. Setup and the full allowlist grammar:
[06-universal-proxy.md](06-universal-proxy.md).

## What a client can observe

| Condition | What the client sees |
| --- | --- |
| A lane answers `429` | The request is retried on another lane, transparently (POST bodies up to `RELAY_LANE_RETRY_MAX_BODY` are buffered for the replay). |
| A lane refuses the connection or fails its probe | Cooled for `RELAY_LANE_COOLDOWN_S`; other lanes keep serving. |
| Every lane is parked by `429` | Traffic is still served through one of them — a throttle is not a dead egress. |
| Every lane is parked by a failed probe | `503` with `Retry-After: 30`. A genuinely empty pool, not a throttle. |
| An upstream `5xx` on a `POST` | Returned as-is. A `POST` is never replayed after a `502`/`503`/`504`; only `GET`/`HEAD` are. |
| Target not in the allowlist | `403 {"error":"Forbidden target"}`. |

## Affinity

Reasoning models return encrypted reasoning that only the issuing egress can
decrypt. The router therefore keeps a **session-sticky** pin: requests carrying
the same `x-opencode-session` / `x-session-affinity` / `x-session-id` header
land on the same lane, with no TTL. Without a session header the key falls back
to the peer address.

A lane that 429s or fails is cooled and the client fails over; the sticky pin
moves with it. When every lane is exhausted by 429s, the request is served
through a throttled lane rather than rejected — the alternative is answering
`503` to the whole fleet for the duration of the cooldown window.
