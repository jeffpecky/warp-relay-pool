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
| A `CONNECT` client reports its own `429` | The lane it was bound to is cooled immediately and its next tunnel rotates; the report also reaches `LIVE_SIGNAL_FILE`, so the container restarts. See "Reported 429s" below. |
| Target not in the allowlist | `403 {"error":"Forbidden target"}`. |

## Affinity

Reasoning models return encrypted reasoning that only the issuing egress can
decrypt. The router therefore keeps a **session-sticky** pin: requests carrying
the same `x-opencode-session` / `x-session-affinity` / `x-session-id` header
land on the same lane, with no TTL. Without a session header the key falls back
to the peer address.

A `CONNECT` tunnel carries none of those headers, so an authenticating client
names itself in the proxy userinfo instead (`<user>:<pass>`). The stickiness key
becomes `c:<user>`, which is the only way the pool can spread one host's tunnels
across lanes instead of collapsing them onto its peer address forever.

**Both halves are credentials.** `RELAY_PROXY_AUTH` is one or more `user:pass`
pairs, comma-separated; a caller must match one exactly, and only the first
colon separates the halves. A right password with an invented username is a
`407`, exactly like a wrong password — otherwise one caller holding the password
could mint unlimited identities and squat every lane. To add a client, append
`,user:pass` and regenerate the compose. The one reserved name is `lane-N`,
which the keep-cold probe pins a lane with; it authenticates with the first
account's password and is never a client identity.

## Rotation mode

`RELAY_LANE_MODE` selects how a request picks a lane:

- `sticky` (default) — a client keeps one egress, as above. Reasoning models
  need this: they return encrypted reasoning only the issuing egress can
  decrypt.
- `round-robin` — every request advances to the next usable lane. The binding is
  still recorded, so a reported 429 still condemns the lane the client was last
  served by.

`setup.sh` asks once and persists the answer to `.env`; `generate-compose.sh`
bakes it into the relay's environment, so regenerate after changing it.

A lane that 429s or fails is cooled and the client fails over; the sticky pin
moves with it. When every lane is exhausted by 429s, the request is served
through a throttled lane rather than rejected — the alternative is answering
`503` to the whole fleet for the duration of the cooldown window.

## Reported 429s (the control path)

The relay cannot read a status inside a `CONNECT` tunnel: it forwards raw
bytes, and the TLS is terminated by the client. A client that does terminate
TLS (zen-proxy) therefore reports the verdict out of band:

```
POST /__signal                     (same host and port as the proxy)
authorization: Bearer <RELAY_SIGNAL_TOKEN>
{ "clientId": "<user>", "status": 429, "model": "…", "retryAfter": 30 }
```

- The report names the **client**, never a lane. The relay looks up the lane
  that client id is bound to and cools it; a client cannot choose an egress.
- `clientId` is the proxy-auth **username** of a configured account. Both halves
  are credentials, so a report can only name an account that already
  authenticated. `lane-N` is refused here because it is the prober's lane pin
  and carries no identity. 429 reporting is enabled by `RELAY_SIGNAL_TOKEN`, not
  by the username: point `PROXY_URL` at any other rotating proxy with no token
  and it is a plain proxy, with no control surface here.
- Unset `RELAY_SIGNAL_TOKEN` ⇒ the endpoint answers `404`, so an install that
  does not use signalling exposes no new surface. Wrong token ⇒ `401`.
- The cooling is immediate (the next tunnel from that client rotates), and the
  same report is written to `LIVE_SIGNAL_FILE`, which is what restarts the
  container onto a fresh exit IP.
- `status: 200` clears the cooldown: a subsequent success on the lane is
  evidence the egress serves again.
- A lane's 429 cooldown is also lifted by the lane probe when it answers `200`
  from a **different** egress IP — the restarted container already did its job,
  so there is no reason to wait out `RELAY_LANE_429_S`. The same IP answering
  again proves nothing and keeps the cooldown.
