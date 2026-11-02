# 06 — Universal proxy mode

Closed mode (exact origins in `RELAY_ALLOWED_TARGETS`, no token) is the
default and covers the pi-freeflow lane. Universal mode widens one pool to
arbitrary upstreams: wildcard/suffix/regex allowlist entries plus a shared
token. Additive only — closed mode, keep-cold, and the probe path are
untouched.

## One-URL mux mode (default on)

Since the pool can grow to N relays, tracking N client URLs is painful. The
relay app therefore also listens as the **all-in-one mux** on the container
`BASE_PORT` and the host port just below the instance range:

    host 3100            = mux (ONE client URL for the whole pool)
    host 3101..3100+N    = per-relay instances (unchanged)

Client config needs exactly one URL — `http://100.72.14X.XX:3100` in mesh
mode (host mapping is generated as `3100-3100+N:3000-3000+N`), or the
`tunnel-0` Quick-Tunnel URL in tunnels mode (added by default).

Policy, all client-invisible:

- **Round-robin over health:** each connection lands on a spread-out relay
  whose aggregate health check (`GET /` — the same egress-walking health
  path) passed within the window. Failures (`probe`, connect-refused)
  cool that relay for `RELAY_MUX_COOLDOWN_S` (default 240s) and a passing
  probe re-admits it immediately.
- **429-aware:** with `LIVE_SIGNALS=1` the mux also reads the signal file;
  a relay that just answered 429 gets a short cooldown (`RELAY_MUX_429_S`,
  default 120s, or the upstream `retry-after` clamped) so traffic hops to
  a sibling egress while keep-cold does the slower real repair.
- **Sticky egress:** connections from the same client IP prefer to stay
  on the relay they used (`RELAY_MUX_STICKY_S`, default 600s), so OpenCode
  free-tier per-`/64` counters stay per-client. Affinity is per peer
  address, not per API key — see the ponytail note in `mux.mjs`.
- **All-egress outage:** with no usable relay the mux answers plain
  `HTTP 503 Retry-After: 30` instead of hanging clients.

Turning it off: bake `RELAY_MUX=0` and regenerate. The per-port lanes and
`.env` knobs are unchanged; the mux only adds one listener before
`warp-1`.


## Allowlist entries

`RELAY_ALLOWED_TARGETS` is a comma list. Each entry matches against the
request **origin**:

| Entry | Matches |
| ----- | ------- |
| `https://opencode.ai` | That exact origin only |
| `*` | Any `http`/`https` origin |
| `*.example.com` | The apex plus all subdomains |
| `re:^https://[a-z0-9-]+\.example\.com$` | Anchored JS regex on the origin |

## Token setup

Requests whose target matched **only** via a wildcard (`*`, `*.suffix`)
or `re:` entry must carry `x-relay-auth: <token>` or they get `401` —
including when no token is configured, so a wildcard/regex entry without
`RELAY_AUTH_TOKEN` fails closed instead of becoming an open proxy.
Exact-origin matches never need it, and the no-target health check is
always exempt. `x-relay-auth` is stripped before upstream (never forwarded).

A wildcard/regex entry **requires** `RELAY_AUTH_TOKEN` — without it those
dispatches are rejected (`401`). Pass the token via host env only, never
commit it:

```sh
RELAY_AUTH_TOKEN=<token> ./generate-compose.sh 5 [--with-tunnels]
docker compose up -d --build
```

Regenerate after changing either variable — both are baked into the relay
service environment at generation time.

## Request shapes

All three go through the same per-relay SOCKS5 egress. Origin confinement
(allowlist match on every hop) and redirect refusal (upstream `3xx`
refused with `502`) apply unchanged in every mode.

Standard closed-mode headers (works in both modes for exact origins):

```sh
curl -s -N http://<tailscale-ip>:3101/v1/models \
  -H "x-relay-target: https://opencode.ai" \
  -H "x-relay-path: /zen/v1/models"
```

Standard forward-proxy form — absolute URI, no `x-relay-target` needed.
The relay derives target + path from `req.url`:

```sh
curl -s -N -x http://<tailscale-ip>:3101 http://example.com/v1/models \
  --proxy-header "x-relay-auth: <token>"
```

`CONNECT` tunnel (HTTPS through the relay). Host:port is restricted to
ports `443`/`80` and the hostname is matched against the allowlist as an
`https` origin. On success the relay answers `200` and splices a raw TCP
pipe over the SOCKS-connected socket — no interception, no re-encryption —
released when the connection closes:

```sh
curl -s -N -x http://<tailscale-ip>:3101 https://example.com/v1/models \
  --proxy-header "x-relay-auth: <token>"
```

(`--proxy-header` attaches `x-relay-auth` to the `CONNECT` request itself;
omit it for exact-origin targets when no token is configured.)

## Exposure warning

Universal mode widens what the relay *can* reach, so narrow who can reach
the relay:

- Keep `WARP_BIND` / `INGRESS_BIND` on loopback, or bind `INGRESS_BIND`
  to the Tailscale IP (mesh-VPN clients only). Never publish `3100+N` or
  `10800+N` to the open internet.
- In tunnels mode the router is not host-published; each tunnel URL is a
  public capability — treat tunnel URLs as secrets once a wildcard entry
  exists, and rotate the token by regenerating (`RELAY_AUTH_TOKEN=<new>
  ./generate-compose.sh … && docker compose up -d --build relay`).
- Quota note: 429 rotation and the keep-cold probe lane still target
  `PROBE_TARGET` (OpenCode Zen) only — universal traffic rides the same
  egress IPs but does not extend probing to new upstreams.

## Direct public-IP ingress (wizard option 3)

Binds the router to `0.0.0.0` and enforces **HTTP Basic proxy credentials**
on every dispatched request and every CONNECT — exact-origin allowlist hits
included (`407` without `Proxy-Authorization: Basic base64(user:pass)`,
with the challenge header on both the request and tunnel paths). The
no-target health path stays exempt so keep-cold and the mux probes keep
working. Plain proxy clients (curl `-U user:pass`, browsers, and undici's
ProxyAgent via `http://user:pass@host:port`) speak this natively — which is
why the public arm uses it instead of `x-relay-auth`.

Standards:
- The generator fails closed: `RELAY_DIRECT_PUBLIC=1` without
  `RELAY_PROXY_AUTH=user:pass` refuses to write a compose file.
- `RELAY_PROXY_AUTH` is baked ONLY in direct mode (`RELAY_PROXY_AUTH_BAKED`
  is empty otherwise), so the mesh/tunnels arms never inherit the gate.
- The wizard persists both values in `.env` so later
  `./generate-compose.sh N` runs keep the same credentials.
- Client lines from `get-status.sh` carry the public IP (ipify) with the
  credentials embedded (`http://user:pass@<public-ip>:310N`).
- Raw WARP SOCKS ports (`10800+N`) stay on loopback; the pool's isolation
  contract is the credential-gated router.

