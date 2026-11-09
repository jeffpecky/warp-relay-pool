# 06 — Universal proxy mode

Closed mode (exact origins in `RELAY_ALLOWED_TARGETS`, no token) is the
default. Universal mode widens one pool to arbitrary upstreams:
wildcard/suffix/regex allowlist entries plus a shared token. Additive only —
closed mode, keep-cold, and the probe path are untouched.

## One URL for the whole pool

Universal mode changes what the relay may reach, not how traffic is routed.
Every request — closed or universal — goes to one client URL and the router
picks a lane per request:

    http://<tailscale-ip>:3100        mesh-VPN (the wizard writes the bind)
    https://<hash>.trycloudflare.com  tunnel-0, tunnels mode
    http://user:pass@<public-ip>:3100 direct public (Basic proxy credentials)

Routing policy, all client-invisible:

- **Round-robin over health:** each request lands on a spread-out lane whose
  aggregate health check (`GET /` — the same egress-walking health path)
  passed within the window. Failures (probe, connect-refused) cool that lane
  for `RELAY_LANE_COOLDOWN_S` (default 240s); a passing probe re-admits it.
- **429-aware:** a lane that answered 429 is parked for `RELAY_LANE_429_S`
  (default 120s) so traffic hops to a sibling egress while keep-cold does the
  slower real repair. When every lane is parked by 429s the request is served
  through one anyway — a throttle is not a broken egress, and a fleet-wide
  `503` is worse than a throttled attempt. On the L7 path the relay reads the
  status itself; inside a `CONNECT` tunnel it cannot, so a TLS-terminating
  client reports the verdict to `POST /__signal` (see
  [07-single-url-router.md](07-single-url-router.md#reported-429s-the-control-path)).
- **Session-sticky:** requests sharing a session header stay on the lane they
  used, with no TTL, so reasoning-issuer affinity holds per session.
- **All-egress outage:** with no lane usable at all the router answers plain
  `HTTP 503 Retry-After: 30` instead of hanging clients.

Client-facing detail: [07-single-url-router.md](07-single-url-router.md).

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

All three go through the same lane's SOCKS5 egress. Origin confinement
(allowlist match on every hop) and redirect refusal (upstream `3xx`
refused with `502`) apply unchanged in every mode.

Standard closed-mode headers (works in both modes for exact origins):

```sh
curl -s -N http://<tailscale-ip>:3100/v1/models \
  -H "x-relay-target: https://opencode.ai" \
  -H "x-relay-path: /zen/v1/models"
```

Standard forward-proxy form — absolute URI, no `x-relay-target` needed.
The relay derives target + path from `req.url`:

```sh
curl -s -N -x http://<tailscale-ip>:3100 http://example.com/v1/models \
  --proxy-header "x-relay-auth: <token>"
```

`CONNECT` tunnel (HTTPS through the relay). Host:port is restricted to
ports `443`/`80` and the hostname is matched against the allowlist as an
`https` origin. On success the relay answers `200` and splices a raw TCP
pipe over the SOCKS-connected socket — no interception, no re-encryption —
released when the connection closes. The tunnel pins one lane for its life:

```sh
curl -s -N -x http://<tailscale-ip>:3100 https://example.com/v1/models \
  --proxy-header "x-relay-auth: <token>"
```

(`--proxy-header` attaches `x-relay-auth` to the `CONNECT` request itself;
omit it for exact-origin targets when no token is configured.)

## Exposure warning

Universal mode widens what the relay *can* reach, so narrow who can reach
the relay:

- Keep `WARP_BIND` / `INGRESS_BIND` on loopback, or bind `INGRESS_BIND`
  to the Tailscale IP (mesh-VPN clients only). Never publish `3100` or
  `10800+N` to the open internet.
- In tunnels mode the router is not host-published; the `tunnel-0` URL is a
  public capability — treat it as a secret once a wildcard entry exists, and
  rotate the token by regenerating (`RELAY_AUTH_TOKEN=<new>
  ./generate-compose.sh … && docker compose up -d --build relay`).
- Quota note: keep-cold's probe lane still targets `PROBE_TARGET` (OpenCode
  Zen) only, and a probe 429 is liveness evidence, not a rotation trigger.
  Universal traffic rides the same egress IPs but does not extend probing to
  new upstreams; a throttle seen on another upstream reaches the pool only if
  that client reports it.

## Direct public-IP ingress (wizard option)

Binds the router to `0.0.0.0` and enforces **HTTP Basic proxy credentials**
on every dispatched request and every CONNECT — exact-origin allowlist hits
included (`407` without `Proxy-Authorization: Basic base64(user:pass)`,
with the challenge header on both the request and tunnel paths). The
no-target health path stays exempt so keep-cold's probes keep working.
Plain proxy clients (curl `-U user:pass`, browsers, and undici's
ProxyAgent via `http://user:pass@host:port`) speak this natively — which is
why the public arm uses it instead of `x-relay-auth`.

Standards:
- The generator fails closed: `RELAY_DIRECT_PUBLIC=1` without
  `RELAY_PROXY_AUTH=user:pass` refuses to write a compose file.
- `RELAY_PROXY_AUTH` is baked ONLY in direct mode (`RELAY_PROXY_AUTH_BAKED`
  is empty otherwise), so the mesh/tunnels arms never inherit the gate.
- The wizard persists both values in `.env` so later
  `./generate-compose.sh N` runs keep the same credentials.
- `get-status.sh` prints the public IP (ipify) with the credentials embedded
  (`http://user:pass@<public-ip>:3100`).
- Raw WARP SOCKS ports (`10800+N`) stay on loopback; the pool's isolation
  contract is the credential-gated router.
