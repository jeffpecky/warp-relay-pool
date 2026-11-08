# warp-relay-pool

Self-hosted WARP egress relay pool — **one URL, many egresses**. N isolated
Cloudflare WARP egress addresses behind a single L7 relay router, published as
a public HTTPS URL via **Cloudflare Quick Tunnels (TryCloudflare)** or a
private static URL via **Tailscale Mesh-VPN**.

```
[any HTTP client] ──┐
                    ├──► [relay router :3100 → :3000 (10.244.0.100)]
[OMP / Pi client] ──┘         │  (one URL; picks a lane per request)
                              ▼
                 [MicroWARP SOCKS :1080 × N (Docker bridge 10.244.0.0/16)]
                              │  (Alpine WireGuard + microsocks, ~1.8 MiB RAM/node)
                              ▼
                 [Cloudflare Edge ──► upstream API, egress <EGRESS-IP>]
```

The router is what makes one URL enough: it picks a WARP lane per request,
keeps a session on one lane so reasoning-issuer affinity holds, fails over when
a lane is throttled or dead, and retries a `429` internally. Detail:
[docs/01-overview-how-it-works.md](docs/01-overview-how-it-works.md) and
[docs/07-single-url-router.md](docs/07-single-url-router.md).

---

## Quickstart: One-Command Auto Setup

Deploy on any Linux VPS with systemd (Ubuntu, Debian, Fedora, CentOS):

```bash
git clone https://github.com/trefeon/warp-relay-pool.git
cd warp-relay-pool
./setup.sh
```

Or run non-interactively with auto-detected sizing recommendations:
```bash
./setup.sh -y
```

### What the Setup Wizard Does:
1. **Prerequisite Check & Auto-Install:** Verifies and automatically installs Docker, Docker Compose, and Node.js 18+ (for the prober) if missing.
2. **Hardware Sizing:** Detects VPS RAM and CPU cores, recommends safe relay limits, and defaults to the **5-relay sweet spot**.
3. **MicroWARP Build:** Builds the ultra-lightweight WireGuard + microsocks engine (~1.8 MiB RAM per relay).
4. **Ingress Wiring:** Exposes the router on a public HTTPS URL (`--with-tunnels`, no Cloudflare account or open inbound port needed) or on your Tailscale address.
5. **Keep-Cold Timers:** Installs `warp-keep-cold.timer` — a full sweep every 5 minutes (≤ 10 relays) or 15 minutes (> 10). Any relay returning HTTP 429 is restarted onto a fresh `/64` egress and re-probed until it streams again. Pool is "safe" once at least half the relays are clean (10 of 20). No time cap on rotation — just attempt-bounded. Plus `warp-keep-cold-signals.timer`, which consumes 429 signals every minute so a fresh throttle is repaired long before the next sweep.
6. **One URL:** Prints the single relay URL to dial, plus a `curl` health check.

---

## Sizing: Why 5 Relays is the Sweet Spot

For most users and developers running parallel coding subagents, **5 relays** is optimal:
- **5 Egress Paths:** Upstream rate limits (HTTP 429) are tracked per-model per-egress `/64` block.
  The keep-cold controller automatically verifies that rotations acquire a fresh `/64` prefix
  block, escalating from soft restarts (zero Cloudflare registrations) to full re-keys only when
  the address block fails to change.
  IPv4-only upstreams see Cloudflare's shared CGNAT pool, which is why the prober rotates on
  batches and the status table flags duplicate egress IPs.
- **Ultra-Low Memory Consumption:**
  - With **TryCloudflare Quick Tunnels**: one tunnel container (~20 MB) on top of the relays.
  - With **Tailscale Mesh-VPN**: ~50 MB total RAM.
- **Auto-Recovery:** If one relay hits a rate limit, the background controller rotates that single container's egress IP while the remaining lanes continue serving — and the client never notices, because its URL did not change.

| VPS Plan | Ingress Mode | Recommended Relays | Total Stack Memory |
|---|---|---|---|
| **512 MB VPS** | TryCloudflare Tunnels | 5 relays | ~30 MB |
| **512 MB VPS** | Tailscale Mesh-VPN | 5 – 10 relays | ~50 – 60 MB |
| **1 GB VPS** | TryCloudflare Tunnels | 5 – 10 relays | ~30 – 40 MB |
| **2 GB+ VPS** | TryCloudflare Tunnels | 10 – 20 relays | ~40 – 70 MB |

---

## Connecting a Client

`./setup.sh` and `./get-status.sh` print **one URL** for the whole pool:

```text
http://<tailscale-ip>:3100                                      (mesh-VPN)
https://<hash>.trycloudflare.com                                (tunnels)
http://user:pass@<public-ip>:3100                               (direct public)
```

Point any allowlisted client at it. Entry shapes, health checks and the
failure semantics a client can observe live in
[docs/07-single-url-router.md](docs/07-single-url-router.md).

---

## Security Model (Safe for Public Internet)

Full model and reporting in [SECURITY.md](SECURITY.md):
- **Closed default (403):** the relay router (`server.mjs`) enforces
  `RELAY_ALLOWED_TARGETS` (`https://opencode.ai`, `https://api.kilo.ai`,
  `https://oai.endpoints.kepler.ai.cloud.ovh.net`). Any other origin
  returns **HTTP 403 Forbidden**; upstream `3xx` redirects are refused
  (`502`), so an allowlisted origin cannot steer the relay elsewhere.
- **Wildcard-requires-token boundary (401):** wildcard/suffix/regex entries
  (`*`, `*.suffix`, `re:…`, see [docs/06-universal-proxy.md](docs/06-universal-proxy.md))
  widen the relay to arbitrary origins, so matches via wildcard alone must
  carry `x-relay-auth: <token>` (else **401**; exact-origin matches never
  need it, health check exempt). A wildcard entry with no
  `RELAY_AUTH_TOKEN` configured is an open proxy for anyone who can reach
  the listener.
- **Lane targeting is relay-internal.** `x-relay-lane` and the `lane-N`
  proxy userinfo are honored only on `x-keep-cold-probe` traffic, so no client
  can pin itself to an egress and defeat the spread.
- **Bind guidance:** keep `3100` on loopback or the Tailscale IP; never
  publish the router to the open internet. `x-relay-auth` and
  `proxy-authorization` are stripped and never forwarded.
- **URL-as-capability:** anyone holding the relay URL can make allowlisted
  upstream requests through the pool's egress IPs and read the answering
  egress IP from the health check — treat relay URLs as sensitive even
  though TryCloudflare URLs are public by construction.

---

## Universal Proxy Mode

Closed mode above is the default. Universal mode widens the allowlist to
arbitrary upstreams: allowlist entry shapes, `RELAY_AUTH_TOKEN` /
`x-relay-auth`, `CONNECT` / absolute-form usage, quota and egress notes —
[docs/06-universal-proxy.md](docs/06-universal-proxy.md).

---

## Manual Deployment

If you prefer manual control over the compose generation and lifecycle:

### Option A: TryCloudflare Quick Tunnels (Zero VPN required)
```bash
./generate-compose.sh 5 --with-tunnels
docker compose up -d --build
./get-status.sh
```

### Option B: Tailscale Mesh-VPN Direct
```bash
# The router defaults to 127.0.0.1; bind it to the tailnet address so the
# documented http://<tailscale-ip>:3100 URL is actually reachable.
INGRESS_BIND=$(tailscale ip -4) ./generate-compose.sh 5
docker compose up -d --build
./get-status.sh
```

---

## Rotation Rule: Warp-Only

When a relay gets throttled (HTTP 429), **only restart its `warp-N` container**:
```bash
docker restart warp-relay-warp-1
```
- **Never restart `tunnel-0` or `relay`**:
  - Restarting `tunnel-0` generates a new `.trycloudflare.com` URL, breaking every configured client.
  - Restarting `relay` drops active connections across all lanes.
- Restarting `warp-N` acquires a fresh Cloudflare egress IP while keeping the client URL completely stable.
- The automated monitor (`keep-cold.mjs`) follows this rule strictly.

To run a single probe pass manually:
```bash
node keep-cold.mjs --once      # full sweep
node keep-cold.mjs --signals   # consume 429 signals only, no probing
```

---

## File Map

| File | Role |
| ---- | ---- |
| `setup.sh` | Interactive and non-interactive auto-setup wizard with VPS hardware sizing |
| `server.mjs` | Relay: one listener, allowlist, body buffering, attempt loop, `CONNECT`, SSE/no-buffer streaming |
| `router.mjs` | Lane pool: per-request pick, session stickiness, probe/429 cooldowns, lane pinning for probes |
| `keep-cold.mjs` | Two-tier 429 prober / rotator for MicroWARP backends (`--once`, `--loop`, `--sweep`, `--signals`) |
| `docker-compose.yml` | N × MicroWARP + relay app; one tunnel appended only by `--with-tunnels` |
| `Dockerfile` / `package.json` | Relay image (COPY-baked) / dependencies |
| `generate-compose.sh` | Regenerate `docker-compose.yml`: `[count] [--with-tunnels] [--legacy-warp]` |
| `get-status.sh` | Status script: egress IPs and the one client URL |
| `systemd/` | systemd service units and timers for keep-cold, its signal pass, and logwatch |
| `docs/` | Step-by-step technical guides (01–07) |
| `warp-lite/` | Minimal MicroWARP image source (Alpine WireGuard + microsocks, ~1.8 MiB runtime) |

---

## Security & Privacy

Repository privacy rules — no live tunnel URLs, egress IPs, account IDs, tokens, emails, or private host addresses are ever committed; docs use `<RELAY-URL>` / `<EGRESS-IP>` / `<tailscale-ip>`, and `*.trycloudflare.com` always means the Quick Tunnel domain pattern — plus the full security model and vulnerability reporting live in [SECURITY.md](SECURITY.md).
