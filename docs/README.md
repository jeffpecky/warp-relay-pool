# Docs

Numbered user journey. Read in order.

1. [01 — Overview: how it works](01-overview-how-it-works.md) — request
   lifecycle (one listener → lane pick → allowlist → replayable buffer →
   attempt loop), the `x-relay-target` / `x-relay-path` protocol,
   mesh-VPN vs Quick-Tunnel ingress, why one URL, why per-relay WARP egress.
2. [02 — Deploy](02-deploy.md) — prereqs, pure vs `--with-tunnels` deploy,
   `docker compose up`, systemd keep-cold and signal-timer install, rescale
   with `generate-compose.sh`, port collisions, rebuild gotcha.
3. [04 — Operations: keep-cold](04-operations-keep-cold.md) — probe/rotate
   logic, tunable env table, quota-guard tuning, warp-only rule, logs.
4. [05 — Troubleshooting / FAQ](05-troubleshooting-faq.md) — URL changed,
   403, 401, 429, 503, stale image, not-ready containers, port collisions.
5. [06 — Universal proxy mode](06-universal-proxy.md) — wildcard/suffix/regex
   allowlist entries, `RELAY_AUTH_TOKEN` setup, `CONNECT` / absolute-form
   usage, open-proxy warning and bind guidance.
6. [07 — The single relay URL](07-single-url-router.md) — the one URL, health
   check, entry shapes, failure semantics a client can observe, affinity.

(03 was the retired pi-freeflow track; there is one client URL now, not a
per-relay list, so [07](07-single-url-router.md) replaces it.)

Privacy: live tunnel URLs and egress IPs never enter this repo — use
`<RELAY-URL>` / `<EGRESS-IP>` / `<tailscale-ip>`. Real addresses live only
in host `get-status.sh` output. `*.trycloudflare.com` below means the Quick Tunnel
domain pattern, never a live address. Canonical rules: [SECURITY.md](../SECURITY.md).
