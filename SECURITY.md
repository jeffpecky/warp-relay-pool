# Security Policy

warp-relay-pool pairs Cloudflare WARP egress addresses with public HTTPS
(TryCloudflare Quick Tunnel) or private (Tailscale Mesh-VPN) URLs so AI
coding agents can reach a fixed set of upstream APIs. This document covers
the security model, the repository privacy rules, and how to report issues.

## Security model

**Allowlist gate (`server.mjs`).** Every relayed request must carry an
`x-relay-target` header that matches an entry of
`RELAY_ALLOWED_TARGETS` (default: `https://opencode.ai`,
`https://api.kilo.ai`, `https://oai.endpoints.kepler.ai.cloud.ovh.net`).
Exact-origin entries match that origin only; `*` matches any `http`/`https`
origin, `*.suffix` matches the apex plus subdomains, and `re:<regex>` is an
anchored JS regex on the origin. Anything else gets `403 Forbidden`. The upstream
request is always `<allowlisted-target>` + `x-relay-path`, so a closed
allowlist (exact origins only) can only ever reach allowlisted hosts — it
is not a general-purpose open proxy.

**Universal mode boundary.** A wildcard (`*`, `*.suffix`) or `re:` entry
widens the relay to arbitrary origins, so `RELAY_AUTH_TOKEN` is mandatory
there: requests matching only via wildcard/regex must carry
`x-relay-auth: <token>` (else `401`; exact-origin matches never need it,
health check exempt). A wildcard entry with no token configured is an open
proxy for anyone who can reach the listener. Pass the token via host env at
generation time, never commit it, and keep relay ports on loopback or the
Tailscale IP. Details: `docs/06-universal-proxy.md`.

**No client authentication (mesh and tunnel arms).** The relay trusts anyone
who can reach a listener; direct public-IP ingress instead requires HTTP Basic
(see below). Possession of a relay URL is enough to (a) make allowlisted
upstream requests through that relay's egress IP and (b) read the relay's
current egress IP from the built-in health check (a request without
`x-relay-target` returns the Cloudflare trace for that egress). Treat relay
URLs as sensitive even though Tunnel URLs are public by construction.

**Host-published ports.** Both port families bind to `127.0.0.1` by
default: nothing is reachable from the internet unless you opt in.

- `10800+N` → raw WARP SOCKS5 proxy. No allowlist — a plain egress proxy.
  Expose deliberately with `WARP_BIND=0.0.0.0` (or an interface address)
  at generation time; when exposed, keep it on loopback, your Tailscale
  interface, or a trusted network — never the open internet.
- `3100` → relay router (mesh-VPN ingress), published only in mesh mode;
  `setup.sh` sets `INGRESS_BIND` to the VPS Tailscale IP. In tunnels mode
  it is not host-published at all — `tunnel-0` reaches it in-network at
  `http://10.244.0.100:3000`.
- **Direct public-IP ingress (`RELAY_DIRECT_PUBLIC=1`)** publishes the
  router on `0.0.0.0`, so the generator refuses to write the file
  unless `RELAY_PROXY_AUTH=user:pass` is set. Every dispatched request and
  CONNECT then requires HTTP Basic `Proxy-Authorization` (`407` +
  challenge otherwise) — exact-origin allowlist hits included; only the
  no-target health path is exempt. Treat the printed
  `http://user:pass@<public-ip>:3100` client line as a secret: anyone
  holding that string shares the pool's egresses (same capability model
  as tunnel URLs). Raw WARP SOCKS ports stay loopback-bound in this mode.

**Cleartext transport on the direct public-IP arm.** That listener is plain
HTTP, so `Proxy-Authorization: Basic` is base64 rather than encryption — anyone
on the path can recover `RELAY_PROXY_AUTH` and then use the pool. It exists for
clients that cannot join a VPN, not as the supported production path. For real
traffic use the Tailscale arm (WireGuard-encrypted, private port) or the
TryCloudflare arm (public HTTPS). If you must use the public arm: keep the
password long and random, rotate it if it may have crossed an untrusted
network, and restrict reachability at the network tier with `DOCKER-USER`
rules (see the port-publishing note below). This repo terminates no TLS; front
`:3100` with your own CONNECT-capable TLS terminator if you need encryption to
the client.

- Overrides: `WARP_BIND=<addr> ./generate-compose.sh N` and
  `INGRESS_BIND=<addr> ./generate-compose.sh N` (default `127.0.0.1`).
  Binds are IPv4 only (an IPv6 value is rejected at generation time).
- Interface-specific SOCKS binds (`WARP_BIND=10.0.0.5`) have no loopback
  listener: set `SOCKS_HOST=<that address>` for keep-cold's probes
  (`get-status.sh` reads the bind from the compose file automatically).
  `0.0.0.0` keeps loopback working and needs no change.

Docker publishes ports via DNAT before host `INPUT` rules are evaluated, so
a plain `ufw`/`firewalld` setup does not filter these mappings; use
`DOCKER-USER` rules if you deliberately expose anything.

## Repository privacy rules

Never commit, paste into issues/PRs, or put in docs:

1. Live tunnel URLs (`https://<random>.trycloudflare.com`).
2. Egress IPs, Tailscale/tailnet IPs, or private host addresses.
3. Account IDs, API tokens, or personal emails.
4. `get-status.sh` output — it stays on the host; scrub before sharing
   anywhere.

Docs use placeholders only: `<RELAY-N-URL>` / `<EGRESS-IP>` /
`<tailscale-ip>` / `<hash-N>.trycloudflare.com`. `*.trycloudflare.com` in
this repository always means the Quick Tunnel domain pattern, never a live
address. Real addresses live only in host `get-status.sh` output and local
`.env` files.

Supporting measures already in the tree:

- `.gitignore` excludes `.env*` (except `.env.example`), `accounts/`,
  `warp*.conf`, `wgcf-*.{conf,toml}`, `*-reg.json`, `backups/`, `devdocs/`,
  and `reference/`.
- CI runs a gitleaks scan on every push and pull request.
- The maintainer's pre-push workflow additionally scans with `trufflehog`;
  keep your own branches free of anything on the list above.

## Reporting a vulnerability

Use GitHub private vulnerability reporting: **Security tab → Report a
vulnerability**
(https://github.com/trefeon/warp-relay-pool/security/advisories/new).
Do not open a public issue for allowlist bypass, secret exposure, or
anything exploitable. Include the affected file/commit, a reproduction,
and the impact. Only the latest `main` is supported.
