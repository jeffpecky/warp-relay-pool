# 05 — Troubleshooting / FAQ

**The relay URL never changes.** In default (pure) mode there are no tunnel
containers, so container restarts, daemon reboots, and relay rebuilds keep the
single `http://<tailscale-ip>:3100` URL working. If it stops working, the fault
is ingress-side (relay app down, host offline, tailnet split) — check
`docker logs warp-relay-app` and host reachability, not egress rotation.

**Tunnel URL changed (tunnels mode only).** Expected after a `tunnel-0`
restart or host reboot — Quick Tunnels are ephemeral. Relay rebuilds and
per-egress 429 `warp-N` rotations never touch `tunnel-0`, so those keep the
URL; only a tunnel restart/reboot mints a new one. Re-run `./get-status.sh`
and update your client. Never restart `tunnel-0` for a throttled relay; that
trades a per-egress 429 for a URL churn. See
[04](04-operations-keep-cold.md). Relay rebuilds can't cause this: the tunnel
carries no `depends_on: - relay`.

**403 `Forbidden target`.** `x-relay-target` is not in the allowlist
(`RELAY_ALLOWED_TARGETS`, default `https://opencode.ai`,
`https://api.kilo.ai`, `https://oai.endpoints.kepler.ai.cloud.ovh.net`). Fix the header (a trailing `/` is stripped for you)
or set the env and re-run
`./generate-compose.sh <count> [--with-tunnels] && docker compose up -d`
(the generator bakes the allowlist into the relay service environment;
editing `.env` alone rebuilds nothing).

**401 on a wildcard target.** A request whose origin matched only via `*`,
`*.suffix` or `re:` needs `x-relay-auth: <token>`, and `RELAY_AUTH_TOKEN` must
be configured. See [06](06-universal-proxy.md).

**429 on one lane, one model.** Normal per-model-per-egress throttle — the
other lanes still serve, and the router moves the request. Either wait or
`docker restart warp-relay-warp-N`; keep-cold does this automatically, and the
signals watcher acts within milliseconds of the report. 429 on nearly every
relay for one model = upstream quota (`GLOBAL_QUOTA_GUARD`), no rotation helps.

A 429 is only reported when the **client** says so: the relay is a raw CONNECT
pipe and cannot read a status inside the tunnel. That means a client which does
not report (anything other than zen-proxy with `RELAY_SIGNAL_TOKEN` set) leaves
only the periodic sweep as detection.

**`503` with `Retry-After: 30`.** Every lane is parked by a **failed probe**,
i.e. the pool has no egress at all. Check the WARP containers; a pool merely
full of 429s is served through one of them instead, so this is a real outage.
Look at `docker logs warp-relay-warp-N` and the keep-cold journal.

**Stale code after editing `server.mjs`.** The image is `COPY`-baked:
`docker compose restart relay` re-runs old code. Always
`docker compose build relay && docker compose up -d relay`.

**Containers not ready / `(not ready)`.** WARP clients
need a minute to handshake after `up` or rotation. Check per layer:
`docker logs warp-relay-warp-N` (client state), in tunnels mode `docker logs
warp-relay-tunnel-0` (edge URL line appears only once connected),
`curl -s http://10.244.0.100:3000/` (relay + lane path without mesh or
tunnel). One relay red = its `warp-N`; all red = relay app or host
network. `warpReady` in `keep-cold.mjs` gates rotation on Connected +
real egress for exactly this reason.

`setup.sh` verifies every lane after the handshake wait: an in-container
`https://cloudflare.com/cdn-cgi/trace` must print `warp=on`. A lane with
`handshake=0` gets exactly one tier-2 re-registration (one Cloudflare
registration), then prints `RECOVERED` or `FAILED`. `FAILED` means that
lane's `wg0.conf`/endpoint is bad — the pool itself is fine, the router skips it.

`warp-lite/entrypoint.sh` also walks a small endpoint ladder before declaring
the lane up (`ensure_handshake`): if the endpoint baked into the compose env
does not answer within `HANDSHAKE_WAIT_S` (default 12 s), it retries
alternates with the same keys and re-pins the host route for each — no extra
Cloudflare registrations. `HANDSHAKE_FAILOVER=0` disables it.

**Host port already in use.** If `docker compose up` fails binding `3100`
or `10800+N` (a local service already claims it — `:3102` or `:10802`
collisions are the classic case), regenerate with a different `BASE_PORT`, or
edit the `ports:` lines in `docker-compose.yml` (and the generator), then `up`
again. Only one host port is needed for the router now.

**Multi-stack or systemd failures.** Check systemd logs with
`journalctl -u warp-keep-cold.service -n 50 --no-pager` (or
`-u warp-keep-cold-signals.service`) or `journalctl -u docker`.
If containers stop unexpectedly, inspect exit codes with
`docker inspect <container> --format "exit={{.State.ExitCode}} oom={{.State.OOMKilled}}"`.
