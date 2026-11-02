# Warp pool signal-driven rotation — design

Date: 2026-11-02
Status: approved (user), pending implementation plan

## Problem

The warp-relay-pool pool's only repair trigger was keep-cold's 5-minute sweep,
whose probes ride relay-terminated TLS (the relay's Node ClientHello), so a
"clean" verdict does not describe what a real client (9router's wreq-js Bun
impersonation) experiences. Quota-exhausted lanes therefore took up to 5
minutes to be discovered, and could be re-verified clean while real traffic
still 429'd.

## Chosen architecture (round-tripped options with user)

Dropped options and why:

- Faster polling timer (30 s): rotation is not free (~8–10 s lane downtime,
  a Cloudflare registration each time, /64 churn). Polling scales overhead
  and damage linearly with frequency.
- 9router drives docker directly (Docker Engine API over TLS :2376): useful
  raw control, but redundant once the relay side owns rotation, and it puts
  remote docker credentials on the 9router client machine. Not built.
- docker.sock inside the relay app container: root-equivalent compromise of
  - the host if the internet-facing container is ever compromised. Rejected on
  security grounds.
- keep-cold fully deleted: signals only report; something must restart
  containers. The executor cannot live on the client.

Final shape:

```
CC 429 → 9router (true TLS view)
  - rotate synthetic session (existing), reset wreq TLS cache (existing)
  - cooldown lane N in-process, retry on a sibling lane lane (existing)
  - POST /signal {relay:N} to the relay (existing, token-gated)
relay app (server.mjs)
  - POST /signal intake → signals.json (existing)
  - never executes infrastructure commands itself
mux (mux.mjs)
  - signal-file watcher cools lane N; next connections hop to sibling (existing)
keep-cold (host daemon) — the executor, upgraded:
  - NEW watch mode: fs-watch (.keep-cold/signals.json), classify on
    signal arrival instead of sleeping 5 minutes
  - classification guards unchanged: retry-after ≤120 s wait,
    GLOBAL_QUOTA_GUARD, per-relay rotations-1h ceiling, pure-429 batch logic
  - tier-1 rotation = docker restart warp-N (host-side; sockets stay on host)
  - verification probe through wreq-js BUN-FINGERPRINT session per lane
    (createSession({proxy}), same pattern as 9router's opencodeTlsClient.js),
    so the verdict is measured on the same fingerprint class as real traffic
  - probe bodies carry the opener@0 fingerprint prepend (port from
    9router open-sse/utils/opencodeFingerprint.js if buildProbe lacks it)
  - fallback: wreq-js missing/failed → legacy relay-terminated probe,
    logged as "probe mode legacy (fallback)"; classification unchanged
  - tier-2 re-key (wg0.conf wipe + Cloudflare re-register) = manual --sweep only
```

## Not built (explicit)

- 9router remote docker manager (pool deploy, lane state mirror)
- docker.sock in any container
- automatic tier-2 escalation
- provision-from-9router; fresh VPS installs stay with setup.sh
- keep-cold audit sweep and speed ranking (dead paths once audits stay at 0)

## Config / deploy summary

- VPS: `git pull`, `docker compose up -d --build relay` (signal intake image),
  `LIVE_SIGNALS=1` + `RELAY_AUTH_TOKEN=<token>` in compose env,
  keep-cold unit `Environment=LIVE_SIGNALS=1`, `Environment=PROBE_AUDIT_SAMPLE=0`,
  timer to 15 min (signals make it a repair-independent backstop, not the main
  detector). keep-cold runs with the fs-watch process (new unit or ExecStart
  change: `--watch` instead of `--sweep` timer).
- Windows: `setx OPENCODE_RELAY_SIGNAL_TOKEN <token>`, restart 9router.
- setup.sh (already patched, fc0d95f): generates RELAY_AUTH_TOKEN, persists
  LIVE_SIGNALS=1, prints the setx line — fresh installs need no manual env.

## Failure modes

- wreq-js binary/module missing on host → legacy probe path (clearly logged)
- fs-watch misses events (rename semantics) → 1 s poll fallback on mtime,
  coalesced; classification dedupes via relay last-restart timestamps
- rotation loop protection: rotations-1h ceiling per relay + GLOBAL_QUOTA_GUARD
- 9router never blocks user traffic on relay state: signal POST is fire-and-forget

## Tests

- unit: watch-mode signal consumption (dedupe, retry-after scheduling, ceiling),
  wreq probe fallback selection, probe body fingerprint prepend presence
- integration (manual, live window): fake 429 via signal POST → observe container
  restart + wreq probe verification in keep-cold.log; CC turn lands on sibling lane
