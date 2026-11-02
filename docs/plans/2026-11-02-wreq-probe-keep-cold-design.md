# Design: keep-cold as true-fingerprint repair daemon (wreq-js probes, 2-min timer)

Date: 2026-11-02 · Repo: warp-relay-pool (`keep-cold.mjs`, `setup.sh`) · Status: approved

## Problem

keep-cold's probes terminated TLS at the relay (lane ports, `x-relay-target`), so the upstream
gate saw Node/OpenSSL's ClientHello — a different TLS identity than real clients (9router's
Bun-impersonating wreq sessions). A probe "clean" verdict could disagree with what a wreq client
experiences from the same egress /64, and a throttle could be re-verified clean while real traffic
still 429'd.

## Final architecture (user decision: no client signals at all)

```
keep-cold (systemd timer, every 2 min for ≤10 relays):
  probe lanes with wreq-js Bun-fingerprint sessions (session rotated per probe)
  → probe 429 ⇒ existing guarded ladder: tier-1 restart → fresh /64 → verify probe
  → pure-429 batches / two failed verifications ⇒ tier-2 re-key (unchanged)
  → healthy lanes: audit sample probed every pass (stale-clean detection)
```

9router's 429 handling stays entirely on the client: rotate session + drop TLS cache + retry.
The push-signal path (9router `POST /signal`, relay intake, keep-cold consumption) was shipped and
**removed**: reversals of relay commits `795ca67` and `fc0d15f`. `LIVE_SIGNALS`/signal-writer
plumbing remains in the tree but is dormant (default 0).

What replaced detection-by-traffic: probes themselves speak the exact simulation 9router performs
(per-lane `createSession({proxy})` with the Bun TLS profile copied from
`9router/open-sse/services/opencodeTlsClient.js`, session rotated per probe), so a probe 429 IS
the verdict a real client would get from that egress.

## Shipped pieces (commits)

1. `fix(keep-cold): opener-first probe system prompt` — probe bodies carry the CLI opener line +
   `<env>` block the gate enforces (Nov-02 gate law), so a 429 is attributable to egress quota,
   not probe shape.
2. `feat(keep-cold): wreq-js probe transport` — `PROBE_TRANSPORT=auto|legacy|wreq` (fail-fast);
   auto prefers wreq for opencode.ai targets; per-lane sessions are closed on rotation so no
   pooled connection probes a replaced egress; legacy relay-terminated transport stays as
   explicit and fallback mode with a loud log line; test hook
   `KEEP_COLD_PROBE_WREQ_FAILFAST=1` for deterministic fallback tests.
3. Cadence: `SWEEP_INTERVAL_MIN` default 2 for pools ≤10 relays, 15 above (setup.sh; env pinned
   wins).
4. `wreq-js` dependency in `package.json` + best-effort `npm install` during setup; failure
   degrades to legacy probes with a warning, nobody stranded.

## Explicitly not built

- Any client → relay signal path (removed after user decision).
- Docker Engine API exposure on the VPS; docker.sock anywhere.
- Automatic-on-failure changes to rigged tier-2 behavior (manual `--sweep` covers paranoia runs).
- `PROBE_AUDIT_SAMPLE=0` — retracted: with no live signals, the audit sample is the only detector
  for lanes whose clean state has gone stale, so the default sample (2/pass) stays.

## Testing

- Unit (56/0): opener-first probe body, transport selection + legacy fallback, signal consumption
  behaviors, sweep/reset ladders untouched.
- VPS manual check: `npm install` on host, `node keep-cold.mjs --probe` shows wreq verdicts;
  compare against a live 9router request on the same lane seconds apart (both 200 or both 429).
