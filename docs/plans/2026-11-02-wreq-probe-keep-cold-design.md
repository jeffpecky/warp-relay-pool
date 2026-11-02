# Design: keep-cold as true-fingerprint repair daemon (wreq-js probes, 2-min cadence)

Date: 2026-11-02 · Repo: warp-relay-pool (host prober `keep-cold.mjs`, `setup.sh`) · Status: approved

## Problem

keep-cold's probes terminate TLS at the relay (lane ports, `x-relay-target`), so the upstream gate
sees Node/OpenSSL's ClientHello — a different TLS identity than the real clients (9router's
Bun-impersonating wreq sessions). Probe verdicts ("clean") are therefore measured on a different
fingerprint class than live traffic, and can disagree with what 9router actually observes from the
same egress /64. Detection was fixed separately (9router → `POST /signal` consumer below); this
design fixes the instrument and the cadence.

## Decisions

1. **No rotation logic in 9router.** The Docker Engine API management idea (9router restarting
   containers via `tcp://…:2376`) is dropped. 9router's shipped role: on 429 rotate session + drop
   TLS cache, cooldown the lane, POST `/signal` to the pool (already live on `main`).
2. **Repair stays host-side in keep-cold** — it already owns docker access, restart/re-key ladders,
   and guard logic (`retry-after ≤ 120 s` wait, `GLOBAL_QUOTA_GUARD`, attempt ceilings). Those are
   unchanged.
3. **Probe instrument upgrade**: when `wreq-js` is importable on the host, probes open true
   client-side TLS through the lane's egress:
   `createSession({ proxy: "http://<RELAY_PROXY_AUTH creds>@<host>:<lanePort>" })` — the same
   pattern 9router's `opencodeTlsClient.js` uses. Upstream sees a Bun-class ClientHello, so a
   probe 200/429 means what the real client's 200/429 means from that egress.
   - Probe bodies must carry the same `opener@0` fingerprint prepend the gate expects
     (mirror `open-sse/utils/opencodeFingerprint.js` semantics; verify `buildProbe` output).
   - One wreq session per lane, created per probe run (sessions are cheap; no sharing state to
     decay), closed after the pass to free file handles.
4. **Cadence: signal-driven watch + backstop** — supersede the timer-only cadence: keep-cold runs
   as a long-lived `--watch` service (fstat poll on `.keep-cold/signals.json`, ≈2 s latency),
   with a backstop sweep tick every 15 min (`WATCH_SWEEP_MIN`, `PROBE_AUDIT_SAMPLE=0` default).
   Probe volume scales with actual guilt — steady-state cost ≈ 0. Full sweep remains opt-in
   (`node keep-cold.mjs --sweep`). Canonical plan: `docs/plans/2026-11-02-warppool-signals-plan.md`
   (commit `c02ec76`); this doc is the decision record.
5. **Fallback**: `wreq-js` missing or import fails → legacy relay-terminated probe, explicitly
   logged (`probe mode: legacy fallback`), classification untouched. Nobody is stranded by the
   native dep; `setup.sh` best-effort installs `wreq-js` alongside the Node install and warns on
   skip.

## Explicitly not built

- Docker Engine API exposure on the VPS (root-equivalent surface, dies with decision 1).
- Tier-2 auto-escalation changes (manual `--sweep` covers the paranoid path).
- Audit/speed-ranking probes (`PROBE_AUDIT_SAMPLE=0`), redesign of `mux.mjs` cooldowns.
- Any change to 9router (its signal path + ladder are already shipped).

## Resulting architecture

```
CC/CLI turn → 9router TLS(Bun) → lane egress
429 seen → 9router: rotate session + TLS, cooldown lane, POST /signal
mux: cooldown makes the next connection hop lanes (quota relief ~seconds)
keep-cold (--watch): fs-watch on signals.json → consume → wreq-js true-fingerprint probe
  → still 429 → guarded ladder: restart container → fresh /64 → verify probe
```

Repair latency: ≤ 2 min + restart seconds. Steady-state probe traffic ≈ 0.

## Testing

- Unit: probe-mode selection (wreq → legacy fallback), `PROBE_AUDIT_SAMPLE=0` probe-set
  construction (signal-hot ∪ just-rotated only), signal file consumption unchanged.
- Live (existing procedures): force a 429 window on a lane; confirm signal → next pass probes
  that lane → container restart → verification probe 200 → mux cooldown clears.
