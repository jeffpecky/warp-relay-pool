# Signal-driven warp pool rotation — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Rotation latency from ≤5 min to seconds: keep-cold becomes a signal-driven watcher (`--watch`) whose verification probes ride the same wreq-js Bun TLS fingerprint as real client traffic.

**Architecture:** 9router keeps reporting 429s via `POST /signal` (already shipped). Relay app records them (shipped). Mux cooldowns (shipped). The new work is all keep-cold-side: watch mode consumes `signals.json` immediately, `pass()` machinery classifies/rotates as today, and probes move from relay-terminated Node TLS to true client-fingerprint wreq sessions per lane. No docker.sock in any container; no 9router changes.

**Tech Stack:** Node 22 (host), `wreq-js` native module (client TLS impersonation), node:test spawn harness (`test/keep-cold.test.mjs`), systemd unit via setup.sh.

---

## Context for a zero-context engineer

- Repo: `warp-relay-pool` (design doc: `docs/plans/2026-11-02-warppool-signals-design.md`, commit `9b39655`).
- `keep-cold.mjs` is a CLI with mutually-exclusive modes: `--sweep --once --loop --probe --fresh-ip-sweep --reset-until-clean`. Entry-point if/else chain near EOF (search "Entry point").
- Probe path today: `probe()` (line ~862) POSTs to `http://RELAY_HOST:BASE_PORT+index` with `x-relay-target: https://opencode.ai` headers → relay terminates TLS → upstream sees Node's ClientHello. That is the instrument we are replacing for opencode targets.
- `pass()` (line ~1931) already reads `readSignals()` (file consumed atomically) and merges them into `signalHot`; `classifySignal()` (via `ctx`) queues rotation jobs, executed by `runPool(ctx.rotations, ..., runRotation)` (line ~2071).
- `runRotation()` (line ~1611) shells out to `docker` (`containerName(job.index)`), patches state incl. `lastRotate`, `cooldownUntil`, `pendingVerify`.
- State file: `STATE_FILE` (default `.keep-cold/state.json`); signals: `LIVE_SIGNAL_FILE` (default `.keep-cold/signals.json`); both atomic-replace writers/readers.
- Tests: `node --test test/*.test.mjs`; harness binds fake relay HTTP servers in port bands 25000+, installs a fake `docker` binary via `FAKE_DOCKER_LOG`, uses `KEEP_COLD_CLOCK_SKEW_S` to move virtual time. Scenarios spawn `node keep-cold.mjs <mode>` with env overrides.
- wreq-js session API (from 9router `open-sse/services/opencodeTlsClient.js`): `const { createSession } = await import("wreq-js")`; await createSession({emulation:{tlsOptions:{...},alpnProtocols:[...]}, proxy: "http://user:pass@host:port"})`; then `session.fetch(url, {method, headers, body, redirect:"follow", signal})` → `{status, headers (forEach), body}`. Copy the `tlsOptions`/`alpnProtocols` constants verbatim from `opencodeTlsClient.js` (`o` object).
- Gate law for probe bodies (9router `open-sse/utils/opencodeFingerprint.js`, probes 2026-11-01/02): system prompt must OPEN with the CLI opener line and carry an `<env>` block containing a non-install `Working directory:`; foreign or appended-after shapes 429. keep-cold's current `PROBE_SYSTEM_PROMPT` opens `"You are a coding agent."` — wrong opener.

---

### Task 1: opener-first probe system prompt

**Files:**
- Modify: `keep-cold.mjs` (~line 543: `PROBE_SYSTEM_PROMPT`, ~line 549: `timestampPrompt()`)
- Test: `test/keep-cold.test.mjs` (new scenario at end)

**Step 1: Write the failing test**

New test in `test/keep-cold.test.mjs`: scenario with a fake relay capturing the JSON body; assert the system content starts with the opener line and contains `<env>` + `Working directory:` for `PROBE_FINGERPRINT=legacy`. Fake relay `switch` on captured body → write to a scenario file the test asserts on. (Reuse whatever body-capture mechanism the harness already has; if none exists, record bodies in the fake relay server like `FAKE_DOCKER_LOG` does.)

**Step 2: Run test to verify it fails**

Run: `node --test test/keep-cold.test.mjs`
Expected: new test FAILS (opener line absent).

**Step 3: Implement**

```js
// keep-cold.mjs — replace PROBE_SYSTEM_PROMPT and timestampPrompt's system text.
// Upstream gate (9router probes 2026-11-01/02): system must open with the CLI
// opener line AND carry an <env> block; appended-after shapes 429. Mirrors
// opencodeFingerprint.js ENV_FINGERPRINT_PROMPT, with a synthetic workspace dir.
const PROBE_OPENER =
  "You are an AI agent running in OpenCode, a coding agent harness. Help the user accomplish their goals using the tools you have available.";
const PROBE_SYSTEM_PROMPT = [
  PROBE_OPENER,
  "",
  "You are a coding agent.",
  "<env>",
  "  Working directory: /tmp/keep-cold-probe",
  "  Workspace root folder: /tmp/keep-cold-probe",
  "  Is directory a git repo: no",
  // Platform may be the VPS host (linux); a literal is fine — the gate checks the marker + opener, not the value.
  "  Platform: linux",
  "  Prefer /tmp over generic system temporary directories such as os.tmpdir(); it is pre-created and approved for external access.",
  "</env>",
].join("\n");
```
`timestampPrompt()` becomes:
```js
function timestampPrompt() {
  return `${PROBE_OPENER}\nCurrent time: ${new Date().toISOString()}`;
}
```
`minimal` mode stays unchanged.

**Step 4: Run tests, verify pass**

Run: `node --test test/keep-cold.test.mjs` — all scenarios pass (old scenarios unaffected: they assert status/docker calls, not prompt bytes... verify; if an old assertion byte-matches the old prompt, update THAT expectation deliberately).

**Step 5: Commit** `git commit -m "fix(keep-cold): opener-first probe system prompt (Nov-02 gate law)"`

---

### Task 2: wreq-js probe transport (true client fingerprint)

**Files:**
- Modify: `keep-cold.mjs` (`probe()` ~line 862; new helpers near it; `runRotation()` ~line 1611 for session invalidation)
- Test: `test/keep-cold.test.mjs`

**Design:**
- New env: `PROBE_TRANSPORT` = `auto` (default) | `legacy` | `wreq`.
- `auto`: for `PROBE_TARGET` opencode.ai, try `import("wreq-js")` ONCE at first probe; remember availability module-scope. Import/protocol failure → legacy for the process lifetime, one log line `probe transport: wreq unavailable (<reason>) - legacy fallback`.
- wreq path: bypass the lane's relay-terminated HTTP entirely; `session.fetch("https://opencode.ai" + relayPath, ...)` where the session holds `proxy:` = lane proxy URL `http://<RELAY_PROXY_AUTH basic-encrypted>@<RELAY_HOST>:<BASE_PORT+index>` — CONNECT through lane N, client-side Bun TLS from lane N's egress. (wreq handles Basic proxy auth.)
- Probe headers on the wreq path: current `probeHeaders()` minus the relay-internal trio (`x-relay-target`, `x-relay-path`, `x-keep-cold-probe` — those are relay app internals; a CONNECTed request must not carry them... verify against server.mjs: `x-keep-cold-probe` suppression only matters through the app's sniffer; harmless to keep `x-keep-cold-probe`, but relay-internal ones MUST be stripped on the wreq path since `x-relay-target` would trigger relay-terminated mode even through CONNECT... actually CONNECT tunnels them as opaque bytes; they'd only matter for header byte-shaping vs real client. Real client never sends them. Strip all three for fidelity).
- Session cache: `const wreqSessions = new Map()` keyed by lane index; LRU unneeded (COUNT caps it); on `runRotation()` completion for `job.index`, close+delete that lane's session (egress /64 changed under it; a stale pooled connection would probe the dead egress).
- Response handling: reuse existing 200-envelope + retry-after parsing by constructing the same return objects; timeout `PROBE_TIMEOUT_MS` via `signal`.

**Step 1: Failing tests**
1. Mode-selection test: with `PROBE_TRANSPORT=legacy` forced and a fake relay logging which wire it received (legacy = lane HTTP request w/ x-relay-target; wreq = CONNECT attempt), assert legacy hits the lane HTTP listener.
2. wreq path test cannot hit real upstream in CI: fake a Module by monkeypatch? Simplest true test: `PROBE_TRANSPORT=wreq` on a wreq-less environment (fake wreq-js missing) asserts the fallback log line appears and legacy probe still executes. Deeper wreq behavior stays manual-verify (see Step 5).

**Step 2: Run tests** — fail (no PROBE_TRANSPORT support: unknown env is ignored today → legacy always; test #2 must fail because no `probe transport` log line).

**Step 3: Implement** helpers + `probe()` branch + `runRotation()` session invalidation.

**Step 4: Run tests** — pass.

**Step 5: Manual verification (not unit-automatable, document in KB):**
- VPS: `npm install wreq-js` (host; record engine/ABI issues if any), run `node keep-cold.mjs --once` manually → log shows `probe transport: wreq`; compare probe verdicts against a 9router live request on the same lane within seconds (both 200 or both 429).
- Add `wreq-js` to `package.json` dependencies ONLY if found stable; otherwise document as optional peer + legacy fallback (lazy default: package.json dependency, since VPS installs this same package tree).

Commit.

---

### Task 3: `--watch` mode (signal-driven loop)

**Files:**
- Modify: `keep-cold.mjs` (argv lists ~173–193; entry chain ~2099; new `WATCH` const + loop)
- Test: `test/keep-cold.test.mjs`

**Design:**
```js
const WATCH = argv.includes("--watch");
// in modeFlags array + unknownArgs whitelist

} else if (WATCH) {
  log(`watch mode: signals.json drives passes; backstop sweep every ${WATCH_SWEEP_MIN} min`);
  await pass(); // baseline + service pendingVerify on start
  const SIGNAL_POLL_MS = envInt("WATCH_SIGNAL_POLL_MS", 2000, { min: 500, max: 60_000 });
  const SWEEP_TICK_MS = WATCH_SWEEP_MIN * 60_000;
  let lastStat = `${fs.statSync(LIVE_SIGNAL_FILE)?.mtimeMs ?? 0}`;
  for (;;) {
    const tickAt = Date.now() + SWEEP_TICK_MS;
    for (;;) {
      await sleep(SIGNAL_POLL_MS);
      try {
        const s = fs.statSync(LIVE_SIGNAL_FILE);
        if (String(s.mtimeMs) !== lastStat) { lastStat = String(s.mtimeMs); break; }
      } catch { /* file absent: no signals yet */ }
      if (Date.now() >= tickAt) break;
    }
    try { await pass(); } catch (e) { log(`pass failed: ${shortReason(e).slice(0, 200)}`); }
    try { lastStat = String(fs.statSync(LIVE_SIGNAL_FILE)?.mtimeMs ?? lastStat); } catch { }
  }
}
```
- `WATCH_SWEEP_MIN` env (default from `SWEEP_INTERVAL_MIN` computation; 0 disables backstop tick).
- Latency: signal write → stat differs → `pass()` → `readSignals` consumes → classify → rotate. ≈2 s poll + rotation time. Zero cost when idle.
- Do NOT remove `--loop`/`--sweep`/timer semantics in code; the systemd changes (Task 4) decide what runs.

**Step 1: Failing test** — scenario harness: spawn `node keep-cold.mjs --watch` with `COUNT=2`, `WATCH_SIGNAL_POLL_MS=500`, fake docker, relays that answer clean. Test writes a 429 signal record for relay 2 → assert within ~5 s: fake-docker log gains a `restart` of container 2, and a second write does NOT double-restart within the cooldown (state respects `lastRotate` via the stale-signal guard).

**Step 2: Run** — FAIL (unknown arg → exits with usage error).

**Step 3: Implement.**

**Step 4: Run** — pass, plus full `npm test` green.

**Step 5: Commit** `feat(keep-cold): --watch mode (signal-driven passes, backstop sweep)`

---

### Task 4: unit + scripts wiring (setup.sh, systemd)

**Files:**
- Modify: `systemd/warp-keep-cold.service` (ExecStart), `setup.sh` (sed block ~637–644 was updated by fc0d15f — adjust), `.env.example` (watch knobs)

**Design:**
- Unit becomes long-running watcher: remove the timer dependency for keep-cold:
  - `ExecStart=/usr/bin/flock -n -E 0 /tmp/warp-keep-cold.lock ${NODE_BIN} ${CURRENT_DIR}/keep-cold.mjs --watch`
  - `Restart=always` + `RestartSec=10` (long-running service now; flock retained as harmless belt).
- Timer: `warp-keep-cold.timer` no longer installed for the cold service (logwatch timer keeps flowing) — in setup.sh, skip `systemctl enable --now warp-keep-cold.timer` and skip copying `warp-keep-cold.timer` when the new watch unit is installed; leave the timer file on disk for manual backstop use, document `node keep-cold.mjs --sweep` as the manual backstop.
- Keep `Environment=LIVE_SIGNALS=` sed rewrite (fc0d15f) — watch mode also gates on it: if `LIVE_SIGNALS=0`, warn and still run (watch degrades to pure backstop sweeps, still better than nothing).
- Unit template gains `Environment=WATCH_SWEEP_MIN=15` (sed-rewritable).
- setup.sh final summary: swap the "Every N minutes..." prose for signal-driven wording (matches fc0d15f's printed token block).

**Step 1: Manual verify** — `bash -n setup.sh` in an LF copy still OK (CRLF caveat: test only LF conversions); run through the systemd block on a scratch `TMPDIR` if feasible, else eyeball `systemd-analyze verify`.

**Step 2: Commit** `feat(setup): keep-cold as --watch service; timer becomes manual backstop`

---

### Task 5: no-op guard — 9router side (verification only)

**Files:** none modified. Verify already-shipped pieces remain the full client story:
- `executors/opencode.js` ladder: rotateSession + `resetOpencodeTlsSession()` + `signalRelay429()` (repo code + installed bundle chunks/5330.js — bundle mirror already live).
- No docker-API manager: confirm `docs/kb-opencode-free-429.md` deploy checklist doesn't mention one (edit only if an outdated line exists).

**Step: KB touch-up** — replace sweep-centric wording in `9router/docs/kb-opencode-free-429.md` with watch-mode wording (signal → seconds → rotate → wreq verification), then `git -C 9router commit -m "docs(kb): watch-mode repair semantics"`.

---

## Out of scope (deliberate)

- Tier-2 auto-escalation (`--sweep` manual backstop covers).
- Provision-from-9router, remote docker from 9router, docker.sock in containers.
- Audit sweep restore — `PROBE_AUDIT_SAMPLE=0` stays the default in the watch world.
- Speed ranking (`SLOW_THRESHOLD_MS`) only meaningful with a full probe pass; keep code, sane with watch because passes are rare.

## Execution order / dependency

Task 1 independent. Task 2 independent of Task 1. Task 3 independent of both (uses `pass()` only). Task 4 depends on Task 3. Task 5 last. Parallelizable as 1+2+3, then 4, then 5.
