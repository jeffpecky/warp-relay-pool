# 04 — Operations: keep-cold

`keep-cold.mjs` is the **rotation controller** for the relay pool. Every timer
fire it runs a full sweep: probes every relay with a real streaming inference
request on each daily model, then continuously rotates any relay that answers
HTTP 429 until it streams 200 again. Your public tunnel URLs never change —
only the underlying WARP egress container is restarted.

**Naming:** `server.mjs` is the relay router (the HTTPS reverse-proxy);
`keep-cold.mjs` is the rotation controller/prober.

## Default behaviour (timer / `--sweep`)

Every 5 minutes (≤ 10 relays) or 15 minutes (> 10 relays):
1. Probes **every** relay on every popular free model (`deepseek`, `glm`, `mimo` by default).
2. Relays answering 200 on all models → marked clean.
3. Relays answering 429 are a **liveness** result only: the tunnel works, and the
   client owns rate-limit verdicts (see "Live signals" below). A 429 on a probe
   is logged and rotated by nothing.
4. Relays that fail some other way (401/403/400/unreachable) are left alone —
   a fresh exit IP cannot fix a protocol or auth failure.
5. Safe-floor verdict: exit 0 when ≥ `SAFE_FLOOR` relays are clean (default half the pool — 10 of 20).

> `--reset-until-clean` and `--fresh-ip-sweep` are the exceptions: they are
> explicit operator tools whose whole job is "rotate this egress until it
> streams again", so they still act on a probe 429.

- **Two-Tier Rotation:**
  - **Tier 1 (Default):** `docker restart` with `wg0.conf` kept — same keys, fresh `/64`, zero Cloudflare registrations.
  - **Tier 2 (Fallback):** Deletes `wg0.conf` and restarts — new keypair, one Cloudflare registration. Escalates when a soft restart cannot move the `/64` block, at most once per relay per `TIER2_MIN_INTERVAL_S`.
- **Egress Rotation in Sweep:** `--sweep` no longer rotates on a probe 429 —
  the sweep is a liveness pass, and rate-limit evidence comes from the client
  (see "Live signals"). Incremental `--once` behaves the same way. Only the
  explicit operator tools (`--reset-until-clean`, `--fresh-ip-sweep`) still
  treat a probe 429 as "rotate this egress".
- Concurrency: `ROTATE_CONCURRENCY` (default `1`) serialises restarts; `ROTATE_SPACING_MS` adds a mandatory gap between each.

### Healthcheck remediation lane

A relay whose WireGuard handshake never completes answers nothing at all, so
its probe result is not a 429 and the rotation logic above leaves it alone —
which is how a pool could sit with half its relays `(not ready)` indefinitely.

Before probing, each sweep therefore inspects Docker health for every relay and
restarts any container the healthcheck has marked `unhealthy`, using the same
tier-1 soft restart as rotation. Remediation is capped at one restart per relay
per `RELAY_COOLDOWN_S` and stamped in the state file as `lastUnhealthyRestart`,
so a genuinely broken container cannot restart-loop the host. Restarted relays
are flagged `pendingVerify` and re-probed after `EGRESS_SETTLE_S`, exactly like
a rotation.

Inspect it with `node keep-cold.mjs --status` (`lastUnhealthyRestart` is not
printed, but rotation timestamps are) or `journalctl -u warp-keep-cold` for the
`container healthcheck unhealthy — restarting` lines.

```bash
node keep-cold.mjs --sweep             # full sweep (what the timer runs)
node keep-cold.mjs --once             # incremental pass: suspects + audit sample only
INTERVAL_S=600 node keep-cold.mjs --loop   # incremental pass every 10 min, foreground
node keep-cold.mjs --fresh-ip-sweep   # on-demand full reset sweep (wipes wg0.conf)
node keep-cold.mjs --help             # usage (unknown flags exit 2)
```

### On-demand fresh-IP sweep (`--fresh-ip-sweep`)

When investigating pool-wide throttling or clearing sticky rate limits,
`--fresh-ip-sweep` runs a deliberate full-pool re-key and verification pass:

1. **Preflight:** Inspects every container for `/etc/wireguard/wg0.conf` and
   records its baseline egress IP before mutating anything. If any container is
   missing configuration or unreachable, the sweep aborts immediately with zero
   mutations.
2. **Serial full reset:** Deletes `/etc/wireguard/wg0.conf` in the container and
   triggers a restart. Operations are strictly serialised and spaced by
   `ROTATE_SPACING_MS` (at least 4 s) to prevent host I/O saturation and
   avoid Cloudflare registration rate limits.
3. **Verification:** Polls each container after restart until `warp=on` and
   verifies that its egress IP has demonstrably changed from the preflight address.
4. **Probing:** Probes the target model (`FRESH_IP_MODEL`, default `PROBE_MODELS[0]`)
   through each refreshed egress.
5. **Termination:** Bounded to exactly 1 full reset per relay. Stops with exit 0
   if at least one relay clears rate limits and answers cleanly. If throttles on
   the fresh IPs reach or exceed `GLOBAL_QUOTA_GUARD`, it halts and exits 1,
   confirming global upstream quota exhaustion and preventing futile re-registration loops.

### On-demand reset until clean (`--reset-until-clean`)

OpenCode's free-tier throttle is scoped to **one egress IP**, so a relay that
answers 429 is sitting on a throttled address, not on an exhausted account.
`--reset-until-clean` therefore rotates each rate-limited relay until it owns a
fresh address that streams `200 OK`, and never halts the pool because a
`retry-after` header is long — that header describes the address being
abandoned.

1. **Baseline:** probes `TARGET_MODEL` on every relay. Relays already at 200 are
   never touched; anything that is neither 200 nor 429 (401/403/400/unreachable)
   is left alone, since a new address cannot fix it.
2. **Tier 1 — soft restart:** `docker restart` with `wg0.conf` kept. Same keys,
   fresh WARP `/64`, **zero** Cloudflare registrations.
3. **Fresh-/64 check:** polls `cloudflare.com/cdn-cgi/trace` until `warp=on` and
   the `/64` prefix block differs from the pre-restart address (`READY_TIMEOUT_MS`).
   A soft restart that only moves the interface suffix inside an already-throttled
   `/64` does not count as fresh and escalates to a full reset immediately.
4. **Tier 2 — full reset:** escalates immediately when the soft restart could not
   move the `/64` block, or after `SOFT_ROTATIONS_BEFORE_TIER2` (default 1) soft restarts.
   Deletes `wg0.conf` and restarts: mints a new keypair and spends one Cloudflare
   registration.
5. **Probe:** 200 → CLEAN (relay ready); 429 → queued for the next round;
   anything else → marked not-resettable.
6. **Repeat:** up to `MAX_RESET_ATTEMPTS` rounds. `MAX_POOL_REGISTRATIONS_PER_HOUR`
   is a Cloudflare safety net that only blocks tier-2 escalation (soft restarts
   keep going), and only tier-2 spends count against it.

```bash
TARGET_MODEL=deepseek-v4.1-flash-free node keep-cold.mjs --reset-until-clean
```

Exit `0` when every relay streams 200 on the target model, `1` when any relay is
still throttled after the attempt budget, `2` on usage/configuration error.

### Read-only status probe (`--probe`)

Evaluates every relay against the target model using the exact production client
fingerprint without performing any docker restarts or mutating state files:

```bash
node keep-cold.mjs --probe
TARGET_MODEL=mimo-v2.6-flash-free node keep-cold.mjs --probe
```

The probe carries the same client shape as production traffic: pinned client
headers, the six declared tools, and the environment block real coding
sessions send (working directory). The free tier refuses requests without
that block with an all-egress 429 that looks exactly like throttling, so a
probe that omitted it could never tell a hot address from a clean one.

A 429 on some relays is per-address throttling — rotating those relays onto
fresh addresses clears it. A 429 on every relay at once, surviving a full
reset with retry-after counting down to 00:00 UTC, is either upstream quota
(no rotation helps; the run reports it as quota-capped) or the probe shape
itself being refused (check the prober; rotating the pool cannot fix that).

## Cost control

An inference probe is itself traffic that can trip the throttle it is looking
for, and every `docker restart` is a container stop/start plus a WARP
re-handshake. Both are therefore budgeted rather than run on every relay every
pass.

**Baseline vs demand-driven.** The first pass on a host with no
`RELAY_STATE_FILE` sweeps the whole pool and writes per-relay state. Every
later pass probes only:

| Candidate | Why it is probed |
| --- | --- |
| relay flagged `pendingVerify` and past `EGRESS_SETTLE_S` | it was just restarted; nothing is known about the new egress yet |
| relay still dirty whose `cooldownUntil` has expired | the throttle was never confirmed fixed |
| round-robin audit sample of `PROBE_AUDIT_SAMPLE` clean relays | catches a silent throttle before live traffic reports it |

On a healthy pool that is the audit sample (default 2 relays) per pass instead
of `COUNT` relays × `len(PROBE_MODELS)`.

**Rotation budget.** One restart per relay per pass, gated by
`RELAY_COOLDOWN_S` (600 s) and a hard `RELAY_MAX_ROTATIONS_PER_HOUR` (2) per
rolling hour. Consecutive failures escalate the cooldown exponentially —
`max(RELAY_BACKOFF_BASE_S · 2^(n-1), RELAY_BACKOFF_MAX_S)`, i.e. 300 s → 600 s
→ 1200 s → capped at 3600 s — so a genuinely bad egress cannot spend the hourly
budget in a few passes.

**Settle window.** A restarted relay is treated as throttled-free for
`EGRESS_SETTLE_S` (150 s) and is not probed inside that window. Probing during
the WARP handshake is what used to produce false 429s and back-to-back restarts.

**`Retry-After` is not a restart veto.** A live 429 signal restarts the lane
whether or not it carries `retry-after`, and however short the value is: the
header describes the egress being abandoned, and a restart mints a new one, so
waiting it out would keep a throttled lane in a small pool for minutes and gain
nothing. The relay separately holds that lane out of rotation for
`LANE_COOLDOWN_429_MS`; the restart is what puts a clean lane back behind it. A
*probe* 429 still schedules nothing — probes are liveness-only (below).

**Restart spacing.** Restarts are serialised at least `ROTATE_SPACING_MS`
(4000 ms) apart, so the host disk never sees two container stop/starts at once.

**Live signals — the only 429 detector.** A probe never decides that an egress
is throttled any more. A probe 429 proves only that the tunnel is alive, so it
is logged and ignored. Rate-limit evidence comes from the client: it terminates
TLS itself, sees the real upstream status, and — because the relay is a raw
CONNECT pipe that can read no status inside the tunnel — reports it out of band
over `POST /__signal` (`RELAY_SIGNAL_TOKEN`, see `.env.example`). The report
names the client, never a lane; the relay resolves which lane that client was
bound to and cools it, which is also what makes the next tunnel rotate. The
relay then appends one record per `(relay, model)` state change to
`LIVE_SIGNAL_FILE`, and keep-cold processes it with no probe at all: a
`status: 429` record routes that relay straight through the rotation gate (and
feeds the quota-guard history), any other status marks that relay clean. The
file is consumed destructively — after a pass it is atomically rewritten as
`{"signals":[]}` — so the relay app owns appends and keep-cold owns resets, and
one writer each keeps the host disk quiet. keep-cold marks its own probe
requests (`x-keep-cold-probe`): the relay app neither signals them nor forwards
the marker upstream, and a stale record can never re-rotate a relay that was
already restarted after it (records older than the relay's `lastRotate` are
ignored).

The shipped client is zen-proxy (`zen-proxy/src/relay.mjs`); it must carry its
own `c-<id>` as the proxy userinfo, because that is the identity the relay
resolved the lane for. Anything else that dials the relay over CONNECT is
invisible to this path and gets the periodic sweep's liveness-only treatment.

`LIVE_SIGNALS=1` is no longer required for the file to be consumed:
`RELAY_SIGNAL_TOKEN` implies the signal writer, since a report that only cools
a lane in memory would leave the container throttled. With no token the
endpoint answers 404 and the install exposes no new surface — and that is the
failure mode to watch for, because it is silent on the client side.

`setup.sh` mints the token on every install that has proxy auth (the direct
public-IP ingress), prints it once in the closing summary, and persists it to
`.env` at mode 600. A tunnels install has no proxy auth, so no token is
generated and this path stays inert. To add one to an existing install:

```sh
# host, in the relay checkout
printf '\nRELAY_SIGNAL_TOKEN=%s\n' "$(openssl rand -hex 12)" >> .env
chmod 600 .env
./generate-compose.sh <COUNT> && docker compose up -d   # bakes it into the relay env
```

Then set the same value as `RELAY_SIGNAL_TOKEN` in zen-proxy. The client id is
the client's own choice, not a secret and not relay-generated: zen-proxy reads
it from `PROXY_URL`'s userinfo (`c-<id>`) when `ZEN_PROXY_CLIENT_ID` is unset,
so the token plus a `c-<id>` proxy URL is the whole client-side configuration.

This path is optional: when the file is absent or corrupt the probe scheduler
above runs unchanged, so a pool that never enables signals still converges. The
relay app sees the file at `/signals/signals.json` only because the compose file
bind-mounts `./.keep-cold:/signals`; the host prober reads the same bytes as
`.keep-cold/signals.json`.

**Degradation.** State lives in `RELAY_STATE_FILE` (`.keep-cold/state.json`,
gitignored) and is written merge-on-write so concurrent writers cannot lose
fields. A corrupt or unwritable state file never fails a pass — it falls back
to a baseline sweep, and the exit status is unaffected.

## Exit status

- `0` — every relay answered 200 for every probed model.
- `1` — pool is not cold: a relay is throttled, quota-capped,
  `server_error`, unreachable, or returned a bad gateway, or a relay was just
  rotated and is still awaiting verification (pending the settle window). Quota-capped
  relays are deliberately **not** counted clean: when the upstream free
  tier throttles a model on `GLOBAL_QUOTA_GUARD` relays, rotation cannot
  help and the pass skips it, but the unit still reports failure so a
  fully throttled pool never looks green.
- `2` — usage or configuration error.

## Tunables

| Env | Default | Meaning |
| --- | --- | --- |
| `RELAY_HOST` | `10.244.0.100` | Relay app bridge address probed directly (not via tunnels) |
| `BASE_PORT` | `3000` | Relay N probed at `BASE_PORT+N` |
| `COUNT` | `10` | Relays probed |
| `PROBE_TARGET` | `https://opencode.ai` | Upstream the probes go through (defaults to OpenCode Zen inference with official fingerprint) |
| `PROBE_CHAT_PATH` / `PROBE_RESPONSES_PATH` | `/inference/v1/chat/completions` / `/inference/v1/responses` | Wire paths under `PROBE_TARGET` |
| `PROBE_MODELS_MODE` | `popular` | Auto-probe filter: `popular` (focuses on daily main models `deepseek`, `glm`, `mimo`) or `all` |
| `PROBE_MODELS` | Dynamically discovered free models | Free tag models auto-probed from OpenCode `/inference/v1/models` (survives model changes) |
| `FRESH_IP_MODEL` | First model in `PROBE_MODELS` | Target model probed during `--fresh-ip-sweep` (defaults to `deepseek-v4.1-flash-free`) |
| `PROBE_MODELS_MODE` | `popular` | Auto-probe filter: `popular` (deepseek, glm, mimo) or `all` |
| `PROBE_MODELS` | Dynamically discovered free models | Free tag models auto-probed from OpenCode `/inference/v1/models` |
| `MIN_CLEAN_RATIO` | `0.5` | Fraction of pool that must be clean for `--sweep` to exit 0 (0 < ratio ≤ 1) |
| `MIN_CLEAN_COUNT` | `ceil(COUNT × MIN_CLEAN_RATIO)` | Exact override for the clean floor (overrides `MIN_CLEAN_RATIO`) |
| `GLOBAL_QUOTA_GUARD` | `max(8, COUNT - 1)` (= 9 at `COUNT=10`); `COUNT + 1` below three relays | 429-on-this-many-relays = upstream quota, skip rotation. Below three relays the default never fires: on a two-lane pool "throttled on a threshold of lanes" and "throttled everywhere" are the same count, so a rotation is the one thing that can still help |
| `SPEED_MODEL` / `SLOW_THRESHOLD_MS` / `SLOW_MAX_ROTATIONS` | `nvidia/nemotron-3.5-lightning:free` / `0` / `2` | Speed rotation, disabled by default (`SLOW_MAX_ROTATIONS` is legacy: no per-pass rotation loop remains) |
| `RELAY_STATE_FILE` | `.keep-cold/state.json` | Per-relay scheduler state (gitignored; corrupt state degrades to a baseline sweep) |
| `RELAY_COOLDOWN_S` | `600` | Minimum gap between two rotations of the same relay |
| `RELAY_BACKOFF_BASE_S` / `RELAY_BACKOFF_MAX_S` | `300` / `3600` | Exponential cooldown on consecutive failures: `max(300 · 2^(n-1), 3600)` |
| `RELAY_MAX_ROTATIONS_PER_HOUR` | `2` | Hard cap on restarts of one relay in a rolling hour |
| `PROBE_AUDIT_SAMPLE` | `2` | Clean relays probed per pass (round-robin) to catch silent throttles |
| `EGRESS_SETTLE_S` | `150` | Quiet window after a restart before the relay is probed again |
| `ROTATE_SPACING_MS` | `4000` | Minimum gap between `docker restart` calls |
| `PROBE_CONCURRENCY` | `4` | Parallel probes per pass (probes only, never restarts) |
| `LIVE_SIGNALS` / `LIVE_SIGNAL_FILE` | `0` / `.keep-cold/signals.json` (container: `/signals/signals.json`) | Live-429 input written by the relay app. `RELAY_SIGNAL_TOKEN` alone implies the writer. Absent file = probe scheduler unchanged |
| `WATCH_DEBOUNCE_MS` / `WATCH_POLL_MS` | `250` / `2000` | `--watch-signals` only: burst collapse, and the portable poll fallback where `fs.watch` misses a rename |
| `TIER2_MIN_INTERVAL_S` / `TIER2_STATE_FILE` | `3600` / `.keep-cold/tier2-state.json` | Minimum gap between tier-2 re-keys for one relay, persisted across timer fires |
| `ROTATE_CONCURRENCY` | `1` | Parallel rotations (serial by default) |
| `MAX_ROTATIONS` | `4` | **Legacy** — accepted, no longer used: a pass makes at most one restart per relay (see `RELAY_COOLDOWN_S` / `RELAY_MAX_ROTATIONS_PER_HOUR`) |
| `SOCKS_HOST` / `SOCKS_PORT_BASE` | `127.0.0.1` / `10800` | **Legacy** — accepted; a restarted relay is skipped until `EGRESS_SETTLE_S` instead of being waited on inline |
| `SERVER_ERROR_RETRY` / `SERVER_ERROR_REPROBE_MS` | `1` / `5000` | Same-egress re-probes before a server_error t1 |
| `SERVER_ERROR_GUARD` | `9` (capped at `COUNT`) | server_error on this many relays = upstream incident, skip rotation |
| `CONTAINER_PREFIX` | `warp-relay-warp-` | `docker restart ${PREFIX}N` target |
| `TARGET_MODEL` | `FRESH_IP_MODEL`, then `PROBE_MODELS[0]` | Model `--reset-until-clean` must get streaming 200 on |
| `MAX_RESET_ATTEMPTS` | `10` | Rotation rounds `--reset-until-clean` spends per relay before giving up |
| `SOFT_ROTATIONS_BEFORE_TIER2` | `1` | Soft restarts that moved the address but still 429 before escalating to a full reset |
| `MAX_POOL_REGISTRATIONS_PER_HOUR` | `60` | Pool-wide Cloudflare registration ceiling (tier-2 only; soft restarts are not gated) |
| `READY_TIMEOUT_MS` / `PROBE_TIMEOUT_MS` | `180000` / `30000` | `READY_TIMEOUT_MS` bounds the fresh-egress wait in the reset commands; `PROBE_TIMEOUT_MS` is the per-probe timeout |
| `INTERVAL_S` | `300` | `--loop` seconds between passes |
| `KEEP_COLD_LOG` (+ `_MAX_BYTES` / `_KEEP_BYTES`) | stdout only / `2 MiB` / `200 KiB` | Optional file log with self-rotation |

## Warp-only rule

`docker restart warp-relay-warp-N` (or `docker compose restart warp-N`) is
the only rotation action. Never restart `tunnel-0` or `relay` for a hot
egress: a tunnel restart mints a new `*.trycloudflare.com` URL (breaking the
one URL every client uses) and a relay restart drops every in-flight
connection across the pool for nothing.

## Logs

Stdout always (journald: `journalctl -u warp-keep-cold.service`);
`KEEP_COLD_LOG=/path/keep-cold.log` additionally appends to a self-
rotating file.

An idle pass on a clean pool is a few lines — the audit sample it probed, the
verdict, and nothing else (budget: 10 lines idle, 50 on a noisy pass). Clean
relays never log a per-probe line. A busy pass adds the rotation it made, the
cooldown it is holding, the quota-guard skip, and the speed ranking line.

Logs never print egress IPs: rotation is reported as a relay index, not as an
address, so a journal export or a pasted log never leaks where the pool egresses
from. `./get-status.sh` stays the only place addresses are shown, and it prints
them for the operator at the console only.
