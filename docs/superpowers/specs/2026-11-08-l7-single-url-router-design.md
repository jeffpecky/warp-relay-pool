# Design: L7 single-URL router (rota-style), pi-freeflow lane removed

- **Date:** 2026-11-08
- **Supersedes:** the per-port relay lanes and `mux.mjs` (TCP switcher) described
  in `docs/01-overview-how-it-works.md` and `docs/06-universal-proxy.md`

---

## 1. Intent

Today the pool exposes **N + 1 ports**: one TCP mux on `BASE_PORT` plus one
HTTP listener per relay on `BASE_PORT+1..+N`. The mux is a pure L4 switcher
(`mux.mjs:200`, `client.pipe(upstream)`), so a connection is pinned to one
egress for its whole life and the mux cannot parse or retry anything.

Wanted, and what `alpkeskin/rota` does: **one URL, many upstreams inside**,
where routing happens *per request* on the proxy protocol, with transparent
failover inside a single client request.

Rota's shape, for reference (read from `core/internal/proxy/`):

| Piece | Rota | Consequence |
|---|---|---|
| Ingress | one `http.Server` (`server.go:150-155`) | one URL |
| HTTP request | `selector.Select()` per request, `http.Client` over a cached per-proxy `http.Transport` (`handler.go:599-677`, `transport.go:57-70`) | rotation between requests |
| Failure | `SendWithRetry` → next proxy, up to `FallbackMaxRetries`; `PoolChain` evicts after 3 consecutive fails (`pool_chain.go`) | failover inside one request |
| CONNECT | pick one proxy, `socks5Connector`, then `BidirectionalCopy` splice (`handler.go:463-553`) | a tunnel stays on one egress |
| Sticky | per user (`http://user:pass@host`), `user_auth.go` | affinity by credential |

We port the **policy**, not the code. No Go, no Postgres, no dashboard.

Non-goal, stated plainly: this does not make a 3-minute SSE chat stream rotate
mid-flight. Rotation applies between requests and on failover before the
response starts. That is where 429s actually land.

## 2. Decisions (made by the user)

1. **429 failover: yes, buffer + retry.** Request bodies are buffered up to a
   cap and replayed on another egress when the first answers 429, or when the
   connection failed before any response. Streamed responses still stream.
   Bodies over the cap stream with no retry.
2. **Selection: sticky per session, no TTL.** Affinity is keyed on the client's
   session headers (`x-opencode-session` / `x-session-affinity` /
   `x-session-id`), held in a capped LRU with no expiry, and moved only on a
   429 or a lane failure. Rationale: `docs/01` records that reasoning output is
   only decryptable by the issuing egress IP, so a conversation must not change
   lanes mid-thread. This also fixes the current tunnel-mode bug where every
   client arrives from cloudflared's address and shares one sticky key.
   Credentials are **not** the key: on OpenCode free tier every client sends
   the literal `Authorization: Bearer public`.
3. **Scope: routing core only.** Env-var config. No DB, no pools UI, no users
   table, no usage stats.
4. **pi-freeflow lane: removed.** One URL for everyone. Per-port listeners,
   `tunnel-1..N`, `/freeflow` output, and `docs/03-*` go away.
5. **429 → restart: signal → existing host watcher.** The router writes the
   same `signals.json` record it writes today; a `--signals` mode in
   `keep-cold.mjs`, invoked on a short timer, consumes it and restarts
   `warp-N`. The relay container keeps **no** Docker socket.
6. **POST replay scope: conservative.** A POST body is replayed only on 429 and
   on pre-response connection failure. `502`/`503`/`504` are forwarded to the
   client without replay — those statuses do not prove the request was not
   billed, and this workload is billable chat-completions. `GET`/`HEAD` may
   retry on all of them.

## 3. Target architecture

```
client ──► ONE http.Server on BASE_PORT (host 3100 or one Quick Tunnel)
             │
             ├─ auth gate      : proxy Basic / x-relay-auth / allowlist
             ├─ router         : pick lane (sticky per session)
             ├─ HTTP path      : buffer body (cap) ─► fetch via lane Agent
             │                   └─ 429/5xx/connect-fail → next lane, replay
             ├─ CONNECT path   : pick lane → socks5Connector → 200 → pipe
             └─ health path    : no target → GET cdn-cgi/trace via lane
                                 │
                                 ▼
                     lane N = Agent{ socks5Connector(warp-N:1080) }
                                 │
                                 ▼
                       warp-N ──► Cloudflare WARP ──► upstream
```

No per-port listeners. No mux. One `net.Server` for CONNECT and one
`http.Server` for everything else — Node's `http.Server` already owns both
(`server.on("connect")`), so it is literally one listener.

### 3.1 Lane model

One object per WARP container, replacing `createRelayInstance`:

```js
{ n, host, port, agent,           // Agent with socks5Connector
  sick, probeOutUntil, cool429Until,   // reuses mux.mjs's two-lane cooldown
  inflight, last429At }
```

`server.mjs` keeps `socks5Connector` unchanged (`server.mjs:157-249`); it is
already the correct primitive. It moves from "one Agent per listener" to "one
Agent per lane in a shared array".

### 3.2 Selection

`pickLane(key)` replaces `mux.mjs:75-90`:

- usable = `!sick && now >= probeOutUntil && now >= cool429Until`
- `sticky.get(key)` and usable → that lane (no TTL)
- else round-robin over usable, record sticky
- none usable → `503 Retry-After: 30` (unchanged contract, `mux.mjs:206-212`)

**Key derivation, in order:**

1. `x-opencode-session` || `x-session-affinity` || `x-session-id`
2. peer address — the pre-rewrite fallback, still per-client behind a mesh
   ingress, and wrong behind a tunnel (accepted: those clients are already
   grouped by the tunnel, and only session-less clients reach this tier)
3. the constant `"unknown"` when the peer address is absent

A project-header hash was considered for tier 2 and rejected: the system
prompt is identical across clients, so it collides exactly like credentials.

Credentials are deliberately **not** in the key: `Authorization: Bearer public`
is byte-identical across all OpenCode free-tier clients (keep-cold sends the
same literal, `keep-cold.mjs:702`), so a credential key collapses every client
onto one lane — the exact bug this rewrite removes.

**No TTL, capped LRU.** An entry survives until evicted by
`RELAY_LANE_STICKY_MAX` (default 10 000). No expiry: a conversation idle for
an hour must still resume on its issuing egress, or its prior reasoning stops
decrypting. Eviction is a memory guard only; the evicted key simply re-picks.
`lastSeen` is stored but only used for LRU ordering. This intentionally
replaces `docs/01`'s "the client pins follow-up turns to the issuing relay"
directive — the router now does that, and its stickiness is stronger than the
old 600 s window, so that part of `docs/01` is rewritten rather than dropped
silently.

**Sticky moves with the failover.** When a request on lane A fails over to
lane B and B serves it, the key is re-pointed at B. A stays on the sticky key
and every later request would retry the known-bad lane first.

### 3.3 HTTP request path

Order matters; each step is a place the current code already is.

1. Resolve target + path (existing `server.mjs:332-348`, absolute-form
   support retained).
2. Health path (no target) — unchanged, but through a picked lane instead of
   `instance N`'s fixed `dispatcher`.
3. Allowlist + `x-relay-auth` + proxy-auth (existing `server.mjs:370-406`,
   unchanged, including the origin-escape check).
4. Buffer the body when it is replayable:
   - `GET`/`HEAD`: nothing to buffer
   - any body: read up to `RELAY_LANE_RETRY_MAX_BODY` (default 1 MiB). If the
     body ends within the cap, it is replayable (this covers `Content-Length`
     **and** chunked bodies — the chat-completions shape is small either way).
     If it overflows, mark non-replayable and stream `buffer + remainder`
     onward without buffering further.
5. Attempt loop over lanes, max `RELAY_LANE_RETRY_MAX` (default 3):
   - `fetch(url, { dispatcher: lane.agent, body, redirect: "error", signal })`
   - Retry predicate:
     - `429` on any method → replayable request moves to the next lane
     - `502`/`503`/`504` → moves only for `GET`/`HEAD`; a POST is forwarded
       as-is (it may already have been billed)
     - connection error / dial timeout before any response → always retryable
   - The lane that failed is cooled (`cool429Until` for 429, `probeOutUntil`
     for a connect failure) and the key's sticky entry re-points at the last
     lane that served the request
   - a response that is not retried → forward it exactly as today
     (`server.mjs:466-485`, including the `flushHeaders` TTFB kickstart and
     the zero-copy `pipeline`)
6. Signal **every** lane that 429s, not just the final outcome. Each failed
   attempt that saw a 429 writes `{relay: n, model, status: 429, retryAfter}`
   through the existing `signalWriter` (`server.mjs:446-464`). A 200 on the
   eventual lane writes its recovery record as today. Without per-attempt
   signalling, a lane that 429s and is immediately bypassed would never be
   repaired by keep-cold. `x-keep-cold-probe` traffic still never signals.
7. In-flight cap stays per process (`MAX_INFLIGHT`), since there is one
   listener now.

**Retry must not double-charge.** A 429 is the signal to move, not an error to
hide from the client: the retried request on lane M is the client's request
and is signalled normally. If every lane 429s, the last 429 is forwarded.

**Buffering cap is a guard, not a feature.** `RELAY_LANE_RETRY_MAX_BODY` bounds
memory; a chat-completions preamble is a few KiB, so the cap can stay small.
`ponytail:` comment names the ceiling and the upgrade path (spill to disk if a
real workload ever needs large replayable bodies).

### 3.4 CONNECT path

Existing `server.on("connect")` handler (`server.mjs:516-613`) keeps its
allowlist, token, proxy-auth, and 443/80 rules. Only the dial changes: it uses
`pickLane(key)` and that lane's SOCKS coordinates instead of `warpHost` of
instance N. A tunnel stays on one egress, as in rota.

### 3.5 Health probes (internal)

`mux.mjs:107-153` moves into the router unchanged: staggered `GET /` per lane,
pass clears `sick` + `probeOutUntil`, fail sets them for `RELAY_LANE_COOLDOWN_S`.
The health path walks a real SOCKS egress, so an egress-level death still
surfaces as a failed probe, exactly as today.

### 3.6 keep-cold lane targeting

keep-cold probes each egress individually. With one port it needs a way to
name a lane. Two transports, two ways to say "lane N":

**Legacy transport** (`keep-cold.mjs:1063`): add a `x-relay-lane: N` request
header, honored only when `x-keep-cold-probe` is also present.

**wreq transport** (`keep-cold.mjs:1000-1010`): wreq's only proxy knob is
`proxy?: string` (`node_modules/wreq-js/dist/wreq-js.d.ts:342,447,484`) — it
cannot inject proxy headers. Refer to the lane in the proxy URL's userinfo:

```
http://lane-<N>@<RELAY_HOST>:<BASE_PORT>
```

The relay recognizes a `lane-<N>` user, serves lane N directly (no sticky, no
failover), and otherwise treats the request as an ordinary proxy request. This
keeps the probe traversing the real relay path — allowlist, auth, target
headers, SOCKS egress — instead of pointing wreq at `warp-N:1080` and
measuring something that is not the shipped route.

Guard against lane pinning by clients: both forms are honored **only** when
`x-keep-cold-probe` is present (header) or the proxy user matches `lane-*`
together with a configured `RELAY_PROXY_AUTH` (userinfo). A client-visible lane
pin would undo the rewrite. `x-relay-lane` and `Proxy-Authorization` are in
`DROP_HEADERS` and never reach upstream.

keep-cold's URL becomes `http://${RELAY_HOST}:${BASE_PORT}` for every lane,
with the lane carried as above. `hard-reset-hunter.mjs:33` builds
`http://10.244.0.100:${3000+index}` separately; same fix.

### 3.7 429 → restart

Already working end to end, just slowly:

```
router  ──signals.json──►  keep-cold --sweep  ──docker restart warp-N──► warp-N
        (instant)          (every 5/15 min)
```

Add `keep-cold.mjs --signals`:

- reads + consumes `signals.json` via the existing `readSignals()`
- skips records older than the relay's `lastRotate` (existing rule,
  `keep-cold.mjs:2159`)
- rebuilds only the **history-derived** guards, since there is no probe pass:
  `quotaCapped` / `serverErrCapped` are computed from `state.relays[*].last429`
  inside the existing 900 s window, exactly as `pass()` does at
  `keep-cold.mjs:2200-2225`. This is what stops a model that is capped
  pool-wide from restarting every lane within one minute.
- applies `classifyRelay`'s numeric retry-after rule
  (`keep-cold.mjs:2039-2051`): a 429 carrying `retry-after <= 120` schedules a
  later probe instead of restarting
- otherwise calls the **existing** `classifySignal()` — gate, cooldown,
  tier-1/tier-2, `rotateContainer`
- exits without probing anything else (no sweep, no audit)

Wiring: `systemd/warp-keep-cold-signals.timer`, `OnCalendar=*:0/1` (every
minute, `AccuracySec=5s`), unit runs
`flock -n /tmp/warp-keep-cold.lock node keep-cold.mjs --signals`. Reusing the
sweep's lock means the two can never rotate concurrently, and a sweep in
progress makes the signals pass skip its minute rather than queue.

Worst case added latency is one timer period. If that is not fast enough, the
upgrade path is a systemd path unit on `.keep-cold/signals.json` — noted in a
`ponytail:` comment, not built now.

Why not give the relay container the Docker socket: it is the network
boundary that terminates untrusted proxy traffic. Host root inside it turns
any relay-side bug into host compromise. The signal file already crosses that
boundary and is the correct seam.

## 4. Files

### New
| File | Role |
|---|---|
| `router.mjs` | lane pool, selection, retry loop, probe scheduler — lifted from `mux.mjs` and grown |
| `test/router.test.mjs` | replaces `test/mux.test.mjs`; single port, N fake lanes |
| `systemd/warp-keep-cold-signals.service` / `.timer` | minute-granularity signal consumption |
| `docs/07-single-url-router.md` | client-facing: one URL, how routing works |

### Rewritten
| File | Change |
|---|---|
| `server.mjs` | one listener; lanes instead of `createRelayInstance`; retry loop; `x-relay-lane` |
| `generate-compose.sh` | one published port; no `tunnel-1..N`, only `tunnel-0` |
| `docker-compose.yml` | regenerated: `3100:3000`, no tunnel siblings |
| `get-status.sh` | one URL; per-lane egress table reads `warp-N` directly |
| `setup.sh` | drop `/freeflow` printing; print the single URL |
| `keep-cold.mjs` | `--signals` mode; `x-relay-lane`; lane URL from one port |
| `hard-reset-hunter.mjs` | lane URL from one port |
| `docs/01`, `docs/06`, `docs/README.md` | rewritten for one listener; `docs/01`'s "why one port per relay" section is replaced by the router's session-stickiness explanation |

### Deleted
`mux.mjs`, `test/mux.test.mjs`, `docs/03-use-with-pi-freeflow.md`,
`tunnel-1..N` generation, `/freeflow` output in `setup.sh`, `RELAY_MUX_*` knobs.

### Untouched
`signal-writer.mjs`, `warp-lite/`, `Dockerfile` (only the `COPY` line loses
`mux.mjs`), `logwatch.sh`, `SECURITY.md` model, allowlist/token/proxy-auth
semantics, keep-cold's probe/rotation engine.

## 5. Env surface

Removed: `RELAY_MUX`, `RELAY_MUX_PROBE_S`, `RELAY_MUX_COOLDOWN_S`,
`RELAY_MUX_429_S`, `RELAY_MUX_STICKY_S`.

**New relay knobs take the `RELAY_LANE_` prefix on purpose.**
`RELAY_COOLDOWN_S=600` already exists and means something different —
keep-cold's minimum gap between rotations of one relay
(`keep-cold.mjs:448`, `.env.example:180`) — and `generate-compose.sh` bakes
relay env from the generation-time environment including `.env`, so an
unprefixed `RELAY_COOLDOWN_S` would silently feed 600 into the router. The
prefix removes the collision instead of documenting around it.

| Var | Default | Meaning |
|---|---|---|
| `RELAY_LANE_RETRY_MAX` | `3` | lanes tried per request |
| `RELAY_LANE_RETRY_MAX_BODY` | `1048576` | largest replayable body, bytes |
| `RELAY_LANE_STICKY_MAX` | `10000` | sticky LRU capacity (no TTL) |
| `RELAY_LANE_PROBE_S` | `30` | internal lane health probe |
| `RELAY_LANE_COOLDOWN_S` | `240` | probe/connect-failure cooldown |
| `RELAY_LANE_429_S` | `120` | 429-signal cooldown |

Statuses that trigger a lane move are **not** a knob: 429 always, 502/503/504
only for `GET`/`HEAD` (§3.3 step 5). A tunable here would invite replaying
billable POSTs.

Unchanged: `RELAY_COUNT`, `BASE_PORT`, `RELAY_ALLOWED_TARGETS`,
`RELAY_AUTH_TOKEN`, `RELAY_DIRECT_PUBLIC`, `RELAY_PROXY_AUTH`,
`RELAY_MAX_INFLIGHT`, `LIVE_SIGNALS`, `LIVE_SIGNAL_FILE`, and keep-cold's
`RELAY_COOLDOWN_S`.

## 6. Testing

Existing suites keep their shape; ports move because there is one listener.

- `test/router.test.mjs` — fake lanes on distinct loopback ports, real
  `router.mjs`:
  - round-robin spread across healthy lanes
  - a probe-failing lane is skipped; all-sick yields 503
  - 429 from lane A retries the same body on lane B and the client sees B's 200
  - that failover writes a 429 signal for A *and* moves the sticky entry to B
  - chunked body within the cap is replayable; over-cap is not
  - a POST that gets 502 is not replayed; a GET that gets 502 is
  - a POST on a connect failure is replayed
  - sticky: same session header lands on the same lane across requests and
    across the sticky LRU's capacity boundary (no TTL expiry)
  - `x-relay-lane` honored with `x-keep-cold-probe`, ignored without;
    `lane-N@host` proxy user honored, plain client user not
  - CONNECT picks a lane and stays on it
- `test/relay-guard.test.mjs`, `test/universal-proxy.test.mjs`,
  `test/relay-basic-auth.test.mjs` — replace `BASE_PORT+1` with `BASE_PORT`;
  drop `RELAY_MUX: "0"`; the `pi-freeflow`-named closed-mode test gets renamed
  (behavior is unchanged and still worth testing).
- `test/keep-cold.test.mjs` — add a `--signals` case: a 429 record produces one
  restart call and no probe; a `retry-after: 60` record produces no restart;
  a model whose history hits `GLOBAL_QUOTA_GUARD` produces no restart.
- `logwatch.test.sh` — untouched.

Run: `npm test` (unchanged).

## 7. Acceptance

1. A client configured with exactly one URL serves traffic; no other port
   accepts a proxy request.
2. Killing `warp-3` mid-pool: requests recover on a sibling without the client
   seeing a failure (when replayable), and the client URL never changes.
3. A 429 from lane N produces a `{"relay":N,"status":429}` record and a
   `warp-N` restart within ~1 minute, without the relay container having any
   Docker access.
4. Two clients with different session headers do not pin to the same lane,
   including behind the Quick Tunnel, and the same session header stays on one
   lane for the whole conversation with no expiry.
5. `docker compose config -q` passes; `generate-compose.sh 10` is
   deterministic; every documented URL in `docs/` matches what the generator
   emits.

## 8. Risks

| Risk | Mitigation |
|---|---|
| Replayed POST double-bills the upstream | POST replay limited to 429 and pre-response connect failure; 502/503/504 never replay a POST (§3.3) |
| Buffering grows memory | Cap is explicit and small; over-cap bodies stream unretried |
| Sticky collapses behind a tunnel | Key is session-header-derived, not address- or credential-derived |
| Sticky LRU never expires and grows | Hard cap of 10 000 entries, LRU eviction; an evicted key just re-picks |
| Rotation loses reasoning continuity | Sticky is per session and non-expiring, so a conversation keeps its issuing egress; only 429/failure moves it |
| Rotation pole-vaults a genuinely quota-exhausted model | keep-cold's `GLOBAL_QUOTA_GUARD` skips rotation when a model is capped pool-wide; `--signals` recomputes that guard from state history |
| Concurrent sweeps + signal passes | Both take the same flock |
| Someone pins lanes to defeat rotation | Lane targeting honored only alongside `x-keep-cold-probe` / a `lane-*` proxy user |

## 9. Migration

No data migration. A deployed host runs
`./generate-compose.sh <N> --with-tunnels`, then `docker compose up -d --build`;
`tunnel-1..N` containers are orphaned — `docker compose down --remove-orphans`
clears them. Client config changes once: paste the single URL (the `tunnel-0`
URL already exists) instead of N `/freeflow add` lines.
