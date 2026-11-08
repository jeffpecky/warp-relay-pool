# L7 Single-URL Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the N+1-port relay (per-port listeners + L4 TCP mux) with one L7 listener that routes each request across the WARP lane pool with per-request failover.

**Architecture:** `server.mjs` collapses to a single `http.Server` that owns both the HTTP and `CONNECT` paths. A new `router.mjs` holds the lane pool (one undici `Agent` per WARP container), per-session sticky selection, health probing, and the replayable-body + retry policy. `mux.mjs` and the per-port listeners are deleted; the pi-freeflow multi-URL lane with them.

**Tech Stack:** Node 22 (ESM), `node:http`, `node:net`, `undici` `Agent`, `node:test`.

**Spec:** `docs/superpowers/specs/2026-11-08-l7-single-url-router-design.md`

## Global Constraints

- Node `>=22`, ESM (`.mjs`), `npm test` must pass unchanged as the entry point.
- The relay container keeps **no Docker socket** and runs `USER node`.
- `mux.mjs` must not exist at the end of Task 5; nothing may import it.
- New relay env knobs use the `RELAY_LANE_` prefix. Never introduce an
  unprefixed `RELAY_COOLDOWN_S`, `RELAY_PROBE_S`, `RELAY_429_S` or
  `RELAY_STICKY_S` into relay env — `RELAY_COOLDOWN_S` belongs to keep-cold.
- Defaults, exactly: `RELAY_LANE_RETRY_MAX=3`,
  `RELAY_LANE_RETRY_MAX_BODY=1048576`, `RELAY_LANE_STICKY_MAX=10000`,
  `RELAY_LANE_PROBE_S=30`, `RELAY_LANE_COOLDOWN_S=240`, `RELAY_LANE_429_S=120`.
- Retry statuses are not configurable: 429 always; 502/503/504 only for
  `GET`/`HEAD`; any pre-response connection error always.
- Sticky keys are session-derived. Credentials are never the key.
- Allowlist / `x-relay-auth` / proxy-auth semantics are frozen: no test in
  `test/relay-guard.test.mjs` or `test/universal-proxy.test.mjs` may change
  its assertions, only its port wiring and its `pi-freeflow` test title.
- Lane targeting (`x-relay-lane`, `lane-N` userinfo) is honored **only** for
  keep-cold traffic. A plain client can never pin a lane.

## Review Focus

- **`x-relay-lane` out of range or malformed** (`999`, `0`, `abc`, `-1`, empty)
  — expected: ignored, normal selection, no crash. *(Task 7)*
- **Every lane unusable** — expected: `503` with `Retry-After: 30` on both the
  HTTP and the `CONNECT` path, never a hang or a partial write.
  *(Tasks 1, 5, 6)*
- **Client disconnects while the body is being buffered for replay** —
  expected: the read rejects, the caller answers or drops the socket, the
  in-flight counter decrements, no unhandled rejection. *(Task 2)*
- **`Proxy-Authorization: Basic lane-3:<wrong>` with `RELAY_PROXY_AUTH` set** —
  expected: no pin. **With `RELAY_PROXY_AUTH` unset** — expected: no pin
  either; the userinfo form requires configured credentials. *(Task 7)*
- **Sticky LRU eviction mid-conversation** — expected: the key re-picks a
  usable lane; no error surfaced to the client. *(Task 1)*

---

### Task 1: Lane pool — selection, cooldowns, sticky LRU

**Files:**
- Create: `router.mjs`
- Test: `test/router.test.mjs`

**Interfaces:**
- Produces:
  - `createLanes({ count, agentFor, probe, probeIntervalMs, cooldownMs, cooldown429Ms, stickyMax, log }) -> LanePool`
  - `LanePool = { lanes, pick(key), bind(key, lane), cool(lane, ms, why, kind), start(), stop(), stickySize() }`
  - `Lane = { n, host, port, agent, sick, probeOutUntil, cool429Until, inflight }`
  - `laneKey(headers, peerAddress) -> string` (never `""`; falls back to the peer address, `"unknown"` when that is absent)
- `agentFor(n)` returns an undici `Agent` and sets the lane's `host`/`port`.
- `probe` defaults to an internal `GET /` against `127.0.0.1:BASE_PORT` carrying
  `x-relay-lane: n` and `x-keep-cold-probe: 1`, i.e. the same health path a
  client's lane would answer. Tests always pass their own `probe`.

- [ ] **Step 1: Write the failing test**

```js
// test/router.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import { createLanes, laneKey } from "../router.mjs";

const quiet = () => {};
function pool(count, over = {}) {
  return createLanes({ count, agentFor: (n) => ({ n }), probe: async () => true, log: quiet, ...over });
}

test("round-robin spreads across usable lanes", () => {
  const p = pool(3);
  const got = [p.pick("a"), p.pick("b"), p.pick("c"), p.pick("d")].map((l) => l.n);
  assert.deepEqual(got, [1, 2, 3, 1]);
});

test("sticky key holds its lane with no TTL", () => {
  const p = pool(3);
  const first = p.pick("ses_a");
  assert.equal(p.pick("ses_a").n, first.n);
  assert.equal(p.pick("ses_a").n, first.n, "still held on a later call");
});

test("a cooled lane is skipped and the sticky key re-picks", () => {
  const p = pool(2);
  const held = p.pick("ses_b");
  p.cool(held, 60_000, "test");
  assert.notEqual(p.pick("ses_b").n, held.n);
});

test("all lanes unusable yields null", () => {
  const p = pool(2);
  for (const l of p.lanes) p.cool(l, 60_000, "test");
  assert.equal(p.pick("x"), null);
});

test("the 429 lane and the probe lane decay independently", () => {
  const p = pool(1);
  const lane = p.lanes[0];
  p.cool(lane, 60_000, "429", "signal");
  assert.equal(lane.probeOutUntil, 0, "a 429 must not write the probe lane");
  p.cool(lane, 1_000, "probe");
  assert.ok(lane.cool429Until > Date.now() + 50_000, "a probe cooldown must not clear the 429 lane");
});

test("laneKey prefers session headers over credentials", () => {
  assert.equal(laneKey({ "x-opencode-session": "s1", authorization: "Bearer public" }, "1.2.3.4"), "s1");
  assert.equal(laneKey({ "x-session-affinity": "s2" }, "1.2.3.4"), "s2");
  assert.equal(laneKey({ "x-session-id": "s3" }, "1.2.3.4"), "s3");
});

test("laneKey falls back to the peer address, then a constant", () => {
  assert.equal(laneKey({ authorization: "Bearer public" }, "1.2.3.4"), "1.2.3.4");
  assert.equal(laneKey({}, ""), "unknown");
});

test("sticky LRU evicts beyond stickyMax, and the evicted key re-picks", () => {
  const p = pool(2, { stickyMax: 2 });
  const a = p.pick("a");
  p.pick("b");
  p.pick("c");
  assert.equal(p.stickySize(), 2);
  assert.ok(p.pick("a"), "an evicted key still gets a lane");
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/router.test.mjs`
Expected: FAIL — `Cannot find module '../router.mjs'`.

- [ ] **Step 3: Implement `router.mjs`**

Lift selection and cooldown logic from `mux.mjs:56-103` and `:75-90`, replacing
the TTL field with an LRU `Map` (`delete`+`set` on hit) capped at `stickyMax`.
Keep the two-lane cooldown exactly: `cool(lane, ms, why, "signal")` writes only
`cool429Until`, `cool(lane, ms, why)` writes only `probeOutUntil`, and
`usable()` reads `!sick && now >= probeOutUntil && now >= cool429Until`.
`pick` returns `null` when no lane is usable — the caller owns the 503.
`laneKey` order: `x-opencode-session`, `x-session-affinity`, `x-session-id`,
peer address, `"unknown"`. Credentials are never consulted.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/router.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add router.mjs test/router.test.mjs
git commit -m "feat(router): lane pool with session-sticky selection"
```

---

### Task 2: Replayable body buffering

**Files:**
- Modify: `router.mjs`
- Test: `test/router.test.mjs`

**Interfaces:**
- Produces: `readReplayableBody(req, maxBytes) -> Promise<{ replayable: boolean, body: Buffer | Readable | null }>`
  - `GET`/`HEAD` → `{ replayable: true, body: null }`
  - body fully read within `maxBytes` → `{ replayable: true, body: Buffer }`
    (covers `Content-Length` **and** chunked)
  - body overflows → `{ replayable: false, body: Readable }` that emits the
    already-read prefix followed by the remainder, without buffering further
  - client gone mid-read → rejects with the socket error

- [ ] **Step 1: Write the failing test**

```js
test("a body under the cap is buffered and replayable", async () => {
  const out = await readReplayableBody(Readable.from([Buffer.from("hello ")]), 1024);
  assert.equal(out.replayable, true);
  assert.equal(out.body.toString(), "hello ");
});

test("a body over the cap streams and is not replayable", async () => {
  const chunks = [Buffer.alloc(600, "a"), Buffer.alloc(600, "b")];
  const out = await readReplayableBody(Readable.from(chunks), 512);
  assert.equal(out.replayable, false);
  const got = [];
  for await (const c of out.body) got.push(c);
  assert.equal(Buffer.concat(got).length, 1200, "no byte lost to the overflow");
});

test("a client that dies mid-buffer rejects instead of hanging", async () => {
  const req = new Readable({ read() { this.destroy(Object.assign(new Error("aborted"), { code: "ECONNRESET" })); } });
  await assert.rejects(readReplayableBody(req, 1024), /aborted/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/router.test.mjs`
Expected: FAIL — `readReplayableBody is not a function`.

- [ ] **Step 3: Implement `readReplayableBody` in `router.mjs`**

Accumulate until the cap is crossed; on overflow return
`Readable.from([prefix, ...rest])`, where `rest` is a generator delegating to the
un-read portion of `req`. `GET`/`HEAD` short-circuit. Add a `ponytail:` comment
naming the ceiling (memory-bound replay) and the upgrade path (spill to a temp
file if large replayable bodies are ever needed).

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/router.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add router.mjs test/router.test.mjs
git commit -m "feat(router): cap-bounded replayable body buffering"
```

---

### Task 3: Retry predicate

**Files:**
- Modify: `router.mjs`
- Test: `test/router.test.mjs`

**Interfaces:**
- Produces: `retryable({ method, status, error, replayable }) -> boolean`
  - `error` truthy (pre-response failure) → `true`
  - `replayable === false` → `false`
  - `status === 429` → `true`
  - `status in {502,503,504}` → `method` is `GET` or `HEAD`
  - otherwise `false`

- [ ] **Step 1: Write the failing test**

```js
test("a 429 replays a POST", () => {
  assert.equal(retryable({ method: "POST", status: 429, replayable: true }), true);
});
test("a 502 replays a GET but never a POST", () => {
  assert.equal(retryable({ method: "GET", status: 502, replayable: true }), true);
  assert.equal(retryable({ method: "POST", status: 502, replayable: true }), false);
});
test("a 504 never replays a POST", () => {
  assert.equal(retryable({ method: "POST", status: 504, replayable: true }), false);
});
test("a connect error replays regardless of method", () => {
  assert.equal(retryable({ method: "POST", error: new Error("ECONNREFUSED"), replayable: true }), true);
});
test("a non-replayable body is never retried", () => {
  assert.equal(retryable({ method: "POST", status: 429, replayable: false }), false);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/router.test.mjs`
Expected: FAIL — `retryable is not a function`.

- [ ] **Step 3: Implement `retryable` in `router.mjs`**

Pure function, no I/O, exact predicate from Global Constraints.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/router.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add router.mjs test/router.test.mjs
git commit -m "feat(router): retry predicate with POST replay limits"
```

---

### Task 4: Single listener — HTTP path through the lane pool

**Files:**
- Modify: `server.mjs`

**Interfaces:**
- Consumes: `createLanes`, `laneKey`, `readReplayableBody`, `retryable` from `router.mjs`.
- Produces: one `http.Server` listening on `BASE_PORT`; each lane carries the
  `agent` the old per-instance dispatcher built.
- Keeps: health path, allowlist gate, `x-relay-auth`, `proxyCredOk`, SSE
  streaming, 5-minute upstream cap, client-disconnect abort, `MAX_INFLIGHT`.

- [ ] **Step 1: Replace `createRelayInstance` with a lane pool**

Delete `createRelayInstance` and the `for (let i = 1; i <= COUNT; i++)` loop
(`server.mjs:301-630`). Build the pool once:

```js
const lanes = createLanes({
  count: COUNT,
  agentFor: (n) => new Agent({
    connect: socks5Connector(process.env[`WARP_HOST_${n}`] || `warp-${n}`,
                             process.env[`WARP_PORT_${n}`] || "1080", 30_000),
    keepAliveTimeout: 60_000, keepAliveMaxTimeout: 90_000,
    headersTimeout: 60_000, bodyTimeout: 0, connections: 32,
  }),
  probeIntervalMs: intEnv(RELAY_LANE_PROBE_S, 30) * 1000,
  cooldownMs: intEnv(RELAY_LANE_COOLDOWN_S, 240) * 1000,
  cooldown429Ms: intEnv(RELAY_LANE_429_S, 120) * 1000,
  stickyMax: intEnv(RELAY_LANE_STICKY_MAX, 10000),
  log: (msg) => console.log(`[router] ${msg}`),
});
```

`createLanes` sets each lane's `host`/`port` from the same env it hands
`agentFor`, so the `CONNECT` path can dial them.

- [ ] **Step 2: Move the handler onto the single server**

Create one `http.createServer(handler)` on `BASE_PORT` with the current
`keepAliveTimeout` / `headersTimeout` / `requestTimeout: 0` / `timeout: 0` /
`setNoDelay` settings. Keep the health, allowlist, auth and proxy-auth blocks
byte-for-byte. Replace the single `fetch` with the attempt loop:

```js
const key = laneKey(req.headers, req.socket?.remoteAddress || "");
const pinned = null; // Task 7 wires resolvePinnedLane here; never retries.
let lane = pinned || lanes.pick(key);
if (!lane) {
  res.writeHead(503, { "content-type": "application/json", "retry-after": "30" });
  return res.end(JSON.stringify({ error: "No relay available" }));
}
const { replayable, body } = await readReplayableBody(req, intEnv(RELAY_LANE_RETRY_MAX_BODY, 1048576));
for (let attempt = 0, tried = 0; ; attempt++) {
  if (attempt > 0) {
    lane = lanes.pick(key);
    if (!lane || ++tried > intEnv(RELAY_LANE_RETRY_MAX, 3)) /* forward the last response */;
  }
  // fetch(upstreamUrl, { dispatcher: lane.agent, body, ... })
  // on 429: signalWriter.record({ relay: lane.n, model, status: 429, retryAfter });
  //         lanes.cool(lane, COOLDOWN_429, "429 from upstream", "signal");
  // on connect error: lanes.cool(lane, COOLDOWN, "lane connect failed");
  // retryable(...) decides whether to continue; otherwise forward.
}
```

On success, `lanes.bind(key, lane)`. When the body was buffered, sniff the model
with `MODEL_FIELD_RE` against it directly; keep `createModelSniffer()` only for
the non-replayable stream path. Signal **every** attempt that saw a 429, not
just the final one.

- [ ] **Step 3: Emit one startup line**

Log exactly one `listening on ${bind}:${BASE_PORT} -> ${COUNT} lane(s)` line.
The test harnesses wait for the substring `listening on`.

- [ ] **Step 4: Confirm the old suites now fail on port wiring**

Run: `node --test test/relay-guard.test.mjs test/universal-proxy.test.mjs test/relay-basic-auth.test.mjs`
Expected: FAIL — connections refused on `BASE_PORT+1`. This is the expected
state until Task 5.

- [ ] **Step 5: Commit**

```bash
git add server.mjs
git commit -m "feat(relay): one L7 listener with per-request lane failover"
```

---

### Task 5: Delete the mux and migrate the port wiring

**Files:**
- Delete: `mux.mjs`, `test/mux.test.mjs`
- Modify: `test/relay-guard.test.mjs`, `test/universal-proxy.test.mjs`, `test/relay-basic-auth.test.mjs`, `Dockerfile`, `.github/workflows/ci.yml`
- Test: `test/router.test.mjs` (503 contract)

- [ ] **Step 1: Write the failing 503 test**

```js
test("a pool with no usable lane answers 503 with Retry-After", async () => {
  // One lane pointed at a closed port: the connect failure cools it, so the
  // second request must be refused at the edge instead of hanging.
  const port = 33000 + Math.floor(Math.random() * 500);
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: { ...process.env, RELAY_COUNT: "1", BASE_PORT: String(port),
           WARP_HOST_1: "127.0.0.1", WARP_PORT_1: "1", RELAY_LANE_PROBE_S: "0",
           RELAY_LANE_COOLDOWN_S: "600" },
    stdio: ["ignore", "pipe", "pipe"],
  });
  await once(child.stdout, "data");
  const send = () => fetch(`http://127.0.0.1:${port}/v1/models`, {
    headers: { "x-relay-target": "https://opencode.ai", "x-relay-path": "/v1/models" },
  });
  await send();                      // warms the failure and cools the lane
  const res = await send();
  assert.equal(res.status, 503);
  assert.equal(res.headers.get("retry-after"), "30");
  child.kill();
});
```

- [ ] **Step 2: Delete the mux and its test**

```bash
git rm mux.mjs test/mux.test.mjs
```

- [ ] **Step 3: Repoint the three relay suites at the single port**

In each: set `RELAY_PORT = BASE_PORT` (drop every `+ 1` in the port constants),
delete the `RELAY_MUX: "0"` line and its comment, and rename the
`"... (pi-freeflow default)"` test title to `"... (closed-mode default)"`
without touching its assertions. Keep the disjoint port bands.

- [ ] **Step 4: Update the image and CI**

`Dockerfile`: `COPY server.mjs router.mjs signal-writer.mjs ./` and
`EXPOSE 3000`.
`ci.yml`: add `node --check router.mjs` to the JS syntax check.

- [ ] **Step 5: Run the full suite**

Run: `npm test`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "refactor: remove the TCP mux and per-port listeners"
```

---

### Task 6: CONNECT path through a lane

**Files:**
- Modify: `server.mjs`
- Test: `test/universal-proxy.test.mjs` (existing CONNECT cases now exercise it)

**Interfaces:**
- Consumes: `lanes.pick`, `laneKey`.
- Produces: `CONNECT` keeps every existing gate; only the dial targets the
  picked lane.

- [ ] **Step 1: Rewire the dial**

In `server.on("connect")` (`server.mjs:516-613`) replace the fixed
`socks5Connector(warpHost, warpPort, 30_000)` with
`socks5Connector(lane.host, lane.port, 30_000)`, where `lane` came from
`lanes.pick(laneKey(req.headers, req.socket?.remoteAddress || ""))`. If `lane`
is `null`, answer `503 Service Unavailable` through the handler's existing
`deny()` helper — matching the HTTP path, never a hang. Leave the allowlist,
`isAuthorized`, `proxyCredOk`, and 443/80 rules untouched.

- [ ] **Step 2: Run the CONNECT cases**

Run: `node --test test/universal-proxy.test.mjs`
Expected: PASS, including the two `CONNECT pipes raw TCP` cases and the
`CONNECT ... is 403/401` cases (they now traverse the lane pick).

- [ ] **Step 3: Commit**

```bash
git add server.mjs
git commit -m "feat(relay): CONNECT dials its picked lane"
```

---

### Task 7: keep-cold lane targeting

**Files:**
- Modify: `router.mjs`, `server.mjs`
- Test: `test/router.test.mjs`

**Interfaces:**
- Produces: `resolvePinnedLane({ headers, proxyUser, proxyPass, lanes, proxyAuth }) -> Lane | null`
  - `headers["x-relay-lane"]` present **and** `headers["x-keep-cold-probe"]`
    present, value an integer in `1..lanes.length` → that lane
  - `proxyUser` matching `/^lane-(\d+)$/` in range, `proxyAuth` non-empty, and
    `proxyPass` equal to `proxyAuth`'s password half → that lane
  - anything else (out of range, non-numeric, no probe marker, no credentials)
    → `null`
- `x-relay-lane` joins `DROP_HEADERS`; `proxy-authorization` already there.
- A pinned lane bypasses `lanes.pick` and `lanes.bind`, and never retries.

- [ ] **Step 1: Write the failing test**

```js
test("x-relay-lane pins only with the probe marker", () => {
  const lanes = pool(3).lanes;
  assert.equal(resolvePinnedLane({ headers: { "x-relay-lane": "2", "x-keep-cold-probe": "1" }, lanes })?.n, 2);
  assert.equal(resolvePinnedLane({ headers: { "x-relay-lane": "2" }, lanes }), null);
});

test("out-of-range and malformed lane ids are ignored", () => {
  const lanes = pool(3).lanes;
  for (const v of ["0", "4", "999", "abc", "-1", ""]) {
    assert.equal(resolvePinnedLane({ headers: { "x-relay-lane": v, "x-keep-cold-probe": "1" }, lanes }), null);
  }
});

test("lane-N userinfo pins only with configured credentials", () => {
  const lanes = pool(3).lanes;
  const base = { headers: { "x-keep-cold-probe": "1" }, proxyUser: "lane-2", lanes };
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "u:p" })?.n, 2);
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "" }), null, "no RELAY_PROXY_AUTH means no pin");
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "u:p", proxyPass: "wrong" }), null);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/router.test.mjs`
Expected: FAIL — `resolvePinnedLane is not a function`.

- [ ] **Step 3: Implement `resolvePinnedLane` in `router.mjs`**

Split `proxyCredOk` in `server.mjs` into `proxyAuthInfo(req) -> { ok, user, pass }`
so the handler gets the user and password without parsing twice, then replace
the `const pinned = null;` placeholder from Task 4 with
`resolvePinnedLane({ headers: req.headers, proxyUser, proxyPass, lanes, proxyAuth: RELAY_PROXY_AUTH })`.
A pinned lane skips `lanes.bind` and the attempt loop entirely.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/router.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add router.mjs server.mjs test/router.test.mjs
git commit -m "feat(relay): keep-cold lane targeting via header and userinfo"
```

---

### Task 8: `keep-cold.mjs --signals`

**Files:**
- Modify: `keep-cold.mjs`
- Test: `test/keep-cold.test.mjs`

**Interfaces:**
- Consumes: existing `readSignals()`, `classifySignal(index, model, ctx)`,
  `runRotation(job, state, dirty)`, `relayEntry`, `patchRelay`, `runPool`, `log`.
- Produces: `--signals` mode. Reads the signal file **unconditionally** (does
  not require `LIVE_SIGNALS=1`), probes nothing, rotates only what the records
  name. Exit `0` when nothing is left dirty, `1` otherwise.

- [ ] **Step 1: Write the failing test**

```js
test("--signals restarts the named relay without probing anything", async () => {
  const sc = newScenario({ count: 20 });
  await startRelays(sc);
  seedCleanState(sc);
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now(), relay: 12, model: "model-a", status: 429, retryAfter: null }],
  }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.equal(r.code, 1, r.stdout + r.stderr);
  assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-12"]);
  assert.equal(requests(sc, 12), 0, "--signals must not probe");
  assert.deepEqual(JSON.parse(fs.readFileSync(sc.signalFile, "utf8")), { signals: [] });
});

test("--signals leaves a short retry-after alone", async () => {
  const sc = newScenario({ count: 4 });
  await startRelays(sc);
  seedCleanState(sc);
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now(), relay: 2, model: "model-a", status: 429, retryAfter: 60 }],
  }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.equal(r.code, 1, r.stdout + r.stderr);
  assert.deepEqual(restartLines(sc), []);
  assert.match(r.stdout, /retry-after 60s/);
});

test("--signals restarts nothing when history says the model is quota-capped", async () => {
  const sc = newScenario({ count: 20 });
  await startRelays(sc);
  const relays = {};
  for (let i = 1; i <= 20; i++) {
    relays[String(i)] = entry({
      lastProbe: Date.now(), lastClean: Date.now(),
      last429: { model: "model-a", ts: Date.now() - 60_000 },
    });
  }
  writeStateFile(sc, relays);
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now(), relay: 3, model: "model-a", status: 429, retryAfter: null }],
  }));
  const r = await runOnce(sc, { GLOBAL_QUOTA_GUARD: "9" }, ["--signals"]);
  assert.deepEqual(restartLines(sc), []);
  assert.match(r.stdout, /upstream quota/);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `node --test test/keep-cold.test.mjs`
Expected: FAIL — `--signals` rejected as an unknown argument.

- [ ] **Step 3: Implement `--signals`**

Add `"--signals"` to the accepted-arguments list (`keep-cold.mjs:179`), the
mutually-exclusive group (`:196`), and `USAGE`. Implement `signalsPass()`:

1. `loadState()`.
2. Build `quotaCapped` / `serverErrCapped` from `state.relays[*].last429` inside
   the 900 s window using `GLOBAL_QUOTA_GUARD` / `SERVER_ERROR_GUARD`, exactly
   as `pass()` does at `keep-cold.mjs:2200-2225`.
3. `readSignals()`; skip records with `ts <= entry.lastRotate`.
4. For a 429 with numeric `retryAfter` in `0..120`: `patchRelay({ dirty: true,
   probeAfter: at + retryAfter * 1000 })`, log `retry-after Ns — scheduling, no
   restart`, and do **not** rotate (mirrors `keep-cold.mjs:2039-2051`).
5. Otherwise `classifySignal(record.relay, record.model, ctx)` with the same
   `ctx` shape `pass()` builds.
6. `await runPool(ctx.rotations, ROTATE_CONCURRENCY, (job) => runRotation(job, state, dirty))`.
7. `process.exitCode = dirty.size === 0 ? 0 : 1`.

- [ ] **Step 4: Run test to verify it passes**

Run: `node --test test/keep-cold.test.mjs`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add keep-cold.mjs test/keep-cold.test.mjs
git commit -m "feat(keep-cold): --signals mode for minute-granularity repair"
```

---

### Task 9: keep-cold and hard-reset-hunter speak one port + a lane

**Files:**
- Modify: `keep-cold.mjs`, `hard-reset-hunter.mjs`

**Interfaces:**
- Consumes: Task 7's lane targeting.
- Produces: both probe `http://${RELAY_HOST}:${BASE_PORT}` and carry the lane as
  `x-relay-lane: <n>` (legacy transport) or `lane-<n>@` userinfo (wreq).

- [ ] **Step 1: Point every probe at the base port**

In `keep-cold.mjs` replace `BASE_PORT + index` with `BASE_PORT` at the two URL
sites (`:1009`, `:1063`) and in the two log lines printing a port range
(`:1505`, `:2176`). In `hard-reset-hunter.mjs:33` replace `3000 + index` with
`BASE_PORT`.

- [ ] **Step 2: Carry the lane**

`probeHeaders()` (`keep-cold.mjs:667-694`) gains an `index` parameter and adds
`"x-relay-lane": String(index)` to **both** the minimal and default branches;
pass `index` from `probe()`. In `wreqSession(index, wreq)` (`:1000-1010`) build
the proxy URL as

```
http://lane-<N>[:<RELAY_PROXY_AUTH password half>]@<RELAY_HOST>:<BASE_PORT>
```

and keep deleting the relay-internal headers in the wreq branch (`:1041-1043`),
adding `x-relay-lane` to that delete list. If `RELAY_PROXY_AUTH` is empty the
userinfo carries no password, which is the shape the relay accepts only when no
proxy auth is configured.

- [ ] **Step 3: Verify nothing still builds a per-lane port**

Run: `grep -n "BASE_PORT + index\|BASE_PORT + i\|3000 + index" keep-cold.mjs hard-reset-hunter.mjs`
Expected: no output.

- [ ] **Step 4: Run the keep-cold suite**

Run: `node --test test/keep-cold.test.mjs`
Expected: PASS (the harness's fake relays ignore the new header).

- [ ] **Step 5: Commit**

```bash
git add keep-cold.mjs hard-reset-hunter.mjs
git commit -m "fix(keep-cold): probe the single relay port with a lane header"
```

---

### Task 10: Signal-consumption systemd unit

**Files:**
- Create: `systemd/warp-keep-cold-signals.service`, `systemd/warp-keep-cold-signals.timer`
- Modify: `setup.sh`

- [ ] **Step 1: Write the unit and timer**

`.service`: `Type=oneshot`, `User=`/`Group=`/`WorkingDirectory=` placeholders
mirroring `warp-keep-cold.service`, `Environment=COUNT=10`,
`Environment=BASE_PORT=3000`, `Environment=RELAY_PROXY_AUTH=`, and
`ExecStart=/usr/bin/flock -n -E 0 /tmp/warp-keep-cold.lock /usr/bin/env node .../keep-cold.mjs --signals`.
`.timer`: `OnBootSec=2min`, `OnCalendar=*:0/1`, `AccuracySec=5s`,
`Unit=warp-keep-cold-signals.service`, `WantedBy=timers.target`. Reusing
`/tmp/warp-keep-cold.lock` makes a signals pass and a sweep mutually exclusive.

- [ ] **Step 2: Install it from the wizard**

In `setup.sh` beside the existing keep-cold install block (`~:690-715`), sed the
new unit's `User=`/`Group=`/`WorkingDirectory=`/`Documentation=`/`ExecStart=`/
`Environment=COUNT=`/`Environment=BASE_PORT=`/`Environment=RELAY_PROXY_AUTH=` as
the sweep's is done, copy both files to `/etc/systemd/system/`, and add
`warp-keep-cold-signals.timer` to the `systemctl enable --now` invocation.

- [ ] **Step 3: Verify the units parse**

Run: `sh -n setup.sh && grep -n "warp-keep-cold-signals" setup.sh`
Expected: `sh -n` silent; the timer appears in the enable list.

- [ ] **Step 4: Commit**

```bash
git add systemd/warp-keep-cold-signals.service systemd/warp-keep-cold-signals.timer setup.sh
git commit -m "feat(systemd): consume 429 signals every minute"
```

---

### Task 11: Compose generator — one published port, one tunnel

**Files:**
- Modify: `generate-compose.sh`, `docker-compose.yml` (regenerated), `.env.example`

**Interfaces:**
- Produces: relay published at `${ROUTER_BIND}:${HOST_LO}:${CONT_LO}` (one port),
  `tunnel-0` only, `RELAY_LANE_*` baked, `RELAY_MUX*` gone.

- [ ] **Step 1: Collapse the port range**

`HOST_HI` / `CONT_HI` become unused: emit
`- "${ROUTER_BIND}:${HOST_LO}:${CONT_LO}"`. Delete the `tunnel-1..N` while-loop
(`:312-329`) and keep only `tunnel-0`.

- [ ] **Step 2: Swap the env block**

Replace the five `RELAY_MUX_*` lines with the six `RELAY_LANE_*` knobs from
Global Constraints, keeping the `${VAR:-default}` form.

- [ ] **Step 3: Update `.env.example`**

Delete the `RELAY_MUX*` block (`:70-74`), add a `RELAY_LANE_*` block, and note
that `RELAY_COOLDOWN_S` belongs to keep-cold only.

- [ ] **Step 4: Regenerate and verify determinism**

Run: `sh ./generate-compose.sh 10 && docker compose config -q && sh ./generate-compose.sh 10 && git diff --exit-code docker-compose.yml`
Expected: valid config; the second generation is byte-identical.

- [ ] **Step 5: Commit**

```bash
git add generate-compose.sh docker-compose.yml .env.example
git commit -m "feat(compose): publish one relay port and one tunnel"
```

---

### Task 12: Client-facing scripts — one URL

**Files:**
- Modify: `get-status.sh`, `setup.sh`

- [ ] **Step 1: Rewrite the status output**

`ROUTER_PUBLISHED` sed becomes `s/^ *- "\([^":]*\):3100:.*$/\1/p`. Keep the
per-lane `EGRESS IP` rows; drop the per-lane `PORT` and `TUNNEL URL` columns and
print one header block with the single dialable URL (`${ROUTER_BIND}:3100`, or
`tunnel_url 0`). Delete `print_next_steps` and every `/freeflow` line.

- [ ] **Step 2: Rewrite the wizard's final output**

In `setup.sh` `~:730-755`, replace section 7 with: the single URL, one `curl`
health check against it, and one line naming
`warp-keep-cold-signals.timer`.

- [ ] **Step 3: Verify the scripts parse and print cleanly**

Run: `sh -n get-status.sh && sh -n setup.sh && grep -rn "freeflow" get-status.sh setup.sh`
Expected: syntax clean; no `freeflow` matches.

- [ ] **Step 4: Commit**

```bash
git add get-status.sh setup.sh
git commit -m "feat(scripts): hand out a single relay URL"
```

---

### Task 13: Docs

**Files:**
- Delete: `docs/03-use-with-pi-freeflow.md`
- Create: `docs/07-single-url-router.md`
- Modify: `docs/01-overview-how-it-works.md`, `docs/06-universal-proxy.md`, `docs/README.md`, `docs/02-deploy.md`, `docs/05-troubleshooting-faq.md`, `README.md`, `SECURITY.md`, `CONTRIBUTING.md`

- [ ] **Step 1: Rewrite the contract in `docs/01`**

Request lifecycle becomes: one listener → lane pick → allowlist → replayable
buffer → attempt loop. Replace "Why one port per relay" with "Why one URL": the
session-sticky rule (reasoning-issuer affinity is now enforced by the router,
per session, no TTL), per-request failover, and the note that a `CONNECT`
tunnel still occupies one egress for its life.

- [ ] **Step 2: Write `docs/07`**

Client-facing: the one URL, a `curl` health check, both entry shapes
(`x-relay-target`/`x-relay-path` and absolute-form), a pointer to the token
setup, and the failure semantics a client can observe (429 replay, 503 when the
pool is empty).

- [ ] **Step 3: Purge the pi-freeflow track**

`docs/README.md`: drop item 3, add 07. `README.md`: delete the "Which track am
I" table and Track B, keep the universal-proxy description, update the file map
(`mux.mjs` → `router.mjs`) and the RAM sizing that assumed N tunnels.
`docs/02`, `docs/05`, `SECURITY.md`, `CONTRIBUTING.md`: replace `:310N` / `:300N`
with `:3100`, drop `/freeflow` lines and the frozen "pi-freeflow probe lane"
wording, and describe the single-URL topology.

- [ ] **Step 4: Verify no stale references**

Run: `grep -rn "freeflow\|mux\|RELAY_MUX\|310N\|300N\|:3101" README.md docs/ SECURITY.md CONTRIBUTING.md`
Expected: no matches outside `docs/superpowers/` (historical specs and plans).

- [ ] **Step 5: Commit**

```bash
git add -A docs README.md SECURITY.md CONTRIBUTING.md
git commit -m "docs: document the single-URL router, drop the pi-freeflow track"
```

---

## Verification (whole plan)

- [ ] `npm test` passes.
- [ ] `sh ./generate-compose.sh 10 && git diff --exit-code docker-compose.yml`.
- [ ] `docker compose config -q` passes; the relay service publishes exactly one port.
- [ ] `grep -rn "mux\.mjs" .` returns nothing outside `docs/superpowers/`.
- [ ] Manual, on a host with the pool: `curl -s http://<url>/` returns a trace
      body; a POST to `/inference/v1/chat/completions` with a valid body streams;
      a 429 appears in `.keep-cold/signals.json` and `warp-N` restarts within
      ~1 minute.
