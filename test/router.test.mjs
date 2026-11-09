// test/router.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { createLanes, laneKey, readReplayableBody, resolvePinnedLane, retryable } from "../router.mjs";

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

test("a pool parked only by 429 cooldowns still serves", () => {
  // A 429 is a throttle on one egress IP, not a dead lane. Parking the last
  // usable lane would turn a throttle into a fleet-wide 503 for the whole
  // cooldown window, so a 429-only exhaustion falls back to a lane.
  const p = pool(2);
  for (const l of p.lanes) p.cool(l, 60_000, "429", "signal");
  assert.notEqual(p.pick("x"), null);
});

test("a probe-parked lane is not resurrected by the 429 fallback", () => {
  const p = pool(1);
  p.cool(p.lanes[0], 60_000, "429", "signal");
  p.cool(p.lanes[0], 60_000, "probe");
  assert.equal(p.pick("x"), null, "a broken egress must stay excluded");
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

test("laneKey orders the session headers opencode > affinity > id", () => {
  assert.equal(
    laneKey({ "x-session-id": "s3", "x-session-affinity": "s2", "x-opencode-session": "s1" }, "1.2.3.4"),
    "s1"
  );
  assert.equal(laneKey({ "x-session-id": "s3", "x-session-affinity": "s2" }, "1.2.3.4"), "s2");
});

test("laneKey falls back to the peer address, then a constant", () => {
  assert.equal(laneKey({ authorization: "Bearer public" }, "1.2.3.4"), "1.2.3.4");
  assert.equal(laneKey({}, ""), "unknown");
});

test("sticky LRU evicts beyond stickyMax, and the evicted key re-picks", () => {
  const p = pool(2, { stickyMax: 2 });
  const a = p.pick("a");
  assert.equal(a.n, 1);
  p.pick("b");
  p.pick("c");
  assert.equal(p.stickySize(), 2);
  assert.ok(p.pick("a"), "an evicted key still gets a lane");
});

test("sticky LRU refreshes on hit, so the least-recently-used key is evicted", () => {
  const p = pool(3, { stickyMax: 2 });
  assert.equal(p.pick("a").n, 1);
  assert.equal(p.pick("b").n, 2);
  assert.equal(p.pick("a").n, 1, "hit keeps its lane");
  assert.equal(p.pick("c").n, 3);
  assert.equal(p.stickySize(), 2);
  // "b" was least recently used, so it was evicted; a fresh pick continues
  // round-robin to lane 1. A retained "b" would answer with lane 2.
  assert.equal(p.pick("b").n, 1);
});

test("a failing probe marks the lane sick and parks it", async () => {
  const p = pool(1, { probe: async () => false, probeIntervalMs: 5 });
  p.start();
  try {
    await new Promise((r) => setTimeout(r, 40));
    assert.equal(p.lanes[0].sick, true);
    assert.ok(p.lanes[0].probeOutUntil > Date.now());
    assert.equal(p.pick("k"), null);
  } finally {
    p.stop();
  }
});

test("a passing probe clears only its own lane", async () => {
  // The probe answers without an address, which is the only case where a bare
  // 200 can lift a 429 cooldown; an address that MATCHES the cooled one must
  // not (see the tests below).
  const p = pool(2, { probe: async (l) => l.n === 1, probeIntervalMs: 5 });
  p.lanes[0].sick = true;
  p.lanes[0].probeOutUntil = Date.now() + 60_000;
  p.lanes[0].cool429Until = Date.now() + 60_000;
  p.start();
  try {
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(p.lanes[0].sick, false, "the pass re-admits the lane");
    assert.equal(p.lanes[0].probeOutUntil, 0, "the pass clears the probe cooldown");
    assert.equal(p.lanes[0].cool429Until, 0, "a probe that cannot see an address trusts its 200");
    assert.equal(p.lanes[1].sick, true, "the failing lane is untouched by lane 1's pass");
  } finally {
    p.stop();
  }
});

test("a probe from a new address lifts the 429 cooldown; the same address does not", async () => {
  let servedFrom = "2001:db8:1::1";
  const p = pool(1, { probe: async () => ({ ok: true, ip: servedFrom }), probeIntervalMs: 5 });
  const lane = p.lanes[0];

  // The 429 was earned from ::1, so a probe still seeing ::1 proves nothing.
  lane.ip = "2001:db8:1::1";
  p.cool(lane, 120_000, "429 reported", "signal");
  p.start();
  try {
    await new Promise((r) => setTimeout(r, 60));
    assert.ok(lane.cool429Until > Date.now(), "same address keeps the cooldown");
    // pick() still serves a parked lane when nothing else is usable (a single
    // lane must not 503), so the cooldown itself is the evidence, not pick().

    // The container was restarted: same lane, different exit IP.
    servedFrom = "2001:db8:2::1";
    await new Promise((r) => setTimeout(r, 60));
    assert.equal(lane.cool429Until, 0, "a different address proves the verdict stale");
    assert.equal(p.pick("k")?.n, 1, "the lane is back in the pool without waiting out the timer");
  } finally {
    p.stop();
  }
});

test("construction rejects a non-positive stickyMax and a missing agentFor", () => {
  assert.throws(() => createLanes({ count: 1, agentFor: (n) => ({ n }), stickyMax: -1 }), /stickyMax/);
  assert.throws(() => createLanes({ count: 1, agentFor: (n) => ({ n }), stickyMax: 0 }), /stickyMax/);
  assert.throws(() => createLanes({ count: 1 }), /agentFor/);
});

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

test("GET and HEAD skip the body entirely", async () => {
  for (const method of ["GET", "HEAD"]) {
    const req = Readable.from([Buffer.from("ignored")]);
    req.method = method;
    const out = await readReplayableBody(req, 1024);
    assert.deepEqual(out, { replayable: true, body: null }, `${method} must not read or buffer`);
  }
});

test("the first chunk already over the cap still streams whole", async () => {
  const out = await readReplayableBody(Readable.from([Buffer.alloc(100, "x")]), 10);
  assert.equal(out.replayable, false);
  const got = [];
  for await (const c of out.body) got.push(c);
  assert.equal(Buffer.concat(got).length, 100);
});

test("destroying the overflow stream destroys the client request", async () => {
  // Without this the request never completes, so keep-alive parks the socket.
  async function* src() {
    for (let i = 0; i < 50; i++) yield Buffer.alloc(40, "y");
  }
  const req = Readable.from(src());
  const out = await readReplayableBody(req, 10);
  assert.equal(out.replayable, false);
  out.body.destroy();
  await new Promise((r) => setImmediate(r));
  assert.equal(req.destroyed, true, "the source req must be torn down with the body");
});

test("a non-finite cap falls back to the documented default, a negative one disables replay", async () => {
  // A NaN cap makes every `size > cap` comparison false — the cap must not vanish.
  const big = [Buffer.alloc(900_000, "a"), Buffer.alloc(900_000, "b")];
  const nan = await readReplayableBody(Readable.from(big), NaN);
  assert.equal(nan.replayable, false, "NaN must not disable the cap");
  nan.body?.destroy?.();
  const neg = await readReplayableBody(Readable.from([Buffer.from("hi")]), -5);
  assert.equal(neg.replayable, false, "a negative cap must not buffer");
  neg.body?.destroy?.();
});

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

// ── Pinned lanes (keep-cold only) ────────────────────────────────────
// Targeting a lane lets a prober attribute a result to the egress that
// produced it. It is honored ONLY for traffic that carries the probe marker
// and, for the userinfo form, valid credentials — otherwise any client could
// pin any egress and defeat the pool's spreading.

test("x-relay-lane pins only with the probe marker", () => {
  const lanes = pool(3).lanes;
  assert.equal(resolvePinnedLane({ headers: { "x-relay-lane": "2", "x-keep-cold-probe": "1" }, lanes })?.n, 2);
  assert.equal(resolvePinnedLane({ headers: { "x-relay-lane": "2" }, lanes }), null);
});

test("out-of-range and malformed lane ids are ignored", () => {
  const lanes = pool(3).lanes;
  for (const v of ["0", "4", "999", "abc", "-1", "", "2.5", " 2", "1e1"]) {
    assert.equal(
      resolvePinnedLane({ headers: { "x-relay-lane": v, "x-keep-cold-probe": "1" }, lanes }),
      null,
      `lane id ${JSON.stringify(v)} must not pin`,
    );
  }
});

test("lane-N userinfo pins only with configured credentials", () => {
  const lanes = pool(3).lanes;
  const base = { headers: { "x-keep-cold-probe": "1" }, proxyUser: "lane-2", lanes };
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "u:p", proxyPass: "p" })?.n, 2);
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "" }), null, "no RELAY_PROXY_AUTH means no pin");
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "u:p", proxyPass: "wrong" }), null);
  assert.equal(
    resolvePinnedLane({ ...base, proxyAuth: "u:p" }),
    null,
    "a missing password is not a matching password",
  );
  assert.equal(resolvePinnedLane({ ...base, proxyAuth: "u:p", proxyUser: "lane-9", proxyPass: "p" }), null);
});

test("a lane id is matched in range, never as a prefix or a fraction", () => {
  const lanes = pool(12).lanes;
  const pin = (v) => resolvePinnedLane({ headers: { "x-relay-lane": v, "x-keep-cold-probe": "1" }, lanes })?.n ?? null;
  assert.equal(pin("12"), 12);
  assert.equal(pin("13"), null);
  assert.equal(pin("01"), null);
});
