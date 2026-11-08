// test/router.test.mjs
import { test } from "node:test";
import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { createLanes, laneKey, readReplayableBody, retryable } from "../router.mjs";

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
  const p = pool(2, { probe: async (l) => l.n === 1, probeIntervalMs: 5 });
  // Start from a parked lane so "cleared" can only come from the probe passing.
  p.lanes[0].sick = true;
  p.lanes[0].probeOutUntil = Date.now() + 60_000;
  p.lanes[0].cool429Until = Date.now() + 60_000;
  p.start();
  try {
    await new Promise((r) => setTimeout(r, 80));
    assert.equal(p.lanes[0].sick, false, "the pass re-admits the lane");
    assert.equal(p.lanes[0].probeOutUntil, 0, "the pass clears the probe cooldown");
    assert.ok(p.lanes[0].cool429Until > Date.now() + 50_000, "the 429 lane survives a passing probe");
    assert.equal(p.lanes[1].sick, true, "the failing lane is untouched by lane 1's pass");
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
