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
