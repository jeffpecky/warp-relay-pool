// The diagnostic endpoint must stay reachable when every lane is cooled or
// sick. If `/` 503s in that state, every probe fails, every failed probe marks
// its lane sick, and a sick lane is only cleared by a probe that needs a
// usable lane — a fleet-wide deadlock that only a process restart escapes.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import path from "node:path";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function boot(port, env) {
  return spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: { ...process.env, RELAY_COUNT: "1", BASE_PORT: String(port), RELAY_LANE_PROBE_S: "0", ...env },
    stdio: ["ignore", "pipe", "pipe"],
  });
}

async function waitListening(child) {
  for await (const chunk of child.stdout) {
    if (String(chunk).includes("listening on")) return;
  }
}

test("the health path answers even when the pool is exhausted", { timeout: 60_000 }, async () => {
  const port = 33500 + Math.floor(Math.random() * 400);
  // Dead SOCKS hop: every dispatch fails, so the lane gets cooled.
  const child = boot(port, { WARP_HOST_1: "127.0.0.1", WARP_PORT_1: "1", RELAY_LANE_COOLDOWN_S: "600" });
  try {
    await waitListening(child);
    // Warm the failure so the lane is cooled, then check the health path.
    try {
      const warm = await fetch(`http://127.0.0.1:${port}/v1/models`, {
        headers: { "x-relay-target": "https://opencode.ai", "x-relay-path": "/v1/models" },
      });
      await warm.arrayBuffer().catch(() => {});
    } catch {
      // Expected: the dead hop can reset the connection outright.
    }
    const health = await fetch(`http://127.0.0.1:${port}/`);
    assert.notEqual(health.status, 503, "the health path must never refuse when the pool is empty");
    await health.arrayBuffer().catch(() => {});
  } finally {
    child.kill();
    await Promise.race([once(child, "exit"), new Promise((r) => setTimeout(r, 5_000))]);
  }
});

test("a non-positive RELAY_LANE_STICKY_MAX stops the process instead of silently capping", { timeout: 60_000 }, async () => {
  const port = 33900 + Math.floor(Math.random() * 90);
  const child = boot(port, { RELAY_LANE_STICKY_MAX: "-1" });
  let out = "";
  child.stdout.on("data", (c) => (out += c));
  child.stderr.on("data", (c) => (out += c));
  await Promise.race([once(child, "exit"), new Promise((r) => setTimeout(r, 15_000))]);
  child.kill();
  assert.match(out, /stickyMax/, `expected a stickyMax failure, got: ${out}`);
  assert.doesNotMatch(out, /listening on/, "the relay must not serve with a bad stickyMax");
});
