// A listener with no usable lane must refuse at the edge, never hang.
import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import path from "node:path";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

test("a pool with no usable lane answers 503 with Retry-After", { timeout: 60_000 }, async () => {
  // One lane pointed at a closed port: the connect failure cools it, so the
  // second request must be refused at the edge instead of hanging.
  const port = 33000 + Math.floor(Math.random() * 500);
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      RELAY_COUNT: "1",
      BASE_PORT: String(port),
      WARP_HOST_1: "127.0.0.1",
      WARP_PORT_1: "1",
      RELAY_LANE_PROBE_S: "0",
      RELAY_LANE_COOLDOWN_S: "600",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  try {
    // Wait for the startup line, not the first byte: the harness other suites
    // use matches on "listening on".
    for await (const chunk of child.stdout) {
      if (String(chunk).includes("listening on")) break;
    }
    const send = () =>
      fetch(`http://127.0.0.1:${port}/v1/models`, {
        headers: { "x-relay-target": "https://opencode.ai", "x-relay-path": "/v1/models" },
      });
    // The first call may 502 on the dead lane (or throw); either way it cools
    // it, which is what the second call observes.
    try {
      const warm = await send();
      await warm.arrayBuffer().catch(() => {});
    } catch {
      // Expected: a dead SOCKS hop can reset the connection outright.
    }
    const res = await send();
    assert.equal(res.status, 503);
    assert.equal(res.headers.get("retry-after"), "30");
    await res.arrayBuffer().catch(() => {});
  } finally {
    child.kill();
    await Promise.race([once(child, "exit"), new Promise((r) => setTimeout(r, 5_000))]);
  }
});
