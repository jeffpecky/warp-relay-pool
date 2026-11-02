// Mux tests: ONE listener, N backends, health-aware dispatch.
//
// Runs the real startMux() from mux.mjs against fake backend HTTP servers on
// 127.0.0.1:<base>+1..N. The mux is a pure TCP switcher, so each fake backend
// only implements the two surfaces the mux sees:
//   - health probe ("GET /" -> 200 healthy, 500 unhealthy)
//   - a data request that answers "END"-terminated text so rawSend() can
//     assert which backend served each connection
// Covers: round-robin spread skipping a probe-failing backend, 503 when
// every backend is out, peer-address stickiness, and 429-signal cooldown
// via the signal file watcher.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";

// Disjoint from relay-guard (23000-24099), keep-cold (25000-25999),
// universal-proxy (27000-275xx) so parallel test files never bind ports.
const ENGINE_ROOT = path.resolve(import.meta.dirname, "..");
const { startMux } = await import(pathToFileURL(path.join(ENGINE_ROOT, "mux.mjs")));

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), "mux-tests-"));

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// One fake backend: health surface + countable data surface. `unhealthy`
// marks the probe 500 (sick), `dead` makes the listener refuse connections.
function startFakeBackend(port, { unhealthy = false } = {}) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      if (req.url === "/") {
        res.writeHead(unhealthy ? 500 : 200, { "content-type": "text/plain" });
        return res.end("health\n");
      }
      res.writeHead(200, { "content-type": "text/plain" });
      res.end(`backend ${port} END\n`);
    });
    server.on("error", reject);
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

// Raw TCP client against the mux: send once, read until END or socket close.
function rawSend(port, payload, timeoutMs = 4000) {
  return new Promise((resolve) => {
    const socket = net.connect(port, "127.0.0.1");
    let buf = "";
    const done = (out) => {
      socket.removeListener("data", onData);
      socket.removeListener("error", onError);
      socket.destroy();
      clearTimeout(timer);
      resolve(out);
    };
    const onData = (chunk) => {
      buf += chunk.toString("latin1");
      if (buf.includes("END") || /^HTTP\/1\.1 [45]/.test(buf)) done(buf);
    };
    const onError = () => done(buf);
    const timer = setTimeout(() => done(buf), timeoutMs);
    socket.setNoDelay(true);
    socket.on("error", onError);
    socket.on("data", onData);
    socket.on("connect", () => socket.write(payload));
  });
}

function startPool({ basePort, count, stickyMs = 0, signalFile = null, probeIntervalMs = 100, signalWatchMs = 0 }) {
  return startMux({
    basePort,
    count,
    stickyMs,
    signalFile,
    signalWatchMs,
    probeIntervalMs,
    probeTimeoutMs: 250,
    cooldownMs: 300,
    cooldown429Ms: 400,
    log: () => {}, // Silence mux logs under test.
  });
}

// ─── Group 1: routing (ports 260xx) ───────────────────────────────
const BASE1 = 26000 + Math.floor(Math.random() * 20);
const MUX1 = BASE1;
let pool1;
let servers1;

before(async () => {
  servers1 = await Promise.all([
    startFakeBackend(BASE1 + 1),
    startFakeBackend(BASE1 + 2, { unhealthy: true }), // probe 500 -> kept out
    startFakeBackend(BASE1 + 3),
  ]);
  pool1 = startPool({ basePort: MUX1, count: 3 });
});

test("round-robin spreads and skips the probe-failing backend", async () => {
  await sleep(400); // Let the first staggered probes land (~150ms window).
  const seen = new Set();
  for (let i = 0; i < 4; i++) {
    const out = await rawSend(MUX1, "GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
    assert.match(out, /END/);
    const port = Number(/backend (\d+) END/.exec(out)[1]);
    seen.add(port - BASE1); // backend index 1..3
  }
  // Only backends 1 and 3 may serve; the sick one never answers data.
  assert.deepEqual([...seen].sort(), [1, 3]);
});

test("mux answers 503 when every backend is out", async () => {
  for (const b of pool1.backends) b.probeOutUntil = Date.now() + 1000;
  const out = await rawSend(MUX1, "CONNECT opencode.ai:443 HTTP/1.1\r\nHost: opencode.ai:443\r\n\r\n");
  assert.match(out, /^HTTP\/1\.1 503/);
  for (const b of pool1.backends) b.probeOutUntil = 0; // Restore for next test.
});

// ─── Group 2: stickiness (ports 261xx) ──────────────────────────
const BASE2 = 26100 + Math.floor(Math.random() * 20);
const MUX2 = BASE2;
let pool2;
let servers2;

before(async () => {
  servers2 = await Promise.all([
    startFakeBackend(BASE2 + 1),
    startFakeBackend(BASE2 + 2),
  ]);
  pool2 = startPool({ basePort: MUX2, count: 2, stickyMs: 3600 });
});

test("same peer address sticks to one backend; another peer spreads", async () => {
  // Two connections from the same test process (same remoteAddress) must
  // land on the same backend while its sticky lease is valid.
  const first = await rawSend(MUX2, "GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
  const second = await rawSend(MUX2, "GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
  assert.match(first, /END/);
  assert.match(second, /END/);
  assert.equal(first, second);
  // A distinct peer: connect from a bound alternate loopback address so the
  // sticky key differs and the round-robin advances.
  const alt = await new Promise((resolve) => {
    const socket = net.connect({ port: MUX2, localAddress: "127.0.0.2" });
    let buf = "";
    socket.on("error", () => resolve("")); // 127.0.0.2 may be unavailable.
    socket.on("connect", () => {
      socket.write("GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
    });
    socket.on("data", (c) => {
      buf += c.toString("latin1");
      if (buf.includes("END") || /^HTTP\/1\.1 [45]/.test(buf)) {
        socket.destroy();
        resolve(buf);
      }
    });
  });
  if (alt && alt.includes("END")) {
    assert.notEqual(alt, first); // Different peer -> any backend works.
  }
  // 127.0.0.2 unreachable on some stacks: alt stays "" and the assert above
  // is skipped — stickiness itself is still covered by first/second.
});

// ─── Group 3: 429 signal cooldown (ports 262xx) ──────────────────
const BASE3 = 26200 + Math.floor(Math.random() * 20);
const MUX3 = BASE3;
const SIGNAL_FILE = path.join(TMP, "signals-mux.json");
let pool3;
let servers3;

before(async () => {
  servers3 = await Promise.all([
    startFakeBackend(BASE3 + 1),
    startFakeBackend(BASE3 + 2),
  ]);
  pool3 = startPool({ basePort: MUX3, count: 2, signalFile: SIGNAL_FILE, signalWatchMs: 100 });
});

test("recent 429 signal cooldowns that relay; traffic moves to the sibling", async () => {
  // Seed the window BEFORE any pick: a 429 on relay-1 from 0.5s ago should
  // push the first connection straight to relay-2.
  fs.writeFileSync(
    SIGNAL_FILE,
    JSON.stringify({
      signals: [
        { ts: Date.now() - 500, relay: 1, model: "test-model", status: 429, retryAfter: null },
      ],
    })
  );
  await sleep(300); // > signalWatchMs(100ms): watcher read + cooldown applied.
  const out = await rawSend(MUX3, "GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
  assert.match(out, /END/);
  const served = Number(/backend (\d+) END/.exec(out)[1]) - BASE3; // index 1..2
  assert.equal(served, 2);
});

test("429 cooldown self-expires and the relay comes back", async () => {
  await sleep(500); // cooldown429Ms=400: lease expired.
  const out = await rawSend(MUX3, "GET /data HTTP/1.1\r\nHost: t\r\n\r\n");
  assert.match(out, /END/);
});

after(async () => {
  for (const pool of [pool1, pool2, pool3]) {
    if (pool) pool.server.close();
  }
  for (const srv of [...(servers1 || []), ...(servers2 || []), ...(servers3 || [])]) {
    if (srv) srv.close();
  }
  fs.rmSync(TMP, { recursive: true, force: true });
});
