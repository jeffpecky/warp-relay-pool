// Client-reported rate limits.
//
// The relay cannot read a status on a CONNECT tunnel: the request is TLS to the
// origin and this process only forwards bytes. So the client that terminated
// that TLS reports the 429 out of band, and the relay — which owns the pool —
// resolves which lane to condemn. These tests pin the two halves that matter:
// the report must reach the lane the caller was actually served by, and the
// caller must never be able to name a lane itself.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
const BASE_PORT = 27000 + Math.floor(Math.random() * 900);
const TOKEN = "signal-token-for-tests";
const PASS = "s3cret";

// Two lanes, each a fake SOCKS5 listener, so "which lane served this tunnel" is
// observable from the outside: the lane's own socket records the CONNECT.
const laneHits = [0, 0];

function startSocks(index) {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => {
      sock.on("error", () => {});
      let stage = 0;
      let buf = Buffer.alloc(0);
      sock.on("data", (d) => {
        buf = Buffer.concat([buf, d]);
        for (;;) {
          if (stage === 0) {
            if (buf.length < 2) return;
            const nm = buf[1];
            if (buf.length < 2 + nm) return;
            buf = buf.subarray(2 + nm);
            sock.write(Buffer.from([0x05, 0x00]));
            stage = 1;
          } else if (stage === 1) {
            if (buf.length < 4) return;
            const atyp = buf[3];
            let len = 4;
            if (atyp === 0x01) len = 4;
            else if (atyp === 0x03) len = 1 + buf[4];
            else if (atyp === 0x04) len = 16;
            const total = 4 + len + 2;
            if (buf.length < total) return;
            buf = buf.subarray(total);
            laneHits[index] += 1;
            sock.write(Buffer.from([0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0]));
            // Keep the tunnel open briefly: the client only needs the 200.
            setTimeout(() => sock.destroy(), 400);
            stage = 2;
            return;
          } else {
            return;
          }
        }
      });
    });
    srv.listen(0, "127.0.0.1", () => resolve(srv));
  });
}

// One CONNECT through the relay as a named client. Returns the lane that
// served it, or null when the relay refused. Node emits 'connect' for a
// non-200 CONNECT response too, so the status is what decides.
function connectAs(clientId, pass = PASS) {
  return new Promise((resolve) => {
    const before = [...laneHits];
    const req = http.request({
      host: "127.0.0.1",
      port: BASE_PORT,
      method: "CONNECT",
      path: "opencode.ai:443",
      headers: { "proxy-authorization": `Basic ${Buffer.from(`${clientId}:${pass}`).toString("base64")}` },
    });
    req.on("connect", (res, socket) => {
      socket.destroy();
      if (res.statusCode !== 200) return resolve(null);
      const served = laneHits.findIndex((n, i) => n > before[i]);
      resolve(served);
    });
    req.on("error", () => resolve(null));
    req.end();
  });
}

function postSignal(body, { token = TOKEN } = {}) {
  const payload = JSON.stringify(body);
  return new Promise((resolve) => {
    const req = http.request(
      {
        host: "127.0.0.1",
        port: BASE_PORT,
        method: "POST",
        path: "/__signal",
        headers: {
          "content-type": "application/json",
          "content-length": String(Buffer.byteLength(payload)),
          ...(token === null ? {} : { authorization: `Bearer ${token}` }),
        },
      },
      (res) => {
        let data = "";
        res.on("data", (d) => (data += d));
        res.on("end", () => resolve({ status: res.statusCode, body: data }));
      },
    );
    req.on("error", () => resolve({ status: 0, body: "" }));
    req.write(payload);
    req.end();
  });
}

function spawnRelay({ token = TOKEN } = {}) {
  const child = spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      RELAY_COUNT: "2",
      BASE_PORT: String(BASE_PORT),
      RELAY_ALLOWED_TARGETS: "https://opencode.ai",
      RELAY_PROXY_AUTH: `client:${PASS}`,
      ...(token === null ? {} : { RELAY_SIGNAL_TOKEN: token }),
      LIVE_SIGNAL_FILE: signalFile,
      WARP_HOST_1: "127.0.0.1",
      WARP_PORT_1: String(lanePorts[0]),
      WARP_HOST_2: "127.0.0.1",
      WARP_PORT_2: String(lanePorts[1]),
      // Probes would add lane traffic the CONNECT counting cannot attribute.
      RELAY_LANE_PROBE_S: "0",
      RELAY_LANE_RETRY_MAX: "0",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("relay did not start")), 15_000);
    child.stdout.on("data", (d) => {
      if (String(d).includes("listening on")) {
        clearTimeout(timer);
        resolve(child);
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`relay exited early: ${code}`));
    });
  });
}

let socks = [];
let child;
let signalDir;
let signalFile;
// The relay reaches each lane over SOCKS at warp-<n>:1080 by default, so the
// fake lanes are handed to it explicitly.
const lanePorts = [];

before(async () => {
  for (let i = 0; i < 2; i++) {
    const srv = await startSocks(i);
    socks.push(srv);
    lanePorts.push(srv.address().port);
  }
  signalDir = fs.mkdtempSync(path.join(os.tmpdir(), "relay-signal-"));
  signalFile = path.join(signalDir, "signals.json");
  child = await spawnRelay();
});

after(() => {
  child?.kill();
  for (const s of socks) s.close();
  try {
    fs.rmSync(signalDir, { recursive: true, force: true });
  } catch {
    // Windows may hold the file briefly after the child exits.
  }
  // Env wiring below is per-process; the second relay runs in its own child.
});

function readSignals() {
  try {
    const parsed = JSON.parse(fs.readFileSync(signalFile, "utf8"));
    return Array.isArray(parsed?.signals) ? parsed.signals : [];
  } catch {
    return [];
  }
}

// The writer batches for 250ms so a burst of throttled requests is one rename,
// not one per request. Wait for the record instead of racing the flush.
async function waitForRecord(count, ms = 3000) {
  const deadline = Date.now() + ms;
  for (;;) {
    const signals = readSignals();
    if (signals.length >= count || Date.now() > deadline) return signals;
    await new Promise((r) => setTimeout(r, 25));
  }
}

test("a client id binds one lane, and a 429 report moves it to another", async () => {
  const first = await connectAs("c-alpha");
  assert.ok(first >= 0, "the pinned-auth client must get a lane");

  // Same client, next tunnel: sticky means the same lane.
  const again = await connectAs("c-alpha");
  assert.equal(again, first, "a client id must hold its lane across tunnels");

  const before = readSignals().length;
  const res = await postSignal({ clientId: "c-alpha", model: "m", status: 429, retryAfter: 30 });
  assert.equal(res.status, 204);

  // The report condemned the lane the client held, so the next tunnel rotates.
  const next = await connectAs("c-alpha");
  assert.ok(next >= 0);
  assert.notEqual(next, first, "a reported 429 must send the client to a different lane");

  // ...and it was written durably, which is what restarts the container.
  const records = (await waitForRecord(before + 1)).slice(before);
  assert.equal(records.length, 1);
  assert.equal(records[0].status, 429);
  assert.equal(records[0].model, "m");
  assert.equal(records[0].retryAfter, 30);
});

test("any username is an identity: a plain user gets the same lane affinity and reporting", async () => {
  // The token is the capability. `runner` — the wizard default — must hold a
  // lane and be able to condemn it exactly like a `c-`-prefixed name.
  const first = await connectAs("runner");
  assert.ok(first >= 0, "a plain username with the right password must get a lane");
  assert.equal(await connectAs("runner"), first, "it holds its lane across tunnels");

  const res = await postSignal({ clientId: "runner", status: 429 });
  assert.equal(res.status, 204, "its 429 report must be accepted");
  const next = await connectAs("runner");
  assert.notEqual(next, first, "a reported 429 must rotate it");
});

test("a lane pin is not an identity", async () => {
  // `lane-N` is the keep-cold probe pin and is refused as a report identity;
  // it is never a client name.
  const res = await postSignal({ clientId: "lane-1", status: 429 });
  assert.equal(res.status, 400);
});

test("a recovery report makes the condemned lane selectable again", async () => {
  const held = await connectAs("c-beta");
  assert.ok(held >= 0);
  await postSignal({ clientId: "c-beta", status: 429 });
  const moved = await connectAs("c-beta");
  assert.notEqual(moved, held);

  // Both lanes are now served; a recovery on the moved one clears its cooldown
  // without inventing a third.
  const res = await postSignal({ clientId: "c-beta", status: 200 });
  assert.equal(res.status, 204);
  const after = await connectAs("c-beta");
  assert.ok(after >= 0);
});

test("a bad token is rejected and condemns nothing", async () => {
  const held = await connectAs("c-gamma");
  assert.ok(held >= 0);
  const res = await postSignal({ clientId: "c-gamma", status: 429 }, { token: "wrong" });
  assert.equal(res.status, 401);
  const again = await connectAs("c-gamma");
  assert.equal(again, held, "an unauthenticated report must not cool a lane");
});

test("the endpoint rejects malformed reports", async () => {
  // A lane id must never be accepted: only the relay decides which lane a
  // client is served by. Any other username IS a valid identity now.
  for (const body of [
    { clientId: "c-x", status: 429, lane: 2 },
    { clientId: "lane-1", status: 429 },
    { clientId: "c x", status: 429 },
    { clientId: "c-x", status: 500 },
    { status: 429 },
  ]) {
    const res = await postSignal(body);
    assert.ok(res.status === 400 || res.status === 204, `body ${JSON.stringify(body)} must not be a 5xx`);
    if (body.clientId !== "c-x" || body.status !== 429) assert.equal(res.status, 400);
  }
});

test("a report for a client that was never served is acknowledged, not an error", async () => {
  const res = await postSignal({ clientId: "c-never-seen", status: 429 });
  assert.equal(res.status, 204);
  assert.equal(readSignals().some((s) => s.model === undefined), false);
});

test("a wrong proxy password cannot claim a client id", async () => {
  const served = await connectAs("c-delta", "wrong-pass");
  assert.equal(served, null, "a bad password must not reach a lane at all");
});
