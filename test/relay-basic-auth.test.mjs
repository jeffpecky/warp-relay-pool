// RELAY_PROXY_AUTH tests: direct public-IP mode's Basic proxy gate.
//
// Spawns one server.mjs child with RELAY_PROXY_AUTH set and no usable
// upstream. A request that passes BOTH the allowlist and the credential
// gate therefore answers 502 (upstream unreachable) — any 401/407 means a
// gate rejected the call. Covers: missing cred (407 + challenge header),
// wrong cred (407), correct cred (502 -> dispatch happened), header-only
// token exemption NOT present in this mode (exact origins also gated), and
// CONNECT 407 on the raw socket.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import net from "node:net";
import { spawn } from "node:child_process";
import path from "node:path";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");
// Disjoint from relay-guard (23000-24099), keep-cold (25000-25999),
// universal-proxy (27000-275xx).
const BASE = 28400 + Math.floor(Math.random() * 100);
const RELAY_PORT = BASE;
const CRED = "runner:opardmpl";
const TOKEN = Buffer.from(CRED).toString("base64");

let child;

before(async () => {
  child = spawn(process.execPath, ["server.mjs"], {
    cwd: REPO_ROOT,
    env: {
      ...process.env,
      RELAY_COUNT: "1",
      BASE_PORT: String(BASE),
      // Marker env so this child is recognizably the public-IP variant; the
      // credential itself is what server.mjs reads.
      RELAY_PROXY_AUTH: CRED,
      RELAY_ALLOWED_TARGETS: "https://opencode.ai",
      // No probes: the lane points at a closed port, so a probe would park it
      // before the tests run and every dispatch would 503 instead of 502.
      RELAY_LANE_PROBE_S: "0",
      WARP_HOST_1: "127.0.0.1",
      WARP_PORT_1: "1", // closed port: dispatch gives 502
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  child.stderr.on("data", () => {});
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("relay did not start")), 15_000);
    child.stdout.on("data", (d) => {
      if (String(d).includes("listening on")) {
        clearTimeout(timer);
        resolve();
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`relay exited early: ${code}`));
    });
  });
});

after(() => {
  try {
    child.kill();
  } catch {
    // Already gone.
  }
});

function get(headers) {
  // node:http, not global fetch: on this harness the 407 + Proxy-Authenticate
  // challenge provokes an undici internal network error before the response
  // surfaces; a plain request delivers the bytes reliably.
  return new Promise((resolve, reject) => {
    const req = http.request(
      { host: "127.0.0.1", port: RELAY_PORT, path: "/v1/models", method: "GET",
        headers: { "x-relay-target": "https://opencode.ai", "x-relay-path": "/v1/models", ...headers } },
      (res) => {
        let body = "";
        res.on("data", (d) => (body += d));
        res.on("end", () => resolve({ status: res.statusCode, headers: res.headers, body }));
      }
    );
    req.on("error", reject);
    req.end();
  });
}

test("missing proxy credentials → 407 with challenge", async () => {
  const res = await get({});
  assert.equal(res.status, 407);
  assert.match(res.headers["proxy-authenticate"] || "", /^Basic realm=/);
});

test("wrong proxy credentials → 407", async () => {
  const res = await get({ "proxy-authorization": `Basic ${Buffer.from("runner:nope").toString("base64")}` });
  assert.equal(res.status, 407);
});

test("valid x-relay-auth token alone is NOT enough in this mode (exact origins gated too)", async () => {
  const res = await get({ "x-relay-auth": CRED });
  assert.equal(res.status, 407);
});

test("correct Basic cred dispatches (502 = auth passed, upstream unreachable)", async () => {
  const res = await get({ "proxy-authorization": `Basic ${TOKEN}` });
  assert.equal(res.status, 502);
});

test("CONNECT without credentials → 407 on the tunnel socket", async () => {
  const out = await new Promise((resolve) => {
    const socket = net.connect(RELAY_PORT, "127.0.0.1");
    let buf = "";
    socket.on("connect", () =>
      socket.write("CONNECT opencode.ai:443 HTTP/1.1\r\nHost: opencode.ai:443\r\n\r\n")
    );
    socket.on("data", (c) => {
      buf += c.toString("latin1");
      if (/^HTTP\/1\.1 \d+/.test(buf)) {
        socket.destroy();
        resolve(buf);
      }
    });
    socket.on("error", () => resolve(buf));
  });
  assert.match(out, /HTTP\/1\.1 407/);
  assert.match(out, /Proxy-Authenticate: Basic realm=/);
});
