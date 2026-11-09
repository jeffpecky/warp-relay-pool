// Behaviour tests for keep-cold's demand-driven probing and rotation budget.
//
// No docker and no real upstream: fake relay listeners answer the probe wire
// shape directly (recording the model they were asked for), and a fake docker
// binary records every invocation, so restart decisions are asserted from the
// docker log instead of from a running pool. Each spawn gets its own state,
// signal and tier-2 files under a per-scenario temp dir; time is moved forward
// with KEEP_COLD_CLOCK_SKEW_S, so a multi-hour rotation budget is exercised in
// milliseconds of wall clock.
import { test, after } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { spawn } from "node:child_process";

const REPO_ROOT = path.resolve(import.meta.dirname, "..");

// Node --test runs test files in parallel: relay-guard.test.mjs owns
// 23000-23999, this file owns 25000-26999 and never reuses a block. The start
// is fixed rather than random: the band is this file's alone, and a random
// start only ever bought a run that occasionally reserved too little of it.
const PORT_MIN = 25000;
const PORT_MAX = 26999;
const PORT_BLOCK = 32;
let nextBlock = PORT_MIN;

const servers = [];
const tmpDirs = [];

function takeBlock() {
 const start = nextBlock;
 nextBlock += PORT_BLOCK;
 assert.ok(start + PORT_BLOCK - 1 <= PORT_MAX, `port band exhausted at ${start}`);
 return start;
}

function newScenario({ count, throttle = {} }) {
 const dir = fs.mkdtempSync(path.join(os.tmpdir(), "wrp-keep-cold-"));
 tmpDirs.push(dir);
 const fakeDocker = path.join(dir, "fake-docker.mjs");
 fs.writeFileSync(
  fakeDocker,
  `import fs from "node:fs";
const args = process.argv.slice(2);
fs.appendFileSync(process.env.FAKE_DOCKER_LOG, args.join(" ") + "\\n");

const restartStateFile = (process.env.FAKE_DOCKER_LOG || "") + ".restarts.json";
function getRestarts() {
  try { return JSON.parse(fs.readFileSync(restartStateFile, "utf8")); } catch { return {}; }
}
function recordRestart(c) {
  const s = getRestarts();
  s[c] = (s[c] || 0) + 1;
  fs.writeFileSync(restartStateFile, JSON.stringify(s));
}

if (args[0] === "restart") {
  recordRestart(args[1]);
} else if (args[0] === "exec") {
  const container = args[1];
  const subcmd = args[2];
  if (subcmd === "test" && args[3] === "-f" && args[4] === "/etc/wireguard/wg0.conf") {
    const missing = (process.env.FAKE_DOCKER_MISSING_WG0 || "").split(",").filter(Boolean);
    if (missing.includes(container)) {
      process.exit(1);
    }
    process.exit(0);
  } else if (subcmd === "curl") {
    const restarts = getRestarts()[container] || 0;
    const sticky = Number(process.env.FAKE_DOCKER_STICKY_RESTARTS || 0);
    const prefixSticky = Number(process.env.FAKE_DOCKER_PREFIX_STICKY || 0);
    const effective = restarts > sticky ? restarts : 0;
    const idx = container.replace(/^.*?-/, "");
    const stuckBlock = prefixSticky > 0 && restarts <= prefixSticky;
    let ip;
    if (effective === 0) ip = \`2a09:1:0::\${idx}\`;
    else if (stuckBlock) ip = \`2a09:1:0::\${effective}\${idx}\`;
    else ip = \`2a09:2:\${effective}:0::\${idx}\`;
    process.stdout.write(\`ip=\${ip}\\nwarp=on\\n\`);
    process.exit(0);
  }
}
`,
 );
 const throttles = new Map();
 for (let i = 1; i <= count; i++) throttles.set(i, new Map());
 for (const [relay, models] of Object.entries(throttle)) {
  for (const [model, spec] of Object.entries(models)) throttles.get(Number(relay)).set(model, spec);
 }
 return {
  dir,
  count,
  basePort: null,
  stateFile: path.join(dir, "state.json"),
  signalFile: path.join(dir, "signals.json"),
  tier2File: path.join(dir, "tier2.json"),
  blacklistFile: path.join(dir, "blacklist.json"),
  dockerLog: path.join(dir, "docker.log"),
  fakeDocker,
  throttles,
 };
}

// One listener serves the whole pool now, so the fake relay dispatches on the
// lane header and keeps one log per lane — every existing assertion still reads
// `relay-<i>.log`, it just comes from a single port.
function startRelay(port, logs, throttles, dead) {
 return new Promise((resolve, reject) => {
  const srv = http.createServer((req, res) => {
   let body = "";
   req.on("data", (d) => (body += d));
   req.on("end", () => {
    // A request that names no lane (or an unknown one) still has to land
    // somewhere for the harness to observe it; lane 1 is the catch-all.
    const lane = Number(req.headers["x-relay-lane"]) || 1;
    const logFile = logs.get(lane) ?? logs.get(1);
    let model = "";
    let tools = -1;
    let sys = "";
    try {
      const parsed = JSON.parse(body);
      model = String(parsed.model ?? "");
      tools = Array.isArray(parsed.tools) ? parsed.tools.length : -1;
      if (Array.isArray(parsed.messages)) {
        sys = parsed.messages.filter((m) => m && m.role === "system").map((m) => String(m.content ?? "")).join("\n");
      } else if (typeof parsed.instructions === "string") {
        sys = parsed.instructions;
      }
    } catch {
      model = "";
    }
    fs.appendFileSync(logFile, `${JSON.stringify({ model, ua: req.headers["user-agent"] ?? "", ocClient: req.headers["x-opencode-client"] ?? "", auth: req.headers["authorization"] ?? "", relayTarget: req.headers["x-relay-target"] ?? "", relayPath: req.headers["x-relay-path"] ?? "", keepCold: req.headers["x-keep-cold-probe"] ?? "", session: req.headers["x-opencode-session"] ?? "", tools, sys })}\n`);
    // A "dead" lane fails its connection: the probe must report status 0.
    if (dead.includes(lane)) {
      req.socket.destroy();
      return;
    }
    const rawSpec = throttles.get(lane)?.get(model);
    const spec = typeof rawSpec === "function" ? rawSpec() : rawSpec;
    if (spec) {
     const headers = { "content-type": "application/json" };
     if (spec.retryAfter !== undefined) headers["retry-after"] = String(spec.retryAfter);
     res.writeHead(spec.status, headers);
     res.end('{"error":"rate limited"}');
     return;
    }
    res.writeHead(200, { "content-type": "text/event-stream" });
    res.end("data: {}\n\n");
   });
  });
  srv.once("error", reject);
  srv.listen(port, "127.0.0.1", () => resolve(srv));
 });
}

async function startRelays(sc, { dead = [] } = {}) {
 if (sc.basePort === null) sc.basePort = takeBlock();
 const logs = new Map();
 for (let i = 1; i <= sc.count; i++) logs.set(i, path.join(sc.dir, `relay-${i}.log`));
 const srv = await startRelay(sc.basePort, logs, sc.throttles, dead);
 servers.push(srv);
}

function baseEnv(sc) {
 return {
  RELAY_HOST: "127.0.0.1",
  BASE_PORT: String(sc.basePort ?? 0),
  COUNT: String(sc.count),
  PROBE_MODELS: "model-a",
  PROBE_TIMEOUT_MS: "3000",
  PROBE_CONCURRENCY: "2",
  RELAY_STATE_FILE: sc.stateFile,
  LIVE_SIGNAL_FILE: sc.signalFile,
  TIER2_STATE_FILE: sc.tier2File,
  PREFIX_BLACKLIST_FILE: sc.blacklistFile,
  LIVE_SIGNALS: "0",
  KEEP_COLD_LOG: "",
  DOCKER_BIN: process.execPath,
  DOCKER_PREFIX_ARGS: sc.fakeDocker,
  FAKE_DOCKER_LOG: sc.dockerLog,
  // Harness relays speak relay-terminated HTTP only (no CONNECT); the real
  // wreq transport is exercised on the VPS (plan Task 2 Step 5 manual check).
  PROBE_TRANSPORT: "legacy",
 };
}

function runOnce(sc, env = {}, args = ["--once"]) {
 return new Promise((resolve, reject) => {
  const combinedEnv = { ...process.env, ...baseEnv(sc), ...env };
  for (const [k, v] of Object.entries(env)) {
   if (v === undefined || v === null) delete combinedEnv[k];
  }
  const child = spawn(process.execPath, ["keep-cold.mjs", ...args], {
   cwd: REPO_ROOT,
   env: combinedEnv,
  });
  let stdout = "";
  let stderr = "";
  const timer = setTimeout(() => child.kill(), 60_000);
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  child.once("error", reject);
  child.on("exit", (code) => {
   clearTimeout(timer);
   resolve({ code, stdout, stderr });
  });
 });
}

function requests(sc, index) {
 try {
  return fs.readFileSync(path.join(sc.dir, `relay-${index}.log`), "utf8").split("\n").filter(Boolean).length;
 } catch {
  return 0;
 }
}

function touchedRelays(sc) {
 const out = [];
 for (let i = 1; i <= sc.count; i++) if (requests(sc, i) > 0) out.push(i);
 return out;
}

function dockerLines(sc) {
 try {
  return fs.readFileSync(sc.dockerLog, "utf8").split("\n").filter(Boolean);
 } catch {
  return [];
 }
}

function restartLines(sc) {
 return dockerLines(sc).filter((line) => line.startsWith("restart "));
}

function entry(extra = {}) {
 return {
  lastProbe: 0,
  lastClean: 0,
  lastRotate: 0,
  consecutiveFailures: 0,
  cooldownUntil: 0,
  probeAfter: 0,
  dirty: false,
  pendingVerify: false,
  last429: { model: "", ts: 0 },
  rotations: [],
  ...extra,
 };
}

function writeStateFile(sc, relays, auditCursor = 0) {
 fs.writeFileSync(sc.stateFile, JSON.stringify({ version: 1, auditCursor, relays }));
}

function seedCleanState(sc, auditCursor = 0) {
 const relays = {};
 for (let i = 1; i <= sc.count; i++) relays[String(i)] = entry({ lastProbe: Date.now(), lastClean: Date.now() });
 writeStateFile(sc, relays, auditCursor);
}

function readState(sc) {
 return JSON.parse(fs.readFileSync(sc.stateFile, "utf8"));
}

function stdoutLines(r) {
 return r.stdout.split("\n").filter(Boolean);
}

// Poll a predicate for a long-running child; the detail thunk prints its output
// on failure so a timeout says what the process actually did.
async function waitFor(predicate, ms, detail = () => "") {
 const deadline = Date.now() + ms;
 for (;;) {
  if (predicate()) return;
  if (Date.now() > deadline) throw new Error(`timed out after ${ms}ms\n${detail()}`);
  await new Promise((r) => setTimeout(r, 25));
 }
}

after(() => {
 for (const srv of servers) srv.close();
 for (const dir of tmpDirs) fs.rmSync(dir, { recursive: true, force: true });
});

// ── env validation ──────────────────────────────────────────────────
test("--help prints usage and exits 0", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, {}, ["--help"]);
 assert.equal(r.code, 0);
 assert.match(r.stdout, /--once/);
});

test("blank COUNT fails fast instead of probing nothing", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, { COUNT: "" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /COUNT/);
});

test("blank PROBE_MODELS fails fast instead of a clean no-op", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, { PROBE_MODELS: "" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /PROBE_MODELS/);
});

test("log rotation caps must be ordered", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, { KEEP_COLD_LOG_MAX_BYTES: "100", KEEP_COLD_LOG_KEEP_BYTES: "100" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /KEEP_COLD_LOG_KEEP_BYTES/);
});

test("blank RELAY_STATE_FILE is rejected", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, { RELAY_STATE_FILE: "" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /RELAY_STATE_FILE/);
});

test("backoff base below one second is rejected", async () => {
 const sc = newScenario({ count: 1 });
 const r = await runOnce(sc, { RELAY_BACKOFF_BASE_S: "0" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /RELAY_BACKOFF_BASE_S/);
});

test("audit sample cannot exceed COUNT", async () => {
 const sc = newScenario({ count: 2 });
 const r = await runOnce(sc, { PROBE_AUDIT_SAMPLE: "3" });
 assert.equal(r.code, 2);
 assert.match(r.stderr, /PROBE_AUDIT_SAMPLE/);
});

// ── case 1 + 10: a healthy pool costs the audit sample only ──────────
test("clean 20-relay pool with state probes only the audit sample", async () => {
 const sc = newScenario({ count: 20 });
 await startRelays(sc);
 seedCleanState(sc, 0);
 const r = await runOnce(sc, { PROBE_AUDIT_SAMPLE: "2" });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.deepEqual(touchedRelays(sc), [1, 2]);
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /probing 2 of 20 relay\(s\) x 1 model\(s\)/);
 assert.match(r.stdout, /audit sample: #1 #2/);
 assert.match(r.stdout, /clean 20\/20 relay\(s\)/);
 assert.match(r.stdout, /pool is cold/);
 assert.ok(stdoutLines(r).length <= 10, `stdout had ${stdoutLines(r).length} lines:\n${r.stdout}`);
});

// ── case 2 + 3: one throttle, exactly one restart, then held ─────────
test("baseline sweep leaves a probe-throttled relay alone", async () => {
 const sc = newScenario({ count: 20, throttle: { 7: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 const r = await runOnce(sc);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.match(r.stdout, /\(baseline\)/);
 assert.match(r.stdout, /probing 20 of 20 relay\(s\)/);
 // Probe 429s are no longer a rotation verdict: the relay is reachable, which is
 // all a probe proves. The client's report is what condemns an egress.
 assert.deepEqual(restartLines(sc), [], "a probe 429 must not restart the container");
 assert.match(r.stdout, /#7 model-a 429 on probe — ignored \(client reports rate limits\)/);
 assert.match(r.stdout, /clean 19\/20 relay\(s\)/);
 assert.match(r.stdout, /pool is not cold: 1 relay\(s\) held or throttled/);

 const withinCooldown = await runOnce(sc, { KEEP_COLD_CLOCK_SKEW_S: "120" });
 assert.equal(withinCooldown.code, 1, withinCooldown.stdout);
 assert.equal(restartLines(sc).length, 0);
 assert.equal(requests(sc, 7), 1, "relay 7 must not be probed inside the settle window");

 // Out of the settle window is not out of the cooldown: the relay is still
 // dirty with no rotation owed, so it stays unprobed until probeAfter.
 const later = await runOnce(sc, { KEEP_COLD_CLOCK_SKEW_S: "240" });
 assert.equal(later.code, 1, later.stdout);
 assert.equal(restartLines(sc).length, 0);
 assert.equal(requests(sc, 7), 1, "still inside the 600s cooldown");
 assert.doesNotMatch(later.stdout, /#7 .*429 on probe/, "not re-probed");
});

// ── case 4: a restarted relay is still verifying, not re-restarted ──
test("a relay re-probed with another 429 is not restarted again", async () => {
 const sc = newScenario({ count: 6, throttle: { 3: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 const restartPasses = [];
 for (let pass = 0; pass < 24; pass++) {
  const before = restartLines(sc).length;
  const r = await runOnce(sc, { KEEP_COLD_CLOCK_SKEW_S: String(pass * 300) });
  const added = restartLines(sc).length - before;
  assert.ok(added <= 1, `pass ${pass} restarted ${added} relays:\n${r.stdout}`);
  for (let k = 0; k < added; k++) restartPasses.push(pass);
 }
 // Nothing a probe sees can trigger a restart any more, so the budget is never
 // spent. Rate-limit rotations now arrive as signals, on --signals.
 assert.deepEqual(restartPasses, [], `unexpected restarts at passes ${restartPasses.join(",")}`);
});

// ── case 4b: tier 2 waits for two failed rotations ─────────────────
test("a client-reported 429 rotates tier 1 first, tier 2 only after two failures", async () => {
 const sc = newScenario({ count: 2 });
 await startRelays(sc);
 seedCleanState(sc);
 const report = (relay) => fs.writeFileSync(sc.signalFile, JSON.stringify({
  signals: [{ ts: Date.now(), relay, model: "model-a", status: 429, retryAfter: null }],
 }));

 report(1);
 const first = await runOnce(sc, {}, ["--signals"]);
 assert.equal(first.code, 1, first.stdout + first.stderr);
 assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-1"]);
 assert.doesNotMatch(
  fs.readFileSync(sc.dockerLog, "utf8"),
  /rm -f \/etc\/wireguard\/wg0\.conf/,
  "the first 429 must not spend a Cloudflare registration",
 );

 // Two rotations already failed on this relay: the next 429 is the evidence
 // that a new /64 is needed, not just a new container. The cooldown and the
 // hourly budget are cleared so this pass is about the escalation ladder only.
 const relays = readState(sc).relays;
 relays["1"] = entry({
  consecutiveFailures: 2,
  dirty: true,
  lastRotate: Date.now() - 700_000,
  rotations: [],
 });
 writeStateFile(sc, relays);
 report(1);
 const second = await runOnce(sc, {}, ["--signals"]);
 assert.equal(second.code, 1, second.stdout + second.stderr);
 assert.match(
  fs.readFileSync(sc.dockerLog, "utf8"),
  /rm -f \/etc\/wireguard\/wg0\.conf/,
  "the third 429 escalates to a tier-2 re-key",
 );
});

// ── case 4c: a probe 429 must not poison the client's backoff ──────
test("probe 429s do not inflate the counter the client's 429 is gated on", async () => {
 // The sweep used to bump consecutiveFailures on every probe 429. That counter
 // drives effectiveCooldown() and the tier-2 escalation on the signal path, so
 // ninety sweeps of probe throttling turned the very first client report into a
 // 3600 s lockout plus a Cloudflare registration. Probes are liveness-only; they
 // must leave the counter exactly where they found it.
 const sc = newScenario({ count: 2, throttle: { 1: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 for (let i = 0; i < 3; i++) {
  await runOnce(sc, { KEEP_COLD_CLOCK_SKEW_S: String(i * 3600), MIN_CLEAN_COUNT: "1" }, ["--sweep"]);
 }
 assert.equal(readState(sc).relays["1"].consecutiveFailures, 0, "probe 429s must not count as failures");

 // The first client report must get a tier-1 restart on the base cooldown.
 fs.writeFileSync(sc.signalFile, JSON.stringify({
  signals: [{ ts: Date.now(), relay: 1, model: "model-a", status: 429, retryAfter: null }],
 }));
 const r = await runOnce(sc, { KEEP_COLD_CLOCK_SKEW_S: "10800" }, ["--signals"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-1"]);
 assert.doesNotMatch(
  fs.readFileSync(sc.dockerLog, "utf8"),
  /rm -f \/etc\/wireguard\/wg0\.conf/,
  "the first client 429 must not spend a Cloudflare registration",
 );
 const st = readState(sc).relays["1"];
 assert.equal(st.consecutiveFailures, 1);
 // Cooldowns are stamped on the (skewed) scheduler clock, so measure against
 // the rotation this pass recorded, not wall time.
 assert.ok(
  st.cooldownUntil - st.lastRotate <= 610_000,
  `cooldown ${st.cooldownUntil - st.lastRotate}ms exceeds the base RELAY_COOLDOWN_S`,
 );
});

// ── case 5: a probe 429 is never a rotation trigger ────────────────
test("a probe 429 on 9 of 20 relays rotates nothing and restarts nothing", async () => {
 const throttle = {};
 for (let i = 1; i <= 9; i++) throttle[i] = { "model-a": { status: 429 } };
 const sc = newScenario({ count: 20, throttle });
 await startRelays(sc);
 const r = await runOnce(sc, { GLOBAL_QUOTA_GUARD: "9" });
 assert.equal(r.code, 1, r.stdout + r.stderr);
 // Rate-limit verdicts belong to the client that read the real status, so a
 // probe answer of 429 is recorded and ignored — no restart, on any relay,
 // whether or not the batch looks like upstream quota.
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /#1 model-a 429 on probe — ignored \(client reports rate limits\)/);
 assert.match(r.stdout, /#9 model-a 429 on probe — ignored \(client reports rate limits\)/);
 assert.match(r.stdout, /clean 11\/20 relay\(s\)/);
 assert.doesNotMatch(r.stdout, /pool is cold/);
});

test("probe 429s cannot make the quota guard veto a client's report", async () => {
 // Nine probe 429s are not nine reports. The guard exists to stop a rotation
 // storm on a model that the *clients* say is capped pool-wide; letting probe
 // evidence fill that history means the prober can silently suppress the very
 // report the client made, which is the native 429 checker surviving through
 // the quota guard instead of the prober.
 const throttle = {};
 for (let i = 1; i <= 9; i++) throttle[i] = { "model-a": { status: 429 } };
 const sc = newScenario({ count: 20, throttle });
 await startRelays(sc);
 fs.writeFileSync(sc.signalFile, JSON.stringify({
  signals: [{ ts: Date.now(), relay: 10, model: "model-a", status: 429, retryAfter: null }],
 }));
 const r = await runOnce(sc, { LIVE_SIGNALS: "1", GLOBAL_QUOTA_GUARD: "9" }, []);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.doesNotMatch(r.stdout, /upstream quota/);
 assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-10"]);
});

// ── case 6: settle window ──────────────────────────────────────────
test("a relay inside the post-restart settle window is not probed", async () => {
 const sc = newScenario({ count: 20 });
 await startRelays(sc);
 const lastRotate = Date.now() - 10_000;
 writeStateFile(sc, {
  7: entry({
   dirty: true,
   pendingVerify: true,
   lastRotate,
   probeAfter: lastRotate + 150_000,
   rotations: [lastRotate],
   consecutiveFailures: 1,
  }),
 });
 const r = await runOnce(sc, { EGRESS_SETTLE_S: "150" });
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.equal(requests(sc, 7), 0, r.stdout);
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /clean 19\/20 relay\(s\)/);
});

// ── case 6b: unreachable relay keeps the non-rotation path ─────────
test("an unreachable relay is troubled, never restarted", async () => {
 const sc = newScenario({ count: 2 });
 await startRelays(sc, { dead: [2] });
 const r = await runOnce(sc);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /#2 model-a unexpected HTTP unreachable — /);
 assert.match(r.stdout, /clean 1\/2 relay\(s\)/);
 const st = readState(sc).relays["2"];
 assert.equal(st.dirty, true);
 assert.ok(st.probeAfter > Date.now() + 590_000, `probeAfter ${st.probeAfter}`);
 // An unreachable lane is the prober's problem, not evidence for the client's
 // failure ladder. If it bumped the counter, a healthy-then-broken lane would
 // hand the first real client 429 a maxed-out cooldown and a tier-2 re-key.
 assert.equal(st.consecutiveFailures, 0, "a probe health failure must not advance the client's ladder");
});

// ── case 7: a probe 429 no longer schedules anything ───────────────
test("retry-after 30 on a probe is ignored instead of scheduling a probe", async () => {
 const sc = newScenario({ count: 2, throttle: { 1: { "model-a": { status: 429, retryAfter: 30 } } } });
 await startRelays(sc);
 const r = await runOnce(sc);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /#1 model-a 429 on probe — ignored \(client reports rate limits\)/);
 // The client's report carries its own retry-after to /__signal; a probe's
 // header no longer steers the scheduler.
 assert.doesNotMatch(r.stdout, /retry-after 30s — scheduling/);
});

// ── case 8: live signals ───────────────────────────────────────────
test("a live 429 signal rotates the named relay without probing it", async () => {
 const sc = newScenario({ count: 20 });
 await startRelays(sc);
 seedCleanState(sc, 1);
 fs.writeFileSync(sc.signalFile, JSON.stringify({
  signals: [{ ts: Date.now(), relay: 12, model: "model-a", status: 429, retryAfter: 30 }],
 }));
 const r = await runOnce(sc, { LIVE_SIGNALS: "1", PROBE_AUDIT_SAMPLE: "2" });
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-12"]);
 assert.equal(requests(sc, 12), 0, "the signalled relay must not be probed");
 assert.deepEqual(JSON.parse(fs.readFileSync(sc.signalFile, "utf8")), { signals: [] });
 assert.match(r.stdout, /audit sample: #2 #3/);
 assert.match(r.stdout, /pool is not cold: 1 relay\(s\) held or throttled/);
});

test("a live recovery signal marks the relay clean without probing it", async () => {
 const sc = newScenario({ count: 6 });
 await startRelays(sc);
 writeStateFile(sc, {
  4: entry({ dirty: true, pendingVerify: true, lastRotate: Date.now() - 3600_000, consecutiveFailures: 3, last429: { model: "model-a", ts: Date.now() - 60_000 } }),
 }, 0);
 fs.writeFileSync(sc.signalFile, JSON.stringify({ signals: [{ ts: Date.now(), relay: 4, model: "model-a", status: 200 }] }));
 const r = await runOnce(sc, { LIVE_SIGNALS: "1", PROBE_AUDIT_SAMPLE: "2" });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), []);
 assert.equal(requests(sc, 4), 0);
 assert.equal(readState(sc).relays["4"].dirty, false);
 assert.match(r.stdout, /pool is cold/);
});

// ── case 8b: signals never outvote the quota guard ─────────────────
test("live signals for a quota-capped model restart nothing", async () => {
 const sc = newScenario({ count: 20 });
 await startRelays(sc);
 seedCleanState(sc, 5);
 // Nine relays reporting the same model is the pool-wide quota signature:
 // rotating each of them would be the exact churn this scheduler removes.
 const signals = [];
 for (let i = 1; i <= 9; i++) {
  signals.push({ ts: Date.now(), relay: i, model: "model-a", status: 429, retryAfter: null });
 }
 fs.writeFileSync(sc.signalFile, JSON.stringify({ signals }));
 const r = await runOnce(sc, { LIVE_SIGNALS: "1", GLOBAL_QUOTA_GUARD: "9" });
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), []);
 assert.match(r.stdout, /model-a throttled on 9\/20 relays — upstream quota/);
 assert.match(r.stdout, /quota-capped: #1 #2 #3 #4 #5 #6 #7 #8 #9/);
});

// ── case 8c: a stale signal never re-rotates a restarted relay ──────
test("a signal older than the relay's last rotation is ignored", async () => {
 const sc = newScenario({ count: 4 });
 await startRelays(sc);
 const rotatedAt = Date.now() - 120_000;
 writeStateFile(sc, {
  2: entry({ lastRotate: rotatedAt, lastClean: Date.now() - 60_000 }),
 }, 0);
 fs.writeFileSync(sc.signalFile, JSON.stringify({
  signals: [{ ts: rotatedAt - 30_000, relay: 2, model: "model-a", status: 429, retryAfter: null }],
 }));
 const r = await runOnce(sc, { LIVE_SIGNALS: "1", PROBE_AUDIT_SAMPLE: "0" });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), []);
 assert.equal(requests(sc, 2), 0, "a stale signal must not probe the relay either");
});

// ── case 9: state file robustness ──────────────────────────────────
test("a corrupt state file degrades to a full sweep", async () => {
 const sc = newScenario({ count: 2 });
 await startRelays(sc);
 fs.writeFileSync(sc.stateFile, "{not json");
 const r = await runOnce(sc);
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.equal((r.stdout.match(/state file unusable/g) ?? []).length, 1, r.stdout);
 assert.equal(requests(sc, 1), 1);
 assert.equal(requests(sc, 2), 1);
});

test("an unwritable state path still completes the pass", async () => {
 const sc = newScenario({ count: 2 });
 await startRelays(sc);
 const blocker = path.join(sc.dir, "blocker");
 fs.writeFileSync(blocker, "not a directory");
 const r = await runOnce(sc, { RELAY_STATE_FILE: path.join(blocker, "state.json") });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.equal(requests(sc, 1), 1);
 assert.equal(requests(sc, 2), 1);
});

// ── case 10: exit code contract ────────────────────────────────────
test("--once exits 0 on a clean pool and 1 on a held one", async () => {
 const cold = newScenario({ count: 2 });
 await startRelays(cold);
 const ok = await runOnce(cold);
 assert.equal(ok.code, 0, ok.stdout + ok.stderr);
 assert.match(ok.stdout, /pool is cold/);

 const held = newScenario({ count: 2, throttle: { 2: { "model-a": { status: 429 } } } });
 await startRelays(held);
 const bad = await runOnce(held);
 assert.equal(bad.code, 1, bad.stdout + bad.stderr);
 assert.match(bad.stdout, /pool is not cold: 1 relay\(s\) held or throttled/);
});

// ── case 11: --fresh-ip-sweep contract ──────────────────────────────
test("--fresh-ip-sweep succeeds when at least one relay clears on the target model", async () => {
 const sc = newScenario({ count: 2, throttle: { 2: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0" }, ["--fresh-ip-sweep"]);
 assert.equal(r.code, 0, r.stdout + r.stderr);
 const lines = dockerLines(sc);
 assert.ok(lines.some((l) => l.startsWith("exec warp-relay-warp-1 rm -f /etc/wireguard/wg0.conf")), "warp-1 rm wg0.conf recorded");
 assert.ok(lines.some((l) => l.startsWith("restart warp-relay-warp-1")), "warp-1 restart recorded");
 assert.ok(lines.some((l) => l.startsWith("exec warp-relay-warp-2 rm -f /etc/wireguard/wg0.conf")), "warp-2 rm wg0.conf recorded");
 assert.ok(lines.some((l) => l.startsWith("restart warp-relay-warp-2")), "warp-2 restart recorded");
 assert.match(r.stdout, /fresh IP verified/i);
});

test("--fresh-ip-sweep identifies global upstream quota when all relays return 429", async () => {
 const sc = newScenario({
  count: 2,
  throttle: {
   1: { "model-a": { status: 429 } },
   2: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0", GLOBAL_QUOTA_GUARD: "2" }, ["--fresh-ip-sweep"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const restarts = restartLines(sc);
 assert.equal(restarts.length, 2, "all relays rotated once");
 assert.match(r.stdout, /global.*upstream.*quota/i);
});

test("--fresh-ip-sweep aborts during preflight if a container lacks wg0.conf", async () => {
 const sc = newScenario({ count: 2 });
 await startRelays(sc);
 const r = await runOnce(sc, { FAKE_DOCKER_MISSING_WG0: "warp-relay-warp-2" }, ["--fresh-ip-sweep"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(restartLines(sc), [], "zero restarts performed on preflight failure");
 assert.match(r.stderr + r.stdout, /preflight|wg0\.conf/i);
});

test("--fresh-ip-sweep rejects mutual exclusion with --once or --loop", async () => {
 const sc = newScenario({ count: 1 });
 const rOnce = await runOnce(sc, {}, ["--fresh-ip-sweep", "--once"]);
 assert.equal(rOnce.code, 2);
 assert.match(rOnce.stderr, /mutually exclusive/i);

 const rLoop = await runOnce(sc, {}, ["--fresh-ip-sweep", "--loop"]);
 assert.equal(rLoop.code, 2);
 assert.match(rLoop.stderr, /mutually exclusive/i);
});

// ── case 12: --reset-until-clean contract ───────────────────────────
test("--reset-until-clean skips already clean relays and resets only rate-limited relays until clean", async () => {
 let r3Attempts = 0;
 let r4Attempts = 0;
 const sc = newScenario({
  count: 4,
  throttle: {
   3: {
    "model-a": () => {
     r3Attempts++;
     return r3Attempts <= 1 ? { status: 429 } : null;
    },
   },
   4: {
    "model-a": () => {
     r4Attempts++;
     return r4Attempts <= 2 ? { status: 429 } : null;
    },
   },
  },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0" }, ["--reset-until-clean"]);
 assert.equal(r.code, 0, r.stdout + r.stderr);
 const restarts = restartLines(sc);
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-1")).length, 0, "warp-1 never reset");
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-2")).length, 0, "warp-2 never reset");
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-3")).length, 1, "warp-3 reset exactly once");
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-4")).length, 2, "warp-4 reset exactly twice");
 assert.match(r.stdout, /all.*clean|reset complete/i);
});

test("--reset-until-clean respects MAX_RESET_ATTEMPTS and exits 1 if relay remains throttled", async () => {
 const sc = newScenario({
  count: 2,
  throttle: {
   2: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "2" }, ["--reset-until-clean"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const restarts = restartLines(sc);
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-1")).length, 0, "warp-1 never reset");
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-2")).length, 2, "warp-2 reset exactly 2 times");
 assert.match(r.stdout + r.stderr, /exceeded|throttled|rate-limited/i);
});

test("--reset-until-clean never resets a relay failing for a non-rate-limit reason", async () => {
 const sc = newScenario({
  count: 3,
  throttle: {
   2: { "model-a": { status: 401 } },
   3: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "1" }, ["--reset-until-clean"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const restarts = restartLines(sc);
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-2")).length, 0, "a 401 is not an egress problem");
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-3")).length, 1, "the 429 relay is reset once");
 assert.match(r.stdout, /#2 not an egress rate limit/);
});

test("--reset-until-clean rejects mutual exclusion with --once, --loop, or --fresh-ip-sweep", async () => {
 const sc = newScenario({ count: 1 });
 const rOnce = await runOnce(sc, {}, ["--reset-until-clean", "--once"]);
 assert.equal(rOnce.code, 2);
 assert.match(rOnce.stderr, /mutually exclusive/i);

 const rLoop = await runOnce(sc, {}, ["--reset-until-clean", "--loop"]);
 assert.equal(rLoop.code, 2);
 assert.match(rLoop.stderr, /mutually exclusive/i);

 const rSweep = await runOnce(sc, {}, ["--reset-until-clean", "--fresh-ip-sweep"]);
 assert.equal(rSweep.code, 2);
 assert.match(rSweep.stderr, /mutually exclusive/i);
});

// ── case 13: per-egress rotation contract ───────────────────────────
test("--reset-until-clean soft-restarts first and spends no Cloudflare registration", async () => {
 let attempts = 0;
 const sc = newScenario({
  count: 2,
  throttle: {
   2: {
    "model-a": () => {
     attempts++;
     return attempts <= 1 ? { status: 429 } : null;
    },
   },
  },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", TARGET_MODEL: "model-a" },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 0, r.stdout + r.stderr);
 const lines = dockerLines(sc);
 assert.equal(lines.filter((l) => l.includes("rm -f /etc/wireguard/wg0.conf")).length, 0, "no wg0.conf deleted: tier-1 only");
 assert.equal(lines.filter((l) => l.startsWith("restart warp-relay-warp-2")).length, 1, "one soft restart");
 assert.match(r.stdout, /soft restart \(t1\)/);
 assert.match(r.stdout, /got 200 streaming \(CLEAN\)/);
});

test("--reset-until-clean escalates to a full reset when the soft restart cannot move the egress", async () => {
 const sc = newScenario({ count: 2, throttle: { 2: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  {
   ROTATE_SPACING_MS: "0",
   READY_TIMEOUT_MS: "4000",
   MAX_RESET_ATTEMPTS: "1",
   FAKE_DOCKER_STICKY_RESTARTS: "1",
   TARGET_MODEL: "model-a",
  },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const lines = dockerLines(sc);
 assert.equal(lines.filter((l) => l.includes("rm -f /etc/wireguard/wg0.conf")).length, 1, "one full reset after the stuck soft restart");
 assert.equal(lines.filter((l) => l.startsWith("restart warp-relay-warp-2")).length, 2, "soft restart then full reset");
 assert.match(r.stdout, /kept the same egress — escalating to a full reset/);
});

test("--reset-until-clean rejects a new address inside the same throttled /64 and escalates", async () => {
 const sc = newScenario({ count: 2, throttle: { 2: { "model-a": { status: 429 } } } });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  {
   ROTATE_SPACING_MS: "0",
   READY_TIMEOUT_MS: "4000",
   MAX_RESET_ATTEMPTS: "1",
   FAKE_DOCKER_PREFIX_STICKY: "1",
   TARGET_MODEL: "model-a",
  },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const lines = dockerLines(sc);
 assert.equal(lines.filter((l) => l.includes("rm -f /etc/wireguard/wg0.conf")).length, 1, "same-/64 address is not a fresh egress: full reset");
 assert.equal(lines.filter((l) => l.startsWith("restart warp-relay-warp-2")).length, 2, "soft restart then full reset");
 assert.match(r.stdout, /kept the same egress — escalating to a full reset/);
});

test("--reset-until-clean keeps rotating through multi-hour retry-after instead of halting", async () => {
 let attempts = 0;
 const sc = newScenario({
  count: 2,
  throttle: {
   2: {
    "model-a": () => {
     attempts++;
     return attempts <= 2 ? { status: 429, retryAfter: 25000 } : null;
    },
   },
  },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "4", TARGET_MODEL: "model-a" },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 0, r.stdout + r.stderr);
 const restarts = restartLines(sc);
 assert.equal(restarts.filter((l) => l.includes("warp-relay-warp-2")).length, 2, "kept rotating until the address streamed");
 assert.match(r.stdout, /retry-after: 25000/);
 assert.doesNotMatch(r.stdout, /Halting resets/);
 assert.match(r.stdout, /got 200 streaming \(CLEAN\)/);
});

test("--reset-until-clean keeps retrying a zero-yield round until MAX_RESET_ATTEMPTS", async () => {
 const sc = newScenario({
  count: 4,
  throttle: {
   1: { "model-a": { status: 429 } },
   2: { "model-a": { status: 429 } },
   3: { "model-a": { status: 429 } },
   4: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "2", TARGET_MODEL: "model-a" },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.equal(restartLines(sc).length, 8, "every relay rotates on both rounds; no zero-yield halt");
 assert.match(r.stdout, /round 2\/2/);
});

test("--reset-until-clean ceiling blocks tier-2 escalation but soft restarts continue", async () => {
 const sc = newScenario({
  count: 3,
  throttle: {
   1: { "model-a": { status: 429 } },
   2: { "model-a": { status: 429 } },
   3: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  {
   ROTATE_SPACING_MS: "0",
   MAX_RESET_ATTEMPTS: "2",
   SOFT_ROTATIONS_BEFORE_TIER2: "1",
   MAX_POOL_REGISTRATIONS_PER_HOUR: "1",
   TARGET_MODEL: "model-a",
  },
  ["--reset-until-clean"],
 );
 assert.equal(r.code, 1, r.stdout + r.stderr);
 const lines = dockerLines(sc);
 assert.equal(lines.filter((l) => l.includes("rm -f /etc/wireguard/wg0.conf")).length, 1, "only one registration spent");
 assert.equal(restartLines(sc).length, 6, "all three relays still rotated on both rounds");
 assert.match(r.stdout, /\[ceiling\]/);
});
test("--reset-until-clean skips probing when fresh IP is in throttled prefix blacklist cache", async () => {
 const sc = newScenario({
  count: 1,
  throttle: { 1: { "model-a": { status: 429 } } },
 });
 // Pre-seed blacklist with the prefix that the fake docker returns on attempt 1 (2a09:2:1:0)
 fs.writeFileSync(
  sc.blacklistFile,
  JSON.stringify({ "2a09:2:1:0": Date.now() + 3600_000 }),
 );
 await startRelays(sc);
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "2", TARGET_MODEL: "model-a" },
  ["--reset-until-clean"],
 );
 assert.match(r.stdout, /in blacklist cache/);
});



test("--probe reports per-relay status without touching any container", async () => {
 const sc = newScenario({
  count: 3,
  throttle: { 2: { "model-a": { status: 429, retryAfter: 7200 } } },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { TARGET_MODEL: "model-a" }, ["--probe"]);
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.deepEqual(dockerLines(sc), [], "probe mode never invokes docker");
 assert.match(r.stdout, /#1 200 streaming/);
 assert.match(r.stdout, /#2 429 rate limited \(retry-after: 7200s\)/);
 assert.match(r.stdout, /probe summary: 2\/3 streaming 200/);
 assert.equal(fs.existsSync(sc.stateFile), false, "probe mode writes no state");
});

test("PROBE_FINGERPRINT=minimal strips fingerprint headers but keeps relay headers", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  const legacy = await runOnce(sc, { TARGET_MODEL: "model-a" }, ["--probe"]);
  assert.equal(legacy.code, 0, legacy.stdout + legacy.stderr);
  const legacyReq = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.equal(legacyReq.ua, "opencode/latest/2.1.4/cli");
  assert.equal(legacyReq.ocClient, "cli");
  assert.equal(legacyReq.auth, "Bearer public");
  assert.match(legacyReq.session, /^ses_/);
  assert.equal(legacyReq.tools, 6);
  assert.equal(legacyReq.relayTarget, "https://opencode.ai");
  assert.equal(legacyReq.keepCold, "1");
  fs.writeFileSync(logPath, "");
  const minimal = await runOnce(sc, { TARGET_MODEL: "model-a", PROBE_FINGERPRINT: "minimal" }, ["--probe"]);
  assert.equal(minimal.code, 0, minimal.stdout + minimal.stderr);
  const minimalReq = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.equal(minimalReq.ua, "keep-cold/1.0");
  assert.equal(minimalReq.ocClient, "");
  assert.equal(minimalReq.auth, "");
  assert.equal(minimalReq.session, "");
  assert.equal(minimalReq.tools, -1);
  assert.equal(minimalReq.relayTarget, "https://opencode.ai");
  assert.equal(minimalReq.keepCold, "1");
});

test("invalid PROBE_FINGERPRINT fails fast instead of probing", async () => {
  const sc = newScenario({ count: 1 });
  const r = await runOnce(sc, { PROBE_FINGERPRINT: "bogus" });
  assert.equal(r.code, 2);
  assert.match(r.stderr, /PROBE_FINGERPRINT/);
});

test("legacy probe system content opens with the CLI opener line and carries a non-install env dir", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  const r = await runOnce(sc, { TARGET_MODEL: "model-a" }, ["--probe"]);
  assert.equal(r.code, 0, r.stdout + r.stderr);
  const req = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.ok(
    req.sys.startsWith("You are an AI agent running in OpenCode, a coding agent harness."),
    `system must open with the CLI opener line, got: ${req.sys.slice(0, 80)}`,
  );
  const idxEnv = req.sys.indexOf("<env>");
  assert.ok(idxEnv > 0, "<env> box must be appended after the opener, not before");
  assert.match(req.sys, /Working directory: \/tmp\/keep-cold-probe/, "non-install workspace dir");
  assert.equal(req.sys.trim(), req.sys, "no stray leading/trailing bytes");
});

test("invalid PROBE_FINGERPRINT fails fast instead of probing", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  const legacy = await runOnce(sc, { TARGET_MODEL: "model-a" }, ["--probe"]);
  assert.equal(legacy.code, 0, legacy.stdout + legacy.stderr);
  const req = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.match(req.session, /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/, "session id keeps the 12-hex+14-alnum gate shape");
  assert.equal((req.sys.match(/<env>/g) ?? []).length, 1, `exactly one <env> pair: ${req.sys}`);
  assert.equal((req.sys.match(/Working directory:/g) ?? []).length, 1, `exactly one Working directory pair: ${req.sys}`);
  fs.writeFileSync(logPath, "");
  const minimal = await runOnce(sc, { TARGET_MODEL: "model-a", PROBE_FINGERPRINT: "minimal" }, ["--probe"]);
  assert.equal(minimal.code, 0, minimal.stdout + minimal.stderr);
  const minReq = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.equal(minReq.sys, "", "minimal mode sends no system block");

  const scR = newScenario({ count: 1 });
  await startRelays(scR);
  const rLog = path.join(scR.dir, "relay-1.log");
  const rProbe = await runOnce(scR, {
    PROBE_MODELS: "dots-studio/dots-3-note-preview:free",
    TARGET_MODEL: "dots-studio/dots-3-note-preview:free",
  }, ["--probe"]);
  assert.equal(rProbe.code, 0, rProbe.stdout + rProbe.stderr);
  const rReq = JSON.parse(fs.readFileSync(rLog, "utf8").trim().split("\n").at(-1));
  assert.equal((rReq.sys.match(/<env>/g) ?? []).length, 1, `responses path carries one <env> pair: ${rReq.sys}`);
  assert.equal((rReq.sys.match(/Working directory:/g) ?? []).length, 1, `responses path carries Working directory: ${rReq.sys}`);
});

test("fingerprint matrix isolates body elements across four modes", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  async function lastReq(env) {
    fs.writeFileSync(logPath, "");
    const r = await runOnce(sc, { TARGET_MODEL: "model-a", ...env }, ["--probe"]);
    assert.equal(r.code, 0, r.stdout + r.stderr);
    return JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  }
  const legacy = await lastReq({});
  assert.equal(legacy.tools, 6, "legacy keeps the tools sextet");
  assert.equal((legacy.sys.match(/<env>/g) ?? []).length, 1, "legacy keeps one <env> pair");
  assert.match(legacy.session, /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/);
  const envOnly = await lastReq({ PROBE_FINGERPRINT: "env-only" });
  assert.equal(envOnly.tools, -1, "env-only drops tools");
  assert.equal((envOnly.sys.match(/<env>/g) ?? []).length, 1, "env-only keeps one <env> pair");
  assert.equal((envOnly.sys.match(/Working directory:/g) ?? []).length, 1);
  assert.equal(envOnly.ua, legacy.ua, "env-only shares legacy headers");
  assert.equal(envOnly.auth, legacy.auth, "env-only shares legacy auth");
  assert.match(envOnly.session, /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/);
  const tsOnly = await lastReq({ PROBE_FINGERPRINT: "timestamp-only" });
  assert.equal(tsOnly.tools, 6, "timestamp-only keeps the tools sextet");
  assert.ok(tsOnly.sys.length > 0, "timestamp-only keeps system content");
  assert.doesNotMatch(tsOnly.sys, /<env>/, "timestamp-only carries no <env> block");
  assert.equal(tsOnly.ua, legacy.ua, "timestamp-only shares legacy headers");
  assert.equal(tsOnly.auth, legacy.auth, "timestamp-only shares legacy auth");
  assert.match(tsOnly.session, /^ses_[0-9a-f]{12}[0-9A-Za-z]{14}$/);
  const minimal = await lastReq({ PROBE_FINGERPRINT: "minimal" });
  assert.equal(minimal.tools, -1, "minimal sends no tools");
  assert.equal(minimal.sys, "", "minimal sends no system block");
  assert.equal(minimal.session, "", "minimal sends no session header");
  async function lastResponsesReq(mode) {
    fs.writeFileSync(logPath, "");
    const r = await runOnce(sc, {
      PROBE_MODELS: "dots-studio/dots-3-note-preview:free",
      TARGET_MODEL: "dots-studio/dots-3-note-preview:free",
      ...(mode === "legacy" ? {} : { PROBE_FINGERPRINT: mode }),
    }, ["--probe"]);
    assert.equal(r.code, 0, r.stdout + r.stderr);
    return JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  }
  const rEnvOnly = await lastResponsesReq("env-only");
  assert.equal(rEnvOnly.tools, -1, "responses env-only drops tools");
  assert.equal((rEnvOnly.sys.match(/<env>/g) ?? []).length, 1, "responses env-only keeps one <env> pair");
  const rTsOnly = await lastResponsesReq("timestamp-only");
  assert.equal(rTsOnly.tools, 6, "responses timestamp-only keeps tools");
  assert.doesNotMatch(rTsOnly.sys, /<env>/, "responses timestamp-only carries no <env> block");
});

test("429 probe log carries redacted fp descriptor in all four modes", async () => {
  for (const [mode, tools] of [["env-only", 0], ["timestamp-only", 6]]) {
    const sc = newScenario({ count: 2, throttle: { 1: { "model-a": { status: 429, retryAfter: 30 } } } });
    await startRelays(sc);
    const env = mode === "env-only" ? { PROBE_FINGERPRINT: "env-only" } : { PROBE_FINGERPRINT: "timestamp-only" };
    const r = await runOnce(sc, env);
    assert.equal(r.code, 1, r.stdout + r.stderr);
    // The descriptor still rides the probe log; only the verdict it used to
    // drive is gone, so the assertion moves to the line that remains.
    assert.match(r.stdout, new RegExp(`#1 model-a 429 on probe — ignored \\(client reports rate limits\\)|fp=${mode}\\/tools=${tools}\\/auth=1`));
    assert.doesNotMatch(r.stdout, /Bearer public/);
    assert.doesNotMatch(r.stdout, /ses_[0-9a-f]{12}[0-9A-Za-z]{14}/);
  }
});

test("429 probe log leaks no fingerprint material in any mode", async () => {
  const throttle = { 1: { "model-a": { status: 429, retryAfter: 30 } } };
  for (const mode of ["legacy", "minimal"]) {
    const sc = newScenario({ count: 2, throttle });
    await startRelays(sc);
    const r = await runOnce(sc, mode === "minimal" ? { PROBE_FINGERPRINT: "minimal" } : {});
    assert.equal(r.code, 1, r.stdout + r.stderr);
    assert.match(r.stdout, /#1 model-a 429 on probe — ignored \(client reports rate limits\)/);
    assert.doesNotMatch(r.stdout, /Bearer public/);
    assert.doesNotMatch(r.stdout, /ses_[0-9a-f]{12}[0-9A-Za-z]{14}/);
  }
});

test("auto-discovers popular free models (deepseek, glm, mimo) from OpenCode by default", async () => {
 const sc = newScenario({ count: 1 });
 await startRelays(sc);
 const mockPort = takeBlock();
 const mockServer = http.createServer((req, res) => {
  if (req.url === "/opencode.ai/inference/v1/models") {
   res.writeHead(200, { "content-type": "application/json" });
   res.end(
    JSON.stringify({
     data: [
      { id: "deepseek-v4.1-flash-free" },
      { id: "mimo-v2.6-flash-free" },
      { id: "paid-gpt-4o" },
      { id: "jev-1.13-free" },
      { id: "glm-5.3-flash-free" },
      { id: "nemotron-3.5-lightning-free" },
     ],
    }),
   );
   return;
  }
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end("data: {}\\n\\n");
 });
 servers.push(mockServer);
 await new Promise((resolve) => mockServer.listen(mockPort, "127.0.0.1", resolve));

 const r = await runOnce(sc, {
  PROBE_TARGET: `http://127.0.0.1:${mockPort}/opencode.ai`,
  PROBE_MODELS: undefined,
 });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.match(r.stdout, /auto-discovered 3 free model\(s\) from OpenCode/);
 assert.match(r.stdout, /deepseek-v4.1-flash-free/);
 assert.match(r.stdout, /glm-5.3-flash-free/);
 assert.match(r.stdout, /mimo-v2.6-flash-free/);
 assert.doesNotMatch(r.stdout, /nemotron-3.5-lightning-free/);
 assert.doesNotMatch(r.stdout, /paid-gpt-4o/);
});

test("auto-discovers all free models when PROBE_MODELS_MODE is set to all", async () => {
 const sc = newScenario({ count: 1 });
 await startRelays(sc);
 const mockPort = takeBlock();
 const mockServer = http.createServer((req, res) => {
  if (req.url === "/opencode.ai/inference/v1/models") {
   res.writeHead(200, { "content-type": "application/json" });
   res.end(
    JSON.stringify({
     data: [
      { id: "deepseek-v4.1-flash-free" },
      { id: "mimo-v2.6-flash-free" },
      { id: "paid-gpt-4o" },
      { id: "glm-5.3-flash-free" },
      { id: "nemotron-3.5-lightning-free" },
     ],
    }),
   );
   return;
  }
  res.writeHead(200, { "content-type": "text/event-stream" });
  res.end("data: {}\\n\\n");
 });
 servers.push(mockServer);
 await new Promise((resolve) => mockServer.listen(mockPort, "127.0.0.1", resolve));

 const r = await runOnce(sc, {
  PROBE_TARGET: `http://127.0.0.1:${mockPort}/opencode.ai`,
  PROBE_MODELS: undefined,
  PROBE_MODELS_MODE: "all",
 });
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.match(r.stdout, /auto-discovered 4 free model\(s\) from OpenCode/);
 assert.match(r.stdout, /nemotron-3.5-lightning-free/);
});

test("falls back to default OpenCode free models if discovery endpoint fails", async () => {
 const sc = newScenario({ count: 1 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  {
   PROBE_TARGET: "http://127.0.0.1:24999/opencode.ai",
   PROBE_MODELS: undefined,
  },
  ["--probe"],
 );
 assert.match(r.stdout, /deepseek-v4.1-flash-free/);
});
// ── --sweep mode ────────────────────────────────────────────────────
test("--sweep probes all relays, restarts none on a probe 429", async () => {
 const sc = newScenario({
  count: 4,
  throttle: { 2: { "model-a": { status: 429 } } },
 });
 await startRelays(sc);
 const r = await runOnce(sc, { ROTATE_SPACING_MS: "0", MIN_CLEAN_COUNT: "3" }, ["--sweep"]);
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.match(r.stdout, /full sweep: 4 relay\(s\)/);
 assert.equal(touchedRelays(sc).length, 4, "all relays probed");
 // The sweep still proves liveness; it no longer treats a 429 as its verdict.
 assert.deepEqual(restartLines(sc), [], "a probe 429 must not restart a container");
 assert.match(r.stdout, /#2 model-a 429 on probe — ignored \(client reports rate limits\)/);
 assert.match(r.stdout, /pool is safe/);
});

test("--sweep does not rotate even when every relay answers 429", async () => {
 const sc = newScenario({
  count: 4,
  throttle: {
   1: { "model-a": { status: 429 } },
   2: { "model-a": { status: 429 } },
   3: { "model-a": { status: 429 } },
   4: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(sc, {}, ["--sweep"]);
 assert.deepEqual(restartLines(sc), [], "a fleet-wide probe 429 rotates nothing");
 // A lane that answers 429 is serving: the sweep's floor is a liveness floor,
 // and the rate-limit verdict is the client's to report. It is not degradation.
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.match(r.stdout, /every lane answered 429/);
 assert.match(r.stdout, /pool is safe/);
});

test("--sweep still counts a probe-throttled lane as clean for its health", async () => {
 const sc = newScenario({
  count: 4,
  throttle: {
   1: { "model-a": { status: 429 } },
   2: { "model-a": { status: 429 } },
  },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "1", MIN_CLEAN_COUNT: "2" },
  ["--sweep"],
 );
 assert.equal(r.code, 0, r.stdout + r.stderr);
 assert.match(r.stdout, /sweep result: 2\/4 relay\(s\) clean/);
 assert.match(r.stdout, /2 rate-limited \(client's verdict\)/);
 assert.match(r.stdout, /safe floor 2/);
 assert.match(r.stdout, /pool is safe/);
});

test("--sweep counts an unreachable lane against the floor, not a throttled one", async () => {
 const sc = newScenario({ count: 4 });
 await startRelays(sc, { dead: [1, 2, 3] });
 const r = await runOnce(
  sc,
  { ROTATE_SPACING_MS: "0", MAX_RESET_ATTEMPTS: "1", MIN_CLEAN_COUNT: "3" },
  ["--sweep"],
 );
 assert.equal(r.code, 1, r.stdout + r.stderr);
 assert.match(r.stdout, /pool is degraded: 3 relay\(s\) not alive/);
});

test("--sweep never spends a Cloudflare registration on a probe 429", async () => {
 const sc = newScenario({
  count: 2,
  throttle: { 2: { "model-a": { status: 429 } } },
 });
 await startRelays(sc);
 const r = await runOnce(
  sc,
  {
   ROTATE_SPACING_MS: "0",
   READY_TIMEOUT_MS: "4000",
   MAX_RESET_ATTEMPTS: "1",
   FAKE_DOCKER_STICKY_RESTARTS: "1",
   MIN_CLEAN_COUNT: "2",
  },
  ["--sweep"],
 );
 // 1 clean + 1 rate-limited = 2 alive, so the floor is met; what this test
 // guards is that a probe 429 never rotates anything.
 assert.equal(r.code, 0, r.stdout + r.stderr);
 // The sweep's escalation machinery is intact (--reset-until-clean still
 // re-keys a stuck /64), but a sweep probes only for liveness, so this path
 // must not rotate anything — no restart, no registration. (A `docker inspect`
 // for container health is not a rotation.)
 assert.deepEqual(restartLines(sc), [], "a probe 429 must not restart a container");
 assert.deepEqual(
  dockerLines(sc).filter((l) => l.includes("wg0.conf")),
  [],
  "a probe 429 must not spend a Cloudflare registration",
 );
});

test("PROBE_TRANSPORT=legacy probes the relay-terminated lane as before", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  const r = await runOnce(sc, { TARGET_MODEL: "model-a", PROBE_TRANSPORT: "legacy" }, ["--probe"]);
  assert.equal(r.code, 0, r.stdout + r.stderr);
  const req = JSON.parse(fs.readFileSync(logPath, "utf8").trim().split("\n").at(-1));
  assert.equal(req.relayTarget, "https://opencode.ai", "legacy mode rides the lane HTTP listener");
  assert.equal(req.keepCold, "1");
  assert.doesNotMatch(r.stdout, /probe transport/);
});

test("PROBE_TRANSPORT=wreq on a wreq-less host is fatal, not a silent degrade", async () => {
  const sc = newScenario({ count: 1 });
  await startRelays(sc);
  const logPath = path.join(sc.dir, "relay-1.log");
  const before = fs.existsSync(logPath) ? fs.readFileSync(logPath, "utf8") : "";
  const r = await runOnce(
    sc,
    { TARGET_MODEL: "model-a", PROBE_TRANSPORT: "wreq", KEEP_COLD_PROBE_WREQ_FAILFAST: "1" },
    ["--probe"],
  );
  assert.equal(r.code, 2, `expected fatal exit 2, got ${r.code}\n${r.stdout}${r.stderr}`);
  assert.match(r.stdout + r.stderr, /fatal - probe transport 'wreq' requires wreq-js/, "fatal is loud, names the fix");
  assert.doesNotMatch(r.stdout + r.stderr, /legacy fallback/, "no silent fallback line may appear");
  const after = fs.existsSync(logPath) ? fs.readFileSync(logPath, "utf8") : "";
  assert.equal(before, after, "no probe may execute without the truthful transport");
});

// ── case 9: --signals mode ─────────────────────────────────────────
// --signals is the signal-only pass: no probes, no audit sample, just the
// relays the records name. It exists so a minutes-granularity consumer can
// act on a live 429 without paying for a full sweep.
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

test("--signals restarts on a short retry-after instead of waiting it out", async () => {
  // A short retry-after describes the egress being abandoned, not the lane:
  // restarting mints a new one, so waiting the header out leaves a throttled
  // lane in a small pool for two minutes for no benefit. This is the shape
  // zen-proxy reports (upstream sends retry-after ~30), so it is the regression
  // that matters most here.
  const sc = newScenario({ count: 4 });
  await startRelays(sc);
  seedCleanState(sc);
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now(), relay: 2, model: "model-a", status: 429, retryAfter: 60 }],
  }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.equal(r.code, 1, r.stdout + r.stderr);
  assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-2"]);
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

test("--signals rotates on a two-lane pool where both lanes report the same model", async () => {
  // On a two-lane pool "throttled on a threshold of lanes" and "throttled
  // everywhere" are the same count, so a default guard would refuse the only
  // rotation that can still help. Each lane is a different exit IP; rotating
  // is the right answer even when both report it.
  const sc = newScenario({ count: 2 });
  await startRelays(sc);
  writeStateFile(sc, {
    1: entry({ lastProbe: Date.now(), lastClean: Date.now(), last429: { model: "model-a", ts: Date.now() } }),
    2: entry({ lastProbe: Date.now(), lastClean: Date.now(), last429: { model: "model-a", ts: Date.now() } }),
  });
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now(), relay: 1, model: "model-a", status: 429, retryAfter: null }],
  }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.doesNotMatch(r.stdout, /upstream quota/);
  assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-1"]);
});

test("--signals exits 0 with nothing to do and never probes", async () => {
  const sc = newScenario({ count: 4 });
  await startRelays(sc);
  seedCleanState(sc);
  fs.writeFileSync(sc.signalFile, JSON.stringify({ signals: [] }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.equal(r.code, 0, r.stdout + r.stderr);
  assert.deepEqual(restartLines(sc), []);
  assert.deepEqual(touchedRelays(sc), []);
});

// ── --watch-signals ─────────────────────────────────────────────────
// A one-minute timer is too slow for a 2-3 lane pool: by the time it fires the
// restarted lane could already be back, and the lanes still serving are the
// only ones left. These tests pin the two properties that make the watch mode
// worth running: it reacts to the write (not to a timer), and a burst of
// reports is one pass that still acts on every record.

function startWatch(sc, env = {}) {
  const combinedEnv = { ...process.env, ...baseEnv(sc), ...env };
  const child = spawn(process.execPath, ["keep-cold.mjs", "--watch-signals"], {
    cwd: REPO_ROOT,
    env: combinedEnv,
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (d) => (stdout += d));
  child.stderr.on("data", (d) => (stderr += d));
  const exited = new Promise((r) => child.once("exit", r));
  return {
    child,
    output: () => stdout + stderr,
    stop: async () => {
      child.kill();
      await Promise.race([exited, new Promise((r) => setTimeout(r, 5_000))]);
    },
  };
}

// The real writer renames a temp file into place, so the harness does too: the
// watch sees exactly the event shape production produces.
function writeSignalsAtomically(sc, records) {
  const tmp = `${sc.signalFile}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ signals: records }));
  fs.renameSync(tmp, sc.signalFile);
}

test("--watch-signals restarts on the file write instead of waiting for a timer", async () => {
  const sc = newScenario({ count: 2 });
  await startRelays(sc);
  writeStateFile(sc, {
    1: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
    2: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
  });
  // The guarantee is latency, not the mechanism: fs.watch where it fires, the
  // poll where it does not (Windows misses a rename on a watched directory
  // often enough that asserting on the watch alone would be a flaky test for a
  // property the product does not actually need). What must never happen is
  // the old behaviour: waiting for the next one-minute tick.
  const w = startWatch(sc, { WATCH_POLL_MS: "500", ROTATE_SPACING_MS: "0" });
  try {
    await waitFor(() => w.output().includes("signal watch:"), 10_000, w.output);
    const started = Date.now();
    writeSignalsAtomically(sc, [{ ts: Date.now(), relay: 1, model: "model-a", status: 429, retryAfter: null }]);
    await waitFor(() => restartLines(sc).length >= 1, 8_000, w.output);
    assert.ok(Date.now() - started < 5_000, "a report must not wait for a timer tick");
    assert.deepEqual(restartLines(sc), ["restart warp-relay-warp-1"]);
  } finally {
    await w.stop();
  }
});

test("--watch-signals acts on every record in one burst", async () => {
  const sc = newScenario({ count: 3 });
  await startRelays(sc);
  writeStateFile(sc, {
    1: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
    2: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
    3: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
  });
  // ROTATE_SPACING_MS=0: the production 4s gap between restarts exists to
  // spare a spinning disk, and two records in one burst are still two
  // restarts either way.
  const w = startWatch(sc, { WATCH_POLL_MS: "500", ROTATE_SPACING_MS: "0" });
  try {
    await waitFor(() => w.output().includes("signal watch:"), 10_000, w.output);
    const ts = Date.now();
    writeSignalsAtomically(sc, [
      { ts, relay: 1, model: "model-a", status: 429, retryAfter: null },
      { ts, relay: 2, model: "model-a", status: 429, retryAfter: null },
    ]);
    await waitFor(() => restartLines(sc).length >= 2, 8_000, w.output);
    assert.deepEqual(restartLines(sc).sort(), ["restart warp-relay-warp-1", "restart warp-relay-warp-2"]);
  } finally {
    await w.stop();
  }
});

test("--watch-signals leaves a lane alone when its report is quota-capped", async () => {
  // The watcher must not become a way around the guard: a model throttled on
  // enough lanes is upstream quota, and restarting would be pure churn. The
  // guard is set explicitly because the default never fires below three lanes
  // (a two-lane throttle is not evidence of an upstream-wide cap).
  const sc = newScenario({ count: 4 });
  await startRelays(sc);
  writeStateFile(sc, {
    1: entry({ lastProbe: Date.now(), lastClean: Date.now(), last429: { model: "model-a", ts: Date.now() } }),
    2: entry({ lastProbe: Date.now(), lastClean: Date.now(), last429: { model: "model-a", ts: Date.now() } }),
    3: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
    4: entry({ lastProbe: Date.now(), lastClean: Date.now() }),
  });
  const w = startWatch(sc, { WATCH_POLL_MS: "500", GLOBAL_QUOTA_GUARD: "2" });
  try {
    await waitFor(() => w.output().includes("signal watch:"), 10_000, w.output);
    writeSignalsAtomically(sc, [{ ts: Date.now(), relay: 1, model: "model-a", status: 429, retryAfter: null }]);
    await waitFor(() => /pool is (cold|not cold)/.test(w.output()), 8_000, w.output);
    assert.deepEqual(restartLines(sc), []);
  } finally {
    await w.stop();
  }
});

test("--signals ignores a record older than the relay's last rotation", async () => {
  const sc = newScenario({ count: 4 });
  await startRelays(sc);
  const relays = {};
  for (let i = 1; i <= 4; i++) {
    relays[String(i)] = entry({
      lastProbe: Date.now(), lastClean: Date.now(),
      lastRotate: Date.now() - 1_000,
      dirty: false,
    });
  }
  writeStateFile(sc, relays);
  fs.writeFileSync(sc.signalFile, JSON.stringify({
    signals: [{ ts: Date.now() - 60_000, relay: 2, model: "model-a", status: 429, retryAfter: null }],
  }));
  const r = await runOnce(sc, {}, ["--signals"]);
  assert.equal(r.code, 0, r.stdout + r.stderr);
  assert.deepEqual(restartLines(sc), []);
});
