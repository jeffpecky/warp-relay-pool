#!/usr/bin/env node
/**
 * keep-cold.mjs — demand-driven MicroWARP egress prober/rotator.
 *
 * Every relay in this pool reaches the upstream through its own Cloudflare WARP
 * client container, so each has its own egress IPv6 address. Cloudflare
 * recycles those addresses across public WARP users, which means a container
 * can come up already throttled by the upstream provider (HTTP 429) even
 * though the relay itself is perfectly healthy. Throttling is PER MODEL per
 * egress IP: warp-1 can serve one model 200 while 429ing another on the same
 * address (observed 2026-10-02), so one model is never enough to declare a
 * relay clean.
 *
 * Demand-driven probing
 * ---------------------
 * The first pass (RELAY_STATE_FILE holds no relay entries) is a full baseline
 * sweep of every relay 1..COUNT x PROBE_MODELS: only a 200/429 answers tell us
 * what the pool is doing. Every later pass probes only
 *   - relays a restart is still waiting to verify (pendingVerify, once the
 *     EGRESS_SETTLE_S window after that restart has elapsed),
 *   - relays still marked dirty whose probeAfter timer expired,
 *   - a round-robin audit sample of PROBE_AUDIT_SAMPLE clean relays, so a
 *     silent per-egress throttle is still discovered,
 *   - relays named by live traffic signals (written by server.mjs from a
 *     client's POST /__signal), which are rotated without any inference probe
 *     at all.
 * On a healthy 20-relay pool that is PROBE_AUDIT_SAMPLE requests per pass
 * instead of COUNT * |PROBE_MODELS| (60 before this change) — the disk-I/O
 * storm the old full sweep caused on the 5400 rpm host is gone.
 *
 * One restart per cooldown
 * ------------------------
 * A relay is restarted at most once per RELAY_COOLDOWN_S, at most
 * RELAY_MAX_ROTATIONS_PER_HOUR times per rolling hour, never inside the
 * EGRESS_SETTLE_S window that follows its previous restart, and only when its
 * cooldown has expired. Repeated failing batches multiply the cooldown
 * (RELAY_BACKOFF_BASE_S * 2^(n-1), capped at RELAY_BACKOFF_MAX_S), so a relay
 * that stays throttled backs off instead of churning. There is NO post-restart
 * probe, NO readiness wait and NO retry loop: the restart is recorded and a
 * later pass verifies it once the settle window has passed. All docker
 * restarts (tier-1, tier-2, 502 and server_error) share one serialisation
 * chain that starts them at least ROTATE_SPACING_MS apart.
 *
 * A 429 from the client's report always restarts (subject to the gate): the
 * report names an egress whose throttle the client already saw, and restarting
 * is what replaces it. The `retry-after` header is not consulted — it describes
 * the address being abandoned, not the lane. A probe 429 no longer rotates
 * anything (it is liveness evidence, and the client owns the verdict), and it
 * never reaches the state the client path reads: not `consecutiveFailures`, not
 * the `last429` quota history. Tier-2 (wipe /etc/wireguard/wg0.conf + restart,
 * one Cloudflare registration) is used only for 429 evidence, after two failed
 * post-restart verifications, and at most once per relay per TIER2_MIN_INTERVAL_S;
 * server_error and 502 rotations are tier-1 only. Nodes without wg0.conf abort
 * the re-key and fall back to a plain restart, leaving their registration untouched.
 *
 * Guardrails
 * ----------
 * A model the CLIENTS report as throttled on GLOBAL_QUOTA_GUARD relays is
 * upstream model quota: rotation is skipped for it this pass. The guard reads
 * the 429 history kept in the state file (900 s window), which only a client
 * report writes — probes cannot populate it. A model failing with server_error
 * on SERVER_ERROR_GUARD relays is an upstream incident: rotation is skipped for
 * it this pass.
 *
 * Status 0 (the host cannot reach the relay) and any other unexpected status
 * (403/500/503/...) are troubled: no restart, the relay waits
 * RELAY_COOLDOWN_S before the next probe. A server_error keeps its same-egress
 * re-probe filter (SERVER_ERROR_RETRY x SERVER_ERROR_REPROBE_MS) before the
 * gate; a server_error that clears on re-probe is clean.
 *
 * Speed ranking is logged per pass (`speed <model>: #N Nms …`) but rotation on
 * speed is DISABLED by default (SLOW_THRESHOLD_MS=0): the 2026-10-02 trial
 * falsified single-sample rank as an egress signal. With a threshold set, a
 * slow relay goes through the same gate/restart path, tier-1 only.
 *
 * The public relay URLs never change: the `*.trycloudflare.com` tunnels and the
 * relay application are different containers and are never touched here. Only
 * `warp-relay-warp-N` is ever restarted, so every URL already configured in the
 * client pool keeps working through the rotation.
 *
 * State (JSON, ms epochs) and signals are optimisations: a missing, corrupt or
 * unwritable state file (or signal file) degrades to a full sweep and a pass
 * that still runs, never a failed pass.
 *
 * Exit codes
 *  0  every relay is known clean at the end of the pass: nothing dirty, no
 *     quota-capped relay left failing, nothing troubled, no rotation awaiting
 *     verification
 *  1  at least one relay is dirty / held / throttled / awaiting verification
 *  2  bad usage or invalid environment
 *
 * Usage:
 *   node keep-cold.mjs --once                 # single pass
 *   node keep-cold.mjs --loop                 # pass every INTERVAL_S seconds
 *   INTERVAL_S=600 node keep-cold.mjs --loop
 *
 * Environment (new in this revision):
 *   RELAY_STATE_FILE    per-relay state (default .keep-cold/state.json)
 *   RELAY_COOLDOWN_S    minimum gap between rotations of one relay (default 600)
 *   RELAY_BACKOFF_BASE_S / RELAY_BACKOFF_MAX_S  exponential cooldown on repeated
 *                       failure, seconds (default 300 / 3600)
 *   RELAY_MAX_ROTATIONS_PER_HOUR  hard hourly budget per relay (default 2)
 *   PROBE_AUDIT_SAMPLE  clean relays probed per pass (default 2, min 0, max COUNT)
 *   EGRESS_SETTLE_S     post-restart quiet window before probing again (default 150)
 *   ROTATE_SPACING_MS   serialisation gap between restart operations (default 4000)
 *   LIVE_SIGNALS        1 = consume LIVE_SIGNAL_FILE (default 0)
 *   LIVE_SIGNAL_FILE    live 429/recovery records from server.mjs
 *                       (default .keep-cold/signals.json)
 *   PROBE_CONCURRENCY   relays probed in parallel (default 4, 1..64)
 *   KEEP_COLD_CLOCK_SKEW_S  test aid: now() = Date.now() + skew*1000 (default 0)
 *   DOCKER_BIN          docker binary (default docker)
 *   DOCKER_PREFIX_ARGS  space-separated args inserted before every docker
 *                       argument list (default empty)
 *
 * Environment (unchanged, still honoured):
 *   RELAY_HOST          relay app bridge address      (default 10.244.0.100)
 *   BASE_PORT           first relay port              (default 3000)
 *   COUNT               number of relays              (default 10)
 *   PROBE_TARGET        upstream the probes go through (default
 *                       https://opencode.ai — OpenCode Zen inference)
 *   PROBE_CHAT_PATH     chat wire path under PROBE_TARGET (default
 *                       /inference/v1/chat/completions)
 *   PROBE_RESPONSES_PATH  responses wire path under PROBE_TARGET (default
 *                       /inference/v1/responses)
 *   PROBE_MODELS        comma-separated models to probe (default: dynamically
 *                       discovered free tag models from OpenCode)
 *   PROBE_MODEL         legacy single-model override (used only when
 *                       PROBE_MODELS is unset)
 *   PROBE_FINGERPRINT   probe wire shape: legacy (default, current OpenCode
 *                       sextet+headers, byte-identical), env-only (legacy
 *                       headers + env block, no tools), timestamp-only
 *                       (legacy headers + tools + timestamp system prompt,
 *                       no env block), or minimal (relay-only, no
 *                       fingerprint headers/tools)
 *   PROBE_DIRECT        probe from this host's own egress instead of a lane
 *                       (diagnostic: separates egress throttling from shape)
 *   PROBE_PROJECT       x-opencode-project value (default "global"; the free
 *                       tier appears to meter quota per project, so a unique
 *                       hash may hold its own budget)
 *   GLOBAL_QUOTA_GUARD  relays-throttled threshold at which a model counts as
 *                       upstream quota (default max(8, COUNT-1), capped at COUNT)
 *   SERVER_ERROR_RETRY  same-egress re-probes before a server_error rotation
 *                       (default 1)
 *   SERVER_ERROR_REPROBE_MS  delay before each re-probe (default 5000)
 *   SERVER_ERROR_GUARD  relays-with-server_error threshold at which a model
 *                       counts as upstream incident (default 9, capped at COUNT)
 *   SPEED_MODEL         model whose latency decides slow rotation
 *   SLOW_THRESHOLD_MS   200-response slower than this is rotated for speed
 *                       (default 0 = disabled)
 *   CONTAINER_PREFIX    docker container prefix       (default warp-relay-warp-)
 *   PROBE_TIMEOUT_MS    per-probe request timeout     (default 30000)
 *   TIER2_MIN_INTERVAL_S  minimum seconds between tier-2 re-keys for one relay,
 *                       persisted in TIER2_STATE_FILE (default 3600)
 *   ROTATE_CONCURRENCY  parallel rotation workers     (default 1)
 *   INTERVAL_S          seconds between loop passes   (default 300)
 *   KEEP_COLD_LOG       append the log to this file   (default stdout only)
 *   KEEP_COLD_LOG_MAX_BYTES  rotate above this size    (default 2 MiB)
 *   KEEP_COLD_LOG_KEEP_BYTES keep this much on rotate   (default 200 KiB)
 *
 * Environment (accepted and ignored — kept so installed unit files stay valid):
 *   READY_TIMEOUT_MS, SOCKS_HOST, SOCKS_PORT_BASE, MAX_ROTATIONS,
 *   SLOW_MAX_ROTATIONS. Nothing here waits for readiness or verifies a rotation
 *   inline any more; those knobs only guarded the old retry loop.
 */
import { execFile, spawn } from "node:child_process";
import { randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const USAGE = `Usage: node keep-cold.mjs [--sweep|--once|--loop|--probe|--signals|--watch-signals|--fresh-ip-sweep|--reset-until-clean] [--help]

  --sweep            full-pool liveness scan (what the systemd timer runs)
  --once             incremental pass: probe only suspects + audit sample
  --loop             incremental pass every INTERVAL_S seconds (foreground)
  --probe            read-only per-relay probe report (no rotation, no state writes)
  --signals          act only on LIVE_SIGNAL_FILE records: no probes, no audit
                     sample, no state scan
  --watch-signals    like --signals, but run the moment a record lands (long-running)
  --fresh-ip-sweep   full-pool tier-2 re-key + fresh IP verification + probe sweep
  --reset-until-clean smart targeted full reset of rate-limited relays until 200 OK
  --help             print this text and exit`;

const argv = process.argv.slice(2);
const unknownArgs = argv.filter((a) => !["--sweep", "--once", "--loop", "--probe", "--signals", "--watch-signals", "--status", "--fresh-ip-sweep", "--reset-until-clean", "--help", "-h"].includes(a));
if (unknownArgs.length) {
 console.error(`keep-cold: unknown argument(s): ${unknownArgs.join(" ")}`);
 console.error(USAGE);
 process.exit(2);
}
if (argv.includes("--help") || argv.includes("-h")) {
 console.log(USAGE);
 process.exit(0);
}
const PROBE_ONLY = argv.includes("--probe") || argv.includes("--status");
const FRESH_IP_SWEEP = argv.includes("--fresh-ip-sweep");
const RESET_UNTIL_CLEAN = argv.includes("--reset-until-clean");
const SWEEP = argv.includes("--sweep");
const LOOP = argv.includes("--loop");
const ONCE = argv.includes("--once");
const SIGNALS = argv.includes("--signals");
const WATCH_SIGNALS = argv.includes("--watch-signals");

const modeFlags = [ONCE, LOOP, PROBE_ONLY, FRESH_IP_SWEEP, RESET_UNTIL_CLEAN, SWEEP, SIGNALS, WATCH_SIGNALS].filter(Boolean);
if (modeFlags.length > 1) {
 console.error("keep-cold: --sweep, --once, --loop, --probe, --signals, --watch-signals, --fresh-ip-sweep, and --reset-until-clean are mutually exclusive");
 process.exit(2);
}

/**
 * Parse an integer env var, failing fast on blank/garbage values. A silent
 * fallback here used to turn a pass into a no-op that still exited 0.
 */
function envInt(name, dflt, { min = 0, max = Number.MAX_SAFE_INTEGER } = {}) {
 const raw = process.env[name];
 if (raw === undefined) return dflt;
 if (raw.trim() === "") {
  console.error(`keep-cold: ${name} is set but blank`);
  process.exit(2);
 }
 if (!/^-?\d+$/.test(raw.trim())) {
  console.error(`keep-cold: ${name}='${raw}' must be an integer`);
  process.exit(2);
 }
 const n = Number(raw.trim());
 if (!Number.isSafeInteger(n) || n < min || n > max) {
  console.error(`keep-cold: ${name}='${raw}' must be an integer in ${min}..${max}`);
  process.exit(2);
 }
 return n;
}

/** Validate a probe wire path (rooted, no scheme). */
function envPath(name, dflt) {
 const value = process.env[name] ?? dflt;
 if (!value.startsWith("/")) {
  console.error(`keep-cold: ${name}='${value}' must be a rooted path`);
  process.exit(2);
 }
 return value;
}

function envUrl(name, dflt) {
 const value = (process.env[name] ?? dflt).replace(/\/+$/, "");
 try {
  const url = new URL(value);
  if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("not http(s)");
 } catch {
  console.error(`keep-cold: ${name}='${value}' is not a valid http(s) URL`);
  process.exit(2);
 }
 return value;
}

/**
 * Filesystem path: relative (the repo-relative defaults) or absolute (unit
 * files, tests). Only blank values are rejected.
 */
function envFile(name, dflt) {
 const value = process.env[name] ?? dflt;
 if (value.trim() === "") {
  console.error(`keep-cold: ${name} is set but blank`);
  process.exit(2);
 }
 return value;
}

// ── Logging ─────────────────────────────────────────────────────────
const LOG_PATH = process.env.KEEP_COLD_LOG ?? "";
const LOG_MAX_BYTES = envInt("KEEP_COLD_LOG_MAX_BYTES", 2 * 1024 * 1024, { min: 1 });
const LOG_KEEP_BYTES = envInt("KEEP_COLD_LOG_KEEP_BYTES", 200 * 1024, { min: 1 });
if (LOG_KEEP_BYTES >= LOG_MAX_BYTES) {
 console.error("keep-cold: KEEP_COLD_LOG_KEEP_BYTES must be smaller than KEEP_COLD_LOG_MAX_BYTES");
 process.exit(2);
}

/** Trim the log once it passes the cap, keeping the newest whole lines. */
function rotateLogIfNeeded() {
 try {
  if (fs.statSync(LOG_PATH).size <= LOG_MAX_BYTES) return;
  const buf = fs.readFileSync(LOG_PATH);
  const tail = buf.subarray(Math.max(0, buf.length - LOG_KEEP_BYTES));
  const nl = tail.indexOf(0x0a);
  const kept = nl >= 0 ? tail.subarray(nl + 1) : tail;
  // temp+rename: a kill mid-rotation must not lose the whole log, and a
  // concurrent manual run must not interleave with a truncate-then-write.
  const tmp = `${LOG_PATH}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, kept);
  fs.renameSync(tmp, LOG_PATH);
 } catch {
  // Missing log file — nothing to rotate.
 }
}

/** Always to stdout (journald captures it); optionally also to a file. */
function log(msg) {
 const line = `[${new Date().toISOString()}] ${msg}\n`;
 process.stdout.write(line);
 if (!LOG_PATH) return;
 try {
  rotateLogIfNeeded();
  fs.appendFileSync(LOG_PATH, line);
 } catch {
  // Logging must never break a pass.
 }
}

const RELAY_HOST = process.env.RELAY_HOST ?? "10.244.0.100";
const BASE_PORT = envInt("BASE_PORT", 3000, { max: 65535 });
// Direct public-IP ingress bakes RELAY_PROXY_AUTH into the relay containers;
// the dispatch lanes now demand HTTP Basic Proxy-Authorization on EVERY
// request with a target (health stays exempt), so the host prober must speak
// it too — otherwise every probe 407s, every relay reads dirty and rotation
// never happens. Supplied via the systemd unit's Environment= (setup.sh),
// or `--env-file=.env` for manual runs; standalone mesh/tunnels installs
// simply leave it unset.
const RELAY_PROXY_AUTH = process.env.RELAY_PROXY_AUTH ?? "";
const COUNT = envInt("COUNT", 10, { min: 1, max: 64 });
const PROBE_TARGET = envUrl("PROBE_TARGET", "https://opencode.ai");
const PROBE_CHAT_PATH = envPath("PROBE_CHAT_PATH", "/inference/v1/chat/completions");
const PROBE_RESPONSES_PATH = envPath("PROBE_RESPONSES_PATH", "/inference/v1/responses");

const PROBE_MODELS_MODE = (process.env.PROBE_MODELS_MODE ?? "popular").trim().toLowerCase();
const PROBE_FINGERPRINT = (process.env.PROBE_FINGERPRINT ?? "legacy").trim().toLowerCase();
if (!["legacy", "minimal", "env-only", "timestamp-only"].includes(PROBE_FINGERPRINT)) {
  console.error(`keep-cold: PROBE_FINGERPRINT='${process.env.PROBE_FINGERPRINT}' must be legacy|minimal|env-only|timestamp-only`);
  process.exit(2);
}
/** Probe transport: how the probe reaches the upstream TLS view.
 *  auto (default) .. wreq-js Bun-fingerprint session through the lane when the
 *                    target is opencode.ai, else legacy
 *  legacy .......... relay-terminated HTTP (lane listener, Node TLS upstream)
 *  wreq ............ force wreq (fail loud to legacy unusable-check) */
const PROBE_TRANSPORT = (process.env.PROBE_TRANSPORT ?? "auto").trim().toLowerCase();
if (!["auto", "legacy", "wreq"].includes(PROBE_TRANSPORT)) {
  console.error(`keep-cold: PROBE_TRANSPORT='${process.env.PROBE_TRANSPORT}' must be auto|legacy|wreq`);
  process.exit(2);
}
/** Pinned client version for the probe UA: the ecosystem's current pinned
 * fallback (live npm lookup is a proxy-side concern; the prober stays
 * deterministic). Declared up here because model discovery runs at module
 * load, before the fingerprint block below. Minimal mode still sends
 * keep-cold/1.0. */
const OPENCODE_PROBE_UA = "opencode/latest/2.1.4/cli";

const POPULAR_MODEL_PATTERNS = [
 /deepseek.*free/i,
 /glm.*free/i,
 /mimo.*free/i,
];

const DEFAULT_FALLBACK_OPENCODE_FREE_MODELS = [
 "deepseek-v4.1-flash-free",
 "glm-5.3-flash-free",
 "mimo-v2.6-flash-free",
];
const EXCLUDE_PROBE_MODELS = new Set([
 "jev-1.13-free", // non-chat decision model, 400s on chat
 "deepseek-v4-flash-free", // unserved upstream (400 unavailable)
]);

/**
 * Dynamically discover free tag models from OpenCode Zen.
 * Probes the upstream models endpoint and extracts models containing
 * '-free', ':free', or '/free', ensuring rotation survives model catalogue updates.
 */
async function discoverOpenCodeFreeModels(targetUrl) {
 try {
  const url = `${targetUrl.replace(/\/+$/, "")}/inference/v1/models`;
  const res = await fetch(url, {
   headers: {
    "user-agent": OPENCODE_PROBE_UA,
    "x-opencode-client": "cli",
    "accept": "application/json",
   },
   signal: AbortSignal.timeout(5000),
  });
  if (res.ok) {
   const body = await res.json();
   const list = Array.isArray(body) ? body : (body && Array.isArray(body.data) ? body.data : []);
   const freeIds = list
    .map((m) => (m && typeof m.id === "string" ? m.id.trim() : ""))
    .filter((id) => id && (id.includes("-free") || id.includes(":free") || id.includes("/free")) && !EXCLUDE_PROBE_MODELS.has(id));
   if (freeIds.length > 0) {
    freeIds.sort((a, b) => {
     if (a === "deepseek-v4.1-flash-free") return -1;
     if (b === "deepseek-v4.1-flash-free") return 1;
     return a.localeCompare(b);
    });
    if (PROBE_MODELS_MODE === "popular") {
     const popular = freeIds.filter((id) => POPULAR_MODEL_PATTERNS.some((re) => re.test(id)));
     if (popular.length > 0) {
      return popular;
     }
    }
    return freeIds;
   }
  }
 } catch {
  // Offline, mock/test environment, or network timeout -> fallback safely
 }
 return DEFAULT_FALLBACK_OPENCODE_FREE_MODELS;
}

const rawProbeEnv = process.env.PROBE_MODELS ?? process.env.PROBE_MODEL;
let PROBE_MODELS;
if (rawProbeEnv !== undefined) {
 if (rawProbeEnv.trim() === "") {
  console.error("keep-cold: PROBE_MODELS is set but blank");
  process.exit(2);
 }
 PROBE_MODELS = rawProbeEnv
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
 if (PROBE_MODELS.length === 0) {
  console.error("keep-cold: PROBE_MODELS is empty — nothing to probe");
  process.exit(2);
 }
} else {
 if (PROBE_TARGET.includes("opencode.ai")) {
  PROBE_MODELS = await discoverOpenCodeFreeModels(PROBE_TARGET);
  log(`auto-discovered ${PROBE_MODELS.length} free model(s) from OpenCode: ${PROBE_MODELS.join(", ")}`);
 } else {
  PROBE_MODELS = ["nvidia/nemotron-3.5-lightning:free", "cohere/north-mini-code:free", "dots-studio/dots-3-note-preview:free"];
 }
}
const RESPONSES_MODELS = new Set(
 (process.env.RESPONSES_MODELS
  ?? "dots-studio/dots-3-note-preview:free")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean),
);
const FRESH_IP_MODEL = process.env.FRESH_IP_MODEL ?? PROBE_MODELS[0];
const MAX_RESET_ATTEMPTS = envInt("MAX_RESET_ATTEMPTS", 10, { min: 1, max: 50 });
const SOFT_ROTATIONS_BEFORE_TIER2 = envInt("SOFT_ROTATIONS_BEFORE_TIER2", 1, { min: 1, max: 50 });
// Below three relays, "throttled on a threshold of lanes" and "throttled
// everywhere" are the same statement, so the default must not fire: on a
// two-lane pool the only surviving lane is the whole pool, and a rotation is
// the one thing that can still help. An explicit GLOBAL_QUOTA_GUARD still wins
// — the operator may know the model really is capped.
const GLOBAL_QUOTA_GUARD = Math.min(
 envInt(
  "GLOBAL_QUOTA_GUARD",
  COUNT < 3 ? COUNT + 1 : Math.max(8, COUNT - 1),
  { min: 1 },
 ),
 COUNT + 1,
);
const MAX_POOL_REGISTRATIONS_PER_HOUR = envInt("MAX_POOL_REGISTRATIONS_PER_HOUR", 60, { min: 1, max: 200 });
const SPEED_MODEL = process.env.SPEED_MODEL ?? "nvidia/nemotron-3.5-lightning:free";
const SLOW_THRESHOLD_MS = envInt("SLOW_THRESHOLD_MS", 0);
const CONTAINER_PREFIX = process.env.CONTAINER_PREFIX ?? "warp-relay-warp-";
const PROBE_TIMEOUT_MS = envInt("PROBE_TIMEOUT_MS", 30_000, { min: 1 });
const ROTATE_CONCURRENCY = envInt("ROTATE_CONCURRENCY", 1, { min: 1, max: 64 });
const SERVER_ERROR_RETRY = envInt("SERVER_ERROR_RETRY", 1);
const SERVER_ERROR_REPROBE_MS = envInt("SERVER_ERROR_REPROBE_MS", 5_000);
const SERVER_ERROR_GUARD = Math.min(envInt("SERVER_ERROR_GUARD", 9, { min: 1 }), COUNT);
const INTERVAL_S = envInt("INTERVAL_S", 300, { min: 1 });
const TIER2_MIN_INTERVAL_S = envInt("TIER2_MIN_INTERVAL_S", 60);

// ── Demand-driven scheduling knobs ──────────────────────────────────
const RELAY_STATE_FILE = envFile("RELAY_STATE_FILE", ".keep-cold/state.json");
const LIVE_SIGNAL_FILE = envFile("LIVE_SIGNAL_FILE", ".keep-cold/signals.json");
const TIER2_STATE_FILE = envFile("TIER2_STATE_FILE", ".keep-cold/tier2-state.json");
const PREFIX_BLACKLIST_FILE = envFile("PREFIX_BLACKLIST_FILE", ".keep-cold/throttled-prefixes.json");
const RELAY_COOLDOWN_S = envInt("RELAY_COOLDOWN_S", 600);
const RELAY_BACKOFF_BASE_S = envInt("RELAY_BACKOFF_BASE_S", 300, { min: 1 });
const RELAY_BACKOFF_MAX_S = envInt("RELAY_BACKOFF_MAX_S", 3600, { min: 1 });
const RELAY_MAX_ROTATIONS_PER_HOUR = envInt("RELAY_MAX_ROTATIONS_PER_HOUR", 2, { min: 1 });
const PROBE_AUDIT_SAMPLE = envInt("PROBE_AUDIT_SAMPLE", 2, { min: 0, max: COUNT });

// Safety floor for the full sweep: a pool counts as safe when at least this
// many relays answer 200 on every probed model. Defaults to half the deployed
// pool (10 of 20); MIN_CLEAN_COUNT pins an exact number instead.
const MIN_CLEAN_RATIO = (() => {
 const raw = process.env.MIN_CLEAN_RATIO;
 if (raw === undefined) return 0.5;
 const ratio = Number(raw.trim());
 if (raw.trim() === "" || !Number.isFinite(ratio) || ratio <= 0 || ratio > 1) {
  console.error(`keep-cold: MIN_CLEAN_RATIO='${raw}' must be a number in (0, 1]`);
  process.exit(2);
 }
 return ratio;
})();
const SAFE_FLOOR = Math.min(
 envInt("MIN_CLEAN_COUNT", Math.max(1, Math.ceil(COUNT * MIN_CLEAN_RATIO)), { min: 1 }),
 COUNT,
);
const EGRESS_SETTLE_S = envInt("EGRESS_SETTLE_S", 150);
const ROTATE_SPACING_MS = envInt("ROTATE_SPACING_MS", 4000);
const LIVE_SIGNALS = envInt("LIVE_SIGNALS", 0, { min: 0, max: 1 }) === 1;
const PROBE_CONCURRENCY = envInt("PROBE_CONCURRENCY", 4, { min: 1, max: 64 });
const CLOCK_SKEW_S = envInt("KEEP_COLD_CLOCK_SKEW_S", 0, { min: -Number.MAX_SAFE_INTEGER });
const DOCKER_BIN = envFile("DOCKER_BIN", "docker");
const DOCKER_PREFIX_ARGS = (process.env.DOCKER_PREFIX_ARGS ?? "").split(/\s+/).filter(Boolean);

// Accepted for backwards compatibility with installed unit files, then
// ignored: this controller no longer waits for readiness or verifies a
// rotation inline. Parsed (not just read) so a broken unit file still fails
// fast instead of silently changing behaviour.
const READY_TIMEOUT_MS = envInt("READY_TIMEOUT_MS", 180_000, { min: 1 });
envInt("SOCKS_PORT_BASE", 10800, { min: 1, max: 65535 });
envInt("MAX_ROTATIONS", 4, { min: 1 });
envInt("SLOW_MAX_ROTATIONS", 2);

/**
 * Every state timestamp, comparison and log-visible decision runs on now().
 * KEEP_COLD_CLOCK_SKEW_S exists so tests can walk a scenario forward in time
 * without touching the state file.
 */
function now() {
 return Date.now() + CLOCK_SKEW_S * 1000;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Short single-line reason for state-file diagnostics. */
function shortReason(e) {
 return String(e?.message ?? e ?? "unknown").replace(/\s+/g, " ").trim().slice(0, 80);
}

// ── Upstream fingerprint ────────────────────────────────────────────
// The opencode.ai free tier refuses requests that do not look like the
// official CLI: it compares the user-agent, the `read`/`shell` tool
// definitions byte-for-byte, and requires the `<env>` system block. These
// values mirror 9router's opencode executor (the only client known to pass),
// so a probe 429 means throttling rather than a rejected shape.
const OPENCODE_CLIENT = "cli";
// My best guess: free quota is accounted per project, because 9router defaults
// to the literal "global" and a real CLI sends a hash of its git remote, so a
// shared default can be burnt out by other users while a unique hash keeps its
// own budget. PROBE_PROJECT overrides it for a one-shot test.
const OPENCODE_PROJECT = process.env.PROBE_PROJECT ?? "global";
const ID_CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/** `ses_` + 12 hex + 14 alphanumeric, the shape the upstream gate validates. */
function sessionId() {
 const hex = randomBytes(6).toString("hex");
 let rest = "";
 const bytes = randomBytes(14);
 for (let i = 0; i < 14; i++) rest += ID_CHARS[bytes[i] % ID_CHARS.length];
 return `ses_${hex}${rest}`;
}

// Verbatim from the OpenCode CLI (9router's opencodeFingerprint.js). Upstream
// compares description and parameters byte-for-byte for these two names, and
// a request without them is refused with 429 regardless of quota. Do not
// reword or reformat. Other tool names are not gated.
const FINGERPRINT_TOOL_DEFINITIONS = {
 read: {
  description: "Read the contents of a file or directory. Supports text files, images, and PDFs. Images and PDFs are presented directly to the model. Each text line is prefixed by its 1-based line number as <line>: <content>. The prefix is for reference and is not part of the file content. Directory entries are returned one per line. Use offset and limit to read large files or directories in sections. Prefer one larger read over many small slices, and use grep to find specific content in large files.",
  parameters: {
   type: "object",
   properties: {
    path: { type: "string", description: "File or directory to read" },
    offset: { type: "integer", minimum: 0, description: "The line or directory entry to start reading from (1-based)" },
    limit: { type: "integer", minimum: 0, description: "The maximum number of lines or directory entries to read (defaults to and capped at 2000)" },
   },
   required: ["path"],
   additionalProperties: false,
  },
 },
 shell: {
  description: "Execute a shell command and return its output. Commands run on Linux using bash. Quote file paths containing spaces or special characters. Prefer dedicated tools over shell commands when possible. When output is large, the full result is saved to a file and a truncated preview is returned. Rely on automatic truncation unless filtering the output is more useful. Commands accept an optional timeout, background commands have no timeout by default. Background commands return immediately, and you will be notified when they complete.",
  parameters: {
   type: "object",
   properties: {
    command: { type: "string", description: "Shell command string to execute" },
    workdir: { type: "string", description: "Working directory to execute the command in. Defaults to the current working directory. When possible, avoid changing directories in the command and set the working directory here instead." },
    timeout: { type: "integer", minimum: 0, description: "Timeout in milliseconds. Set to 0 to disable the timeout. Defaults to 120000 for foreground commands. Background commands have no timeout by default." },
    background: { type: "boolean", description: "Run the command in the background and return immediately (useful for dev servers and long-running builds). You do not need to use '&' at the end of the command when using this parameter. You will be notified when it completes. DO NOT poll for completion." },
   },
   required: ["command"],
   additionalProperties: false,
  },
 },
};
// Real OpenCode CLI tool names; read/shell carry the gate-checked definitions.
const FINGERPRINT_TOOLS = ["bash", "glob", "grep", "read", "edit", "write"];
/** Definition for a declared tool: the verbatim one where upstream checks it,
 * a stub otherwise (only read/shell are compared). */
function fingerprintTool(name) {
 return FINGERPRINT_TOOL_DEFINITIONS[name] ?? {
  description: `${name} (probe)`,
  parameters: { type: "object", properties: {} },
 };
}
// Chat Completions placeholder shape: { type, function: { name, ... } }.
const PLACEHOLDER_TOOLS = FINGERPRINT_TOOLS.map((name) => ({
 type: "function",
 function: { name, ...fingerprintTool(name) },
}));
// Responses placeholder shape: flat { type, name, ... }.
const RESPONSE_PLACEHOLDER_TOOLS = FINGERPRINT_TOOLS.map((name) => ({
 type: "function",
 name,
 ...fingerprintTool(name),
}));

/** Redacted fingerprint descriptor for 429 logs: mode + tool count + auth + sid prefix. Never logs the full session id or Bearer value. */
function fpDescriptor(sid) {
  const toolCount = PROBE_FINGERPRINT === "legacy" || PROBE_FINGERPRINT === "timestamp-only" ? FINGERPRINT_TOOLS.length : 0;
  const hasAuth = PROBE_FINGERPRINT !== "minimal" && PROBE_TARGET.includes("opencode.ai") ? 1 : 0;
  return `fp=${PROBE_FINGERPRINT}/tools=${toolCount}/auth=${hasAuth}/sid=${String(sid).slice(0, 10)}`;
}

/** System prompt carrying the environment block the OpenCode free tier gates on:
 * a request whose system prompt has no `<env>` pair with `Working directory:`
 * is refused with deterministic 429 FreeUsageLimitError on any egress, so a
 * probe without it can never distinguish throttled from clean. A missing block
 * reads exactly like throttling: every relay 429s, including fresh egresses a
 * rotation just minted — rotation can never clear it. Tell the two apart by
 * scope: per-egress quota clears on a fresh address and trips only some relays
 * (the pool-wide guard counts agreement across relays), while a gate refusal
 * hits every egress at once with retry-after counting down to 00:00 UTC. If a
 * full reset still 429s everywhere, suspect the probe shape, not the pool. */
const PROBE_SYSTEM_PROMPT = [
  "You are an AI agent running in OpenCode, a coding agent harness. Help the user accomplish their goals using the tools you have available.",
  "",
  "You are a coding agent.",
  "<env>",
  "  Working directory: /tmp/keep-cold-probe",
  "  Workspace root folder: /tmp/keep-cold-probe",
  "  Is directory a git repo: no",
  // The gate checks the opener marker + the <env> shape, not the values; a
  // synthetic non-install workspace dir is what matters (as in opencodeFingerprint.js).
  "  Platform: linux",
  "  Prefer /tmp over generic system temporary directories such as os.tmpdir(); it is pre-created and approved for external access.",
  "</env>",
].join("\n");

/** Timestamp-only system content for the fingerprint matrix: carries a fresh
 * per-request timestamp but no `<env>` block, isolating whether the gate
 * checks the env block specifically or just fresh system-prompt content.
 * Built per call so every probe carries current time. */
function timestampPrompt() {
  return "You are an AI agent running in OpenCode, a coding agent harness. Help the user accomplish their goals using the tools you have available." +
    `\nCurrent time: ${new Date().toISOString()}`;
}

/** Build the wire-path + body for a probed model (chat vs responses).
 * Matrix (headers stay legacy for every mode but minimal):
 *   legacy ......... env block + tools (default, byte-identical)
 *   env-only ....... env block, no tools (is the tools sextet gated?)
 *   timestamp-only . timestamp system content + tools, no env block
 *                    (is the env block specifically gated?)
 *   minimal ........ neither (relay-only baseline) */
function buildProbe(model) {
  const minimal = PROBE_FINGERPRINT === "minimal";
  const withTools = PROBE_FINGERPRINT === "legacy" || PROBE_FINGERPRINT === "timestamp-only";
  const sysContent = minimal
    ? null
    : PROBE_FINGERPRINT === "timestamp-only"
      ? timestampPrompt()
      : PROBE_SYSTEM_PROMPT;
  if (RESPONSES_MODELS.has(model)) {
    return {
      relayPath: PROBE_RESPONSES_PATH,
      body: JSON.stringify({
        model,
        input: "ping",
        stream: true,
        ...(minimal ? {} : { instructions: sysContent, ...(withTools ? { tools: RESPONSE_PLACEHOLDER_TOOLS } : {}) }),
      }),
    };
  }
  return {
    relayPath: PROBE_CHAT_PATH,
    body: JSON.stringify({
      model,
      stream: true,
      max_tokens: 4,
      messages: [
        ...(minimal ? [] : [{ role: "system", content: sysContent }]),
        { role: "user", content: "ping" },
      ],
      ...(minimal || !withTools ? {} : { tools: PLACEHOLDER_TOOLS }),
    }),
  };
}

/** Legacy headers for legacy, env-only and timestamp-only alike: the matrix
 * varies the body only, so header bytes stay comparable across modes. Only
 * minimal strips down to relay-only headers. */
function probeHeaders(relayPath, sidOverride, index) {
  const isOpenCode = PROBE_TARGET.includes("opencode.ai");
  const sid = sidOverride ?? sessionId();
  // One relay port now serves the whole pool, so the lane has to travel as a
  // header. Honored only alongside x-keep-cold-probe, which every probe sends.
  const lane = index === undefined ? {} : { "x-relay-lane": String(index) };
  if (PROBE_FINGERPRINT === "minimal") {
    return {
      "content-type": "application/json",
      "user-agent": "keep-cold/1.0",
      // Route through the in-container relay app; the public tunnel is bypassed
      // on purpose so a rotation never depends on tunnel state.
      "x-relay-target": PROBE_TARGET,
      "x-relay-path": relayPath,
      // Marks keep-cold's own traffic: the relay app's live-signal writer must not
      // turn a probe's 429 into a "live traffic" signal (it would feed the next
      // pass a throttle the probe itself just created). Stripped before upstream.
      "x-keep-cold-probe": "1",
      ...lane,
      accept: "text/event-stream",
    };
  }
  const headers = {
    "content-type": "application/json",
    "user-agent": OPENCODE_PROBE_UA,
    "x-opencode-client": OPENCODE_CLIENT,
    "x-opencode-project": OPENCODE_PROJECT,
    "x-opencode-session": sid,
    // Route through the in-container relay app; the public tunnel is bypassed
    // on purpose so a rotation never depends on tunnel state.
    "x-relay-target": PROBE_TARGET,
    "x-relay-path": relayPath,
    // Marks keep-cold's own traffic: the relay app's live-signal writer must not
    // turn a probe's 429 into a "live traffic" signal (it would feed the next
    // pass a throttle the probe itself just created). Stripped before upstream.
    "x-keep-cold-probe": "1",
    ...lane,
    accept: "text/event-stream",
  };
  if (isOpenCode) {
    headers["authorization"] = "Bearer public";
    headers["x-opencode-session-id"] = sid;
    headers["x-session-affinity"] = sid;
    headers["x-session-id"] = sid;
  } else {
    headers["x-opencode-request"] = `msg_${randomBytes(6).toString("hex")}${randomBytes(14).toString("base64url").slice(0, 14)}`;
  }
  return headers;
}

// ── Per-relay state ─────────────────────────────────────────────────
// State is an optimisation: every read/write path degrades to the baseline
// full sweep instead of failing the pass.
let passBrokenStateLogged = false;
let passWriteFailureLogged = false;

function logBrokenStateOnce(reason) {
 if (passBrokenStateLogged) return;
 passBrokenStateLogged = true;
 log(`state file unusable (${reason}) — full sweep`);
}

function logWriteFailureOnce(reason) {
 if (passWriteFailureLogged) return;
 passWriteFailureLogged = true;
 log(`state file write failed (${reason}) — continuing`);
}

function emptyState() {
 return { version: 1, auditCursor: 0, relays: {} };
}

/** Read + shape-check the state file. Throws on anything unusable. */
function readStateFile() {
 const raw = fs.readFileSync(RELAY_STATE_FILE, "utf8");
 const parsed = JSON.parse(raw);
 if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("not an object");
 const relays = parsed.relays && typeof parsed.relays === "object" && !Array.isArray(parsed.relays)
  ? parsed.relays
  : {};
 const auditCursor = Number.isFinite(parsed.auditCursor) ? parsed.auditCursor : 0;
 return { version: 1, auditCursor, relays };
}

function loadState() {
 try {
  return readStateFile();
 } catch (e) {
  // ENOENT is the documented first run, not a broken file.
  if (e?.code !== "ENOENT") logBrokenStateOnce(e?.code ?? shortReason(e));
  return emptyState();
 }
}

function num(v) {
 return Number.isFinite(v) ? v : 0;
}

/** One relay's state with every field defaulted. */
function relayEntry(state, index) {
 const raw = state.relays?.[String(index)];
 const e = raw && typeof raw === "object" && !Array.isArray(raw) ? raw : {};
 const last429 = e.last429 && typeof e.last429 === "object" ? e.last429 : {};
 return {
  lastProbe: num(e.lastProbe),
  lastClean: num(e.lastClean),
  lastRotate: num(e.lastRotate),
  consecutiveFailures: num(e.consecutiveFailures),
  cooldownUntil: num(e.cooldownUntil),
  probeAfter: num(e.probeAfter),
  dirty: e.dirty === true,
  pendingVerify: e.pendingVerify === true,
  last429: { model: typeof last429.model === "string" ? last429.model : "", ts: num(last429.ts) },
  // Container-level healthcheck remediation stamp (separate lane from the
  // 429 rotation budget: a dead handshake is not an egress throttle).
  lastUnhealthyRestart: num(e.lastUnhealthyRestart),
  rotations: Array.isArray(e.rotations) ? e.rotations.map(num).filter((t) => t > 0) : [],
 };
}

/** Restart stamps are kept for the rolling-hour budget only. */
function pruneRotations(rotations, at) {
 const cutoff = at - 3_600_000;
 return rotations.filter((ts) => ts > cutoff).slice(-64);
}

/**
 * Merge-on-write: re-read the file, patch one relay (or the audit cursor), then
 * tmp+rename. Concurrent writers therefore cannot erase each other's stamps.
 */
function writeState(mutator, suffix) {
 try {
  fs.mkdirSync(path.dirname(RELAY_STATE_FILE), { recursive: true });
  let fresh;
  try {
   fresh = readStateFile();
  } catch {
   fresh = emptyState();
  }
  mutator(fresh);
  fresh.version = 1;
  const tmp = `${RELAY_STATE_FILE}.${process.pid}.${suffix}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(fresh));
  fs.renameSync(tmp, RELAY_STATE_FILE);
 } catch (e) {
  logWriteFailureOnce(shortReason(e));
 }
}

/** Patch one relay in memory and on disk; returns the merged entry. */
function patchRelay(state, index, patch) {
 const merged = { ...relayEntry(state, index), ...patch };
 merged.rotations = pruneRotations(merged.rotations, now());
 state.relays[String(index)] = merged;
 writeState((fresh) => {
  const next = { ...relayEntry(fresh, index), ...patch };
  next.rotations = pruneRotations(next.rotations, now());
  fresh.relays[String(index)] = next;
 }, String(index));
 return merged;
}

function saveAuditCursor(cursor) {
 writeState((fresh) => {
  fresh.auditCursor = cursor;
 }, "audit");
}

// ── Live signals ────────────────────────────────────────────────────
/**
 * Read and consume LIVE_SIGNAL_FILE: server.mjs appends one record per state
 * change, this controller rewrites the file as {"signals":[]} so no record is
 * processed twice. Missing/corrupt file = no signals, no log line.
 */
function readSignals() {
 let raw;
 try {
  raw = fs.readFileSync(LIVE_SIGNAL_FILE, "utf8");
 } catch {
  return null;
 }
 let parsed;
 try {
  parsed = JSON.parse(raw);
 } catch {
  return null;
 }
 const list = Array.isArray(parsed?.signals) ? parsed.signals : null;
 if (!list) return null;
 // Nothing to consume means nothing to write. Without this an idle pass
 // rewrites the file, which the --watch-signals watcher would read as a new
 // report and answer with another pass — a self-sustaining loop.
 if (list.length === 0) return list;
 try {
  fs.mkdirSync(path.dirname(LIVE_SIGNAL_FILE), { recursive: true });
  const tmp = `${LIVE_SIGNAL_FILE}.${process.pid}.signals.tmp`;
  fs.writeFileSync(tmp, JSON.stringify({ signals: [] }));
  fs.renameSync(tmp, LIVE_SIGNAL_FILE);
 } catch {
  // Consuming is best effort; a leftover record is re-read next pass.
 }
 return list.filter((s) => s && Number.isInteger(s.relay) && s.relay >= 1 && s.relay <= COUNT);
}

// ── Docker helpers ──────────────────────────────────────────────────
function containerName(index) {
 return `${CONTAINER_PREFIX}${index}`;
}

async function docker(args, timeout = 30_000) {
 const { stdout } = await execFileAsync(DOCKER_BIN, [...DOCKER_PREFIX_ARGS, ...args], {
  encoding: "utf8",
  timeout,
  maxBuffer: 4 * 1024 * 1024,
 });
 return stdout;
}

// ONE serialisation chain for every restart operation (tier-1, tier-2,
// 502/server_error): consecutive operations start at least ROTATE_SPACING_MS
// apart, no matter which phase queued them.
let restartChain = Promise.resolve();
let lastRestartAt = 0;

function enqueueRestart(work) {
 const next = restartChain.then(async () => {
  if (lastRestartAt > 0) {
   const gap = ROTATE_SPACING_MS - (now() - lastRestartAt);
   if (gap > 0) await sleep(gap);
  }
  lastRestartAt = now();
  return work();
 });
 // A failed operation must not poison the chain for the next relay.
 restartChain = next.then(() => { }, () => { });
 return next;
}

/** Matches both server_error shapes: transport JSON and 200-SSE envelope. */
const SERVER_ERROR_RE = /server_error|response\.failed|failed to generate/i;

/** Transport shape: non-200/non-429 JSON carrying the upstream failure. */
function isServerError(status, detail) {
 if (status === 200 || status === 429 || status === 0) return false;
 return SERVER_ERROR_RE.test(detail ?? "");
}

/**
 * Read at most the first 2 SSE chunks (~4KB cap) of a 200 body, then cancel.
 * Envelope-shape server_error hides inside 200 streams, which a
 * cancel-unread probe would count as clean.
 */
async function readEnvelopeHead(res) {
 const reader = res.body?.getReader?.();
 if (!reader) {
  await res.body?.cancel().catch(() => { });
  return "";
 }
 const dec = new TextDecoder();
 let head = "";
 try {
  for (let n = 0; n < 2; n++) {
   const { done, value } = await reader.read();
   if (done) break;
   head += dec.decode(value, { stream: true });
   if (head.length >= 4096) break;
  }
 } finally {
  await reader.cancel().catch(() => { });
 }
 return head.slice(0, 4096);
}

// ── Probing ─────────────────────────────────────────────────────────
// Bun TLS profile, copied verbatim from 9router's
// open-sse/services/opencodeTlsClient.js (Bun 1.3.14 measured via tls.peet.ws,
// 2026-10-27): ALPN HTTP1 only, X25519:P-256:P-384 groups, OCSP+ SCT on. The
// point of the wreq probe is to measure the gate from the SAME fingerprint
// class as real client traffic - a probe that rides the relay's Node TLS can
// disagree with what a wreq client sees from the same egress. Only the
// transport differs; the classification stays legacy.
const WREQ_CIPHER_LIST = [
  "TLS_AES_128_GCM_SHA256",
  "TLS_AES_256_GCM_SHA384",
  "TLS_CHACHA20_POLY1305_SHA256",
  "ECDHE-ECDSA-AES128-GCM-SHA256",
  "ECDHE-RSA-AES128-GCM-SHA256",
  "ECDHE-ECDSA-AES256-GCM-SHA384",
  "ECDHE-RSA-AES256-GCM-SHA384",
  "ECDHE-ECDSA-CHACHA20-POLY1305",
  "ECDHE-RSA-CHACHA20-POLY1305",
  "ECDHE-ECDSA-AES128-SHA",
  "ECDHE-RSA-AES128-SHA",
  "ECDHE-ECDSA-AES256-SHA",
  "ECDHE-RSA-AES256-SHA",
  "AES128-GCM-SHA256",
  "AES128-SHA",
  "AES256-SHA",
].join(":");
const WREQ_TLS_PROFILE = {
  cipherList: WREQ_CIPHER_LIST,
  alpnProtocols: ["HTTP1"],
  curvesList: "X25519:P-256:P-384",
  enableOcspStapling: true,
  enableSignedCertTimestamps: true,
};

// One wreq session per lane. Egress /64 belongs to the lane, and wreq's proxy
// is session-scoped, so this mirrors 9router's one-session-per-proxy-URL rule.
const wreqSessions = new Map();

async function wreqAvailable() {
  // Test hook: deterministic unavailability regardless of what the host
  // happens to have installed globally (fake-relay scenarios must not ride).
  if (process.env.KEEP_COLD_PROBE_WREQ_FAILFAST === "1") {
    throw new Error("test failfast (KEEP_COLD_PROBE_WREQ_FAILFAST=1)");
  }
  const wreq = await import("wreq-js");
  if (typeof wreq?.createSession !== "function") throw new Error("missing createSession");
  return wreq;
}

/** Transport choice for the opencode gate: measurable truth about the egress
 * comes from speaking its ClientHello. wreq-js is REQUIRED for auto/wreq on
 * opencode.ai targets — no silent degrade to relay-terminated probes (a
 * "clean" verdict measured on a different TLS identity lies). Explicit
 * PROBE_TRANSPORT=legacy stays a user choice; anything else fatal-exits. */
async function pickProbeTransport() {
  if (PROBE_TRANSPORT === "legacy" || !PROBE_TARGET.includes("opencode.ai")) return "legacy";
  try {
    return { wreq: await wreqAvailable() };
  } catch (e) {
    console.error(
      `keep-cold: fatal - probe transport '${PROBE_TRANSPORT}' requires wreq-js but it is unavailable: ${String(e?.message ?? e).slice(0, 150)}. Install it in this checkout: npm install (or run with PROBE_TRANSPORT=legacy to accept relay-terminated probes).`,
    );
    process.exit(2);
  }
}

async function wreqSession(index, wreq) {
  let entry = wreqSessions.get(index);
  if (entry) return entry;
  // Direct lane ingress speaks the same Basic Proxy-Authorization as the
  // relay-wide router; empty RELAY_PROXY_AUTH means a mesh install without
  // credentials (the probe then rides the unauthenticated lane shape). The
  // user carries the lane: `lane-N`, and the configured password when auth is
  // on — that pair is what the relay accepts as a pin.
  const pass = RELAY_PROXY_AUTH.includes(":") ? RELAY_PROXY_AUTH.slice(RELAY_PROXY_AUTH.indexOf(":") + 1) : "";
  const user = `lane-${index}${pass ? `:${pass}` : ""}`;
  // PROBE_DIRECT=1 drops the lane proxy so the same request rides this host's
  // own egress. Diagnostic only: distinguishes "every WARP egress is throttled"
  // from "the probe shape is still wrong".
  const proxy = process.env.PROBE_DIRECT === "1"
    ? null
    : `http://${user}@${RELAY_HOST}:${BASE_PORT}`;
  entry = wreq.createSession({
    emulation: { tlsOptions: { ...WREQ_TLS_PROFILE, alpnProtocols: [...WREQ_TLS_PROFILE.alpnProtocols] } },
    ...(proxy ? { proxy } : {}),
  });
  wreqSessions.set(index, entry);
  return entry;
}

function closeWreqSession(index) {
  const entry = wreqSessions.get(index);
  if (!entry) return;
  wreqSessions.delete(index);
  Promise.resolve(entry).then((s) => s?.close?.()).catch(() => {});
}

async function probe(index, model) {
  const { relayPath, body } = buildProbe(model);
  const sid = sessionId();
  const fp = fpDescriptor(sid);
  const t0 = Date.now();

  // wreq transport: direct client-side TLS through lane N's egress. The
  // relay-internal headers (x-relay-target/path, x-keep-cold-probe) are
  // relay app internals a tunneled request must not carry — a real client
  // never sends them.
  const transport = await pickProbeTransport();
  if (typeof transport === "object" && transport?.wreq) {
    try {
      const session = await wreqSession(index, transport.wreq);
      const target = new URL(relayPath, PROBE_TARGET);
      const headers = probeHeaders(relayPath, sid, index);
      delete headers["x-relay-target"];
      delete headers["x-relay-path"];
      delete headers["x-keep-cold-probe"];
      // The session's proxy userinfo already carries the lane.
      delete headers["x-relay-lane"];
      const res = await session.fetch(target, {
        method: "POST",
        headers,
        body,
        signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      });
      const verdict = await probeResponse(index, model, res, t0, fp, { transport: "wreq" });
      // Upstream's quota follows the TLS flow, so a reused session keeps
      // answering 429 after the egress rotates. Drop it and let the next
      // attempt build a fresh flow (same reset 9router does on 429).
      if (res.status === 429) closeWreqSession(index);
      return verdict;
    } catch (e) {
      // One dead probe must not poison the lane verdict with transport errors
      // forever: report status 0 (same shape as a legacy transport failure).
      return { index, model, status: 0, serverError: false, detail: `wreq: ${String(e?.message ?? e).slice(0, 150)}`, ms: Date.now() - t0, fp };
    }
  }

  const url = `http://${RELAY_HOST}:${BASE_PORT}`;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        ...probeHeaders(relayPath, sid, index),
        ...(RELAY_PROXY_AUTH
          ? { "proxy-authorization": `Basic ${Buffer.from(RELAY_PROXY_AUTH).toString("base64")}` }
          : {}),
      },
      body,
      signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
    });
    return probeResponse(index, model, res, t0, fp, { transport: "legacy" });
  } catch (e) {
    return { index, model, status: 0, serverError: false, detail: String(e?.message ?? e).slice(0, 160), ms: Date.now() - t0, fp };
  }
}

/** Shared verdict parsing for both transports: 200-envelope head, server_error
 * shape, numeric retry-after. `probeResponse` mutates nothing else - the
 * return shape is what classifyRelay() and the speed ranking already parse. */
async function probeResponse(index, model, res, t0, fp, { transport }) {
  if (res.status === 200) {
    // A body that dies mid-read is a wedged egress, not a clean probe: the
    // old `.catch(() => "")` silently turned it into a 200.
    let head;
    try {
      head = await readEnvelopeHead(res);
    } catch (e) {
      return { index, model, status: 0, serverError: false, detail: `body read failed: ${String(e?.message ?? e)}`.slice(0, 160), ms: Date.now() - t0, fp };
    }
    const ms = Date.now() - t0;
    if (SERVER_ERROR_RE.test(head)) {
      const evt = /response\.failed/i.test(head) ? "response.failed" : /failed to generate/i.test(head) ? "failed to generate" : "server_error";
      return { index, model, status: 200, serverError: true, detail: `envelope SSE ${evt}: ${head.replace(/\s+/g, " ").slice(0, 120)}`, ms, fp };
    }
    return { index, model, status: 200, serverError: false, detail: "", ms, fp };
  }
  const text = await res.text().catch(() => "");
  const detail = text.replace(/\s+/g, " ").slice(0, 160);
  // Numeric Retry-After only; an HTTP-date header is left to the gate.
  const raRaw = res.status === 429 ? res.headers?.get?.("retry-after") : null;
  const retryAfter = raRaw && /^\d+$/.test(raRaw.trim()) ? Number(raRaw.trim()) : null;
  return { index, model, status: res.status, serverError: isServerError(res.status, detail), detail, retryAfter, ms: Date.now() - t0, fp };
}

/** Probe a set of models on one relay (serial - one relay, one egress). */

/** Probe a set of models on one relay (serial — one relay, one egress). */
async function probeRelay(index, models = PROBE_MODELS) {
 const out = [];
 for (const model of models) out.push(await probe(index, model));
 return out;
}

/** Query a container's current egress IPv6 via Cloudflare trace. */
async function getEgressIp(index) {
 try {
  const out = await docker(["exec", containerName(index), "curl", "-s", "-m", "8", "https://cloudflare.com/cdn-cgi/trace"], 25_000);
  const ipMatch = out.match(/^ip=(.+)$/m);
  const warpMatch = out.match(/^warp=(.+)$/m);
  if (ipMatch && warpMatch && warpMatch[1].trim() === "on") {
   return ipMatch[1].trim();
  }
  return null;
 } catch {
  return null;
 }
}

/**
 * Expand an IPv6 literal and return its /64. Upstream throttle state follows
 * the block, not the interface address: two containers can hold different
 * addresses inside one throttled /64 and share the same 429 window, so a
 * rotation that only moves the suffix has changed nothing.
 */
function egressPrefix(ip) {
 if (!ip) return "";
 const s = String(ip).trim();
 if (!s.includes(":")) return s;
 const [head, tail = ""] = s.split("::");
 const headParts = head ? head.split(":") : [];
 const tailParts = tail ? tail.split(":") : [];
 const missing = Math.max(0, 8 - headParts.length - tailParts.length);
 const full = [...headParts, ...Array(missing).fill("0"), ...tailParts];
 return full.slice(0, 4).join(":");
}
function loadThrottledPrefixes(file = PREFIX_BLACKLIST_FILE) {
 try {
  const raw = fs.readFileSync(file, "utf8");
  const data = JSON.parse(raw);
  const nowMs = now();
  const cleaned = {};
  for (const [pfx, exp] of Object.entries(data)) {
   if (Number(exp) > nowMs) cleaned[pfx] = Number(exp);
  }
  return cleaned;
 } catch {
  return {};
 }
}

function rememberThrottledPrefix(prefix, retryAfterS = 3600, file = PREFIX_BLACKLIST_FILE) {
 if (!prefix) return;
 const map = loadThrottledPrefixes(file);
 const exp = now() + (parseInt(retryAfterS, 10) || 3600) * 1000;
 map[prefix] = exp;
 try {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(map, null, 2), "utf8");
 } catch {}
}

function isPrefixThrottled(prefix, file = PREFIX_BLACKLIST_FILE) {
 if (!prefix) return false;
 const map = loadThrottledPrefixes(file);
 const exp = Number(map[prefix] ?? 0);
 return exp > now();
}


/**
 * --fresh-ip-sweep: serial full reset (tier-2) of every relay in the pool,
 * verifying each relay receives a distinct fresh egress IP from Cloudflare,
 * then probing a single target model to separate per-relay rate limits from
 * global upstream quota.
 */
async function freshIpSweep() {
 const state = loadState();
 const initialIps = new Map();

 // ── Phase 1: Preflight ──
 for (let i = 1; i <= COUNT; i++) {
  let hasWg0 = false;
  try {
   await docker(["exec", containerName(i), "test", "-f", "/etc/wireguard/wg0.conf"], 30_000);
   hasWg0 = true;
  } catch {
   hasWg0 = false;
  }
  if (!hasWg0) {
   log(`#${i} preflight failed: missing /etc/wireguard/wg0.conf — aborting fresh-IP sweep with zero mutations`);
   return false;
  }
  const ip = await getEgressIp(i);
  if (!ip) {
   log(`#${i} preflight failed: unable to query egress IP`);
   return false;
  }
  initialIps.set(i, ip);
 }
 log(`preflight passed: ${COUNT}/${COUNT} relays verified with active wg0.conf`);

 // ── Phase 2: Serial Full-Reset + IP verification + Probe ──
 const cleanRelays = [];
 const throttledRelays = [];
 const failedRotations = [];

 for (let index = 1; index <= COUNT; index++) {
  const oldIp = initialIps.get(index);
  const container = containerName(index);
  const at = await enqueueRestart(async () => {
   const started = now();
   log(`#${index} full reset (t2) — deleting wg0.conf and restarting ${container}`);
   await docker(["exec", container, "rm", "-f", "/etc/wireguard/wg0.conf"], 30_000);
   await docker(["restart", container], 60_000);
   saveTier2State(index, started);
   return started;
  });

  // Poll for a fresh /64
  const deadline = Date.now() + READY_TIMEOUT_MS;
  const oldPrefix = egressPrefix(oldIp);
  let newIp = null;
  while (Date.now() < deadline) {
   newIp = await getEgressIp(index);
   if (newIp && egressPrefix(newIp) !== oldPrefix) break;
   await sleep(3000);
  }

  if (!newIp || egressPrefix(newIp) === oldPrefix) {
   log(`#${index} fresh IP verification failed (same IP or not ready)`);
   failedRotations.push(index);
   continue;
  }
  log(`#${index} fresh IP verified (changed from previous egress)`);

  patchRelay(state, index, { lastRotate: at, consecutiveFailures: 0, dirty: false, pendingVerify: false });

  const [res] = await probeRelay(index, [FRESH_IP_MODEL]);
  if (res.status === 200 && !res.serverError) {
   log(`#${index} probe ${FRESH_IP_MODEL} -> 200 OK (clean)`);
   cleanRelays.push(index);
  } else if (res.status === 429) {
   log(`#${index} probe ${FRESH_IP_MODEL} -> 429 rate limited` + (res.retryAfter ? ` (retry-after: ${res.retryAfter}s)` : "") + ` (${res.fp})`);
   throttledRelays.push(index);
   // Still rate-limited, so stay dirty — but no `last429` write: that field is
   // the signal path's quota history, and this is a probe. The guard below
   // uses this pass's own count.
   patchRelay(state, index, { dirty: true });
  } else {
   log(`#${index} probe ${FRESH_IP_MODEL} -> HTTP ${res.status || "unreachable"} (${res.detail})`);
  }
 }

 // ── Phase 3: Summary and Exit ──
 log(`fresh-IP sweep complete: ${cleanRelays.length}/${COUNT} clean, ${throttledRelays.length}/${COUNT} throttled, ${failedRotations.length} failed verification`);
 if (cleanRelays.length > 0) {
  log(`success: ${cleanRelays.length} relay(s) cleared rate limits on ${FRESH_IP_MODEL}`);
  return true;
 }
 if (throttledRelays.length >= GLOBAL_QUOTA_GUARD) {
  log(`${FRESH_IP_MODEL} throttled on ${throttledRelays.length}/${COUNT} relays across fresh IPs — confirmed global upstream quota`);
  return false;
 }
 return false;
}

/**
 * --probe: read-only per-relay status using the exact production probe shape.
 * No docker calls, no rotation, no state writes — safe to run any time,
 * including while the timer pass is mid-flight.
 */
async function probePool() {
 const targetModel = process.env.TARGET_MODEL ?? process.env.FRESH_IP_MODEL ?? PROBE_MODELS[0];
 log(`probing ${COUNT} relay(s) on ${targetModel}...`);
 const rows = [];
 await runPool(
  Array.from({ length: COUNT }, (_, i) => i + 1),
  PROBE_CONCURRENCY,
  async (index) => {
   const [res] = await probeRelay(index, [targetModel]);
   rows.push({ index, res });
  },
 );
 rows.sort((a, b) => a.index - b.index);
 let clean = 0;
 for (const { index, res } of rows) {
  if (res.status === 200 && !res.serverError) {
   clean++;
   log(`#${index} 200 streaming`);
  } else if (res.status === 429) {
   log(`#${index} 429 rate limited` + (res.retryAfter ? ` (retry-after: ${res.retryAfter}s)` : "") + ` (${res.fp})`);
  } else {
   log(`#${index} ${res.status || "unreachable"} ${res.detail}`.slice(0, 200));
  }
 }
 log(`probe summary: ${clean}/${COUNT} streaming 200 on ${targetModel}`);
 return clean === COUNT;
}

/**
 * Restart one relay on the requested tier. Tier 1 keeps wg0.conf (same keys,
 * fresh WARP /64, zero Cloudflare registrations); tier 2 deletes it first and
 * therefore spends one registration. Serialised through the restart queue so
 * two stop/starts never overlap.
 */
async function rotateContainer(index, tier, attempt) {
 const container = containerName(index);
 return enqueueRestart(async () => {
  const started = now();
  if (tier === 2) {
   log(`#${index} [attempt ${attempt}] full reset (t2) — deleting wg0.conf and restarting ${container}`);
   await docker(["exec", container, "rm", "-f", "/etc/wireguard/wg0.conf"], 30_000);
   await docker(["restart", container], 60_000);
   saveTier2State(index, started);
  } else {
   log(`#${index} [attempt ${attempt}] soft restart (t1) — restarting ${container} (wg0.conf kept)`);
   await docker(["restart", container], 60_000);
  }
  return started;
 });
}

/** Poll the relay's trace endpoint until its /64 differs from `oldIp`. */
async function waitForFreshIp(index, oldIp) {
 const oldPrefix = egressPrefix(oldIp);
 const deadline = Date.now() + READY_TIMEOUT_MS;
 while (Date.now() < deadline) {
  await sleep(3000);
  const ip = await getEgressIp(index);
  if (ip && egressPrefix(ip) !== oldPrefix) return ip;
 }
 return null;
}

/**
 * Rotate one relay until it streams 200 on every probed model, or the attempt
 * cap runs out. Time is never the criterion: a multi-hour retry-after applies
 * to the address being abandoned, so the loop keeps minting new egresses.
 * Escalates to a tier-2 re-key when a soft restart cannot move the /64 block,
 * after `SOFT_ROTATIONS_BEFORE_TIER2` soft attempts, or as soon as the new
 * address is still throttled with the soft streak exhausted.
 *
 * Returns true when the relay ends clean.
 */
async function resetRelayUntilClean(index, models, state) {
 let oldIp = await getEgressIp(index);
 let streak = 0;
 for (let attempt = 1; attempt <= MAX_RESET_ATTEMPTS; attempt++) {
  let tier = streak >= SOFT_ROTATIONS_BEFORE_TIER2 ? 2 : 1;
  if (tier === 2 && !tier2Allowed(index)) {
   log(`#${index} [attempt ${attempt}] tier-2 budget for this relay is spent — soft restart instead`);
   tier = 1;
  }
  if (tier === 2 && poolRegistrationsWithinHour(state, now()).length >= MAX_POOL_REGISTRATIONS_PER_HOUR) {
   log(`[ceiling] pool-wide registration ceiling reached (${MAX_POOL_REGISTRATIONS_PER_HOUR}/hour) — #${index} stays on soft restarts`);
   tier = 1;
  }

  let at = await rotateContainer(index, tier, attempt);
  let newIp = await waitForFreshIp(index, oldIp);

  // A soft restart that cannot move the /64 is exactly what the full reset
  // exists for: re-register now instead of spending the next attempt.
  if (!newIp && tier === 1) {
   const ceilingHit = poolRegistrationsWithinHour(state, now()).length >= MAX_POOL_REGISTRATIONS_PER_HOUR;
   if (ceilingHit) {
    log(`[ceiling] pool-wide registration ceiling reached (${MAX_POOL_REGISTRATIONS_PER_HOUR}/hour) — #${index} cannot escalate to a full reset`);
   } else if (!tier2Allowed(index)) {
    log(`#${index} tier-2 budget for this relay is spent — cannot escalate this round`);
   } else {
    log(`#${index} soft restart kept the same egress — escalating to a full reset (t2)`);
    tier = 2;
    at = await rotateContainer(index, tier, attempt);
    newIp = await waitForFreshIp(index, oldIp);
   }
  }

  if (!newIp) {
   log(`#${index} [attempt ${attempt}] fresh egress not verified (same /64 or not ready) — rotating again`);
   streak += 1;
   continue;
  }
  oldIp = newIp;
  const pfx = egressPrefix(newIp);
  if (isPrefixThrottled(pfx)) {
   log(`#${index} [attempt ${attempt}] prefix ${pfx} is in blacklist cache (throttled) — skipping probe and re-rolling`);
   streak += 1;
   continue;
  }
  patchRelay(state, index, {
   lastRotate: at,
   dirty: false,
   pendingVerify: false,
   probeAfter: 0,
   cooldownUntil: 0,
   consecutiveFailures: 0,
   last429: { model: "", ts: 0 },
   // Only a full reset spends a Cloudflare registration; the hourly budget
   // counts registrations, so a soft restart must not consume one.
   ...(tier === 2 ? { rotations: [...relayEntry(state, index).rotations, at] } : {}),
  });

  const results = await probeRelay(index, models);
  const failing = results.filter((r) => r.status !== 200 || r.serverError);
  if (failing.length === 0) {
   patchRelay(state, index, cleanPatch());
   log(`#${index} clean on all ${models.length} model(s) — egress ${newIp}`);
   return true;
  }
  if (failing.every((r) => r.status === 429)) {
   rememberThrottledPrefix(pfx, failing[0].retryAfter || 3600);
   log(
    `#${index} [attempt ${attempt}] still 429 on the new egress` +
     (failing[0].retryAfter ? ` (retry-after: ${failing[0].retryAfter}s)` : "") +
     ` — rotating again for a clean address (${failing[0].fp})`,
   );
   streak += 1;
   continue;
  }
  for (const r of failing) log(`#${index} ${r.model} unexpected ${r.status || "unreachable"} on a fresh egress (${r.detail})`.slice(0, 200));
  // Probe evidence: leave the client's failure ladder alone (see the sweep).
  patchRelay(state, index, {
   dirty: true,
   lastProbe: now(),
   pendingVerify: false,
   probeAfter: now() + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: relayEntry(state, index).consecutiveFailures,
  });
  return false;
 }
 patchRelay(state, index, { dirty: true, lastProbe: now(), probeAfter: now() + RELAY_COOLDOWN_S * 1000 });
 log(`#${index} still rate-limited after ${MAX_RESET_ATTEMPTS} rotation attempt(s)`);
 return false;
}

/**
 * Full-pool sweep: probe every relay on every model for liveness and health.
 * A probe 429 is logged and rotates nothing — the client owns that verdict
 * (POST /__signal → --signals). This is what the systemd timer runs.
 *
 * Returns true when at least SAFE_FLOOR relays are alive (clean or 429).
 */
/** Container-level healthcheck status, or "none" when no healthcheck exists. */
async function containerHealth(index) {
 try {
  const out = await docker(
   ["inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}", containerName(index)],
   20_000,
  );
  return out.trim() || "none";
 } catch {
  return "missing";
 }
}

/**
 * Healthcheck remediation lane. A container docker flags as `unhealthy` is
 * either handshake-less or has a dead route — neither is an upstream 429, so
 * the rotation budget below never touches it (`not an egress rate limit, no
 * reset`), which is how half a pool could sit "not ready" indefinitely.
 * Restart it, honouring RELAY_COOLDOWN_S per relay so a permanently broken
 * container cannot restart-loop the host.
 */
async function restartUnhealthyRelays(state) {
 const restarted = [];
 for (let index = 1; index <= COUNT; index++) {
  const health = await containerHealth(index);
  if (health !== "unhealthy") continue;
  const last = relayEntry(state, index).lastUnhealthyRestart;
  const waitLeftMs = last ? last + RELAY_COOLDOWN_S * 1000 - now() : 0;
  if (waitLeftMs > 0) {
   log(`#${index} unhealthy — last remediation restart ${Math.round((now() - last) / 1000)}s ago, waiting ${Math.ceil(waitLeftMs / 1000)}s`);
   continue;
  }
  log(`#${index} container healthcheck unhealthy — restarting (WireGuard handshake remediation)`);
  try {
   await rotateContainer(index, 1, "health");
   patchRelay(state, index, {
    lastUnhealthyRestart: now(),
    pendingVerify: true,
    probeAfter: now() + EGRESS_SETTLE_S * 1000,
   });
   restarted.push(index);
  } catch (err) {
   log(`#${index} health remediation restart failed: ${shortReason(err)}`);
  }
 }
 return restarted;
}

async function sweep() {
 const state = loadState();
 const models = PROBE_MODELS;
 log(`full sweep: ${COUNT} relay(s) x ${models.length} model(s) via ${RELAY_HOST}:${BASE_PORT} (lane header)`);

 const restartedUnhealthy = await restartUnhealthyRelays(state);
 if (restartedUnhealthy.length) {
  log(`restarted ${restartedUnhealthy.length} unhealthy relay(s): ${restartedUnhealthy.map((i) => `#${i}`).join(" ")}`);
 }

 const results = new Map();
 await runPool(
  Array.from({ length: COUNT }, (_, i) => i + 1),
  PROBE_CONCURRENCY,
  async (index) => {
   results.set(index, await probeRelay(index, models));
  },
 );

 // SWEEP_QUOTA_GUARD and its per-model 429 count are gone with probe-driven
 // rate-limit handling: the sweep no longer rotates on a 429, so there is
 // nothing for a quota guard to suppress here. The guard that still matters
 // lives on the signal path (see classifySignal).
 const cleanSet = new Set();
 // Answered 429: the lane is reachable, and the rate-limit verdict belongs to
 // the client that terminated the TLS (POST /__signal). Liveness, nothing more.
 const reachable = new Set();
 // Failed some other way (401/403/400/unreachable): troubled, no reset.
 const leftAlone = [];
 for (const index of [...results.keys()].sort((a, b) => a - b)) {
  const rows = results.get(index);
  const failing = rows.filter((r) => r.status !== 200 || r.serverError);
  if (failing.length === 0) {
   patchRelay(state, index, cleanPatch());
   cleanSet.add(index);
   continue;
  }
  // --sweep does not reset on a probe 429 any more. Rate-limit evidence comes
  // from the client that read the real status (POST /__signal → signals file →
  // --signals), so the sweep's job here is liveness only: a 429 probe proves the
  // lane is reachable, which is exactly what a probe can prove.
  for (const r of failing.filter((r) => r.status === 429)) {
   log(`#${index} ${r.model} 429 on probe — ignored (client reports rate limits)`);
  }
  const other = failing.filter((r) => r.status !== 429);
  if (other.length === 0) {
   // Every failing probe answered 429: the lane is reachable, and the
   // rate-limit verdict belongs to the client that terminated the TLS.
   reachable.add(index);
  } else {
   for (const r of other) {
    log(`#${index} ${r.model} ${r.status || "unreachable"} (${r.detail}) — not an egress rate limit, no reset`.slice(0, 200));
   }
   leftAlone.push(index);
  }
  // `consecutiveFailures` stays FLAT on both branches. It is the backoff
  // counter and the tier-2 trigger of the CLIENT path (classifySignal), so any
  // probe result counted in it would turn the first real client report into a
  // RELAY_BACKOFF_MAX_S lockout plus a Cloudflare registration. `dirty` +
  // `probeAfter` is all the sweep needs to probe the lane again later.
  patchRelay(state, index, {
   dirty: true,
   lastProbe: now(),
   pendingVerify: false,
   probeAfter: now() + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: relayEntry(state, index).consecutiveFailures,
  });
 }

 // The floor is a liveness floor: a lane answering 429 is serving, so it counts
 // toward it. Only lanes that failed some other way are degraded, because a
 // rate-limited lane is the client's report to rotate, not a pool fault the
 // sweep can diagnose any more.
 const alive = cleanSet.size + reachable.size;
 log(
  `sweep result: ${cleanSet.size}/${COUNT} relay(s) clean` +
   (reachable.size ? ` | ${reachable.size} rate-limited (client's verdict)` : "") +
   (leftAlone.length ? ` | troubled: ${leftAlone.map((i) => `#${i}`).join(" ")}` : "") +
   ` | safe floor ${SAFE_FLOOR}`,
 );
 if (cleanSet.size === 0 && reachable.size > 0) {
  log(`every lane answered 429: reachable, but no lane is known clean — the client's reports are what rotate them`);
 }
 const ok = alive >= SAFE_FLOOR;
 log(ok ? "pool is safe" : `pool is degraded: ${COUNT - alive} relay(s) not alive (floor ${SAFE_FLOOR})`);
 return ok;
}

/**
 * Smart targeted full reset of rate-limited relays until 200 OK.
 */
async function resetUntilClean() {
 const state = loadState();
 const targetModel = process.env.TARGET_MODEL ?? process.env.FRESH_IP_MODEL ?? PROBE_MODELS[0];

 // ── Preflight ──
 for (let i = 1; i <= COUNT; i++) {
  let hasWg0 = false;
  try {
   await docker(["exec", containerName(i), "test", "-f", "/etc/wireguard/wg0.conf"], 30_000);
   hasWg0 = true;
  } catch {
   hasWg0 = false;
  }
  if (!hasWg0) {
   log(`#${i} preflight failed: missing /etc/wireguard/wg0.conf`);
   return false;
  }
 }

 // ── Baseline Evaluation ──
 log(`evaluating pool baseline for ${COUNT} relay(s) on ${targetModel}...`);
 const cleanIndexes = [];
 const throttledIndexes = [];
 // Anything that is neither 200 nor 429 (401 auth, 403 gate, 400 protocol,
 // unreachable) is not an egress problem: a reset cannot fix it and would burn
 // a Cloudflare registration for nothing.
 const unusable = new Map();
 const allIndexes = Array.from({ length: COUNT }, (_, i) => i + 1);

 await runPool(allIndexes, PROBE_CONCURRENCY, async (index) => {
  const [res] = await probeRelay(index, [targetModel]);
  if (res.status === 200 && !res.serverError) {
   cleanIndexes.push(index);
  } else if (res.status === 429) {
   throttledIndexes.push(index);
  } else {
   unusable.set(index, `${res.status || "unreachable"} ${res.detail}`.slice(0, 120));
  }
 });

 let pending = throttledIndexes.sort((a, b) => a - b);
 log(`baseline: ${cleanIndexes.length}/${COUNT} clean, ${pending.length} rate-limited, ${unusable.size} not resettable`);
 for (const [index, why] of [...unusable.entries()].sort((a, b) => a[0] - b[0])) {
  log(`#${index} not an egress rate limit (${why}) — left alone`);
 }
 if (pending.length === 0) {
  log(unusable.size === 0 ? `all relays already clean on ${targetModel}` : `no rate-limited relay to reset on ${targetModel}`);
  return unusable.size === 0;
 }

 // ── Targeted Reset Rounds ──
 // OpenCode's free-tier throttle is scoped to one egress IP, so a relay that
 // answers 429 is not "quota capped" — it is sitting on a throttled address.
 // The only fix is a new egress: soft restart first (wg0.conf kept: same keys,
 // fresh WARP /64, zero Cloudflare registrations), and escalate to a full reset
 // (delete wg0.conf, new keypair, one registration) when the soft restart fails
 // to change the address or after SOFT_ROTATIONS_BEFORE_TIER2 soft attempts.
 // Rotation continues until every relay streams 200 OK or MAX_RESET_ATTEMPTS
 // runs out; a multi-hour retry-after applies to the address being abandoned,
 // never to the pool, so it never halts the loop.
 const softStreak = new Map();
 for (let attempt = 1; attempt <= MAX_RESET_ATTEMPTS && pending.length > 0; attempt++) {
  log(`round ${attempt}/${MAX_RESET_ATTEMPTS}: rotating ${pending.length} rate-limited relay(s)...`);
  const nextPending = [];

  for (const index of pending) {
   const oldIp = await getEgressIp(index);
   const streak = softStreak.get(index) ?? 0;
   let tier = streak >= SOFT_ROTATIONS_BEFORE_TIER2 ? 2 : 1;
   if (tier === 2 && poolRegistrationsWithinHour(state, now()).length >= MAX_POOL_REGISTRATIONS_PER_HOUR) {
    log(`[ceiling] Pool-wide registration ceiling reached (${MAX_POOL_REGISTRATIONS_PER_HOUR}/hour) — #${index} stays on soft restarts this round`);
    tier = 1;
   }

   let at = await rotateContainer(index, tier, attempt);
   let newIp = await waitForFreshIp(index, oldIp);

   // A soft restart that cannot move the egress is exactly the case the full
   // reset exists for: re-register the device and try again immediately.
   if (!newIp && tier === 1) {
    if (poolRegistrationsWithinHour(state, now()).length >= MAX_POOL_REGISTRATIONS_PER_HOUR) {
     log(`[ceiling] Pool-wide registration ceiling reached (${MAX_POOL_REGISTRATIONS_PER_HOUR}/hour) — #${index} cannot escalate to a full reset this round`);
    } else {
     log(`#${index} soft restart kept the same egress — escalating to a full reset (t2)`);
     tier = 2;
     at = await rotateContainer(index, tier, attempt);
     newIp = await waitForFreshIp(index, oldIp);
    }
   }

   if (!newIp) {
    log(`#${index} fresh IP verification failed (same IP or not ready)`);
    nextPending.push(index);
    softStreak.set(index, streak + 1);
    continue;
   }
   log(`#${index} fresh IP verified (changed from previous egress)`);
   const pfx = egressPrefix(newIp);
   if (isPrefixThrottled(pfx)) {
    log(`#${index} [attempt ${attempt}] prefix ${pfx} is in blacklist cache (throttled) — skipping probe and re-rolling`);
    nextPending.push(index);
    softStreak.set(index, streak + 1);
    continue;
   }

   patchRelay(state, index, {
    lastRotate: at,
    consecutiveFailures: 0,
    dirty: false,
    pendingVerify: false,
    // Only a full reset spends a Cloudflare registration; the hourly budget
    // counts registrations, so a soft restart must not consume one.
    ...(tier === 2 ? { rotations: [...relayEntry(state, index).rotations, at] } : {}),
   });

   const [res] = await probeRelay(index, [targetModel]);
   if (res.status === 200 && !res.serverError) {
    log(`#${index} got 200 streaming (CLEAN) on ${targetModel}`);
    softStreak.delete(index);
   } else if (res.status === 429) {
    rememberThrottledPrefix(pfx, res.retryAfter || 3600);
    log(
      `#${index}` + (res.retryAfter ? ` (retry-after: ${res.retryAfter}s)` : "") +
      ` — rotating again for a clean address (${res.fp})`,
    );
    nextPending.push(index);
    softStreak.set(index, tier === 1 ? streak + 1 : 0);
   } else {
    log(`#${index} now ${res.status || "unreachable"} on new egress (${res.detail}) — not an egress rate limit, no further resets`);
    unusable.set(index, `${res.status || "unreachable"}`);
   }
  }

  pending = nextPending;
 }

 // ── Summary & Exit ──
 const totalClean = COUNT - pending.length - unusable.size;
 log(`smart reset finished: ${totalClean}/${COUNT} relays clean, ${pending.length} still rate-limited, ${unusable.size} not resettable`);
 if (pending.length === 0 && unusable.size === 0) {
  log(`success: all relays streaming 200 OK (all clean) on ${targetModel} — reset complete`);
  return true;
 }
 return false;
}

// Tier-2 spends a Cloudflare registration. One per relay per TIER2_MIN_INTERVAL_S
// is the budget; the state file keeps a stuck relay from re-spending it on
// every timer tick.
function loadTier2State() {
 try {
  return JSON.parse(fs.readFileSync(TIER2_STATE_FILE, "utf8")) ?? {};
 } catch {
  return {};
 }
}

function saveTier2State(index, stamp) {
 try {
  fs.mkdirSync(path.dirname(TIER2_STATE_FILE), { recursive: true });
  // Concurrent rotations each hold a snapshot from pass start; merge against
  // the current file so a slow writer cannot erase another relay's stamp.
  const merged = { ...loadTier2State(), [index]: stamp };
  const tmp = `${TIER2_STATE_FILE}.${process.pid}.${index}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(merged));
  fs.renameSync(tmp, TIER2_STATE_FILE);
 } catch {
  // State is an optimization; never fail a pass over it.
 }
}

function tier2Allowed(index, tier2State = loadTier2State()) {
 if (TIER2_MIN_INTERVAL_S === 0) return true;
 return now() - Number(tier2State[index] ?? 0) >= TIER2_MIN_INTERVAL_S * 1000;
}

/** Run `worker` over `items` with bounded concurrency. */
async function runPool(items, limit, worker) {
 const queue = [...items];
 const runners = Array.from({ length: Math.min(limit, queue.length) }, async () => {
  while (queue.length > 0) {
   const item = queue.shift();
   if (item === undefined) return;
   await worker(item);
  }
 });
 await Promise.all(runners);
}

// ── Rotation budget ─────────────────────────────────────────────────
/** Exponential cooldown for the n-th consecutive failing batch. */
function effectiveCooldown(n) {
 const base = RELAY_BACKOFF_BASE_S * 1000 * 2 ** Math.max(0, n - 1);
 return Math.max(RELAY_COOLDOWN_S * 1000, Math.min(base, RELAY_BACKOFF_MAX_S * 1000));
}

function rotationsWithinHour(entry, at) {
 return entry.rotations.filter((ts) => at - ts < 3_600_000);
}
function poolRegistrationsWithinHour(state, at) {
 let count = 0;
 for (let i = 1; i <= COUNT; i++) {
  count += rotationsWithinHour(relayEntry(state, i), at).length;
 }
 return { length: count };
}


/**
 * The rotation gate: cooldown, minimum gap between rotations, settle window,
 * rolling-hour budget. `allowed: false` carries the exact reason the pass logs.
 */
function gate(entry, at) {
 const cooldown = entry.cooldownUntil - at;
 if (cooldown > 0) return { allowed: false, reason: `cooldown ${Math.ceil(cooldown / 1000)}s remaining` };
 const gap = RELAY_COOLDOWN_S * 1000 - (at - entry.lastRotate);
 if (gap > 0) return { allowed: false, reason: `cooldown ${Math.ceil(gap / 1000)}s remaining` };
 if (at < entry.lastRotate + EGRESS_SETTLE_S * 1000) return { allowed: false, reason: "settle window" };
 const recent = rotationsWithinHour(entry, at);
 if (recent.length >= RELAY_MAX_ROTATIONS_PER_HOUR) {
  return { allowed: false, reason: `hourly budget ${recent.length}/${RELAY_MAX_ROTATIONS_PER_HOUR}` };
 }
 return { allowed: true };
}

/**
 * Perform one rotation through the spacing chain, then record it. There is no
 * readiness wait and no verification probe here: the settle window plus
 * pendingVerify make a later pass the verifier.
 */
async function runRotation(job, state, dirty) {
 const container = containerName(job.index);
 // The lane's egress /64 changes under this restart: the pooled wreq session
 // would probe a dead egress. Drop it before any verification probe.
 closeWreqSession(job.index);
 const at = await enqueueRestart(async () => {
  const started = now();
  if (job.tier2) {
   let rekeyable = true;
   try {
    await docker(["exec", container, "test", "-f", "/etc/wireguard/wg0.conf"], 30_000);
   } catch {
    rekeyable = false;
   }
   if (!rekeyable) {
    log(`#${job.index} tier-2 aborted: no /etc/wireguard/wg0.conf — leaving registration untouched`);
    log(`#${job.index} rotating egress (t1) — restarting ${container}`);
    try {
     await docker(["restart", container], 60_000);
    } catch (e) {
     log(`#${job.index} restart failed: ${shortReason(e)}`);
    }
    return started;
   }
   log(`#${job.index} rotating egress (t2) — re-keying ${container}`);
   try {
    await docker(["exec", container, "rm", "-f", "/etc/wireguard/wg0.conf"], 30_000);
    await docker(["restart", container], 60_000);
   } catch (e) {
    log(`#${job.index} restart failed: ${shortReason(e)}`);
   }
   saveTier2State(job.index, started);
   return started;
  }
  log(`#${job.index} rotating egress (t1) — restarting ${container}`);
  try {
   await docker(["restart", container], 60_000);
  } catch (e) {
   log(`#${job.index} restart failed: ${shortReason(e)}`);
  }
  return started;
 });
 const entry = relayEntry(state, job.index);
 const patch = {
  lastRotate: at,
  dirty: true,
  pendingVerify: job.verify !== false,
  lastProbe: at,
  probeAfter: at + EGRESS_SETTLE_S * 1000,
  cooldownUntil: at + effectiveCooldown(job.n),
  rotations: [...entry.rotations, at],
  consecutiveFailures: job.n,
 };
 if (job.model) patch.last429 = { model: job.model, ts: at };
 patchRelay(state, job.index, patch);
 dirty.add(job.index);
}

// ── Suspect selection ───────────────────────────────────────────────
/**
 * Demand-driven suspect list: pendingVerify relays past their settle window,
 * dirty relays whose probeAfter expired, plus one round-robin audit sample of
 * clean relays. Baseline (no state yet) selects everything.
 */
function selectSuspects(state, { baseline, excluded }) {
 const at = now();
 const selected = new Set();
 const audit = [];
 if (baseline) {
  // A live signal already names a relay: probing it in the same pass would
  // waste a request and could race the restart it is about to get.
  for (let i = 1; i <= COUNT; i++) if (!excluded.has(i)) selected.add(i);
  return { selected, audit };
 }
 for (let i = 1; i <= COUNT; i++) {
  if (excluded.has(i)) continue;
  const e = relayEntry(state, i);
  if (e.pendingVerify && at >= e.lastRotate + EGRESS_SETTLE_S * 1000) {
   selected.add(i);
   continue;
  }
  if (e.dirty && at >= e.probeAfter) selected.add(i);
 }
 const cursor = Number.isInteger(state.auditCursor) ? state.auditCursor : 0;
 let nextCursor = cursor;
 for (let k = 1; k <= COUNT && audit.length < PROBE_AUDIT_SAMPLE; k++) {
  const index = ((cursor + k - 1) % COUNT) + 1;
  if (selected.has(index) || excluded.has(index)) continue;
  const e = relayEntry(state, index);
  if (e.dirty || e.pendingVerify) continue;
  if (at < e.lastRotate + EGRESS_SETTLE_S * 1000) continue;
  audit.push(index);
  selected.add(index);
  nextCursor = index;
 }
 if (audit.length) saveAuditCursor(nextCursor);
 return { selected, audit };
}

// ── Result handling ─────────────────────────────────────────────────
function cleanPatch() {
 return {
  lastProbe: now(),
  lastClean: now(),
  dirty: false,
  pendingVerify: false,
  consecutiveFailures: 0,
  cooldownUntil: 0,
  probeAfter: 0,
  // A relay that answers 200 on every probed model is no longer evidence for
  // the pool-wide quota history; leaving the stamp would cap a model for 900 s
  // after the pool recovered.
  last429: { model: "", ts: 0 },
 };
}

function markClean(ctx, index) {
 patchRelay(ctx.state, index, cleanPatch());
 ctx.dirty.delete(index);
}

/** Same-egress transient filter for server_error results, before the gate. */
async function reProbeServerErrors(index, failed) {
 let left = failed;
 for (let round = 0; round < SERVER_ERROR_RETRY && left.length > 0; round++) {
  if (SERVER_ERROR_REPROBE_MS > 0) await sleep(SERVER_ERROR_REPROBE_MS);
  const next = [];
  for (const r of left) {
   const again = await probe(index, r.model);
   if (again.status === 200 && !again.serverError) {
    log(`#${index} ${r.model} server_error transient - re-probe clean`);
   } else {
    next.push(again);
   }
  }
  left = next;
 }
 return left;
}

/** Classify one relay's probe batch: patch state, or queue a rotation. */
async function classifyRelay(index, results, ctx) {
 const at = now();
 const failing = results.filter((r) => r.status !== 200 || r.serverError);
 if (failing.length === 0) {
  markClean(ctx, index);
  return;
 }

 const capped = [];
 const srvErr = [];
 const gateway = [];
 const other = [];
 const throttled = [];
 for (const r of failing) {
  if (r.serverError) {
   if (ctx.serverErrCapped.has(r.model)) capped.push(r);
   else srvErr.push(r);
   continue;
  }
  // A probe that answers 429 is no longer evidence about the egress: upstream
  // throttles a (model, egress) bucket, and the probe's own shape is a guess at
  // real traffic. Rate-limit reporting belongs to the client that terminated
  // the TLS and read the real status — it POSTs /__signal and the relay names
  // the lane (see server.mjs handleSignal). Restarting a container on a probe
  // 429 churned WARP registrations to rediscover what the client already knew.
  if (r.status === 429) {
   throttled.push(r);
   continue;
  }
  if (r.status === 502) {
   gateway.push(r);
   continue;
  }
  other.push(r);
 }

 const seLeft = srvErr.length ? await reProbeServerErrors(index, srvErr) : [];
 for (const r of seLeft) {
  const evt = r.status === 200
   ? (/response\.failed/i.test(r.detail) ? "response.failed" : /failed to generate/i.test(r.detail) ? "failed to generate" : "server_error")
   : null;
  if (evt) log(`#${index} ${r.model} server_error in 200 envelope (SSE ${evt})`);
  else log(`#${index} ${r.model} server_error HTTP ${r.status}`);
 }
 if (seLeft.length) ctx.seRelays.set(index, seLeft.map((r) => r.model));

 const entry = relayEntry(ctx.state, index);
 // Probe-driven rotations never advance the client's failure ladder:
 // `consecutiveFailures` belongs to classifySignal (it sets the client's
 // cooldown and the tier-2 trigger), so a 502/server_error/slow rotation keeps
 // it flat. The probe's own retry pacing is `probeAfter` below.
 const n = entry.consecutiveFailures;
 const rotatable = [...seLeft, ...gateway];

 // Every probe was a 429: the client owns this verdict now, so leave the
 // container alone and just re-check later. Not `clean` — nothing proved the
 // egress healthy — and not a rotation, which is the churn this removes.
 if (rotatable.length === 0 && capped.length === 0 && other.length === 0 && throttled.length) {
  for (const r of throttled) log(`#${index} ${r.model} 429 on probe — ignored (client reports rate limits)`);
  ctx.dirty.add(index);
  patchRelay(ctx.state, index, {
   dirty: true,
   lastProbe: at,
   pendingVerify: false,
   probeAfter: at + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: entry.consecutiveFailures,
  });
  return;
 }

 if (rotatable.length === 0 && capped.length === 0 && other.length === 0) {
  // Only transient server_errors failed: the re-probe proved them gone.
  markClean(ctx, index);
  return;
 }

 // The host or the relay app is unhealthy: restarting WARP cannot help, so
 // this keeps the "cannot reach the relay" non-rotation path.
 if (other.length) {
  for (const r of other) log(`#${index} ${r.model} unexpected HTTP ${r.status || "unreachable"} — ${r.detail}`);
  ctx.dirty.add(index);
  patchRelay(ctx.state, index, {
   dirty: true,
   lastProbe: at,
   pendingVerify: false,
   probeAfter: at + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: n,
  });
  return;
 }

 if (rotatable.length === 0) {
  // Failing only on models the guards call upstream quota / incident.
  ctx.cappedOnly.push(index);
  ctx.dirty.add(index);
  patchRelay(ctx.state, index, {
   dirty: true,
   lastProbe: at,
   pendingVerify: false,
   probeAfter: at + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: n,
  });
  return;
 }

 // Retry-After scheduling used to live here for probe 429s. It is gone with
 // them: a client's report carries its own retry-after to /__signal, and the
 // relay's 429 cooldown is what honours it.

 const verdict = gate(entry, at);
 if (verdict.allowed) {
  ctx.dirty.add(index);
  // A probe-driven rotation is always tier 1. Tier 2 spends a Cloudflare
  // registration and a re-key is what crash-loops a container when CF throttles
  // registration, so the escalation ladder belongs to the path holding real
  // rate-limit evidence: classifySignal, on a client's report.
  ctx.rotations.push({
   index,
   n,
   tier2: false,
   model: seLeft[0]?.model ?? gateway[0]?.model ?? null,
  });
  return;
 }

 ctx.dirty.add(index);
 const patch = {
  dirty: true,
  lastProbe: at,
  pendingVerify: false,
  probeAfter: Math.max(entry.cooldownUntil, at + EGRESS_SETTLE_S * 1000),
  consecutiveFailures: n,
 };
 patchRelay(ctx.state, index, patch);
 log(`#${index} held: ${verdict.reason}`);
}

/** A live 429 signal: same gate, no probe. */
function classifySignal(index, model, ctx) {
 const at = now();
 const entry = relayEntry(ctx.state, index);
 const n = entry.consecutiveFailures + 1;
 // A model the guards call upstream quota/incident cannot be fixed by an
 // egress change: touching every signalled relay would be the exact churn
 // this scheduler exists to prevent.
 if (model && (ctx.quotaCapped.has(model) || ctx.serverErrCapped.has(model))) {
  ctx.cappedOnly.push(index);
  ctx.dirty.add(index);
  patchRelay(ctx.state, index, {
   dirty: true,
   pendingVerify: false,
   probeAfter: at + RELAY_COOLDOWN_S * 1000,
   consecutiveFailures: n,
   last429: { model, ts: at },
  });
  return;
 }
 const verdict = gate(entry, at);
 if (verdict.allowed) {
  ctx.dirty.add(index);
  ctx.rotations.push({
   index,
   n,
   tier2: entry.consecutiveFailures >= 2 && tier2Allowed(index),
   model: model || null,
  });
  return;
 }
 ctx.dirty.add(index);
 patchRelay(ctx.state, index, {
  dirty: true,
  pendingVerify: false,
  probeAfter: Math.max(entry.cooldownUntil, at + EGRESS_SETTLE_S * 1000),
  consecutiveFailures: n,
  ...(model ? { last429: { model, ts: at } } : {}),
 });
 log(`#${index} held: ${verdict.reason}`);
}

// ── One pass ────────────────────────────────────────────────────────
function countByModel(results, predicate) {
 const counts = new Map();
 for (const r of results) {
  if (!predicate(r)) continue;
  counts.set(r.model, (counts.get(r.model) ?? 0) + 1);
 }
 return counts;
}

async function pass() {
 passBrokenStateLogged = false;
 passWriteFailureLogged = false;

 const state = loadState();
 const baseline = Object.keys(state.relays).length === 0;
 const dirty = new Set();
 for (let i = 1; i <= COUNT; i++) if (relayEntry(state, i).dirty) dirty.add(i);

 // ── live signals (phase 2 input, off by default) ──
 const signalHot = new Map();
 if (LIVE_SIGNALS) {
  const signals = readSignals();
  if (signals) {
   for (const sig of signals) {
    const index = sig.relay;
    const entry = relayEntry(state, index);
    // A record older than the relay's own last restart describes an egress
    // that no longer exists: rotating on it would restart a relay that was
    // already rotated (and possibly re-verified clean) after that 429.
    if (!Number.isFinite(Number(sig.ts)) || Number(sig.ts) <= entry.lastRotate) continue;
    if (Number(sig.status) === 429) {
     const model = typeof sig.model === "string" ? sig.model : "";
     if (!signalHot.has(index)) signalHot.set(index, model);
     // Feed last429 so the quota guard sees live traffic too.
     patchRelay(state, index, { dirty: true, last429: { model, ts: now() } });
     dirty.add(index);
    } else {
     patchRelay(state, index, cleanPatch());
     dirty.delete(index);
    }
   }
  }
 }

 const { selected, audit } = selectSuspects(state, { baseline, excluded: new Set(signalHot.keys()) });
 const suspects = [...selected].sort((a, b) => a - b);
 log(`probing ${suspects.length} of ${COUNT} relay(s) x ${PROBE_MODELS.length} model(s) via ${RELAY_HOST}:${BASE_PORT} (lane header)${baseline ? " (baseline)" : ""}`);
 if (audit.length) log(`audit sample: ${audit.map((i) => `#${i}`).join(" ")}`);

 const probed = new Map();
 await runPool(suspects, PROBE_CONCURRENCY, async (index) => {
  probed.set(index, await probeRelay(index, PROBE_MODELS));
 });
 const all = [...probed.values()].flat();

 // Latency ranking on the speed model (200s only) — same prompt, same
 // minute, so spread here is egress quality, not upstream mood.
 const speedRows = all
  .filter((r) => r.model === SPEED_MODEL && r.status === 200 && !r.serverError)
  .sort((a, b) => a.ms - b.ms);
 if (speedRows.length) {
  log(`speed ${SPEED_MODEL}: ` + speedRows.map((r) => `#${r.index} ${r.ms}ms`).join(" "));
 }

 // Global-quota guard: from state history only (900 s window). Probe 429s are
 // deliberately NOT counted: a probe 429 is not evidence about the egress or
 // about upstream quota — the client owns that verdict (POST /__signal), and
 // letting probe results populate this set would let the prober veto a real
 // client report, which is the native 429 checker coming back in through the
 // side door. `last429` is written only by a rotation driven by a client
 // report, so the history below is client evidence.
 const observedSe = countByModel(all, (r) => r.serverError);
 const history429 = new Map();
 for (let i = 1; i <= COUNT; i++) {
  const e = relayEntry(state, i);
  if (!e.last429.model) continue;
  if (now() - e.last429.ts > 900_000) continue;
  history429.set(e.last429.model, (history429.get(e.last429.model) ?? 0) + 1);
 }
 const quotaCapped = new Set();
 // No PROBE_MODELS filter: live signals name the client's real models, and a
 // model capped on a majority of relays is upstream quota whichever lane
 // reported it.
 for (const [model, seen] of history429) {
  if (seen < GLOBAL_QUOTA_GUARD) continue;
  quotaCapped.add(model);
  log(`${model} throttled on ${Math.min(seen, COUNT)}/${COUNT} relays — upstream quota, skipping rotation for it this pass (fp=${PROBE_FINGERPRINT})`);
 }
 const serverErrCapped = new Set();
 for (const [model, n] of observedSe) {
  if (n < SERVER_ERROR_GUARD) continue;
  serverErrCapped.add(model);
  log(`${model} server_error on ${n}/${COUNT} relays - upstream incident, skipping rotation`);
 }

 const ctx = {
  state,
  dirty,
  quotaCapped,
  serverErrCapped,
  rotations: [],
  seRelays: new Map(),
  cappedOnly: [],
 };
 for (const [index, results] of [...probed.entries()].sort((a, b) => a[0] - b[0])) {
  await classifyRelay(index, results, ctx);
 }

 // Speed rotation stays opt-in: only when a fast sibling proves the slowness
 // is egress-side. Tier-1 only, no verification loop.
 if (SLOW_THRESHOLD_MS > 0 && speedRows.length) {
  const fastest = speedRows[0].ms;
  if (fastest > SLOW_THRESHOLD_MS) {
   log(`whole pool slow on ${SPEED_MODEL} (fastest ${fastest}ms) — upstream, no speed rotation`);
  } else {
   const slow = speedRows.filter((r) => r.ms > SLOW_THRESHOLD_MS && !dirty.has(r.index));
   if (slow.length) {
    log(`slow (>${SLOW_THRESHOLD_MS}ms, fastest #${speedRows[0].index} ${fastest}ms): ` + slow.map((r) => `#${r.index} ${r.ms}ms`).join(" "));
    for (const row of slow) {
     const entry = relayEntry(state, row.index);
     const at = now();
     // Latency is not client evidence: keep the client's failure ladder flat.
     const n = entry.consecutiveFailures;
     const verdict = gate(entry, at);
     dirty.add(row.index);
     if (verdict.allowed) {
      ctx.rotations.push({ index: row.index, n, tier2: false, model: null, verify: false });
     } else {
      patchRelay(state, row.index, {
       dirty: true,
       lastProbe: at,
       pendingVerify: false,
       probeAfter: Math.max(entry.cooldownUntil, at + EGRESS_SETTLE_S * 1000),
       consecutiveFailures: n,
      });
      log(`#${row.index} held: ${verdict.reason}`);
     }
    }
   }
  }
 }

 for (const [index, model] of [...signalHot.entries()].sort((a, b) => a[0] - b[0])) {
  classifySignal(index, model, ctx);
 }

 if (ctx.cappedOnly.length) {
  log(`throttled only on quota-capped models (left alone): ${ctx.cappedOnly.sort((a, b) => a - b).map((i) => `#${i}`).join(" ")}`);
 }

 await runPool(ctx.rotations, ROTATE_CONCURRENCY, (job) => runRotation(job, state, dirty));

 // ── summary ──
 const seDesc = [...ctx.seRelays.entries()]
  .sort((a, b) => a[0] - b[0])
  .map(([i, models]) => `#${i}(${models.join(",")})`)
  .join(" ");
 const cleanCount = COUNT - dirty.size;
 log(`clean ${cleanCount}/${COUNT} relay(s)`
  + (seDesc ? ` | server_error: ${seDesc}` : "")
  + (ctx.cappedOnly.length ? ` | quota-capped: ${ctx.cappedOnly.map((i) => `#${i}`).join(" ")}` : ""));
 if (dirty.size === 0) log("pool is cold");
 else log(`pool is not cold: ${dirty.size} relay(s) held or throttled`);

 // Exit 0 only when every relay is known clean: nothing dirty, nothing
 // quota-capped, nothing troubled, no rotation awaiting verification.
 return dirty.size === 0;
}

// ── Signals-only pass ───────────────────────────────────────────────
/**
 * Act on LIVE_SIGNAL_FILE records and nothing else: no probes, no audit
 * sample, no state scan. `pass()` can do this too, but only when
 * LIVE_SIGNALS=1 and only alongside a probe sweep it pays for; a consumer
 * that wants to react within a minute of a live 429 cannot afford that sweep.
 * Reads the file unconditionally — the flag exists for this mode.
 *
 * Exit 0 only when nothing is left dirty, matching `pass()`.
 */
async function signalsPass() {
 passBrokenStateLogged = false;
 passWriteFailureLogged = false;

 const state = loadState();
 const dirty = new Set();
 for (let i = 1; i <= COUNT; i++) if (relayEntry(state, i).dirty) dirty.add(i);

 // Same 900 s history window pass() uses: a model already throttled on a
 // guard's worth of relays is upstream quota, and restarting the one relay
 // this record names would be exactly the churn the guard exists to stop.
 const history429 = new Map();
 for (let i = 1; i <= COUNT; i++) {
  const e = relayEntry(state, i);
  if (!e.last429.model) continue;
  if (now() - e.last429.ts > 900_000) continue;
  history429.set(e.last429.model, (history429.get(e.last429.model) ?? 0) + 1);
 }
 const quotaCapped = new Set();
 for (const [model, n] of history429) {
  if (n < GLOBAL_QUOTA_GUARD) continue;
  quotaCapped.add(model);
  log(`${model} throttled on ${n}/${COUNT} relays — upstream quota, skipping rotation for it this pass`);
 }

 const ctx = {
  state,
  dirty,
  quotaCapped,
  serverErrCapped: new Set(),
  rotations: [],
  seRelays: new Map(),
  cappedOnly: [],
 };

 const signals = readSignals();
 const records = signals ?? [];
 for (const sig of [...records].sort((a, b) => a.relay - b.relay)) {
  const index = sig.relay;
  const entry = relayEntry(state, index);
  // A record older than the relay's own restart describes an egress that no
  // longer exists.
  if (!Number.isFinite(Number(sig.ts)) || Number(sig.ts) <= entry.lastRotate) continue;
  if (Number(sig.status) === 429) {
   // `retry-after` is deliberately not consulted. It describes the egress
   // being abandoned, and a WARP lane gets a fresh one from a restart — so a
   // short header is not a reason to keep a throttled lane in the pool. The
   // relay already holds the lane out of rotation for LANE_COOLDOWN_429_MS;
   // this pass is what puts a clean lane back behind it.
   const model = typeof sig.model === "string" ? sig.model : "";
   classifySignal(index, model, ctx);
  } else {
   patchRelay(state, index, cleanPatch());
   dirty.delete(index);
  }
 }

 if (ctx.cappedOnly.length) {
  log(`throttled only on quota-capped models (left alone): ${ctx.cappedOnly.sort((a, b) => a - b).map((i) => `#${i}`).join(" ")}`);
 }

 await runPool(ctx.rotations, ROTATE_CONCURRENCY, (job) => runRotation(job, state, dirty));

 if (dirty.size === 0) log("pool is cold");
 else log(`pool is not cold: ${dirty.size} relay(s) held or throttled`);
 return dirty.size === 0;
}

// ── Signals watcher ─────────────────────────────────────────────────
/**
 * --watch-signals: run the signals pass the moment a report lands.
 *
 * A timer can only be as fast as its period, and one minute is a long time on a
 * 2-3 lane pool: the lanes still serving are the only ones left, and the
 * restarted lane could already be back. So watch the signal file itself and
 * react on the write. `fs.watch` on a directory does not fire on every platform
 * for a rename, so a poll runs alongside it; the poll is what makes this
 * portable, the watch is what makes it immediate.
 *
 * Fire-and-forget by design: a burst of reports collapses into one pass
 * (debounced), and a running worker is never doubled — a fleet-wide 429 must not
 * start ten restarts at once.
 *
 * Each trigger spawns the SAME `--signals` worker the timer used to run, under
 * the SAME `/tmp/warp-keep-cold.lock`. That is deliberate: this process must not
 * hold the sweep's lock for its own lifetime (a long-running unit holding it
 * would make every full sweep skip), and the one-pass code path stays the only
 * implementation. The cost is a process spawn per report, which is noise next
 * to the container restart that follows.
 */
async function watchSignals() {
 const dir = path.dirname(path.resolve(LIVE_SIGNAL_FILE));
 const DEBOUNCE_MS = envInt("WATCH_DEBOUNCE_MS", 250, { min: 0 });
 const POLL_MS = envInt("WATCH_POLL_MS", 2_000, { min: 250 });
 const LOCK_FILE = "/tmp/warp-keep-cold.lock";
 const script = path.resolve(process.argv[1]);
 let running = null;
 let queued = false;
 let timer = null;

 // One worker at a time: the file is consumed destructively, so two passes
 // racing would split the reports between them.
 function runOnceNow(why) {
  if (running) {
   queued = true;
   return running;
  }
  running = (async () => {
   log(`signals watch: ${why}`);
   const child = spawn(
    // flock where it exists: a pass that collides with a sweep is a skip, not a
    // second concurrent rotation. Windows (the test harness) has no flock.
    process.platform === "win32" ? process.execPath : "/usr/bin/flock",
    process.platform === "win32"
      ? [script, "--signals"]
      : ["-n", "-E", "0", LOCK_FILE, process.execPath, script, "--signals"],
    { cwd: process.cwd(), env: process.env, stdio: ["ignore", "inherit", "inherit"] },
   );
   await new Promise((resolve) => {
    child.once("error", (e) => {
     log(`signals worker failed to start: ${shortReason(e).slice(0, 200)}`);
     resolve();
    });
    child.once("exit", (code) => {
     // flock's -E 0 means "skipped, the sweep holds the lock" — expected, not
     // an error, and the next trigger picks the record up.
     if (code !== 0 && code !== 1) log(`signals worker exited ${code}`);
     resolve();
    });
   });
  })();
  return running.then(() => {
   running = null;
   if (queued) {
    queued = false;
    schedule("queued report");
   }
  });
 }

 function schedule(why) {
  if (timer !== null) return;
  timer = setTimeout(() => {
   timer = null;
   void runOnceNow(why);
  }, DEBOUNCE_MS);
  timer.unref?.();
 }

 log(`signal watch: ${LIVE_SIGNAL_FILE} (debounce ${DEBOUNCE_MS}ms, poll ${POLL_MS}ms)`);
 let watcher = null;
 try {
  watcher = fs.watch(dir, (_event, name) => {
   if (!name || String(name) === path.basename(LIVE_SIGNAL_FILE)) schedule("file change");
  });
  watcher.on("error", (err) => log(`signal watch: watch failed (${shortReason(err)}) — polling only`));
 } catch (err) {
  // A missing directory or an unsupported platform: the poll below still works.
  log(`signal watch: cannot watch ${dir} (${shortReason(err)}) — polling only`);
 }
 const poll = setInterval(() => schedule("poll"), POLL_MS);
 poll.unref?.();

 // Ctrl-C / SIGTERM must not leave a restart half-done: nothing is awaited past
 // its own rotation, and every write is already atomic (tmp + rename).
 const stop = () => {
  clearInterval(poll);
  watcher?.close?.();
  process.exitCode = 0;
 };
 process.once("SIGINT", stop);
 process.once("SIGTERM", stop);
 // Keep the process alive with no other handles (both are unref'd above).
 await new Promise(() => {});
}

// ── Entry point ─────────────────────────────────────────────────────
if (PROBE_ONLY) {
 const ok = await probePool();
 process.exitCode = ok ? 0 : 1;
} else if (WATCH_SIGNALS) {
 await watchSignals();
} else if (SIGNALS) {
 const ok = await signalsPass();
 process.exitCode = ok ? 0 : 1;
} else if (SWEEP) {
 const ok = await sweep();
 process.exitCode = ok ? 0 : 1;
} else if (RESET_UNTIL_CLEAN) {
 const ok = await resetUntilClean();
 process.exitCode = ok ? 0 : 1;
} else if (FRESH_IP_SWEEP) {
 const ok = await freshIpSweep();
 process.exitCode = ok ? 0 : 1;
} else if (LOOP) {
 log(`loop mode: pass every ${INTERVAL_S}s (Ctrl-C to stop)`);
 for (; ;) {
  const started = Date.now();
  try {
   await pass();
  } catch (e) {
   log(`pass failed: ${shortReason(e).slice(0, 200)}`);
  }
  // Fixed deadline: a slow pass must not push every later pass.
  await new Promise((r) => setTimeout(r, Math.max(0, started + INTERVAL_S * 1_000 - Date.now())));
 }
} else {
 const ok = await pass();
 // exitCode lets stdout drain (process.exit() can drop the final lines).
 process.exitCode = ok ? 0 : 1;
}
