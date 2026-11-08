// Lane pool for the single-URL L7 router: picks which relay egress serves a
// request, keeps session stickiness, and owns the two-lane cooldown model
// lifted from mux.mjs (probeOutUntil vs cool429Until).
//
// - sticky: LANE key -> lane, no TTL, LRU-capped at stickyMax. Session
//   headers beat the peer address so one client keeps one /64.
// - usable() = !sick && now >= probeOutUntil && now >= cool429Until.
// - pick() returns null when nothing is usable; the caller owns the 503.
import http from "node:http";
import { Readable } from "node:stream";

// Lane targeting for keep-cold, the only caller allowed to name an egress.
//
// A probe that cannot address a specific lane cannot tell which lane it
// measured, so a dead lane reads healthy forever. Two forms, both gated on the
// probe marker and, for userinfo, on real credentials — otherwise any client
// could pin an egress and defeat the spread. Returns a Lane or null; a null
// means "not pinned", never "no lane".
export function resolvePinnedLane({ headers = {}, proxyUser, proxyPass, lanes, proxyAuth } = {}) {
  const h = headers || {};
  if (h["x-keep-cold-probe"] === undefined) return null;
  const all = Array.isArray(lanes) ? lanes : lanes?.lanes;
  if (!Array.isArray(all) || all.length === 0) return null;

  // Digits only: "01", " 2", "2.5", "1e1" are all typos, not lane 2.
  const byIndex = (v) => {
    if (typeof v !== "string" || !/^[1-9]\d*$/.test(v)) return null;
    const n = Number(v);
    if (!Number.isSafeInteger(n) || n > all.length) return null;
    return all.find((l) => l?.n === n) ?? null;
  };

  const fromHeader = byIndex(h["x-relay-lane"]);
  if (fromHeader) return fromHeader;

  // Userinfo form: proxy-auth user "lane-N" plus the password half of the
  // configured credential. No configured credential means no pin.
  const m = /^lane-(\d+)$/.exec(String(proxyUser ?? ""));
  if (!m) return null;
  if (typeof proxyAuth !== "string" || proxyAuth === "") return null;
  const colon = proxyAuth.indexOf(":");
  const wantPass = colon < 0 ? "" : proxyAuth.slice(colon + 1);
  if (wantPass === "" || proxyPass !== wantPass) return null;
  return byIndex(m[1]);
}

export function laneKey(headers = {}, peerAddress = "") {
  const h = headers || {};
  for (const name of ["x-opencode-session", "x-session-affinity", "x-session-id"]) {
    const v = h[name];
    if (typeof v === "string" && v.trim()) return v.trim();
  }
  return String(peerAddress || "").trim() || "unknown";
}

// Should the HTTP attempt loop try another lane? Pure predicate, no I/O.
// Order matters: a pre-response failure is retryable even when the body was
// streamed, because no request reached the upstream; once a body was forwarded
// untouched, only a status we can replay without the body counts.
export function retryable({ method, status, error, replayable } = {}) {
  if (error) return true;
  if (replayable === false) return false;
  if (status === 429) return true;
  if (status === 502 || status === 503 || status === 504) {
    return method === "GET" || method === "HEAD";
  }
  return false;
}

// Buffer a small request body so a retry can replay it; a body past the cap is
// forwarded as a stream and never retried. Reads the un-read portion of req
// itself, so a Content-Length and a chunked body take the same path.
// ponytail: replay is memory-bound — upgrade by spilling to a temp file if
// large replayable bodies ever matter.
export async function readReplayableBody(req, maxBytes) {
  // Tolerate a plain Readable (no .method): a missing method means "has a body".
  if (req.method === "GET" || req.method === "HEAD") return { replayable: true, body: null };

  // A NaN/undefined cap would make every `size > maxBytes` false, i.e. unbounded.
  if (!Number.isFinite(maxBytes)) maxBytes = 1_048_576;
  else maxBytes = Math.max(0, Math.floor(maxBytes));

  const it = req[Symbol.asyncIterator]();
  const chunks = [];
  let size = 0;
  for (;;) {
    const step = await it.next(); // rejects if the client dies mid-read
    if (step.done) return { replayable: true, body: Buffer.concat(chunks) };

    const buf = Buffer.isBuffer(step.value) ? step.value : Buffer.from(step.value);
    chunks.push(buf);
    size += buf.length;
    if (size > maxBytes) {
      const prefix = Buffer.concat(chunks);
      const body = Readable.from(
        (async function* () {
          yield prefix;
          yield* it; // drain the rest without buffering it
        })()
      );
      // The generator is suspended in it.next(), so destroying `body` alone never
      // reaches req — the request would stay open for the whole keep-alive window.
      body.once("close", () => {
        if (!req.destroyed) req.destroy();
      });
      return { replayable: false, body };
    }
  }
}

// Real health path: the same GET / a client's lane would answer.
function defaultProbeFactory() {
  const port = parseInt(process.env.BASE_PORT ?? "", 10) || 3000;
  return (lane) =>
    new Promise((resolve) => {
      const req = http.get(
        {
          host: "127.0.0.1",
          port,
          path: "/",
          timeout: 8_000,
          headers: { "x-relay-lane": String(lane.n), "x-keep-cold-probe": "1" },
        },
        (res) => {
          res.resume();
          resolve(res.statusCode === 200);
        }
      );
      req.on("timeout", () => req.destroy(new Error("probe timeout")));
      req.on("error", () => resolve(false));
    });
}

export function createLanes({
  count,
  agentFor,
  probe = defaultProbeFactory(),
  probeIntervalMs = 30_000,
  cooldownMs = 240_000,
  cooldown429Ms = 120_000,
  stickyMax = 10_000,
  log = (msg) => console.log(`[router] ${msg}`),
} = {}) {
  if (!Number.isInteger(count) || count < 1) throw new Error("createLanes: count required");
  if (typeof agentFor !== "function") throw new Error("createLanes: agentFor required");
  if (!Number.isInteger(stickyMax) || stickyMax < 1) {
    // A non-positive cap would hang the eviction loop or silently disable
    // affinity; reject the typo at construction instead.
    throw new Error("createLanes: stickyMax must be a positive integer");
  }

  const lanes = Array.from({ length: count }, (_, i) => {
    const n = i + 1;
    return {
      n,
      // Derived here so Lane.host/port are always right for the CONNECT path.
      host: process.env[`WARP_HOST_${n}`] || `warp-${n}`,
      port: process.env[`WARP_PORT_${n}`] || "1080",
      agent: agentFor(n),
      sick: false,
      probeOutUntil: 0,
      cool429Until: 0,
      inflight: 0,
    };
  });
  const byN = new Map(lanes.map((l) => [l.n, l]));
  const sticky = new Map(); // laneKey -> lane, insertion order = LRU order
  const probing = new Set();
  const timers = [];
  let rr = 0;

  const usable = (l) => !l.sick && Date.now() >= l.probeOutUntil && Date.now() >= l.cool429Until;

  function bindSticky(key, lane) {
    if (!key) return;
    sticky.delete(key); // refresh LRU position
    sticky.set(key, lane);
    while (sticky.size > stickyMax) sticky.delete(sticky.keys().next().value);
  }

  function bind(key, lane) {
    bindSticky(key, lane);
  }

  function dropSticky(n) {
    for (const [key, lane] of sticky) if (lane.n === n) sticky.delete(key);
  }

  function pick(key) {
    const held = key ? sticky.get(key) : undefined;
    if (held && usable(held)) {
      bindSticky(key, held);
      return held;
    }
    for (let i = 0; i < count; i++) {
      rr = (rr % count) + 1;
      const lane = byN.get(rr);
      if (usable(lane)) {
        bindSticky(key, lane);
        return lane;
      }
    }
    // Every lane is parked. A 429 cooldown is a throttle on the egress IP, not
    // a dead lane, so serving through one is strictly better than answering
    // 503 to the whole fleet for the rest of the window. Lanes parked by a
    // failed probe stay excluded — that really is a broken egress.
    for (let i = 0; i < count; i++) {
      rr = (rr % count) + 1;
      const lane = byN.get(rr);
      if (!lane.sick && Date.now() >= lane.probeOutUntil) {
        bindSticky(key, lane);
        return lane;
      }
    }
    return null;
  }

  function cool(lane, ms, why, kind = "probe") {
    // Two independent lanes: a signal cooldown never touches the probe lane.
    const field = kind === "signal" ? "cool429Until" : "probeOutUntil";
    lane[field] = Math.max(lane[field], Date.now() + ms);
    dropSticky(lane.n);
    log(`lane #${lane.n}: cooldown ${Math.round(ms / 1000)}s (${why})`);
  }

  // A pass clears ONLY the probe-owned lane; a fail parks the lane for cooldownMs.
  async function runProbe(lane) {
    if (probing.has(lane.n)) return;
    probing.add(lane.n);
    let ok = false;
    try {
      ok = await probe(lane);
    } catch {
      ok = false;
    } finally {
      probing.delete(lane.n);
    }
    if (ok) {
      if (lane.sick) log(`lane #${lane.n}: probe OK, re-admitted`);
      lane.sick = false;
      lane.probeOutUntil = 0;
    } else {
      lane.sick = true;
      cool(lane, cooldownMs, "probe failed");
    }
  }

  // Stagger probes so N lanes never share one alarm tick.
  function start() {
    if (!Number.isFinite(probeIntervalMs) || probeIntervalMs <= 0) return;
    lanes.forEach((lane, i) => {
      const timer = setInterval(() => runProbe(lane), probeIntervalMs);
      timer.unref?.();
      const first = setTimeout(() => runProbe(lane), (i * probeIntervalMs) / count);
      first.unref?.();
      timers.push(timer, first);
    });
  }

  function stop() {
    for (const t of timers.splice(0)) {
      clearInterval(t);
      clearTimeout(t);
    }
  }

  return {
    lanes,
    pick,
    bind,
    cool,
    start,
    stop,
    stickySize: () => sticky.size,
  };
}
