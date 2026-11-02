// All-in-one mux listener: ONE client URL that internally spreads across
// every relay instance's already-running per-port listener (basePort+1 ..
// basePort+COUNT, the same listeners server.mjs creates).
//
// Design (kept small on purpose):
// - Pure TCP switcher. Allowlist, token gate, CONNECT, streaming, and the
//   health endpoint all stay owned by the per-instance handlers; the mux
//   only chooses which listener forwards a fresh connection.
// - Backend health = probe each instance's health endpoint ("GET /" — the
//   same path its no-target handler serves) on a staggered interval, plus a
//   short cooldown on listener-level connect failure. Egress-level deaths
//   surface as a failed probe (health walks the real SOCKS egress).
// - Live 429s: with LIVE_SIGNALS=1 the mux also reads the signal file every
//   watch tick and cooldowns a relay that just answered 429, so traffic
//   hops to a sibling egress while keep-cold does its slower repair.
// - Stickiness: by TCP peer address only, so repeat connections from one
//   client keep one `/64` (OpenCode free tier counts per egress). A client
//   that needs true request-level affinity needs a header-aware mux — see
//   the ponytail note below.
// ponytail: session affinity is per client-IP, not per API key/conversation;
// upgrade path: parse the first bytes client-side and hash authorization +
// model, which requires moving this upstream of the per-instance dispatch.
// ponytail: the 429 watcher clears nothing on a 200 recovery record; cooldowns
// are minutes-long by design and keep-cold (5-min sweeps) owns real repair,
// so a stale two-minute cooldown self-expires before its sweep re-rotates.
import net from "node:net";
import http from "node:http";
import fs from "node:fs";

function intEnv({ raw, fallback, min, max } = {}) {
  const n = parseInt(raw ?? "", 10);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(min ?? 0, Math.min(max ?? Number.MAX_SAFE_INTEGER, n));
}

export function startMux({
  basePort,
  count,
  bind = "0.0.0.0",
  // Mux listens on basePort itself: callers publish host basePort -> this
  // port; instance listeners keep untouched ports basePort+1..+COUNT.
  signalFile = null,
  signalWatchMs = 30_000,
  signalWindowMs = 120_000,
  signalCooldownMaxMs = 30 * 60_000,
  probeIntervalMs = 30_000,
  probeTimeoutMs = 8_000,
  cooldownMs = 240_000,
  cooldown429Ms = 120_000,
  stickyMs = 600_000,
  log = (msg) => console.log(`[mux] ${msg}`),
} = {}) {
  if (!Number.isInteger(basePort) || basePort < 1) throw new Error("mux: basePort required");
  if (!Number.isInteger(count) || count < 1) throw new Error("mux: count required");

  const backends = Array.from({ length: count }, (_, i) => ({
    n: i + 1,
    port: basePort + i + 1,
    // Unknown health until the first probe: eligible by default so a pool
    // that boots healthy serves traffic immediately; a failed probe flips
    // it out until the next passing probe.
    sick: false,
    // Two cooldown lanes so one lane never erases the other: a passing
    // probe clears only probeOut (its own failure state), a 429 signal
    // sets cool429 which decays on its own clock.
    probeOutUntil: 0,
    cool429Until: 0,
    probing: false,
  }));
  const sticky = new Map(); // peer address -> { n, until }
  let rr = 0;

  const usable = (b) => !b.sick && Date.now() >= b.probeOutUntil && Date.now() >= b.cool429Until;

  function pick(peerKey) {
    if (stickyMs > 0 && peerKey) {
      const held = sticky.get(peerKey);
      if (held) {
        const b = backends.find((x) => x.n === held.n);
        if (b && Date.now() < held.until && usable(b)) return b;
        sticky.delete(peerKey);
      }
    }
    for (let i = 0; i < count; i++) {
      rr = (rr % count) + 1;
      const b = backends[rr - 1];
      if (usable(b)) return b;
    }
    return null;
  }

  function dropSticky(n) {
    for (const [key, held] of sticky) {
      if (held.n === n || Date.now() >= held.until) sticky.delete(key);
    }
  }

  function cool(b, ms, why, kind = "probe") {
    const field = kind === "signal" ? "cool429Until" : "probeOutUntil";
    b[field] = Math.max(b[field], Date.now() + ms);
    dropSticky(b.n);
    log(`relay #${b.n}: cooldown ${Math.round(ms / 1000)}s (${why})`);
  }

  // Probe the real health path of instance N. A pass clears sick + cooldown;
  // a fail parks the relay for cooldownMs.
  function probe(b) {
    if (b.probing) return;
    b.probing = true;
    let done = false;
    const finish = (ok) => {
      if (done) return;
      done = true;
      b.probing = false;
      if (ok) {
        if (b.sick) log(`relay #${b.n}: probe OK, re-admitted`);
        // Clear ONLY the probe-owned lane: a passing health check says
        // nothing about a 429 signal that is still within its window.
        b.sick = false;
        b.probeOutUntil = 0;
      } else {
        b.sick = true;
        cool(b, cooldownMs, "probe failed");
      }
    };
    try {
      const req = http.get(
        { host: "127.0.0.1", port: b.port, path: "/", timeout: probeTimeoutMs },
        (res) => {
          res.resume();
          finish(res.statusCode === 200);
        }
      );
      req.on("timeout", () => req.destroy(new Error("probe timeout")));
      req.on("error", () => finish(false));
    } catch {
      finish(false);
    }
  }

  // Stagger probes so N instances never share one alarm tick.
  const probeTimers = [];
  function startProbes() {
    if (!Number.isFinite(probeIntervalMs) || probeIntervalMs <= 0) return;
    backends.forEach((b, i) => {
      const timer = setInterval(() => probe(b), probeIntervalMs);
      timer.unref?.();
      // First pass fires after i * interval / count so startup probes fan out.
      const first = setTimeout(() => probe(b), (i * probeIntervalMs) / count);
      first.unref?.();
      probeTimers.push(timer, first);
    });
  }

  // 429 signals: read-only. A recent 429 record cooldowns that relay for
  // retry-after (clamped) or the default window; recovery records ignored.
  let lastSignalTs = 0;
  function watchSignals() {
    if (!signalFile) {
      log("signals: disabled (LIVE_SIGNALS=0) — probing only");
      return;
    }
    if (!Number.isFinite(signalWatchMs) || signalWatchMs <= 0) return;
    const timer = setInterval(() => {
      let signals;
      try {
        const parsed = JSON.parse(fs.readFileSync(signalFile, "utf8"));
        signals = Array.isArray(parsed?.signals) ? parsed.signals : [];
      } catch {
        return; // Missing/corrupt file: keep last route table, try next tick.
      }
      const cutoff = Date.now() - signalWindowMs;
      for (const rec of signals) {
        if (!rec || rec.status !== 429) continue;
        if (rec.ts <= cutoff) continue;
        if (rec.ts <= lastSignalTs) continue; // Already-actioned record.
        lastSignalTs = rec.ts;
        const b = backends.find((x) => x.n === rec.relay);
        if (!b || !usable(b)) continue;
        const byRetryAfter = Number(rec.retryAfter) * 1000;
        const base429 = Number.isFinite(cooldown429Ms) && cooldown429Ms > 0 ? cooldown429Ms : cooldownMs;
        const ms = Number.isFinite(byRetryAfter) && byRetryAfter > 0
          ? Math.min(Math.max(byRetryAfter, 5_000), signalCooldownMaxMs)
          : base429;
        cool(b, ms, "429 signal", "signal");
      }
    }, signalWatchMs);
    timer.unref?.();
    log(`signals: watching ${signalFile} every ${Math.round(signalWatchMs / 1000)}s`);
  }

  const closeTarget = (socket) => {
    try {
      socket.destroy();
    } catch {
      // Already gone.
    }
  };

  const server = net.createServer((client) => {
    client.on("error", () => {});
    const peerKey = stickyMs > 0 ? `${client.remoteAddress || ""}` : "";
    // Pick BEFORE relaying any byte: a mis-pick never leaks the client's
    // CONNECT/absolute-form into a dying pool member.
    const b = peerKey ? pick(peerKey) : pick("");
    if (!b) {
      try {
        client.end("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nRetry-After: 30\r\n\r\n");
      } catch {
        // Ignored: client is gone.
      }
      return;
    }
    if (peerKey) sticky.set(peerKey, { n: b.n, until: Date.now() + stickyMs });

    const upstream = net.connect({ host: "127.0.0.1", port: b.port });
    upstream.on("error", () => {
      // Listener-level failure: whole relay process suspect. The request is
      // still unanswered (we wrote no 200), so answer it now.
      cool(b, cooldownMs, "backend connect failed");
      try {
        client.end("HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n");
      } catch {
        // Ignored.
      }
      closeTarget(client);
    });
    client.pipe(upstream);
    upstream.pipe(client);
    const cleanup = () => {
      closeTarget(client);
      closeTarget(upstream);
    };
    client.once("close", cleanup);
    upstream.once("close", cleanup);
  });

  server.on("error", (err) => {
    log(`fatal listen error: ${err?.stack ?? String(err)}`);
    // Match the per-instance contract: a half-started relay is invisible to
    // `restart: always`, so a failed bind must kill the process.
    process.exit(1);
  });

  server.listen(basePort, bind, () => {
    log(`listening on ${bind}:${basePort} -> relays #1..#${count} (127.0.0.1:${basePort + 1}..${basePort + count})`);
  });

  startProbes();
  watchSignals();

  return { server, backends, _pick: pick, _dropSticky: dropSticky };
}
