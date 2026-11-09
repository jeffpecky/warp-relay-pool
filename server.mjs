import http from "node:http";
import net from "node:net";
import tls from "node:tls";
import { createHash, timingSafeEqual } from "node:crypto";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { Agent } from "undici";
import { createSignalWriter } from "./signal-writer.mjs";
import { createLanes, laneKey, readReplayableBody, resolvePinnedLane, retryable } from "./router.mjs";

const COUNT = parseInt(process.env.RELAY_COUNT || "10", 10);
// Per-process in-flight cap. Streaming responses hold sockets for minutes, and
// the relay intentionally has no request timeout, so unbounded fan-in is the
// one way this process can exhaust memory. 0 disables the cap.
const MAX_INFLIGHT = parseInt(process.env.RELAY_MAX_INFLIGHT || "256", 10);
const BASE_PORT = parseInt(process.env.BASE_PORT || "3000", 10);
const HEADER_TARGET = "x-relay-target";
const HEADER_PATH = "x-relay-path";
// keep-cold's probes carry this: they must never appear as live-traffic
// signals, or a probe's own 429 would be replayed to the next pass.
const HEADER_PROBE = "x-keep-cold-probe";
const HEADER_AUTH = "x-relay-auth";
const DEFAULT_ALLOWED_TARGETS = "https://opencode.ai,https://api.kilo.ai,https://oai.endpoints.kepler.ai.cloud.ovh.net";
const ALLOWED_TARGETS = (process.env.RELAY_ALLOWED_TARGETS ?? DEFAULT_ALLOWED_TARGETS)
  .split(",")
  .map((s) => s.trim().replace(/\/$/, ""))
  .filter(Boolean);
// Shared token for wildcard/regex dispatches. Exact-origin hits never need
// `x-relay-auth` and the health path is exempt. Targets matched ONLY via
// wildcard ("*"/"*.suffix") or "re:" entries require
// `x-relay-auth: <token>` (401 otherwise) — including when no token is
// configured, so a wildcard/regex entry without RELAY_AUTH_TOKEN fails
// closed instead of becoming an open proxy.
const RELAY_AUTH_TOKEN = process.env.RELAY_AUTH_TOKEN || "";

// Direct public-IP ingress (RELAY_DIRECT_PUBLIC=1 + RELAY_PROXY_AUTH=user:pass):
// a standard-proxy Basic gate enforced on EVERY dispatched request and every
// CONNECT, exact-origin hits included. Unlike `x-relay-auth` this is
// speakable by ordinary proxy clients (curl -U, browsers, undici ProxyAgent
// reads `http://user:pass@host` userinfo), which is why the public arm uses
// it. The health path (no target) stays exempt: keep-cold probes through it,
// and it answers only relay metadata, never upstream data.
// The public arm means credentials cross an untrusted network, so compare them
// in constant time: hash both sides to a fixed 32 bytes first (timingSafeEqual
// throws on unequal lengths, which would itself leak the secret's length).
const RELAY_PROXY_AUTH = process.env.RELAY_PROXY_AUTH || "";
const PROXY_AUTH_CHALLENGE = 'Basic realm="warp-relay-pool"';

function secretEquals(given, expected) {
  const a = createHash("sha256").update(String(given)).digest();
  const b = createHash("sha256").update(String(expected)).digest();
  return timingSafeEqual(a, b);
}

// Parse the relay's own Basic gate once: the connect path and the request path
// both need the decoded credentials, not just a yes/no.
//
// Three accepted shapes, all requiring the same password half:
//   "user:pass"          the exact configured credential
//   "lane-N:pass"        a keep-cold probe pinning the lane it measures
//   "c-<id>:pass"        a client naming itself so its tunnels share one lane
// The two non-exact shapes are not a weaker gate: the password half must still
// match exactly, and a caller who has it could already send the exact shape.
function proxyAuthInfo(req) {
  if (!RELAY_PROXY_AUTH) return { ok: true, user: null, pass: null, clientId: null };
  const header = String(req.headers["proxy-authorization"] || "");
  const space = header.indexOf(" ");
  if (!/^basic$/i.test(header.slice(0, space).trim())) return { ok: false, user: null, pass: null, clientId: null };
  const decoded = Buffer.from(header.slice(space + 1), "base64").toString("utf8");
  const colon = decoded.indexOf(":");
  const user = colon < 0 ? decoded : decoded.slice(0, colon);
  const pass = colon < 0 ? "" : decoded.slice(colon + 1);
  const wantPass = RELAY_PROXY_AUTH.includes(":")
    ? RELAY_PROXY_AUTH.slice(RELAY_PROXY_AUTH.indexOf(":") + 1)
    : "";
  const laneForm = /^lane-\d+$/.test(user) && wantPass !== "" && secretEquals(pass, wantPass);
  const clientForm = CLIENT_ID_RE.test(user) && wantPass !== "" && secretEquals(pass, wantPass);
  if (!laneForm && !clientForm && !secretEquals(decoded, RELAY_PROXY_AUTH)) {
    return { ok: false, user: null, pass: null, clientId: null };
  }
  return { ok: true, user, pass, clientId: clientForm ? user : null };
}

// A missing or non-numeric knob takes the documented default; so does an
// out-of-range one, except where 0 is meaningful (0 retries, no probes).
// `passthrough` hands the raw value to the callee's own validation instead:
// createLanes must see a bad stickyMax so a typo stops the container.
function laneEnv(name, fallback, { allowZero = false, passthrough = false } = {}) {
  const n = parseInt(process.env[name] ?? "", 10);
  if (!Number.isFinite(n)) return fallback;
  if (passthrough) return n;
  if (allowZero) return n <= 0 ? 0 : n;
  return n < 1 ? fallback : n;
}

// Client-reported rate limits. The relay cannot see upstream status on a
// CONNECT tunnel — the request is TLS to the origin and this process only
// forwards bytes — so the only process that knows a 429 happened is the client
// that terminated that TLS itself. It reports out of band here, and the relay
// resolves WHICH egress to condemn: the client names itself, never a lane.
//
// No token means the route does not exist (404), so an install that does not
// report exposes no new surface. The token is a shared secret between the relay
// and its own client; it is compared in constant time like every other secret.
const RELAY_SIGNAL_TOKEN = process.env.RELAY_SIGNAL_TOKEN || "";
const SIGNAL_PATH = "/__signal";
const SIGNAL_BODY_MAX = 8 * 1024;
// A self-declared client id is a grouping key, not a capability: it can only
// place the caller in a lane, and the password (or an open relay) is what
// authorizes that. Restricted to a shape that cannot smuggle a separator into
// the sticky key or a newline into a log line.
const CLIENT_ID_RE = /^c-[A-Za-z0-9_-]{1,64}$/;

const RETRY_MAX = laneEnv("RELAY_LANE_RETRY_MAX", 3, { allowZero: true });
const RETRY_MAX_BODY = laneEnv("RELAY_LANE_RETRY_MAX_BODY", 1048576);
const STICKY_MAX = laneEnv("RELAY_LANE_STICKY_MAX", 10000, { passthrough: true });
const LANE_PROBE_MS = laneEnv("RELAY_LANE_PROBE_S", 30, { allowZero: true }) * 1000;
const LANE_COOLDOWN_MS = laneEnv("RELAY_LANE_COOLDOWN_S", 240) * 1000;
const LANE_COOLDOWN_429_MS = laneEnv("RELAY_LANE_429_S", 120) * 1000;

// Universal-proxy allowlist matcher. Each entry is one of:
//   exact origin  ("https://opencode.ai")
//   "*"           (any http/https origin)
//   "*.suffix"    (apex + subdomains over http/https)
//   "re:<regex>"  (JS regex tested against the origin)
// Returns { matched, exact } — exact hits bypass the token gate even when a
// wildcard/regex entry would also match.
function matchTarget(target) {
  if (!target) return { matched: false, exact: false };
  let url;
  try {
    url = new URL(target);
  } catch {
    return { matched: false, exact: false };
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") return { matched: false, exact: false };
  const origin = url.origin;
  const hostname = url.hostname.toLowerCase();
  let matched = false;
  let exact = false;
  for (const entry of ALLOWED_TARGETS) {
    if (entry === "*") {
      matched = true;
    } else if (entry.startsWith("*.")) {
      const suffix = entry.slice(2).toLowerCase();
      if (suffix && (hostname === suffix || hostname.endsWith(`.${suffix}`))) matched = true;
    } else if (entry.startsWith("re:")) {
      let re;
      try {
        re = new RegExp(entry.slice(3));
      } catch {
        continue;
      }
      if (re.test(origin)) matched = true;
    } else {
      try {
        if (new URL(entry).origin === origin) {
          matched = true;
          exact = true;
        }
      } catch {
        if (entry === target) {
          matched = true;
          exact = true;
        }
      }
    }
  }
  if (!matched) return { matched: false, exact: false };
  return { matched, exact };
}

function isAuthorized(req, match) {
  if (match.exact) return true;
  if (!RELAY_AUTH_TOKEN) return false;
  return secretEquals(req.headers[HEADER_AUTH], RELAY_AUTH_TOKEN);
}

// Live rate-limit signals. Two writers share this sink:
//   - the L7 forward-proxy path, which reads the status itself (below), and
//   - the client report endpoint, which exists because a CONNECT tunnel hides
//     that status from this process (see handleSignal).
// Setting RELAY_SIGNAL_TOKEN implies the writer: a report that only cools a lane
// in memory leaves the throttled container running, and making the operator set
// a second knob to get the durable half is how a silent half-config happens.
const LIVE_SIGNALS = process.env.LIVE_SIGNALS === "1";
const LIVE_SIGNAL_FILE = process.env.LIVE_SIGNAL_FILE || ".keep-cold/signals.json";
const signalWriter =
  LIVE_SIGNALS || RELAY_SIGNAL_TOKEN ? createSignalWriter({ file: LIVE_SIGNAL_FILE }) : null;
// Enough to cover a chat-completions preamble (stream flags, tool schemas) on
// every real client; the model field appears before the first message.
const MODEL_SCAN_BYTES = 16 * 1024;
const MODEL_FIELD_RE = /"model"\s*:\s*"([^"]+)"/;

// Request handlers catch their own failures, so an exception that reaches the
// process is a programming error: log it and let Docker restart a clean
// process. Continuing would leave a half-dead relay that `restart: always`
// never replaces.
process.on("unhandledRejection", (err) => {
  console.error(`[relay] unhandledRejection: ${err?.stack ?? String(err)}`);
});
process.on("uncaughtException", (err) => {
  console.error(`[relay] uncaughtException: ${err?.stack ?? String(err)}`);
  signalWriter?.flush();
  process.exit(1);
});

// undici's ProxyAgent drops the client tuning options on its SOCKS5 branch
// (Socks5ProxyAgent builds its Pool from dispatch options, which fetch never
// sets), so the relay speaks SOCKS5 itself and hands undici a normal
// connector. That keeps keep-alive/timeouts/connection caps effective.
function socks5Connector(proxyHost, proxyPort, timeoutMs) {
  return function connect(opts, callback) {
    const { hostname, protocol, servername } = opts;
    const port = opts.port || (protocol === "https:" ? 443 : 80);
    const socket = net.connect({ host: proxyHost, port: proxyPort });
    let buf = Buffer.alloc(0);
    let stage = 0;
    let settled = false;

    const finish = (err, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      socket.removeListener("data", onData);
      socket.removeListener("error", onError);
      if (err) {
        socket.destroy();
        callback(err);
        return;
      }
      callback(null, result);
    };

    const timer = setTimeout(() => finish(new Error("SOCKS5 connect timeout")), timeoutMs);
    const onError = (err) => finish(err);
    socket.once("error", onError);
    socket.setNoDelay(true);
    socket.once("connect", () => socket.write(Buffer.from([0x05, 0x01, 0x00])));

    function onData(chunk) {
      buf = Buffer.concat([buf, chunk]);
      for (;;) {
        if (stage === 0) {
          if (buf.length < 2) return;
          if (buf[0] !== 0x05 || buf[1] !== 0x00) return finish(new Error("SOCKS5 no-auth rejected"));
          buf = buf.subarray(2);
          stage = 1;
          // Build the CONNECT request once the greeting is settled: domain
          // names are resolved by the proxy (microsocks), so DNS never leaks
          // to the relay host.
          const host = hostname.replace(/^\[|\]$/g, "");
          const family = net.isIP(host);
          let addr;
          if (family === 4) {
            addr = Buffer.from([0x01, ...host.split(".").map(Number)]);
          } else if (family === 6) {
            addr = Buffer.from([0x04, ...ipv6Bytes(host)]);
          } else {
            const name = Buffer.from(host, "utf8");
            addr = Buffer.concat([Buffer.from([0x03, name.length]), name]);
          }
          const portBuf = Buffer.alloc(2);
          portBuf.writeUInt16BE(port, 0);
          socket.write(Buffer.concat([Buffer.from([0x05, 0x01, 0x00]), addr, portBuf]));
        } else if (stage === 1) {
          if (buf.length < 4) return;
          if (buf[0] !== 0x05 || buf[1] !== 0x00) return finish(new Error(`SOCKS5 CONNECT failed (${buf[1]})`));
          const atyp = buf[3];
          let addrLen;
          if (atyp === 0x01) addrLen = 4;
          else if (atyp === 0x04) addrLen = 16;
          else if (atyp === 0x03) {
            if (buf.length < 5) return;
            addrLen = 1 + buf[4];
          } else return finish(new Error("SOCKS5 bad reply address type"));
          const replyLen = 4 + addrLen + 2;
          if (buf.length < replyLen) return;
          buf = buf.subarray(replyLen);
          stage = 2;
          if (protocol !== "https:") {
            // Any bytes after the reply already belong to the target.
            if (buf.length) socket.unshift(buf);
            return finish(null, socket);
          }
          const secure = tls.connect({
            socket,
            servername: servername || hostname,
            ALPNProtocols: ["http/1.1"],
          });
          secure.once("secureConnect", () => finish(null, secure));
          secure.once("error", (err) => finish(err));
          // The plain socket is owned by the TLS wrapper now; a late error on
          // it is already surfaced through the TLS socket.
          socket.removeListener("data", onData);
          if (buf.length) secure.unshift(buf);
        } else {
          return;
        }
      }
    }
    socket.on("data", onData);
  };
}

function ipv6Bytes(host) {
  const [head, tail] = host.split("::");
  const left = head ? head.split(":") : [];
  const right = tail ? tail.split(":") : [];
  const groups = [...left, ...Array(8 - left.length - right.length).fill("0"), ...right];
  const out = Buffer.alloc(16);
  groups.forEach((g, i) => out.writeUInt16BE(parseInt(g || "0", 16), i * 2));
  return out;
}

const DROP_HEADERS = new Set([
  "host",
  HEADER_TARGET,
  HEADER_PATH,
  HEADER_PROBE,
  HEADER_AUTH,
  // Lane targeting is relay-internal: forwarding it would let a probed
  // upstream see (and a log-replaying attacker copy) the routing hint.
  "x-relay-lane",
  "proxy-authorization",
  "connection",
  "keep-alive",
  "proxy-connection",
  "transfer-encoding",
  // undici derives the length from the body it is given. Forwarding the
  // client's own content-length throws UND_ERR_INVALID_ARG against a
  // dispatcher (undici 7.30 vs the bundled 7.13 on Node 24), which failed
  // every ordinary POST, and a length a client asserts is not the length we
  // actually send anyway — a short body would hang the upstream waiting.
  "content-length",
  "te",
  "trailer",
  "upgrade",
  "expect",
  // undici decodes gzip/deflate/br and the relay strips content-encoding, so
  // let it advertise only encodings it can always decode.
  "accept-encoding",
]);

// The upstream status alone does not say which model was throttled, and a
// streamed body cannot be buffered (memory). This tee reads the model field
// out of the first MODEL_SCAN_BYTES and forwards every byte.
function createModelSniffer() {
  let seen = "";
  const sniffer = new Transform({
    transform(chunk, _enc, done) {
      if (sniffer.model === null && seen.length < MODEL_SCAN_BYTES) {
        // latin1 keeps one char per byte when a chunk splits a UTF-8 rune;
        // model ids are ASCII, so the pattern still matches.
        seen += chunk.subarray(0, MODEL_SCAN_BYTES - seen.length).toString("latin1");
        const match = MODEL_FIELD_RE.exec(seen);
        if (match) sniffer.model = match[1];
      }
      done(null, chunk);
    },
  });
  sniffer.model = null;
  return sniffer;
}

// One agent per lane: `connections` is per-destination, keep-alive lets the
// warp-<n> SOCKS hop be reused across requests.
const lanes = createLanes({
  count: COUNT,
  agentFor: (n) =>
    new Agent({
      connect: socks5Connector(process.env[`WARP_HOST_${n}`] || `warp-${n}`, process.env[`WARP_PORT_${n}`] || "1080", 30_000),
      keepAliveTimeout: 60_000,
      keepAliveMaxTimeout: 90_000,
      headersTimeout: 60_000,
      bodyTimeout: 0,
      connections: 32,
    }),
  probeIntervalMs: LANE_PROBE_MS,
  cooldownMs: LANE_COOLDOWN_MS,
  cooldown429Ms: LANE_COOLDOWN_429_MS,
  stickyMax: STICKY_MAX,
  log: (msg) => console.log(`[router] ${msg}`),
});

let inflight = 0;

// The blind spot this endpoint exists for: on a CONNECT tunnel the relay pipes
// bytes and never sees the origin's status, so a 429 that the client observed
// would otherwise go unreported and the throttled egress would keep serving.
// The client names itself; the relay maps that to the lane it has bound and
// cools it, which drops the binding so the next tunnel rotates.
//
// Deliberately lane-free: the client must never be able to choose an egress.
// Keep it that way — accepting a lane number here would hand any client holding
// the signal token the ability to park arbitrary lanes.
async function handleSignal(req, res) {
  if (!RELAY_SIGNAL_TOKEN) {
    res.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
    return res.end("Not Found\n");
  }
  if (!secretEquals(req.headers.authorization || "", `Bearer ${RELAY_SIGNAL_TOKEN}`)) {
    res.writeHead(401, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Unauthorized" }));
  }

  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > SIGNAL_BODY_MAX) {
      res.writeHead(413, { "content-type": "application/json" });
      return res.end(JSON.stringify({ error: "Signal too large" }));
    }
    chunks.push(chunk);
  }

  let body;
  try {
    body = JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    res.writeHead(400, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Invalid JSON" }));
  }

  const clientId = body?.clientId;
  const status = body?.status;
  if (typeof clientId !== "string" || !CLIENT_ID_RE.test(clientId)) {
    res.writeHead(400, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Invalid clientId" }));
  }
  // Same two verdicts signal-writer records; anything else is a client bug.
  if (status !== 429 && status !== 200) {
    res.writeHead(400, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "status must be 429 or 200" }));
  }

  const key = `c:${clientId}`;
  const lane = lanes.heldFor(key);
  if (!lane) {
    // Nothing to condemn: the client has not been served a lane yet, or its
    // binding already expired. Acknowledge rather than error — a report that
    // lost the race must not look like a client failure.
    res.writeHead(204);
    return res.end();
  }

  const model = typeof body?.model === "string" && body.model ? body.model.slice(0, 200) : "";
  const retryAfter = Number(body?.retryAfter) || null;
  if (status === 429) {
    // "signal" picks cool429Until, leaving the probe-driven cooldown untouched:
    // a reported throttle must not be confused with a connect failure.
    lanes.cool(lane, LANE_COOLDOWN_429_MS, `429 reported by ${clientId}`, "signal");
  } else {
    // Recovery: clear the throttle cooldown so the lane is selectable again.
    lane.cool429Until = 0;
    lanes.bind(key, lane);
    console.log(`[relay #${lane.n}] 429 cleared by ${clientId}`);
  }
  // Durable half: keep-cold --signals turns this record into a container
  // restart. Without it the report would only steer this process's memory.
  signalWriter?.record({ relay: lane.n, model, status, retryAfter });

  res.writeHead(204);
  res.end();
}

const server = http.createServer(async (req, res) => {
  // A client that vanishes mid-response must not surface as an unhandled
  // 'error' event on either side.
  req.on("error", () => {});
  res.on("error", () => {});

  if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
    res.writeHead(503, { "content-type": "application/json", "retry-after": "5" });
    return res.end(JSON.stringify({ error: "Relay busy" }));
  }
  inflight += 1;
  res.once("close", () => {
    inflight -= 1;
  });

  // Matched on the exact request line, before any proxy handling, so it can
  // never be read as an absolute-form dispatch to an upstream origin.
  if (req.method === "POST" && req.url === SIGNAL_PATH) {
    try {
      await handleSignal(req, res);
    } catch (err) {
      console.error(`[relay] signal handler failed: ${err?.stack ?? String(err)}`);
      if (!res.headersSent) res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "Internal error" }));
    }
    return;
  }

  const auth = proxyAuthInfo(req);
  const key = laneKey(req.headers, req.socket?.remoteAddress || "", auth.clientId);

  let target = req.headers[HEADER_TARGET]?.replace(/\/$/, "");
  let relayPath = req.headers[HEADER_PATH] || "/";

  // Standard forward-proxy absolute-form: `GET http://host/path` arrives
  // with req.url as the absolute URI. When x-relay-target is absent,
  // derive target+path from it so plain proxy clients work header-free.
  if (!target && typeof req.url === "string" && /^https?:\/\//i.test(req.url)) {
    try {
      const abs = new URL(req.url);
      if (abs.protocol === "http:" || abs.protocol === "https:") {
        target = abs.origin;
        relayPath = (abs.pathname + abs.search) || "/";
      }
    } catch {
      // Fall through to the health check below.
    }
  }

  // Direct health check: only when NO x-relay-target header is passed
  // (and the request line is not absolute-form). Exempt from the token gate.
  if (!target) {
    // Fall back to the first lane when the pool is exhausted. A diagnostic
    // endpoint that 503s when every lane is cooled would make every probe
    // fail, which marks every lane sick — and a sick lane is only cleared by
    // a probe that needs a usable lane. Answering here is what lets a cooled
    // or sick fleet recover.
    // A probe naming lane N must reach lane N, or a dead lane reads healthy
    // forever. Above the pick so a pin always wins.
    const pinned = resolvePinnedLane({
      headers: req.headers,
      proxyUser: auth.user,
      proxyPass: auth.pass,
      lanes,
      proxyAuth: RELAY_PROXY_AUTH,
    });
    const lane = pinned || lanes.pick(key) || lanes.lanes[0];
    try {
      const trace = await fetch("https://cloudflare.com/cdn-cgi/trace", {
        dispatcher: lane.agent,
        signal: AbortSignal.timeout(5_000),
      });
      if (!trace.ok) throw new Error(`trace HTTP ${trace.status}`);
      const text = await trace.text();
      res.writeHead(200, { "content-type": "text/plain; charset=utf-8" });
      return res.end(`Relay #${lane.n} OK (via socks5://${lane.host}:${lane.port})\n\n${text}`);
    } catch (err) {
      // Detail stays in the logs: the body used to reflect String(err).
      // Deliberately does NOT cool the lane: keep-cold's probe factory dials
      // this path with no session header, so every probe would share one
      // sticky key. Cooling here would let one bad trace fetch park the lane
      // whose probe then reports it sick — and a sick lane is only cleared by
      // a probe that needs a usable lane. That deadlock is fleet-wide and
      // permanent. Lane health is owned by the probe path in router.mjs.
      console.error(`[relay #${lane.n}] health check failed: ${err?.stack ?? String(err)}`);
      res.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
      return res.end(`Relay #${lane.n} health check failed\n`);
    }
  }

  const match = matchTarget(target);
  if (!match.matched) {
    res.writeHead(403, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Forbidden target" }));
  }
  if (!isAuthorized(req, match)) {
    res.writeHead(401, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Unauthorized" }));
  }
  if (!auth.ok) {
    res.writeHead(407, {
      "content-type": "application/json",
      "proxy-authenticate": PROXY_AUTH_CHALLENGE,
    });
    return res.end(JSON.stringify({ error: "Proxy authentication required" }));
  }

  // The allowlist check above only validates the raw header string; the
  // request URL is target + relayPath, and a crafted path can continue the
  // host label (".attacker.tld/x" -> opencode.ai.attacker.tld), swap the
  // port (":8443/x"), or inject userinfo ("@evil.tld/x"). Build the URL
  // once and require it to stay on the allowlisted origin with a rooted
  // path. Concat (not URL-relative resolution) preserves the legacy
  // target+path semantics for any allowlist entry carrying a base path.
  let upstreamUrl;
  let targetOrigin;
  try {
    targetOrigin = new URL(target).origin;
    upstreamUrl = new URL(target + relayPath);
  } catch {
    upstreamUrl = null;
  }
  if (!upstreamUrl || upstreamUrl.origin !== targetOrigin || !relayPath.startsWith("/")) {
    console.error(`[relay] rejected relay path: target=${target} path=${relayPath}`);
    res.writeHead(400, { "content-type": "application/json" });
    return res.end(JSON.stringify({ error: "Invalid relay path" }));
  }

  // keep-cold traffic may name its lane so a probe measures the egress it
  // reports on. Everything else is load-balanced. `auth` is defined above, so
  // the userinfo form needs no second credential parse.
  const pinned = resolvePinnedLane({
    headers: req.headers,
    proxyUser: auth.user,
    proxyPass: auth.pass,
    lanes,
    proxyAuth: RELAY_PROXY_AUTH,
  });
  let lane = pinned || lanes.pick(key);
  if (!lane) {
    res.writeHead(503, { "content-type": "application/json", "retry-after": "30" });
    return res.end(JSON.stringify({ error: "No relay available" }));
  }

  const headers = new Headers();
  for (const [k, v] of Object.entries(req.headers)) {
    if (!DROP_HEADERS.has(k) && v !== undefined) {
      headers.set(k, v);
    }
  }

  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  const isProbe = req.headers[HEADER_PROBE] !== undefined;
  const sniffEnabled = Boolean(signalWriter) && hasBody && !isProbe;
  let replayable;
  let reqBody;
  try {
    ({ replayable, body: reqBody } = await readReplayableBody(req, RETRY_MAX_BODY));
  } catch (err) {
    // The client vanished mid-body: nothing to answer and no lane to blame.
    console.error(`[relay] request body aborted: ${err?.message ?? String(err)}`);
    if (!res.headersSent) res.destroy();
    return;
  }
  const sniffer = sniffEnabled && reqBody && !Buffer.isBuffer(reqBody) ? createModelSniffer() : null;

  // A buffered body is sniffed directly; a streamed one is teed once.
  const modelOf = () => {
    if (!sniffEnabled) return "unknown";
    if (Buffer.isBuffer(reqBody)) {
      const m = MODEL_FIELD_RE.exec(reqBody.subarray(0, MODEL_SCAN_BYTES).toString("latin1"));
      return m ? m[1] : "unknown";
    }
    return sniffer?.model ?? "unknown";
  };

  // 5-min upstream cap + client-disconnect wiring: a client that goes away
  // must not leave its upstream fetch hanging (leaked sockets pile up under
  // agent retries). Note req 'close' also fires on normal POST body
  // completion (socket still alive), so only abort when the socket actually
  // died; res 'close' covers disconnect-while-waiting for all methods.
  const controller = new AbortController();
  const onClientClose = () => {
    if (!res.writableEnded && req.socket?.destroyed) controller.abort();
  };
  req.once("close", onClientClose);
  res.once("close", onClientClose);
  const upstreamTimeout = AbortSignal.timeout(300000);

  let upstream = null;
  let failure = null;
  try {
    for (let attempt = 0; ; attempt++) {
      upstream = null;
      failure = null;
      try {
        upstream = await fetch(upstreamUrl, {
          method: req.method,
          headers,
          body: reqBody === null ? undefined : sniffer ? reqBody.pipe(sniffer) : reqBody,
          duplex: "half",
          // The allowlist constrains the first hop only; following a redirect
          // would let an allowlisted origin steer the relay to any host.
          redirect: "error",
          dispatcher: lane.agent,
          signal: AbortSignal.any([upstreamTimeout, controller.signal]),
        });
      } catch (err) {
        failure = err;
        // A client abort is not the lane's fault: it would evict a healthy
        // lane and then retry a request whose client is already gone.
        if (controller.signal.aborted) break;
        // Only the lane's own SOCKS hop failing says the lane is bad. Anything
        // past it — TLS handshake, DNS, redirect — is the target's problem, and
        // cooling the lane for it would park a healthy egress.
        const why = [err?.cause?.code, err?.cause?.message, err?.message].filter(Boolean).join(" ");
        const connectFail = /ECONNREFUSED|SOCKS5|connect timeout|EHOSTUNREACH|ENETUNREACH/i.test(why);
        if (connectFail) lanes.cool(lane, LANE_COOLDOWN_MS, "lane connect failed");
      }

      if (upstream) {
        if (signalWriter && !isProbe) {
          // Best-effort and additive: a signal failure must never reach the
          // response path. Every throttled attempt is recorded, not just the
          // one whose response the client finally sees.
          try {
            if (upstream.status === 429) {
              signalWriter.record({
                relay: lane.n,
                model: modelOf(),
                status: 429,
                retryAfter: Number(upstream.headers.get("retry-after")) || null,
              });
            } else if (upstream.status === 200) {
              signalWriter.record({ relay: lane.n, model: modelOf(), status: 200 });
            }
          } catch {
            // Ignored: keep-cold falls back to probing.
          }
        }
        if (upstream.status === 429) {
          lanes.cool(lane, LANE_COOLDOWN_429_MS, "429 from upstream", "signal");
        }
        // A status retry needs the body re-sent, so only a buffered (or
        // bodyless) request may continue; `retryable` enforces that.
        if (!retryable({ method: req.method, status: upstream.status, replayable })) break;
      }

      // A one-shot body cannot be replayed: the attempt already consumed part
      // of it, so a retry would send a truncated request. ponytail: the ceiling
      // is one attempt for bodies over RELAY_LANE_RETRY_MAX_BODY; upgrade by
      // spooling to a temp file.
      const canRetry = !pinned && replayable && attempt < RETRY_MAX;
      if (!canRetry) break;
      const next = lanes.pick(key);
      if (!next) break;
      if (upstream?.body) {
        try {
          await upstream.body.cancel();
        } catch {
          // Ignored: the body is being discarded either way.
        }
      }
      lane = next;
    }

    if (failure) {
      // Body stays generic: String(err) reflected attacker-controlled URLs
      // and internal proxy names back to the caller.
      console.error(`[relay #${lane.n}] upstream error: ${failure?.stack ?? String(failure)}`);
      if (!res.headersSent) {
        res.writeHead(502, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: "upstream request failed" }));
      } else {
        res.destroy();
      }
      return;
    }

    // A pinned lane is a measurement, not a preference: binding it to the
    // sticky key would drag a probed session's later real traffic onto it.
    // A real 200 on a lane is proof the egress answers now, which beats the
    // guess a 429 timestamp makes — so it lifts that lane's 429 cooldown at
    // once. Without this, a lane whose container was just restarted still sits
    // parked for RELAY_LANE_429_S while the pool has nothing else to serve.
    if (upstream.status !== 429) {
      lane.cool429Until = 0;
      if (!pinned) lanes.bind(key, lane);
    }

    // Forward only what the client needs: full header copies cost per-request
    // work and risk stale content-encoding/content-length (fetch decodes
    // gzip/deflate/br, so those upstream values lie about our body).
    const outHeaders = { "cache-control": "no-cache", "x-accel-buffering": "no" };
    const contentType = upstream.headers.get("content-type");
    if (contentType) outHeaders["content-type"] = contentType;
    const retryAfter = upstream.headers.get("retry-after");
    if (retryAfter) outHeaders["retry-after"] = retryAfter;
    res.writeHead(upstream.status, outHeaders);
    // Kickstart: Node buffers headers until the first body byte, so TTFB
    // would wait for the first token. Flush now — headers land at
    // upstream-headers time instead.
    res.flushHeaders();
    if (upstream.body) {
      // Zero-copy pipe with backpressure; for-await + res.write() costs a
      // microtask per chunk and buffers against a slow client.
      await pipeline(Readable.fromWeb(upstream.body), res);
    } else {
      res.end();
    }
  } catch (err) {
    if (!res.headersSent) {
      console.error(`[relay #${lane.n}] upstream error: ${err?.stack ?? String(err)}`);
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ error: "upstream request failed" }));
    } else {
      // Headers already on the wire (mid-stream failure) — just cut it.
      // Unconditional writeHead here crashed the process (ERR_HTTP_HEADERS_SENT).
      res.destroy();
    }
  } finally {
    req.removeListener("close", onClientClose);
    res.removeListener("close", onClientClose);
    // An unconsumed streamed body would leave the client socket paused; the
    // destroy also tears down `req` through router.mjs's close handler.
    if (reqBody && !Buffer.isBuffer(reqBody)) reqBody.destroy();
  }
});

// TTFT tuning: Nagle off (first small frame goes immediately), long
// keep-alive so cloudflared reuses the relay-app connection.
server.keepAliveTimeout = 90_000;
server.headersTimeout = 95_000;
server.requestTimeout = 0;
server.timeout = 0;
server.on("connection", (socket) => socket.setNoDelay(true));

// Standard forward-proxy CONNECT: host:port is restricted to 443/80, the
// hostname is matched as an https-origin through the same allowlist, and the
// token gate applies (exact hits exempt). On success the relay answers 200 and
// pipes raw TCP over a SOCKS-connected socket — no interception, no redirect
// following. The in-flight guard mirrors the request path.
server.on("connect", (req, clientSocket, head) => {
  req.on("error", () => {});
  clientSocket.on("error", () => {});
  const deny = (code, text, extraHeaders = "") => {
    try {
      clientSocket.write(`HTTP/1.1 ${code} ${text}\r\nContent-Length: 0\r\n${extraHeaders}\r\n`);
    } catch {
      // Ignored: the client is already gone.
    }
    clientSocket.destroy();
  };
  if (MAX_INFLIGHT > 0 && inflight >= MAX_INFLIGHT) {
    deny(503, "Service Unavailable");
    return;
  }
  inflight += 1;
  clientSocket.once("close", () => {
    inflight -= 1;
  });
  // CONNECT authority is "hostname:port" ("[v6]:port" for literals).
  let host = null;
  let portNum = 0;
  const authority = req.url || "";
  const v6 = authority.match(/^\[([^\]]+)\]:(\d+)$/);
  if (v6) {
    host = v6[1];
    portNum = Number(v6[2]);
  } else {
    const idx = authority.lastIndexOf(":");
    if (idx > 0) {
      const h = authority.slice(0, idx);
      const p = authority.slice(idx + 1);
      if (h && /^\d+$/.test(p) && !h.includes(":")) {
        host = h;
        portNum = Number(p);
      }
    }
  }
  if (!host || !Number.isInteger(portNum)) {
    deny(403, "Forbidden");
    return;
  }
  // Hostname matched as an https-origin through the shared matcher.
  const hostForUrl = host.includes(":") ? `[${host}]` : host;
  const match = matchTarget(`https://${hostForUrl}`);
  if (!match.matched) {
    deny(403, "Forbidden");
    return;
  }
  if (!isAuthorized(req, match)) {
    deny(401, "Unauthorized");
    return;
  }
  const auth = proxyAuthInfo(req);
  if (!auth.ok) {
    deny(407, "Proxy Authentication Required", `Proxy-Authenticate: ${PROXY_AUTH_CHALLENGE}\r\n`);
    return;
  }
  if (portNum !== 443 && portNum !== 80) {
    deny(403, "Forbidden");
    return;
  }
  // A tunnel occupies one egress for its whole life: pick once, never re-pick.
  // A keep-cold probe's `lane-N` credentials pin the tunnel to that lane.
  const pinned = resolvePinnedLane({
    headers: req.headers,
    proxyUser: auth.user,
    proxyPass: auth.pass,
    lanes,
    proxyAuth: RELAY_PROXY_AUTH,
  });
  const lane = pinned || lanes.pick(laneKey(req.headers, clientSocket.remoteAddress || "", auth.clientId));
  if (!lane) {
    deny(503, "Service Unavailable");
    return;
  }
  // Raw pipe: reuse the SOCKS handshake with a plain (non-TLS) protocol so
  // the socket stays untampered. Port is explicit here (443/80 allowed).
  const rawConnect = socks5Connector(lane.host, lane.port, 30_000);
  rawConnect({ hostname: host, port: portNum, protocol: "http:" }, (err, targetSocket) => {
    if (err) {
      console.error(`[relay #${lane.n}] CONNECT error: ${err?.stack ?? String(err)}`);
      lanes.cool(lane, LANE_COOLDOWN_MS, "CONNECT failed");
      deny(502, "Bad Gateway");
      return;
    }
    targetSocket.on("error", () => {});
    try {
      clientSocket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
    } catch {
      targetSocket.destroy();
      return;
    }
    if (head && head.length) targetSocket.write(head);
    clientSocket.pipe(targetSocket);
    targetSocket.pipe(clientSocket);
    const cleanup = () => {
      try {
        clientSocket.destroy();
      } catch {
        // Ignored.
      }
      try {
        targetSocket.destroy();
      } catch {
        // Ignored.
      }
    };
    clientSocket.once("close", cleanup);
    targetSocket.once("close", cleanup);
  });
});

// A failed bind must be fatal: a half-started relay is invisible to
// `restart: always` and only shows up as a connection refused.
server.on("error", (err) => {
  console.error(`[relay] fatal listen error: ${err?.stack ?? String(err)}`);
  signalWriter?.flush();
  process.exit(1);
});

server.listen(BASE_PORT, "0.0.0.0", () => {
  console.log(`listening on 0.0.0.0:${BASE_PORT} -> ${COUNT} lane(s)`);
  // A `c-<id>` client needs a non-empty password half to authenticate at all,
  // and the failure looks like a client bug rather than a config one.
  if (RELAY_SIGNAL_TOKEN && RELAY_PROXY_AUTH.includes(":")) {
    const wantPass = RELAY_PROXY_AUTH.slice(RELAY_PROXY_AUTH.indexOf(":") + 1);
    if (wantPass === "") {
      console.error("[relay] RELAY_PROXY_AUTH has an empty password half: `c-<id>` clients cannot authenticate.");
    }
  }
  // Without this nothing probes: sick lanes would never be parked or
  // re-admitted, and RELAY_LANE_PROBE_S would be a no-op knob.
  lanes.start();
});

// Drain on deploy: stop accepting, let in-flight streams finish, then force
// the exit so a wedged connection cannot hold the container open.
let shuttingDown = false;
function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[relay] ${signal} received — draining`);
  signalWriter?.flush();
  // Timers only: closing the lane agents here would abort the in-flight
  // streams the 10s drain exists to finish.
  lanes.stop();
  server.close();
  server.closeIdleConnections?.();
  setTimeout(() => process.exit(0), 10_000).unref();
}
process.on("SIGTERM", () => shutdown("SIGTERM"));
process.on("SIGINT", () => shutdown("SIGINT"));
