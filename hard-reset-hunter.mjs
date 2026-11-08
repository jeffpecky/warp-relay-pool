import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomBytes, createHash } from 'node:crypto';

const execAsync = promisify(execFile);
const COUNT = parseInt(process.env.COUNT || '10', 10);
const RELAY_HOST = process.env.RELAY_HOST || '10.244.0.100';
const BASE_PORT = parseInt(process.env.BASE_PORT || '3000', 10);
const TARGET_MODEL = process.env.TARGET_MODEL || 'deepseek-v4.1-flash-free';
const MAX_ATTEMPTS = parseInt(process.env.MAX_ATTEMPTS || '25', 10);

const tools = ['bash', 'glob', 'grep', 'read', 'edit', 'write'].map((name) => ({
  type: 'function',
  function: { name, description: `${name} (probe)`, parameters: { type: 'object', properties: {} } },
}));

function getProbeHeaders(sid, prj, index) {
  return {
    'content-type': 'application/json',
    'user-agent': 'opencode/1.18.31',
    'x-opencode-client': 'cli',
    'x-opencode-project': prj,
    'x-opencode-session': sid,
    'authorization': 'Bearer public',
    'x-opencode-session-id': sid,
    'x-session-affinity': sid,
    'x-session-id': sid,
    'accept': 'text/event-stream',
    'x-relay-target': 'https://opencode.ai',
    'x-relay-path': '/inference/v1/chat/completions',
    'x-keep-cold-probe': '1',
    // One port serves the whole pool, so the lane travels as a header.
    'x-relay-lane': String(index),
  };
}

async function probeRelay(index) {
  const sid = `ses_${randomBytes(6).toString('hex')}${randomBytes(14).toString('base64url').slice(0, 14)}`;
  const prj = createHash('sha1').update('git-remote:github.com/anomalyco/opencode').digest('hex');
  const headers = getProbeHeaders(sid, prj, index);

  try {
    const res = await fetch(`http://${RELAY_HOST}:${BASE_PORT}/inference/v1/chat/completions`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        model: TARGET_MODEL,
        messages: [{ role: 'user', content: 'hi' }],
        max_tokens: 16,
        stream: true,
        tools,
      }),
      signal: AbortSignal.timeout(6000),
    });

    if (res.status === 200) {
      return { ok: true, status: 200 };
    }
    const ra = res.headers.get('retry-after');
    return { ok: false, status: res.status, retryAfter: ra };
  } catch (err) {
    return { ok: false, status: 0, error: err.message };
  }
}

async function getTrace(index) {
  const socksPort = 10800 + index;
  try {
    const { stdout } = await execAsync('curl', [
      '-s', '-m', '3',
      '-x', `socks5h://127.0.0.1:${socksPort}`,
      'https://cloudflare.com/cdn-cgi/trace'
    ]);
    const ipMatch = stdout.match(/^ip=(.*)$/m);
    const warpMatch = stdout.match(/^warp=(.*)$/m);
    return {
      ip: ipMatch ? ipMatch[1].trim() : null,
      warp: warpMatch ? warpMatch[1].trim() : null,
    };
  } catch {
    return { ip: null, warp: null };
  }
}

async function hardResetRelay(index) {
  const cname = `warp-relay-warp-${index}`;
  await execAsync('docker', ['exec', cname, 'rm', '-f', '/etc/wireguard/wg0.conf']);
  await execAsync('docker', ['restart', cname]);

  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 600));
    const trace = await getTrace(index);
    if (trace.warp === 'on' && trace.ip) {
      return trace.ip;
    }
  }
  return null;
}

async function run() {
  console.log(`=== HARD RESET HUNTER for ${TARGET_MODEL} across ${COUNT} relays ===`);
  const cleanRelays = new Set();

  // Step 1: Initial baseline check
  console.log('--- Baseline Probe ---');
  for (let i = 1; i <= COUNT; i++) {
    const trace = await getTrace(i);
    const p = await probeRelay(i);
    if (p.ok) {
      console.log(`Relay #${i} ALREADY CLEAN! (IP: ${trace.ip})`);
      cleanRelays.add(i);
    } else {
      console.log(`Relay #${i} Throttled: HTTP ${p.status} (IP: ${trace.ip}, retry-after: ${p.retryAfter || 'none'})`);
    }
  }

  if (cleanRelays.size === COUNT) {
    console.log(`\nALL ${COUNT} RELAYS ARE ALREADY CLEAN!`);
    return;
  }

  // Step 2: Loop hard resets on dirty relays
  console.log(`\n--- Starting Hard Reset Hunt for ${COUNT - cleanRelays.size} dirty relays ---`);
  for (let i = 1; i <= COUNT; i++) {
    if (cleanRelays.has(i)) continue;

    console.log(`\n>>> Hunting clean /64 block for Relay #${i}...`);
    let attempt = 0;
    let found = false;

    while (attempt < MAX_ATTEMPTS && !found) {
      attempt++;
      const t0 = Date.now();
      const newIp = await hardResetRelay(i);
      const dtReset = ((Date.now() - t0) / 1000).toFixed(1);

      if (!newIp) {
        console.log(`  [Attempt ${attempt}/${MAX_ATTEMPTS}] (${dtReset}s) Reset failed or container not ready, retrying...`);
        await new Promise((r) => setTimeout(r, 2000));
        continue;
      }

      const p = await probeRelay(i);
      const dtTotal = ((Date.now() - t0) / 1000).toFixed(1);

      if (p.ok) {
        console.log(`  [Attempt ${attempt}/${MAX_ATTEMPTS}] (${dtTotal}s) SUCCESS 200 OK! Egress IP: ${newIp}`);
        cleanRelays.add(i);
        found = true;
        break;
      } else {
        const prefix = newIp.split(':').slice(0, 4).join(':');
        console.log(`  [Attempt ${attempt}/${MAX_ATTEMPTS}] (${dtTotal}s) 429 Throttled (Prefix: ${prefix}, IP: ${newIp})`);
      }

      // Safe breathing room between Cloudflare registrations (3s)
      await new Promise((r) => setTimeout(r, 3000));
    }

    if (!found) {
      console.log(`  [!] Relay #${i} reached MAX_ATTEMPTS (${MAX_ATTEMPTS}) without finding clean prefix.`);
    }
  }

  console.log(`\n=== HUNT SUMMARY: ${cleanRelays.size} / ${COUNT} relays clean on ${TARGET_MODEL} ===`);
}

run().catch((e) => {
  console.error('Fatal error:', e);
  process.exit(1);
});
