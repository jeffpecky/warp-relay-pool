// Guards the generated compose file's lane MTU wiring. A WARP lane's inner MTU
// must be settable: on a host whose own uplink is already 1280 (WSL2) the
// default 1280 makes every encapsulated packet oversized, and the failure is
// not a dead lane — it is a TLS handshake that stalls while plain HTTP works.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

function generate(env) {
  const dir = mkdtempSync(join(tmpdir(), "warp-mtu-"));
  try {
    execFileSync("sh", [join(ROOT, "generate-compose.sh"), "1"], {
      cwd: dir,
      // COUNT/BASE_PORT/etc. are not needed; the script only writes a compose
      // file. Point the .env loader at nothing by running in a temp cwd.
      env: { PATH: process.env.PATH, ...env },
      stdio: "pipe",
    });
    return readFileSync(join(dir, "docker-compose.yml"), "utf8");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("lane MTU defaults to Cloudflare's 1280", () => {
  const out = generate({});
  assert.match(out, /^\s+- MTU=1280$/m);
});

test("WARP_MTU is baked into every lane", () => {
  const out = generate({ WARP_MTU: "1200" });
  assert.match(out, /^\s+- MTU=1200$/m);
  assert.doesNotMatch(out, /MTU=1280/);
});

test("an out-of-range WARP_MTU is rejected, not silently clamped", () => {
  assert.throws(() => generate({ WARP_MTU: "100" }));
  assert.throws(() => generate({ WARP_MTU: "lots" }));
});
