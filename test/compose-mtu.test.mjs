// Guards the generated compose file's lane MTU wiring. Every lane is
// dual-stack, and an IPv6 interface cannot carry an MTU below 1280 (RFC 8200)
// — wg-quick then fails on `ip -6 route add ::/0` and the lane crash-loops.
// So the knob exists but must reject anything under the IPv6 minimum.
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
      env: { PATH: process.env.PATH, ...env },
      stdio: "pipe",
    });
    return readFileSync(join(dir, "docker-compose.yml"), "utf8");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("lane MTU defaults to the IPv6 minimum, 1280", () => {
  const out = generate({});
  assert.match(out, /^\s+- MTU=1280$/m);
});

test("a larger WARP_MTU is baked into every lane", () => {
  const out = generate({ WARP_MTU: "1420" });
  assert.match(out, /^\s+- MTU=1420$/m);
  assert.doesNotMatch(out, /MTU=1280/);
});

test("an MTU below the IPv6 minimum is rejected, not baked into a broken lane", () => {
  assert.throws(() => generate({ WARP_MTU: "1200" }));
  assert.throws(() => generate({ WARP_MTU: "lots" }));
});
