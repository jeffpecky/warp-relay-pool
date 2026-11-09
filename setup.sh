#!/bin/sh
# setup.sh — Automated installer and sizing wizard for warp-relay-pool.
# Uses MicroWARP (~1.8 MiB RAM/node) and TryCloudflare Quick Tunnels for instant setup.
set -eu

# Load .env when present (same loader as generate-compose.sh: process env
# wins, KEY=VALUE only, CRLF-safe, no value ever evaluated — EXCEPT that
# COUNT/RELAY_COUNT are excluded below: the wizard's VPS sizing owns the
# pool size, while the generator honors .env COUNT for manual runs). The
# wizard writes the final COUNT and BASE_PORT into the installed unit, so
# these values are the single source for the compose file AND the prober.
if [ -f .env ]; then
  cr="$(printf '\r')"
  dq='"' # quote characters, so the case patterns need no escapes
  sq="'"
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    case "$line" in *=*) ;; *) continue ;; esac
    key="${line%%=*}"
    val="${line#*=}"
    val="${val%"$cr"}"
    case "$key" in ''|[!A-Za-z_]*|*[!A-Za-z0-9_]*) continue ;; esac
    # The wizard owns the pool size for wizard-driven installs (VPS sizing),
    # so a copied .env.example COUNT=10 does not silently out-vote the
    # recommended count on a small VPS; pin one with `COUNT=5 ./setup.sh`
    # (process env still wins above). BASE_PORT and the binds DO flow here:
    # generate-compose.sh bakes them and the unit must match.
    case "$key" in COUNT|RELAY_COUNT) continue ;; esac
    case "$val" in
      "$dq"*) val="${val#"$dq"}"; val="${val%%"$dq"*}" ;;
      "$sq"*) val="${val#"$sq"}"; val="${val%%"$sq"*}" ;;
      # Bare values follow compose's env-file behaviour: a whitespace-preceded
      # `#…` starts an inline comment and is stripped; quoted values end at
      # the closing quote with any trailing comment ignored.
      *) val="${val%%[[:space:]]#*}" ;;
    esac
    # shellcheck disable=SC2163,SC2086 # key is validated above
    eval "cur=\${$key:-}"
    [ -n "$cur" ] || export "$key=$val"
  done < .env
fi

BOLD="\033[1m"
GREEN="\033[32m"
YELLOW="\033[33m"
CYAN="\033[36m"
RED="\033[31m"
RESET="\033[0m"

log_info() { printf "${CYAN}==>${RESET} %s\n" "$*"; }
log_ok() { printf "${GREEN}==>${RESET} %s\n" "$*"; }
log_warn() { printf "${YELLOW}WARNING:${RESET} %s\n" "$*"; }
log_err() { printf "${RED}ERROR:${RESET} %s\n" "$*" >&2; }

# Helper to run commands as root if needed
run_elevated() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  elif command -v sudo >/dev/null 2>&1; then
    sudo "$@"
  else
    log_err "Root privileges required for: $*"
    exit 1
  fi
}

AUTO_YES=false
for arg in "$@"; do
  case "$arg" in
    -y|--yes) AUTO_YES=true ;;
    *) ;;
  esac
done

echo ""
echo "================================================================="
echo "   warp-relay-pool — Automated VPS Deployment Wizard"
echo "   MicroWARP Engine (WireGuard + microsocks, ~1.8 MiB / node)"
echo "================================================================="
echo ""

# -------------------------------------------------------------
# 1. OS and Environment Checks
# -------------------------------------------------------------
OS="$(uname -s 2>/dev/null || echo "Unknown")"
if [ "$OS" != "Linux" ]; then
  log_err "Cloudflare WARP requires Linux kernel features (NET_ADMIN / WireGuard)."
  log_err "Detected OS: $OS. Please deploy this on a Linux VPS or server."
  exit 1
fi

log_info "Checking and preparing prerequisites..."

# Auto-install curl if missing
if ! command -v curl >/dev/null 2>&1; then
  log_info "Installing curl..."
  if command -v apt-get >/dev/null 2>&1; then
    run_elevated apt-get update && run_elevated apt-get install -y curl
  elif command -v dnf >/dev/null 2>&1; then
    run_elevated dnf install -y curl
  elif command -v yum >/dev/null 2>&1; then
    run_elevated yum install -y curl
  elif command -v apk >/dev/null 2>&1; then
    run_elevated apk add curl
  fi
fi

# Auto-install Docker if missing
if ! command -v docker >/dev/null 2>&1; then
  log_warn "Docker is not installed on this VPS."
  DO_INSTALL_DOCKER=true
  if [ "$AUTO_YES" != true ]; then
    printf "Would you like to automatically install Docker now? [Y/n]: "
    read -r ans || true
    case "$ans" in
      [nN]*) DO_INSTALL_DOCKER=false ;;
      *) DO_INSTALL_DOCKER=true ;;
    esac
  fi

  if [ "$DO_INSTALL_DOCKER" = true ]; then
    if ! command -v apt-get >/dev/null 2>&1 && ! command -v dnf >/dev/null 2>&1 && ! command -v yum >/dev/null 2>&1; then
      log_err "Automatic Docker installation is supported on Debian/Ubuntu/Fedora/CentOS only."
      log_err "Install Docker + the compose plugin with your package manager, then re-run."
      exit 1
    fi
    log_info "Installing Docker via official install script..."
    # Download first: a curl failure in a pipe is invisible to `sh` without
    # pipefail, which POSIX sh does not have.
    DOCKER_INSTALLER="$(mktemp)"
    if ! curl -fsSL https://get.docker.com -o "$DOCKER_INSTALLER"; then
      rm -f "$DOCKER_INSTALLER"
      log_err "Downloading https://get.docker.com failed."
      exit 1
    fi
    if ! run_elevated sh "$DOCKER_INSTALLER"; then
      rm -f "$DOCKER_INSTALLER"
      log_err "The Docker install script failed."
      exit 1
    fi
    rm -f "$DOCKER_INSTALLER"
    if [ "$(id -u)" -ne 0 ] && command -v usermod >/dev/null 2>&1; then
      if ! run_elevated usermod -aG docker "$USER"; then
        log_warn "Could not add ${USER} to the docker group; the wizard will continue under sudo."
      fi
    fi
    if command -v systemctl >/dev/null 2>&1; then
      run_elevated systemctl enable --now docker || true
    fi
    command -v docker >/dev/null 2>&1 || { log_err "Docker is still missing after installation."; exit 1; }
    log_ok "Docker installed successfully."
  else
    log_err "Docker is required to run the relay pool. Please install Docker and retry."
    exit 1
  fi
fi

# Check Docker Compose (plugin or standalone)
DOCKER_COMPOSE=""
if docker compose version >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  DOCKER_COMPOSE="docker-compose"
else
  log_info "Installing Docker Compose plugin..."
  if command -v apt-get >/dev/null 2>&1; then
    run_elevated apt-get update && run_elevated apt-get install -y docker-compose-plugin
  elif command -v dnf >/dev/null 2>&1; then
    run_elevated dnf install -y docker-compose-plugin
  elif command -v yum >/dev/null 2>&1; then
    run_elevated yum install -y docker-compose-plugin
  fi

  if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
  else
    log_err "Docker Compose plugin could not be auto-installed."
    echo "Please install it manually: sudo apt-get install -y docker-compose-plugin"
    exit 1
  fi
fi
# The daemon socket needs group membership that a fresh `usermod` does not
# apply to this shell. `docker compose version` is client-side and passes
# anyway, so check the daemon itself and re-exec under sudo when needed.
if ! docker info >/dev/null 2>&1; then
  if [ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1; then
    log_info "Docker daemon not reachable as $(id -un); re-running the wizard under sudo..."
    exec sudo -E sh "$0" "$@"
  fi
  log_err "Cannot reach the Docker daemon."
  log_err "Re-login after 'usermod -aG docker $USER', or run this script with sudo."
  exit 1
fi
log_ok "Docker and Docker Compose verified ($DOCKER_COMPOSE)."

# Auto-install Node.js if missing (for keep-cold rotation prober)
NODE_BIN="$(command -v node 2>/dev/null || true)"
if [ -z "$NODE_BIN" ]; then
  log_warn "Node.js (>=18) is needed for the keep-cold auto-rotation prober."
  DO_INSTALL_NODE=true
  if [ "$AUTO_YES" != true ]; then
    printf "Install Node.js on the VPS for rate-limit auto-rotation? [Y/n]: "
    read -r ans || true
    case "$ans" in
      [nN]*) DO_INSTALL_NODE=false ;;
      *) DO_INSTALL_NODE=true ;;
    esac
  fi

  if [ "$DO_INSTALL_NODE" = true ]; then
    log_info "Installing Node.js..."
    if command -v apt-get >/dev/null 2>&1; then
      NODE_INSTALLER="$(mktemp)"
      if curl -fsSL https://deb.nodesource.com/setup_22.x -o "$NODE_INSTALLER"; then
        run_elevated bash "$NODE_INSTALLER"
        rm -f "$NODE_INSTALLER"
        run_elevated apt-get install -y nodejs
      else
        rm -f "$NODE_INSTALLER"
        log_warn "Could not download the NodeSource setup script."
      fi
    elif command -v dnf >/dev/null 2>&1; then
      run_elevated dnf module install -y nodejs:22 || run_elevated dnf install -y nodejs
    elif command -v apk >/dev/null 2>&1; then
      run_elevated apk add nodejs
    fi
    NODE_BIN="$(command -v node 2>/dev/null || true)"
    if [ -n "$NODE_BIN" ]; then
      log_ok "Node.js installed at $NODE_BIN ($("$NODE_BIN" -v))."
    fi
  fi
else
  log_ok "Node.js found at $NODE_BIN ($("$NODE_BIN" -v))."
fi

# -------------------------------------------------------------
# 2. Hardware Inspection & Dynamic Sizing
# -------------------------------------------------------------
TOTAL_RAM_MB=1024
AVAIL_RAM_MB=512
CPU_CORES=1

if [ -f /proc/meminfo ]; then
  TOTAL_RAM_KB="$(grep -i MemTotal /proc/meminfo | awk '{print $2}')"
  TOTAL_RAM_MB=$((TOTAL_RAM_KB / 1024))
  AVAIL_RAM_KB="$(grep -i MemAvailable /proc/meminfo | awk '{print $2}' || true)"
  if [ -n "$AVAIL_RAM_KB" ] && [ "$AVAIL_RAM_KB" -gt 0 ]; then
    AVAIL_RAM_MB=$((AVAIL_RAM_KB / 1024))
  else
    AVAIL_RAM_MB=$((TOTAL_RAM_MB / 2))
  fi
fi

if command -v nproc >/dev/null 2>&1; then
  CPU_CORES="$(nproc)"
fi

# Memory calculation:
# MicroWARP (~1.8 MiB) + TryCloudflare Tunnel (~20 MiB) = ~22 MiB per relay node.
# Relay router + prober base: ~50 MiB.
# Safe max = available RAM minus 80 MB OS cushion, divided by 22 MiB.
USABLE_RAM=$((AVAIL_RAM_MB - 80))
if [ "$USABLE_RAM" -lt 66 ]; then
  MAX_RELAYS=3
else
  MAX_RELAYS=$((USABLE_RAM / 22))
fi

# Cap at 64 max
if [ "$MAX_RELAYS" -gt 64 ]; then
  MAX_RELAYS=64
fi
if [ "$MAX_RELAYS" -lt 3 ]; then
  MAX_RELAYS=3
fi

# Recommended count: 5 relays is the sweet spot for most users!
# (5 independent egress IPs with auto rate-limit rotation provide huge throughput without wasting memory)
REC_COUNT=5
if [ "$REC_COUNT" -gt "$MAX_RELAYS" ]; then
  REC_COUNT=$MAX_RELAYS
fi

# Check Tailscale
TAILSCALE_DETECTED=false
TS_IP=""
if command -v tailscale >/dev/null 2>&1; then
  TS_IP="$(tailscale ip -4 2>/dev/null | head -n 1 || true)"
  if [ -n "$TS_IP" ]; then
    TAILSCALE_DETECTED=true
  fi
fi

echo ""
echo "-----------------------------------------------------------------"
echo "  VPS Hardware Detected:"
echo "    • Total Memory:     ${TOTAL_RAM_MB} MB"
echo "    • Available Memory: ${AVAIL_RAM_MB} MB"
echo "    • CPU Cores:        ${CPU_CORES}"
echo ""
echo "  Relay Pool Sizing Guide (MicroWARP + TryCloudflare Tunnels):"
echo "    • Recommended:      ${REC_COUNT} relays (~160 MB total RAM)"
echo "      (5 relays + auto-rotation is ideal for heavy parallel coding agent turns)"
echo "    • Max Safe:         ${MAX_RELAYS} relays on this VPS (~$((MAX_RELAYS * 22 + 50)) MB RAM)"
echo "-----------------------------------------------------------------"
echo ""

# -------------------------------------------------------------
# 3. Interactive Relay Selection
# -------------------------------------------------------------
# Re-running the wizard on an existing host must not silently shrink the pool
# (Docker leaves the removed warp containers and their volumes running).
# Adopt the deployed shape unless COUNT/INGRESS are pinned explicitly.
DEPLOYED_COUNT="$(docker inspect warp-relay-app --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null | sed -n 's/^RELAY_COUNT=\([0-9][0-9]*\)$/\1/p' | head -n 1 || true)"
DEPLOYED_INGRESS=""
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx 'warp-relay-tunnel-1'; then
  DEPLOYED_INGRESS="tunnels"
elif grep -q '^ *- RELAY_DIRECT_PUBLIC=1' docker-compose.yml 2>/dev/null; then
  DEPLOYED_INGRESS="direct"
elif docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx 'warp-relay-app'; then
  DEPLOYED_INGRESS="tailscale"
fi
# The binds are part of the deployed shape too: a re-run must not silently
# re-publish a deliberately exposed WARP_BIND (or a custom router bind) as
# loopback. The compose file is the deployed truth; explicit env wins.
if [ -z "${WARP_BIND:-}" ] && [ -f docker-compose.yml ]; then
  WARP_BIND="$(sed -n 's/^ *- "\([^":]*\):[0-9][0-9]*:1080"$/\1/p' docker-compose.yml 2>/dev/null | head -n 1)"
  [ -n "$WARP_BIND" ] && export WARP_BIND
fi
if [ -z "${INGRESS_BIND:-}" ] && [ -f docker-compose.yml ]; then
  # The router is the only published non-SOCKS port, so match the line that is
  # not a 1080 mapping instead of hard-coding the port number.
  INGRESS_BIND="$(sed -n '/:1080"$/! s/^ *- "\([^":]*\):[0-9][0-9]*:[0-9][0-9]*"$/\1/p' docker-compose.yml 2>/dev/null | head -n 1)"
  [ -n "$INGRESS_BIND" ] && export INGRESS_BIND
fi
if [ -z "${ROUTER_PORT:-}" ] && [ -f docker-compose.yml ]; then
  ROUTER_PORT="$(sed -n '/:1080"$/! s/^ *- "\([^":]*\):\([0-9][0-9]*\):[0-9][0-9]*"$/\2/p' docker-compose.yml 2>/dev/null | head -n 1)"
  [ -n "$ROUTER_PORT" ] && export ROUTER_PORT
fi

COUNT_FROM_DEPLOY=0
if [ -z "${COUNT:-}" ] && [ -n "$DEPLOYED_COUNT" ]; then
  COUNT="$DEPLOYED_COUNT"
  COUNT_FROM_DEPLOY=1
else
  COUNT="${COUNT:-$REC_COUNT}"
fi
case "$COUNT" in
  ''|*[!0-9]*)
    log_warn "COUNT='${COUNT}' is not a number; using the recommended ${REC_COUNT}."
    COUNT="$REC_COUNT"
    ;;
esac
if [ "$COUNT" -gt "$MAX_RELAYS" ]; then
  if [ "$COUNT_FROM_DEPLOY" -eq 1 ]; then
    # The stack already runs this size; free RAM is low precisely because of
    # it. Shrinking here would orphan containers/volumes on a re-run.
    log_warn "Deployed count ${COUNT} is above the fresh-install recommendation (${MAX_RELAYS}); keeping it."
  else
    log_warn "COUNT=${COUNT} exceeds the safe maximum for this VPS (${MAX_RELAYS}); using ${MAX_RELAYS}."
    COUNT="$MAX_RELAYS"
  fi
fi
INGRESS_CHOICE="${INGRESS:-${DEPLOYED_INGRESS:-tunnels}}" # TryCloudflare is default for "just works" public access

PROMPT_MAX="$MAX_RELAYS"
if [ "$COUNT" -gt "$PROMPT_MAX" ]; then
  PROMPT_MAX="$COUNT"
fi

if [ "$AUTO_YES" = true ]; then
  log_info "Non-interactive mode (-y): using count = ${COUNT} relays."
  case "$INGRESS_CHOICE" in
    2|tailscale) log_info "Ingress method: Mesh-VPN direct (router bind resolved at generation)." ;;
    3|direct) log_info "Ingress method: Direct Public IP (0.0.0.0, proxy-auth enforced)." ;;
    *) log_info "Ingress method: TryCloudflare Quick Tunnels (public HTTPS URLs)." ;;
  esac
else
  printf "How many relays do you want to create? [1-%s, default: %s]: " "$PROMPT_MAX" "$COUNT"
  read -r user_count || true
  if [ -n "$user_count" ]; then
    COUNT="$user_count"
    COUNT_FROM_DEPLOY=0
  fi

  echo ""
  echo "--- Choose Ingress Exposure ---"
  DEFAULT_INGRESS_NUM=1
  case "$INGRESS_CHOICE" in
    2|tailscale) DEFAULT_INGRESS_NUM=2 ;;
    3|direct) DEFAULT_INGRESS_NUM=3 ;;
  esac
  if [ "$TAILSCALE_DETECTED" = true ]; then
    echo "  1) TryCloudflare Quick Tunnels (Recommended: public HTTPS, zero client VPN needed)"
    echo "  2) Tailscale Mesh-VPN Direct (Private static URL via ${TS_IP})"
    echo "  3) Direct Public IP (binds the router to 0.0.0.0; proxy credentials REQUIRED)"
    printf "Choose ingress method [1/2/3, default: %s]: " "$DEFAULT_INGRESS_NUM"
    read -r user_ingress || true
    if [ -n "$user_ingress" ]; then
      case "$user_ingress" in
        2) INGRESS_CHOICE="tailscale" ;;
        1) INGRESS_CHOICE="tunnels" ;;
        3) INGRESS_CHOICE="direct" ;;
      esac
    fi
  else
    echo "  1) TryCloudflare Quick Tunnels (Recommended: public HTTPS, zero client configuration)"
    echo "  2) Tailscale Mesh-VPN Direct (requires Tailscale on VPS + local machine)"
    echo "  3) Direct Public IP (binds the router to 0.0.0.0; proxy credentials REQUIRED)"
    printf "Choose ingress method [1/2/3, default: %s]: " "$DEFAULT_INGRESS_NUM"
    read -r user_ingress || true
    if [ -n "$user_ingress" ]; then
      case "$user_ingress" in
        2) INGRESS_CHOICE="tailscale" ;;
        1) INGRESS_CHOICE="tunnels" ;;
        3) INGRESS_CHOICE="direct" ;;
      esac
    fi
  fi
fi

# Interactive input can exceed the safe maximum (the clamp above only covers
# env/-y paths); re-validate the resolved count before anything uses it.
case "$COUNT" in
  ''|*[!0-9]*)
    log_warn "COUNT='${COUNT}' is not a number; using the recommended ${REC_COUNT}."
    COUNT="$REC_COUNT"
    ;;
esac
if [ "$COUNT" -gt "$MAX_RELAYS" ]; then
  if [ "$COUNT_FROM_DEPLOY" -eq 1 ]; then
    log_warn "Deployed count ${COUNT} is above the fresh-install recommendation (${MAX_RELAYS}); keeping it."
  else
    log_warn "COUNT=${COUNT} exceeds the safe maximum for this VPS (${MAX_RELAYS}); using ${MAX_RELAYS}."
    COUNT="$MAX_RELAYS"
  fi
fi
if [ "$COUNT" -lt 1 ]; then
  COUNT=1
fi

WITH_TUNNELS_FLAG=""
if [ "$INGRESS_CHOICE" = "tunnels" ] || [ "$INGRESS_CHOICE" = "1" ]; then
  WITH_TUNNELS_FLAG="--with-tunnels"
  log_info "Selected: TryCloudflare Quick Tunnels (${COUNT} relays)."
else
  if [ "$INGRESS_CHOICE" = "direct" ]; then
    # Public router bind: every dispatch (exact origins included) and CONNECT
    # is gated on HTTP Basic Proxy-Authorization. Health stays exempt.
    INGRESS_BIND="0.0.0.0"
    export INGRESS_BIND
    RELAY_DIRECT_PUBLIC=1
    export RELAY_DIRECT_PUBLIC
    gen_pass() {
      openssl rand -hex 12 2>/dev/null \
        || head -c 16 /dev/urandom 2>/dev/null | od -An -tx1 2>/dev/null | tr -d ' \n' \
        || date +%s%N | sha256sum 2>/dev/null | cut -c1-24
    }
    if [ -z "${RELAY_PROXY_AUTH:-}" ]; then
      if [ "$AUTO_YES" = true ]; then
        RELAY_PROXY_AUTH="runner:$(gen_pass)"
        log_info "Auto-generated proxy credentials: ${RELAY_PROXY_AUTH}"
      else
        printf "Proxy username [runner]: "
        read -r BASIC_USER || true
        BASIC_USER="${BASIC_USER:-runner}"
        printf "Proxy password (blank = auto-generate): "
        read -r BASIC_PASS || true
        if [ -z "$BASIC_PASS" ]; then
          BASIC_PASS="$(gen_pass)"
          log_info "Generated password: ${BASIC_PASS}"
        fi
        RELAY_PROXY_AUTH="${BASIC_USER}:${BASIC_PASS}"
      fi
    fi
    export RELAY_PROXY_AUTH
    # Persist to .env so later `./generate-compose.sh N` regenerations keep
    # the same credentials + direct mode instead of demanding the env
    # outlive this shell.
    if [ -f .env ] && grep -q '^RELAY_PROXY_AUTH=' .env; then
      sed -i.bak "s|^RELAY_PROXY_AUTH=.*|RELAY_PROXY_AUTH=${RELAY_PROXY_AUTH}|" .env && rm -f .env.bak
    else
      printf '\nRELAY_PROXY_AUTH=%s\n' "$RELAY_PROXY_AUTH" >> .env
    fi
    # Credentials live in a world-readable file by default (umask 022); a
    # public ingress password should be owner-only.
    chmod 600 .env 2>/dev/null || true
    if [ -f .env ] && grep -q '^RELAY_DIRECT_PUBLIC=' .env; then
      sed -i.bak "s|^RELAY_DIRECT_PUBLIC=.*|RELAY_DIRECT_PUBLIC=1|" .env && rm -f .env.bak
    else
      printf 'RELAY_DIRECT_PUBLIC=1\n' >> .env
    fi
    log_info "Selected: Direct Public IP (${COUNT} relays). Router on 0.0.0.0:310N — proxy-auth enforced."
    log_warn "Public ingress: keep WARP_BIND on loopback (default) and treat the"
    log_warn "credentials like a password — anyone with them shares your egresses."
  elif [ -n "$TS_IP" ]; then
    INGRESS_BIND="$TS_IP"
    export INGRESS_BIND
    log_info "Relay router will be published on ${TS_IP} only (Tailscale interface)."
  else
    log_warn "Tailscale IP not detected; relay router will stay on 127.0.0.1."
    log_warn "Authenticate Tailscale, then re-run: INGRESS_BIND=<tailscale-ip> sh ./generate-compose.sh ${COUNT}"
  fi
fi

# -------------------------------------------------------------
# 4. Build Minimal MicroWARP & Relay Images
# -------------------------------------------------------------
# Preflight: the host uplink must carry the lane's encapsulated packet
# (WG_MTU inner + ~80 outer). A lane is dual-stack, so its MTU cannot go below
# 1280 (RFC 8200) — meaning the host uplink has to be at least ~1360. WSL2's
# vNIC defaults to 1280, and the failure is silent: the lane reports healthy
# in-container while every client HTTPS request stalls in the TLS handshake
# (plain HTTP still works). Catch it here instead of after an hour of debugging.
check_uplink_mtu() {
  up_dev="$(ip route show default 2>/dev/null | awk '{print $5; exit}')"
  [ -n "$up_dev" ] || return 0
  up_mtu="$(cat "/sys/class/net/${up_dev}/mtu" 2>/dev/null || echo "")"
  case "$up_mtu" in ''|*[!0-9]*) return 0 ;; esac
  if [ "$up_mtu" -lt 1360 ]; then
    log_warn "Host uplink ${up_dev} has MTU ${up_mtu}; a WARP lane needs about 1360."
    log_warn "Clients will stall mid-TLS-handshake through the pool (HTTP may still work)."
    log_warn "This is a host/WSL2 limit, not a pool misconfiguration. If this is WSL2:"
    log_warn "  sudo ip link set dev ${up_dev} mtu 1500   # re-run after each WSL restart"
    log_warn "Do NOT lower WARP_MTU instead: lanes are dual-stack and 1280 is the floor."
    log_warn "A normal VPS uplink is already 1500 and needs nothing here."
  else
    log_ok "Host uplink ${up_dev} MTU ${up_mtu} can carry a full-size WARP lane."
  fi
}
check_uplink_mtu

# The generated compose intentionally drops SYS_MODULE. Ensure the host loads
# the WireGuard module at boot so a reboot does not leave wg-quick unable to
# create its interface.
if command -v systemctl >/dev/null 2>&1 && { [ "$(id -u)" -eq 0 ] || command -v sudo >/dev/null 2>&1; }; then
  if run_elevated sh -c 'mkdir -p /etc/modules-load.d && { grep -qx wireguard /etc/modules-load.d/wireguard.conf 2>/dev/null || echo wireguard > /etc/modules-load.d/wireguard.conf; }'; then
    log_info "WireGuard module will load at boot (/etc/modules-load.d/wireguard.conf)."
  else
    log_warn "Could not persist the WireGuard module load; verify 'modprobe wireguard' after reboot."
  fi
fi

# Build once. Rebuilding a byte-identical Dockerfile still produces a new
# image digest, and `compose up` then recreates every warp container — which
# means 10 fresh Cloudflare registrations and 10 pending handshakes on each
# wizard re-run. Set WARP_REBUILD=1 to force a rebuild.
if [ "${WARP_REBUILD:-0}" = "1" ] || ! docker image inspect warp-lite:latest >/dev/null 2>&1; then
  log_info "Building minimal MicroWARP image (warp-lite:latest, ~1.8 MiB RAM/node)..."
  docker build -t warp-lite:latest ./warp-lite
else
  log_info "Reusing existing warp-lite:latest image (WARP_REBUILD=1 to rebuild)."
fi

log_info "Generating docker-compose.yml for ${COUNT} relays..."
# shellcheck disable=SC2086
sh ./generate-compose.sh "$COUNT" $WITH_TUNNELS_FLAG

# Shared scheduler state: the relay app bind-mounts ./.keep-cold at /signals
# and writes signals.json as container root, while the host keep-cold prober
# (User=trefeon) writes state.json here. 0777 is deliberate and safe: the dir
# holds relay indexes, model names and timestamps only — no egress IPs, no
# secrets — and both writers replace files atomically (tmp + rename), so a
# root-owned signal file never blocks the host prober. The wizard may already
# be running as root (daemon re-exec), which is exactly why the mode is what
# grants access instead of ownership.
install -d -m 0777 .keep-cold

log_info "Starting containers with Docker Compose..."
# Build only the relay app (its sources change with the checkout); the warp
# services reuse the cached warp-lite image so their containers — and their
# established WireGuard handshakes — survive a wizard re-run.
$DOCKER_COMPOSE up -d --build relay
$DOCKER_COMPOSE up -d --remove-orphans

# -------------------------------------------------------------
# 5. Wait for Readiness and Public URLs
# -------------------------------------------------------------
# Health only proves a handshake exists; it does not prove the tunnel
# forwards. A lane with a stale wg0.conf sits at handshake=0 and answers
# nothing, which used to be reported as "SETUP COMPLETE" while clients
# timed out. Verify each lane against Cloudflare's trace and re-register a
# lane that has no handshake at all — exactly once, since every re-registration
# spends one Cloudflare registration and must never loop.
verify_lanes() {
  failed=""
  i=1
  while [ "$i" -le "$COUNT" ]; do
    lane="warp-relay-warp-${i}"
    trace="$(docker exec "$lane" curl -s -m 8 https://cloudflare.com/cdn-cgi/trace 2>/dev/null || true)"
    if printf '%s\n' "$trace" | grep -qx 'warp=on'; then
      log_ok "lane ${i}: OK"
    elif [ -n "$(docker exec "$lane" wg show wg0 latest-handshakes 2>/dev/null | awk 'NR==1 && $2+0 > 0 {print $2}')" ]; then
      # A handshake means the profile is valid; a failed trace is transient
      # (endpoint congestion) and re-registering cannot fix routing.
      log_warn "lane ${i}: handshake ok but egress trace failed (transient)"
    else
      log_warn "lane ${i}: no handshake — re-registering once"
      docker exec "$lane" rm -f /etc/wireguard/wg0.conf >/dev/null 2>&1 || true
      docker restart "$lane" >/dev/null 2>&1 || true
      sleep 20
      trace="$(docker exec "$lane" curl -s -m 8 https://cloudflare.com/cdn-cgi/trace 2>/dev/null || true)"
      if printf '%s\n' "$trace" | grep -qx 'warp=on'; then
        log_ok "lane ${i}: RECOVERED"
      else
        log_err "lane ${i}: FAILED — inspect: docker logs --tail 40 ${lane}"
        failed="${failed} ${i}"
      fi
    fi
    i=$((i + 1))
  done
  if [ -n "$failed" ]; then
    log_warn "Lanes failing verification:${failed}. The router skips them; fix them before trusting the pool."
  fi
}

if [ -n "$WITH_TUNNELS_FLAG" ]; then
  log_info "Waiting for TryCloudflare Quick Tunnels to mint public URLs (approx 20-30s)..."
  attempt=0
  max_attempts=15
  tunnels_ready=false

  while [ "$attempt" -lt "$max_attempts" ]; do
    sleep 3
    attempt=$((attempt + 1))
    # Check if first tunnel URL is printed in docker logs
    first_url="$(docker logs warp-relay-tunnel-1 2>&1 | grep -oE 'https://[a-zA-Z0-9.-]+\.trycloudflare\.com' | tail -n 1 || true)"
    if [ -n "$first_url" ]; then
      tunnels_ready=true
      break
    fi
    printf "."
  done
  echo ""
  if [ "$tunnels_ready" = true ]; then
    log_ok "Quick Tunnels established successfully!"
  else
    log_warn "Tunnels are still connecting in the background..."
  fi
else
  log_info "Waiting for WARP WireGuard handshakes..."
  # `compose up` returns as soon as the containers exist; the tunnel needs a
  # handshake after that. Poll container health instead of a fixed sleep so
  # the status table below reflects reality, capped so a dead relay still
  # prints rather than hanging the wizard.
  handshake_wait=0
  while [ "$handshake_wait" -lt 120 ]; do
    unhealthy=0
    i=1
    while [ "$i" -le "$COUNT" ]; do
      state="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "warp-relay-warp-${i}" 2>/dev/null || echo missing)"
      case "$state" in healthy|none) ;; *) unhealthy=$((unhealthy + 1)) ;; esac
      i=$((i + 1))
    done
    [ "$unhealthy" -eq 0 ] && break
    sleep 5
    handshake_wait=$((handshake_wait + 5))
  done
  if [ "$unhealthy" -ne 0 ]; then
    log_warn "${unhealthy} relay(s) still unhealthy after ${handshake_wait}s — keep-cold will retry them."
  else
    log_ok "All ${COUNT} WARP tunnels handshaked."
  fi
fi

verify_lanes

# -------------------------------------------------------------
# 6. Install Keep-Cold Auto-Rotation Service (Systemd)
# -------------------------------------------------------------
NODE_MAJOR=0
if [ -n "$NODE_BIN" ]; then
  NODE_MAJOR="$("$NODE_BIN" -p 'process.versions.node.split(".")[0]' 2>/dev/null || echo 0)"
fi
# A full sweep probes every relay and rotates each throttled one until it
# streams again. At 20 relays a throttled round realistically needs more than
# 5 minutes, so the timer period is sized to the pool: 5 minutes up to 10
# relays, 15 minutes above that. flock -n still skips a fire that arrives while
# the previous sweep is running. The 2-minute default for small pools keeps
# repair latency tight; incremental passes only probe suspects + the audit
# sample, so the tick stays cheap steady-state.
if [ "${SWEEP_INTERVAL_MIN:-}" = "" ]; then
  if [ "${COUNT:-5}" -gt 10 ]; then SWEEP_INTERVAL_MIN=15; else SWEEP_INTERVAL_MIN=2; fi
fi
if [ -n "$NODE_BIN" ] && [ "$NODE_MAJOR" -lt 18 ]; then
  log_warn "Node.js $("$NODE_BIN" -v) is older than 18; the prober needs >=18. Skipping the timers."
fi

if [ -n "$NODE_BIN" ] && [ "$NODE_MAJOR" -ge 18 ] && command -v systemctl >/dev/null 2>&1; then
  SETUP_SYSTEMD=true
  if [ "$AUTO_YES" != true ]; then
    echo ""
    printf "Enable keep-cold + logwatch timers to auto-rotate rate-limited (429) relays? [Y/n]: "
    read -r ans || true
    case "$ans" in
      [nN]*) SETUP_SYSTEMD=false ;;
      *) SETUP_SYSTEMD=true ;;
    esac
  fi

  if [ "$SETUP_SYSTEMD" = true ]; then
    log_info "Configuring systemd keep-cold and logwatch services..."
    # keep-cold's probe transport REQUIRES wreq-js (Bun-fingerprint ClientHello
    # through the lane egress - the same simulation the clients perform); a
    # probe verdict on any other TLS identity would be a lie, so missing wreq-js
    # makes keep-cold exit fatally rather than probe with degraded semantics.
    CURRENT_DIR="$(pwd)"
    # --include=dev: wreq-js is a devDependency (the relay image must not carry
    # it), but the host prober imports it, so a NODE_ENV=production host would
    # otherwise install nothing and keep-cold would exit fatally.
    if (cd "$CURRENT_DIR" && npm install --include=dev --no-audit --no-fund --loglevel=error >/dev/null 2>&1); then
      log_info "keep-cold dependencies (wreq-js probe transport) installed."
    else
      log_warn "npm install failed - install wreq-js manually (cd ${CURRENT_DIR} && npm install); without it keep-cold refuses to probe (fatal, not fallback)."
    fi
    CURRENT_USER="$(id -un)"
    CURRENT_GROUP="$(id -gn)"

    TMP_KEEP="$(mktemp)"
    sed -e "s|^User=.*|User=${CURRENT_USER}|" \
        -e "s|^Group=.*|Group=${CURRENT_GROUP}|" \
        -e "s|^WorkingDirectory=.*|WorkingDirectory=${CURRENT_DIR}|" \
        -e "s|^Documentation=.*|Documentation=file:${CURRENT_DIR}/keep-cold.mjs|" \
        -e "s|^Environment=KEEP_COLD_LOG=.*|Environment=KEEP_COLD_LOG=${CURRENT_DIR}/keep-cold.log|" \
        -e "s|^Environment=COUNT=.*|Environment=COUNT=${COUNT}|" \
        -e "s|^Environment=BASE_PORT=.*|Environment=BASE_PORT=${BASE_PORT:-3000}|" \
        -e "s|^Environment=RELAY_PROXY_AUTH=.*|Environment=RELAY_PROXY_AUTH=${RELAY_PROXY_AUTH:-}|" \
        -e "s|^ExecStart=.*|ExecStart=/usr/bin/flock -n -E 0 /tmp/warp-keep-cold.lock ${NODE_BIN} ${CURRENT_DIR}/keep-cold.mjs --sweep|" \
        systemd/warp-keep-cold.service > "$TMP_KEEP"

    TMP_COLD_TIMER="$(mktemp)"
    sed -e "s|^OnCalendar=.*|OnCalendar=*:0/${SWEEP_INTERVAL_MIN}|" \
        -e "s|^Description=.*|Description=Run the WARP keep-cold full sweep every ${SWEEP_INTERVAL_MIN} minutes|" \
        systemd/warp-keep-cold.timer > "$TMP_COLD_TIMER"

    # The signals watcher runs for the life of the host and spawns the locked
    # one-pass worker on every signal-file write, so a 429 is acted on in
    # milliseconds rather than waiting for the next minute tick.
    TMP_SIGNALS="$(mktemp)"
    sed -e "s|^User=.*|User=${CURRENT_USER}|" \
        -e "s|^Group=.*|Group=${CURRENT_GROUP}|" \
        -e "s|^WorkingDirectory=.*|WorkingDirectory=${CURRENT_DIR}|" \
        -e "s|^Documentation=.*|Documentation=file:${CURRENT_DIR}/keep-cold.mjs|" \
        -e "s|^Environment=COUNT=.*|Environment=COUNT=${COUNT}|" \
        -e "s|^Environment=BASE_PORT=.*|Environment=BASE_PORT=${BASE_PORT:-3000}|" \
        -e "s|^Environment=RELAY_PROXY_AUTH=.*|Environment=RELAY_PROXY_AUTH=${RELAY_PROXY_AUTH:-}|" \
        -e "s|^ExecStart=.*|ExecStart=${NODE_BIN} ${CURRENT_DIR}/keep-cold.mjs --watch-signals|" \
        systemd/warp-keep-cold-signals.service > "$TMP_SIGNALS"

    TMP_WATCH="$(mktemp)"
    sed -e "s|^User=.*|User=${CURRENT_USER}|" \
        -e "s|^Group=.*|Group=${CURRENT_GROUP}|" \
        -e "s|^WorkingDirectory=.*|WorkingDirectory=${CURRENT_DIR}|" \
        -e "s|^Documentation=.*|Documentation=file:${CURRENT_DIR}/logwatch.sh|" \
        -e "s|^Environment=COUNT=.*|Environment=COUNT=${COUNT}|" \
        -e "s|^ExecStart=.*|ExecStart=/usr/bin/flock -n -E 0 /tmp/warp-logwatch.lock /bin/sh ${CURRENT_DIR}/logwatch.sh|" \
        systemd/warp-logwatch.service > "$TMP_WATCH"

    if run_elevated cp "$TMP_KEEP" /etc/systemd/system/warp-keep-cold.service \
       && run_elevated cp "$TMP_COLD_TIMER" /etc/systemd/system/warp-keep-cold.timer \
       && run_elevated cp "$TMP_SIGNALS" /etc/systemd/system/warp-keep-cold-signals.service \
       && run_elevated rm -f /etc/systemd/system/warp-keep-cold-signals.timer \
       && run_elevated cp "$TMP_WATCH" /etc/systemd/system/warp-logwatch.service \
       && run_elevated cp systemd/warp-logwatch.timer /etc/systemd/system/warp-logwatch.timer \
       && run_elevated systemctl daemon-reload \
       && run_elevated systemctl enable --now warp-keep-cold.timer warp-keep-cold-signals.service warp-logwatch.timer; then
      log_ok "Keep-cold full sweep every ${SWEEP_INTERVAL_MIN} minutes + signal watcher + logwatch active (COUNT=${COUNT})."
    else
      log_warn "systemd units were not installed; run 'node keep-cold.mjs --sweep' manually or re-run with sudo."
    fi
    rm -f "$TMP_KEEP" "$TMP_COLD_TIMER" "$TMP_SIGNALS" "$TMP_WATCH"
  fi
fi

# -------------------------------------------------------------
# 7. Final output — the one relay URL
# -------------------------------------------------------------
echo ""
sh ./get-status.sh "$COUNT"

echo ""
echo "======================================================================="
printf "%s\n" "${GREEN}${BOLD}SETUP COMPLETE${RESET}"
echo "======================================================================="
echo "Dial the single relay URL printed above. It fronts all ${COUNT} relays:"
echo "every request is routed to a lane, and a throttled or dead lane is"
echo "skipped without the client noticing."
echo ""
echo "Health check (returns the egress IP of whichever lane answered):"
echo "  curl -s -x <relay-url> https://cloudflare.com/cdn-cgi/trace"
echo ""
echo "Rotation: warp-keep-cold.timer sweeps all ${COUNT} relays every"
echo "   ${SWEEP_INTERVAL_MIN} minutes, and warp-keep-cold-signals.service watches"
echo "   429 signals as they land, so a freshly throttled egress is rotated"
echo "   long before the next sweep. A rotated relay gets a new egress and"
echo "   rejoins the pool automatically."
echo "======================================================================="
echo ""
