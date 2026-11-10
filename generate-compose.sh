#!/bin/sh
# Generate docker-compose.yml for N relays.
#
# Usage: ./generate-compose.sh [count] [--with-tunnels] [--legacy-warp]
#   count: integer 1..64 (default COUNT env, else 10)
#
# Default (pure) mode: warp-1..N on minimal MicroWARP (warp-lite: ~1.8 MiB RAM each),
# each with `ports: "<10800+i>:1080"` (raw proxy), plus one relay app at 10.244.0.100
# with `ports: "<ROUTER_PORT=3100>:3000"` for mesh-VPN direct ingress. One listener
# serves the whole pool; a probe names its lane in a header, not by port.
# ROUTER_PORT moves that one public port when 3100 is already taken on the host.
# With --with-tunnels: appends one tunnel-0 (Quick Tunnel -> http://10.244.0.100:3000).
# With --legacy-warp: uses caomingjun/warp:latest (~100 MiB each) instead of warp-lite.
# Rotation rule: restart warp-N only, never tunnel-N/relay.
set -eu

# Load .env when present. Only KEY=VALUE lines with shell-safe key names are
# applied, and values already present in the process environment win — so
# `COUNT=5 ./generate-compose.sh` and setup.sh prompts are never overridden.
# The relevant values are baked into the generated compose file, which is why
# editing .env alone changes nothing until this script runs again.
if [ -f .env ]; then
  cr="$(printf '\r')"
  dq='"' # quote characters, so the case patterns need no escapes
  sq="'"
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in ''|'#'*) continue ;; esac
    # A value-less line is not a KEY=VALUE assignment — skip it instead of
    # self-assigning KEY=KEY.
    case "$line" in *=*) ;; *) continue ;; esac
    key="${line%%=*}"
    val="${line#*=}"
    # Windows-authored files may carry CRLF: a stray CR in a baked value
    # (URL, comma-list) would silently break matching downstream.
    val="${val%"$cr"}"
    # Shell-identifier keys only; a leading digit would alias positional
    # parameters or die under `set -u` despite the skip contract.
    case "$key" in ''|[!A-Za-z_]*|*[!A-Za-z0-9_]*) continue ;; esac
    case "$val" in
      "$dq"*) val="${val#"$dq"}"; val="${val%%"$dq"*}" ;;
      "$sq"*) val="${val#"$sq"}"; val="${val%%"$sq"*}" ;;
      # Bare values follow compose's env-file behaviour: a whitespace-preceded
      # `#…` starts an inline comment and is stripped. Quoted values run up to
      # the closing quote, with any trailing comment ignored; a ` #` inside the
      # quotes is part of the value.
      *) val="${val%%[[:space:]]#*}" ;;
    esac
    # shellcheck disable=SC2163,SC2086 # key is validated above
    eval "cur=\${$key:-}"
    [ -n "$cur" ] || export "$key=$val"
  done < .env
fi

COUNT="${COUNT:-10}"
WITH_TUNNELS=false
USE_LITE=true
for arg in "$@"; do
  case "$arg" in
    --with-tunnels) WITH_TUNNELS=true ;;
    --legacy-warp) USE_LITE=false ;;
    *) COUNT="$arg" ;;
  esac
done


case "$COUNT" in
  ''|*[!0-9]*)
    echo "error: COUNT must be an integer 1..64, got '$COUNT'" >&2
    exit 1
    ;;
esac
if [ "$COUNT" -lt 1 ] || [ "$COUNT" -gt 64 ]; then
  echo "error: COUNT must be an integer 1..64, got '$COUNT'" >&2
  exit 1
fi
if [ "$WITH_TUNNELS" = true ]; then TUNNEL_FLAG=" --with-tunnels"; else TUNNEL_FLAG=""; fi

BASE_PORT="${BASE_PORT:-3000}"
case "$BASE_PORT" in
  ''|*[!0-9]*)
    echo "error: BASE_PORT must be an integer 1..65535, got '$BASE_PORT'" >&2
    exit 1
    ;;
esac
if [ "$BASE_PORT" -lt 1 ] || [ "$BASE_PORT" -gt 65535 ]; then
  echo "error: BASE_PORT must be <= 65535, got $BASE_PORT" >&2
  exit 1
fi
# Direct public-IP ingress: RELAY_DIRECT_PUBLIC=1 must carry proxy credentials,
# otherwise an unauthenticated proxy is published to the internet.
RELAY_DIRECT_PUBLIC="${RELAY_DIRECT_PUBLIC:-0}"
RELAY_PROXY_AUTH="${RELAY_PROXY_AUTH:-}"
RELAY_PROXY_AUTH_BAKED=""
if [ "$RELAY_DIRECT_PUBLIC" = "1" ]; then
  RELAY_PROXY_AUTH_BAKED="$RELAY_PROXY_AUTH"
fi
if [ "$RELAY_DIRECT_PUBLIC" = "1" ] && [ -z "$RELAY_PROXY_AUTH" ]; then
  echo "error: RELAY_DIRECT_PUBLIC=1 requires RELAY_PROXY_AUTH=user:pass (Basic proxy credentials)." >&2
  echo "       A public router without credentials is an open proxy." >&2
  exit 1
fi
# Rotation mode. `sticky` holds one account on one lane; `round-robin` spreads
# every request. Anything else is a typo that would otherwise be silently
# coerced at runtime.
LANE_MODE="${RELAY_LANE_MODE:-sticky}"
case "$LANE_MODE" in
  sticky|round-robin) ;;
  *)
    echo "error: RELAY_LANE_MODE must be sticky|round-robin, got '$LANE_MODE'" >&2
    exit 1
    ;;
esac

# Inner WireGuard MTU. 1280 is Cloudflare's safe default and the IPv6 minimum
# (RFC 8200) — every lane is dual-stack by default, and an IPv6 interface
# cannot carry an MTU below 1280, so a smaller value makes wg-quick fail on
# `ip -6 route add ::/0` and the lane crash-loop. Do NOT lower this to work
# around a small host uplink; raise the uplink instead (see docs). Only an
# IPv4-only lane (ENABLE_IPV6=0) can go below 1280.
WG_MTU="${WARP_MTU:-1280}"
case "$WG_MTU" in
  ''|*[!0-9]*)
    echo "error: WARP_MTU must be an integer 1280..1500, got '$WG_MTU'" >&2
    exit 1
    ;;
esac
if [ "$WG_MTU" -lt 1280 ] || [ "$WG_MTU" -gt 1500 ]; then
  echo "error: WARP_MTU must be 1280..1500, got $WG_MTU (IPv6 requires >= 1280)" >&2
  exit 1
fi

HOST_LO="${ROUTER_PORT:-3100}"
case "$HOST_LO" in
  ''|*[!0-9]*)
    echo "error: ROUTER_PORT must be an integer 1..65535, got '$HOST_LO'" >&2
    exit 1
    ;;
esac
if [ "$HOST_LO" -lt 1 ] || [ "$HOST_LO" -gt 65535 ]; then
  echo "error: ROUTER_PORT must be <= 65535, got $HOST_LO" >&2
  exit 1
fi
# One listener serves the whole pool: host ROUTER_PORT -> container BASE_PORT.
# There is no per-relay host mapping any more (a probe names its lane in a
# header). ROUTER_PORT exists so a host that already uses 3100 (another
# service, a Docker Desktop forward) can move the one public port without
# touching the client-visible shape: it is still one URL.
CONT_LO=$((BASE_PORT))

# Host binds default to loopback: the SOCKS proxies are unauthenticated and
# the relay router trusts anyone who can reach it. The relay app dials
# warp-N:1080 over the bridge and all shipped tooling probes 127.0.0.1, so
# only deliberate exposure needs WARP_BIND / INGRESS_BIND.
SOCKS_BIND="${WARP_BIND:-127.0.0.1}"
ROUTER_BIND="${INGRESS_BIND:-0.0.0.0}"
# IPv6 binds would need Compose's bracketed port syntax ("[::]:port:port"),
# which the templates below do not emit — reject rather than write a file
# Docker cannot parse.
case "$SOCKS_BIND$ROUTER_BIND" in
  ''|*[!0-9.]*)
    echo "error: WARP_BIND/INGRESS_BIND must be an IPv4 address; got WARP_BIND='$SOCKS_BIND' INGRESS_BIND='$ROUTER_BIND'" >&2
    exit 1
    ;;
esac
if [ "$WITH_TUNNELS" = true ]; then
ROUTER_LINE="relay router not host-published; tunnels reach it in-network at http://10.244.0.100:300N"
else
ROUTER_LINE="relay router (mesh-VPN ingress) on ${ROUTER_BIND}:${HOST_LO} (override bind: INGRESS_BIND, port: ROUTER_PORT)"
fi

{
cat <<HEADER
# Generated by ./generate-compose.sh — do not hand-edit.
# Regenerate: ./generate-compose.sh ${COUNT}${TUNNEL_FLAG}   (COUNT 1..64, default 10)
# Topology: bridge 10.244.0.0/16, relay app fixed at 10.244.0.100.
# warp-N raw SOCKS5 proxy on ${SOCKS_BIND}:<10800+N> (override bind: WARP_BIND),
# ${ROUTER_LINE}.
# Relay app env (RELAY_COUNT, BASE_PORT, RELAY_ALLOWED_TARGETS,
# RELAY_AUTH_TOKEN, RELAY_LANE_MODE, LIVE_SIGNALS, LIVE_SIGNAL_FILE) is baked
# from the generation-time environment / .env —
# regenerate after editing either.
# The relay app bind-mounts ./.keep-cold at /signals so its 429 signal file
# lands in the same directory the host keep-cold reads (LIVE_SIGNALS=1).
networks:
  default:
    ipam:
      config:
        - subnet: 10.244.0.0/16

services:
HEADER

get_endpoint_for_index() {
  case "$(( ($1 - 1) % 10 ))" in
    0) echo "162.159.192.1:2408" ;;
    1) echo "162.159.193.1:2408" ;;
    2) echo "162.159.195.1:2408" ;;
    3) echo "188.114.96.1:2408" ;;
    4) echo "188.114.97.1:2408" ;;
    5) echo "188.114.98.1:2408" ;;
    6) echo "188.114.99.1:2408" ;;
    7) echo "162.159.192.2:2408" ;;
    8) echo "162.159.193.2:2408" ;;
    9) echo "162.159.195.2:2408" ;;
  esac
}

if [ "$USE_LITE" = true ]; then
i=1
while [ "$i" -le "$COUNT" ]; do
WARP_PORT=$((10800 + i))
ENDPOINT_FOR_RELAY="$(get_endpoint_for_index "$i")"
cat <<ENTRY
  warp-${i}:
    image: warp-lite:latest
    build: ./warp-lite
    container_name: warp-relay-warp-${i}
    restart: always
    cap_add:
      - NET_ADMIN
    sysctls:
      - net.ipv4.conf.all.src_valid_mark=1
      - net.ipv6.conf.all.disable_ipv6=0
      - net.ipv6.conf.all.forwarding=1
    environment:
      - ALLOW_NO_AUTH=1
      - ENDPOINT_IP=${ENDPOINT_FOR_RELAY}
      - MTU=${WG_MTU}
    volumes:
      - warp-data-${i}:/etc/wireguard
    mem_limit: 32M
    mem_reservation: 8M
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    ports:
      - "${SOCKS_BIND}:${WARP_PORT}:1080"

ENTRY
i=$((i + 1))
done
else
i=1
while [ "$i" -le "$COUNT" ]; do
WARP_PORT=$((10800 + i))
ENDPOINT_FOR_RELAY="$(get_endpoint_for_index "$i")"
cat <<ENTRY
  warp-${i}:
    image: caomingjun/warp:latest
    container_name: warp-relay-warp-${i}
    restart: always
    cap_add:
      - NET_ADMIN
    sysctls:
      - net.ipv6.conf.all.disable_ipv6=0
    environment:
      - ENDPOINT_IP=${ENDPOINT_FOR_RELAY}
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    ports:
      - "${SOCKS_BIND}:${WARP_PORT}:1080"

ENTRY
i=$((i + 1))
done
fi

if [ "$WITH_TUNNELS" = true ]; then
cat <<ENTRY
  relay:
    build: .
    container_name: warp-relay-app
    restart: always
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    environment:
      - RELAY_COUNT=${COUNT}
      - BASE_PORT=${BASE_PORT:-3000}
      - RELAY_LANE_PROBE_S=${RELAY_LANE_PROBE_S:-30}
      - RELAY_LANE_COOLDOWN_S=${RELAY_LANE_COOLDOWN_S:-240}
      - RELAY_LANE_429_S=${RELAY_LANE_429_S:-120}
      - RELAY_LANE_STICKY_MAX=${RELAY_LANE_STICKY_MAX:-10000}
      - RELAY_LANE_MODE=${LANE_MODE}
      - RELAY_LANE_RETRY_MAX=${RELAY_LANE_RETRY_MAX:-3}
      - RELAY_LANE_RETRY_MAX_BODY=${RELAY_LANE_RETRY_MAX_BODY:-1048576}
      - RELAY_DIRECT_PUBLIC=${RELAY_DIRECT_PUBLIC:-0}
      - RELAY_PROXY_AUTH=${RELAY_PROXY_AUTH_BAKED:-}
      - RELAY_ALLOWED_TARGETS=${RELAY_ALLOWED_TARGETS:-https://opencode.ai,https://api.kilo.ai,https://oai.endpoints.kepler.ai.cloud.ovh.net,https://cloudflare.com}
      - RELAY_AUTH_TOKEN=${RELAY_AUTH_TOKEN:-}
      - LIVE_SIGNALS=${LIVE_SIGNALS:-0}
      - LIVE_SIGNAL_FILE=${LIVE_SIGNAL_FILE:-/signals/signals.json}
      - RELAY_SIGNAL_TOKEN=${RELAY_SIGNAL_TOKEN:-}
    volumes:
      - ./.keep-cold:/signals
    networks:
      default:
        ipv4_address: 10.244.0.100
    depends_on:
ENTRY
else
cat <<ENTRY
  relay:
    build: .
    container_name: warp-relay-app
    restart: always
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    environment:
      - RELAY_COUNT=${COUNT}
      - BASE_PORT=${BASE_PORT:-3000}
      - RELAY_LANE_PROBE_S=${RELAY_LANE_PROBE_S:-30}
      - RELAY_LANE_COOLDOWN_S=${RELAY_LANE_COOLDOWN_S:-240}
      - RELAY_LANE_429_S=${RELAY_LANE_429_S:-120}
      - RELAY_LANE_STICKY_MAX=${RELAY_LANE_STICKY_MAX:-10000}
      - RELAY_LANE_MODE=${LANE_MODE}
      - RELAY_LANE_RETRY_MAX=${RELAY_LANE_RETRY_MAX:-3}
      - RELAY_LANE_RETRY_MAX_BODY=${RELAY_LANE_RETRY_MAX_BODY:-1048576}
      - RELAY_DIRECT_PUBLIC=${RELAY_DIRECT_PUBLIC:-0}
      - RELAY_PROXY_AUTH=${RELAY_PROXY_AUTH_BAKED:-}
      - RELAY_ALLOWED_TARGETS=${RELAY_ALLOWED_TARGETS:-https://opencode.ai,https://api.kilo.ai,https://oai.endpoints.kepler.ai.cloud.ovh.net,https://cloudflare.com}
      - RELAY_AUTH_TOKEN=${RELAY_AUTH_TOKEN:-}
      - LIVE_SIGNALS=${LIVE_SIGNALS:-0}
      - LIVE_SIGNAL_FILE=${LIVE_SIGNAL_FILE:-/signals/signals.json}
      - RELAY_SIGNAL_TOKEN=${RELAY_SIGNAL_TOKEN:-}
    volumes:
      - ./.keep-cold:/signals
    networks:
      default:
        ipv4_address: 10.244.0.100
    ports:
      - "${ROUTER_BIND}:${HOST_LO}:${CONT_LO}"
    depends_on:
ENTRY
fi

i=1
while [ "$i" -le "$COUNT" ]; do
echo "      - warp-${i}"
i=$((i + 1))
done

printf '\n'

if [ "$WITH_TUNNELS" = true ]; then
# No `depends_on: - relay` on tunnels by design: relay rebuilds must never
# cascade-restart these ephemeral tunnels (each restart mints new edge URLs
# and breaks every configured client line).
# tunnel-0 -> the single router port: one public URL for the whole pool.
PORT=$((BASE_PORT))
cat <<ENTRY
  tunnel-0:
    image: cloudflare/cloudflared:latest
    container_name: warp-relay-tunnel-0
    restart: always
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    command: tunnel --no-autoupdate --url http://10.244.0.100:${PORT}

ENTRY
fi

if [ "$USE_LITE" = true ]; then
echo "volumes:"
i=1
while [ "$i" -le "$COUNT" ]; do
echo "  warp-data-${i}:"
i=$((i + 1))
done
fi
} > docker-compose.yml

echo "Generated docker-compose.yml with static IP 10.244.0.100 and ${COUNT} instances."
