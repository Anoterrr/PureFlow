#!/usr/bin/env bash
# scripts/doctor.sh: preflight check before running or presenting PureFlow.
#
# Written for bash 3.2, which is what macOS still ships. No associative
# arrays, no ${var,,}, no mapfile. Runs on macOS, Linux and WSL alike.
#
# Exit code is 0 only when nothing is marked MISSING, so it is usable in CI
# or as a gate in a script. PENDING means "works, but worth knowing".

set -u

fails=0
ok()      { printf 'OK       %s\n' "$1"; }
missing() { printf 'MISSING  %s\n' "$1"; fails=$((fails + 1)); }
pending() { printf 'PENDING  %s\n' "$1"; }
section() { printf '\n== %s ==\n' "$1"; }

cd "$(dirname "$0")/.." || exit 1

section "Platform"
os=$(uname -s)
arch=$(uname -m)
ok "$os $arch"
case "$arch" in
  arm64 | aarch64)
    ok "arm64: every image this project uses (python, minio, mc, uv) is built natively for it"
    ;;
esac

section "Container runtime"
if command -v docker > /dev/null 2>&1; then
  ok "docker on PATH"
  if docker info > /dev/null 2>&1; then
    ok "docker daemon reachable"
  else
    missing "docker daemon not reachable (start Docker Desktop and re-run)"
  fi
  # Compose v2 is a docker subcommand. The old hyphenated docker-compose is
  # gone from current Docker Desktop, which is why every command in the
  # README uses `docker compose`.
  if docker compose version > /dev/null 2>&1; then
    ok "docker compose v2 ($(docker compose version --short 2> /dev/null))"
  else
    missing "docker compose v2 (this project needs the v2 subcommand, not docker-compose)"
  fi
else
  missing "docker not on PATH"
fi

section "Project configuration"
if [ -f .env ]; then
  ok ".env present"
else
  missing ".env not found (run: cp .env.example .env)"
fi
[ -f docker-compose.yml ] && ok "docker-compose.yml" || missing "docker-compose.yml"
[ -f uv.lock ] && ok "uv.lock (pinned dependencies)" || missing "uv.lock"

section "Ports the stack publishes"
# Every port is bound to 127.0.0.1 by compose, so only the local machine matters.
for entry in "3000 Dagster UI" "8501 Streamlit" "8081 dbt Docs" "8082 GX reports" "9000 MinIO API" "9001 MinIO console"; do
  port=${entry%% *}
  name=${entry#* }
  busy=""
  if command -v lsof > /dev/null 2>&1; then
    busy=$(lsof -nP -iTCP:"$port" -sTCP:LISTEN -t 2> /dev/null | head -1)
  elif command -v ss > /dev/null 2>&1; then
    ss -lntH "sport = :$port" 2> /dev/null | grep -q . && busy="yes"
  fi
  if [ -n "$busy" ]; then
    pending "port $port ($name) already in use, the stack will fail to bind it"
  else
    ok "port $port free ($name)"
  fi
done

section "Disk"
# MinIO holds the Delta tables; a full clean run of the 1M-row generator
# lands in the low hundreds of MB, so this is a floor, not a target.
# Parsed with shell word splitting rather than awk: `df -P` guarantees one
# line per filesystem, and awk is not always present on a minimal image.
avail_kb=""
if command -v df > /dev/null 2>&1; then
  df_line=$(df -Pk . 2> /dev/null | tail -1)
  # shellcheck disable=SC2086
  set -- $df_line
  [ "$#" -ge 4 ] && avail_kb=$4
fi
if [ -n "$avail_kb" ] && [ "$avail_kb" -eq "$avail_kb" ] 2> /dev/null; then
  avail_gb=$((avail_kb / 1024 / 1024))
  if [ "$avail_gb" -ge 5 ]; then
    ok "${avail_gb}GB free"
  else
    pending "only ${avail_gb}GB free, the image plus a full generated dataset wants ~5GB"
  fi
fi

section "Local development (optional, not needed to run the stack)"
if command -v uv > /dev/null 2>&1; then
  ok "uv on PATH ($(uv --version 2> /dev/null))"
  if [ -d .venv ]; then
    ok ".venv present (uv run pytest tests/ will work)"
  else
    pending ".venv absent (run: uv sync)"
  fi
else
  pending "uv not installed, only needed for tests and pre-commit outside Docker"
fi

printf '\n'
if [ "$fails" -eq 0 ]; then
  printf 'Ready. Next: docker compose up -d --build\n'
else
  printf '%s blocking item(s) above. Fix those first.\n' "$fails"
fi
exit "$fails"
