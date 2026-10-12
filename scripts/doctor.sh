#!/usr/bin/env bash
# Preflight check before running PureFlow. Exits non-zero if anything is MISSING.
# Kept compatible with bash 3.2 (macOS).

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
    ok "arm64: every image this project uses (python, seaweedfs, aws-cli, uv) is built natively for it"
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
for entry in "3000 Dagster UI" "8501 Streamlit" "8081 dbt Docs" "8082 GX reports" "18900 S3 storage API" "18901 Storage console"; do
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
# No awk: it is missing on some minimal images.
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
