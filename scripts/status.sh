#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_env

status_one() {
  local name="$1"
  local url="$2"
  local port="$3"
  local listeners
  if is_running "${name}"; then
    echo "${name}: running (pid $(read_pid "${name}"))"
  else
    echo "${name}: not running"
  fi
  listeners="$(port_listen_pids "${port}" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -n "${listeners}" ]]; then
    echo "  port ${port}: listening (pid ${listeners})"
  else
    echo "  port ${port}: free"
  fi
  if curl -fsS "${url}" >/dev/null 2>&1; then
    echo "  health: OK (${url})"
  else
    echo "  health: unavailable (${url})"
  fi
}

status_one ml "http://127.0.0.1:${ML_SERVICE_PORT:-8000}/health" "${ML_SERVICE_PORT:-8000}"
status_one backend "http://127.0.0.1:${BACKEND_PORT:-8080}/actuator/health" "${BACKEND_PORT:-8080}"
status_one frontend "http://127.0.0.1:${VITE_DEV_PORT:-5173}/" "${VITE_DEV_PORT:-5173}"
