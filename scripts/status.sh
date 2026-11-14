#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_env

status_one() {
  local name="$1"
  local url="$2"
  if is_running "${name}"; then
    echo "${name}: running (pid $(read_pid "${name}"))"
  else
    echo "${name}: not running"
  fi
  if curl -fsS "${url}" >/dev/null 2>&1; then
    echo "  health: OK (${url})"
  else
    echo "  health: unavailable (${url})"
  fi
}

status_one ml "http://127.0.0.1:${ML_SERVICE_PORT:-8000}/health"
status_one backend "http://127.0.0.1:${BACKEND_PORT:-8080}/actuator/health"
status_one frontend "http://127.0.0.1:${VITE_DEV_PORT:-5173}/"
