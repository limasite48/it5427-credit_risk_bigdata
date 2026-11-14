#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

load_env

ML_PORT="${ML_SERVICE_PORT:-8000}"
API_PORT="${BACKEND_PORT:-8080}"
UI_PORT="${VITE_DEV_PORT:-5173}"
FORCE=0
if [[ "${1:-}" == "--force" ]]; then
  FORCE=1
fi

echo "==> Stopping ML service, backend, and frontend"
stop_service frontend "${UI_PORT}" "${FORCE}"
stop_service backend "${API_PORT}" "${FORCE}"
stop_service ml "${ML_PORT}" "${FORCE}"
echo "Stopped."
