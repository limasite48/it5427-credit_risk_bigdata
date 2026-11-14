#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

echo "==> Stopping local services"
stop_pid frontend
stop_pid backend
stop_pid ml
echo "Stopped."
