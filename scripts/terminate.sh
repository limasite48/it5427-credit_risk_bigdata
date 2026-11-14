#!/usr/bin/env bash
set -euo pipefail
# Immediate SIGKILL of the platform process trees and of whatever is still
# listening on the ML, API, and UI ports.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${ROOT_DIR}/stop.sh" --force
