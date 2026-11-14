#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

JAVA_HOME_RESOLVED="$(resolve_java)" || { echo "Java 25 required" >&2; exit 1; }
export JAVA_HOME="${JAVA_HOME_RESOLVED}"
export PATH="${JAVA_HOME}/bin:${PATH}"
PYTHON_BIN="$(resolve_python)" || { echo "Python >= 3.10 required" >&2; exit 1; }

load_env

echo "==> Python tests"
if [[ -d "${ROOT_DIR}/.venv" ]]; then
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.venv/bin/activate"
fi
(
  cd "${ROOT_DIR}"
  export PYTHONPATH="${ROOT_DIR}"
  export ML_SERVICE_TOKEN="${ML_SERVICE_TOKEN:-test-token}"
  # LightGBM on macOS needs Homebrew libomp (keg-only).
  if [[ -d /opt/homebrew/opt/libomp/lib ]]; then
    export DYLD_LIBRARY_PATH="/opt/homebrew/opt/libomp/lib:${DYLD_LIBRARY_PATH:-}"
  fi
  python -m pytest tests/ -v
)

echo "==> Backend tests"
(
  cd "${ROOT_DIR}/backend"
  ./mvnw test
)

echo "==> Frontend tests"
(
  cd "${ROOT_DIR}/frontend"
  npm test
)

echo
echo "All automated test suites finished."
