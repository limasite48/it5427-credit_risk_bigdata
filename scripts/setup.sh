#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

echo "==> Credit Risk platform setup"
echo "Root: ${ROOT_DIR}"

JAVA_HOME_RESOLVED="$(resolve_java)" || {
  echo "Java 25 is required. Install JDK 25 and retry." >&2
  exit 1
}
export JAVA_HOME="${JAVA_HOME_RESOLVED}"
export PATH="${JAVA_HOME}/bin:${PATH}"
echo "Java: $(java -version 2>&1 | head -1)"

PYTHON_BIN="$(resolve_python)" || {
  echo "Python >= 3.10 is required. On macOS: brew install python@3.12" >&2
  exit 1
}
echo "Python: $(${PYTHON_BIN} --version)"

if [[ "$(uname -s)" == "Darwin" && ! -d /opt/homebrew/opt/libomp/lib ]]; then
  echo "Note: LightGBM on Apple Silicon needs OpenMP. Install with: brew install libomp" >&2
fi

command -v node >/dev/null || { echo "Node.js is required" >&2; exit 1; }
echo "Node: $(node --version)"

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${ROOT_DIR}/.env.example" "${ENV_FILE}"
  echo "Created .env from .env.example"
else
  echo ".env already exists (not overwritten)"
fi

load_env

ARTIFACTS=(
  "outputs/models/credit_risk_model.joblib"
  "outputs/metrics/risk_summary.json"
  "outputs/metrics/demographic_profiles.json"
  "outputs/scored_applications_sample.csv"
  "outputs/figures/roc_curve.png"
)
for rel in "${ARTIFACTS[@]}"; do
  if [[ ! -f "${ROOT_DIR}/${rel}" ]]; then
    echo "Missing required artifact: ${rel}" >&2
    exit 1
  fi
  echo "OK ${rel}"
done

if [[ ! -d "${ROOT_DIR}/.venv" ]]; then
  "${PYTHON_BIN}" -m venv "${ROOT_DIR}/.venv"
fi
# shellcheck disable=SC1091
source "${ROOT_DIR}/.venv/bin/activate"
pip install -q -r "${ROOT_DIR}/requirements.txt"
echo "Python dependencies installed"

(
  cd "${ROOT_DIR}/frontend"
  if [[ ! -d node_modules ]]; then
    npm install
  else
    npm install --prefer-offline
  fi
)
echo "Frontend dependencies installed"

(
  cd "${ROOT_DIR}/backend"
  ./mvnw -q -DskipTests dependency:resolve
)
echo "Maven dependencies prepared"

echo
echo "Setup complete."
echo "Next: ./scripts/start.sh   or   docker compose up --build -d"
