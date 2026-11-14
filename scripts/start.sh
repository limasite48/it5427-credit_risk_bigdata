#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

load_env
require_env JWT_SECRET ML_SERVICE_TOKEN \
  DEMO_ADMIN_USERNAME DEMO_ADMIN_PASSWORD \
  DEMO_ANALYST_USERNAME DEMO_ANALYST_PASSWORD \
  DEMO_UNDERWRITER_USERNAME DEMO_UNDERWRITER_PASSWORD

JAVA_HOME_RESOLVED="$(resolve_java)" || { echo "Java 25 required" >&2; exit 1; }
export JAVA_HOME="${JAVA_HOME_RESOLVED}"
export PATH="${JAVA_HOME}/bin:${PATH}"

PYTHON_BIN="$(resolve_python)" || { echo "Python >= 3.10 required" >&2; exit 1; }

for svc in ml backend frontend; do
  if is_running "${svc}"; then
    echo "${svc} already running (pid $(read_pid "${svc}")). Use ./scripts/stop.sh first." >&2
    exit 1
  fi
done

if [[ ! -d "${ROOT_DIR}/.venv" ]]; then
  echo "Missing .venv — run ./scripts/setup.sh first" >&2
  exit 1
fi

mkdir -p "${ROOT_DIR}/.run/logs"

echo "==> Starting ML service"
(
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.venv/bin/activate"
  cd "${ROOT_DIR}"
  # Always use an absolute model path for local process startup.
  export MODEL_PATH="${ROOT_DIR}/outputs/models/credit_risk_model.joblib"
  export ML_SERVICE_TOKEN
  export PYTHONPATH="${ROOT_DIR}"
  if [[ -d /opt/homebrew/opt/libomp/lib ]]; then
    export DYLD_LIBRARY_PATH="/opt/homebrew/opt/libomp/lib:${DYLD_LIBRARY_PATH:-}"
  fi
  nohup uvicorn ml-service.app:app \
    --host 127.0.0.1 \
    --port "${ML_SERVICE_PORT:-8000}" \
    > "${ROOT_DIR}/.run/logs/ml-service.log" 2>&1 &
  write_pid ml $!
  disown || true
)
wait_http "http://127.0.0.1:${ML_SERVICE_PORT:-8000}/health" "ML service"

echo "==> Starting Spring Boot backend"
(
  cd "${ROOT_DIR}/backend"
  export BACKEND_PORT="${BACKEND_PORT:-8080}"
  export ML_SERVICE_URL="${ML_SERVICE_URL:-http://127.0.0.1:${ML_SERVICE_PORT:-8000}}"
  export ML_SERVICE_TOKEN JWT_SECRET JWT_ACCESS_MINUTES JWT_REFRESH_DAYS
  export CORS_ALLOWED_ORIGINS
  export DEMO_ADMIN_USERNAME DEMO_ADMIN_PASSWORD
  export DEMO_ANALYST_USERNAME DEMO_ANALYST_PASSWORD
  export DEMO_UNDERWRITER_USERNAME DEMO_UNDERWRITER_PASSWORD
  export DATA_DIR="${ROOT_DIR}/outputs"
  export MODEL_PATH="${ROOT_DIR}/outputs/models/credit_risk_model.joblib"
  export SCORED_CSV="${ROOT_DIR}/outputs/scored_applications_sample.csv"
  export RISK_SUMMARY="${ROOT_DIR}/outputs/metrics/risk_summary.json"
  export DEMOGRAPHICS="${ROOT_DIR}/outputs/metrics/demographic_profiles.json"
  export ROC_CURVE="${ROOT_DIR}/outputs/figures/roc_curve.png"
  export MODEL_EVALUATION="${ROOT_DIR}/outputs/metrics/model_evaluation.json"
  export SUPPLEMENTARY_DIR="${ROOT_DIR}/ingestion-minio-kafka/data/supplementary"
  export SPARK_REPORTS_DIR="${ROOT_DIR}/apache-spark/reports"
  export SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:-local}"
  nohup ./mvnw -q spring-boot:run \
    > "${ROOT_DIR}/.run/logs/backend.log" 2>&1 &
  write_pid backend $!
  disown || true
)
wait_http "http://127.0.0.1:${BACKEND_PORT:-8080}/actuator/health" "Backend" 120

echo "==> Starting React frontend"
(
  cd "${ROOT_DIR}/frontend"
  export PATH
  nohup npm run dev -- --host 127.0.0.1 --port "${VITE_DEV_PORT:-5173}" \
    > "${ROOT_DIR}/.run/logs/frontend.log" 2>&1 &
  write_pid frontend $!
  disown || true
)
wait_http "http://127.0.0.1:${VITE_DEV_PORT:-5173}/" "Frontend" 60

echo
echo "Platform started."
echo "  Frontend : http://127.0.0.1:${VITE_DEV_PORT:-5173}"
echo "  Backend  : http://127.0.0.1:${BACKEND_PORT:-8080}"
echo "  ML       : http://127.0.0.1:${ML_SERVICE_PORT:-8000} (local only)"
echo "  Logs     : ${ROOT_DIR}/.run/logs/"
echo "Demo users : ${DEMO_ADMIN_USERNAME} / ${DEMO_ANALYST_USERNAME} / ${DEMO_UNDERWRITER_USERNAME}"
