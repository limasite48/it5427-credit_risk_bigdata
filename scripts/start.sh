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
export PYTHON_BIN

ML_PORT="${ML_SERVICE_PORT:-8000}"
API_PORT="${BACKEND_PORT:-8080}"
UI_PORT="${VITE_DEV_PORT:-5173}"

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
if [[ ! -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  echo "Missing ${ROOT_DIR}/.venv/bin/python — run ./scripts/setup.sh first" >&2
  exit 1
fi
if ! command -v npm >/dev/null 2>&1; then
  echo "npm is required to start the frontend" >&2
  exit 1
fi

assert_port_free "${ML_PORT}" "ML service"
assert_port_free "${API_PORT}" "Backend"
assert_port_free "${UI_PORT}" "Frontend"

mkdir -p "${ROOT_DIR}/.run/logs"

cleanup_on_fail() {
  local status=$?
  trap - ERR
  echo "Startup failed. Stopping anything this script launched." >&2
  stop_service frontend "${UI_PORT}" 1 || true
  stop_service backend "${API_PORT}" 1 || true
  stop_service ml "${ML_PORT}" 1 || true
  exit "${status}"
}
trap cleanup_on_fail ERR

LIBOMP_DIR="/opt/homebrew/opt/libomp/lib"
if [[ -d "${LIBOMP_DIR}" ]]; then
  export DYLD_LIBRARY_PATH="${LIBOMP_DIR}${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
fi

echo "==> Starting ML service"
spawn_detached ml "${ROOT_DIR}/.run/logs/ml-service.log" \
  env \
  MODEL_PATH="${ROOT_DIR}/outputs/models/credit_risk_model.joblib" \
  ML_SERVICE_TOKEN="${ML_SERVICE_TOKEN}" \
  PYTHONPATH="${ROOT_DIR}" \
  DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH:-}" \
  "${ROOT_DIR}/.venv/bin/python" -m uvicorn ml-service.app:app \
  --host 127.0.0.1 \
  --port "${ML_PORT}" \
  --app-dir "${ROOT_DIR}"
wait_http "http://127.0.0.1:${ML_PORT}/health" "ML service"

echo "==> Starting Spring Boot backend"
spawn_detached backend "${ROOT_DIR}/.run/logs/backend.log" \
  env \
  JAVA_HOME="${JAVA_HOME}" \
  PATH="${JAVA_HOME}/bin:${PATH}" \
  BACKEND_PORT="${API_PORT}" \
  ML_SERVICE_URL="${ML_SERVICE_URL:-http://127.0.0.1:${ML_PORT}}" \
  ML_SERVICE_TOKEN="${ML_SERVICE_TOKEN}" \
  JWT_SECRET="${JWT_SECRET}" \
  JWT_ACCESS_MINUTES="${JWT_ACCESS_MINUTES:-15}" \
  JWT_REFRESH_DAYS="${JWT_REFRESH_DAYS:-7}" \
  CORS_ALLOWED_ORIGINS="${CORS_ALLOWED_ORIGINS}" \
  DEMO_ADMIN_USERNAME="${DEMO_ADMIN_USERNAME}" \
  DEMO_ADMIN_PASSWORD="${DEMO_ADMIN_PASSWORD}" \
  DEMO_ANALYST_USERNAME="${DEMO_ANALYST_USERNAME}" \
  DEMO_ANALYST_PASSWORD="${DEMO_ANALYST_PASSWORD}" \
  DEMO_UNDERWRITER_USERNAME="${DEMO_UNDERWRITER_USERNAME}" \
  DEMO_UNDERWRITER_PASSWORD="${DEMO_UNDERWRITER_PASSWORD}" \
  DATA_DIR="${ROOT_DIR}/outputs" \
  MODEL_PATH="${ROOT_DIR}/outputs/models/credit_risk_model.joblib" \
  SCORED_CSV="${ROOT_DIR}/outputs/scored_applications_sample.csv" \
  GOLD_SCORED_CSV="${ROOT_DIR}/outputs/gold_scored_applications.csv" \
  RISK_SUMMARY="${ROOT_DIR}/outputs/metrics/risk_summary.json" \
  DEMOGRAPHICS="${ROOT_DIR}/outputs/metrics/demographic_profiles.json" \
  ROC_CURVE="${ROOT_DIR}/outputs/figures/roc_curve.png" \
  SHAP_SUMMARY="${ROOT_DIR}/outputs/figures/shap_summary.png" \
  MODEL_EVALUATION="${ROOT_DIR}/outputs/metrics/model_evaluation.json" \
  SUPPLEMENTARY_DIR="${ROOT_DIR}/ingestion-minio-kafka/data/supplementary" \
  SPARK_REPORTS_DIR="${ROOT_DIR}/apache-spark/reports" \
  SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:-local}" \
  LOGIN_MAX_ATTEMPTS="${LOGIN_MAX_ATTEMPTS:-5}" \
  LOGIN_LOCKOUT_SECONDS="${LOGIN_LOCKOUT_SECONDS:-60}" \
  SCORE_RATE_LIMIT="${SCORE_RATE_LIMIT:-30}" \
  bash -c 'cd "$1" && exec ./mvnw -q spring-boot:run' _ "${ROOT_DIR}/backend"
wait_http "http://127.0.0.1:${API_PORT}/actuator/health" "Backend" 120

echo "==> Starting React frontend"
spawn_detached frontend "${ROOT_DIR}/.run/logs/frontend.log" \
  env \
  PATH="${PATH}" \
  bash -c 'cd "$1" && exec npm run dev -- --host 127.0.0.1 --port "$2"' _ \
  "${ROOT_DIR}/frontend" "${UI_PORT}"
wait_http "http://127.0.0.1:${UI_PORT}/" "Frontend" 60

trap - ERR

echo
echo "Platform started."
echo "  Frontend : http://127.0.0.1:${UI_PORT}"
echo "  Backend  : http://127.0.0.1:${API_PORT}"
echo "  ML       : http://127.0.0.1:${ML_PORT} (local only)"
echo "  Logs     : ${ROOT_DIR}/.run/logs/"
echo "Demo users : ${DEMO_ADMIN_USERNAME} / ${DEMO_ANALYST_USERNAME} / ${DEMO_UNDERWRITER_USERNAME}"
echo "Stop       : ./scripts/stop.sh"
echo "Terminate  : ./scripts/terminate.sh"
