#!/usr/bin/env bash
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
load_env

BACKEND="${SMOKE_BACKEND_URL:-http://127.0.0.1:${BACKEND_PORT:-8080}}"
USER_NAME="${DEMO_UNDERWRITER_USERNAME:-underwriter}"
PASS="${DEMO_UNDERWRITER_PASSWORD:-UnderwriterDemo!234}"
COOKIE_JAR="$(mktemp)"
trap 'rm -f "${COOKIE_JAR}"' EXIT

echo "==> Smoke test against ${BACKEND}"

curl -fsS "${BACKEND}/actuator/health" >/dev/null
echo "actuator health OK"

LOGIN="$(curl -fsS -c "${COOKIE_JAR}" -H 'Content-Type: application/json' \
  -d "{\"username\":\"${USER_NAME}\",\"password\":\"${PASS}\"}" \
  "${BACKEND}/api/auth/login")"
TOKEN="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["accessToken"])' <<<"${LOGIN}")"
AUTH=( -H "Authorization: Bearer ${TOKEN}" )
echo "login OK"

curl -fsS "${AUTH[@]}" "${BACKEND}/api/dashboard/overview" | python3 -c '
import json, sys
d = json.load(sys.stdin)
summary = json.load(open("'"${ROOT_DIR}/outputs/metrics/risk_summary.json"'"))["portfolio_summary"]
assert d["totalApplications"] == summary["total_applications"], (d["totalApplications"], summary["total_applications"])
assert abs(d["nplRate"] - summary["npl_default_ratio"]) < 1e-6, (d["nplRate"], summary["npl_default_ratio"])
print(f"overview OK (n={d[\"totalApplications\"]}, npl={d[\"nplRate\"]})")
'
curl -fsS "${AUTH[@]}" "${BACKEND}/api/dashboard/segments" | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["byOccupation"]; print("segments OK")'
curl -fsS -o /dev/null "${AUTH[@]}" "${BACKEND}/api/dashboard/figures/roc-curve" && echo "roc-curve OK"
curl -fsS -o /dev/null "${AUTH[@]}" "${BACKEND}/api/dashboard/figures/shap-summary" && echo "shap-summary OK"

DEMO_ID="$(python3 -c '
import csv, json, pathlib
root = pathlib.Path("'"${ROOT_DIR}"'")
ids_file = root / "outputs" / "gold_demo_ids.json"
if ids_file.exists():
    data = json.loads(ids_file.read_text())
    val = data.get("high_example") or data.get("low_example")
    if val:
        print(val)
        raise SystemExit(0)
with open(root / "outputs" / "scored_applications_sample.csv", newline="") as f:
    print(next(csv.DictReader(f))["SK_ID_CURR"])
')"
curl -fsS "${AUTH[@]}" "${BACKEND}/api/applications/${DEMO_ID}" | python3 -c "import json,sys; d=json.load(sys.stdin); assert str(d['applicationId'])=='${DEMO_ID}'; print('lookup OK:', d['applicationId'], d['riskTier'])"

score_one() {
  local file="$1"
  local label="$2"
  curl -fsS "${AUTH[@]}" -H 'Content-Type: application/json' \
    --data-binary @"${ROOT_DIR}/${file}" \
    "${BACKEND}/api/scoring/score" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); assert 'defaultProbability' in d; assert d['riskTier'] in ('LOW','MEDIUM','HIGH'); print('${label}', d['riskTier'], d['action'], 'PD=', d['defaultProbability']); assert d.get('topRiskDrivers')"
}

score_one sample_full_applicant.json "full"
score_one sample_subprime_applicant.json "subprime"

curl -fsS -b "${COOKIE_JAR}" -X POST "${BACKEND}/api/auth/logout" >/dev/null
echo "logout OK"
echo "Smoke test passed."
