#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_DIR="${ROOT_DIR}/.run"
ENV_FILE="${ROOT_DIR}/.env"

mkdir -p "${PID_DIR}"

resolve_java() {
  if [[ -n "${JAVA_HOME:-}" && -x "${JAVA_HOME}/bin/java" ]]; then
    echo "${JAVA_HOME}"
    return
  fi
  if [[ -x "/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home/bin/java" ]]; then
    echo "/Library/Java/JavaVirtualMachines/jdk-25.jdk/Contents/Home"
    return
  fi
  if command -v /usr/libexec/java_home >/dev/null 2>&1; then
    /usr/libexec/java_home -v 25 2>/dev/null && return
  fi
  return 1
}

resolve_python() {
  for candidate in python3.12 python3.11 python3.10 python3; do
    if command -v "${candidate}" >/dev/null 2>&1; then
      local ver
      ver="$("${candidate}" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')"
      local major minor
      IFS=. read -r major minor <<<"${ver}"
      if (( major > 3 || (major == 3 && minor >= 10) )); then
        echo "${candidate}"
        return
      fi
    fi
  done
  if [[ -x /opt/homebrew/opt/python@3.12/bin/python3.12 ]]; then
    echo /opt/homebrew/opt/python@3.12/bin/python3.12
    return
  fi
  return 1
}

load_env() {
  if [[ -f "${ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
  fi
}

require_env() {
  local missing=0
  for key in "$@"; do
    if [[ -z "${!key:-}" ]]; then
      echo "Missing required environment variable: ${key}" >&2
      missing=1
    fi
  done
  if [[ "${missing}" -ne 0 ]]; then
    exit 1
  fi
}

wait_http() {
  local url="$1"
  local name="$2"
  local attempts="${3:-60}"
  local i
  for ((i = 1; i <= attempts; i++)); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      echo "${name} is ready: ${url}"
      return 0
    fi
    sleep 1
  done
  echo "${name} did not become ready: ${url}" >&2
  return 1
}

write_pid() {
  local name="$1"
  local pid="$2"
  echo "${pid}" > "${PID_DIR}/${name}.pid"
}

read_pid() {
  local name="$1"
  local file="${PID_DIR}/${name}.pid"
  if [[ -f "${file}" ]]; then
    cat "${file}"
  fi
}

is_running() {
  local name="$1"
  local pid
  pid="$(read_pid "${name}" || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  return 1
}

stop_pid() {
  local name="$1"
  local pid
  pid="$(read_pid "${name}" || true)"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    sleep 1
    if kill -0 "${pid}" 2>/dev/null; then
      kill -9 "${pid}" 2>/dev/null || true
    fi
  fi
  rm -f "${PID_DIR}/${name}.pid"
}
