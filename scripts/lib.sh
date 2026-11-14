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

# Kill a process and every child. Leaves are signaled first so a parent
# such as Maven or npm cannot leave the real server running.
kill_tree() {
  local pid="$1"
  local sig="${2:-TERM}"
  local child
  if [[ -z "${pid}" ]] || ! [[ "${pid}" =~ ^[0-9]+$ ]]; then
    return 0
  fi
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  while IFS= read -r child; do
    [[ -n "${child}" ]] && kill_tree "${child}" "${sig}"
  done < <(pgrep -P "${pid}" 2>/dev/null || true)
  kill "-${sig}" "${pid}" 2>/dev/null || true
}

port_listen_pids() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"${port}" -sTCP:LISTEN -t 2>/dev/null || true
  fi
}

assert_port_free() {
  local port="$1"
  local name="$2"
  local pids
  pids="$(port_listen_pids "${port}" | tr '\n' ' ' | sed 's/[[:space:]]*$//')"
  if [[ -n "${pids}" ]]; then
    echo "${name} port ${port} is already in use (pid ${pids})." >&2
    echo "Run ./scripts/stop.sh or ./scripts/terminate.sh first." >&2
    exit 1
  fi
}

# Stop one platform service. With force=1 the tree is SIGKILL'd immediately.
# Listeners on the service port are included, even when they were not started
# by this script and therefore have no pid file.
stop_service() {
  local name="$1"
  local port="$2"
  local force="${3:-0}"
  local pid lp

  pid="$(read_pid "${name}" || true)"
  if [[ -n "${pid}" ]]; then
    echo "Stopping ${name} (pid ${pid})"
    if [[ "${force}" == "1" ]]; then
      kill_tree "${pid}" KILL
    else
      kill_tree "${pid}" TERM
      sleep 1
      if kill -0 "${pid}" 2>/dev/null; then
        kill_tree "${pid}" KILL
      fi
    fi
  fi

  while IFS= read -r lp; do
    [[ -z "${lp}" ]] && continue
    echo "Releasing ${name} port ${port} (pid ${lp})"
    if [[ "${force}" == "1" ]]; then
      kill_tree "${lp}" KILL
    else
      kill_tree "${lp}" TERM
    fi
  done < <(port_listen_pids "${port}")

  sleep 1
  while IFS= read -r lp; do
    [[ -z "${lp}" ]] && continue
    kill_tree "${lp}" KILL
  done < <(port_listen_pids "${port}")

  rm -f "${PID_DIR}/${name}.pid"
}

# Replace the helper with the service itself and put it in a new session,
# so stop/terminate can signal that session without killing this script.
spawn_detached() {
  local name="$1"
  local logfile="$2"
  shift 2
  local launcher="${PYTHON_BIN:-python3}"
  "${launcher}" -c 'import os, sys; os.setsid(); os.execvp(sys.argv[1], sys.argv[1:])' "$@" \
    >"${logfile}" 2>&1 &
  write_pid "${name}" "$!"
  disown "$!" 2>/dev/null || disown || true
}
