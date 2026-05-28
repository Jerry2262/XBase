#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${SCRIPT_DIR}/build"
KVROCKS_PROXY="${BUILD_DIR}/kvrocks-proxy"
KVROCKS_DATANODE="${BUILD_DIR}/kvrocks-datanode"
KVROCKS_ALL="${BUILD_DIR}/kvrocks-all"
TEST_DIR="${SCRIPT_DIR}/test_run"

DATANODE_PORT=16666
PROXY_PORT=17777
DATANODE_DIR="${TEST_DIR}/datanode"
PROXY_DIR="${TEST_DIR}/proxy"
ALLINONE_DIR="${TEST_DIR}/all"
DATANODE_PID=""
PROXY_PID=""
APP_PID=""

stop_processes() {
  if [[ -n "${APP_PID}" ]] && kill -0 "${APP_PID}" 2>/dev/null; then
    kill "${APP_PID}" || true
    wait "${APP_PID}" || true
  fi
  APP_PID=""
  if [[ -n "${PROXY_PID}" ]] && kill -0 "${PROXY_PID}" 2>/dev/null; then
    kill "${PROXY_PID}" || true
    wait "${PROXY_PID}" || true
  fi
  PROXY_PID=""
  if [[ -n "${DATANODE_PID}" ]] && kill -0 "${DATANODE_PID}" 2>/dev/null; then
    kill "${DATANODE_PID}" || true
    wait "${DATANODE_PID}" || true
  fi
  DATANODE_PID=""
}

cleanup() {
  stop_processes
  rm -rf "${TEST_DIR}"
}

trap cleanup EXIT

assert_eq() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "ASSERT FAILED: ${label}"
    echo "  expected: ${expected}"
    echo "  actual:   ${actual}"
    exit 1
  fi
}

echo "=== Kvrocks BRPC Integration Test ==="

if [[ ! -x "${KVROCKS_PROXY}" ]] || [[ ! -x "${KVROCKS_DATANODE}" ]] || [[ ! -x "${KVROCKS_ALL}" ]]; then
  echo "ERROR: expected kvrocks-proxy, kvrocks-datanode, and kvrocks-all binaries in ${BUILD_DIR}"
  exit 1
fi

run_redis_cmd() {
  local port="$1"
  shift

  if command -v redis-cli >/dev/null 2>&1; then
    redis-cli --raw -p "${port}" "$@"
    return
  fi

  python3 - "${port}" "$@" <<'PY'
import socket
import sys

port = int(sys.argv[1])
args = sys.argv[2:]

payload = f"*{len(args)}\r\n".encode()
for arg in args:
    data = arg.encode()
    payload += f"${len(data)}\r\n".encode() + data + b"\r\n"

sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(payload)
reader = sock.makefile("rb")

def read_line():
    line = reader.readline()
    if not line:
        raise RuntimeError("unexpected EOF")
    return line[:-2]

def read_resp():
    prefix = reader.read(1)
    if not prefix:
        raise RuntimeError("empty response")
    if prefix in (b"+", b"-", b":"):
        return read_line().decode()
    if prefix == b"$":
        size = int(read_line())
        if size == -1:
            return None
        data = reader.read(size)
        reader.read(2)
        return data.decode()
    if prefix == b"*":
        size = int(read_line())
        if size == -1:
            return []
        return [read_resp() for _ in range(size)]
    raise RuntimeError(f"unsupported RESP prefix: {prefix!r}")

result = read_resp()
if isinstance(result, list):
    for item in result:
        print("" if item is None else item)
elif result is None:
    print("")
else:
    print(result)
PY
}

run_binary_set_cmd() {
  local port="$1"
  local key="$2"
  local input_file="$3"

  python3 - "${port}" "${key}" "${input_file}" <<'PY'
import socket
import sys

port = int(sys.argv[1])
key = sys.argv[2].encode()
input_file = sys.argv[3]
with open(input_file, "rb") as f:
    value = f.read()

payload = (
    b"*3\r\n"
    + b"$3\r\nSET\r\n"
    + f"${len(key)}\r\n".encode() + key + b"\r\n"
    + f"${len(value)}\r\n".encode() + value + b"\r\n"
)

sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(payload)
reader = sock.makefile("rb")
line = reader.readline()
if not line or line[:1] != b"+":
    raise RuntimeError(f"unexpected response: {line!r}")
sys.stdout.write(line[1:-2].decode())
PY
}

run_binary_get_hex() {
  local port="$1"
  local key="$2"

  python3 - "${port}" "${key}" <<'PY'
import binascii
import socket
import sys

port = int(sys.argv[1])
key = sys.argv[2].encode()
payload = (
    b"*2\r\n"
    + b"$3\r\nGET\r\n"
    + f"${len(key)}\r\n".encode() + key + b"\r\n"
)

sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(payload)
reader = sock.makefile("rb")
prefix = reader.read(1)
if prefix != b"$":
    raise RuntimeError(f"unexpected response prefix: {prefix!r}")
size = int(reader.readline()[:-2])
if size < 0:
    sys.stdout.write("")
    sys.exit(0)
data = reader.read(size)
reader.read(2)
sys.stdout.write(binascii.hexlify(data).decode())
PY
}

wait_ready() {
  local pid="$1"
  local log_file="$2"
  local name="$3"
  sleep 3
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "ERROR: ${name} failed to start"
    cat "${log_file}" || true
    exit 1
  fi
}

run_proxy_side_checks() {
  echo "Running proxy-side data path checks"
  ping_reply="$(run_redis_cmd "${PROXY_PORT}" ping)"
  echo "PING -> ${ping_reply}"
  assert_eq "${ping_reply}" "PONG" "PING"

  set_reply="$(run_redis_cmd "${PROXY_PORT}" set proxy:key value-1)"
  echo "SET -> ${set_reply}"
  assert_eq "${set_reply}" "OK" "SET"

  get_reply="$(run_redis_cmd "${PROXY_PORT}" get proxy:key)"
  echo "GET -> ${get_reply}"
  assert_eq "${get_reply}" "value-1" "GET"

  mset_reply="$(run_redis_cmd "${PROXY_PORT}" mset proxy:k1 v1 proxy:k2 v2)"
  echo "MSET -> ${mset_reply}"
  assert_eq "${mset_reply}" "OK" "MSET"

  readarray -t mget_values < <(run_redis_cmd "${PROXY_PORT}" mget proxy:k1 proxy:k2 proxy:missing)
  echo "MGET -> ${mget_values[*]}"
  assert_eq "${mget_values[0]:-}" "v1" "MGET[0]"
  assert_eq "${mget_values[1]:-}" "v2" "MGET[1]"
  assert_eq "${mget_values[2]:-}" "" "MGET[2]"

  hset_reply="$(run_redis_cmd "${PROXY_PORT}" hset proxy:hash field-1 hash-value)"
  echo "HSET -> ${hset_reply}"
  assert_eq "${hset_reply}" "1" "HSET"

  hget_reply="$(run_redis_cmd "${PROXY_PORT}" hget proxy:hash field-1)"
  echo "HGET -> ${hget_reply}"
  assert_eq "${hget_reply}" "hash-value" "HGET"

  local payload_file="${TEST_DIR}/payload.bin"
  local expected_hex="616c70686100626574610d0a67616d6d612064656c7461"
  printf 'alpha\000beta\r\ngamma delta' > "${payload_file}"

  binary_set_reply="$(run_binary_set_cmd "${PROXY_PORT}" proxy:binary "${payload_file}")"
  echo "SET(binary) -> ${binary_set_reply}"
  assert_eq "${binary_set_reply}" "OK" "SET(binary)"

  binary_hex="$(run_binary_get_hex "${PROXY_PORT}" proxy:binary)"
  echo "GET(binary hex) -> ${binary_hex}"
  assert_eq "${binary_hex}" "${expected_hex}" "GET(binary)"
}

mkdir -p "${DATANODE_DIR}" "${PROXY_DIR}" "${ALLINONE_DIR}"

cat > "${DATANODE_DIR}/kvrocks.conf" <<EOF
bind 127.0.0.1
port ${DATANODE_PORT}
dir ${DATANODE_DIR}
daemonize no
pidfile ""
log-dir stdout
EOF

cat > "${PROXY_DIR}/kvrocks.conf" <<EOF
bind 127.0.0.1
port ${PROXY_PORT}
dir ${PROXY_DIR}
daemonize no
pidfile ""
log-dir stdout
storage-backend-addrs 127.0.0.1:${DATANODE_PORT}
EOF

cat > "${ALLINONE_DIR}/kvrocks.conf" <<EOF
bind 127.0.0.1
port ${PROXY_PORT}
datanode-port ${DATANODE_PORT}
dir ${ALLINONE_DIR}
daemonize no
pidfile ""
log-dir stdout
EOF

echo "Scenario 1: split proxy + datanode"
echo "Starting datanode on ${DATANODE_PORT}"
"${KVROCKS_DATANODE}" -c "${DATANODE_DIR}/kvrocks.conf" >"${DATANODE_DIR}/stdout.log" 2>&1 &
DATANODE_PID=$!
wait_ready "${DATANODE_PID}" "${DATANODE_DIR}/stdout.log" "datanode"

echo "Starting proxy on ${PROXY_PORT}"
"${KVROCKS_PROXY}" -c "${PROXY_DIR}/kvrocks.conf" >"${PROXY_DIR}/stdout.log" 2>&1 &
PROXY_PID=$!
wait_ready "${PROXY_PID}" "${PROXY_DIR}/stdout.log" "proxy"
run_proxy_side_checks
stop_processes

echo "Scenario 2: single process all-in-one"
"${KVROCKS_ALL}" -c "${ALLINONE_DIR}/kvrocks.conf" >"${ALLINONE_DIR}/stdout.log" 2>&1 &
APP_PID=$!
wait_ready "${APP_PID}" "${ALLINONE_DIR}/stdout.log" "all-in-one server"
run_proxy_side_checks

echo "Integration test passed"
