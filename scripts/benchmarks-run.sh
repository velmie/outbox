#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RESULTS_ROOT="${RESULTS_ROOT:-$ROOT_DIR/docs/benchmarks/results}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="${RESULTS_ROOT}/${RUN_ID}"
JSONL="${OUT_DIR}/results.jsonl"
CSV="${OUT_DIR}/results.csv"
SUMMARY="${OUT_DIR}/summary.csv"

DSN="${DSN:-root:secret@tcp(127.0.0.1:3307)/outbox?parseTime=true}"
MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
MYSQL_USER="${MYSQL_USER:-root}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-secret}"
MYSQL_DB="${MYSQL_DB:-outbox}"
MYSQL_CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-outbox-mysql-bench}"
MYSQL_PROFILE="${MYSQL_PROFILE:-fast}"
MYSQL_WAIT_SECONDS="${MYSQL_WAIT_SECONDS:-}"
MYSQL_REMOVE_CONTAINER="${MYSQL_REMOVE_CONTAINER:-1}"
PURGE_BINLOG="${PURGE_BINLOG:-}"

BENCH_BIN="${BENCH_BIN:-/tmp/outbox-bench}"
BUILD_BENCH="${BUILD_BENCH:-1}"
START_MYSQL="${START_MYSQL:-1}"

PLAN="${PLAN:-full}"
REPEATS="${REPEATS:-5}"
WARMUP="${WARMUP:-1}"
WARMUP_RECORDS="${WARMUP_RECORDS:-20000}"
RESUME="${RESUME:-1}"
PAYLOAD_RANDOM="${PAYLOAD_RANDOM:-0}"
PAYLOAD_SEED="${PAYLOAD_SEED:-1}"
ENQUEUE_TIMEOUT="${ENQUEUE_TIMEOUT:-0s}"
MEASURE_LATENCY="${MEASURE_LATENCY:-1}"
AUTO_TARGET="${AUTO_TARGET:-1}"

CONSUME_RECORDS="${CONSUME_RECORDS:-200000}"
ENQUEUE_RECORDS="${ENQUEUE_RECORDS:-200000}"
MIXED_RECORDS="${MIXED_RECORDS:-${CONSUME_RECORDS}}"
MIXED_DURATION="${MIXED_DURATION:-0s}"
MIXED_PRODUCER_INTERVAL="${MIXED_PRODUCER_INTERVAL:-0s}"
MIXED_DRAIN_TIMEOUT="${MIXED_DRAIN_TIMEOUT:-2m}"
MIXED="${MIXED:-1}"

PARTITION_AHEAD="${PARTITION_AHEAD:-168h}"
PARTITION_LOOKBACK="${PARTITION_LOOKBACK:-0s}"
PARTITION_WINDOW="${PARTITION_WINDOW:-1h}"

PARTITION_EFFECT_RECORDS="${PARTITION_EFFECT_RECORDS:-2000000}"
PARTITION_EFFECT_DAYS="${PARTITION_EFFECT_DAYS:-90}"
PARTITION_EFFECT_SEED_AGE="${PARTITION_EFFECT_SEED_AGE:-2160h}"
PARTITION_EFFECT_LOOKBACK="${PARTITION_EFFECT_LOOKBACK:-${PARTITION_EFFECT_SEED_AGE}}"
LARGE_PAYLOAD_RECORDS="${LARGE_PAYLOAD_RECORDS:-20000}"

WORKERS_LIST=(1 2 4 8 16)
BATCH_LIST=(10 50 100 200)
PAYLOAD_LIST=(128 512 4096)
LARGE_PAYLOAD_LIST=(16384 131072)
PRODUCERS_LIST=(1 2 4 8)

if [[ "${PLAN}" == "mini" ]]; then
  WORKERS_LIST=(1 2)
  BATCH_LIST=(10 50)
  PAYLOAD_LIST=(128 512)
  LARGE_PAYLOAD_LIST=(16384)
  PRODUCERS_LIST=(2)
  REPEATS=2
  WARMUP_RECORDS=5000
  CONSUME_RECORDS=20000
  ENQUEUE_RECORDS=20000
  PARTITION_EFFECT_RECORDS=200000
  PARTITION_EFFECT_DAYS=7
  PARTITION_EFFECT_SEED_AGE=168h
  PARTITION_EFFECT_LOOKBACK="${PARTITION_EFFECT_SEED_AGE}"
  LARGE_PAYLOAD_RECORDS=20000
  MIXED_RECORDS=20000
  MIXED=0
fi

if [[ -z "${PURGE_BINLOG}" ]]; then
  if [[ "${START_MYSQL}" == "1" ]]; then
    PURGE_BINLOG=1
  else
    PURGE_BINLOG=0
  fi
fi

mkdir -p "${OUT_DIR}"

cleanup() {
  if [[ "${START_MYSQL}" == "1" ]]; then
    NAME="${MYSQL_CONTAINER_NAME}" "${ROOT_DIR}/scripts/mysql-bench-stop.sh" >/dev/null || true
  fi
}

purge_binlog() {
  if [[ "${PURGE_BINLOG}" != "1" ]]; then
    return
  fi
  if [[ "${START_MYSQL}" != "1" ]]; then
    return
  fi
  docker exec "${MYSQL_CONTAINER_NAME}" \
    mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -uroot -p"${MYSQL_PASSWORD}" \
    -e "RESET MASTER;" >/dev/null 2>&1 || true
}

if [[ "${START_MYSQL}" == "1" ]]; then
  NAME="${MYSQL_CONTAINER_NAME}" PORT="${MYSQL_PORT}" ROOT_PASSWORD="${MYSQL_PASSWORD}" DATABASE="${MYSQL_DB}" \
    PROFILE="${MYSQL_PROFILE}" REMOVE_CONTAINER="${MYSQL_REMOVE_CONTAINER}" \
    "${ROOT_DIR}/scripts/mysql-bench.sh"
  trap cleanup EXIT
fi

if [[ "${START_MYSQL}" == "1" ]]; then
  if [[ -z "${MYSQL_WAIT_SECONDS}" ]]; then
    if [[ "${MYSQL_PROFILE}" == "prod" ]]; then
      MYSQL_WAIT_SECONDS=180
    else
      MYSQL_WAIT_SECONDS=60
    fi
  fi
  echo "Waiting for MySQL to accept connections..."
  ready=0
  for _ in $(seq 1 "${MYSQL_WAIT_SECONDS}"); do
    if docker exec "${MYSQL_CONTAINER_NAME}" \
      mysqladmin ping -h 127.0.0.1 -P "${MYSQL_PORT}" -uroot -p"${MYSQL_PASSWORD}" --silent >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 1
  done
  if [[ "${ready}" != "1" ]]; then
    echo "MySQL did not become ready within timeout." >&2
    if docker ps -a --format '{{.Names}}' | grep -q "^${MYSQL_CONTAINER_NAME}\$"; then
      echo "Last MySQL logs:" >&2
      docker logs --tail 200 "${MYSQL_CONTAINER_NAME}" >&2 || true
    else
      echo "Container ${MYSQL_CONTAINER_NAME} not found (may have exited and been removed)." >&2
    fi
    exit 1
  fi
fi

if [[ "${BUILD_BENCH}" == "1" || ! -x "${BENCH_BIN}" ]]; then
  (cd "${ROOT_DIR}" && go build -o "${BENCH_BIN}" ./cmd/outbox-bench)
fi

touch "${JSONL}"
if [[ -f "${OUT_DIR}/metadata.txt" && "${RESUME}" == "1" ]]; then
  {
    echo
    echo "resumed_at=$(date -u +%Y%m%dT%H%M%SZ)"
  } >> "${OUT_DIR}/metadata.txt"
else
  {
    echo "run_id=${RUN_ID}"
    echo "plan=${PLAN}"
    echo "dsn=${DSN}"
    echo "records_consume=${CONSUME_RECORDS}"
    echo "records_enqueue=${ENQUEUE_RECORDS}"
    echo "records_mixed=${MIXED_RECORDS}"
    echo "partition_ahead=${PARTITION_AHEAD}"
    echo "partition_lookback=${PARTITION_LOOKBACK}"
    echo "partition_window=${PARTITION_WINDOW}"
    echo "partition_effect_records=${PARTITION_EFFECT_RECORDS}"
    echo "partition_effect_days=${PARTITION_EFFECT_DAYS}"
    echo "partition_effect_seed_age=${PARTITION_EFFECT_SEED_AGE}"
    echo "partition_effect_lookback=${PARTITION_EFFECT_LOOKBACK}"
    echo "large_payload_records=${LARGE_PAYLOAD_RECORDS}"
    echo "payload_random=${PAYLOAD_RANDOM}"
    echo "payload_seed=${PAYLOAD_SEED}"
    echo "enqueue_timeout=${ENQUEUE_TIMEOUT}"
    echo "mixed_duration=${MIXED_DURATION}"
    echo "mixed_producer_interval=${MIXED_PRODUCER_INTERVAL}"
    echo "mixed_drain_timeout=${MIXED_DRAIN_TIMEOUT}"
    echo "mixed_enabled=${MIXED}"
    echo "auto_target=${AUTO_TARGET}"
    echo "repeats=${REPEATS}"
    echo "warmup=${WARMUP}"
    echo "warmup_records=${WARMUP_RECORDS}"
    echo "resume=${RESUME}"
    echo
    lscpu || true
    echo
    free -h || true
    echo
    mysql --version || true
    echo
    MYSQL_PWD="${MYSQL_PASSWORD}" mysql -h "${MYSQL_HOST}" -P "${MYSQL_PORT}" -u "${MYSQL_USER}" "${MYSQL_DB}" -e \
      "SELECT @@version, @@innodb_flush_log_at_trx_commit, @@sync_binlog, @@innodb_buffer_pool_size, @@innodb_log_file_size, @@innodb_io_capacity;" \
      || true
  } > "${OUT_DIR}/metadata.txt"
fi

declare -A DONE=()
if [[ "${RESUME}" == "1" && -s "${JSONL}" ]]; then
  while IFS= read -r key; do
    DONE["${key}"]=1
  done < <(python3 - "${JSONL}" <<'PY'
import json
import sys

path = sys.argv[1]
with open(path) as f:
    for line in f:
        row = json.loads(line)
        key = row.get("checkpoint_key")
        if not key:
            continue
        print(key)
PY
)
fi

RUN_START_MS="$(date +%s%3N)"
TOTAL_RUNS=0
COMPLETED_RUNS=0
EXECUTED_RUNS=0
EXECUTED_MS=0
declare -A PHASE_TOTAL=()
declare -A PHASE_DONE=()

add_phase_runs() {
  local phase="$1"
  local count="$2"
  if [[ -z "${phase}" || -z "${count}" ]]; then
    return
  fi
  TOTAL_RUNS=$((TOTAL_RUNS + count))
  local prev="${PHASE_TOTAL["${phase}"]:-0}"
  PHASE_TOTAL["${phase}"]=$((prev + count))
}

format_ms() {
  local ms="$1"
  local sec=$((ms / 1000))
  local h=$((sec / 3600))
  local m=$(((sec % 3600) / 60))
  local s=$((sec % 60))
  printf "%02d:%02d:%02d" "${h}" "${m}" "${s}"
}

progress_update() {
  local phase="$1"
  local mode="$2"
  local status="$3"

  local now_ms
  now_ms="$(date +%s%3N)"
  local elapsed_ms=$((now_ms - RUN_START_MS))
  local remaining=$((TOTAL_RUNS - COMPLETED_RUNS))
  local avg_ms=0
  if [[ "${EXECUTED_RUNS}" -gt 0 ]]; then
    avg_ms=$((EXECUTED_MS / EXECUTED_RUNS))
  fi
  local eta_ms=$((avg_ms * remaining))
  local pct="0.0"
  if [[ "${TOTAL_RUNS}" -gt 0 ]]; then
    pct=$(awk -v c="${COMPLETED_RUNS}" -v t="${TOTAL_RUNS}" 'BEGIN {printf "%.1f", (c*100)/t}')
  fi

  local avg_sec="0.0"
  if [[ "${EXECUTED_RUNS}" -gt 0 ]]; then
    avg_sec=$(awk -v ms="${avg_ms}" 'BEGIN {printf "%.1f", ms/1000}')
  fi

  local phase_done="${PHASE_DONE["${phase}"]:-0}"
  local phase_total="${PHASE_TOTAL["${phase}"]:-0}"
  echo "progress: ${COMPLETED_RUNS}/${TOTAL_RUNS} (${pct}%) elapsed $(format_ms "${elapsed_ms}") avg ${avg_sec}s eta $(format_ms "${eta_ms}") phase=${phase} ${phase_done}/${phase_total} mode=${mode} status=${status}"
}

mark_run() {
  local phase="$1"
  local mode="$2"
  local status="$3"
  local run_ms="${4:-0}"

  if [[ "${status}" == "done" ]]; then
    EXECUTED_RUNS=$((EXECUTED_RUNS + 1))
    EXECUTED_MS=$((EXECUTED_MS + run_ms))
  fi

  COMPLETED_RUNS=$((COMPLETED_RUNS + 1))
  local prev="${PHASE_DONE["${phase}"]:-0}"
  PHASE_DONE["${phase}"]=$((prev + 1))
  progress_update "${phase}" "${mode}" "${status}"
}

announce_phase_segment() {
  local phase="$1"
  local detail="$2"
  local count="$3"
  local phase_total="${PHASE_TOTAL["${phase}"]:-0}"
  if [[ -n "${detail}" ]]; then
    echo "phase ${phase}: +${count} runs (${detail}) total=${phase_total}"
  else
    echo "phase ${phase}: +${count} runs total=${phase_total}"
  fi
}

run_bench() {
  local phase="$1"
  local mode="$2"
  local records="$3"
  local workers="$4"
  local batch="$5"
  local producers="$6"
  local payload="$7"
  local partitioned="$8"
  local partition_window="$9"
  local partition_lookback="${10}"
  local use_tx="${11}"
  local seed_age="${12}"
  local seed_days="${13}"
  local warmup="${14}"
  local repeat="${15}"

  local checkpoint_key
  checkpoint_key="${phase}|${mode}|${records}|${workers}|${batch}|${producers}|${payload}|${partitioned}|${partition_window}|${partition_lookback}|${PARTITION_AHEAD}|${use_tx}|${seed_age}|${seed_days}|${warmup}|${repeat}"

  if [[ "${RESUME}" == "1" && -n "${DONE["${checkpoint_key}"]+x}" ]]; then
    echo "Skipping ${phase} ${mode} (checkpoint)"
    mark_run "${phase}" "${mode}" "skipped" 0
    return
  fi

  local args=(
    -json=true
    -dsn "${DSN}"
    -mode "${mode}"
    -records "${records}"
    -payload-bytes "${payload}"
    -payload-random="${PAYLOAD_RANDOM}"
    -payload-seed "${PAYLOAD_SEED}"
    -partitioned="${partitioned}"
    -partition-ahead "${PARTITION_AHEAD}"
    -partition-lookback "${partition_lookback}"
    -partition-window "${partition_window}"
    -producer-interval "${MIXED_PRODUCER_INTERVAL}"
    -mixed-duration "${MIXED_DURATION}"
    -drain-timeout "${MIXED_DRAIN_TIMEOUT}"
    -enqueue-timeout "${ENQUEUE_TIMEOUT}"
    -measure-latency="${MEASURE_LATENCY}"
    -auto-target="${AUTO_TARGET}"
    -reset=true
    -seed-age "${seed_age}"
    -seed-days "${seed_days}"
  )

  if [[ "${mode}" == "consume" ]]; then
    args+=(-workers "${workers}" -batch-size "${batch}")
  elif [[ "${mode}" == "mixed" ]]; then
    args+=(-workers "${workers}" -batch-size "${batch}" -producers "${producers}" -use-tx="${use_tx}")
  else
    args+=(-producers "${producers}" -use-tx="${use_tx}")
  fi

  local json
  local bench_cpu_max=0
  local bench_rss_max=0
  local mysql_cpu_max=0
  local mysql_rss_max=0
  local mysql_pid=""
  local json_file
  json_file="$(mktemp)"

  if [[ "${START_MYSQL}" == "1" ]]; then
    mysql_pid="$(docker inspect --format '{{.State.Pid}}' "${MYSQL_CONTAINER_NAME}" 2>/dev/null || true)"
  fi

  local run_start
  run_start="$(date +%s%3N)"
  "${BENCH_BIN}" "${args[@]}" > "${json_file}" &
  local bench_pid=$!

  while kill -0 "${bench_pid}" >/dev/null 2>&1; do
    sample_ps "${bench_pid}" bench_cpu_max bench_rss_max
    if [[ -n "${mysql_pid}" ]]; then
      sample_ps "${mysql_pid}" mysql_cpu_max mysql_rss_max
    fi
    sleep 1
  done
  wait "${bench_pid}"
  local run_end
  run_end="$(date +%s%3N)"
  local run_ms=$((run_end - run_start))

  json="$(cat "${json_file}")"
  rm -f "${json_file}"

  python3 - "${json}" "${mode}" "${records}" "${workers}" "${batch}" "${producers}" "${payload}" \
    "${partitioned}" "${partition_window}" "${partition_lookback}" "${PARTITION_AHEAD}" \
    "${use_tx}" "${seed_age}" "${seed_days}" "${PAYLOAD_RANDOM}" "${PAYLOAD_SEED}" "${AUTO_TARGET}" <<'PY'
import json
import re
import sys

data = json.loads(sys.argv[1])

def parse_bool(value: str) -> bool:
    return str(value).lower() in {"1", "true", "yes", "on"}

def parse_duration(value: str) -> int:
    if value is None:
        return 0
    value = str(value).strip()
    if value in {"", "0"}:
        return 0
    if value.isdigit():
        return int(value)
    if value in {"0ns", "0us", "0µs", "0ms", "0s", "0m", "0h"}:
        return 0
    units = {
        "ns": 1,
        "us": 1000,
        "µs": 1000,
        "ms": 1000_000,
        "s": 1000_000_000,
        "m": 60 * 1000_000_000,
        "h": 3600 * 1000_000_000,
    }
    total = 0
    for number, unit in re.findall(r"(\d+)(ns|us|µs|ms|s|m|h)", value):
        total += int(number) * units[unit]
    if total == 0:
        raise ValueError(f"invalid duration: {value}")
    return total

expected = {
    "mode": sys.argv[2],
    "records": int(sys.argv[3]),
    "workers": int(sys.argv[4]),
    "batch_size": int(sys.argv[5]),
    "producers": int(sys.argv[6]),
    "payload_bytes": int(sys.argv[7]),
    "partitioned": parse_bool(sys.argv[8]),
    "partition_window": parse_duration(sys.argv[9]),
    "partition_lookback": parse_duration(sys.argv[10]),
    "partition_ahead": parse_duration(sys.argv[11]),
    "use_tx": parse_bool(sys.argv[12]),
    "seed_age": parse_duration(sys.argv[13]),
    "seed_days": int(sys.argv[14]),
    "payload_random": parse_bool(sys.argv[15]),
    "payload_seed": int(sys.argv[16]),
    "auto_target": parse_bool(sys.argv[17]),
}

mismatches = []
for key, expected_value in expected.items():
    actual_value = data.get(key)
    if actual_value != expected_value:
        mismatches.append((key, expected_value, actual_value))

if mismatches:
    lines = ["benchmark parameters mismatch (flag parsing error?)"]
    for key, expected_value, actual_value in mismatches:
        lines.append(f"- {key}: expected={expected_value!r} actual={actual_value!r}")
    raise SystemExit("\\n".join(lines))
PY

  python3 - "${json}" "${phase}" "${warmup}" "${repeat}" \
    "${bench_cpu_max}" "${bench_rss_max}" "${mysql_cpu_max}" "${mysql_rss_max}" "${checkpoint_key}" \
    >> "${JSONL}" <<'PY'
import json
import sys

data = json.loads(sys.argv[1])
data["phase"] = sys.argv[2]
data["warmup"] = sys.argv[3] == "1"
data["repeat"] = int(sys.argv[4])
data["bench_cpu_max_percent"] = float(sys.argv[5] or 0)
data["bench_rss_kb"] = int(float(sys.argv[6] or 0))
data["mysql_cpu_max_percent"] = float(sys.argv[7] or 0)
data["mysql_rss_kb"] = int(float(sys.argv[8] or 0))
data["checkpoint_key"] = sys.argv[9]
print(json.dumps(data, separators=(",", ":")))
PY
  mark_run "${phase}" "${mode}" "done" "${run_ms}"
  purge_binlog
}

sample_ps() {
  local pid="$1"
  local cpu_var="$2"
  local rss_var="$3"
  local output

  output="$(ps -o %cpu=,rss= -p "${pid}" 2>/dev/null || true)"
  if [[ -z "${output}" ]]; then
    return
  fi

  local cpu rss
  read -r cpu rss <<< "${output}"
  if [[ -n "${cpu}" ]]; then
    if awk "BEGIN {exit !(${cpu} > ${!cpu_var})}"; then
      printf -v "${cpu_var}" "%s" "${cpu}"
    fi
  fi
  if [[ -n "${rss}" ]]; then
    if [[ "${rss}" -gt "${!rss_var}" ]]; then
      printf -v "${rss_var}" "%s" "${rss}"
    fi
  fi
}

run_matrix() {
  local phase="$1"
  local mode="$2"
  local records="$3"
  local partitioned="$4"
  local partition_window="$5"
  local partition_lookback="$6"
  local use_tx="$7"
  local seed_age="$8"
  local seed_days="$9"

  for workers in "${WORKERS_LIST[@]}"; do
    for batch in "${BATCH_LIST[@]}"; do
      for payload in "${PAYLOAD_LIST[@]}"; do
        if [[ "${mode}" == "consume" ]]; then
          if [[ "${WARMUP}" == "1" ]]; then
            run_bench "${phase}" "${mode}" "${WARMUP_RECORDS}" "${workers}" "${batch}" 0 "${payload}" \
              "${partitioned}" "${partition_window}" "${partition_lookback}" "${use_tx}" "${seed_age}" "${seed_days}" 1 0
          fi
          for repeat in $(seq 1 "${REPEATS}"); do
            run_bench "${phase}" "${mode}" "${records}" "${workers}" "${batch}" 0 "${payload}" \
              "${partitioned}" "${partition_window}" "${partition_lookback}" "${use_tx}" "${seed_age}" "${seed_days}" 0 "${repeat}"
          done
        fi
      done
    done
  done
}

run_consume_phase() {
  local phase="$1"
  local records="$2"
  local partitioned="$3"
  local partition_window="$4"
  local partition_lookback="$5"
  local seed_age="$6"
  local seed_days="$7"

  local combos=$(( ${#WORKERS_LIST[@]} * ${#BATCH_LIST[@]} ))
  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$((combos * runs_per_combo))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "partitioned=${partitioned} window=${partition_window}" "${runs_total}"

  for workers in "${WORKERS_LIST[@]}"; do
    for batch in "${BATCH_LIST[@]}"; do
      local payload=512
      if [[ "${WARMUP}" == "1" ]]; then
        run_bench "${phase}" consume "${WARMUP_RECORDS}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 1 0
      fi
      for repeat in $(seq 1 "${REPEATS}"); do
        run_bench "${phase}" consume "${records}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 0 "${repeat}"
      done
    done
  done
}

select_top_combos() {
  python3 - "${JSONL}" <<'PY'
import json
import sys
from collections import defaultdict

path = sys.argv[1]
data = []
with open(path) as f:
    for line in f:
        row = json.loads(line)
        if row.get("warmup"):
            continue
        if row.get("phase") != "screen":
            continue
        if row.get("mode") != "consume":
            continue
        key = (row["workers"], row["batch_size"])
        data.append((key, row["throughput_msg_per_sec"]))

if not data:
    sys.exit(0)

agg = defaultdict(list)
for key, val in data:
    agg[key].append(val)

ranking = sorted(((sum(vals)/len(vals), key) for key, vals in agg.items()), reverse=True)
for _, key in ranking[:2]:
    print(f"{key[0]},{key[1]}")
PY
}

run_payload_sweep() {
  local phase="$1"
  local records="$2"
  local partitioned="$3"
  local partition_window="$4"
  local partition_lookback="$5"
  local seed_age="$6"
  local seed_days="$7"

  local combos
  combos="$(select_top_combos || true)"
  if [[ -z "${combos}" ]]; then
    combos="4,50"$'\n'"8,100"
  fi
  local combo_count
  combo_count="$(printf '%s\n' "${combos}" | awk 'NF {c++} END {print c+0}')"
  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$((combo_count * ${#PAYLOAD_LIST[@]} * runs_per_combo))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "combos=${combo_count} payloads=${#PAYLOAD_LIST[@]}" "${runs_total}"

  while IFS=',' read -r workers batch; do
    for payload in "${PAYLOAD_LIST[@]}"; do
      if [[ "${WARMUP}" == "1" ]]; then
        run_bench "${phase}" consume "${WARMUP_RECORDS}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 1 0
      fi
      for repeat in $(seq 1 "${REPEATS}"); do
        run_bench "${phase}" consume "${records}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 0 "${repeat}"
      done
    done
  done <<< "${combos}"
}

run_large_payload_sweep() {
  local phase="$1"
  local records="$2"
  local partitioned="$3"
  local partition_window="$4"
  local partition_lookback="$5"
  local seed_age="$6"
  local seed_days="$7"

  local combos
  combos="$(select_top_combos || true)"
  if [[ -z "${combos}" ]]; then
    combos="4,50"
  fi
  local combo_count
  combo_count="$(printf '%s\n' "${combos}" | awk 'NF {c++} END {print c+0}')"
  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$((combo_count * ${#LARGE_PAYLOAD_LIST[@]} * runs_per_combo))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "combos=${combo_count} payloads=${#LARGE_PAYLOAD_LIST[@]}" "${runs_total}"

  while IFS=',' read -r workers batch; do
    for payload in "${LARGE_PAYLOAD_LIST[@]}"; do
      if [[ "${WARMUP}" == "1" ]]; then
        run_bench "${phase}" consume "${WARMUP_RECORDS}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 1 0
      fi
      for repeat in $(seq 1 "${REPEATS}"); do
        run_bench "${phase}" consume "${records}" "${workers}" "${batch}" 0 "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 0 "${repeat}"
      done
    done
  done <<< "${combos}"
}

run_durability() {
  local phase="$1"
  local records="$2"
  local partitioned="$3"
  local partition_window="$4"
  local partition_lookback="$5"
  local seed_age="$6"
  local seed_days="$7"

  local combos
  combos="$(select_top_combos || true)"
  if [[ -z "${combos}" ]]; then
    combos="4,50"
  fi
  local combo_count
  combo_count="$(printf '%s\n' "${combos}" | awk 'NF {c++} END {print c+0}')"
  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$((combo_count * runs_per_combo))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "combos=${combo_count}" "${runs_total}"

  while IFS=',' read -r workers batch; do
    local payload=512
    if [[ "${WARMUP}" == "1" ]]; then
      run_bench "${phase}" consume "${WARMUP_RECORDS}" "${workers}" "${batch}" 0 "${payload}" \
        "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 1 0
    fi
    for repeat in $(seq 1 "${REPEATS}"); do
      run_bench "${phase}" consume "${records}" "${workers}" "${batch}" 0 "${payload}" \
        "${partitioned}" "${partition_window}" "${partition_lookback}" false "${seed_age}" "${seed_days}" 0 "${repeat}"
    done
  done <<< "${combos}"
}

run_enqueue() {
  local phase="$1"
  local records="$2"
  local use_tx="$3"
  local payload="$4"
  local partitioned="$5"
  local partition_lookback="$6"

  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$(( ${#PRODUCERS_LIST[@]} * runs_per_combo ))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "producers=${#PRODUCERS_LIST[@]} payload=${payload}" "${runs_total}"

  for producers in "${PRODUCERS_LIST[@]}"; do
    if [[ "${WARMUP}" == "1" ]]; then
      run_bench "${phase}" enqueue "${WARMUP_RECORDS}" 0 0 "${producers}" "${payload}" \
        "${partitioned}" 0 "${partition_lookback}" "${use_tx}" 0 0 1 0
    fi
    for repeat in $(seq 1 "${REPEATS}"); do
      run_bench "${phase}" enqueue "${records}" 0 0 "${producers}" "${payload}" \
        "${partitioned}" 0 "${partition_lookback}" "${use_tx}" 0 0 0 "${repeat}"
    done
  done
}

run_mixed_phase() {
  local phase="$1"
  local records="$2"
  local partitioned="$3"
  local partition_window="$4"
  local partition_lookback="$5"

  local combos
  combos="$(select_top_combos || true)"
  if [[ -z "${combos}" ]]; then
    combos="4,50"
  fi

  local combo_count
  combo_count="$(printf '%s\n' "${combos}" | awk 'NF {c++} END {print c+0}')"
  local runs_per_combo="${REPEATS}"
  if [[ "${WARMUP}" == "1" ]]; then
    runs_per_combo=$((runs_per_combo + 1))
  fi
  local runs_total=$((combo_count * ${#PRODUCERS_LIST[@]} * runs_per_combo))
  add_phase_runs "${phase}" "${runs_total}"
  announce_phase_segment "${phase}" "combos=${combo_count} producers=${#PRODUCERS_LIST[@]}" "${runs_total}"

  while IFS=',' read -r workers batch; do
    local payload=512
    for producers in "${PRODUCERS_LIST[@]}"; do
      if [[ "${WARMUP}" == "1" ]]; then
        run_bench "${phase}" mixed "${WARMUP_RECORDS}" "${workers}" "${batch}" "${producers}" "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" true 0 0 1 0
      fi
      for repeat in $(seq 1 "${REPEATS}"); do
        run_bench "${phase}" mixed "${records}" "${workers}" "${batch}" "${producers}" "${payload}" \
          "${partitioned}" "${partition_window}" "${partition_lookback}" true 0 0 0 "${repeat}"
      done
    done
  done <<< "${combos}"
}

echo "Running phase: screen (consume workers x batch)"
run_consume_phase "screen" "${CONSUME_RECORDS}" true "${PARTITION_WINDOW}" "${PARTITION_LOOKBACK}" 0 0
purge_binlog

echo "Running phase: payload sweep (top combos)"
run_payload_sweep "payload" "${CONSUME_RECORDS}" true "${PARTITION_WINDOW}" "${PARTITION_LOOKBACK}" 0 0
purge_binlog

echo "Running phase: large payload sweep (top combos)"
run_large_payload_sweep "payload_large" "${LARGE_PAYLOAD_RECORDS}" true "${PARTITION_WINDOW}" "${PARTITION_LOOKBACK}" 0 0
purge_binlog

if [[ "${MIXED}" == "1" ]]; then
  echo "Running phase: mixed workload (top combos)"
  run_mixed_phase "mixed" "${MIXED_RECORDS}" true "${PARTITION_WINDOW}" "${PARTITION_LOOKBACK}"
  purge_binlog
fi

echo "Running phase: partition effect (large, window on/off, partitioned on/off)"
WORKERS_LIST=(4)
BATCH_LIST=(50)
PAYLOAD_LIST=(512)
for partitioned in true false; do
  for window in "${PARTITION_WINDOW}" "0s"; do
    run_consume_phase "partition" "${PARTITION_EFFECT_RECORDS}" "${partitioned}" "${window}" "${PARTITION_EFFECT_LOOKBACK}" "${PARTITION_EFFECT_SEED_AGE}" "${PARTITION_EFFECT_DAYS}"
    purge_binlog
  done
done

echo "Running phase: durability (top combos, durability-specific runs)"
if [[ "${PLAN}" == "full" ]]; then
  run_durability "durable" "${CONSUME_RECORDS}" true "${PARTITION_WINDOW}" "${PARTITION_LOOKBACK}" 0 0
  purge_binlog
fi

echo "Running phase: enqueue (producers sweep, use_tx true/false)"
run_enqueue "enqueue_tx" "${ENQUEUE_RECORDS}" true 512 true "${PARTITION_LOOKBACK}"
purge_binlog
run_enqueue "enqueue_no_tx" "${ENQUEUE_RECORDS}" false 512 true "${PARTITION_LOOKBACK}"
purge_binlog

python3 - "${JSONL}" "${CSV}" "${SUMMARY}" <<'PY'
import csv
import json
import statistics
import sys
from collections import defaultdict

jsonl, csv_path, summary_path = sys.argv[1], sys.argv[2], sys.argv[3]
rows = []
with open(jsonl) as f:
    for line in f:
        rows.append(json.loads(line))

if not rows:
    sys.exit(0)

fieldnames = sorted({k for row in rows for k in row.keys()})
with open(csv_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

metric_fields = [
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "latency_max_ms",
    "latency_mean_ms",
    "max_lag",
    "batch_p50_ms",
    "batch_p95_ms",
    "batch_p99_ms",
    "batch_max_ms",
    "batch_mean_ms",
]

summary = defaultdict(lambda: {"throughput": [], "metrics": {name: [] for name in metric_fields}})
for row in rows:
    if row.get("warmup"):
        continue
    key = (
        row.get("phase"),
        row.get("mode"),
        row.get("workers"),
        row.get("batch_size"),
        row.get("producers"),
        row.get("payload_bytes"),
        row.get("partitioned"),
        row.get("partition_window"),
        row.get("use_tx"),
        row.get("seed_age"),
        row.get("seed_days"),
    )
    summary[key]["throughput"].append(row.get("throughput_msg_per_sec", 0))
    for name in metric_fields:
        value = row.get(name)
        if value is not None:
            summary[key]["metrics"][name].append(value)

with open(summary_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "phase",
        "mode",
        "workers",
        "batch_size",
        "producers",
        "payload_bytes",
        "partitioned",
        "partition_window",
        "use_tx",
        "seed_age",
        "seed_days",
        "mean_throughput",
        "p50",
        "p95",
        "samples",
        *[f"mean_{name}" for name in metric_fields],
    ])
    for key, stats in summary.items():
        values = sorted(stats["throughput"])
        mean = sum(values) / len(values)
        p50 = statistics.median(values)
        if len(values) >= 20:
            p95 = values[int(len(values) * 0.95) - 1]
            p95_out = round(p95, 2)
        else:
            p95_out = ""
        row = [round(mean, 2), round(p50, 2), p95_out, len(values)]
        for name in metric_fields:
            metric_values = stats["metrics"][name]
            if metric_values:
                metric_mean = sum(metric_values) / len(metric_values)
                row.append(round(metric_mean, 2))
            else:
                row.append("")
        writer.writerow(list(key) + row)
PY

echo "Results written to ${OUT_DIR}"
