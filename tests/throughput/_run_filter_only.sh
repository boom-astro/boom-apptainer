#!/usr/bin/env bash

# Benchmark only the filter-worker latency.
#
# Measures the wall time between:
#   - START: the scheduler logs "starting filter worker"
#   - END:   every alert has been processed by every filter. Mirrors _run.sh:
#            sum the last `/`-delimited number across all "passed filter" log
#            lines (= total alerts processed across all filter executions),
#            then divide by N_FILTERS to get alerts processed per filter, and
#            wait for that to reach EXPECTED_ALERTS. This is independent of
#            pass-rate and so survives filter-logic changes.
#
# Prerequisites (this script does NOT pre-fill state; it validates it):
#   1. The benchmark services (mongo, valkey, kafka, boom) must be running.
#      Bring them up with `python3 tests/throughput/run.py --apptainer --phase setup`.
#   2. `ZTF_alerts_filter_queue` must already contain EXPECTED_ALERTS entries
#      and MongoDB must already contain the corresponding alert documents +
#      enrichment data. The natural way to reach this state is to run
#      `run_alert_only.py` followed by `run_enrichment_only.py`.
#
# Usage: $0 [logs_dir]

set -euo pipefail

YELLOW="\e[33m"
GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

if [ -z "${BOOM_REPO_ROOT:-}" ]; then
    echo "Error: BOOM_REPO_ROOT is not set; set BOOM_REPO_ROOT environment variable"
    exit 1
fi

MONGO_PORT=$BENCHMARK_MONGO_PORT
REDIS_PORT=$BENCHMARK_REDIS_PORT
KAFKA_PORT=$BENCHMARK_KAFKA_PORT

TESTS_DIR="$BOOM_REPO_ROOT/tests"
BG_PIDS=()

LOGS_DIR="${1:-$BOOM_REPO_ROOT/logs/boom_filter_only}"
mkdir -p "$LOGS_DIR"

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"

cleanup() {
    trap '' INT TERM
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
        BG_PIDS=()
    fi
}
trap cleanup EXIT INT TERM

require_instance() {
    local name="$1"
    if ! apptainer instance list | awk 'NR > 1 {print $1}' | grep -qx "$name"; then
        echo -e "${RED}Error: apptainer instance '$name' is not running.${END}"
        echo "Run 'python3 tests/throughput/run.py --apptainer --phase setup' first."
        exit 1
    fi
}
for instance in benchmark_mongo benchmark_valkey benchmark_kafka benchmark_boom; do
    require_instance "$instance"
done

valkey_cli() {
    apptainer exec instance://benchmark_valkey valkey-cli -p "$REDIS_PORT" "$@"
}
valkey_llen() {
    local raw
    raw=$(valkey_cli llen "$1" 2>/dev/null || echo 0)
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

# Precondition: filter queue must already be fully populated.
FILTER_QUEUE_LEN=$(valkey_llen "ZTF_alerts_filter_queue")
if [ "$FILTER_QUEUE_LEN" -ne "$EXPECTED_ALERTS" ]; then
    echo -e "${RED}Error: ZTF_alerts_filter_queue has $FILTER_QUEUE_LEN entries, expected $EXPECTED_ALERTS.${END}"
    echo "Run 'run_alert_only.py' followed by 'run_enrichment_only.py' first."
    exit 1
fi
echo "$(current_datetime) - Precondition OK: ZTF_alerts_filter_queue has $FILTER_QUEUE_LEN alerts"

: > "$LOGS_DIR/scheduler.log"

# -----------------------------
# Start scheduler. The config (written by run_filter_only.py) sets n_alert=0
# and n_enrichment=0 so only the filter workers run.
# -----------------------------
echo "$(current_datetime) - Starting Scheduler"
apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
BG_PIDS+=($SCHEDULER_PID)

echo "$(current_datetime) - Waiting for first 'starting filter worker' log line"
WAIT_START=$(date +%s)
while ! grep -q "starting filter worker" "$LOGS_DIR/scheduler.log" 2>/dev/null; do
    if [ $(($(date +%s) - WAIT_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout waiting for filter worker startup${END}"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - Filter workers started"

# Wait until every alert has been seen by every filter. Mirrors _run.sh: sum
# the last `/`-delimited number on each "passed filter" log line (= total
# alerts the filter processed in that batch), then divide by N_FILTERS to get
# per-filter processed count.
echo "$(current_datetime) - Waiting for every alert to be processed by every filter"
POLL_START=$(date +%s)
PASSED_ALERTS=0
while [ "$PASSED_ALERTS" -lt "$EXPECTED_ALERTS" ]; do
    PASSED_ALERTS=$(grep "passed filter" "$LOGS_DIR/scheduler.log" 2>/dev/null \
        | awk -F'/' '{sum += $NF} END {print sum + 0}')
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    if [ $(($(date +%s) - POLL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout: $PASSED_ALERTS / $EXPECTED_ALERTS alerts processed${END}"
        exit 1
    fi
    sleep 1
done

T_END=$(current_datetime)
echo "$T_END" > "$LOGS_DIR/filter_worker_end_time.txt"
echo -e "$(current_datetime) - ${GREEN}All $EXPECTED_ALERTS alerts processed by all $N_FILTERS filters${END}"

if [ ${#BG_PIDS[@]} -gt 0 ]; then
    kill "${BG_PIDS[@]}" 2>/dev/null || true
    wait "${BG_PIDS[@]}" 2>/dev/null || true
    BG_PIDS=()
fi
exit 0
