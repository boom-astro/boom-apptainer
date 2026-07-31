#!/usr/bin/env bash

# Benchmark only the enrichment-worker latency.
#
# Measures the wall time between:
#   - START: the scheduler logs "starting enrichment worker" (i.e. the worker
#            pool is up and any GPU/ONNX warmup that runs at scheduler startup
#            has already completed)
#   - END:   the enrichment worker has pushed every alert to the filter queue
#            (signal: LLEN ZTF_alerts_filter_queue == EXPECTED_ALERTS)
#
# Prerequisites (this script does NOT pre-fill state; it validates it):
#   1. The benchmark services (mongo, valkey, kafka, boom) must be running.
#      Bring them up with `python3 tests/throughput/run.py --apptainer --phase setup`.
#   2. `ZTF_alerts_enrichment_queue` must already contain EXPECTED_ALERTS
#      candids and MongoDB must already contain the corresponding alert
#      documents. The natural way to get this state is to run
#      `python3 tests/throughput/run_alert_only.py --apptainer ...` first.
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

LOGS_DIR="${1:-$BOOM_REPO_ROOT/logs/boom_enrichment_only}"
mkdir -p "$LOGS_DIR"

EXPECTED_ALERTS=29142
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
N_ENRICHMENT="${BENCHMARK_N_ENRICHMENT:?BENCHMARK_N_ENRICHMENT must be set (number of enrichment workers)}"

# Babamul kafka topics are populated by the enrichment worker; reset them so
# their offsets reflect only this iteration. ZTF_alerts_results is filter-only
# so we leave it alone here.
BABAMUL_OUTPUT_TOPICS=(
    "babamul.ztf.lsst-match.stellar"
    "babamul.ztf.lsst-match.hosted"
    "babamul.ztf.lsst-match.hostless"
    "babamul.ztf.no-lsst-match.stellar"
    "babamul.ztf.no-lsst-match.hosted"
    "babamul.ztf.no-lsst-match.hostless"
)
ZTF_OUTPUT_PARTITIONS=15

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

# Precondition: enrichment queue must already be fully populated.
ENRICHMENT_QUEUE_LEN=$(valkey_llen "ZTF_alerts_enrichment_queue")
if [ "$ENRICHMENT_QUEUE_LEN" -ne "$EXPECTED_ALERTS" ]; then
    echo -e "${RED}Error: ZTF_alerts_enrichment_queue has $ENRICHMENT_QUEUE_LEN entries, expected $EXPECTED_ALERTS.${END}"
    echo "Run 'python3 tests/throughput/run_alert_only.py --apptainer ...' first to pre-fill the queue."
    exit 1
fi
echo "$(current_datetime) - Precondition OK: ZTF_alerts_enrichment_queue has $ENRICHMENT_QUEUE_LEN alerts"

# Reset the next-stage queue + babamul kafka topics so the LLEN / offset
# measurements at the end of this iteration only see this iteration's output.
valkey_cli del "ZTF_alerts_filter_queue" > /dev/null

json_path=$(mktemp /tmp/boom-enrichment-only-delete-records.XXXXXX.json)
{
    printf '{"partitions":['
    first=true
    for topic in "${BABAMUL_OUTPUT_TOPICS[@]}"; do
        for p in $(seq 0 $((ZTF_OUTPUT_PARTITIONS - 1))); do
            if [ "$first" = true ]; then first=false; else printf ','; fi
            printf '{"topic":"%s","partition":%d,"offset":-1}' "$topic" "$p"
        done
    done
    printf '],"version":1}\n'
} > "$json_path"
apptainer exec instance://benchmark_kafka /opt/kafka/bin/kafka-delete-records.sh \
    --bootstrap-server localhost:"$KAFKA_PORT" --offset-json-file "$json_path" > /dev/null
rm -f "$json_path"

: > "$LOGS_DIR/scheduler.log"

# -----------------------------
# Start scheduler. The config (written by run_enrichment_only.py) sets
# n_alert=0 and n_filter=0 so only the enrichment workers consume the
# enrichment queue. With no consumer/alert worker, the input is the pre-filled
# valkey queue we just validated.
# -----------------------------
echo "$(current_datetime) - Starting Scheduler"
apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
BG_PIDS+=($SCHEDULER_PID)

# Wait for all N workers to log "enrichment worker ready" (emitted just before
# each worker enters its consume loop, AFTER model load + redis/mongo init).
# This is a clean start marker regardless of CPU/GPU mode.
echo "$(current_datetime) - Waiting for $N_ENRICHMENT 'enrichment worker ready' log lines"
WAIT_START=$(date +%s)
while true; do
    READY_COUNT=$(grep -c "enrichment worker ready" "$LOGS_DIR/scheduler.log" 2>/dev/null || true)
    READY_COUNT=${READY_COUNT:-0}
    if [ "$READY_COUNT" -ge "$N_ENRICHMENT" ]; then
        break
    fi
    if [ $(($(date +%s) - WAIT_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout waiting for enrichment workers ready ($READY_COUNT / $N_ENRICHMENT)${END}"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - All $N_ENRICHMENT enrichment workers ready"

# Poll the output queue until the enrichment worker has pushed every alert.
echo "$(current_datetime) - Waiting for LLEN(ZTF_alerts_filter_queue) == $EXPECTED_ALERTS"
POLL_START=$(date +%s)
QUEUE_LEN=0
while [ "$QUEUE_LEN" -lt "$EXPECTED_ALERTS" ]; do
    QUEUE_LEN=$(valkey_llen "ZTF_alerts_filter_queue")
    if [ $(($(date +%s) - POLL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout: filter queue has $QUEUE_LEN / $EXPECTED_ALERTS alerts${END}"
        exit 1
    fi
    sleep 1
done

T_END=$(current_datetime)
echo "$T_END" > "$LOGS_DIR/enrichment_worker_end_time.txt"
echo -e "$(current_datetime) - ${GREEN}All $EXPECTED_ALERTS alerts pushed to ZTF_alerts_filter_queue${END}"

if [ ${#BG_PIDS[@]} -gt 0 ]; then
    kill "${BG_PIDS[@]}" 2>/dev/null || true
    wait "${BG_PIDS[@]}" 2>/dev/null || true
    BG_PIDS=()
fi
exit 0
