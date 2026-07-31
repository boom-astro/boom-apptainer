#!/usr/bin/env bash

# Benchmark only the alert-worker wall time (no kafka consumer running during
# the measurement window).
#
# Sequence:
#   1. Reset state (mongo + valkey + kafka output topics).
#   2. PREFILL: run kafka_consumer in the background and poll LLEN until
#      ZTF_alerts_packets_queue == EXPECTED_ALERTS, then kill the consumer.
#      No scheduler runs during this phase. (We do NOT use --exit-on-eof:
#      with multiple partitions, librdkafka treats a momentarily empty local
#      buffer on one partition as EOF and exits before draining the others.)
#   3. Start scheduler (config has n_enrichment=0 and n_filter=0 so only alert
#      workers consume the packets queue).
#   4. START measurement: wait for N "alert worker ready" log lines.
#   5. END measurement: LLEN(ZTF_alerts_enrichment_queue) == EXPECTED_ALERTS.
#
# The scheduler indexing / connection setup overhead is excluded from the
# measurement because we wait for the "alert worker ready" markers, which are
# emitted right before each worker enters its consume loop (mirrors the
# enrichment-only START signal).
#
# Prerequisite: the benchmark services (mongo, valkey, kafka, boom) must be
# running. Bring them up with:
#     python3 tests/throughput/run.py --apptainer --phase setup
#
# Required env vars:
#   BOOM_REPO_ROOT, BENCHMARK_MONGO_PORT, BENCHMARK_REDIS_PORT,
#   BENCHMARK_KAFKA_PORT, BENCHMARK_N_ALERT (number of alert workers),
#   TIMEOUT_SECS (optional, default 300),
#   BENCHMARK_MAX_IN_QUEUE (optional, default 50000; must be > EXPECTED_ALERTS).
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

LOGS_DIR="${1:-$BOOM_REPO_ROOT/logs/boom_alert_worker_only}"
mkdir -p "$LOGS_DIR"

EXPECTED_ALERTS=29142
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
MAX_IN_QUEUE="${BENCHMARK_MAX_IN_QUEUE:-50000}"
N_ALERT="${BENCHMARK_N_ALERT:?BENCHMARK_N_ALERT must be set (number of alert workers)}"

if [ "$MAX_IN_QUEUE" -le "$EXPECTED_ALERTS" ]; then
    echo -e "${RED}Error: BENCHMARK_MAX_IN_QUEUE ($MAX_IN_QUEUE) must be > EXPECTED_ALERTS ($EXPECTED_ALERTS).${END}"
    echo "Otherwise the prefill consumer blocks on a full packets queue (no alert workers run during prefill)."
    exit 1
fi

ZTF_OUTPUT_TOPICS=(
    "babamul.ztf.lsst-match.stellar"
    "babamul.ztf.lsst-match.hosted"
    "babamul.ztf.lsst-match.hostless"
    "babamul.ztf.no-lsst-match.stellar"
    "babamul.ztf.no-lsst-match.hosted"
    "babamul.ztf.no-lsst-match.hostless"
    "ZTF_alerts_results"
)
ZTF_OUTPUT_PARTITIONS=15

# Valkey queues the alert worker touches (and downstream queues, wiped for
# cleanliness so LLEN measurements are not inflated by stale entries).
ALERT_WORKER_QUEUES=(
    "ZTF_alerts_packets_queue"
    "ZTF_alerts_packets_queue_temp"
    "ZTF_alerts_enrichment_queue"
)

cleanup() {
    trap '' INT TERM
    echo "$(current_datetime) - Cleaning up background processes"
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

run_mongo_query() {
    local query="$1"
    local as_admin="${2:-false}"
    local auth=""
    if [ "$as_admin" == "true" ]; then
        auth="/admin?authSource=admin"
    fi
    apptainer exec instance://benchmark_mongo mongosh \
        "mongodb://mongoadmin:mongoadminsecret@localhost:${MONGO_PORT}${auth}" \
        --quiet --eval "$query"
}

reset_mutable_state() {
    echo "$(current_datetime) - Resetting mutable state for alert-worker-only iteration"

    run_mongo_query "
        const target = db.getSiblingDB('boom-benchmarking');
        target.ZTF_alerts.drop();
        target.ZTF_alerts_cutouts.drop();
        target.ZTF_alerts_aux.drop();
        target.ZTF_alerts_aux_snapshot.aggregate([{ \$out: 'ZTF_alerts_aux' }]);
        target.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' });
    " "true" > /dev/null

    for queue in "${ALERT_WORKER_QUEUES[@]}"; do
        valkey_cli del "$queue" > /dev/null
    done

    local json_path
    json_path=$(mktemp /tmp/boom-alert-worker-only-delete-records.XXXXXX.json)
    {
        printf '{"partitions":['
        local first=true
        for topic in "${ZTF_OUTPUT_TOPICS[@]}"; do
            for p in $(seq 0 $((ZTF_OUTPUT_PARTITIONS - 1))); do
                if [ "$first" = true ]; then
                    first=false
                else
                    printf ','
                fi
                printf '{"topic":"%s","partition":%d,"offset":-1}' "$topic" "$p"
            done
        done
        printf '],"version":1}\n'
    } > "$json_path"
    apptainer exec instance://benchmark_kafka \
        /opt/kafka/bin/kafka-delete-records.sh \
        --bootstrap-server localhost:"$KAFKA_PORT" \
        --offset-json-file "$json_path" > /dev/null
    rm -f "$json_path"

    echo "$(current_datetime) - Mutable state reset"
}

reset_mutable_state

: > "$LOGS_DIR/prefill_consumer.log"
: > "$LOGS_DIR/scheduler.log"

# -----------------------------
# PREFILL phase. Run kafka_consumer in the background and poll LLEN until it
# hits EXPECTED_ALERTS, then kill it. We do NOT use --exit-on-eof here: with
# multiple partitions, librdkafka treats a momentarily empty local buffer on
# one partition as EOF and the consumer exits before draining the others,
# leaving the packets queue partially populated. Polling LLEN is the same
# pattern _run_alert_only.sh uses for its end-of-iteration signal.
#
# The config (written by run_alert_worker_only.py) carries a fresh group_id
# so the consumer always starts from earliest, regardless of any committed
# offsets from prior runs.
# -----------------------------
echo && echo "$(current_datetime) - Prefilling packets queue (max-in-queue=$MAX_IN_QUEUE)"
apptainer exec --pwd /app instance://benchmark_boom \
    /app/kafka_consumer ztf 20250311 --programids public \
        --max-in-queue "$MAX_IN_QUEUE" \
    > "$LOGS_DIR/prefill_consumer.log" 2>&1 &
PREFILL_PID=$!
BG_PIDS+=($PREFILL_PID)

echo "$(current_datetime) - Waiting for LLEN(ZTF_alerts_packets_queue) == $EXPECTED_ALERTS"
PREFILL_START=$(date +%s)
QUEUE_LEN=0
while [ "$QUEUE_LEN" -lt "$EXPECTED_ALERTS" ]; do
    QUEUE_LEN=$(valkey_llen "ZTF_alerts_packets_queue")
    if [ $(($(date +%s) - PREFILL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Prefill timeout: packets queue has $QUEUE_LEN / $EXPECTED_ALERTS alerts${END}"
        echo "See $LOGS_DIR/prefill_consumer.log for consumer output."
        exit 1
    fi
    sleep 1
done

# Stop the prefill consumer; the alert workers will drain the prefilled queue
# without it (and we want a clean separation between prefill and measurement).
kill "$PREFILL_PID" 2>/dev/null || true
wait "$PREFILL_PID" 2>/dev/null || true
# Drop the dead PID from BG_PIDS so the EXIT trap does not try to kill it again.
for i in "${!BG_PIDS[@]}"; do
    if [ "${BG_PIDS[$i]}" = "$PREFILL_PID" ]; then
        unset 'BG_PIDS[i]'
    fi
done
BG_PIDS=("${BG_PIDS[@]}")
echo "$(current_datetime) - Prefill OK: $QUEUE_LEN alerts queued"

# -----------------------------
# Start Scheduler. Config sets n_enrichment=0 and n_filter=0 so only the alert
# workers consume the packets queue. We do NOT drop ZTF_alerts_packets_queue
# here — it is the prefilled input we want to measure draining.
# -----------------------------
echo && echo "$(current_datetime) - Starting Scheduler"
apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
BG_PIDS+=($SCHEDULER_PID)
echo -e "${GREEN}Scheduler started${END}"

# -----------------------------
# Wait for N "alert worker ready" log lines (START of measurement). Emitted by
# run_alert_worker just before entering the consume loop (src/alert/base.rs).
# This matches the enrichment-only "enrichment worker ready" pattern and
# excludes scheduler startup / index creation / mongo+redis client init from
# the measurement.
# -----------------------------
echo "$(current_datetime) - Waiting for $N_ALERT 'alert worker ready' log lines"
WAIT_START=$(date +%s)
while true; do
    READY_COUNT=$(grep -c "alert worker ready" "$LOGS_DIR/scheduler.log" 2>/dev/null || true)
    READY_COUNT=${READY_COUNT:-0}
    if [ "$READY_COUNT" -ge "$N_ALERT" ]; then
        break
    fi
    if [ $(($(date +%s) - WAIT_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout waiting for alert workers ready ($READY_COUNT / $N_ALERT)${END}"
        exit 1
    fi
    sleep 1
done

# Capture the START timestamp from the N-th "alert worker ready" log line, so
# the wall-time matches what the worker pool saw rather than this shell loop's
# polling cadence.
T_START_LINE=$(grep "alert worker ready" "$LOGS_DIR/scheduler.log" | sed -n "${N_ALERT}p")
echo "$(current_datetime) - All $N_ALERT alert workers ready"

# -----------------------------
# Poll LLEN(ZTF_alerts_enrichment_queue) until EXPECTED_ALERTS (END of
# measurement).
# -----------------------------
echo "$(current_datetime) - Waiting for LLEN(ZTF_alerts_enrichment_queue) == $EXPECTED_ALERTS"
POLL_START=$(date +%s)
QUEUE_LEN=0
while [ "$QUEUE_LEN" -lt "$EXPECTED_ALERTS" ]; do
    QUEUE_LEN=$(valkey_llen "ZTF_alerts_enrichment_queue")
    if [ $(($(date +%s) - POLL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "${RED}Timeout: enrichment queue has $QUEUE_LEN / $EXPECTED_ALERTS alerts${END}"
        exit 1
    fi
    sleep 1
done

T_END=$(current_datetime)
echo "$T_END" > "$LOGS_DIR/alert_worker_end_time.txt"
# Persist the start timestamp from the scheduler log so the Python wrapper can
# read it without re-grepping (and so the line stays adjacent to the end time).
echo "$T_START_LINE" > "$LOGS_DIR/alert_worker_start_line.txt"
echo -e "$(current_datetime) - ${GREEN}All $EXPECTED_ALERTS alerts pushed to ZTF_alerts_enrichment_queue${END}"

if [ ${#BG_PIDS[@]} -gt 0 ]; then
    kill "${BG_PIDS[@]}" 2>/dev/null || true
    wait "${BG_PIDS[@]}" 2>/dev/null || true
    BG_PIDS=()
fi

echo -e "$(current_datetime) - ${GREEN}Alert-worker-only benchmark iteration complete${END}"
exit 0
