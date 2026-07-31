#!/usr/bin/env bash

# Benchmark only the alert-worker latency.
#
# Measures the wall time between:
#   - START: the Kafka consumer receives its first message
#   - END:   the alert worker has pushed all alerts to the Valkey enrichment
#            queue (signal: LLEN ZTF_alerts_enrichment_queue == EXPECTED_ALERTS)
#
# Prerequisite: the benchmark services (mongo, valkey, kafka, boom) must
# already be running. Bring them up once with:
#     python3 tests/throughput/run.py --apptainer --phase setup
# and tear them down at the end with:
#     python3 tests/throughput/run.py --apptainer --phase teardown
#
# This script is intentionally apptainer-only and reuses the same env vars as
# tests/throughput/_run.sh (BOOM_REPO_ROOT, BENCHMARK_*_PORT, TIMEOUT_SECS).
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

LOGS_DIR="${1:-$BOOM_REPO_ROOT/logs/boom_alert_only}"
mkdir -p "$LOGS_DIR"

EXPECTED_ALERTS=29142
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"

# Output topics created by the BOOM pipeline. We reset them to give a clean
# Kafka state, matching _run.sh's bench-phase behavior.
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

# Valkey queues touched by the alert worker (input + temp + output).
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

valkey_cli() {
    apptainer exec instance://benchmark_valkey valkey-cli -p "$REDIS_PORT" "$@"
}

valkey_llen() {
    local raw
    raw=$(valkey_cli llen "$1" 2>/dev/null || echo 0)
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

reset_mutable_state() {
    echo "$(current_datetime) - Resetting mutable state for alert-only iteration"

    # Mongo: drop populated collections, restore aux from the snapshot built by
    # apptainer_mongo-init.sh during the setup phase.
    run_mongo_query "
        const target = db.getSiblingDB('boom-benchmarking');
        target.ZTF_alerts.drop();
        target.ZTF_alerts_cutouts.drop();
        target.ZTF_alerts_aux.drop();
        target.ZTF_alerts_aux_snapshot.aggregate([{ \$out: 'ZTF_alerts_aux' }]);
        target.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' });
    " "true" > /dev/null

    # Valkey: explicitly drop the alert-worker queues so leftover entries from
    # a prior run cannot inflate the LLEN measurement. FLUSHALL is blocked by
    # VALKEY_DISABLE_COMMANDS in the SIF, so use DEL on the known keys.
    for queue in "${ALERT_WORKER_QUEUES[@]}"; do
        valkey_cli del "$queue" > /dev/null
    done

    # Kafka: same delete-records trick as _run.sh's bench phase — advance
    # log-start-offset to high-water-mark for each partition of each output
    # topic, so previous-iteration records are gone but topic metadata stays.
    local json_path
    json_path=$(mktemp /tmp/boom-alert-only-delete-records.XXXXXX.json)
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

# Fresh logs for this iteration
: > "$LOGS_DIR/scheduler.log"
: > "$LOGS_DIR/consumer.log"

PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')

# -----------------------------
# Start Scheduler
# -----------------------------
# The scheduler is launched against the same config.yaml the setup phase used.
# run_alert_only.py is responsible for writing that config with n_enrichment=0
# and n_filter=0 so the alert worker is the only stage that consumes the
# packets queue.
echo && echo "$(current_datetime) - Starting Scheduler"
apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
BG_PIDS+=($SCHEDULER_PID)
echo -e "${GREEN}Scheduler started${END}"

# Wait for GPU readiness before starting the consumer, so that warmup time is
# not part of the measured alert-worker wall time.
if [ "${BOOM_GPU__ENABLED:-false}" = "true" ] && [ "$PLATFORM" = "linux" ]; then
    echo "$(current_datetime) - Waiting for GPU readiness before starting Consumer"
    WARMUP_START=$(date +%s)
    while ! grep -q "all GPU model sets loaded successfully" "$LOGS_DIR/scheduler.log" 2>/dev/null; do
        if [ $(($(date +%s) - WARMUP_START)) -ge "$TIMEOUT_SECS" ]; then
            echo -e "$(current_datetime) - ${RED}Timeout waiting for GPU warmup${END}"
            exit 1
        fi
        sleep 1
    done
    echo "$(current_datetime) - GPU warmup completed in $(( $(date +%s) - WARMUP_START )) seconds"
fi

# -----------------------------
# Start Consumer (single instance)
# -----------------------------
# The alert-only benchmark intentionally uses a single consumer to keep the
# measurement focused on alert-worker latency rather than consumer parallelism.
echo && echo "$(current_datetime) - Starting Consumer"
apptainer exec --pwd /app instance://benchmark_boom \
    /app/kafka_consumer ztf 20250311 --programids public \
    > "$LOGS_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$!
BG_PIDS+=($CONSUMER_PID)
echo -e "${GREEN}Consumer started${END}"

# -----------------------------
# Wait for the consumer to log its first message. This is the START of the
# alert-worker latency measurement.
# -----------------------------
echo && echo "$(current_datetime) - Waiting for Consumer first message"
WAIT_START=$(date +%s)
while ! grep -q "Consumer received first message, continuing..." "$LOGS_DIR/consumer.log" 2>/dev/null; do
    if [ $(($(date +%s) - WAIT_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "$(current_datetime) - ${RED}Timeout waiting for Consumer first message${END}"
        exit 1
    fi
    sleep 1
done
echo "$(current_datetime) - Consumer received first message"

# -----------------------------
# Poll Valkey until the alert worker has pushed every alert. This is the END
# of the alert-worker latency measurement.
# -----------------------------
echo "$(current_datetime) - Waiting for alert worker to push all $EXPECTED_ALERTS alerts to ZTF_alerts_enrichment_queue"
POLL_START=$(date +%s)
QUEUE_LEN=0
while [ "$QUEUE_LEN" -lt "$EXPECTED_ALERTS" ]; do
    QUEUE_LEN=$(valkey_llen "ZTF_alerts_enrichment_queue")
    if [ $(($(date +%s) - POLL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "$(current_datetime) - ${RED}Timeout: enrichment queue has $QUEUE_LEN / $EXPECTED_ALERTS alerts${END}"
        exit 1
    fi
    sleep 1
done

# Record the END timestamp in the same UTC format used by current_datetime so
# the Python wrapper can subtract it from the consumer.log "first message"
# timestamp.
T_END=$(current_datetime)
echo "$T_END" > "$LOGS_DIR/alert_worker_end_time.txt"
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts pushed to ZTF_alerts_enrichment_queue"

# Stop scheduler + consumer; services stay up for the next iteration.
if [ ${#BG_PIDS[@]} -gt 0 ]; then
    kill "${BG_PIDS[@]}" 2>/dev/null || true
    wait "${BG_PIDS[@]}" 2>/dev/null || true
    BG_PIDS=()
fi

echo -e "$(current_datetime) - ${GREEN}Alert-only benchmark iteration complete${END}"
exit 0
