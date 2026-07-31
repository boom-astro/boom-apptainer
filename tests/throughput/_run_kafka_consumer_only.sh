#!/usr/bin/env bash

# Benchmark only the kafka_consumer wall time.
#
# Measures the wall time between:
#   - START: the Kafka consumer logs its first message
#            ("Consumer received first message, continuing...")
#   - END:   LLEN ZTF_alerts_packets_queue == EXPECTED_ALERTS
#
# No scheduler / alert workers run during this benchmark, so the packets
# queue strictly accumulates and the LLEN signal cleanly marks the end of
# the kafka -> valkey transfer.
#
# Prerequisite: the benchmark services (mongo, valkey, kafka, boom) must
# already be running. Bring them up once with:
#     python3 tests/throughput/run.py --apptainer --phase setup
# and tear them down at the end with:
#     python3 tests/throughput/run.py --apptainer --phase teardown
#
# Required env vars (mirroring _run_alert_only.sh):
#   BOOM_REPO_ROOT, BENCHMARK_MONGO_PORT, BENCHMARK_REDIS_PORT,
#   BENCHMARK_KAFKA_PORT, BENCHMARK_N_PROCESSES (kafka_consumer --processes N),
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

LOGS_DIR="${1:-$BOOM_REPO_ROOT/logs/boom_kafka_consumer_only}"
mkdir -p "$LOGS_DIR"

EXPECTED_ALERTS=29142
TIMEOUT_SECS="${TIMEOUT_SECS:-300}"
MAX_IN_QUEUE="${BENCHMARK_MAX_IN_QUEUE:-50000}"
N_PROCESSES="${BENCHMARK_N_PROCESSES:?BENCHMARK_N_PROCESSES must be set (kafka_consumer --processes)}"

if [ "$MAX_IN_QUEUE" -le "$EXPECTED_ALERTS" ]; then
    echo -e "${RED}Error: BENCHMARK_MAX_IN_QUEUE ($MAX_IN_QUEUE) must be > EXPECTED_ALERTS ($EXPECTED_ALERTS).${END}"
    echo "Otherwise the consumer blocks on a full packets queue (no alert workers drain it here)."
    exit 1
fi

# Output topics produced by the BOOM pipeline. We reset them to give a clean
# Kafka state, matching _run.sh's bench-phase behavior. They have no impact on
# the kafka_consumer-only measurement itself (no scheduler runs), but keeping
# the reset symmetric across all *_only benchmarks avoids leaks between runs.
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

# Valkey queues the consumer writes to (and adjacent ones from later stages,
# wiped for cleanliness).
CONSUMER_QUEUES=(
    "ZTF_alerts_packets_queue"
    "ZTF_alerts_packets_queue_temp"
    "ZTF_alerts_enrichment_queue"
    "ZTF_alerts_filter_queue"
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
    echo "$(current_datetime) - Resetting mutable state for kafka-consumer-only iteration"

    # Mongo: restore the alert-aux snapshot even though no scheduler runs in
    # this benchmark. Keeping the reset identical across *_only modes avoids
    # cross-iteration state leaks if the user chains different modes.
    run_mongo_query "
        const target = db.getSiblingDB('boom-benchmarking');
        target.ZTF_alerts.drop();
        target.ZTF_alerts_cutouts.drop();
        target.ZTF_alerts_aux.drop();
        target.ZTF_alerts_aux_snapshot.aggregate([{ \$out: 'ZTF_alerts_aux' }]);
        target.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' });
    " "true" > /dev/null

    # Valkey: wipe the packets queue + any downstream queues that could have
    # leftover entries from a prior run.
    for queue in "${CONSUMER_QUEUES[@]}"; do
        valkey_cli del "$queue" > /dev/null
    done

    # Kafka: advance log-start-offset to high-water-mark for every partition of
    # every output topic. The input topic ztf_20250311_programid1 is left
    # untouched; we rely on the per-iteration consumer group_id (written by the
    # Python wrapper) to start from the earliest offset on every iteration.
    local json_path
    json_path=$(mktemp /tmp/boom-kafka-only-delete-records.XXXXXX.json)
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

: > "$LOGS_DIR/consumer.log"

# -----------------------------
# Start Consumer (single instance, no scheduler).
# --max-in-queue is forced > EXPECTED_ALERTS so the consumer can push every
# alert into valkey without backpressure (no alert worker is draining it).
# -----------------------------
BATCH_SIZE="${BENCHMARK_KAFKA_BATCH_SIZE:-}"
BATCH_ENV_FLAGS=()
if [ -n "$BATCH_SIZE" ]; then
    BATCH_ENV_FLAGS=(--env "BOOM_KAFKA_BATCH_SIZE=$BATCH_SIZE")
fi
echo && echo "$(current_datetime) - Starting Consumer (processes=$N_PROCESSES, max-in-queue=$MAX_IN_QUEUE, batch_size=${BATCH_SIZE:-default})"
apptainer exec --pwd /app "${BATCH_ENV_FLAGS[@]}" instance://benchmark_boom \
    /app/kafka_consumer ztf 20250311 --programids public \
        --processes "$N_PROCESSES" --max-in-queue "$MAX_IN_QUEUE" \
    > "$LOGS_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$!
BG_PIDS+=($CONSUMER_PID)
echo -e "${GREEN}Consumer started${END}"

# -----------------------------
# Wait for the consumer's first-message log line (START of measurement).
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
# Poll LLEN ZTF_alerts_packets_queue until it hits EXPECTED_ALERTS (END of
# measurement). Since no alert worker pops from it, the queue strictly grows.
# -----------------------------
echo "$(current_datetime) - Waiting for LLEN(ZTF_alerts_packets_queue) == $EXPECTED_ALERTS"
POLL_START=$(date +%s)
QUEUE_LEN=0
while [ "$QUEUE_LEN" -lt "$EXPECTED_ALERTS" ]; do
    QUEUE_LEN=$(valkey_llen "ZTF_alerts_packets_queue")
    if [ $(($(date +%s) - POLL_START)) -ge "$TIMEOUT_SECS" ]; then
        echo -e "$(current_datetime) - ${RED}Timeout: packets queue has $QUEUE_LEN / $EXPECTED_ALERTS alerts${END}"
        exit 1
    fi
    sleep 1
done

T_END=$(current_datetime)
echo "$T_END" > "$LOGS_DIR/kafka_consumer_end_time.txt"
echo -e "$(current_datetime) - ${GREEN}All $EXPECTED_ALERTS alerts pushed to ZTF_alerts_packets_queue${END}"

if [ ${#BG_PIDS[@]} -gt 0 ]; then
    kill "${BG_PIDS[@]}" 2>/dev/null || true
    wait "${BG_PIDS[@]}" 2>/dev/null || true
    BG_PIDS=()
fi

echo -e "$(current_datetime) - ${GREEN}Kafka-consumer-only benchmark iteration complete${END}"
exit 0
