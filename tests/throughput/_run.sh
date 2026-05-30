#!/usr/bin/env bash

# Script to benchmark BOOM throughput using Docker or Apptainer containers.
# Usage: $0 [--keep-up] [--apptainer] [logs_dir]
#   --keep-up     Leave services running after the script finishes
#   --apptainer   Use Apptainer instead of Docker
#   logs_dir      Log directory (optional; default: $BOOM_REPO_ROOT/logs/boom_benchmark)

set -euo pipefail

YELLOW="\e[33m"
GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

if [ -z "${BOOM_REPO_ROOT:-}" ]; then
    echo "Error: BOOM_REPO_ROOT is not set; set BOOM_REPO_ROOT environment variable"
    exit 1
fi

# Ports
MONGO_PORT=$BENCHMARK_MONGO_PORT
REDIS_PORT=$BENCHMARK_REDIS_PORT
KAFKA_PORT=$BENCHMARK_KAFKA_PORT

# Paths
TESTS_DIR="$BOOM_REPO_ROOT/tests"
# Kafka and MongoDB issue thousands of small fsyncs during startup, which
# becomes pathologically slow on NFS-mounted home directories (we observed
# ~1s per partition log creation on NFS, vs. milliseconds on local disk).
# Default to a local /tmp path; override with BENCHMARK_PERSISTENCE_DIR.
PERSISTENCE_DIR="${BENCHMARK_PERSISTENCE_DIR:-/tmp/boom-benchmark-${USER:-$(id -un)}/persistent}"
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"
HEALTHCHECK_DIR="$BOOM_REPO_ROOT/apptainer/scripts/healthcheck"
BENCHMARK_SIF_DIR="$TESTS_DIR/apptainer/sif"
BG_PIDS=()

# Parse args
KEEP_UP=false
APPTAINER=false
# PHASE selects which portion of the script runs.
#   full     - default; the original end-to-end behavior (start, bench, stop).
#   setup    - start services, initialize MongoDB (including the
#              ZTF_alerts_aux_snapshot used for warm resets), pre-create Kafka
#              topics, start the BOOM apptainer instance, and run the producer.
#              Leaves all instances running so that subsequent bench phases can
#              reuse them.
#   bench    - assume services are already up (from a prior setup phase).
#              Reset mutable state (drop ZTF alerts collections, restore
#              ZTF_alerts_aux from the snapshot, recreate output topics, reset
#              the consumer group offset), then start scheduler+consumer and
#              wait for completion.
#   teardown - stop all running benchmark instances and exit.
PHASE=full
POSITIONAL_ARGS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --keep-up)
            KEEP_UP=true
            shift
            ;;
        --apptainer)
            APPTAINER=true
            shift
            ;;
        --phase)
            PHASE="$2"
            shift 2
            ;;
        --phase=*)
            PHASE="${1#--phase=}"
            shift
            ;;
        --*)
            echo "Unknown option: $1"
            echo "Usage: $0 [--keep-up] [--apptainer] [--phase full|setup|bench|teardown] [logs_dir]"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done
if [ ${#POSITIONAL_ARGS[@]} -gt 1 ]; then
    echo "Usage: $0 [--keep-up] [--apptainer] [--phase full|setup|bench|teardown] [logs_dir]"
    exit 1
fi
case "$PHASE" in
    full|setup|bench|teardown) ;;
    *)
        echo "Invalid --phase value: $PHASE (must be one of: full, setup, bench, teardown)"
        exit 1
        ;;
esac
if [ "$PHASE" != "full" ] && [ "$APPTAINER" != "true" ]; then
    echo "Phases other than 'full' are only supported with --apptainer"
    exit 1
fi

PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')

COMPOSE_CONFIG=()
if [ "$APPTAINER" == "false" ]; then
  COMPOSE_CONFIG=("-f" "$TESTS_DIR/throughput/compose.yaml")

  # Select the cutout storage overlay based on BOOM_CUTOUTS_STORAGE__TYPE (default: mongo)
  CUTOUTS_TYPE="${BOOM_CUTOUTS_STORAGE__TYPE:-mongo}"
  if [ "$CUTOUTS_TYPE" = "s3" ]; then
      COMPOSE_CONFIG+=("-f" "$BOOM_REPO_ROOT/tests/throughput/compose.cutouts-s3.yaml")
  else
      COMPOSE_CONFIG+=("-f" "$BOOM_REPO_ROOT/tests/throughput/compose.cutouts-mongo.yaml")
  fi

  if [ "${BOOM_GPU__ENABLED:-false}" = "true" ] && [ "$PLATFORM" = "linux" ]; then
      echo "BOOM_GPU__ENABLED is true and platform is Linux; adding GPU override to Docker Compose configuration (CUDA support)"
      COMPOSE_CONFIG+=("-f" "$TESTS_DIR/throughput/compose.cuda.yaml")
  fi

  # If LOW_STORAGE mode is enabled, use the override to prevent volume mounts
  if [ "${LOW_STORAGE:-}" = "true" ]; then
      COMPOSE_CONFIG+=("-f" "$TESTS_DIR/throughput/compose.low-storage.yaml")
  fi
fi

# Logs folder is the optional positional argument to the script
LOGS_DIR="${POSITIONAL_ARGS[0]:-$BOOM_REPO_ROOT/logs/boom_benchmark}"

cleanup() {
    trap '' INT TERM # Ignore further signals during cleanup
    echo "$(current_datetime) - Cleaning up background processes"
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
    fi
    # In setup/bench phases the warm-sweep orchestrator owns the service
    # lifecycle, so we deliberately leave instances running even on error: the
    # operator can re-run the failing phase against the live services, or
    # invoke `--phase teardown` explicitly to release them.
    case "$PHASE" in
        setup|bench) ;;
        *) stop_all_instances ;;
    esac
}

trap cleanup EXIT INT TERM

STOP_ALL_INSTANCES_DONE=false
stop_all_instances() {
  # Idempotent: explicit error/success paths call this directly, and the EXIT
  # trap also calls it via cleanup() — guard so the shutdown runs only once.
  if [ "$STOP_ALL_INSTANCES_DONE" = true ]; then
      return
  fi
  STOP_ALL_INSTANCES_DONE=true
  # In setup/bench phases the warm-sweep orchestrator owns the service
  # lifecycle: never tear down here, even on error. Teardown is requested
  # explicitly via --phase teardown.
  case "$PHASE" in
      setup|bench)
          echo -e "$(current_datetime) - ${YELLOW}Phase $PHASE: leaving BOOM services running (teardown is the orchestrator's job)${END}"
          return
          ;;
  esac
  if [ "$KEEP_UP" = true ]; then
      echo -e "$(current_datetime) - ${YELLOW}--keep-up flag is set; leaving BOOM services running${END}"
      return
  fi
  echo -e "$(current_datetime) - ${GREEN}Shutting down BOOM services${END}"
  if [ "$APPTAINER" == "true" ]; then
    apptainer instance stop benchmark_boom || true
    apptainer instance stop benchmark_kafka || true
    apptainer instance stop benchmark_valkey || true
    apptainer instance stop benchmark_mongo || true
    rm -rf "$PERSISTENCE_DIR/kafka"
  else
    docker compose "${COMPOSE_CONFIG[@]}" down
  fi
}

run_mongo_query(){
    local query="$1"
    local as_admin="${2:-false}"
    local auth=""
    if [ "$as_admin" == "true" ]; then
        auth="/admin?authSource=admin"
    fi
    if [ "$APPTAINER" == "true" ]; then
      apptainer exec instance://benchmark_mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:${MONGO_PORT}${auth}" --quiet --eval "$query"
    else
      docker compose "${COMPOSE_CONFIG[@]}" exec -T mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017${auth}" --quiet --eval "$query"
    fi
}

mongo_count() {
    local query="$1"
    local raw
    raw=$(run_mongo_query "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

# Periodically sample CPU/RAM for a PID and write JSON lines (apptainer equivalent of docker stats).
# Runs in background; exits when the target PID disappears.
collect_stats() {
    local pid="$1"
    local service="$2"
    local logfile="$3"
    : > "$logfile"
    while kill -0 "$pid" 2>/dev/null; do
        local sample
        sample=$(ps -p "$pid" -o pcpu=,pmem=,rss=,vsz=,nlwp= 2>/dev/null | awk '{$1=$1}1')
        if [ -n "$sample" ]; then
            local pcpu pmem rss_kb vsz_kb threads
            read -r pcpu pmem rss_kb vsz_kb threads <<<"$sample"
            printf '{"Timestamp":"%s","Service":"%s","PID":%s,"CPUPerc":"%s%%","MemPerc":"%s%%","MemRssKiB":%s,"MemVszKiB":%s,"Threads":%s}\n' \
                "$(date -u +%FT%TZ)" "$service" "$pid" "$pcpu" "$pmem" "$rss_kb" "$vsz_kb" "$threads" \
                >> "$logfile"
        fi
        sleep 2
    done
}

# Kafka topics used by the throughput benchmark.
#   - ZTF_SOURCE_TOPIC: where the producer writes the input alerts. Preserved
#     across warm-sweep iterations so the producer only runs once.
#   - ZTF_OUTPUT_TOPICS: topics that BOOM publishes into. Warm resets
#     delete+recreate them so each iteration starts at offset 0 and the verify
#     consumer reads exactly the messages produced by that iteration.
ZTF_SOURCE_TOPIC="ztf_20250311_programid1"
ZTF_OUTPUT_TOPICS=(
    "babamul.ztf.lsst-match.stellar"
    "babamul.ztf.lsst-match.hosted"
    "babamul.ztf.lsst-match.hostless"
    "babamul.ztf.no-lsst-match.stellar"
    "babamul.ztf.no-lsst-match.hosted"
    "babamul.ztf.no-lsst-match.hostless"
    "ZTF_alerts_results"
)
ZTF_TOPICS=("${ZTF_OUTPUT_TOPICS[@]}" "$ZTF_SOURCE_TOPIC")

kafka_topics_sh() {
    apptainer exec instance://benchmark_kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server localhost:"$KAFKA_PORT" "$@"
}

# Number of partitions that ZTF_OUTPUT_TOPICS are created with. Used to build
# the delete-records JSON spec.
ZTF_OUTPUT_PARTITIONS=15

# reset_mutable_state brings the cluster back to a known pre-bench state without
# repeating the heavy parts of setup (mongorestore, producer, NED import). It
# is only called in the bench phase, which assumes services are already up.
#
# Kafka output topics: instead of delete+recreate (which is async and costs
# 35-40s per iteration waiting for the broker to finish the deletion), we use
# kafka-delete-records --offset=-1 to advance each partition's log-start-offset
# to its current high-water-mark. That makes prior records inaccessible
# without touching topic metadata, so the call is near-instant.
#
# This also implicitly fixes the original verify-consumer offset-leak bug:
# even if a previous iteration committed a partial offset (e.g. exited early
# on the 1s silence break), the LSO advance pushes the consumer's effective
# position past the leftover records, so the next iteration's verify reads
# exactly this iteration's output.
reset_mutable_state() {
    echo "$(current_datetime) - Resetting mutable state for warm bench iteration"

    # MongoDB: drop the collections that BOOM populates during the bench, and
    # restore ZTF_alerts_aux from its pre-built in-server snapshot (created by
    # apptainer_mongo-init.sh). The aggregate $out copy runs entirely
    # server-side, which is ~5-10x faster than re-running mongorestore on the
    # gzipped archive.
    run_mongo_query "
        const target = db.getSiblingDB('boom-benchmarking');
        target.ZTF_alerts.drop();
        target.ZTF_alerts_cutouts.drop();
        target.ZTF_alerts_aux.drop();
        target.ZTF_alerts_aux_snapshot.aggregate([{ \$out: 'ZTF_alerts_aux' }]);
        target.ZTF_alerts_aux.createIndex({ 'coordinates.radec_geojson': '2dsphere' });
    " "true" > /dev/null

    # Kafka: build the delete-records JSON spec (every partition of every
    # output topic, offset -1 = high-water-mark) and pass it to
    # kafka-delete-records.sh inside the broker container.
    #
    # We write the JSON to host /tmp (no --bind) because apptainer auto-mounts
    # the host's /tmp into the container at the same path, so the broker can
    # read it directly. Trying to --bind something at /tmp/X is silently
    # shadowed by that auto-mount.
    local json_path
    json_path=$(mktemp /tmp/boom-delete-records.XXXXXX.json)
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

# Teardown phase short-circuits: stop everything and exit before any of the
# startup / bench logic runs.
if [ "$PHASE" = "teardown" ]; then
    echo "$(current_datetime) - Phase teardown: stopping all benchmark services"
    PHASE=full  # let stop_all_instances actually run instead of being a no-op
    stop_all_instances
    exit 0
fi

if [ "$APPTAINER" == "true" ]; then

  # The startup block below (MongoDB through producer) only runs in phases
  # that need to provision the cluster. The bench phase skips it entirely
  # under the assumption that a prior setup phase left the services running.
  if [ "$PHASE" = "full" ] || [ "$PHASE" = "setup" ]; then

  # -----------------------------
  # Start MongoDB
  # -----------------------------
  echo && echo "$(current_datetime) - Starting MongoDB"
  mkdir -p "$LOGS_DIR/mongodb" "$PERSISTENCE_DIR/mongodb"
  apptainer instance run \
    --bind "$PERSISTENCE_DIR/mongodb:/data/db" \
    --bind "$LOGS_DIR/mongodb:/log" \
    "$BENCHMARK_SIF_DIR/mongo.sif" benchmark_mongo
  sleep 5
  "$HEALTHCHECK_DIR/mongodb-healthcheck.sh" --port "$MONGO_PORT" --instance benchmark_mongo

  echo "$(current_datetime) - Initializing MongoDB with test data"
  apptainer exec \
      --bind "$BOOM_REPO_ROOT/data/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
      --bind "$BOOM_REPO_ROOT/data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz:/boom_throughput.ZTF_alerts_aux.dump.gz" \
      --bind "$TESTS_DIR/throughput/apptainer_mongo-init.sh:/mongo-init.sh" \
      --bind "$TESTS_DIR/throughput/cats150.filter.json:/cats150.filter.json" \
      --env DB_NAME=boom-benchmarking \
      --env DB_ADD_URI= \
      "$BENCHMARK_SIF_DIR/mongo.sif" \
      /bin/bash /mongo-init.sh

  # -----------------------------
  # Start Valkey
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Valkey"
  mkdir -p "$LOGS_DIR/valkey"
  apptainer instance run --bind "$LOGS_DIR/valkey:/log" "$BENCHMARK_SIF_DIR/valkey.sif" benchmark_valkey
  "$HEALTHCHECK_DIR/valkey-healthcheck.sh" --port "$REDIS_PORT" --instance benchmark_valkey

  # -----------------------------
  # Start Kafka
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Kafka"
  # Defensive: a previous run killed before cleanup may have left Kafka data
  # behind. Loading stale partitions on startup races with the harness's
  # pre-creation step.
  rm -rf "$PERSISTENCE_DIR/kafka"
  mkdir -p "$LOGS_DIR/kafka" "$PERSISTENCE_DIR/kafka"
  apptainer instance run \
      --bind "$PERSISTENCE_DIR/kafka:/var/lib/kafka/data" \
      --bind "$PERSISTENCE_DIR/kafka:/opt/kafka/config" \
      --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
      "$BENCHMARK_SIF_DIR/kafka.sif" benchmark_kafka
  "$HEALTHCHECK_DIR/kafka-healthcheck.sh" --port "$KAFKA_PORT" --instance benchmark_kafka

  # -----------------------------
  # Pre-create  topics so the scheduler does not pay the topic-creation
  # warmup cost when the first enriched alerts arrive. ZTF_TOPICS is defined
  # at the top of the script so that reset_mutable_state can reuse it.
  # -----------------------------
  echo && echo "$(current_datetime) - Pre-creating Kafka topics"
  for topic in "${ZTF_TOPICS[@]}"; do
    kafka_topics_sh --create --topic "$topic" \
      --partitions 15 --replication-factor 1 --if-not-exists > /dev/null
  done
  echo "$(current_datetime) - Kafka topics created: ${ZTF_TOPICS[*]}"

  # -----------------------------
  # Start Boom
  # -----------------------------
  echo && echo "$(current_datetime) - Starting BOOM instance"
  BOOM_SIF="boom.sif"
  NV_FLAG=""
  if [ "${BOOM_GPU__ENABLED:-false}" = "true" ]; then
    echo -e "${YELLOW}$(current_datetime) - BOOM_GPU__ENABLED is true; using BOOM GPU image with --nv flag for GPU support${END}"
    BOOM_SIF="boom-gpu.sif"
    NV_FLAG="--nv"
  fi
  apptainer instance start $NV_FLAG \
    --bind "$CONFIG_FILE:/app/config.yaml" \
    --bind "$BOOM_REPO_ROOT/data/alerts:/app/data/alerts" \
    "$TESTS_DIR/apptainer/sif/$BOOM_SIF" benchmark_boom
  sleep 3

  # -----------------------------
  # Start Producer
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Producer"
  apptainer exec --pwd /app \
    instance://benchmark_boom /app/kafka_producer ztf 20250311 public --server-url localhost:"$KAFKA_PORT" \
    > "$LOGS_DIR/producer.log" 2>&1
  echo -e "${GREEN}$(current_datetime) - Producer finished sending alerts${END}"

  fi  # end of "PHASE in (full, setup)" startup block

  if [ "$PHASE" = "setup" ]; then
      echo -e "$(current_datetime) - ${GREEN}Setup phase complete; services left running for warm bench iterations${END}"
      # The EXIT trap's cleanup() is phase-aware and will not stop instances
      # in setup phase, so exiting here leaves the cluster warm.
      exit 0
  fi

  if [ "$PHASE" = "bench" ]; then
      # Bring the cluster back to a known pre-bench state. Then truncate the
      # scheduler / consumer logs so that the GPU-warmup and
      # ingestion/classification/filter checks below operate on a fresh log
      # for this iteration (the previous iteration may have written the same
      # "all GPU model sets loaded successfully" / "passed filter" lines).
      reset_mutable_state
      mkdir -p "$LOGS_DIR"
      : > "$LOGS_DIR/scheduler.log"
      : > "$LOGS_DIR/consumer.log"
  fi

  # -----------------------------
  # Start Scheduler — before the Consumer so that the (potentially slow) GPU
  # warmup happens while Redis is still empty. We then wait for GPU readiness
  # and only after that bring up the Consumer, so the first Kafka message
  # arrives into an already-warm pipeline. This keeps `t1_b` (= "Consumer
  # received first message" timestamp in run.py) from including warmup time.
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Scheduler"
  apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
  SCHEDULER_PID=$!
  BG_PIDS+=($SCHEDULER_PID)
  collect_stats "$SCHEDULER_PID" scheduler "$LOGS_DIR/scheduler.stats.log" &
  BG_PIDS+=($!)
  echo -e "${GREEN}Boom scheduler started for survey ztf${END}"

  # -----------------------------
  # Wait for GPU readiness (apptainer + GPU only) before the Consumer starts.
  # -----------------------------
  if [ "${BOOM_GPU__ENABLED:-false}" = "true" ] && [ "$PLATFORM" = "linux" ]; then
      echo "$(current_datetime) - GPU support is enabled; waiting for GPUs to be inference-ready before starting Consumer"
      WARMUP_START=$(date +%s)
      while ! grep -q "all GPU model sets loaded successfully" "$LOGS_DIR/scheduler.log" 2>/dev/null; do
          ELAPSED=$(( $(date +%s) - WARMUP_START ))
          if [ $ELAPSED -ge ${TIMEOUT_SECS:-300} ]; then
              echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for GPU inference to be validated${END}"
              stop_all_instances
              exit 1
          fi
          sleep 1
      done
      WARMUP_TIME=$(( $(date +%s) - WARMUP_START ))
      echo "$(current_datetime) - ONNX CUDA warmup completed in $WARMUP_TIME seconds"
  fi

  sleep 1

  # -----------------------------
  # Start Consumers
  # -----------------------------
  N_KAFKA_CONSUMERS="${N_KAFKA_CONSUMERS:-1}"
  echo && echo "$(current_datetime) - Starting $N_KAFKA_CONSUMERS Consumer(s)"
  for i in $(seq 1 "$N_KAFKA_CONSUMERS"); do
    consumer_log="$LOGS_DIR/consumer.log"
    if [ "$N_KAFKA_CONSUMERS" -gt 1 ] && [ "$i" -gt 1 ]; then
        consumer_log="$LOGS_DIR/consumer-$i.log"
    fi
    apptainer exec --pwd /app \
      instance://benchmark_boom /app/kafka_consumer ztf 20250311 --programids public \
      > "$consumer_log" 2>&1 &
    pid=$!
    BG_PIDS+=($pid)
    # Only sample stats from the first consumer; that's enough to characterize
    # behavior and avoids spamming /proc with $N_KAFKA_CONSUMERS samplers.
    if [ "$i" -eq 1 ]; then
        CONSUMER_PID=$pid
        collect_stats "$CONSUMER_PID" consumer "$LOGS_DIR/consumer.stats.log" &
        BG_PIDS+=($!)
    fi
  done
  echo -e "${GREEN}$N_KAFKA_CONSUMERS Boom consumer(s) started for survey ztf${END}"

else
  # Remove any existing containers
  docker compose "${COMPOSE_CONFIG[@]}" down

  # Spin up BOOM services with Docker Compose
  if ! docker compose "${COMPOSE_CONFIG[@]}" up --build -d; then
      echo "$(current_datetime) - ERROR: Failed to start Docker Compose services"
      docker compose "${COMPOSE_CONFIG[@]}" logs mongo-init || true
      exit 1
  fi

  # Send the logs to file so we can analyze later
  mkdir -p "$LOGS_DIR"
  docker compose "${COMPOSE_CONFIG[@]}" logs -f producer > "$LOGS_DIR/producer.log" &
  BG_PIDS+=($!)
  docker compose "${COMPOSE_CONFIG[@]}" logs -f consumer > "$LOGS_DIR/consumer.log" &
  BG_PIDS+=($!)
  docker compose "${COMPOSE_CONFIG[@]}" logs -f scheduler > "$LOGS_DIR/scheduler.log" &
  BG_PIDS+=($!)
  docker compose "${COMPOSE_CONFIG[@]}" logs -f mongo-init > "$LOGS_DIR/mongo-init.log" &
  BG_PIDS+=($!)
  # Also log stats from containers for later analysis
  docker compose "${COMPOSE_CONFIG[@]}" stats consumer --format json > "$LOGS_DIR/consumer.stats.log" &
  BG_PIDS+=($!)
  docker compose "${COMPOSE_CONFIG[@]}" stats scheduler --format json > "$LOGS_DIR/scheduler.stats.log" &
  BG_PIDS+=($!)
  if [ "$CUTOUTS_TYPE" = "s3" ]; then
      docker compose "${COMPOSE_CONFIG[@]}" stats valkey-cutouts --format json > "$LOGS_DIR/valkey-cutouts.stats.log" &
      BG_PIDS+=($!)
  fi
fi

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=${TIMEOUT_SECS:-300} # 5 minutes default

# -----------------------------
# Wait for the kafka consumer to start expecting messages (when it logs "Consumer received first message, continuing...")
# -----------------------------
echo && echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while true; do
    if [ "$APPTAINER" == "true" ]; then
        grep -q "Consumer received first message, continuing..." "$LOGS_DIR/consumer.log" 2>/dev/null && break
    else
        docker compose "${COMPOSE_CONFIG[@]}" logs consumer | grep -q "Consumer received first message, continuing..." && break
        MONGO_INIT_CONTAINER_ID=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq mongo-init | tail -n 1)
        if [ -n "$MONGO_INIT_CONTAINER_ID" ]; then
            MONGO_INIT_EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "$MONGO_INIT_CONTAINER_ID" 2>/dev/null || true)
            if [[ "$MONGO_INIT_EXIT_CODE" =~ ^[0-9]+$ ]] && [ "$MONGO_INIT_EXIT_CODE" -ne 0 ]; then
                echo "$(current_datetime) - ERROR: mongo-init did not complete successfully (exit $MONGO_INIT_EXIT_CODE)"
                stop_all_instances
                exit 1
            fi
        fi
    fi

    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED} Timeout reached while waiting for Kafka consumer to start${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
STARTUP_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - Kafka consumer started in $STARTUP_TIME seconds"

# If we are in LOW_STORAGE mode, clean up the downloaded files (producer files are not mounted)
if [ "${LOW_STORAGE:-}" = "true" ]; then
    echo "$(current_datetime) - LOW_STORAGE mode enabled; cleaning up downloaded files to save space"
    rm -rf ./data/alerts/kowalski.NED.json.gz || true
    rm -rf ./data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz || true
fi

# -----------------------------
# Wait for alerts ingestion
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be ingested${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
INGESTION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts ingested in $INGESTION_TIME seconds"

# -----------------------------
# Wait for alerts classification
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be classified"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be classified${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
CLASSIFICATION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts classified in $CLASSIFICATION_TIME seconds"

# -----------------------------
# Wait for all filters to run on all alerts
# -----------------------------
echo "$(current_datetime) - Waiting for filters to run on all alerts"
START_TIME=$(date +%s)
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    if [ "$APPTAINER" == "true" ]; then
      PASSED_ALERTS=$(cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}' || true)
    else
      PASSED_ALERTS=$(docker compose "${COMPOSE_CONFIG[@]}" logs scheduler | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}' || true)
    fi
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for filters to run on all alerts"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
FILTERING_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts filtered in $FILTERING_TIME seconds"

echo "$(current_datetime) - All alerts ingested, classified, and filtered"
echo "$(current_datetime) - Reading from Kafka output topic"
if [ "$APPTAINER" == "true" ]; then
  python "$TESTS_DIR/throughput/read-kafka-output.py" --server "localhost:$KAFKA_PORT"
else
  python "$TESTS_DIR/throughput/read-kafka-output.py"
fi
# TODO: check for uv implementation
# uv run "$TESTS_DIR/throughput/read-kafka-output.py"

# -----------------------------
# Export MongoDB collection stats to JSON for analysis
# -----------------------------
echo "$(current_datetime) - Collecting MongoDB collection stats"

MONGO_RESULT="$({ run_mongo_query '
const dbName = "boom-benchmarking";
const d = db.getSiblingDB(dbName);
function collectionStats(name) {
	const c = d.getCollection(name);
	const s = c.stats();
  return {
	collection: name,
	count: c.countDocuments(),
	data_size_bytes: s.size,
	storage_size_bytes: s.storageSize,
	total_index_size_bytes: s.totalIndexSize,
	total_size_bytes: s.totalSize
  };
}
const collectionNames = d
	.getCollectionInfos({ type: "collection" })
	.map((info) => info.name)
	.sort();
const out = {
  generated_at_utc: new Date().toISOString(),
  database: dbName,
  collections: collectionNames.map(collectionStats)
};
print(JSON.stringify(out));
' "true"; } | tail -n 1)"

if [ -n "$MONGO_RESULT" ]; then
	mkdir -p "$LOGS_DIR"
	if command -v jq >/dev/null 2>&1; then
		printf '%s\n' "$MONGO_RESULT" | jq . > "$LOGS_DIR/collection_stats.json"
	else
		printf '%s\n' "$MONGO_RESULT" > "$LOGS_DIR/collection_stats.json"
	fi
	echo "$(current_datetime) - Wrote collection stats to $LOGS_DIR/collection_stats.json"
fi

# -----------------------------
# Stop all instances
# -----------------------------
if [ "$APPTAINER" == "false" ]; then
  # Check to see if any of our containers have exited with a non-zero status, which would indicate an error
  EXIT_CODE=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq | xargs docker inspect -f '{{.State.ExitCode}}' | grep -v '^0$' || true)
  if [ -n "$EXIT_CODE" ]; then
      echo "$(current_datetime) - ERROR: One or more containers exited with a non-zero status"
      stop_all_instances
      exit 1
  fi
fi

echo -e "$(current_datetime) - ${GREEN}All tasks completed${END}"

if [ "$PHASE" = "bench" ]; then
    # Bench phase keeps mongo/valkey/kafka alive for the next iteration, but the
    # scheduler/consumer started in this iteration must die before we return —
    # otherwise the next iteration's apptainer exec would race against this
    # one. The EXIT trap also runs cleanup(), which is a no-op against an
    # already-drained BG_PIDS list.
    echo "$(current_datetime) - Bench iteration complete; stopping scheduler/consumer and leaving services up"
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
        BG_PIDS=()
    fi
    exit 0
fi

echo && stop_all_instances

exit 0
