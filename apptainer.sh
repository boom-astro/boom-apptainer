#!/bin/bash

# Script to manage Boom using Apptainer.
# $1 = action: build | start | stop | restart | health | benchmark | filters | mpc | backup | restore | log | error | show

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
HEALTHCHECK_DIR="$SCRIPTS_DIR/healthcheck"
LOGS_DIR="$BOOM_DIR/logs/boom"
SIF_DIR="$BOOM_DIR/apptainer/sif"
MONGO_SHUTDOWN_TIMEOUT=${MONGO_SHUTDOWN_TIMEOUT:-900}

BLUE="\e[0;34m"
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
END="\e[0m"

load_env() {
  # Load environment variables from .env file
  if [ -f "$BOOM_DIR/.env" ]; then
    set -a
    source "$BOOM_DIR/.env"
    set +a
  else
    echo -e "${RED}Error: .env file not found in $BOOM_DIR.${END}"
    exit 1
  fi
}

kill_process() {
  local process="$1"
  local name="$2"
  local match_mode="$3"  # "exact" to match the exact process, "partial" to match any process containing the string (default: "partial")

  if [ "$match_mode" == "exact" ]; then
    process="${process}$"
  fi

  if pgrep -f "$process" > /dev/null; then
    pkill -f "$process"
    echo -e "${BLUE}INFO${END}:    Stopping $name process"
  else
    echo -e "${YELLOW}WARNING${END}: $name process is not running"
  fi
}

mongod_alive() {
  local state
  state=$(ps -o stat= -p "$1" 2>/dev/null)
  [ -n "$state" ] && [ "${state:0:1}" != "Z" ]
}

stop_mongo() {
  local instance_pid mongod_pid
  instance_pid=$(apptainer instance list 2>/dev/null | awk '$1 == "mongo" {print $2}')
  if [ -z "$instance_pid" ]; then
    echo -e "${YELLOW}WARNING${END}: mongo instance is not running"
    return 0
  fi

  # mongod closes its port when shutdown starts and apptainer force-kills at 10s, so wait on the process.
  mongod_pid=$(pgrep -P "$instance_pid" mongod | head -1)
  [ -z "$mongod_pid" ] && mongod_pid="$instance_pid"

  load_env

  echo -e "${BLUE}INFO${END}:    Stopping MongoDB, flushing the cache (this can take several minutes)"
  local output
  output=$(apptainer exec instance://mongo mongosh \
    "mongodb://$BOOM_DATABASE__USERNAME:$BOOM_DATABASE__PASSWORD@localhost:27017/admin?authSource=admin" \
    --quiet --eval 'db.adminCommand({shutdown: 1})' 2>&1)

  if grep -qiE "authentication failed|not authorized|unauthorized" <<< "$output"; then
    echo -e "${RED}ERROR${END}:   MongoDB refused the shutdown command:"
    echo "$output" | tail -3
    return 1
  fi

  local waited=0
  while mongod_alive "$mongod_pid"; do
    if [ "$waited" -ge "$MONGO_SHUTDOWN_TIMEOUT" ]; then
      echo -e "${RED}ERROR${END}:   MongoDB still up after ${MONGO_SHUTDOWN_TIMEOUT}s. Leaving it running"
      echo -e "         rather than force-killing it; see $LOGS_DIR/mongodb/mongo.log."
      return 1
    fi
    sleep 5
    waited=$((waited + 5))
    [ $((waited % 60)) -eq 0 ] && echo -e "${BLUE}INFO${END}:    still flushing (${waited}s)"
  done

  apptainer instance stop mongo > /dev/null 2>&1
  echo -e "${GREEN}MongoDB shut down cleanly after ${waited}s${END}"
}

stop_service() {
    local service="$1"
    local target="$2"
    if [[ -z "$target" || "$target" = "all" || "$target" = "$service" ]]; then
        return 0
    fi
    return 1
}

colorize_log() {
    awk '
    {
        for (i = 1; i <= NF; i++) {
            if      ($i == "ERROR") $i = "\033[31m" $i "\033[0m"
            else if ($i == "WARN")  $i = "\033[33m" $i "\033[0m"
            else if ($i == "INFO")  $i = "\033[32m" $i "\033[0m"
            else if ($i == "DEBUG") $i = "\033[36m" $i "\033[0m"
            else if ($i == "TRACE") $i = "\033[35m" $i "\033[0m"
            else if ($i ~ /^[0-9]+-[0-9]+-[0-9]+T/) $i = "\033[90m" $i "\033[0m"
        }
        print
        fflush()
    }'
}

if [ "$1" != "build" ] && [ "$1" != "start" ] && [ "$1" != "stop" ] && [ "$1" != "restart" ] \
  && [ "$1" != "health" ] && [ "$1" != "benchmark" ] && [ "$1" != "filters" ] && [ "$1" != "mpc" ] \
  && [ "$1" != "backup" ] && [ "$1" != "restore" ] && [ "$1" != "log" ] && [ "$1" != "error" ] && [ "$1" != "show" ]; then
  echo "Usage: $0 {build|start|stop|restart|health|benchmark|filters|mpc|backup|restore|error|show} [args...]"
  exit 1
fi

# -----------------------------
# Build SIF files
# -----------------------------
if [ "$1" = "build" ]; then
  # See build-sif.sh for the full explanation of the argument
  ./apptainer/scripts/build-sif.sh "${@:2}"
  exit 0
fi

# -----------------------------
# Start services
# -----------------------------
if [ "$1" == "start" ]; then
  ARGS=("$BOOM_DIR")
  # Check if $2 is a survey name
  if [ -z "$2" ] || [ "$2" = "lsst" ] || [ "$2" = "ztf" ] || [ "$2" = "decam" ] || [ "$2" = "winter" ]; then
    ARGS+=("all") # service to start
  else
    [ -n "$2" ] && ARGS+=("$2") # service to start
    shift
  fi
  if [ -n "$2" ]; then
    ARGS+=("$2") # survey name
    shift
    if [[ "$2" =~ ^--(from|on)$ ]]; then
      ARGS+=("$2=$3")
      shift 2
    elif [[ "$2" =~ ^(--(from|on)=)?[0-9]{8}$ ]]; then
      ARGS+=("$2")
      shift
    else
      ARGS+=("")
      [ -z "$2" ] && shift
    fi
    ARGS+=("$2" "$3") # program ID, scheduler config path
  fi
  # See apptainer_start.sh for the full explanation of each argument
  "$SCRIPTS_DIR/apptainer_start.sh" "${ARGS[@]}"
  exit 0
fi

# -----------------------------
# Stop services
# -----------------------------
if [ "$1" == "stop" ]; then
  target="$2"
  if [ -n "$target" ] && [ "$target" != "all" ] && [[ "$target" != boom* ]] && [ "$target" != "consumer" ] && [ "$target" != "scheduler" ] \
    && [ "$target" != "api" ] && [ "$target" != "dev" ] && [ "$target" != "mongo" ] && [ "$target" != "kafka" ] && [ "$target" != "valkey" ] \
    && [ "$target" != "prometheus" ] && [ "$target" != "grafana" ] && [ "$target" != "otel" ] && [ "$target" != "tempo" ] \
    && [ "$target" != "listener" ] && [ "$target" != "kuma" ]; then
    echo -e "${RED}Error: Invalid service name '$target'.${END}"
    echo -e "Usage: ${BLUE}$0 stop [service|all|'empty']${END} ${YELLOW}('empty' will default to all)${END}"
    echo -e "  ${BLUE}[service]:${END} ${GREEN}boom_<survey> | consumer | scheduler | api | dev | mongo | kafka | valkey | prometheus | grafana | otel | tempo | listener | kuma ${END}"
    exit 1
  fi

  if stop_service "kuma" "$target"; then
    apptainer instance stop kuma
  fi
  if stop_service "listener" "$target"; then
    kill_process "boom-healthcheck-listener.py" "boom healthcheck listener"
  fi
  if stop_service "otel" "$target"; then
    kill_process "/otelcol" "Otel collector"
  fi
  if stop_service "tempo" "$target"; then
    kill_process "/tempo" "Tempo"
  fi
  if stop_service "grafana" "$target"; then
    apptainer instance stop grafana
  fi
  if stop_service "prometheus" "$target"; then
    apptainer instance stop prometheus
  fi
  if stop_service "api" "$target"; then
    apptainer instance stop api
  fi
  if stop_service "dev" "$target"; then
      apptainer instance stop dev
    fi
  if stop_service "boom" "$target"; then
    if [ "$target" = "boom" ] && [ -n "$3" ]; then
      apptainer instance stop "boom_$3"
      exit 0
    fi
    if apptainer instance list | grep -q "boom "; then
      # If a generic "boom" instance is running, stop only that one
      # and exit early to avoid stopping survey-specific instances.
      apptainer instance stop "boom"
      exit 0
    fi
    if apptainer instance list | grep -q "boom_lsst"; then
      apptainer instance stop "boom_lsst"
    fi
    if apptainer instance list | grep -q "boom_ztf"; then
      apptainer instance stop "boom_ztf"
    fi
    if apptainer instance list | grep -q "boom_decam"; then
      apptainer instance stop "boom_decam"
    fi
    if apptainer instance list | grep -q "boom_winter"; then
      apptainer instance stop "boom_winter"
    fi
  elif stop_service "consumer" "$target"; then
    match_mode="partial"
    ARGS=()
    [ -n "$3" ] && ARGS+=("$3") # survey, if not provided, all consumers are killed
    progs="$5"
    if [[ "$4" =~ ^--(from|on)$ ]]; then
      ARGS+=("$4=$5")
      progs="$6"
    elif [[ "$4" =~ ^[0-9]{8}$ ]]; then
      ARGS+=("--from=$4")
    elif [[ "$4" =~ ^--(from|on)=[0-9]{8}$ ]]; then
      ARGS+=("$4")
    else
      progs="$4"
    fi
    if [ -n "$progs" ]; then
      if [ "$progs" == "all" ]; then
        ARGS+=("--programids" "public,partnership,caltech")
      else
        match_mode="exact"
        ARGS+=("--programids" "$progs") # program ID, if not provided, all program IDs are killed
      fi
    fi
    kill_process "/app/kafka_consumer ${ARGS[*]}" consumer "$match_mode"
  elif stop_service "scheduler" "$target"; then
    survey=$3 # if no survey is provided, all schedulers are killed
    kill_process "/app/scheduler $survey" scheduler
  fi
  if stop_service "valkey" "$target"; then
    apptainer instance stop valkey
  fi
  if stop_service "kafka" "$target"; then
    apptainer instance stop kafka
  fi
  mongo_status=0
  if stop_service "mongo" "$target"; then
    stop_mongo || mongo_status=$?
  fi
  exit "$mongo_status"
fi

# -----------------------------
# Restart services
# -----------------------------
if [ "$1" == "restart" ]; then
  shift
  "$0" stop "$@" || exit $?
  "$0" start "$@"
  exit 0
fi

# -----------------------------
# Health checks
# -----------------------------
if [ "$1" == "health" ]; then
  apptainer instance list && echo
  "$HEALTHCHECK_DIR/mongodb-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/valkey-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/kafka-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/api-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/boom-healthcheck.sh"
  "$HEALTHCHECK_DIR/prometheus-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/process-healthcheck.sh" "/otelcol" otel-collector
  "$HEALTHCHECK_DIR/boom-listener-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/kuma-healthcheck.sh" 0
  exit 0
fi

# -----------------------------
# Run benchmark
# -----------------------------
if [ "$1" == "benchmark" ]; then
  shift # drop "benchmark"

  # If "init" is passed, install the required Python packages
  if [ "$2" == "init" ]; then
    pip install pandas pyyaml astropy confluent-kafka
    shift # drop "init"
  fi

  # Check if "gpu" is passed as an argument to enable GPU benchmark mode
  if [ "$1" == "gpu" ]; then
    shift # drop "gpu"
    gpu_ids=""
    if [[ "$1" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
      gpu_ids="$1"
      shift # drop gpu IDs
    fi
    echo -e "${YELLOW}GPU benchmark mode enabled. Setting BOOM_GPU__ENABLED to true.${END}"
    export BOOM_GPU__ENABLED=true
    if [ -n "$gpu_ids" ]; then
      echo -e "${YELLOW}Using GPU device IDs: $gpu_ids (count: $(($(echo "$gpu_ids" | tr -cd ',' | wc -c) + 1)))${END}"
      export BOOM_GPU__DEVICE_IDS="$gpu_ids"
    fi
  else
    echo -e "${YELLOW}GPU benchmark mode disabled. To enable, pass 'gpu' as an argument.${END}"
    export BOOM_GPU__ENABLED=false
  fi

  # Forward every remaining arg as-is to run.py
  python3 "$BOOM_DIR/tests/throughput/run.py" --apptainer "$@"
  exit 0
fi

# -----------------------------
# Add filters
# -----------------------------
if [ "$1" == "filters" ]; then
  path_to_file="$2"
  "$SCRIPTS_DIR/add_filters.sh" "$path_to_file"
  exit 0
fi

# -----------------------------
# Refresh MPC orbital elements
# -----------------------------
if [ "$1" == "mpc" ]; then
  shift
  mkdir -p "$LOGS_DIR"
  # A one-shot job, so it runs straight from the SIF instead of a boom instance.
  # It only needs Mongo and the network, so the CPU image is enough even when
  # the ZTF stack runs on the GPU one. Run it from cron ahead of the night.
  apptainer exec --pwd /app \
    --bind "$BOOM_DIR/.env:/app/.env" \
    --bind "$BOOM_DIR/config.yaml:/app/config.yaml" \
    "$SIF_DIR/boom.sif" /app/mpcorb_ingest "$@" \
    2>&1 | tee "$LOGS_DIR/mpcorb_ingest.log"
  exit "${PIPESTATUS[0]}"
fi

# -----------------------------
# Backup MongoDB
# -----------------------------
if [ "$1" == "backup" ]; then
  load_env # Load environment variables
  path_to_folder=${2:-/tmp/mongo_backups} # Folder to save the backup to
  mkdir -p "$path_to_folder"
  apptainer exec instance://mongo mongodump \
  --uri="mongodb://$BOOM_DATABASE__USERNAME:$BOOM_DATABASE__PASSWORD@localhost:27017/boom?authSource=admin" \
  --archive="$path_to_folder/mongo_$(date +%Y-%m-%d).gz" \
  --gzip
  exit 0
fi

# -----------------------------
# Restore MongoDB
# -----------------------------
if [ "$1" == "restore" ]; then
  load_env # Load environment variables
  path_to_file="$2" # Path to the backup file
  if [ -z "$path_to_file" ]; then
    echo -e "${RED}Error: Missing path to the backup file.${END}"
    echo -e "Usage: ${BLUE}$0 restore <path_to_backup_file>${END}"
    exit 1
  fi
  apptainer exec instance://mongo mongorestore \
  --uri="mongodb://$BOOM_DATABASE__USERNAME:$BOOM_DATABASE__PASSWORD@localhost:27017/boom?authSource=admin" \
  --archive="$path_to_file" \
  --gzip \
  --drop
  exit 0
fi

# -----------------------------
# Display log
# -----------------------------
if [ "$1" == "log" ]; then
  survey="${2:-lsst}"
  error_log=$3

  if [ "$survey" == "error" ]; then
    survey="lsst"
    error_log="error"
  fi

  if { [ "$survey" != "lsst" ] && [ "$survey" != "ztf" ] && [ "$survey" != "decam" ] && [ "$survey" != "winter" ]; } || { [ -n "$error_log" ] && [ "$error_log" != "error" ]; }; then
    echo -e "${RED}Error: Invalid survey name '$survey'.${END}"
    echo -e "  ${BLUE}<survey>:${END} ${GREEN}lsst | ztf | decam | winter${END} ${YELLOW}(optional, defaults to lsst)${END}"
    echo -e "  ${BLUE}<error_log>:${END} ${GREEN}error${END} ${YELLOW}(optional, if provided, will grep for ERROR|WARN in the logs)${END}"
    exit 1
  fi
  log_file="$LOGS_DIR/${survey}_scheduler.log"

  echo -e "${BLUE}Displaying $survey scheduler ${error_log:+ERROR and WARN }log...${END}"
  if [ -n "$error_log" ]; then
    grep -E "ERROR|WARN" "$log_file" | colorize_log
  else
    tail -f "$log_file" | colorize_log
  fi

  exit 0
fi

# -----------------------------
# Display log error
# -----------------------------
if [ "$1" == "error" ]; then
  for survey in lsst ztf; do
     log_file="$LOGS_DIR/${survey}_scheduler.log"
     if [ -f "$log_file" ]; then
       echo -e "${BLUE}Displaying $survey scheduler ERROR and WARN log...${END}"
       grep -E "ERROR|WARN" "$log_file" | colorize_log
     else
       echo -e "${YELLOW}WARNING${END}: Log file for $survey scheduler not found at $log_file"
     fi
  done
  exit 0
fi

# -----------------------------
# Show information from selected service
# -----------------------------
if [ "$1" == "show" ]; then
  info_to_show="$2"
  if [ -z "$info_to_show" ] || [ "$info_to_show" == "valkey" ]; then
    if ! "$HEALTHCHECK_DIR/valkey-healthcheck.sh" 0 > /dev/null 2>&1; then
      echo -e "${RED}Error: Valkey service is not running.${END}"
      exit 1
    fi
    echo -e "${BLUE}Valkey keys and their lengths:${END}"
    keys=$(apptainer exec instance://valkey valkey-cli keys "*")
    if [ -z "$keys" ]; then
      echo "  (no keys)"
    else
      echo "$keys" | while read key; do
        list_len=$(apptainer exec instance://valkey valkey-cli llen "$key")
        list_len_with_space=$(echo "$list_len" | sed ':a;s/\B[0-9]\{3\}\>/ &/;ta')
        echo "  $key: $list_len_with_space"
      done
    fi
  fi
fi