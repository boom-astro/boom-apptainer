#!/bin/bash

# Script to build SIF files using Apptainer.
# $1 = service to build (optional):
#     - "all" (default): builds all services
#     - "dev"          : builds the BOOM dev image (no source baked in; bind-mounted at runtime for hot reload)
#     - "benchmark"    : builds only benchmark services (MongoDB, Kafka, Valkey, and BOOM)
#     - "mongo"        : builds MongoDB service
#     - "valkey"       : builds Valkey service
#     - "kafka"        : builds Kafka service
#     - "boom"         : builds BOOM service (CPU or GPU variant depending on BOOM_GPU__ENABLED)
#     - "boom-cpu"     : builds BOOM CPU variant explicitly
#     - "boom-gpu"     : builds BOOM GPU variant explicitly
#     - "prometheus"   : builds Prometheus service
#     - "otel"         : builds OpenTelemetry Collector service
#     - "kuma"         : builds Uptime Kuma service

YELLOW="\e[33m"
END="\e[0m"

mkdir -p apptainer/sif

# Load environment variables from .env file (for BOOM_GPU__ENABLED)
if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

start_service() {
    local service="$1"
    local target="$2"
    # Return 0 (true) if target is empty, "all" or matches the service name
    [[ -z "$target" || "$target" = "all" || "$target" = "$service" ]]
}

# -----------------------------
# Build SIF files for the benchmark
# -----------------------------
if [ "$1" == "benchmark" ]; then
  # Build BOOM
  BOOM="boom" # default BOOM variant
  if [ "${BOOM_GPU__ENABLED:-false}" == "true" ]; then
    echo -e "${YELLOW}$(current_datetime) - BOOM_GPU__ENABLED is true, building BOOM GPU image${END}"
    BOOM="boom-gpu"
  fi
  apptainer build --force "apptainer/sif/${BOOM}.sif" "apptainer/def/${BOOM}.def"

  # Build other benchmark services (excluding monitoring services)
  mkdir -p "tests/apptainer/sif"
  for service in mongo kafka valkey; do
    apptainer build --force "tests/apptainer/sif/$service.sif" "tests/apptainer/def/$service.def"
  done

  exit 0
fi

# -----------------------------
# Build SIF file for BOOM dev image
# -----------------------------
if [ "$1" = "dev" ]; then
  apptainer build --force apptainer/sif/dev.sif apptainer/def/dev.def
  exit 0
fi

# -----------------------------
# Build SIF files for BOOM services
# -----------------------------
if start_service "boom" "$1" || [ "$1" = "boom-gpu" ] || [ "$1" = "boom-cpu" ]; then
  BOOM="boom" # default BOOM variant
  if [[ "$1" == "boom-gpu" ]] || { start_service "boom" "$1" && [[ "${BOOM_GPU__ENABLED:-false}" == "true" ]]; }; then
    echo -e "${YELLOW}$(current_datetime) - Building BOOM GPU image${END}"
    BOOM="boom-gpu"
  fi
  apptainer build --force apptainer/sif/"$BOOM".sif apptainer/def/"$BOOM".def
fi

if start_service "otel" "$1"; then
  apptainer build --force apptainer/sif/otel.sif "docker://otel/opentelemetry-collector:0.131.1"
fi

for service in mongo kafka valkey api prometheus kuma; do
  if start_service "$service" "$1"; then
    apptainer build --force apptainer/sif/"$service".sif apptainer/def/"$service".def
  fi
done