#!/bin/bash

# Grafana health check
# $1 = NB_RETRIES max retries (empty = unlimited)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}
GRAFANA_URL="http://localhost:3000/api/health"

cpt=0
until curl -sf "$GRAFANA_URL" > /dev/null; do
  echo -e "${RED}$(current_datetime) - grafana unhealthy${END}"
  if [ -n "$NB_RETRIES" ] && [ $cpt -ge "$NB_RETRIES" ]; then
    exit 1
  fi

  ((cpt++))
  sleep 3
done

echo -e "${GREEN}$(current_datetime) - grafana is healthy${END}"
exit 0
