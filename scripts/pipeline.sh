#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PODMAN_COMPOSE_WARNING_LOGS=false
case "${1:-up}" in
  up)
    podman compose -f compose.yaml up -d --build
    stack="${PIPELINE_STACK_NAME:-kafka-airflow}"
    for volume in "${stack}_parquet_data" "${stack}_astro_postgres_data" "${stack}_astro_airflow_logs"; do
      if ! podman volume exists "$volume"; then
        podman volume create "$volume" >/dev/null
      fi
    done
    ./scripts/astro.sh dev start --no-browser --no-proxy --wait 2m
    ;;
  down)
    ./scripts/astro.sh dev stop
    podman compose -f compose.yaml stop
    ;;
  status)
    podman compose -f compose.yaml ps -a
    ./scripts/astro.sh dev ps
    ;;
  logs)
    if [[ "${2:-airflow}" == airflow ]]; then
      ./scripts/astro.sh dev logs
    else
      podman compose -f compose.yaml logs --tail=100 "${@:2}"
    fi
    ;;
  verify)
    scheduler_id="$(python3 scripts/astro_container.py scheduler)"
    podman exec -i "$scheduler_id" python /usr/local/airflow/include/container_verify.py
    ;;
  *) echo "Usage: $0 {up|down|status|logs [service]|verify}" >&2; exit 2 ;;
esac
