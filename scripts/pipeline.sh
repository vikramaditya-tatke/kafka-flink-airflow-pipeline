#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export PODMAN_COMPOSE_WARNING_LOGS=false
case "${1:-up}" in
  up)
    podman compose -f compose.yaml up -d --build
    if ! podman volume exists kafka-airflow_event_data; then
      podman volume create kafka-airflow_event_data >/dev/null
    fi
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
