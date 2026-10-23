#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
if [[ "$(uname -s)" == Linux && -z "${DOCKER_HOST:-}" ]]; then
  runtime_dir="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
  export DOCKER_HOST="unix://${runtime_dir}/podman/podman.sock"
fi
exec astro "$@"
