"""Find this checkout's Astro container without relying on generated names."""

import json
import subprocess
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
service = sys.argv[1] if len(sys.argv) > 1 else "scheduler"
ids = subprocess.check_output(
    [
        "podman",
        "ps",
        "-q",
        "--filter",
        f"label=com.docker.compose.service={service}",
        "--filter",
        "label=io.astronomer.docker.cli=true",
    ],
    text=True,
).split()
if ids:
    containers = json.loads(
        subprocess.check_output(["podman", "inspect", *ids], text=True)
    )
    for container in containers:
        if any(
            Path(mount["Source"]) == root / "dags"
            for mount in container.get("Mounts", [])
        ):
            print(container["Id"])
            sys.exit(0)
raise SystemExit(f"No running Astro {service} for {root}; run ./scripts/pipeline.sh up")
