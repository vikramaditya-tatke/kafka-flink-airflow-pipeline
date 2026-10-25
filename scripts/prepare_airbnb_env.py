"""Prepare ignored local secrets for Compose, without logging credentials.

This is infrastructure setup, not a data execution interface. AWS remains
optional for the Kafka workload. Refresh SSO before startup when using a profile.
"""

import json
import os
import secrets
import subprocess
from pathlib import Path


def prepare():
    root = Path(__file__).resolve().parents[1]
    dotenv = root / ".env"
    text = dotenv.read_text() if dotenv.exists() else ""
    values = {}
    for line in text.splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            name, value = line.split("=", 1)
            values[name.strip()] = value.strip().strip("'\"")
    if not values.get("AIRBNB_CLICKHOUSE_PASSWORD") and not os.getenv(
        "AIRBNB_CLICKHOUSE_PASSWORD"
    ):
        values["AIRBNB_CLICKHOUSE_PASSWORD"] = secrets.token_hex(24)
        text += (
            "\nAIRBNB_CLICKHOUSE_PASSWORD="
            + values["AIRBNB_CLICKHOUSE_PASSWORD"]
            + "\n"
        )
        dotenv.touch(mode=0o600, exist_ok=True)
        dotenv.write_text(text)
        dotenv.chmod(0o600)
    connection = os.getenv("AIRFLOW_CONN_AWS_AIRBNB")
    profile = os.getenv("AIRBNB_AWS_PROFILE", values.get("AIRBNB_AWS_PROFILE"))
    if not connection and profile:
        result = subprocess.run(
            [
                "aws",
                "configure",
                "export-credentials",
                "--profile",
                profile,
                "--format",
                "process",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode:
            raise RuntimeError(
                f"AWS profile {profile} is unavailable. Refresh its SSO session before startup."
            )
        credentials = json.loads(result.stdout)
        connection = json.dumps(
            {
                "conn_type": "aws",
                "login": credentials["AccessKeyId"],
                "password": credentials["SecretAccessKey"],
                "extra": {
                    "aws_session_token": credentials.get("SessionToken"),
                    "region_name": "eu-west-2",
                },
            }
        )
    connection = connection or json.dumps(
        {"conn_type": "aws", "extra": {"region_name": "eu-west-2"}}
    )
    if "\n" in connection or "\r" in connection:
        raise ValueError("Airflow AWS connection must be single-line JSON")
    json.loads(connection)
    directory = root / ".aws"
    directory.mkdir(mode=0o700, exist_ok=True)
    path = directory / "airflow.env"
    path.touch(mode=0o600, exist_ok=True)
    clickhouse = json.dumps(
        {
            "conn_type": "generic",
            "host": "airbnb-clickhouse",
            "port": 8123,
            "schema": "airbnb_analytics",
            "login": "airbnb",
            "password": os.getenv(
                "AIRBNB_CLICKHOUSE_PASSWORD", values.get("AIRBNB_CLICKHOUSE_PASSWORD")
            ),
        }
    )
    path.write_text(
        "AIRFLOW_CONN_AWS_AIRBNB="
        + connection
        + "\nAIRFLOW_CONN_CLICKHOUSE_AIRBNB="
        + clickhouse
        + "\n"
    )
    path.chmod(0o600)
    print("Local Airflow/ClickHouse environment prepared; no credentials printed.")


if __name__ == "__main__":
    prepare()
