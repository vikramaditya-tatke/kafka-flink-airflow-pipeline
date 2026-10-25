"""Idempotent Airflow pool setup through the public REST API."""

from container_verify import AirflowAPI


def setup():
    api = AirflowAPI()
    existing = {p["name"] for p in api.request("/pools")["pools"]}
    value = {
        "name": "airbnb_batch",
        "slots": 2,
        "description": "Bounded Airbnb analytics; leaves capacity for Kafka",
        "include_deferred": False,
    }
    if "airbnb_batch" in existing:
        api.request(
            "/pools/airbnb_batch",
            method="PATCH",
            body={
                "pool": "airbnb_batch",
                **{k: v for k, v in value.items() if k != "name"},
            },
        )
    else:
        api.request("/pools", method="POST", body=value)
    print("Airbnb task pool configured with two slots.")


if __name__ == "__main__":
    setup()
