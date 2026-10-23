# Astro migration verified on 23 October 2026

The complete local pipeline is running with **Astro CLI 1.46.1 on Podman 5.8.6**.
The root Dockerfile uses Runtime `3.3-10-python-3.12`, which runs
**Airflow 3.3.2+astro.2** and Python 3.12.15. The upstream services remain Java 17,
Flink 1.20.3 / Kafka connector 3.3.0-1.20, and Apache Kafka 3.9.1.
Astro manages the local Airflow components and their separate PostgreSQL database.

## Verified results

- All 17 consumer tests passed inside the actual Astro Runtime with
  `./scripts/astro.sh dev pytest`; they also passed in the isolated host test run.
- Ruff, shell/Python syntax checks and the Airflow 3 migration rules passed.
- The Runtime dependency check reported that all 191 installed packages are
  compatible.
- The Airflow API reported zero DAG import errors and healthy metadata database,
  scheduler, triggerer and DAG processor. The DAG is unpaused and runs every minute.
- The migrated verifier uses the public Airflow 3 REST API to trigger an actual
  scheduler run, inspect its task state and read the task's XCom result.
- Initial Astro integration run: 30 fresh Java-generated events traversed
  TCP → Flink → Kafka → Airflow → persistent SQLite. The committed next offset
  was `windows-events:0 = 10050`.
- After exercising the completed launcher and recreating Astro services, the
  final integration run consumed another 30 fresh events and committed next
  offset `windows-events:0 = 12837`.
- Both runs verified payload preservation and broker offset commits. Independent
  replay groups inserted zero duplicate rows. SQLite integrity checks passed.
- The final report contained 12,516 stored Kafka records; the scheduled pipeline
  continues to add events while it remains running.
- The documented `pipeline.sh up`, `status`, `verify`, Astro `dev run version`,
  and Astro `dev pytest` commands were exercised.

## Persistent data and migration

The existing `kafka-airflow_event_data` volume was reused, with the same numeric
container UID. Samples stored under Airflow 2 and both Astro verification runs
were checked in the output database after migration.

The old Airflow 2 metadata was backed up to ignored
`artifacts/airflow2-metadata.sql`. Its stopped service containers were removed;
the old named metadata/log volumes were retained. Astro uses a separate metadata
database, so old Airflow 2 run history is preserved in the backup rather than
converted into Airflow 3 history.

The native Linux Podman API is handled by the existing enabled systemd socket.
The Astro wrapper sets `DOCKER_HOST` to that socket, avoiding VM setup. An unused
VM created during the initial CLI check was removed before final setup.

Machine-readable local reports under ignored `artifacts/` include:

- `verification-astro-initial.json`
- `verification-astro-final.json`
- `verification-astro-summary.json`

The latest report also remains at `/usr/local/airflow/data/verification.json` in
the event volume. Run `./scripts/pipeline.sh verify` to generate fresh evidence.

## Earlier baseline and boundaries

Before the Astro migration, the Airflow 2 setup passed end-to-end checks before
and after generator/broker restarts (next offsets 450 and 973). Those reports
remain in `artifacts/verification-initial.json` and
`artifacts/verification-recovery.json` as historical evidence.

No loss-free replay is claimed for the synthetic TCP segment before Kafka. The
broker is a single development broker. Consumer replay safety was verified for
records already in Kafka.

The selected environment is local. AWS access was checked read-only earlier;
no AWS resource was created or changed, and no cloud account is needed to run
this pipeline.

Version references: [Astro Runtime release notes](https://www.astronomer.io/docs/runtime/runtime-release-notes/),
[Airflow 3 migration guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/upgrading_to_airflow3.html).
