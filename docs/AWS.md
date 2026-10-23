# AWS status

The selected deployment is local: Astro CLI manages Airflow on Podman alongside
local Kafka, Flink and the Java generator. No cloud deployment is configured.

The `aws-personal-prod` SSO profile was successfully checked in `eu-west-2`.
Read-only discovery found no MWAA environments or MSK clusters. No AWS resources
were created or modified. No AWS authentication is needed to run this project.

The consumer's optional Secrets Manager and S3 sink code remains available for
future adaptation. The current Airflow 3 DAG is not deployed to MWAA, and the
previous Airflow 2 MWAA helper was removed to avoid advertising an incompatible
cloud deployment path.
