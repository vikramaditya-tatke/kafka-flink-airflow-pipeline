# AWS integration

Airflow remains local on Astro/Podman. Kafka/Flink/SQLite requires no AWS
credentials. The Airbnb DAG uses **airbnb-analytics-bucket-v2** in **eu-west-2**,
created and hardened by the original ingestion project. No MWAA environment,
MSK cluster, additional bucket, or cloud compute is needed for this integration.

Airbnb obtains S3 and STS clients through the Amazon Airflow provider using
`aws_airbnb`. Each task checks account ownership and bucket region. Existing
bronze objects remain compatible; silver/gold add immutable versioned model
outputs to the same bucket. Conditional writes protect every published object
and completion manifest, including multipart uploads.

The bucket has public access blocked, bucket-owner-enforced ownership, SSE-S3
encryption, versioning, a TLS-only policy, and seven-day cleanup of incomplete
multipart uploads. No automatic object expiration is configured.

For local SSO credentials, set `AIRBNB_AWS_PROFILE` in ignored `.env` and refresh
that profile before startup. The launcher exports temporary credentials to the
ignored Airflow connection env file without printing them. After expiration,
refresh and restart Astro. For deployed workers, prefer an IAM role and an
Airflow secrets backend. See [the full connection and recovery guide](AIRBNB.md).

The streaming consumer's optional Secrets Manager/S3 integrations remain
available. The selected streaming DAG still writes to persistent local SQLite.
