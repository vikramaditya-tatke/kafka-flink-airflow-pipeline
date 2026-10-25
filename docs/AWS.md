# Local environment

This pipeline runs on Podman with Astro CLI. No AWS deployment is part of the
current implementation, and no AWS resource is required or created.

The output is immutable Parquet on a shared local volume. Moving that publication
protocol to an object store would require a transactional manifest or table
format; POSIX directory rename must not be assumed to work on S3.

The synthetic TCP source remains non-replayable. Changing infrastructure alone
does not strengthen that source's delivery guarantee.
