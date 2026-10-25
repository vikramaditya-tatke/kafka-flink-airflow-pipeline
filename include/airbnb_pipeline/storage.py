"""Owner-checked S3 bootstrap and atomic, create-only object publication.

Each data object is immutable. A separate manifest is the commit record. A
crash between the two writes is repaired using the validated object's metadata.
"""

import base64
import hashlib
import json
import logging
import math
from pathlib import Path
from typing import Any

from botocore.config import Config

from .config import Settings
from .models import CONTRACT, Dataset, IntegrityError

logger = logging.getLogger(__name__)
MIB = 1024 * 1024
MULTIPART_THRESHOLD = 64 * MIB


def create_store(settings: Settings) -> "S3Store":
    """Resolve credentials through the Amazon provider on each Airflow worker."""
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    config = Config(
        signature_version="s3v4",
        connect_timeout=10,
        read_timeout=60,
        retries={"total_max_attempts": 5, "mode": "standard"},
    )
    kwargs = {
        "aws_conn_id": settings.aws_conn_id,
        "region_name": settings.region,
        "config": config,
    }
    owner = (
        AwsBaseHook(client_type="sts", **kwargs)
        .get_conn()
        .get_caller_identity()["Account"]
    )
    return S3Store(S3Hook(**kwargs).get_conn(), settings, owner)


class S3Store:
    """Reuse a single boto3 client across the bounded ingestion workers."""

    def __init__(self, client: Any, settings: Settings, owner: str):
        self.client = client
        self.settings = settings
        self.owner = owner

    @property
    def scope(self) -> dict[str, str]:
        return {"Bucket": self.settings.bucket, "ExpectedBucketOwner": self.owner}

    def check_bucket(self) -> None:
        """Fail on missing buckets, wrong accounts, or wrong configured regions."""
        location = self.client.get_bucket_location(**self.scope)["LocationConstraint"]
        region = {None: "us-east-1", "EU": "eu-west-1"}.get(location, location)
        if region != self.settings.region:
            raise IntegrityError(
                f"Bucket region is {region}, expected {self.settings.region}"
            )

    def _optional_configuration(self, operation: str, missing: str) -> dict:
        # These S3 operations expose their missing-configuration codes only as
        # unmodeled ClientError responses. All other errors must propagate.
        try:
            return getattr(self.client, operation)(**self.scope)
        except self.client.exceptions.ClientError as error:
            if error.response["Error"]["Code"] == missing:
                return {}
            raise

    def bootstrap(self) -> dict[str, str | bool]:
        """Create/harden this bucket; preserve unrelated policy, tags, and rules.

        Never grant public access, expire data, create keys, or change IAM roles.
        Existing buckets must belong to the authenticated account and region.
        """
        created = False
        try:
            self.check_bucket()
        except self.client.exceptions.NoSuchBucket:
            args: dict[str, Any] = {
                "Bucket": self.settings.bucket,
                "ObjectOwnership": "BucketOwnerEnforced",
            }
            if self.settings.region != "us-east-1":
                args["CreateBucketConfiguration"] = {
                    "LocationConstraint": self.settings.region
                }
            try:
                self.client.create_bucket(**args)
                created = True
            except self.client.exceptions.BucketAlreadyOwnedByYou:
                pass  # Another bootstrap process created the same owned bucket.
            self.client.get_waiter("bucket_exists").wait(
                **self.scope, WaiterConfig={"Delay": 1, "MaxAttempts": 20}
            )
            self.check_bucket()

        self.client.put_public_access_block(
            **self.scope,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls": True,
                "IgnorePublicAcls": True,
                "BlockPublicPolicy": True,
                "RestrictPublicBuckets": True,
            },
        )
        self.client.put_bucket_ownership_controls(
            **self.scope,
            OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerEnforced"}]},
        )
        self.client.put_bucket_encryption(
            **self.scope,
            ServerSideEncryptionConfiguration={
                "Rules": [
                    {"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}
                ]
            },
        )
        self.client.put_bucket_versioning(
            **self.scope, VersioningConfiguration={"Status": "Enabled"}
        )
        policy = json.loads(
            self._optional_configuration("get_bucket_policy", "NoSuchBucketPolicy").get(
                "Policy", '{"Version":"2012-10-17","Statement":[]}'
            )
        )
        statements = policy.get("Statement", [])
        if isinstance(statements, dict):
            statements = [statements]
        policy["Statement"] = [
            s for s in statements if s.get("Sid") != "DenyInsecureTransport"
        ] + [
            {
                "Sid": "DenyInsecureTransport",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:*",
                "Resource": [
                    f"arn:aws:s3:::{self.settings.bucket}",
                    f"arn:aws:s3:::{self.settings.bucket}/*",
                ],
                "Condition": {
                    "Bool": {
                        "aws:SecureTransport": "false",
                        "aws:PrincipalIsAWSService": "false",
                    }
                },
            }
        ]
        self.client.put_bucket_policy(**self.scope, Policy=json.dumps(policy))
        rules = self._optional_configuration(
            "get_bucket_lifecycle_configuration", "NoSuchLifecycleConfiguration"
        ).get("Rules", [])
        rules = [
            r for r in rules if r.get("ID") != "AbortIncompleteIngestionUploads"
        ] + [
            {
                "ID": "AbortIncompleteIngestionUploads",
                "Status": "Enabled",
                "Filter": {"Prefix": ""},
                "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 7},
            }
        ]
        self.client.put_bucket_lifecycle_configuration(
            **self.scope, LifecycleConfiguration={"Rules": rules}
        )
        tags = {
            t["Key"]: t["Value"]
            for t in self._optional_configuration(
                "get_bucket_tagging", "NoSuchTagSet"
            ).get("TagSet", [])
        }
        tags.update(Project="airbnb-analytics-pipeline", ManagedBy="airbnb-pipeline")
        self.client.put_bucket_tagging(
            **self.scope,
            Tagging={
                "TagSet": [{"Key": k, "Value": v} for k, v in sorted(tags.items())]
            },
        )
        return {
            "bucket": self.settings.bucket,
            "region": self.settings.region,
            "created": created,
        }

    def head(self, key: str) -> dict | None:
        """HEAD returns an unmodeled 404; never turn 403 into 'not found'."""
        try:
            return self.client.head_object(**self.scope, Key=key)
        except self.client.exceptions.ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return None
            raise

    def read_json(self, key: str) -> dict | None:
        try:
            response = self.client.get_object(**self.scope, Key=key)
        except self.client.exceptions.NoSuchKey:
            return None
        with response["Body"] as body:
            value = json.loads(body.read())
        if not isinstance(value, dict):
            raise IntegrityError(f"Expected a JSON object at {key}")
        return value

    def put_json_once(self, key: str, value: dict) -> bool:
        """Commit a manifest atomically without replacing another writer's work."""
        content = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        try:
            self.client.put_object(
                **self.scope,
                Key=key,
                Body=content,
                ContentType="application/json",
                ServerSideEncryption="AES256",
                ChecksumSHA256=base64.b64encode(
                    hashlib.sha256(content).digest()
                ).decode(),
                IfNoneMatch="*",
            )
            return True
        except self.client.exceptions.ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 412:
                return False
            raise

    def upload_once(self, key: str, path: Path, metadata: dict[str, str]) -> bool:
        """Publish a validated file with a create-only condition, also for multipart.

        Managed upload_file cannot express the destination condition. Use a
        conditional PutObject, or conditional CompleteMultipartUpload for larger
        files, with SHA-256 checksums validated by S3 on every uploaded part.
        """
        size = path.stat().st_size
        args = {**self.scope, "Key": key}
        attributes = {
            "Metadata": metadata,
            "ContentType": "application/vnd.apache.parquet",
            "ServerSideEncryption": "AES256",
        }
        try:
            if size < MULTIPART_THRESHOLD:
                with path.open("rb") as stream:
                    self.client.put_object(
                        **args,
                        **attributes,
                        Body=stream,
                        ContentLength=size,
                        ChecksumSHA256=base64.b64encode(
                            bytes.fromhex(metadata["parquet-sha256"])
                        ).decode(),
                        IfNoneMatch="*",
                    )
            else:
                self._multipart_upload(args, attributes, path)
            return True
        except self.client.exceptions.ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] == 412:
                return False
            raise

    def _multipart_upload(self, args: dict, attributes: dict, path: Path) -> None:
        upload = self.client.create_multipart_upload(
            **args, **attributes, ChecksumAlgorithm="SHA256"
        )["UploadId"]
        # Keep the part count below 10,000 while using bounded part buffers.
        part_size = max(64 * MIB, math.ceil(path.stat().st_size / 9999 / MIB) * MIB)
        parts = []
        try:
            with path.open("rb") as stream:
                while chunk := stream.read(part_size):
                    number = len(parts) + 1
                    checksum = base64.b64encode(hashlib.sha256(chunk).digest()).decode()
                    part = self.client.upload_part(
                        **args,
                        UploadId=upload,
                        PartNumber=number,
                        Body=chunk,
                        ChecksumSHA256=checksum,
                    )
                    parts.append(
                        {
                            "PartNumber": number,
                            "ETag": part["ETag"],
                            "ChecksumSHA256": checksum,
                        }
                    )
            self.client.complete_multipart_upload(
                **args,
                UploadId=upload,
                MultipartUpload={"Parts": parts},
                IfNoneMatch="*",
            )
        except BaseException as error:
            try:
                self.client.abort_multipart_upload(**args, UploadId=upload)
            except self.client.exceptions.NoSuchUpload:
                pass  # Completion may already have succeeded before a timeout.
            except Exception as cleanup_error:  # noqa: BLE001 - preserve the transfer failure
                error.add_note(f"Multipart cleanup also failed: {cleanup_error}")
            raise

    def validate_object(self, dataset: Dataset, head: dict) -> None:
        """A recoverable object must carry proof of completed local validation."""
        metadata = head["Metadata"]
        try:
            valid = (
                metadata["identity"] == dataset.identity
                and metadata["contract"] == CONTRACT
                and int(metadata["row-count"]) > 0
                and int(metadata["size-bytes"]) == head["ContentLength"]
                and all(
                    len(bytes.fromhex(metadata[key])) == 32
                    for key in ("source-sha256", "parquet-sha256", "schema-sha256")
                )
                and metadata["engine"] in ("duckdb", "polars")
            )
        except (KeyError, ValueError, TypeError):
            valid = False
        if not valid:
            raise IntegrityError(
                f"Existing object conflicts with validated source: {dataset.data_key(self.settings.prefix)}"
            )

    def verify_complete(self, dataset: Dataset, manifest: dict) -> dict:
        """Skip only when the manifest and current immutable payload agree."""
        key = dataset.data_key(self.settings.prefix)
        try:
            if not (
                manifest["contract"] == CONTRACT
                and manifest["status"] == "complete"
                and manifest["dataset"] == dataset.as_dict()
                and manifest["data"]["key"] == key
            ):
                raise IntegrityError(f"Manifest identity mismatch for {key}")
            head = self.head(key)
            if head is None:
                raise IntegrityError(f"Committed payload is missing: {key}")
            self.validate_object(dataset, head)
            observed = self.object_record(key, head)
            if manifest["data"] != observed:
                raise IntegrityError(f"Committed payload was changed: {key}")
            return manifest
        except (KeyError, TypeError) as error:
            raise IntegrityError(f"Malformed manifest for {key}") from error

    @staticmethod
    def object_record(key: str, head: dict) -> dict:
        return {
            "key": key,
            "etag": head["ETag"],
            "version_id": head.get("VersionId"),
            "size_bytes": head["ContentLength"],
            "metadata": head["Metadata"],
        }
