"""POSIX publication protocol for a single-host, shared-volume Parquet lake.

Only directories in published/ are visible outputs. A directory rename makes
all its data files and checksum manifest visible together. Staging and Kafka
commits remain separate, so a failed acknowledgement can be retried safely.
"""

import fcntl
import hashlib
import json
import os
import shutil
import uuid
from contextlib import contextmanager
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

TRANSPORT_SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("value", pa.binary()),
    ]
)
ENVELOPE_SCHEMA = pa.schema(
    [
        ("record_id", pa.string()),
        ("payload_hash", pa.string()),
        ("event_time", pa.timestamp("ms", tz="UTC")),
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("payload", pa.string()),
    ]
)
OUTPUT_SCHEMAS = {
    "events": pa.schema(
        list(ENVELOPE_SCHEMA)
        + [
            ("event_id", pa.int32()),
            ("computer", pa.string()),
            ("source", pa.string()),
            ("level", pa.string()),
            ("severity", pa.string()),
            ("category", pa.string()),
            ("user", pa.string()),
            ("message", pa.string()),
        ]
    ),
    "alerts": pa.schema(
        list(ENVELOPE_SCHEMA)
        + [
            ("computer", pa.string()),
            ("failed_logins", pa.int64()),
            ("window_start", pa.timestamp("ms", tz="UTC")),
            ("window_end", pa.timestamp("ms", tz="UTC")),
            ("rule", pa.string()),
        ]
    ),
    "quarantine": pa.schema(
        list(ENVELOPE_SCHEMA)
        + [
            ("reason", pa.string()),
            ("raw", pa.string()),
        ]
    ),
}
KINDS = tuple(OUTPUT_SCHEMAS)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value):
    return hashlib.sha256(value).hexdigest()


def checksum(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with open(temporary, "x") as destination:
            destination.write(canonical(value))
            destination.flush()
            os.fsync(destination.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def write_parquet(path, rows, schema):
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, compression="zstd")
    with open(path, "rb") as source:
        os.fsync(source.fileno())


class Lake:
    def __init__(self, root):
        self.root = Path(root)

    @contextmanager
    def locked(self):
        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.root / ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def pending_path(self, group):
        return self.root / "pending" / (digest(group.encode()) + ".json")

    def batch_path(self, batch_id, *, published=False):
        if len(batch_id) != 64 or any(c not in "0123456789abcdef" for c in batch_id):
            raise ValueError("Invalid batch ID")
        return self.root / ("published" if published else "staged") / batch_id

    def manifest(self, batch_id, *, published=False):
        path = self.batch_path(batch_id, published=published)
        manifest = json.loads((path / "manifest.json").read_text())
        if manifest["batch_id"] != batch_id:
            raise ValueError("Batch identity mismatch")
        return manifest

    def published_manifests(self):
        return [
            json.loads(p.read_text())
            for p in sorted((self.root / "published").glob("*/manifest.json"))
        ]

    def verify(self, batch_id, *, published=False):
        path = self.batch_path(batch_id, published=published)
        manifest = self.manifest(batch_id, published=published)
        for name, expected in manifest["checksums"].items():
            if Path(name).name != name or checksum(path / name) != expected:
                raise ValueError(f"Batch checksum mismatch: {name}")
        return manifest

    def stage(self, records, ranges, group, run_id):
        identity = {
            "ranges": ranges,
            "records_sha256": digest(
                canonical(
                    [
                        [
                            r["topic"],
                            r["partition"],
                            r["offset"],
                            digest(r["value"] or b""),
                        ]
                        for r in records
                    ]
                ).encode()
            ),
        }
        batch_id = digest(canonical(identity).encode())
        path = self.batch_path(batch_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            temporary = path.with_name("." + uuid.uuid4().hex)
            temporary.mkdir()
            try:
                write_parquet(temporary / "records.parquet", records, TRANSPORT_SCHEMA)
                atomic_json(
                    temporary / "manifest.json",
                    {
                        **identity,
                        "batch_id": batch_id,
                        "run_id": run_id,
                        "consumed": len(records),
                        "checksums": {
                            "records.parquet": checksum(temporary / "records.parquet")
                        },
                    },
                )
                os.rename(temporary, path)
                sync_directory(path.parent)
            finally:
                shutil.rmtree(temporary, ignore_errors=True)
        self.verify(batch_id)
        reference = {"batch_id": batch_id, "consumer_group": group}
        atomic_json(self.pending_path(group), reference)
        return reference

    def known_entities(self, kind, identities):
        paths = [str(p) for p in (self.root / "published").glob(f"*/{kind}.parquet")]
        if not identities or not paths:
            return {}
        table = ds.dataset(paths, format="parquet").to_table(
            columns=["record_id", "payload_hash"],
            filter=ds.field("record_id").isin(list(identities)),
        )
        return {row["record_id"]: row["payload_hash"] for row in table.to_pylist()}

    def publish(self, reference, outputs, summary):
        batch_id = reference["batch_id"]
        destination = self.batch_path(batch_id, published=True)
        if destination.exists():
            self.verify(batch_id, published=True)
            return {**reference, "newly_published": False}
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.root / "publishing" / uuid.uuid4().hex
        temporary.parent.mkdir(parents=True, exist_ok=True)
        temporary.mkdir()
        try:
            for kind in KINDS:
                write_parquet(
                    temporary / f"{kind}.parquet", outputs[kind], OUTPUT_SCHEMAS[kind]
                )
            manifest = {
                **self.manifest(batch_id),
                **summary,
                "checksums": {
                    f"{kind}.parquet": checksum(temporary / f"{kind}.parquet")
                    for kind in KINDS
                },
            }
            atomic_json(temporary / "manifest.json", manifest)
            os.rename(temporary, destination)
            sync_directory(destination.parent)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)
        return {**reference, "newly_published": True}
