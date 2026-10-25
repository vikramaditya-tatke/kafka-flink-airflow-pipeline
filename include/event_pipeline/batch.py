"""Three workflow operations. Broker acknowledgements happen only in finalize."""

import json
import time
from datetime import UTC, datetime

import pyarrow.parquet as pq

from event_pipeline.lake import (
    KINDS,
    Lake,
    atomic_json,
    canonical,
    digest,
    sync_directory,
)


class RetentionGapError(RuntimeError):
    """A committed position is no longer available; never silently skip input."""


def partition_snapshot(consumer, topics):
    from confluent_kafka import TopicPartition

    metadata = consumer.list_topics(timeout=10)
    partitions = []
    for topic in topics:
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            raise RuntimeError(f"Kafka topic is unavailable: {topic}")
        partitions.extend(TopicPartition(topic, p) for p in sorted(info.partitions))
    committed = consumer.committed(partitions, timeout=10)
    result = []
    for partition in committed:
        if partition.error:
            raise RuntimeError(f"Unable to read committed offset: {partition.error}")
        low, high = consumer.get_watermark_offsets(partition, timeout=10)
        offset = partition.offset if partition.offset >= 0 else None
        result.append(
            {
                "topic": partition.topic,
                "partition": partition.partition,
                "low": low,
                "high": high,
                "committed": offset,
                "lag": max(0, high - (offset if offset is not None else low)),
                "retention_gap": offset is not None and not low <= offset <= high,
            }
        )
    return result


def stage_batch(settings, *, run_id, offset_reset="earliest"):
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    if offset_reset not in {"earliest", "latest"}:
        raise ValueError("offset_reset must be earliest or latest")
    lake = Lake(settings.directory)
    with lake.locked():
        pending = lake.pending_path(settings.group)
        if pending.exists():
            reference = json.loads(pending.read_text())
            if reference["consumer_group"] != settings.group:
                raise ValueError("Pending batch belongs to another consumer group")
            lake.verify(reference["batch_id"])
            return reference
        consumer = Consumer(settings.consumer_config())
        try:
            snapshot = partition_snapshot(consumer, settings.topics)
            assignment = []
            for partition in snapshot:
                if partition["retention_gap"]:
                    raise RetentionGapError(f"Kafka retention gap: {partition}")
                offset = partition["committed"]
                if offset is None:
                    offset = partition["low" if offset_reset == "earliest" else "high"]
                assignment.append(
                    TopicPartition(partition["topic"], partition["partition"], offset)
                )
            consumer.assign(assignment)
            records = []
            deadline = time.monotonic() + settings.poll_seconds
            while len(records) < settings.batch_size and time.monotonic() < deadline:
                message = consumer.poll(min(1, max(0, deadline - time.monotonic())))
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                records.append(
                    {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                        "value": message.value(),
                    }
                )
            if not records:
                return None
            positions = {}
            for record in records:
                key = (record["topic"], record["partition"])
                start, end = positions.get(
                    key, (record["offset"], record["offset"] + 1)
                )
                positions[key] = (
                    min(start, record["offset"]),
                    max(end, record["offset"] + 1),
                )
            ranges = [
                {"topic": t, "partition": p, "start": start, "end": end}
                for (t, p), (start, end) in sorted(positions.items())
            ]
            return lake.stage(records, ranges, settings.group, run_id)
        finally:
            consumer.close()


def decode_record(record):
    """Validate the Flink-to-batch contract; preserve unexpected data in quarantine."""
    try:
        payload = json.loads(record["value"])
        if not isinstance(payload, dict) or payload.get("schemaVersion") != 2:
            raise ValueError("Expected schemaVersion 2 object")
        kind = payload["recordType"]
        if kind == "event":
            for field in (
                "eventUid",
                "computer",
                "user",
                "message",
                "source",
                "level",
                "category",
                "severity",
            ):
                if not isinstance(payload.get(field), str) or (
                    field != "message" and not payload[field]
                ):
                    raise ValueError(f"Invalid field {field}")
            if type(payload.get("eventId")) is not int:
                raise ValueError("Invalid eventId")
            identity = payload["eventUid"]
            timestamp = datetime.fromisoformat(payload["timestamp"])
            kind = "events"
        elif kind == "security_alert":
            identity = payload["alertId"]
            start = datetime.fromisoformat(payload["windowStart"])
            timestamp = datetime.fromisoformat(payload["windowEnd"])
            if (
                not isinstance(identity, str)
                or not identity
                or not payload.get("computer")
            ):
                raise ValueError("Invalid security alert identity")
            if (
                type(payload.get("failedLogins")) is not int
                or payload["failedLogins"] < 3
                or timestamp <= start
            ):
                raise ValueError("Invalid security window")
            kind = "alerts"
        elif kind == "quarantine":
            identity = payload["quarantineId"]
            if (
                not isinstance(identity, str)
                or not identity
                or not payload.get("reason")
            ):
                raise ValueError("Invalid quarantine record")
            timestamp = None
            kind = "quarantine"
        else:
            raise ValueError(f"Unknown recordType {kind}")
        if timestamp is not None and timestamp.tzinfo is None:
            raise ValueError("Event timestamps must include a timezone")
    except (ValueError, TypeError, KeyError, UnicodeDecodeError) as error:
        identity = digest(
            canonical([record["topic"], record["partition"], record["offset"]]).encode()
        )
        payload = {
            "schemaVersion": 2,
            "recordType": "quarantine",
            "quarantineId": identity,
            "reason": f"batch_contract: {error}",
            "rawHex": (record["value"] or b"").hex(),
        }
        timestamp, kind = None, "quarantine"
    serialized = canonical(payload)
    row = {
        "record_id": identity,
        "payload_hash": digest(serialized.encode()),
        "event_time": timestamp,
        "topic": record["topic"],
        "partition": record["partition"],
        "offset": record["offset"],
        "payload": serialized,
    }
    if kind == "events":
        row.update(
            {
                field: payload[field]
                for field in (
                    "computer",
                    "source",
                    "level",
                    "severity",
                    "category",
                    "user",
                    "message",
                )
            }
        )
        row["event_id"] = payload["eventId"]
    elif kind == "alerts":
        row.update(
            {
                "computer": payload["computer"],
                "failed_logins": payload["failedLogins"],
                "window_start": datetime.fromisoformat(payload["windowStart"]),
                "window_end": timestamp,
                "rule": payload.get("rule"),
            }
        )
    else:
        row.update(
            {
                "reason": str(payload["reason"]),
                "raw": payload.get("raw", payload.get("rawHex", "")),
            }
        )
    return kind, row


def publish_batch(settings, reference):
    lake = Lake(settings.directory)
    with lake.locked():
        manifest = lake.verify(reference["batch_id"])
        if lake.batch_path(reference["batch_id"], published=True).exists():
            lake.verify(reference["batch_id"], published=True)
            return {**reference, "newly_published": False}
        covered = {}
        for previous in lake.published_manifests():
            for span in previous["ranges"]:
                covered.setdefault((span["topic"], span["partition"]), []).append(
                    (span["start"], span["end"])
                )
        records = (
            pq.ParquetFile(lake.batch_path(reference["batch_id"]) / "records.parquet")
            .read()
            .to_pylist()
        )
        candidates = {kind: [] for kind in KINDS}
        replayed = 0
        for record in records:
            if any(
                start <= record["offset"] < end
                for start, end in covered.get(
                    (record["topic"], record["partition"]), []
                )
            ):
                replayed += 1
                continue
            kind, row = decode_record(record)
            candidates[kind].append(row)
        output = {kind: [] for kind in KINDS}
        duplicate_entities = 0
        for kind, rows in candidates.items():
            known = lake.known_entities(kind, {r["record_id"] for r in rows})
            for row in rows:
                existing = known.get(row["record_id"])
                if existing is not None:
                    if existing != row["payload_hash"]:
                        raise ValueError(
                            f"Conflicting immutable {kind} identity: {row['record_id']}"
                        )
                    duplicate_entities += 1
                    continue
                known[row["record_id"]] = row["payload_hash"]
                output[kind].append(row)
        counts = {kind: len(rows) for kind, rows in output.items()}
        if sum(counts.values()) + replayed + duplicate_entities != manifest["consumed"]:
            raise ValueError("Batch accounting does not cover every consumed record")
        return lake.publish(
            reference,
            output,
            {
                "rows": counts,
                "replayed_records": replayed,
                "duplicate_entities": duplicate_entities,
                "published_at": datetime.now(UTC).isoformat(),
            },
        )


def finalize_batch(settings, reference):
    from confluent_kafka import Consumer, TopicPartition

    lake = Lake(settings.directory)
    with lake.locked():
        manifest = lake.verify(reference["batch_id"], published=True)
        config = {**settings.consumer_config(), "group.id": reference["consumer_group"]}
        consumer = Consumer(config)
        try:
            partitions = [
                TopicPartition(r["topic"], r["partition"]) for r in manifest["ranges"]
            ]
            positions = consumer.committed(partitions, timeout=10)
            if any(p.error for p in positions):
                raise RuntimeError("Unable to verify existing broker offsets")
            current = {(p.topic, p.partition): p.offset for p in positions}
            offsets = [
                TopicPartition(
                    r["topic"],
                    r["partition"],
                    max(r["end"], current.get((r["topic"], r["partition"]), -1)),
                )
                for r in manifest["ranges"]
            ]
            committed = consumer.commit(offsets=offsets, asynchronous=False)
            if any(p.error for p in committed or []):
                raise RuntimeError(f"Kafka offset commit failed: {committed}")
        finally:
            consumer.close()
        summary = {
            "batch_id": reference["batch_id"],
            "consumer_group": reference["consumer_group"],
            "consumed": manifest["consumed"],
            "rows": manifest["rows"],
            "written": sum(manifest["rows"].values())
            if reference.get("newly_published")
            else 0,
            "replayed_records": manifest["replayed_records"],
            "duplicate_entities": manifest["duplicate_entities"],
            "offsets": {f"{p.topic}:{p.partition}": p.offset for p in offsets},
        }
        atomic_json(
            lake.root
            / "receipts"
            / digest(reference["consumer_group"].encode())
            / (reference["batch_id"] + ".json"),
            summary,
        )
        pending = lake.pending_path(reference["consumer_group"])
        if (
            pending.exists()
            and json.loads(pending.read_text())["batch_id"] == reference["batch_id"]
        ):
            pending.unlink()
            sync_directory(pending.parent)
        return summary
