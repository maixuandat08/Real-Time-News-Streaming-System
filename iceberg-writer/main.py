"""
Iceberg writer for the demo ETL project.

Pipeline:
    Kafka(processed_news) -> Iceberg writer -> Iceberg table on MinIO

This service is intentionally narrow:
- it consumes the already-normalized `processed_news` topic
- it creates/loads one real Iceberg table: `<namespace>.<table>`
- it appends batches only after successful table writes
- it commits Kafka offsets only after the append succeeds

Current table contract:
- one Silver-style table backed by MinIO object storage
- timestamps are stored as ISO-8601 strings from processed_news
- raw payload is stored as JSON text for trace/debug
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    input_topic: str
    consumer_group: str
    batch_size: int
    batch_timeout_seconds: int
    log_level: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_region: str
    minio_bucket: str
    catalog_name: str
    rest_uri: str
    warehouse_uri: str
    namespace: str
    table_name: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            input_topic=os.environ.get("ICEBERG_INPUT_TOPIC", "processed_news"),
            consumer_group=os.environ.get("ICEBERG_GROUP_ID", "iceberg-writer-group"),
            batch_size=int(os.environ.get("ICEBERG_BATCH_SIZE", "100")),
            batch_timeout_seconds=int(os.environ.get("ICEBERG_BATCH_TIMEOUT", "60")),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            minio_region=os.environ.get("MINIO_REGION", "us-east-1"),
            minio_bucket=os.environ.get("MINIO_BUCKET", "news-archive"),
            catalog_name=os.environ.get("ICEBERG_CATALOG_NAME", "news"),
            rest_uri=os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
            warehouse_uri=os.environ.get(
                "ICEBERG_WAREHOUSE", "s3://news-archive/warehouse"
            ),
            namespace=os.environ.get("ICEBERG_NAMESPACE", "news"),
            table_name=os.environ.get("ICEBERG_TABLE", "processed_news"),
        )


logger = logging.getLogger("iceberg-writer")


def configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None]
    if isinstance(value, str):
        return [value] if value.strip() else []
    return [str(value)]


def _json_text(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


def build_storage_record(
    payload: dict[str, Any],
    *,
    topic: str,
    partition: int,
    offset: int,
) -> dict[str, Any]:
    """Flatten a processed_news payload into a row for Iceberg storage."""
    return {
        "article_id": str(payload.get("article_id", "")).strip(),
        "source": str(payload.get("source", "")).strip(),
        "matched_feed_name": str(payload.get("matched_feed_name", "")).strip(),
        "title": str(payload.get("title", "")).strip(),
        "summary": str(payload.get("summary", "")).strip(),
        "url": str(payload.get("url", "")).strip(),
        "keywords": _string_list(payload.get("keywords")),
        "published_at": str(payload.get("published_at", "")).strip(),
        "ingested_at": str(payload.get("ingested_at", "")).strip(),
        "category": str(payload.get("category", "")).strip(),
        "raw_hash": str(payload.get("raw_hash", "")).strip(),
        "is_duplicate": bool(payload.get("is_duplicate", False)),
        "should_alert": bool(payload.get("should_alert", False)),
        "matched_keywords": _string_list(payload.get("matched_keywords")),
        "raw_payload_json": _json_text(payload.get("raw_payload", {})),
        "published_at_source": str(payload.get("_published_at_source", "")).strip(),
        "source_topic": topic,
        "kafka_partition": int(partition),
        "kafka_offset": int(offset),
    }


def should_flush(batch_len: int, batch_started_at: float | None, now: float, settings: Settings) -> bool:
    if batch_len <= 0:
        return False
    if batch_len >= settings.batch_size:
        return True
    if batch_started_at is None:
        return False
    return (now - batch_started_at) >= settings.batch_timeout_seconds


def build_catalog_properties(settings: Settings) -> dict[str, str]:
    return {
        "type": "rest",
        "uri": settings.rest_uri,
        "warehouse": settings.warehouse_uri,
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.endpoint": settings.minio_endpoint,
        "s3.access-key-id": settings.minio_access_key,
        "s3.secret-access-key": settings.minio_secret_key,
        "s3.region": settings.minio_region,
    }


def build_table_identifier(settings: Settings) -> str:
    return f"{settings.namespace}.{settings.table_name}"


def build_table_location(settings: Settings) -> str:
    warehouse_root = settings.warehouse_uri.rstrip("/")
    return f"{warehouse_root}/{settings.namespace}/{settings.table_name}"


def build_table_schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import BooleanType, ListType, LongType, NestedField, StringType

    return Schema(
        NestedField(1, "article_id", StringType(), required=True),
        NestedField(2, "source", StringType()),
        NestedField(3, "matched_feed_name", StringType()),
        NestedField(4, "title", StringType()),
        NestedField(5, "summary", StringType()),
        NestedField(6, "url", StringType()),
        NestedField(7, "keywords", ListType(element_id=107, element=StringType(), element_required=False)),
        NestedField(8, "published_at", StringType()),
        NestedField(9, "ingested_at", StringType()),
        NestedField(10, "category", StringType()),
        NestedField(11, "raw_hash", StringType()),
        NestedField(12, "is_duplicate", BooleanType()),
        NestedField(13, "should_alert", BooleanType()),
        NestedField(14, "matched_keywords", ListType(element_id=114, element=StringType(), element_required=False)),
        NestedField(15, "raw_payload_json", StringType()),
        NestedField(16, "published_at_source", StringType()),
        NestedField(17, "source_topic", StringType()),
        NestedField(18, "kafka_partition", LongType()),
        NestedField(19, "kafka_offset", LongType()),
    )


def build_arrow_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("article_id", pa.string(), nullable=False),
            pa.field("source", pa.string()),
            pa.field("matched_feed_name", pa.string()),
            pa.field("title", pa.string()),
            pa.field("summary", pa.string()),
            pa.field("url", pa.string()),
            pa.field("keywords", pa.list_(pa.field("element", pa.string()))),
            pa.field("published_at", pa.string()),
            pa.field("ingested_at", pa.string()),
            pa.field("category", pa.string()),
            pa.field("raw_hash", pa.string()),
            pa.field("is_duplicate", pa.bool_()),
            pa.field("should_alert", pa.bool_()),
            pa.field("matched_keywords", pa.list_(pa.field("element", pa.string()))),
            pa.field("raw_payload_json", pa.string()),
            pa.field("published_at_source", pa.string()),
            pa.field("source_topic", pa.string()),
            pa.field("kafka_partition", pa.int64()),
            pa.field("kafka_offset", pa.int64()),
        ]
    )


def create_consumer(settings: Settings, retries: int = 15, delay_seconds: int = 5):
    from kafka import KafkaConsumer

    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                settings.input_topic,
                bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
                group_id=settings.consumer_group,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                consumer_timeout_ms=1000,
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
                max_poll_records=settings.batch_size,
            )
            logger.info(
                "Kafka consumer connected: topic=%s group=%s (attempt %d/%d)",
                settings.input_topic,
                settings.consumer_group,
                attempt,
                retries,
            )
            return consumer
        except Exception as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s. Retrying in %ds.",
                attempt,
                retries,
                exc,
                delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("Cannot connect iceberg-writer consumer to Kafka.")


def ensure_minio_bucket(settings: Settings) -> None:
    import boto3
    from botocore.client import Config
    from botocore.exceptions import ClientError

    client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    try:
        client.head_bucket(Bucket=settings.minio_bucket)
        logger.info("MinIO bucket exists: %s", settings.minio_bucket)
        return
    except ClientError:
        pass

    client.create_bucket(Bucket=settings.minio_bucket)
    logger.info("Created MinIO bucket: %s", settings.minio_bucket)


def load_catalog_and_table(settings: Settings):
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(settings.catalog_name, **build_catalog_properties(settings))
    catalog.create_namespace_if_not_exists(settings.namespace)
    table = catalog.create_table_if_not_exists(
        build_table_identifier(settings),
        schema=build_table_schema(),
        location=build_table_location(settings),
        properties={
            "format-version": "2",
            "write.object-storage.enabled": "true",
            "write.metadata.delete-after-commit.enabled": "true",
            "write.parquet.compression-codec": "zstd",
        },
    )
    logger.info("Iceberg table ready: %s at %s", build_table_identifier(settings), build_table_location(settings))
    return catalog, table


def append_batch(table, rows: list[dict[str, Any]]) -> int:
    import pyarrow as pa

    if not rows:
        return 0
    arrow_table = pa.Table.from_pylist(rows, schema=build_arrow_schema())
    table.append(
        arrow_table,
        snapshot_properties={
            "ingest.source": "demo_etl.iceberg_writer",
            "ingest.record_count": str(len(rows)),
        },
    )
    return len(rows)


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level)

    logger.info("=" * 60)
    logger.info("Iceberg Writer starting up")
    logger.info("  Kafka:       %s", settings.kafka_bootstrap_servers)
    logger.info("  Input topic: %s", settings.input_topic)
    logger.info("  Group:       %s", settings.consumer_group)
    logger.info("  REST:        %s", settings.rest_uri)
    logger.info("  Warehouse:   %s", settings.warehouse_uri)
    logger.info("  Table:       %s", build_table_identifier(settings))
    logger.info("=" * 60)

    ensure_minio_bucket(settings)
    _, table = load_catalog_and_table(settings)
    consumer = create_consumer(settings)

    buffered_rows: list[dict[str, Any]] = []
    buffered_messages = 0
    batch_started_at: float | None = None

    while True:
        records = consumer.poll(timeout_ms=1000, max_records=settings.batch_size)

        for messages in records.values():
            for message in messages:
                payload = message.value
                if not isinstance(payload, dict):
                    logger.warning(
                        "Skipping non-dict message at p=%s o=%s: %r",
                        message.partition,
                        message.offset,
                        payload,
                    )
                    continue

                if batch_started_at is None:
                    batch_started_at = time.monotonic()

                buffered_rows.append(
                    build_storage_record(
                        payload,
                        topic=message.topic,
                        partition=message.partition,
                        offset=message.offset,
                    )
                )
                buffered_messages += 1

        now = time.monotonic()
        if not should_flush(len(buffered_rows), batch_started_at, now, settings):
            continue

        try:
            written = append_batch(table, buffered_rows)
            consumer.commit()
            logger.info(
                "Appended %d rows to Iceberg and committed Kafka offsets.",
                written,
            )
            buffered_rows.clear()
            buffered_messages = 0
            batch_started_at = None
        except Exception as exc:
            logger.exception(
                "Iceberg append failed for %d buffered messages: %s. Offsets not committed.",
                buffered_messages,
                exc,
            )
            time.sleep(5)


if __name__ == "__main__":
    main()
