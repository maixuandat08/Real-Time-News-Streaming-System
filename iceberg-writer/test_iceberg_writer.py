import importlib.util
import os
import sys


_SVC = os.path.dirname(__file__)


def _load(module_name: str):
    path = os.path.join(_SVC, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"_iceberg_writer_{module_name}", path)
    assert spec and spec.loader, f"Cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"_iceberg_writer_{module_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


_main = _load("main")

Settings = _main.Settings
build_catalog_properties = _main.build_catalog_properties
build_storage_record = _main.build_storage_record
build_table_identifier = _main.build_table_identifier
build_table_location = _main.build_table_location
should_flush = _main.should_flush


def test_build_storage_record_flattens_processed_payload():
    payload = {
        "article_id": "abc123",
        "source": "BBC World News",
        "matched_feed_name": "bbc-world",
        "title": "Market update",
        "summary": "A short summary",
        "url": "https://example.com/article",
        "keywords": ["ai", "openai"],
        "published_at": "2026-03-28T07:00:00Z",
        "ingested_at": "2026-03-28T07:01:00Z",
        "category": "Cong nghe",
        "raw_hash": "deadbeef",
        "is_duplicate": False,
        "should_alert": True,
        "matched_keywords": ["openai"],
        "_published_at_source": "rss",
        "raw_payload": {"url": "https://example.com/article", "id": "raw-1"},
    }

    row = build_storage_record(payload, topic="processed_news", partition=2, offset=99)

    assert row["article_id"] == "abc123"
    assert row["keywords"] == ["ai", "openai"]
    assert row["matched_keywords"] == ["openai"]
    assert row["source_topic"] == "processed_news"
    assert row["kafka_partition"] == 2
    assert row["kafka_offset"] == 99
    assert "\"url\": \"https://example.com/article\"" in row["raw_payload_json"]


def test_build_catalog_properties_targets_minio_rest_catalog():
    settings = Settings(
        kafka_bootstrap_servers="kafka:9092",
        input_topic="processed_news",
        consumer_group="iceberg-writer-group",
        batch_size=100,
        batch_timeout_seconds=60,
        log_level="INFO",
        minio_endpoint="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_region="us-east-1",
        minio_bucket="news-archive",
        catalog_name="news",
        rest_uri="http://iceberg-rest:8181",
        warehouse_uri="s3://news-archive/warehouse",
        namespace="news",
        table_name="processed_news",
    )

    props = build_catalog_properties(settings)

    assert props["type"] == "rest"
    assert props["uri"] == "http://iceberg-rest:8181"
    assert props["warehouse"] == "s3://news-archive/warehouse"
    assert props["s3.endpoint"] == "http://minio:9000"
    assert props["s3.access-key-id"] == "minioadmin"


def test_table_identifier_and_location_are_stable():
    settings = Settings(
        kafka_bootstrap_servers="kafka:9092",
        input_topic="processed_news",
        consumer_group="iceberg-writer-group",
        batch_size=100,
        batch_timeout_seconds=60,
        log_level="INFO",
        minio_endpoint="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_region="us-east-1",
        minio_bucket="news-archive",
        catalog_name="news",
        rest_uri="http://iceberg-rest:8181",
        warehouse_uri="s3://news-archive/warehouse/",
        namespace="news",
        table_name="processed_news",
    )

    assert build_table_identifier(settings) == "news.processed_news"
    assert build_table_location(settings) == "s3://news-archive/warehouse/news/processed_news"


def test_should_flush_by_size():
    settings = Settings(
        kafka_bootstrap_servers="kafka:9092",
        input_topic="processed_news",
        consumer_group="iceberg-writer-group",
        batch_size=3,
        batch_timeout_seconds=60,
        log_level="INFO",
        minio_endpoint="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_region="us-east-1",
        minio_bucket="news-archive",
        catalog_name="news",
        rest_uri="http://iceberg-rest:8181",
        warehouse_uri="s3://news-archive/warehouse",
        namespace="news",
        table_name="processed_news",
    )

    assert should_flush(3, 10.0, 11.0, settings) is True


def test_should_flush_by_timeout():
    settings = Settings(
        kafka_bootstrap_servers="kafka:9092",
        input_topic="processed_news",
        consumer_group="iceberg-writer-group",
        batch_size=100,
        batch_timeout_seconds=5,
        log_level="INFO",
        minio_endpoint="http://minio:9000",
        minio_access_key="minioadmin",
        minio_secret_key="minioadmin",
        minio_region="us-east-1",
        minio_bucket="news-archive",
        catalog_name="news",
        rest_uri="http://iceberg-rest:8181",
        warehouse_uri="s3://news-archive/warehouse",
        namespace="news",
        table_name="processed_news",
    )

    assert should_flush(2, 10.0, 16.0, settings) is True
    assert should_flush(2, 10.0, 12.0, settings) is False
