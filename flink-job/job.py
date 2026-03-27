"""
Flink Job — Phase 2: Stream Processing
raw_news → deduplication + keyword filter + enrichment → processed_news

Architecture:
  Kafka source (raw_news)
    → Deduplication (keyed by URL hash, 10-min state TTL)
    → Keyword extraction
    → Category classification
    → Kafka sink (processed_news)

NOTE: This uses PyFlink's Table API with DataStream for maximum compatibility
with Flink 1.18 on ARM64 via Rosetta 2.
"""

import hashlib
import json
import logging
import os
import sys
import time
import uuid
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("flink-job")

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC: str = os.environ.get("KAFKA_INPUT_TOPIC", "raw_news")
OUTPUT_TOPIC: str = os.environ.get("KAFKA_OUTPUT_TOPIC", "processed_news")

# ─── Keyword Taxonomy ─────────────────────────────────────────────────────────

CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "Chính trị": [
        "chính phủ", "quốc hội", "thủ tướng", "bộ trưởng", "chủ tịch",
        "đảng", "bầu cử", "nghị quyết", "hiệp định", "ngoại giao",
        "government", "minister", "parliament", "election", "president",
    ],
    "Kinh tế": [
        "gdp", "tăng trưởng", "lạm phát", "thị trường", "chứng khoán",
        "ngân hàng", "đầu tư", "xuất khẩu", "nhập khẩu", "kinh doanh",
        "economy", "inflation", "market", "stock", "investment", "trade",
        "startup", "doanh nghiệp", "lợi nhuận", "doanh thu",
    ],
    "Công nghệ": [
        "ai", "trí tuệ nhân tạo", "công nghệ", "phần mềm", "điện thoại",
        "internet", "blockchain", "crypto", "tiền số", "robot",
        "technology", "software", "hardware", "app", "data", "cloud",
        "chatgpt", "openai", "google", "meta", "apple", "samsung",
    ],
    "Thể thao": [
        "bóng đá", "bóng rổ", "tennis", "formula", "olympic",
        "vô địch", "giải đấu", "cầu thủ", "hlv", "đội tuyển",
        "football", "soccer", "basketball", "championship", "athlete",
    ],
    "Sức khoẻ": [
        "sức khỏe", "bệnh viện", "y tế", "vaccine", "dịch", "covid",
        "bác sĩ", "thuốc", "nghiên cứu", "ung thư", "tim mạch",
        "health", "hospital", "medicine", "disease", "virus", "cancer",
    ],
    "Xã hội": [
        "giáo dục", "trường học", "sinh viên", "học sinh", "văn hóa",
        "nghệ thuật", "du lịch", "môi trường", "biến đổi khí hậu",
        "education", "school", "culture", "tourism", "environment", "climate",
    ],
    "Thế giới": [
        "mỹ", "trung quốc", "nga", "châu âu", "liên hiệp quốc",
        "biden", "trump", "ukraine", "war", "nato", "asean",
        "united states", "china", "russia", "europe", "un", "conflict",
    ],
}

DEFAULT_CATEGORY = "Tin tức"


def classify_article(title: str, content: str) -> tuple[str, list[str]]:
    """Return (category, keywords) for an article."""
    text = (title + " " + content).lower()
    found_keywords: list[str] = []
    category_scores: dict[str, int] = {}

    for category, terms in CATEGORY_KEYWORDS.items():
        for term in terms:
            if term in text:
                found_keywords.append(term)
                category_scores[category] = category_scores.get(category, 0) + 1

    if category_scores:
        best_category = max(category_scores, key=lambda k: category_scores[k])
    else:
        best_category = DEFAULT_CATEGORY

    # Deduplicate and limit keywords
    unique_keywords = list(dict.fromkeys(found_keywords))[:8]
    return best_category, unique_keywords


def make_summary(title: str, content: str, max_len: int = 200) -> str:
    """Create a short summary from content (simple truncation)."""
    text = content.strip()
    if not text:
        return title.strip()
    if len(text) <= max_len:
        return text
    # Try to break at sentence boundary
    truncated = text[:max_len]
    last_period = max(truncated.rfind(". "), truncated.rfind("。"), truncated.rfind("! "))
    if last_period > max_len // 2:
        return truncated[: last_period + 1].strip()
    return truncated.strip() + "…"


# ─── Deduplication ────────────────────────────────────────────────────────────

class StreamDeduplicator:
    """In-memory deduplication with TTL (10 minutes)."""

    def __init__(self, ttl_seconds: int = 600):
        self._seen: dict[str, float] = {}
        self._ttl = ttl_seconds

    def _fingerprint(self, url: str) -> str:
        return hashlib.sha256(url.strip().lower().encode()).hexdigest()

    def _evict_expired(self) -> None:
        now = time.time()
        expired = [k for k, ts in self._seen.items() if now - ts > self._ttl]
        for k in expired:
            del self._seen[k]

    def is_new(self, url: str) -> bool:
        self._evict_expired()
        fp = self._fingerprint(url)
        if fp in self._seen:
            return False
        self._seen[fp] = time.time()
        return True


# ─── Main Processing Loop ─────────────────────────────────────────────────────

def run_with_kafka_python() -> None:
    """
    Fallback implementation using kafka-python directly.
    Provides the same stream processing semantics as PyFlink but
    runs as a simple Python process — more reliable on ARM64.
    """
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.errors import KafkaError

    logger.info("=" * 60)
    logger.info("Flink Job (Kafka-Python mode) starting")
    logger.info("  Input:  %s → %s", KAFKA_BOOTSTRAP_SERVERS, INPUT_TOPIC)
    logger.info("  Output: %s → %s", KAFKA_BOOTSTRAP_SERVERS, OUTPUT_TOPIC)
    logger.info("=" * 60)

    deduplicator = StreamDeduplicator(ttl_seconds=600)
    processed_count = 0
    duplicate_count = 0
    filtered_count = 0

    # Create producer
    for attempt in range(1, 16):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
            )
            logger.info("Producer connected (attempt %d)", attempt)
            break
        except KafkaError as exc:
            logger.warning("Producer not ready (attempt %d): %s", attempt, exc)
            time.sleep(5)
    else:
        raise RuntimeError("Cannot connect Kafka producer")

    # Create consumer
    for attempt in range(1, 16):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="flink-processor-group",
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=-1,
            )
            logger.info("Consumer connected (attempt %d)", attempt)
            break
        except KafkaError as exc:
            logger.warning("Consumer not ready (attempt %d): %s", attempt, exc)
            time.sleep(5)
    else:
        raise RuntimeError("Cannot connect Kafka consumer")

    logger.info("Processing stream: %s → %s", INPUT_TOPIC, OUTPUT_TOPIC)

    for message in consumer:
        try:
            raw: dict = message.value
            url: str = raw.get("url", "")
            title: str = raw.get("title", "")
            content: str = raw.get("content", "")

            if not url or not title:
                logger.debug("Skipping article with missing url/title")
                filtered_count += 1
                continue

            # Deduplication
            if not deduplicator.is_new(url):
                logger.debug("Duplicate skipped: %s", url)
                duplicate_count += 1
                continue

            # Enrich
            category, keywords = classify_article(title, content)
            summary = make_summary(title, content)

            processed: dict = {
                "id": raw.get("id", str(uuid.uuid4())),
                "title": title,
                "summary": summary,
                "url": url,
                "keywords": keywords,
                "category": category,
                "source": raw.get("source", ""),
                "timestamp": raw.get("timestamp", int(time.time())),
                "processed_at": int(time.time()),
            }

            producer.send(OUTPUT_TOPIC, key=processed["id"], value=processed)
            producer.flush()
            processed_count += 1

            logger.info(
                "[%s] ✓ Processed | category=%s | keywords=%s | %s",
                processed["id"][:8],
                category,
                keywords[:3],
                title[:50],
            )

            # Log stats every 50 messages
            if processed_count % 50 == 0:
                logger.info(
                    "Stats: processed=%d duplicates=%d filtered=%d",
                    processed_count,
                    duplicate_count,
                    filtered_count,
                )

        except json.JSONDecodeError as exc:
            logger.error("JSON decode error: %s", exc)
        except Exception as exc:
            logger.exception("Error processing message: %s", exc)


def run_with_pyflink() -> None:
    """PyFlink Table API implementation."""
    try:
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            FlinkKafkaConsumer,
            FlinkKafkaProducer,
        )
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.typeinfo import Types
        from pyflink.datastream.functions import MapFunction
    except ImportError as exc:
        logger.error("PyFlink import failed: %s — falling back to kafka-python mode", exc)
        run_with_kafka_python()
        return

    logger.info("Starting PyFlink streaming job")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(30_000)  # checkpoint every 30s

    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "pyflink-processor-group",
        "auto.offset.reset": "earliest",
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=INPUT_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )
    kafka_consumer.set_start_from_earliest()

    kafka_producer = FlinkKafkaProducer(
        topic=OUTPUT_TOPIC,
        serialization_schema=SimpleStringSchema(),
        producer_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
    )

    class EnrichmentMapper(MapFunction):
        def __init__(self):
            self._deduplicator = None

        def open(self, runtime_context):
            self._deduplicator = StreamDeduplicator(ttl_seconds=600)

        def map(self, value: str) -> Optional[str]:
            try:
                raw = json.loads(value)
                url = raw.get("url", "")
                title = raw.get("title", "")
                content = raw.get("content", "")

                if not url or not title:
                    return None
                if not self._deduplicator.is_new(url):
                    return None

                category, keywords = classify_article(title, content)
                summary = make_summary(title, content)

                processed = {
                    "id": raw.get("id", str(uuid.uuid4())),
                    "title": title,
                    "summary": summary,
                    "url": url,
                    "keywords": keywords,
                    "category": category,
                    "source": raw.get("source", ""),
                    "timestamp": raw.get("timestamp", int(time.time())),
                    "processed_at": int(time.time()),
                }
                return json.dumps(processed, ensure_ascii=False)
            except Exception as exc:
                logger.error("map error: %s", exc)
                return None

    stream = env.add_source(kafka_consumer)
    processed = stream.map(EnrichmentMapper(), output_type=Types.STRING()).filter(
        lambda x: x is not None
    )
    processed.add_sink(kafka_producer)

    env.execute("NewsEnrichmentJob")


if __name__ == "__main__":
    mode = os.environ.get("FLINK_MODE", "kafka-python").lower()
    if mode == "pyflink":
        logger.info("Running in PyFlink mode")
        run_with_pyflink()
    else:
        logger.info("Running in kafka-python mode (recommended for ARM64)")
        run_with_kafka_python()
