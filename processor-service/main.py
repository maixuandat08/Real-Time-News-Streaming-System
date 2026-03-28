"""
Processor Service — Real-Time News Streaming System

Pipeline: raw_news (Kafka) → normalize + classify + dedup → processed_news (Kafka)

Delivery semantics:
  - enable_auto_commit=False  (manual commit only)
  - Offset is committed ONLY after a successful producer ack (future.get())
  - If publish fails, offset is NOT committed → message will be reprocessed on restart
  - Duplicate messages skip the publish step but DO commit the offset
    (we have already decided to drop them; re-reading won't change the outcome)

Dedupe contract — Direction A:
  processed_news is a DEDUPED stream.
  Duplicates (same raw_hash seen before in this session) are DROPPED before publish.
  Only unique articles are written to processed_news.
  is_duplicate flag is always False in published records (for schema consistency).
  Dropped duplicates are logged at INFO level with a clear prefix.

  Implication: after processor restart, the dedup window is reset (in-memory).
  Articles seen in a prior session may appear again — acceptable at this stage.
  To prevent cross-session duplicates, replace HashDeduplicator with Redis (TODO).

Schema produced (processed_news):
  article_id          : stable SHA-256 of URL (first 32 hex chars)
  source              : string
  title               : string
  summary             : string (truncated content)
  url                 : string
  keywords            : list[str] — matched rule-based keywords
  published_at        : ISO-8601 UTC — real RSS publish time; fallback = ingestion time
  _published_at_source: "rss" | "ingestion_fallback" (debug/audit aid)
  ingested_at         : ISO-8601 UTC — when this processor handled the record
  category            : string (rule-based classifier)
  raw_hash            : SHA-256(url+title) — dedup fingerprint
  is_duplicate        : bool — always False for published records (see dedup contract)
  raw_payload         : dict — original raw_news (trace/debug)
"""

import json
import logging
import os
import sys
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from classifier import classify, extract_keywords
from normalizer import normalize, make_raw_hash

# ─── Configuration ────────────────────────────────────────────────────────────

LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("processor-service")

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC: str = os.environ.get("PROCESSOR_INPUT_TOPIC", "raw_news")
OUTPUT_TOPIC: str = os.environ.get("PROCESSOR_OUTPUT_TOPIC", "processed_news")
CONSUMER_GROUP: str = os.environ.get("PROCESSOR_GROUP_ID", "processor-consumer-group")
DEDUP_WINDOW_SIZE: int = int(os.environ.get("PROCESSOR_DEDUP_WINDOW", "10000"))

# Timeout (seconds) for producer.send().get() — blocks until broker ack
PRODUCER_ACK_TIMEOUT: int = int(os.environ.get("PROCESSOR_ACK_TIMEOUT", "10"))


# ─── In-Memory Deduplication ──────────────────────────────────────────────────

class HashDeduplicator:
    """Fixed-size LRU-like set for deduplicating by raw_hash.

    is_duplicate() returns True if this hash was seen before in the current
    session. Evicts oldest entries when the window is full.

    State is session-scoped (in-memory). After restart, all articles are
    treated as new. For persistent dedup, replace with Redis (TODO Phase 3).
    """

    def __init__(self, max_size: int = 10_000):
        self._seen: set[str] = set()
        self._order: list[str] = []
        self._max_size = max_size

    def is_duplicate(self, raw_hash: str) -> bool:
        """Return True if this hash was already seen; register it if new."""
        if raw_hash in self._seen:
            return True
        if len(self._seen) >= self._max_size:
            oldest = self._order.pop(0)
            self._seen.discard(oldest)
        self._seen.add(raw_hash)
        self._order.append(raw_hash)
        return False


# ─── Kafka Helpers ────────────────────────────────────────────────────────────

def create_consumer(retries: int = 15, delay: int = 5) -> KafkaConsumer:
    """Create Kafka consumer with manual commit and retry on startup."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                # MANUAL COMMIT: we commit only after successful publish
                enable_auto_commit=False,
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                consumer_timeout_ms=-1,  # block indefinitely
                session_timeout_ms=30_000,
                heartbeat_interval_ms=10_000,
            )
            logger.info(
                "Consumer connected — topic=%s group=%s (attempt %d/%d)",
                INPUT_TOPIC, CONSUMER_GROUP, attempt, retries,
            )
            return consumer
        except KafkaError as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s. Retrying in %ds…",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Cannot connect consumer to {KAFKA_BOOTSTRAP_SERVERS} after {retries} attempts."
    )


def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create Kafka producer with retry on startup."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",          # wait for full broker ack
                retries=5,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
            )
            logger.info(
                "Producer connected to %s (attempt %d/%d)",
                KAFKA_BOOTSTRAP_SERVERS, attempt, retries,
            )
            return producer
        except KafkaError as exc:
            logger.warning(
                "Kafka producer not ready (attempt %d/%d): %s. Retrying in %ds…",
                attempt, retries, exc, delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Cannot connect producer to {KAFKA_BOOTSTRAP_SERVERS} after {retries} attempts."
    )


# ─── Processing Logic ─────────────────────────────────────────────────────────

def process_message(raw: dict, deduplicator: HashDeduplicator) -> dict:
    """Transform a raw_news dict into a processed_news dict.

    Steps:
    1. Fingerprint url+title → raw_hash
    2. Detect duplicate via dedup window
    3. Classify category
    4. Normalize into full schema
    """
    url: str = raw.get("url", "").strip()
    title: str = raw.get("title", "").strip()
    content: str = raw.get("content", raw.get("summary", "")).strip()

    raw_hash = make_raw_hash(url, title)
    is_duplicate = deduplicator.is_duplicate(raw_hash)
    category = classify(title, content)
    keywords = extract_keywords(title, content)

    # Preserve alert decision set by crawler's feed keyword rules.
    # Defaults to False if crawler is an older version without these fields.
    should_alert_flag: bool = bool(raw.get("should_alert", False))
    matched_kws: list[str] = raw.get("matched_keywords", [])

    return normalize(
        raw,
        category=category,
        keywords=keywords,
        is_duplicate=is_duplicate,
        should_alert=should_alert_flag,
        matched_keywords=matched_kws,
    )


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Processor Service starting up")
    logger.info("  Kafka:        %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Input:        %s", INPUT_TOPIC)
    logger.info("  Output:       %s", OUTPUT_TOPIC)
    logger.info("  Group:        %s", CONSUMER_GROUP)
    logger.info("  Dedup window: %d", DEDUP_WINDOW_SIZE)
    logger.info("  Dedupe contract: Direction A (duplicates dropped, not published)")
    logger.info("  Commit mode:  manual (after producer ack)")
    logger.info("=" * 60)

    consumer = create_consumer()
    producer = create_producer()
    deduplicator = HashDeduplicator(max_size=DEDUP_WINDOW_SIZE)

    total = published = dropped_duplicates = errors = 0

    logger.info("Waiting for messages on topic '%s'…", INPUT_TOPIC)

    for message in consumer:
        total += 1
        try:
            # ── 1. Deserialize ───────────────────────────────────────────────
            raw: dict = message.value

            if not isinstance(raw, dict):
                logger.warning(
                    "[p=%d o=%d] CONSUME ERROR — not a dict: %r",
                    message.partition, message.offset, raw,
                )
                errors += 1
                # Commit: this message is unrecoverable; skipping is correct.
                consumer.commit()
                continue

            url = raw.get("url", "")
            title = raw.get("title", "")

            if not url:
                logger.warning(
                    "[p=%d o=%d] CONSUME ERROR — missing url, title=%r",
                    message.partition, message.offset, title[:60],
                )
                errors += 1
                consumer.commit()
                continue

            # ── 2. Process (classify + dedup + normalize) ────────────────────
            try:
                processed_msg = process_message(raw, deduplicator)
            except Exception as exc:
                logger.error(
                    "[p=%d o=%d] PROCESS ERROR — %s | title=%r",
                    message.partition, message.offset, exc, title[:60],
                )
                errors += 1
                # Commit: normalization/classification errors are deterministic;
                # retrying the same message won't help.
                consumer.commit()
                continue

            # ── 3. Dedupe contract Direction A: drop duplicates ──────────────
            if processed_msg["is_duplicate"]:
                dropped_duplicates += 1
                logger.info(
                    "[p=%d o=%d] DEDUP DROP — raw_hash=%s | %s",
                    message.partition, message.offset,
                    processed_msg["raw_hash"][:12],
                    title[:60],
                )
                # Commit offset: decision is final, no point reprocessing.
                try:
                    consumer.commit()
                except KafkaError as exc:
                    logger.warning(
                        "[p=%d o=%d] COMMIT WARNING after dedup drop: %s",
                        message.partition, message.offset, exc,
                    )
                continue

            # ── 4. Publish to Kafka (blocking ack) ───────────────────────────
            try:
                future = producer.send(
                    OUTPUT_TOPIC,
                    key=processed_msg["article_id"],
                    value=processed_msg,
                )
                # Block until broker confirms. KafkaError raised on failure.
                record_meta = future.get(timeout=PRODUCER_ACK_TIMEOUT)
                published += 1
                logger.info(
                    "[p=%d o=%d] PUBLISH OK — category=%-20s | pub_at_src=%-18s | %s",
                    message.partition, message.offset,
                    processed_msg["category"],
                    processed_msg.get("_published_at_source", "?"),
                    title[:55],
                )
                logger.debug(
                    "  → %s [partition=%d offset=%d]",
                    OUTPUT_TOPIC, record_meta.partition, record_meta.offset,
                )
            except KafkaError as exc:
                # Publish failed → DO NOT commit offset.
                # The message will be redelivered after restart.
                errors += 1
                logger.error(
                    "[p=%d o=%d] PUBLISH FAIL — offset NOT committed → will retry | %s",
                    message.partition, message.offset, exc,
                )
                continue  # skip commit below

            # ── 5. Commit offset ONLY after successful publish ───────────────
            try:
                consumer.commit()
            except KafkaError as exc:
                # Commit failed but message was published.
                # On restart this message will be reprocessed and re-published
                # (at-least-once semantics). Downstream should handle idempotency.
                logger.warning(
                    "[p=%d o=%d] COMMIT WARNING after publish — "
                    "message published but offset not committed (at-least-once risk): %s",
                    message.partition, message.offset, exc,
                )

            # Log stats every 100 messages
            if total % 100 == 0:
                logger.info(
                    "Stats — total=%d published=%d dropped_duplicates=%d errors=%d",
                    total, published, dropped_duplicates, errors,
                )

        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            errors += 1
            logger.error(
                "[p=%d o=%d] PARSE ERROR — %s",
                message.partition, message.offset, exc,
            )
            try:
                consumer.commit()
            except KafkaError:
                pass
        except Exception as exc:
            errors += 1
            logger.exception(
                "[p=%d o=%d] UNEXPECTED ERROR — %s",
                message.partition, message.offset, exc,
            )


if __name__ == "__main__":
    main()
