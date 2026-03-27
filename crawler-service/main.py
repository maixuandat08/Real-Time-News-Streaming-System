"""
Crawler Service — Real-Time News Streaming System
Fetches RSS feeds, normalizes articles, deduplicates by URL,
and produces to Kafka topic `raw_news`.
"""

import asyncio
import hashlib
import json
import logging
import os
import time
import uuid
from typing import Optional

import aiohttp
import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "raw_news")
CRAWL_INTERVAL_SECONDS: int = int(os.environ.get("CRAWL_INTERVAL_SECONDS", "60"))
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

# RSS feeds to crawl
RSS_FEEDS = [
    {
        "url": "https://vnexpress.net/rss/tin-moi-nhat.rss",
        "source": "VNExpress",
    },
    {
        "url": "https://vnexpress.net/rss/thoi-su.rss",
        "source": "VNExpress - Thời sự",
    },
    {
        "url": "https://vnexpress.net/rss/the-gioi.rss",
        "source": "VNExpress - Thế giới",
    },
    {
        "url": "https://vnexpress.net/rss/kinh-doanh.rss",
        "source": "VNExpress - Kinh doanh",
    },
    {
        "url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "source": "BBC World News",
    },
]

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("crawler-service")


# ─── Kafka Producer ───────────────────────────────────────────────────────────

def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
    """Create a Kafka producer with retry logic for startup."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=5,
                max_in_flight_requests_per_connection=1,
                compression_type="gzip",
            )
            logger.info(
                "Kafka producer connected to %s (attempt %d/%d)",
                KAFKA_BOOTSTRAP_SERVERS,
                attempt,
                retries,
            )
            return producer
        except KafkaError as exc:
            logger.warning(
                "Kafka not ready (attempt %d/%d): %s. Retrying in %ds…",
                attempt,
                retries,
                exc,
                delay,
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} after {retries} attempts."
    )


# ─── Deduplication ────────────────────────────────────────────────────────────

class UrlDeduplicator:
    """In-memory URL deduplication using a fixed-size set.

    For production, replace with Redis or a persistent store.
    Keeps last `max_size` URLs to bound memory usage.
    """

    def __init__(self, max_size: int = 10_000):
        self._seen: set[str] = set()
        self._order: list[str] = []
        self._max_size = max_size

    def _fingerprint(self, url: str) -> str:
        return hashlib.sha256(url.strip().lower().encode()).hexdigest()

    def is_new(self, url: str) -> bool:
        fp = self._fingerprint(url)
        if fp in self._seen:
            return False
        # Evict oldest if at capacity
        if len(self._seen) >= self._max_size:
            oldest = self._order.pop(0)
            self._seen.discard(oldest)
        self._seen.add(fp)
        self._order.append(fp)
        return True


# ─── RSS Fetching ─────────────────────────────────────────────────────────────

async def fetch_feed(session: aiohttp.ClientSession, feed_url: str) -> Optional[str]:
    """Fetch raw RSS XML with a timeout."""
    try:
        async with session.get(feed_url, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status == 200:
                return await resp.text()
            logger.warning("HTTP %d fetching %s", resp.status, feed_url)
            return None
    except asyncio.TimeoutError:
        logger.warning("Timeout fetching %s", feed_url)
        return None
    except aiohttp.ClientError as exc:
        logger.warning("Client error fetching %s: %s", feed_url, exc)
        return None


def parse_feed(raw_xml: str, source: str) -> list[dict]:
    """Parse RSS XML and normalize each entry to raw_news schema."""
    articles = []
    try:
        feed = feedparser.parse(raw_xml)
    except Exception as exc:
        logger.error("Error parsing feed from %s: %s", source, exc)
        return articles

    for entry in feed.entries:
        url: str = entry.get("link", "")
        if not url:
            continue

        title: str = entry.get("title", "").strip()
        # Try various content fields
        content: str = (
            entry.get("summary", "")
            or entry.get("description", "")
            or ""
        ).strip()

        articles.append(
            {
                "id": str(uuid.uuid4()),
                "title": title,
                "content": content,
                "url": url,
                "source": source,
                "timestamp": int(time.time()),
            }
        )
    return articles


# ─── Main Crawl Loop ──────────────────────────────────────────────────────────

async def crawl_once(
    session: aiohttp.ClientSession,
    producer: KafkaProducer,
    deduplicator: UrlDeduplicator,
) -> int:
    """Crawl all feeds once, publish new articles. Returns count of published articles."""
    published = 0
    tasks = [fetch_feed(session, feed["url"]) for feed in RSS_FEEDS]
    results = await asyncio.gather(*tasks)

    for feed_cfg, raw_xml in zip(RSS_FEEDS, results):
        if raw_xml is None:
            continue

        articles = parse_feed(raw_xml, feed_cfg["source"])
        for article in articles:
            if not deduplicator.is_new(article["url"]):
                logger.debug("Duplicate skipped: %s", article["url"])
                continue

            try:
                producer.send(
                    KAFKA_TOPIC,
                    key=article["id"],
                    value=article,
                )
                published += 1
                logger.info(
                    "[%s] Published: %s | %s",
                    article["source"],
                    article["title"][:60],
                    article["url"],
                )
            except KafkaError as exc:
                logger.error("Failed to publish article %s: %s", article["id"], exc)

    try:
        producer.flush(timeout=10)
    except KafkaError as exc:
        logger.error("Kafka flush error: %s", exc)

    return published


async def main() -> None:
    logger.info("=" * 60)
    logger.info("Crawler Service starting up")
    logger.info("  Kafka:    %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:    %s", KAFKA_TOPIC)
    logger.info("  Interval: %ds", CRAWL_INTERVAL_SECONDS)
    logger.info("  Feeds:    %d", len(RSS_FEEDS))
    logger.info("=" * 60)

    producer = create_producer()
    deduplicator = UrlDeduplicator()

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; NewsBot/1.0; +https://github.com/newsbot)"
        )
    }

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            try:
                logger.info("Starting crawl cycle…")
                count = await crawl_once(session, producer, deduplicator)
                logger.info("Crawl cycle complete — published %d new articles.", count)
            except Exception as exc:
                logger.exception("Unexpected error in crawl cycle: %s", exc)
            finally:
                logger.info("Next crawl in %ds.", CRAWL_INTERVAL_SECONDS)
                await asyncio.sleep(CRAWL_INTERVAL_SECONDS)


if __name__ == "__main__":
    asyncio.run(main())
