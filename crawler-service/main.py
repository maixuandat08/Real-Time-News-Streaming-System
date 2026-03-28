"""
Crawler Service — Real-Time News Streaming System

Supports two modes:
  live mode (default): Runs continuously, fetches feeds on interval.
                       Tracks seen URLs in a persistent JSON state file.
                       Only publishes articles not seen before (per state file).
  backfill mode:       One-shot run. Reads ALL entries from enabled feeds
                       and publishes them without checking state.
                       Use with --mode backfill [--limit N] from the CLI.

Reads feed list + keyword rules from feeds.yaml (via feed_config.py).
Each article in raw_news includes:
  - matched_feed_name  : source feed name
  - should_alert       : bool, from keyword rules in feeds.yaml
  - matched_keywords   : list[str], keywords that triggered the alert

State file (live mode):
  JSON file at CRAWLER_STATE_FILE (default: /data/crawler_state.json)
  Format: {"seen_urls": ["sha256...", ...]}
  Persists across restarts. Bounded to CRAWLER_STATE_MAX_URLS entries.
"""

import argparse
import asyncio
import calendar
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from typing import Optional

import aiohttp
import feedparser
from kafka import KafkaProducer
from kafka.errors import KafkaError

from feed_config import FeedConfig, load_feeds, should_alert

# ─── Configuration ────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "raw_news")
CRAWL_INTERVAL_SECONDS: int = int(os.environ.get("CRAWL_INTERVAL_SECONDS", "60"))
LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

FEEDS_CONFIG_PATH: str = os.environ.get(
    "FEEDS_CONFIG_PATH",
    os.path.join(os.path.dirname(__file__), "feeds.yaml"),
)

# Live mode state file — persists seen URL hashes across restarts
CRAWLER_STATE_FILE: str = os.environ.get("CRAWLER_STATE_FILE", "/data/crawler_state.json")
CRAWLER_STATE_MAX_URLS: int = int(os.environ.get("CRAWLER_STATE_MAX_URLS", "50000"))

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("crawler-service")


# ─── Persistent State (live mode) ─────────────────────────────────────────────

class PersistentUrlState:
    """File-backed URL deduplication state for live mode.

    Persists across restarts via a JSON file.
    Keeps at most max_size URL hashes (evicts oldest on overflow).
    """

    def __init__(self, path: str, max_size: int = 50_000):
        self._path = path
        self._max_size = max_size
        self._seen: set[str] = set()
        self._order: list[str] = []
        self._load()

    def _load(self) -> None:
        """Load state from disk. Silently starts fresh if file missing/corrupt."""
        if not os.path.exists(self._path):
            logger.info("State file not found at %s — starting fresh.", self._path)
            return
        try:
            with open(self._path, "r") as fh:
                data = json.load(fh)
            urls = data.get("seen_urls", [])
            self._order = urls[-self._max_size:]
            self._seen = set(self._order)
            logger.info("Loaded %d URL hashes from state file %s.", len(self._seen), self._path)
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read state file %s: %s — starting fresh.", self._path, exc)

    def _save(self) -> None:
        """Persist state to disk atomically."""
        os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
        tmp_path = self._path + ".tmp"
        try:
            with open(tmp_path, "w") as fh:
                json.dump({"seen_urls": self._order}, fh)
            os.replace(tmp_path, self._path)
        except OSError as exc:
            logger.warning("Could not save state file %s: %s", self._path, exc)

    def _fingerprint(self, url: str) -> str:
        return hashlib.sha256(url.strip().lower().encode()).hexdigest()

    def is_new(self, url: str) -> bool:
        """Return True if URL is new, and register it (persisting to disk)."""
        fp = self._fingerprint(url)
        if fp in self._seen:
            return False
        if len(self._seen) >= self._max_size:
            oldest = self._order.pop(0)
            self._seen.discard(oldest)
        self._seen.add(fp)
        self._order.append(fp)
        self._save()
        return True

    def size(self) -> int:
        return len(self._seen)


# ─── Kafka Producer ───────────────────────────────────────────────────────────

def create_producer(retries: int = 10, delay: int = 5) -> KafkaProducer:
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
            logger.info("Producer connected to %s (attempt %d/%d)", KAFKA_BOOTSTRAP_SERVERS, attempt, retries)
            return producer
        except KafkaError as exc:
            logger.warning("Kafka not ready (attempt %d/%d): %s. Retrying in %ds…", attempt, retries, exc, delay)
            time.sleep(delay)
    raise RuntimeError(f"Cannot connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} after {retries} attempts.")


# ─── RSS Fetching & Parsing ───────────────────────────────────────────────────

async def fetch_feed(session: aiohttp.ClientSession, feed_url: str) -> Optional[str]:
    """Fetch raw RSS XML with timeout. Returns None on any error."""
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


def parse_feed(raw_xml: str, feed: FeedConfig) -> list[dict]:
    """Parse RSS XML → list of raw_news dicts enriched with alert metadata."""
    articles = []
    try:
        parsed = feedparser.parse(raw_xml)
    except Exception as exc:
        logger.error("Error parsing feed '%s': %s", feed.name, exc)
        return articles

    for entry in parsed.entries:
        url: str = entry.get("link", "").strip()
        if not url:
            continue

        title: str = entry.get("title", "").strip()
        content: str = (
            entry.get("summary", "")
            or entry.get("description", "")
            or ""
        ).strip()

        # Real article publish time from RSS (UTC safe via calendar.timegm)
        published_ts: Optional[int] = None
        for time_field in ("published_parsed", "updated_parsed"):
            parsed_time = entry.get(time_field)
            if parsed_time:
                try:
                    published_ts = int(calendar.timegm(parsed_time))
                except (TypeError, ValueError, OverflowError):
                    pass
                break

        # Alert decision from feed keyword rules
        alert, matched_kws = should_alert(feed, title, content)

        articles.append({
            "id": str(uuid.uuid4()),
            "title": title,
            "content": content,
            "url": url,
            "source": feed.name,
            "matched_feed_name": feed.name,
            "timestamp": int(time.time()),       # ingestion/fetch time
            "published_ts": published_ts,         # real RSS publish time or None
            "should_alert": alert,
            "matched_keywords": matched_kws,
        })

    return articles


# ─── Publish ──────────────────────────────────────────────────────────────────

def publish(producer: KafkaProducer, article: dict) -> bool:
    """Publish a single article to Kafka. Returns True on success."""
    try:
        producer.send(KAFKA_TOPIC, key=article["id"], value=article)
        return True
    except KafkaError as exc:
        logger.error("Failed to publish '%s': %s", article["title"][:50], exc)
        return False


# ─── Live Mode ────────────────────────────────────────────────────────────────

async def run_live(feeds: list[FeedConfig], producer: KafkaProducer) -> None:
    """Continuous crawl loop with persistent URL state.

    Only publishes articles not seen in previous runs (state file).
    """
    state = PersistentUrlState(CRAWLER_STATE_FILE, max_size=CRAWLER_STATE_MAX_URLS)
    logger.info("Live mode — state file: %s (%d URLs loaded)", CRAWLER_STATE_FILE, state.size())

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            try:
                logger.info("Live crawl cycle starting — %d feeds", len(feeds))
                published = await _crawl_once(session, producer, feeds, state)
                logger.info("Live cycle done — published %d new articles.", published)
            except Exception as exc:
                logger.exception("Unexpected error in live cycle: %s", exc)
            finally:
                logger.info("Next crawl in %ds.", CRAWL_INTERVAL_SECONDS)
                await asyncio.sleep(CRAWL_INTERVAL_SECONDS)


async def _crawl_once(
    session: aiohttp.ClientSession,
    producer: KafkaProducer,
    feeds: list[FeedConfig],
    state: Optional[PersistentUrlState],  # None in backfill mode
) -> int:
    """Fetch all feeds concurrently and publish new articles.

    state=None means backfill mode — no dedup, publish everything.
    """
    tasks = [fetch_feed(session, f.url) for f in feeds]
    results = await asyncio.gather(*tasks)

    published = 0
    for feed, raw_xml in zip(feeds, results):
        if raw_xml is None:
            logger.warning("No data from feed '%s'", feed.name)
            continue

        articles = parse_feed(raw_xml, feed)
        for article in articles:
            # Live mode: skip already-seen URLs
            if state is not None and not state.is_new(article["url"]):
                logger.debug("Live-dedup skip: %s", article["url"])
                continue

            if publish(producer, article):
                published += 1
                alert_tag = " [ALERT]" if article["should_alert"] else ""
                logger.info(
                    "[%s]%s Published: %s",
                    feed.name, alert_tag, article["title"][:60],
                )

    try:
        producer.flush(timeout=10)
    except KafkaError as exc:
        logger.error("Kafka flush error: %s", exc)

    return published


# ─── Backfill Mode ─────────────────────────────────────────────────

def _should_include_in_backfill(article: dict, cutoff_ts: Optional[int]) -> bool:
    """Return True if an article should be included in the current backfill run.

    Rules:
    - If no cutoff is configured, include everything.
    - If article has a valid published_ts, include only when published_ts >= cutoff.
    - If published_ts is missing/None, include it conservatively rather than drop it.
    """
    if cutoff_ts is None:
        return True

    published_ts = article.get("published_ts")
    if published_ts is None:
        return True

    try:
        return int(published_ts) >= cutoff_ts
    except (TypeError, ValueError):
        return True

async def run_backfill(
    feeds: list[FeedConfig],
    producer: KafkaProducer,
    limit: Optional[int],
    days: Optional[int],
) -> None:
    """One-shot backfill: publishes entries from all enabled feeds.

    Ignores state file — intended for historical re-ingestion.

    Args:
        limit: Stop after publishing this many articles total (None = no limit).
               Enforced per-article immediately when the count is reached.
        days:  Only publish articles whose published_ts is within the last N days.
               If an article has no published_ts, it is included (unknown age →
               conservative include rather than silent drop).
    """
    cutoff_ts: Optional[int] = None
    if days is not None:
        cutoff_ts = int(time.time()) - days * 86400
        logger.info(
            "Backfill mode — limit=%s days=%d (cutoff: articles after %s)",
            limit or "unlimited", days,
            time.strftime("%Y-%m-%d", time.gmtime(cutoff_ts)),
        )
    else:
        logger.info("Backfill mode — limit=%s days=unlimited", limit or "unlimited")

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}
    total = 0
    reached_limit = False

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [fetch_feed(session, f.url) for f in feeds]
        results = await asyncio.gather(*tasks)

        for feed, raw_xml in zip(feeds, results):
            if raw_xml is None:
                logger.warning("No data from feed '%s'", feed.name)
                continue

            articles = parse_feed(raw_xml, feed)
            for article in articles:
                if not _should_include_in_backfill(article, cutoff_ts):
                    logger.debug(
                        "Backfill skip by days filter: %s | published_ts=%r cutoff=%r",
                        article.get("url", ""),
                        article.get("published_ts"),
                        cutoff_ts,
                    )
                    continue

                if publish(producer, article):
                    total += 1
                    alert_tag = " [ALERT]" if article["should_alert"] else ""
                    logger.info(
                        "[%s]%s Backfill published: %s",
                        feed.name,
                        alert_tag,
                        article["title"][:60],
                    )

                if limit is not None and total >= limit:
                    logger.info("Backfill limit %d reached.", limit)
                    reached_limit = True
                    break

            if reached_limit:
                break

    producer.flush(timeout=15)
    logger.info("Backfill complete — published %d articles.", total)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def _positive_int(value: str) -> int:
    """argparse helper: require a positive integer."""
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="News Crawler Service")
    parser.add_argument(
        "--mode",
        choices=["live", "backfill"],
        default=os.environ.get("CRAWLER_MODE", "live"),
        help="live (default): continuous crawl with state tracking. "
             "backfill: one-shot re-ingest without state.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=None,
        help="Backfill mode only: max articles to publish.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=None,
        help="Backfill mode only: include only articles from the last N days.",
    )
    return parser.parse_args(argv)


async def main() -> None:
    args = parse_args()

    logger.info("=" * 60)
    logger.info("Crawler Service starting — mode: %s", args.mode)
    logger.info("  Kafka:       %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:       %s", KAFKA_TOPIC)
    logger.info("  Feeds file:  %s", FEEDS_CONFIG_PATH)
    logger.info("=" * 60)

    # Load and validate feed config — fail fast if broken
    try:
        feeds = load_feeds(FEEDS_CONFIG_PATH)
    except (FileNotFoundError, ValueError) as exc:
        logger.critical("Feed config error: %s", exc)
        sys.exit(1)

    if not feeds:
        logger.critical("No enabled feeds found. Check feeds.yaml.")
        sys.exit(1)

    logger.info("Loaded %d enabled feeds.", len(feeds))
    for f in feeds:
        inc = f"include={f.include_keywords[:3]}" if f.include_keywords else "include=all"
        exc_kw = f"exclude={f.exclude_keywords[:2]}" if f.exclude_keywords else ""
        logger.info("  Feed: %-40s %s %s", f.name, inc, exc_kw)

    producer = create_producer()

    if args.mode == "backfill":
        await run_backfill(feeds, producer, limit=args.limit, days=args.days)
    else:
        await run_live(feeds, producer)


if __name__ == "__main__":
    asyncio.run(main())
