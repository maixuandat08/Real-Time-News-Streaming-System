"""
Crawler Service — Real-Time News Streaming System

Supports two modes controlled by CRAWLER_MODE env or --mode CLI flag:

  realtime (default):
    Crawls continuously on a fixed interval.
    Records startup_ts at launch. Only publishes articles where:
      - published_ts exists AND published_ts >= startup_ts
    Articles with no published_ts are silently skipped (realtime guarantee).
    Uses PersistentUrlState to avoid re-publishing the same URL across cycles.
    Goal: zero backlog on first boot — only fresh, newly-published articles land
    in the topic while the service is running.

  backfill:
    One-shot historical ingest of all enabled feeds.
    No startup_ts filter — publishes every article found in each feed.
    No URL state check — sends even previously-seen URLs (clean re-ingest).
    Supports --per-feed-limit N  : stop each feed after N articles published.
    Supports --days N            : skip articles older than N days (uses published_ts;
                                   articles with no published_ts are included).
    Supports --total-limit N     : hard cap across all feeds.
    Logs per-feed progress and totals clearly.

Feed config: feeds.yaml (via feed_config.py)
Each raw_news article includes:
  - matched_feed_name / should_alert / matched_keywords  (keyword alert metadata)
  - published_ts  (real RSS publish timestamp, seconds UTC, or None)
  - timestamp     (ingestion/fetch time, always present)

State file (realtime mode):
  JSON at CRAWLER_STATE_FILE (default: /data/crawler_state.json)
  Format: {"seen_urls": ["sha256...", ...]}
  Bounded to CRAWLER_STATE_MAX_URLS entries (FIFO eviction).
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

# Realtime mode state file — persists seen URL hashes across restarts
CRAWLER_STATE_FILE: str = os.environ.get("CRAWLER_STATE_FILE", "/data/crawler_state.json")
CRAWLER_STATE_MAX_URLS: int = int(os.environ.get("CRAWLER_STATE_MAX_URLS", "50000"))

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("crawler-service")


# ─── Persistent URL State (realtime mode dedup) ───────────────────────────────

class PersistentUrlState:
    """File-backed URL deduplication state for realtime mode.

    Persists across restarts via a JSON file.
    Keeps at most max_size URL hashes (evicts oldest on overflow).
    NOTE: This only prevents re-sending the *same URL* across cycles.
          The startup_ts filter (not this class) prevents sending old articles.
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
        """Return True if URL is first-seen, and register it immediately."""
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

        # Real article publish time from RSS (UTC-safe via calendar.timegm)
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
            "timestamp": int(time.time()),       # ingestion/fetch time (always set)
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


# ─── Realtime Mode ────────────────────────────────────────────────────────────

async def run_realtime(feeds: list[FeedConfig], producer: KafkaProducer) -> None:
    """Continuous crawl loop — only publishes articles newer than startup.

    Semantics:
    - startup_ts is captured ONCE when the service starts.
    - First-seen URLs are registered in PersistentUrlState immediately.
    - Articles with published_ts < startup_ts → skipped (old backlog).
    - Articles with no published_ts            → skipped (no timestamp guarantee).
    - PersistentUrlState prevents re-scanning the same backlog URLs forever.
    - Rate: every CRAWL_INTERVAL_SECONDS seconds.
    """
    startup_ts: int = int(time.time())
    state = PersistentUrlState(CRAWLER_STATE_FILE, max_size=CRAWLER_STATE_MAX_URLS)

    logger.info("=== REALTIME MODE ===")
    logger.info("  startup_ts:  %s (%s UTC)", startup_ts, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(startup_ts)))
    logger.info("  State file:  %s (%d URLs loaded)", CRAWLER_STATE_FILE, state.size())
    logger.info("  Only articles with published_ts >= startup_ts will be published.")
    logger.info("  Articles with no published_ts are skipped (realtime guarantee).")

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        while True:
            try:
                published, skipped_old, skipped_no_ts, skipped_dedup = await _realtime_crawl_once(
                    session, producer, feeds, state, startup_ts
                )
                logger.info(
                    "Realtime cycle done — published=%d  skip_old=%d  skip_no_ts=%d  skip_dedup=%d",
                    published, skipped_old, skipped_no_ts, skipped_dedup,
                )
            except Exception as exc:
                logger.exception("Unexpected error in realtime cycle: %s", exc)
            finally:
                logger.info("Next crawl in %ds.", CRAWL_INTERVAL_SECONDS)
                await asyncio.sleep(CRAWL_INTERVAL_SECONDS)


async def _realtime_crawl_once(
    session: aiohttp.ClientSession,
    producer: KafkaProducer,
    feeds: list[FeedConfig],
    state: PersistentUrlState,
    startup_ts: int,
) -> tuple[int, int, int, int]:
    """Fetch all feeds, apply realtime filters, publish new articles.

    Returns:
        (published, skipped_old, skipped_no_ts, skipped_dedup)
    """
    tasks = [fetch_feed(session, f.url) for f in feeds]
    results = await asyncio.gather(*tasks)

    published = 0
    skipped_old = 0
    skipped_no_ts = 0
    skipped_dedup = 0

    for feed, raw_xml in zip(feeds, results):
        if raw_xml is None:
            logger.warning("No data from feed '%s'", feed.name)
            continue

        articles = parse_feed(raw_xml, feed)
        for article in articles:
            # Warm state on first encounter, even if the article is skipped
            # by realtime filters. This snapshots today's backlog once and
            # prevents reprocessing the same old/no-ts URLs every cycle.
            if not state.is_new(article["url"]):
                skipped_dedup += 1
                logger.debug("SKIP dedup: %s", article["url"])
                continue

            pub_ts = article.get("published_ts")

            # Filter 1: no timestamp → skip (realtime guarantee)
            if pub_ts is None:
                skipped_no_ts += 1
                logger.debug("SKIP no-ts: [%s] %s", feed.name, article["title"][:50])
                continue

            # Filter 2: older than startup → skip (backlog guard)
            if pub_ts < startup_ts:
                skipped_old += 1
                logger.debug("SKIP old: [%s] %s (published_ts=%d < startup_ts=%d)", feed.name, article["title"][:50], pub_ts, startup_ts)
                continue

            if publish(producer, article):
                published += 1
                alert_tag = " [ALERT]" if article["should_alert"] else ""
                logger.info("[%s]%s Published: %s", feed.name, alert_tag, article["title"][:60])

    try:
        producer.flush(timeout=10)
    except KafkaError as exc:
        logger.error("Kafka flush error: %s", exc)

    return published, skipped_old, skipped_no_ts, skipped_dedup


# ─── Backfill Mode ────────────────────────────────────────────────────────────

async def run_backfill(
    feeds: list[FeedConfig],
    producer: KafkaProducer,
    per_feed_limit: Optional[int],
    total_limit: Optional[int],
    days: Optional[int],
) -> None:
    """One-shot historical ingest from all enabled feeds.

    Args:
        per_feed_limit: Max articles published per individual feed.
                        Feed stops immediately when reached (not post-hoc).
                        None = no per-feed limit.
        total_limit:    Hard cap across all feeds combined.
                        None = no total limit.
        days:           Only include articles published within the last N days.
                        Uses published_ts. Articles with no published_ts are
                        INCLUDED (conservative: unknown age ≠ old).
                        None = no time filter.

    Does NOT use PersistentUrlState — intentionally re-publishes all found entries.
    """
    cutoff_ts: Optional[int] = None
    if days is not None:
        cutoff_ts = int(time.time()) - days * 86400

    logger.info("=== BACKFILL MODE ===")
    logger.info("  per_feed_limit: %s", per_feed_limit or "unlimited")
    logger.info("  total_limit:    %s", total_limit or "unlimited")
    if cutoff_ts:
        logger.info("  days filter:    %d  (cutoff: after %s UTC)", days, time.strftime("%Y-%m-%d", time.gmtime(cutoff_ts)))
    else:
        logger.info("  days filter:    none")

    connector = aiohttp.TCPConnector(limit=10, ssl=False)
    headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/1.0)"}

    grand_total = 0
    done = False  # set True when total_limit reached

    async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
        tasks = [fetch_feed(session, f.url) for f in feeds]
        results = await asyncio.gather(*tasks)

        for feed, raw_xml in zip(feeds, results):
            if done:
                break
            if raw_xml is None:
                logger.warning("No data from feed '%s' — skipping.", feed.name)
                continue

            articles = parse_feed(raw_xml, feed)
            feed_published = 0
            feed_skipped_old = 0
            feed_skipped_no_limit = 0

            for article in articles:
                if done:
                    break

                # Days filter (articles with no published_ts are included)
                pub_ts = article.get("published_ts")
                if cutoff_ts is not None and pub_ts is not None and pub_ts < cutoff_ts:
                    feed_skipped_old += 1
                    logger.debug("SKIP old: [%s] %s", feed.name, article["title"][:50])
                    continue

                # Per-feed limit (checked before publish)
                if per_feed_limit is not None and feed_published >= per_feed_limit:
                    feed_skipped_no_limit += 1
                    continue

                # Total limit (checked before publish)
                if total_limit is not None and grand_total >= total_limit:
                    logger.info("Total limit %d reached — stopping backfill.", total_limit)
                    done = True
                    break

                if publish(producer, article):
                    feed_published += 1
                    grand_total += 1
                    alert_tag = " [ALERT]" if article["should_alert"] else ""
                    logger.info(
                        "[%s]%s Backfill (%d/%s total): %s",
                        feed.name, alert_tag,
                        grand_total, total_limit or "∞",
                        article["title"][:60],
                    )

            logger.info(
                "Feed '%s' done — published=%d  skip_old=%d  skip_per_feed_limit=%d",
                feed.name, feed_published, feed_skipped_old, feed_skipped_no_limit,
            )

    try:
        producer.flush(timeout=15)
    except KafkaError as exc:
        logger.error("Kafka flush error: %s", exc)

    logger.info("Backfill complete — total published: %d", grand_total)


# ─── CLI ──────────────────────────────────────────────────────────────────────

def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be > 0")
    return parsed

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="News Crawler Service — realtime or backfill mode.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Realtime (default): only publishes articles published after startup
  python main.py

  # Backfill: publish everything in all feeds (no limit)
  python main.py --mode backfill

  # Backfill: at most 10 articles per feed
  python main.py --mode backfill --per-feed-limit 10

  # Backfill: last 7 days only, max 500 total
  python main.py --mode backfill --days 7 --total-limit 500
""",
    )
    parser.add_argument(
        "--mode",
        choices=["realtime", "backfill"],
        default=os.environ.get("CRAWLER_MODE", "realtime"),
        help="realtime (default): only articles newer than startup. "
             "backfill: one-shot historical ingest.",
    )
    parser.add_argument(
        "--per-feed-limit",
        type=_positive_int,
        default=None,
        metavar="N",
        help="[backfill] Stop each feed after publishing N articles.",
    )
    parser.add_argument(
        "--total-limit",
        type=_positive_int,
        default=None,
        metavar="N",
        help="[backfill] Hard cap on total articles published across all feeds.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=None,
        metavar="N",
        help="[backfill] Only include articles published within the last N days "
             "(uses published_ts; articles with no timestamp are included).",
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
        await run_backfill(
            feeds,
            producer,
            per_feed_limit=args.per_feed_limit,
            total_limit=args.total_limit,
            days=args.days,
        )
    else:
        await run_realtime(feeds, producer)


if __name__ == "__main__":
    asyncio.run(main())
