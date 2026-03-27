"""
Telegram Service — Real-Time News Streaming System

Consumes messages from Kafka (raw_news or processed_news) and forwards
them to a Telegram channel via the Bot API.

Features:
- Reads BOT_TOKEN and CHANNEL_ID from env (REQUIRED — fails fast if missing)
- Rate limiting: ≤1 message/second (Telegram limit)
- Exponential-backoff retry on send failures
- Clean JSON message handling
- Full structured logging
"""

import json
import logging
import os
import sys
import time
from html import escape
from typing import Optional

import httpx
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
    before_sleep_log,
)

# ─── Configuration ────────────────────────────────────────────────────────────

def _require_env(name: str) -> str:
    """Read a required environment variable. Exit with a helpful error if missing."""
    value = os.environ.get(name, "").strip()
    if not value:
        logger.critical(
            "FATAL: Required environment variable '%s' is not set. "
            "Please set it in your .env file and restart the service.",
            name,
        )
        sys.exit(1)
    return value


LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO").upper()

# ─── Logging (must be set up before _require_env calls) ───────────────────────

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("telegram-service")


# ─── Required secrets ─────────────────────────────────────────────────────────

BOT_TOKEN: str = _require_env("BOT_TOKEN")
CHANNEL_ID: str = _require_env("CHANNEL_ID")

# ─── Optional configuration ───────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC: str = os.environ.get("KAFKA_TOPIC", "raw_news")
KAFKA_GROUP_ID: str = os.environ.get("KAFKA_GROUP_ID", "telegram-consumer-group")

# Telegram Bot API
TELEGRAM_API_URL: str = f"https://api.telegram.org/bot{BOT_TOKEN}"
SEND_MESSAGE_URL: str = f"{TELEGRAM_API_URL}/sendMessage"

# Rate limiting (Telegram: max 30 messages/second globally, ~1/sec to same chat)
MIN_SEND_INTERVAL: float = 1.1  # seconds between messages (slightly above 1s)

# Emojis for message formatting
CATEGORY_EMOJIS = {
    "VNExpress - Thời sự": "🇻🇳",
    "VNExpress - Thế giới": "🌏",
    "VNExpress - Kinh doanh": "💼",
    "VNExpress": "📰",
    "BBC World News": "🌍",
}
DEFAULT_EMOJI = "📡"


# ─── Telegram Sender ──────────────────────────────────────────────────────────

def format_message(article: dict) -> str:
    """Format a news article as a Telegram message."""
    source: str = article.get("source", "")
    emoji: str = CATEGORY_EMOJIS.get(source, DEFAULT_EMOJI)

    title: str = escape(article.get("title", "No title").strip())
    url: str = escape(article.get("url", ""), quote=True)

    # For processed_news: use summary + keywords if available
    summary: str = article.get("summary", article.get("content", "")).strip()
    keywords: list = article.get("keywords", [])
    category: str = escape(article.get("category", ""))
    source = escape(source)

    lines = [f"{emoji} <b>{title}</b>"]

    if category:
        lines.append(f"📂 <i>{category}</i>")
    elif source:
        lines.append(f"📌 <i>{source}</i>")

    if summary:
        # Trim long summaries
        if len(summary) > 300:
            summary = summary[:297] + "…"
        lines.append(f"\n{escape(summary)}")

    if keywords:
        tag_str = " ".join(f"#{kw.replace(' ', '_')}" for kw in keywords[:5])
        lines.append(f"\n🏷 {tag_str}")

    if url:
        lines.append(f"\n🔗 <a href=\"{url}\">Read more</a>")

    return "\n".join(lines)


@retry(
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    before_sleep=before_sleep_log(logger, logging.WARNING),
    reraise=True,
)
def send_to_telegram(client: httpx.Client, text: str) -> bool:
    """Send a message to the Telegram channel. Retries on network/HTTP errors."""
    payload = {
        "chat_id": CHANNEL_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": False,
    }
    response = client.post(SEND_MESSAGE_URL, json=payload, timeout=15)

    if response.status_code == 200:
        data = response.json()
        if data.get("ok"):
            return True
        # Telegram-level error (e.g. 403 Forbidden, wrong CHANNEL_ID)
        error_code = data.get("error_code", "unknown")
        description = data.get("description", "")
        logger.error(
            "Telegram API error %s: %s — "
            "Check that your bot is an ADMIN of the channel and CHANNEL_ID is correct.",
            error_code,
            description,
        )
        return False

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 30))
        logger.warning("Telegram rate limit hit. Sleeping %ds.", retry_after)
        time.sleep(retry_after)
        raise httpx.HTTPError(f"Rate limited — retry after {retry_after}s")

    if response.status_code == 403:
        description = response.json().get("description", response.text)
        logger.critical(
            "Telegram returned 403 Forbidden: %s. "
            "Ensure the bot is added as an ADMIN to the channel '%s'.",
            description,
            CHANNEL_ID,
        )
        return False

    if response.status_code == 400:
        description = response.json().get("description", response.text)
        logger.error("Telegram returned 400 Bad Request: %s", description)
        return False

    response.raise_for_status()
    return False


# ─── Kafka Consumer ───────────────────────────────────────────────────────────

def create_consumer(retries: int = 15, delay: int = 5) -> KafkaConsumer:
    """Create a Kafka consumer with retry logic for startup."""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=-1,  # block forever
                session_timeout_ms=30000,
                heartbeat_interval_ms=10000,
            )
            logger.info(
                "Kafka consumer connected — topic: %s, group: %s (attempt %d/%d)",
                KAFKA_TOPIC,
                KAFKA_GROUP_ID,
                attempt,
                retries,
            )
            return consumer
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


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    logger.info("=" * 60)
    logger.info("Telegram Service starting up")
    logger.info("  Kafka:      %s", KAFKA_BOOTSTRAP_SERVERS)
    logger.info("  Topic:      %s", KAFKA_TOPIC)
    logger.info("  Group:      %s", KAFKA_GROUP_ID)
    logger.info("  Channel:    %s", CHANNEL_ID)
    logger.info("=" * 60)

    consumer = create_consumer()
    last_send_time: float = 0.0

    with httpx.Client() as http_client:
        logger.info("Waiting for messages on topic '%s'…", KAFKA_TOPIC)

        for message in consumer:
            try:
                article: dict = message.value
                if not isinstance(article, dict):
                    logger.warning("Unexpected message format (not a dict): %r", article)
                    continue

                article_id = article.get("id", "unknown")
                title = article.get("title", "")[:60]
                logger.info(
                    "Received [partition=%d offset=%d]: [%s] %s",
                    message.partition,
                    message.offset,
                    article_id,
                    title,
                )

                # Rate limiting — enforce minimum gap between sends
                now = time.monotonic()
                elapsed = now - last_send_time
                if elapsed < MIN_SEND_INTERVAL:
                    sleep_time = MIN_SEND_INTERVAL - elapsed
                    logger.debug("Rate-limiting: sleeping %.2fs", sleep_time)
                    time.sleep(sleep_time)

                text = format_message(article)
                success = send_to_telegram(http_client, text)
                last_send_time = time.monotonic()

                if success:
                    logger.info("✓ Sent to Telegram: %s", title)
                else:
                    logger.warning("✗ Failed to send to Telegram: %s", title)

            except json.JSONDecodeError as exc:
                logger.error("JSON decode error on message: %s", exc)
            except Exception as exc:
                logger.exception("Unexpected error processing message: %s", exc)


if __name__ == "__main__":
    main()
