"""
normalizer.py — Normalize raw_news articles into the processed_news schema.

processed_news schema:
  article_id          : stable SHA-256 of URL (hex, 16 chars prefix)
  source              : string — from raw_news
  matched_feed_name   : string — feed name from feeds.yaml
  title               : string — stripped
  summary             : string — truncated content/summary
  url                 : string — canonical
  keywords            : list[str] — rule-based keyword matches for quick tagging
  published_at        : ISO-8601 UTC string — real article publish time from RSS
                        (set by crawler from entry.published_parsed / entry.updated_parsed)
                        Falls back to ingestion/fetch time if feed doesn’t provide it.
  ingested_at         : ISO-8601 UTC string — time this processor touched the record
  category            : string — from rule-based classifier
  raw_hash            : SHA-256 of (url + title) — used for duplicate detection
  is_duplicate        : bool — True if same raw_hash seen before in this session
  should_alert        : bool — True if article matches feed’s include/exclude keyword rules
  matched_keywords    : list[str] — include_keywords that triggered should_alert=True
  raw_payload         : dict  — original raw_news message (for trace/debug)

raw_news schema (fields used here):
  timestamp     : int   — ingestion/fetch UNIX epoch (always present)
  published_ts  : int | None — real article publish UNIX epoch from RSS
                  (None if feed didn’t provide publish time)
"""

import hashlib
import time
from datetime import datetime, timezone
from typing import Optional

SUMMARY_MAX_LEN = 300  # chars


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _utc_now_iso() -> str:
    """Return current UTC time in ISO-8601 format."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _timestamp_to_iso(ts: Optional[float]) -> str:
    """Convert a UNIX timestamp (int/float) to ISO-8601 UTC string.

    Falls back to current time if ts is None or invalid.
    """
    if ts is None:
        return _utc_now_iso()
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except (ValueError, OSError, OverflowError):
        return _utc_now_iso()


def make_article_id(url: str) -> str:
    """Generate a stable, deterministic article ID from the URL.

    Uses first 32 hex chars of SHA-256(url). Stable across restarts.
    """
    return hashlib.sha256(url.strip().lower().encode("utf-8")).hexdigest()[:32]


def make_raw_hash(url: str, title: str) -> str:
    """Hash of (url + title) for duplicate detection.

    Using both url and title catches cases where same url is republished
    with a different title (e.g. updated breaking news).
    """
    combined = f"{url.strip().lower()}|{title.strip().lower()}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


def _truncate_summary(text: str, max_len: int = SUMMARY_MAX_LEN) -> str:
    """Truncate at sentence boundary where possible."""
    if not text or len(text) <= max_len:
        return text.strip()
    chunk = text[:max_len]
    # Try to break at the last sentence-ending punctuation
    for sep in (". ", "。", "! ", "? ", "\n"):
        idx = chunk.rfind(sep)
        if idx > max_len // 2:
            return chunk[: idx + len(sep)].strip() + "…"
    return chunk.strip() + "…"


# ─── Public API ───────────────────────────────────────────────────────────────

def normalize(
    raw: dict,
    category: str,
    keywords: list[str],
    is_duplicate: bool,
    should_alert: bool = False,
    matched_keywords: Optional[list[str]] = None,
) -> dict:
    """Transform a raw_news dict into a processed_news dict.

    Args:
        raw:              The raw_news message (dict from Kafka).
        category:         Category string from classifier.
        keywords:         Matched keywords extracted from title + content.
        is_duplicate:     True if this raw_hash was seen before.
        should_alert:     Passed through from raw_news (set by crawler rules).
        matched_keywords: Include-keywords that triggered should_alert.

    Returns:
        processed_news dict ready for Kafka publish.
    """
    url: str = raw.get("url", "").strip()
    title: str = raw.get("title", "").strip()

    # summary: prefer 'content' from raw (RSS description), fall back to title
    content: str = raw.get("content", raw.get("summary", "")).strip()
    summary: str = _truncate_summary(content) if content else title

    article_id: str = make_article_id(url)
    raw_hash: str = make_raw_hash(url, title)
    ingested_at: str = _utc_now_iso()

    # published_at: use real article publish time from RSS if the crawler
    # extracted it (raw["published_ts"] is set from entry.published_parsed).
    # Fall back to crawler ingestion time only when feed didn’t provide it.
    raw_published_ts = raw.get("published_ts")  # int (UTC epoch) or None
    if raw_published_ts is not None:
        published_at = _timestamp_to_iso(raw_published_ts)
        published_at_source = "rss"
    else:
        published_at = _timestamp_to_iso(raw.get("timestamp"))
        published_at_source = "ingestion_fallback"

    return {
        "article_id": article_id,
        "source": raw.get("source", ""),
        "matched_feed_name": raw.get("matched_feed_name", raw.get("source", "")),
        "title": title,
        "summary": summary,
        "url": url,
        "keywords": keywords,
        "published_at": published_at,
        # Internal field kept in payload for debugging / audit.
        # Not part of the public processed_news contract.
        "_published_at_source": published_at_source,
        "ingested_at": ingested_at,
        "category": category,
        "raw_hash": raw_hash,
        "is_duplicate": is_duplicate,
        "should_alert": should_alert,
        "matched_keywords": matched_keywords if matched_keywords is not None else [],
        "raw_payload": raw,  # full original message kept for tracing/debug
    }
