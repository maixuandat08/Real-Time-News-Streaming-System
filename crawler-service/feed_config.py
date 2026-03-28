"""
feed_config.py — Load and validate feeds.yaml config.

Provides:
  - FeedConfig dataclass: one feed's config
  - load_feeds(path): parse YAML → list[FeedConfig], fail fast on errors
  - should_alert(feed, title, content): keyword alert decision

Used by both live and backfill crawlers.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

import yaml

logger = logging.getLogger("feed-config")

DEFAULT_FEEDS_PATH = os.path.join(os.path.dirname(__file__), "feeds.yaml")


@dataclass
class FeedConfig:
    """Configuration for a single RSS feed."""
    name: str
    url: str
    enabled: bool = True
    category_hint: str = ""
    include_keywords: list[str] = field(default_factory=list)
    exclude_keywords: list[str] = field(default_factory=list)


def load_feeds(path: str = DEFAULT_FEEDS_PATH) -> list[FeedConfig]:
    """Parse feeds.yaml and return list of enabled FeedConfig objects.

    Fails fast on missing file, YAML parse errors, or missing required fields.
    Silently skips feeds with enabled=false.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Feed config not found: {path}. "
            "Create feeds.yaml or set FEEDS_CONFIG_PATH env var."
        )

    try:
        with open(path, "r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in {path}: {exc}") from exc

    if not isinstance(raw, dict) or "feeds" not in raw:
        raise ValueError(f"{path} must have a top-level 'feeds' list.")

    feeds_raw = raw["feeds"]
    if not isinstance(feeds_raw, list):
        raise ValueError(f"'feeds' in {path} must be a list.")

    feeds: list[FeedConfig] = []
    for i, entry in enumerate(feeds_raw):
        if not isinstance(entry, dict):
            raise ValueError(f"Feed entry #{i} in {path} must be a dict.")

        name = entry.get("name", "").strip()
        url = entry.get("url", "").strip()

        if not name:
            raise ValueError(f"Feed entry #{i} in {path} is missing 'name'.")
        if not url:
            raise ValueError(f"Feed '{name}' in {path} is missing 'url'.")

        enabled = bool(entry.get("enabled", True))
        if not enabled:
            logger.debug("Feed '%s' is disabled — skipping.", name)
            continue

        feeds.append(FeedConfig(
            name=name,
            url=url,
            enabled=True,
            category_hint=str(entry.get("category_hint", "")),
            include_keywords=[kw.lower() for kw in entry.get("include_keywords", [])],
            exclude_keywords=[kw.lower() for kw in entry.get("exclude_keywords", [])],
        ))

    if not feeds:
        logger.warning("No enabled feeds found in %s.", path)

    logger.info("Loaded %d enabled feeds from %s.", len(feeds), path)
    return feeds


def should_alert(feed: FeedConfig, title: str, content: str = "") -> tuple[bool, list[str]]:
    """Decide if an article should trigger a Telegram alert based on keyword rules.

    Returns:
        (should_alert: bool, matched_keywords: list[str])

    Logic:
        1. Check exclude_keywords — if any match, suppress alert.
        2. If include_keywords is empty → alert everything not excluded.
        3. If include_keywords is set → only alert if at least one matches.

    Note: This does NOT affect whether the article enters the data pipeline.
    All articles are always published to Kafka regardless.
    """
    text = (title + " " + content).lower()
    matched: list[str] = []

    # Step 1: exclude takes priority
    for kw in feed.exclude_keywords:
        if kw in text:
            return False, []

    # Step 2: include filter
    if not feed.include_keywords:
        # No include filter → alert everything (not excluded)
        return True, []

    for kw in feed.include_keywords:
        if kw in text:
            matched.append(kw)

    return (len(matched) > 0), matched
