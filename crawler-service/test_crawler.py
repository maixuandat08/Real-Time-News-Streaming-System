"""
Tests for crawler-service: feed_config loader, keyword alert logic,
and PersistentUrlState live-mode dedup.

Run from REPO ROOT:
    python3 -m pytest crawler-service/test_crawler.py processor-service/test_processor.py telegram-service/test_format_message.py -q

Or from within the service directory:
    cd crawler-service && python3 -m pytest test_crawler.py -v
"""

import importlib.util
import asyncio
import json
import os
import sys
import tempfile

import pytest
import yaml

# ── Absolute-path import of crawler-service modules ──────────────────────
# Prevents sys.path collision with processor-service/main.py when running
# pytest from the repo root.

_SVC = os.path.dirname(__file__)  # absolute path to crawler-service/


def _load(module_name: str):
    """Load a module from crawler-service/ by absolute path."""
    path = os.path.join(_SVC, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"_crawler_{module_name}", path)
    assert spec and spec.loader, f"Cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"_crawler_{module_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


_feed_config = _load("feed_config")
_main = _load("main")

FeedConfig = _feed_config.FeedConfig
load_feeds = _feed_config.load_feeds
should_alert = _feed_config.should_alert
PersistentUrlState = _main.PersistentUrlState
parse_args = _main.parse_args
run_backfill = _main.run_backfill
_should_include_in_backfill = _main._should_include_in_backfill


# ─── FeedConfig / load_feeds ─────────────────────────────────────────────────

VALID_YAML = """
feeds:
  - name: "Test Feed A"
    url: "https://example.com/a.rss"
    enabled: true
    include_keywords: ["AI", "blockchain"]
    exclude_keywords: ["sponsored"]
  - name: "Test Feed B"
    url: "https://example.com/b.rss"
    enabled: false
  - name: "Test Feed C"
    url: "https://example.com/c.rss"
    enabled: true
    include_keywords: []
    exclude_keywords: []
"""


class TestLoadFeeds:
    def _write_yaml(self, content: str) -> str:
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False)
        f.write(content)
        f.close()
        return f.name

    def test_loads_enabled_feeds_only(self):
        path = self._write_yaml(VALID_YAML)
        feeds = load_feeds(path)
        assert len(feeds) == 2  # Feed B is disabled
        assert feeds[0].name == "Test Feed A"
        assert feeds[1].name == "Test Feed C"

    def test_include_keywords_lowercased(self):
        path = self._write_yaml(VALID_YAML)
        feeds = load_feeds(path)
        assert "ai" in feeds[0].include_keywords
        assert "blockchain" in feeds[0].include_keywords

    def test_exclude_keywords_lowercased(self):
        path = self._write_yaml(VALID_YAML)
        feeds = load_feeds(path)
        assert "sponsored" in feeds[0].exclude_keywords

    def test_disabled_feed_skipped(self):
        path = self._write_yaml(VALID_YAML)
        feeds = load_feeds(path)
        names = [f.name for f in feeds]
        assert "Test Feed B" not in names

    def test_missing_file_raises_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            load_feeds("/nonexistent/path/feeds.yaml")

    def test_invalid_yaml_raises_value_error(self):
        path = self._write_yaml("feeds: [invalid: yaml: here")
        with pytest.raises(ValueError, match="Invalid YAML"):
            load_feeds(path)

    def test_missing_feeds_key_raises_value_error(self):
        path = self._write_yaml("something: else\n")
        with pytest.raises(ValueError, match="'feeds'"):
            load_feeds(path)

    def test_missing_name_raises_value_error(self):
        bad = "feeds:\n  - url: https://example.com/x.rss\n    enabled: true\n"
        path = self._write_yaml(bad)
        with pytest.raises(ValueError, match="missing 'name'"):
            load_feeds(path)

    def test_missing_url_raises_value_error(self):
        bad = "feeds:\n  - name: 'No URL feed'\n    enabled: true\n"
        path = self._write_yaml(bad)
        with pytest.raises(ValueError, match="missing 'url'"):
            load_feeds(path)

    def test_empty_feeds_list_returns_empty(self):
        path = self._write_yaml("feeds: []\n")
        feeds = load_feeds(path)
        assert feeds == []

    def test_default_enabled_is_true(self):
        content = "feeds:\n  - name: 'X'\n    url: 'https://x.com'\n"
        path = self._write_yaml(content)
        feeds = load_feeds(path)
        assert len(feeds) == 1 and feeds[0].enabled is True


# ─── should_alert ─────────────────────────────────────────────────────────────

class TestShouldAlert:
    def _feed(self, include=None, exclude=None):
        return FeedConfig(
            name="Test",
            url="https://example.com",
            include_keywords=[kw.lower() for kw in (include or [])],
            exclude_keywords=[kw.lower() for kw in (exclude or [])],
        )

    def test_no_rules_alert_everything(self):
        feed = self._feed()
        ok, matched = should_alert(feed, "Some random headline", "")
        assert ok is True
        assert matched == []

    def test_include_match_triggers_alert(self):
        feed = self._feed(include=["bitcoin"])
        ok, matched = should_alert(feed, "Bitcoin price surges today", "")
        assert ok is True
        assert "bitcoin" in matched

    def test_include_no_match_suppresses_alert(self):
        feed = self._feed(include=["bitcoin"])
        ok, _ = should_alert(feed, "Football weekend results", "sports news")
        assert ok is False

    def test_exclude_suppresses_even_if_include_matches(self):
        feed = self._feed(include=["bitcoin"], exclude=["sponsored"])
        ok, _ = should_alert(feed, "Bitcoin sponsored content", "")
        assert ok is False

    def test_exclude_only_suppresses_matching(self):
        feed = self._feed(exclude=["quảng cáo"])
        ok, _ = should_alert(feed, "Tin tức quảng cáo mới", "")
        assert ok is False

    def test_exclude_no_match_allows_alert(self):
        feed = self._feed(exclude=["quảng cáo"])
        ok, _ = should_alert(feed, "Tin tức thật sự quan trọng", "")
        assert ok is True

    def test_matching_is_case_insensitive(self):
        feed = self._feed(include=["AI"])
        ok, matched = should_alert(feed, "Major AI breakthrough announced", "")
        assert ok is True
        assert "ai" in matched

    def test_summary_also_checked(self):
        feed = self._feed(include=["vn-index"])
        ok, matched = should_alert(feed, "Thị trường hôm nay", "VN-Index tăng 15 điểm")
        assert ok is True
        assert "vn-index" in matched

    def test_multiple_matched_keywords_returned(self):
        feed = self._feed(include=["ai", "blockchain", "chip"])
        ok, matched = should_alert(feed, "AI and blockchain chip advances", "")
        assert ok is True
        assert len(matched) >= 2


# ─── PersistentUrlState ────────────────────────────────────────────────────────

class TestPersistentUrlState:
    def _state(self, max_size: int = 100) -> tuple:
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "state.json")
        return PersistentUrlState(path, max_size=max_size), path

    def test_new_url_is_new(self):
        state, _ = self._state()
        assert state.is_new("https://example.com/a") is True

    def test_second_visit_not_new(self):
        state, _ = self._state()
        state.is_new("https://example.com/a")
        assert state.is_new("https://example.com/a") is False

    def test_different_url_is_new(self):
        state, _ = self._state()
        state.is_new("https://example.com/a")
        assert state.is_new("https://example.com/b") is True

    def test_persists_to_file(self):
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "state.json")
        state1 = PersistentUrlState(path)
        state1.is_new("https://example.com/article-1")
        # New instance reads same file
        state2 = PersistentUrlState(path)
        assert state2.is_new("https://example.com/article-1") is False

    def test_state_file_format(self):
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "state.json")
        state = PersistentUrlState(path)
        state.is_new("https://example.com/x")
        with open(path) as fh:
            data = json.load(fh)
        assert "seen_urls" in data
        assert isinstance(data["seen_urls"], list)
        assert len(data["seen_urls"]) == 1

    def test_eviction_at_max_size(self):
        state, path = self._state(max_size=3)
        urls = [f"https://example.com/{i}" for i in range(4)]
        for url in urls[:4]:
            state.is_new(url)
        # After 4 items in a window of 3, first item was evicted
        # So it's "new" again
        assert state.is_new(urls[0]) is True

    def test_starts_fresh_if_state_file_corrupt(self):
        tmpdir = tempfile.mkdtemp()
        path = os.path.join(tmpdir, "state.json")
        with open(path, "w") as fh:
            fh.write("not valid json {{{")
        # Should not crash, just start fresh
        state = PersistentUrlState(path)
        assert state.size() == 0

    def test_starts_fresh_if_state_file_missing(self):
        path = "/tmp/nonexistent_state_abc123.json"
        if os.path.exists(path):
            os.remove(path)
        state = PersistentUrlState(path)
        assert state.size() == 0


class TestBackfillFilters:
    def test_includes_everything_without_cutoff(self):
        article = {"published_ts": 1700000000}
        assert _should_include_in_backfill(article, None) is True

    def test_includes_when_published_ts_is_new_enough(self):
        assert _should_include_in_backfill({"published_ts": 200}, 100) is True

    def test_excludes_when_published_ts_is_too_old(self):
        assert _should_include_in_backfill({"published_ts": 50}, 100) is False

    def test_includes_when_published_ts_missing(self):
        assert _should_include_in_backfill({}, 100) is True

    def test_includes_when_published_ts_invalid(self):
        assert _should_include_in_backfill({"published_ts": "bad"}, 100) is True


class TestParseArgs:
    def test_parse_backfill_limit_and_days(self):
        args = parse_args(["--mode", "backfill", "--limit", "10", "--days", "7"])
        assert args.mode == "backfill"
        assert args.limit == 10
        assert args.days == 7

    def test_parse_defaults(self):
        args = parse_args([])
        assert args.mode in {"live", "backfill"}
        assert args.limit is None
        assert args.days is None

    def test_limit_must_be_positive(self):
        with pytest.raises(SystemExit):
            parse_args(["--limit", "0"])

    def test_days_must_be_positive(self):
        with pytest.raises(SystemExit):
            parse_args(["--days", "-1"])


class TestRunBackfill:
    class _FakeProducer:
        def __init__(self):
            self.records = []
            self.flush_calls = 0

        def send(self, topic, key=None, value=None):
            self.records.append({"topic": topic, "key": key, "value": value})

        def flush(self, timeout=None):
            self.flush_calls += 1

    def test_limit_stops_publish_early(self, monkeypatch):
        feeds = [
            FeedConfig(name="Feed A", url="https://example.com/a"),
            FeedConfig(name="Feed B", url="https://example.com/b"),
        ]
        producer = self._FakeProducer()

        async def fake_fetch(session, url):
            return f"xml::{url}"

        def fake_parse(raw_xml, feed):
            return [
                {
                    "id": f"{feed.name}-1",
                    "title": f"{feed.name} one",
                    "content": "",
                    "url": f"https://example.com/{feed.name}/1",
                    "source": feed.name,
                    "should_alert": True,
                    "published_ts": 200,
                },
                {
                    "id": f"{feed.name}-2",
                    "title": f"{feed.name} two",
                    "content": "",
                    "url": f"https://example.com/{feed.name}/2",
                    "source": feed.name,
                    "should_alert": False,
                    "published_ts": 200,
                },
            ]

        monkeypatch.setattr(_main, "fetch_feed", fake_fetch)
        monkeypatch.setattr(_main, "parse_feed", fake_parse)

        asyncio.run(run_backfill(feeds, producer, limit=2, days=None))

        assert len(producer.records) == 2
        assert producer.flush_calls == 1

    def test_days_filter_skips_old_articles(self, monkeypatch):
        feeds = [FeedConfig(name="Feed A", url="https://example.com/a")]
        producer = self._FakeProducer()

        async def fake_fetch(session, url):
            return "xml"

        def fake_parse(raw_xml, feed):
            return [
                {
                    "id": "new",
                    "title": "new",
                    "content": "",
                    "url": "https://example.com/new",
                    "source": feed.name,
                    "should_alert": True,
                    "published_ts": 999_000,
                },
                {
                    "id": "old",
                    "title": "old",
                    "content": "",
                    "url": "https://example.com/old",
                    "source": feed.name,
                    "should_alert": True,
                    "published_ts": 100,
                },
            ]

        monkeypatch.setattr(_main, "fetch_feed", fake_fetch)
        monkeypatch.setattr(_main, "parse_feed", fake_parse)
        monkeypatch.setattr(_main.time, "time", lambda: 1_000_000)

        asyncio.run(run_backfill(feeds, producer, limit=None, days=1))

        assert len(producer.records) == 1
        assert producer.records[0]["value"]["id"] == "new"
