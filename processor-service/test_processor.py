"""
Unit tests for processor-service.

Run from REPO ROOT (no Kafka needed):
    python3 -m pytest crawler-service/test_crawler.py processor-service/test_processor.py telegram-service/test_format_message.py -q

Or from within the service directory:
    cd processor-service && python3 -m pytest test_processor.py -v
"""

import importlib.util
import sys
import os
import time
import pytest

# ── Absolute-path import of processor-service modules ──────────────────────
# Using importlib.util avoids any sys.path ordering conflict when pytest
# is run from the repo root alongside crawler-service (which also has main.py).

_SVC = os.path.dirname(__file__)  # absolute path to processor-service/


def _load(module_name: str):
    """Load a module from processor-service/ by absolute path."""
    path = os.path.join(_SVC, f"{module_name}.py")
    spec = importlib.util.spec_from_file_location(f"_proc_{module_name}", path)
    assert spec and spec.loader, f"Cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[f"_proc_{module_name}"] = mod
    spec.loader.exec_module(mod)
    return mod


_normalizer = _load("normalizer")
_classifier = _load("classifier")
_main = _load("main")

make_article_id = _normalizer.make_article_id
make_raw_hash = _normalizer.make_raw_hash
normalize = _normalizer.normalize
_truncate_summary = _normalizer._truncate_summary
_timestamp_to_iso_fn = _normalizer._timestamp_to_iso

classify = _classifier.classify
extract_keywords = _classifier.extract_keywords

HashDeduplicator = _main.HashDeduplicator
process_message = _main.process_message


# ─── normalizer: make_article_id ─────────────────────────────────────────────

class TestMakeArticleId:
    def test_stable_for_same_url(self):
        url = "https://vnexpress.net/tin-tuc-123456.html"
        assert make_article_id(url) == make_article_id(url)

    def test_different_urls_give_different_ids(self):
        assert make_article_id("https://example.com/a") != make_article_id("https://example.com/b")

    def test_case_insensitive(self):
        assert make_article_id("https://Example.COM/Path") == make_article_id("https://example.com/path")

    def test_length_is_32_chars(self):
        assert len(make_article_id("https://example.com/article")) == 32

    def test_trailing_whitespace_ignored(self):
        assert make_article_id("https://example.com/ ") == make_article_id("https://example.com/")


# ─── normalizer: make_raw_hash ────────────────────────────────────────────────

class TestMakeRawHash:
    def test_same_inputs_same_hash(self):
        h1 = make_raw_hash("https://example.com/a", "Big news")
        h2 = make_raw_hash("https://example.com/a", "Big news")
        assert h1 == h2

    def test_different_title_different_hash(self):
        h1 = make_raw_hash("https://example.com/a", "Title A")
        h2 = make_raw_hash("https://example.com/a", "Title B")
        assert h1 != h2

    def test_hash_is_64_char_hex(self):
        h = make_raw_hash("https://example.com/a", "Title")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)


# ─── normalizer: _truncate_summary ───────────────────────────────────────────

class TestTruncateSummary:
    def test_short_text_unchanged(self):
        text = "Short summary."
        assert _truncate_summary(text, max_len=300) == text

    def test_long_text_truncated(self):
        result = _truncate_summary("word " * 200, max_len=100)
        assert len(result) <= 105

    def test_empty_returns_empty(self):
        assert _truncate_summary("") == ""

    def test_sentence_boundary_preferred(self):
        text = "Sentence one. Sentence two. " + ("extra " * 50)
        result = _truncate_summary(text, max_len=40)
        assert "…" in result or result.endswith(".")


# ─── normalizer: _timestamp_to_iso ───────────────────────────────────────────

class TestTimestampToIso:
    def test_valid_unix_timestamp(self):
        result = _timestamp_to_iso(1711234567)
        assert result.endswith("Z") and "T" in result and "2024" in result

    def test_none_returns_current_time(self):
        result = _timestamp_to_iso(None)
        assert result.endswith("Z")

    def test_invalid_value_returns_current_time(self):
        assert _timestamp_to_iso("not-a-number").endswith("Z")


# ─── normalizer: normalize() — Fix 2: published_at semantics ─────────────────

class TestNormalize:
    def _raw(self, **kwargs):
        base = {
            "id": "test-id-123",
            "title": "Tin tức hôm nay",
            "content": "Nội dung bài viết chi tiết",
            "url": "https://vnexpress.net/example-123.html",
            "source": "VNExpress",
            "timestamp": 1711234567,  # ingestion time
        }
        base.update(kwargs)
        return base

    def test_schema_keys_present(self):
        result = normalize(self._raw(), category="Tin tức", keywords=[], is_duplicate=False)
        required = {
            "article_id", "source", "title", "summary", "url",
            "keywords", "published_at", "ingested_at", "category", "raw_hash",
            "is_duplicate", "raw_payload",
        }
        assert required.issubset(result.keys())

    def test_article_id_is_stable(self):
        raw = self._raw()
        assert (
            normalize(raw, "X", [], False)["article_id"]
            == normalize(raw, "X", [], False)["article_id"]
        )

    def test_is_duplicate_propagated(self):
        assert normalize(self._raw(), "X", [], True)["is_duplicate"] is True

    def test_raw_payload_preserved(self):
        raw = self._raw()
        result = normalize(raw, "X", [], False)
        assert result["raw_payload"]["url"] == raw["url"]

    def test_keywords_preserved(self):
        raw = self._raw()
        result = normalize(raw, "X", ["chứng khoán", "ngân hàng"], False)
        assert result["keywords"] == ["chứng khoán", "ngân hàng"]

    # Fix 2: published_at from real RSS time
    def test_published_at_uses_published_ts_when_present(self):
        """published_ts (real RSS publish time) must be used for published_at."""
        real_publish_ts = 1700000000  # clearly different from ingestion ts
        ingestion_ts = 1711234567
        raw = self._raw(timestamp=ingestion_ts, published_ts=real_publish_ts)
        result = normalize(raw, "X", [], False)
        expected = _timestamp_to_iso(real_publish_ts)
        assert result["published_at"] == expected
        assert result["_published_at_source"] == "rss"

    def test_published_at_falls_back_to_ingestion_when_no_published_ts(self):
        """Without published_ts, published_at falls back to ingestion timestamp."""
        ingestion_ts = 1711234567
        raw = self._raw(timestamp=ingestion_ts)  # no published_ts key
        result = normalize(raw, "X", [], False)
        expected = _timestamp_to_iso(ingestion_ts)
        assert result["published_at"] == expected
        assert result["_published_at_source"] == "ingestion_fallback"

    def test_published_at_falls_back_when_published_ts_is_none(self):
        """published_ts=None (crawler couldn't parse feed time) → fallback."""
        ingestion_ts = 1711234567
        raw = self._raw(timestamp=ingestion_ts, published_ts=None)
        result = normalize(raw, "X", [], False)
        assert result["_published_at_source"] == "ingestion_fallback"

    def test_published_at_is_iso_format(self):
        result = normalize(self._raw(timestamp=1711234567), "X", [], False)
        assert result["published_at"].endswith("Z") and "T" in result["published_at"]

    def test_missing_content_falls_back_to_title(self):
        raw = self._raw(content="", title="Only title here")
        result = normalize(raw, "X", [], False)
        assert result["summary"] == "Only title here"


# Private helper re-exported for convenience in TestNormalize / TestProcessMessage
def _timestamp_to_iso(ts):
    return _timestamp_to_iso_fn(ts)


# ─── classifier ──────────────────────────────────────────────────────────────

class TestClassifier:
    def test_technology_english(self):
        assert classify("OpenAI releases new ChatGPT update", "") == "Công nghệ"

    def test_technology_vietnamese(self):
        assert classify("Trí tuệ nhân tạo thay đổi thế giới", "") == "Công nghệ"

    def test_economy(self):
        assert classify("VN-Index tăng mạnh trong phiên chiều", "") == "Kinh tế"

    def test_politics(self):
        assert classify("Thủ tướng phát biểu tại cuộc họp quốc hội", "") == "Chính trị"

    def test_sports(self):
        assert classify("Premier League: Manchester City win the title", "") == "Thể thao"

    def test_health(self):
        assert classify("WHO cảnh báo về dịch bệnh mới", "") == "Sức khỏe"

    def test_default_category(self):
        assert classify("Lorem ipsum dolor sit amet consectetur", "") == "Tin tức"

    def test_case_insensitive(self):
        assert classify("Bitcoin Price Surges", "") == "Công nghệ"

    def test_summary_used_too(self):
        assert classify("Tin mới nhất", "chứng khoán Mỹ tăng điểm") == "Kinh tế"


class TestExtractKeywords:
    def test_returns_unique_keywords_in_rule_order(self):
        result = extract_keywords(
            "OpenAI và Bitcoin tăng mạnh",
            "OpenAI tiếp tục dẫn đầu AI, bitcoin được nhắc lại",
        )
        assert result[:2] == ["ai", "openai"]
        assert "bitcoin" in result

    def test_returns_empty_for_no_match(self):
        assert extract_keywords("Lorem ipsum", "dolor sit amet") == []


# ─── HashDeduplicator ─────────────────────────────────────────────────────────

class TestHashDeduplicator:
    def test_first_occurrence_not_duplicate(self):
        assert HashDeduplicator().is_duplicate("hash_abc") is False

    def test_second_occurrence_is_duplicate(self):
        d = HashDeduplicator()
        d.is_duplicate("hash_abc")
        assert d.is_duplicate("hash_abc") is True

    def test_eviction_at_max_size(self):
        d = HashDeduplicator(max_size=3)
        d.is_duplicate("h1")
        d.is_duplicate("h2")
        d.is_duplicate("h3")
        d.is_duplicate("h4")       # evicts h1
        assert d.is_duplicate("h1") is False  # h1 was evicted → not duplicate

    def test_different_hashes_not_duplicate(self):
        d = HashDeduplicator()
        d.is_duplicate("hash_a")
        assert d.is_duplicate("hash_b") is False


# ─── process_message — Fix 3: Direction A dedupe ─────────────────────────────

class TestProcessMessage:
    def _raw(self, url="https://vnexpress.net/vn-index-123.html",
             title="VN-Index tăng mạnh nhờ cổ phiếu ngân hàng"):
        return {
            "id": "raw-001",
            "title": title,
            "content": "Thị trường chứng khoán ghi nhận phiên tăng điểm mạnh...",
            "url": url,
            "source": "VNExpress - Kinh doanh",
            "timestamp": 1711234567,
            "published_ts": 1711230000,  # real RSS time
        }

    def test_full_output_schema(self):
        result = process_message(self._raw(), HashDeduplicator())
        required = {
            "article_id", "source", "title", "summary", "url",
            "keywords", "published_at", "ingested_at", "category", "raw_hash",
            "is_duplicate", "raw_payload",
        }
        assert required.issubset(result.keys())

    def test_article_id_is_deterministic(self):
        r1 = process_message(self._raw(), HashDeduplicator())
        r2 = process_message(self._raw(), HashDeduplicator())
        assert r1["article_id"] == r2["article_id"]

    def test_economy_category_detected(self):
        assert process_message(self._raw(), HashDeduplicator())["category"] == "Kinh tế"

    def test_keywords_extracted(self):
        result = process_message(self._raw(), HashDeduplicator())
        assert "chứng khoán" in result["keywords"]
        assert "ngân hàng" in result["keywords"]

    # Fix 2 via process_message: published_at uses real RSS time
    def test_published_at_from_published_ts(self):
        result = process_message(self._raw(), HashDeduplicator())
        assert result["_published_at_source"] == "rss"
        expected = _timestamp_to_iso(1711230000)
        assert result["published_at"] == expected

    def test_published_at_fallback_when_no_published_ts(self):
        raw = self._raw()
        del raw["published_ts"]
        result = process_message(raw, HashDeduplicator())
        assert result["_published_at_source"] == "ingestion_fallback"

    # Fix 3: Direction A — first message is NOT duplicate
    def test_first_message_not_duplicate(self):
        result = process_message(self._raw(), HashDeduplicator())
        assert result["is_duplicate"] is False

    # Fix 3: Direction A — second identical message IS marked as duplicate
    # (caller must check is_duplicate and DROP before publishing)
    def test_second_identical_message_marked_duplicate(self):
        d = HashDeduplicator()
        process_message(self._raw(), d)
        result = process_message(self._raw(), d)
        assert result["is_duplicate"] is True

    # Fix 3: Direction A — different URL → not duplicate
    def test_different_url_not_duplicate(self):
        d = HashDeduplicator()
        process_message(self._raw(url="https://example.com/a"), d)
        result = process_message(self._raw(url="https://example.com/b"), d)
        assert result["is_duplicate"] is False

    # Fix 3: Direction A — same URL, different title → not duplicate
    def test_same_url_different_title_not_duplicate(self):
        d = HashDeduplicator()
        process_message(self._raw(title="Title A"), d)
        result = process_message(self._raw(title="Title B"), d)
        assert result["is_duplicate"] is False
