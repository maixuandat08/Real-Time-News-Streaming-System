"""Unit tests for Telegram message formatting."""

import importlib.util
import os
import sys
import types

os.environ.setdefault("BOT_TOKEN", "test-token")
os.environ.setdefault("CHANNEL_ID", "@test_channel")

TENACITY_STUB = types.ModuleType("tenacity")


def _identity_retry(*args, **kwargs):
    def decorator(fn):
        return fn
    return decorator


TENACITY_STUB.retry = _identity_retry
TENACITY_STUB.retry_if_exception_type = lambda *args, **kwargs: None
TENACITY_STUB.stop_after_attempt = lambda *args, **kwargs: None
TENACITY_STUB.wait_exponential = lambda *args, **kwargs: None
TENACITY_STUB.before_sleep_log = lambda *args, **kwargs: None
sys.modules.setdefault("tenacity", TENACITY_STUB)

MODULE_PATH = os.path.join(os.path.dirname(__file__), "main.py")
SPEC = importlib.util.spec_from_file_location("telegram_service_main", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules["telegram_service_main"] = MODULE
SPEC.loader.exec_module(MODULE)

format_message = MODULE.format_message


def test_format_message_with_processed_news_payload():
    article = {
        "article_id": "abc123",
        "title": "OpenAI & Bitcoin <b>update</b>",
        "summary": "OpenAI mở rộng nền tảng AI cho doanh nghiệp.",
        "url": "https://example.com/story?a=1&b=2",
        "source": "VNExpress - Kinh doanh",
        "category": "Công nghệ",
        "keywords": ["openai", "bitcoin", "cloud"],
    }

    message = format_message(article)

    assert "<b>OpenAI &amp; Bitcoin &lt;b&gt;update&lt;/b&gt;</b>" in message
    assert "📂 <i>Công nghệ</i>" in message
    assert "#openai #bitcoin #cloud" in message
    assert 'href="https://example.com/story?a=1&amp;b=2"' in message


def test_format_message_without_keywords_or_category_uses_source():
    article = {
        "title": "Tin mới",
        "content": "Nội dung ngắn",
        "url": "https://example.com/story",
        "source": "VNExpress",
    }

    message = format_message(article)

    assert "📌 <i>VNExpress</i>" in message
    assert "🏷" not in message
