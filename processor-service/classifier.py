"""
classifier.py — Rule-based category classifier for Vietnamese and English news.

Design goals:
- Simple and transparent: easy to understand, extend, and debug
- Ordered rules: first match wins (most specific categories listed first)
- Ready to be replaced by an ML model later without changing the interface

Usage:
    from classifier import classify
    category = classify("VNExpress, stock index drops 2%", "...")
    # → "Kinh tế"
"""

from __future__ import annotations

# ─── Rule Table ───────────────────────────────────────────────────────────────
# Each entry: (category_name, [keyword_list])
# Matching is case-insensitive substring match on (title + " " + summary).
# Order matters — first matching category wins.

RULES: list[tuple[str, list[str]]] = [
    # ── Technology ───────────────────────────────────────────────────────────
    ("Công nghệ", [
        "ai", "trí tuệ nhân tạo", "machine learning", "deep learning",
        "chatgpt", "openai", "gemini", "claude", "llm",
        "blockchain", "crypto", "bitcoin", "ethereum", "tiền số", "web3",
        "phần mềm", "ứng dụng", "app", "startup", "fintech",
        "chip", "semiconductor", "nvidia", "apple", "google", "meta",
        "software", "hardware", "cloud", "data center",
        "cybersecurity", "bảo mật", "mã độc", "ransomware",
        "điện thoại", "smartphone", "iphone", "android",
    ]),

    # ── Economy & Finance ────────────────────────────────────────────────────
    ("Kinh tế", [
        "gdp", "tăng trưởng kinh tế", "lạm phát", "lãi suất", "tỷ giá",
        "chứng khoán", "stock", "vn-index", "cổ phiếu", "thị trường tài chính",
        "ngân hàng", "tín dụng", "đầu tư", "m&a", "ipo",
        "xuất khẩu", "nhập khẩu", "thương mại", "thuế quan", "tariff",
        "doanh nghiệp", "doanh thu", "lợi nhuận", "thua lỗ",
        "kinh doanh", "thị trường", "economy", "recession", "inflation",
        "federal reserve", "world bank", "imf",
    ]),

    # ── Politics & Society ───────────────────────────────────────────────────
    ("Chính trị", [
        "quốc hội", "chính phủ", "thủ tướng", "chủ tịch nước",
        "bộ trưởng", "bộ chính trị", "đại hội đảng",
        "bầu cử", "election", "congress", "senate", "parliament",
        "president", "prime minister", "white house", "kremlin",
        "chính sách", "nghị quyết", "luật", "hiến pháp",
        "ngoại giao", "diplomatic", "UN", "nato", "asean",
        "tổng thống", "biden", "trump", "putin", "xi jinping",
    ]),

    # ── Conflict & Security ──────────────────────────────────────────────────
    ("Xung đột & An ninh", [
        "chiến tranh", "war", "xung đột", "conflict",
        "tên lửa", "missile", "drone", "uav",
        "ukraine", "russia", "gaza", "israel", "hamas",
        "quân đội", "military", "nato", "sanctions", "trừng phạt",
        "khủng bố", "terrorism", "coup", "đảo chính",
    ]),

    # ── Environment & Climate ────────────────────────────────────────────────
    ("Môi trường", [
        "biến đổi khí hậu", "climate change", "global warming",
        "năng lượng tái tạo", "solar", "wind energy", "điện gió",
        "carbon", "phát thải", "emissions", "cop",
        "bão", "lũ lụt", "hạn hán", "động đất",
        "môi trường", "environment", "ô nhiễm", "pollution",
    ]),

    # ── Health & Medicine ────────────────────────────────────────────────────
    ("Sức khỏe", [
        "sức khỏe", "bệnh viện", "y tế", "bác sĩ", "bệnh nhân",
        "vaccine", "thuốc", "dược", "điều trị", "phẫu thuật",
        "dịch bệnh", "pandemic", "covid", "mpox",
        "ung thư", "cancer", "tim mạch", "đái tháo đường",
        "who", "fda", "clinical trial", "nghiên cứu y khoa",
    ]),

    # ── Sports ───────────────────────────────────────────────────────────────
    ("Thể thao", [
        "bóng đá", "football", "soccer", "premier league", "la liga",
        "champions league", "world cup", "euro", "sea games", "olympic",
        "bóng rổ", "basketball", "nba", "tennis", "formula 1", "f1",
        "vô địch", "champion", "cầu thủ", "golfer", "athlete",
        "hlv", "coach", "transfer", "chuyển nhượng",
    ]),

    # ── Education & Science ──────────────────────────────────────────────────
    ("Giáo dục & Khoa học", [
        "giáo dục", "trường đại học", "học sinh", "sinh viên",
        "kỳ thi", "học bổng", "nghiên cứu", "khoa học",
        "nasa", "spacex", "vũ trụ", "space", "khám phá",
        "phát minh", "innovation", "công nghệ sinh học", "biotech",
    ]),

    # ── Culture, Entertainment & Travel ─────────────────────────────────────
    ("Văn hóa & Giải trí", [
        "phim", "movie", "âm nhạc", "ca sĩ", "nghệ sĩ",
        "festival", "lễ hội", "du lịch", "travel", "resort",
        "giải trí", "entertainment", "celebrity", "sao", "nổi tiếng",
        "văn học", "book", "triển lãm", "museum", "nghệ thuật",
    ]),
]

DEFAULT_CATEGORY = "Tin tức"


def extract_keywords(title: str, summary: str = "", limit: int = 8) -> list[str]:
    """Return unique matched keywords across all categories.

    This keeps the implementation transparent and deterministic for the demo.
    Keywords are returned in rule-table order, deduplicated, and capped.
    """
    text = (title + " " + summary).lower()
    matches: list[str] = []

    for _, keywords in RULES:
        for kw in keywords:
            if kw in text and kw not in matches:
                matches.append(kw)
                if len(matches) >= limit:
                    return matches

    return matches


def classify(title: str, summary: str = "") -> str:
    """Return a category string for the given title + summary.

    Uses first-match rule evaluation (case-insensitive substring search).
    Falls back to DEFAULT_CATEGORY if no rule matches.
    """
    text = (title + " " + summary).lower()

    for category, keywords in RULES:
        for kw in keywords:
            if kw in text:
                return category

    return DEFAULT_CATEGORY
