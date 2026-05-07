from datetime import datetime
from zoneinfo import ZoneInfo

from src.news_client import clean_html, default_news_window, parse_news_item, parse_news_items


def test_clean_html_removes_tags_and_unescapes_entities():
    assert clean_html("<b>KCC</b> &amp; test") == "KCC & test"


def test_clean_html_handles_none_values():
    assert clean_html(None) == ""


def test_default_news_window_uses_previous_day_16_to_morning_7():
    now = datetime(2026, 5, 6, 7, 30, tzinfo=ZoneInfo("Asia/Seoul"))

    start_dt, end_dt = default_news_window(now)

    assert start_dt.isoformat() == "2026-05-05T16:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-06T07:00:00+09:00"


def test_default_news_window_can_use_run_date():
    start_dt, end_dt = default_news_window(run_date="2026-05-06")

    assert start_dt.isoformat() == "2026-05-05T16:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-06T07:00:00+09:00"


def test_parse_news_item_parses_pub_date():
    item = {
        "title": "<b>테스트</b>",
        "description": "내용",
        "originallink": "https://example.com",
        "pubDate": "Wed, 06 May 2026 07:00:00 +0900",
    }

    result = parse_news_item(item)

    assert result.title == "테스트"
    assert result.pub_date.isoformat() == "2026-05-06T07:00:00+09:00"


def test_parse_news_items_skips_invalid_items():
    items = [
        {
            "title": "정상",
            "description": "내용",
            "link": "https://example.com/ok",
            "pubDate": "Wed, 06 May 2026 07:00:00 +0900",
        },
        {
            "title": "비정상",
            "description": "날짜 없음",
        },
    ]

    result = parse_news_items(items)

    assert len(result) == 1
    assert result[0].title == "정상"
