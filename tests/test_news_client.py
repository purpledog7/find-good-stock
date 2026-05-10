from datetime import datetime
from zoneinfo import ZoneInfo

from src.news_client import (
    NaverNewsClient,
    PageMetadata,
    clean_html,
    default_news_window,
    looks_truncated_text,
    parse_news_item,
    parse_news_items,
)


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


def test_search_recent_news_without_window_returns_latest_items():
    client = NaverNewsClient("client-id", "client-secret", request_sleep_seconds=0)

    def fake_request_news(query, display, start=1):
        return {
            "items": [
                {
                    "title": f"{query} latest 1",
                    "description": "body",
                    "link": "https://example.com/1",
                    "pubDate": "Wed, 06 May 2026 09:00:00 +0900",
                },
                {
                    "title": f"{query} latest 2",
                    "description": "body",
                    "link": "https://example.com/2",
                    "pubDate": "Wed, 06 May 2026 08:00:00 +0900",
                },
                {
                    "title": f"{query} latest 3",
                    "description": "body",
                    "link": "https://example.com/3",
                    "pubDate": "Wed, 06 May 2026 07:00:00 +0900",
                },
            ]
        }

    client.request_news = fake_request_news

    result = client.search_recent_news("alpha", start_dt=None, end_dt=None, display=2)

    assert [item.title for item in result] == ["alpha latest 1", "alpha latest 2"]


def test_search_recent_news_resolves_truncated_title_from_page_metadata():
    client = NaverNewsClient("client-id", "client-secret", request_sleep_seconds=0)

    def fake_request_news(query, display, start=1):
        return {
            "items": [
                {
                    "title": "Short headline...",
                    "description": "Short preview...",
                    "originallink": "https://publisher.example.com/article",
                    "link": "https://n.news.naver.com/article/001/0000000000",
                    "pubDate": "Wed, 06 May 2026 09:00:00 +0900",
                }
            ]
        }

    client.request_news = fake_request_news
    client.fetch_page_metadata = lambda url: PageMetadata(
        title="Short headline with the full publisher title",
        description="Short preview with more context from publisher metadata.",
    )

    result = client.search_recent_news("alpha", start_dt=None, end_dt=None, display=1)

    assert result[0].title == "Short headline with the full publisher title"
    assert result[0].description == "Short preview with more context from publisher metadata."


def test_search_recent_news_multi_enriches_after_dedupe_and_limit():
    client = NaverNewsClient("client-id", "client-secret", request_sleep_seconds=0)
    metadata_calls = []

    def fake_request_news(query, display, start=1):
        return {
            "items": [
                {
                    "title": f"{query} headline...",
                    "description": "preview...",
                    "originallink": f"https://publisher.example.com/{query}",
                    "pubDate": "Wed, 06 May 2026 09:00:00 +0900",
                }
            ]
        }

    def fake_fetch_page_metadata(url):
        metadata_calls.append(url)
        return PageMetadata(title=f"resolved {url}", description="resolved preview")

    client.request_news = fake_request_news
    client.fetch_page_metadata = fake_fetch_page_metadata

    result = client.search_recent_news_multi(
        ["alpha", "beta"],
        start_dt=None,
        end_dt=None,
        display=1,
    )

    assert len(result) == 1
    assert result[0].title.startswith("resolved ")
    assert metadata_calls == ["https://publisher.example.com/alpha"]


def test_search_recent_news_uses_naver_metadata_when_original_metadata_is_empty():
    client = NaverNewsClient("client-id", "client-secret", request_sleep_seconds=0)

    def fake_request_news(query, display, start=1):
        return {
            "items": [
                {
                    "title": "Short headline...",
                    "description": "Short preview...",
                    "originallink": "https://publisher.example.com/article",
                    "link": "https://n.news.naver.com/article/001/0000000000",
                    "pubDate": "Wed, 06 May 2026 09:00:00 +0900",
                }
            ]
        }

    def fake_fetch_page_metadata(url):
        if "publisher" in url:
            return PageMetadata()
        return PageMetadata(
            title="Full headline from Naver page",
            description="Full preview with extra context",
        )

    client.request_news = fake_request_news
    client.fetch_page_metadata = fake_fetch_page_metadata

    result = client.search_recent_news("alpha", start_dt=None, end_dt=None, display=1)

    assert result[0].title == "Full headline from Naver page"
    assert result[0].description == "Full preview with extra context"


def test_looks_truncated_text_detects_common_news_ellipsis():
    assert looks_truncated_text("headline...")
    assert looks_truncated_text("headline..")
    assert looks_truncated_text("headline\u2026")
    assert not looks_truncated_text("complete headline")


def test_parse_news_item_parses_pub_date():
    item = {
        "title": "<b>테스트</b>",
        "description": "내용",
        "originallink": "https://example.com",
        "link": "https://n.news.naver.com/article/001/0000000000",
        "pubDate": "Wed, 06 May 2026 07:00:00 +0900",
    }

    result = parse_news_item(item)

    assert result.title == "테스트"
    assert result.link == "https://example.com"
    assert result.naver_link == "https://n.news.naver.com/article/001/0000000000"
    assert result.pub_date.isoformat() == "2026-05-06T07:00:00+09:00"


def test_parse_news_item_falls_back_to_naver_link_when_original_link_is_missing():
    result = parse_news_item(
        {
            "title": "alpha news",
            "description": "body",
            "link": "https://n.news.naver.com/article/001/0000000001",
            "pubDate": "Wed, 06 May 2026 07:00:00 +0900",
        }
    )

    assert result.link == "https://n.news.naver.com/article/001/0000000001"
    assert result.naver_link == "https://n.news.naver.com/article/001/0000000001"


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
