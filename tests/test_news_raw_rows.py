from datetime import datetime
from zoneinfo import ZoneInfo

from src.news_analyzer import build_raw_news_rows, is_truncated_preview
from src.news_client import NewsItem


def test_build_raw_news_rows_keeps_links_and_truncation_flag():
    item = NewsItem(
        title="alpha title",
        description="alpha preview...",
        link="https://publisher.example.com/article",
        pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
        naver_link="https://n.news.naver.com/article/001/0000000000",
    )

    rows = build_raw_news_rows("000001", "alpha", [item])

    assert rows[0]["link"] == "https://publisher.example.com/article"
    assert rows[0]["naver_link"] == "https://n.news.naver.com/article/001/0000000000"
    assert rows[0]["description_truncated"] is True


def test_is_truncated_preview_detects_search_snippet_ellipsis():
    assert is_truncated_preview("short preview...")
    assert is_truncated_preview("short preview\u2026")
    assert not is_truncated_preview("complete preview")
