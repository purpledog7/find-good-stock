from datetime import datetime
from zoneinfo import ZoneInfo

from src.news_analyzer import summarize_news_items
from src.news_client import NewsItem


def test_summarize_news_items_detects_negative_risk_keywords():
    items = [
        NewsItem(
            title="테스트 기업 유상증자 결정",
            description="전환사채 발행도 검토",
            link="https://example.com",
            pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
        )
    ]

    result = summarize_news_items(items)

    assert result["news_count"] == 1
    assert result["news_sentiment"] == "negative"
    assert "capital_raise" in result["news_risk_flags"]
    assert "convertible_bond" in result["news_risk_flags"]


def test_summarize_news_items_handles_no_news():
    result = summarize_news_items([])

    assert result["news_count"] == 0
    assert result["news_sentiment"] == "neutral"
    assert result["news_summary"] == "분석 기간 내 확인된 뉴스 없음."
