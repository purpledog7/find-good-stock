from datetime import datetime
from zoneinfo import ZoneInfo

from src.news_analyzer import filter_relevant_stock_news
from src.news_client import NewsItem


def test_filter_relevant_stock_news_removes_token_noise():
    items = [
        NewsItem(
            title="팜스토리 실적 개선",
            description="팜스토리 027710 관련 기사",
            link="https://example.com/027710",
            pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
        ),
        NewsItem(
            title="인스타그램 스토리 화제",
            description="해당 종목과 무관한 기사",
            link="https://example.com/noise",
            pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
        ),
    ]

    result = filter_relevant_stock_news("팜스토리", "027710", items)

    assert len(result) == 1
    assert result[0].title == "팜스토리 실적 개선"
