from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.news_analyzer import collect_news_info, collect_raw_news_info, summarize_news_items
from src.news_client import NewsItem


class FakeNewsClient:
    def search_recent_news(self, query, start_dt, end_dt, display):
        return [
            NewsItem(
                title=f"{query} 유상증자 결정",
                description="전환사채 발행도 검토",
                link="https://example.com/news-1",
                pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
            ),
            NewsItem(
                title=f"{query} 수주 증가",
                description="신규 계약 공시",
                link="https://example.com/news-2",
                pub_date=datetime(2026, 5, 6, 8, tzinfo=ZoneInfo("Asia/Seoul")),
            ),
        ]

    def search_recent_news_multi(self, queries, start_dt, end_dt, display):
        self.queries = queries
        return self.search_recent_news(queries[0], start_dt, end_dt, display)


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


def test_collect_news_info_returns_summary_and_raw_news_rows():
    df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "테스트",
                "news_count": "",
                "news_sentiment": "",
                "news_risk_flags": "",
                "news_titles": "",
                "news_summary": "",
            }
        ]
    )
    start_dt = datetime(2026, 5, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    end_dt = datetime(2026, 5, 6, 9, tzinfo=ZoneInfo("Asia/Seoul"))

    enriched_df, raw_news_df = collect_news_info(
        df,
        FakeNewsClient(),
        start_dt,
        end_dt,
        max_items=50,
    )

    assert enriched_df.columns.tolist().count("news_count") == 1
    assert enriched_df.loc[0, "news_count"] == 2
    assert len(raw_news_df) == 2
    assert raw_news_df.loc[0, "code"] == "000001"
    assert raw_news_df.loc[0, "news_rank"] == 1
    assert "capital_raise" in raw_news_df.loc[0, "keyword_flags"]


def test_collect_raw_news_info_returns_only_raw_news_rows():
    df = pd.DataFrame([{"code": 1.0, "name": "테스트"}])
    start_dt = datetime(2026, 5, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    end_dt = datetime(2026, 5, 6, 9, tzinfo=ZoneInfo("Asia/Seoul"))

    result = collect_raw_news_info(
        df,
        FakeNewsClient(),
        start_dt,
        end_dt,
        max_items=30,
    )

    assert len(result) == 2
    assert result.loc[0, "code"] == "000001"
    assert result.loc[0, "title"].startswith("테스트")


def test_collect_raw_news_info_can_use_enhanced_queries():
    df = pd.DataFrame([{"code": "000001", "name": "테스트"}])
    start_dt = datetime(2026, 5, 5, tzinfo=ZoneInfo("Asia/Seoul"))
    end_dt = datetime(2026, 5, 6, 9, tzinfo=ZoneInfo("Asia/Seoul"))
    client = FakeNewsClient()

    result = collect_raw_news_info(
        df,
        client,
        start_dt,
        end_dt,
        max_items=30,
        enhanced_queries=True,
    )

    assert len(result) == 2
    assert "테스트 주식" in client.queries
    assert "테스트 공시" in client.queries
