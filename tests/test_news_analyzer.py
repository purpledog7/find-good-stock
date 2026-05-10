import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.news_analyzer import (
    NEGATIVE_KEYWORDS,
    POSITIVE_KEYWORDS,
    collect_news_info,
    collect_raw_news_info,
    summarize_news_items,
)
from src.news_client import NewsItem


CAPITAL_RAISE_KEYWORD = next(
    keyword for keyword, flag in NEGATIVE_KEYWORDS.items() if flag == "capital_raise"
)
CONVERTIBLE_BOND_KEYWORD = next(
    keyword for keyword, flag in NEGATIVE_KEYWORDS.items() if flag == "convertible_bond"
)
ORDER_WIN_KEYWORD = next(
    keyword for keyword, flag in POSITIVE_KEYWORDS.items() if flag == "order_win"
)
CONTRACT_KEYWORD = next(
    keyword for keyword, flag in POSITIVE_KEYWORDS.items() if flag == "contract"
)


class FakeNewsClient:
    def search_recent_news(
        self,
        query,
        start_dt,
        end_dt,
        display,
        enrich_metadata=True,
    ):
        return [
            NewsItem(
                title=f"{query} {CAPITAL_RAISE_KEYWORD}",
                description=f"{CONVERTIBLE_BOND_KEYWORD} issue",
                link="https://example.com/news-1",
                pub_date=datetime(2026, 5, 6, 7, tzinfo=ZoneInfo("Asia/Seoul")),
            ),
            NewsItem(
                title=f"{query} {ORDER_WIN_KEYWORD}",
                description=f"{CONTRACT_KEYWORD} disclosure",
                link="https://example.com/news-2",
                pub_date=datetime(2026, 5, 6, 8, tzinfo=ZoneInfo("Asia/Seoul")),
            ),
        ]

    def search_recent_news_multi(
        self,
        queries,
        start_dt,
        end_dt,
        display,
        enrich_metadata=True,
    ):
        self.queries = queries
        self.enrich_metadata = enrich_metadata
        return self.search_recent_news(queries[0], start_dt, end_dt, display)


def test_summarize_news_items_detects_negative_risk_keywords():
    items = [
        NewsItem(
            title=f"test company {CAPITAL_RAISE_KEYWORD}",
            description=f"{CONVERTIBLE_BOND_KEYWORD} issue",
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
    assert result["news_summary"]


def test_collect_news_info_returns_summary_and_raw_news_rows():
    df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
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
    assert "naver_link" in raw_news_df.columns
    assert "description_truncated" in raw_news_df.columns
    assert "capital_raise" in raw_news_df.loc[0, "keyword_flags"]


def test_collect_raw_news_info_returns_only_raw_news_rows():
    df = pd.DataFrame([{"code": 1.0, "name": "alpha"}])
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
    assert result.loc[0, "title"].startswith("alpha")


def test_collect_raw_news_info_can_use_enhanced_queries():
    df = pd.DataFrame([{"code": "000001", "name": "alpha"}])
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
        enrich_metadata=False,
    )

    assert len(result) == 2
    assert "alpha" in client.queries
    assert client.enrich_metadata is False


def test_collect_raw_news_info_stops_when_deadline_is_reached():
    df = pd.DataFrame([{"code": "000001", "name": "alpha"}])

    result = collect_raw_news_info(
        df,
        FakeNewsClient(),
        start_dt=None,
        end_dt=None,
        max_items=30,
        deadline=time.monotonic() - 1,
    )

    assert result.empty
