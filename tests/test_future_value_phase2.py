import pandas as pd

from src.future_value_phase2 import (
    build_phase2_web_queries,
    collect_future_value_phase2_research,
    dedupe_web_rows,
    extract_employee_candidate,
    extract_important_news,
    extract_revenue_candidate,
    parse_korean_money_to_won,
)
from src.news_client import WebSearchItem


class FakeNaverClient:
    def search_web_documents(self, query, display=10, start=1):
        return [
            WebSearchItem(
                title=f"{query} company profile",
                description="사원수 25명, 매출액 120억원 AI 플랫폼 기업",
                link=f"https://example.com/{query}",
            )
        ]


def test_build_phase2_web_queries_targets_employee_revenue_and_news():
    queries = build_phase2_web_queries("Alpha")

    assert "Alpha 사원수" in queries
    assert "Alpha 매출액" in queries
    assert "Alpha 중요 뉴스" in queries


def test_extract_employee_and_revenue_candidates_from_web_rows():
    rows = [
        {
            "title": "Alpha company",
            "description": "직원수 42명, 연매출 55억원",
            "link": "https://example.com/a",
        }
    ]

    employee = extract_employee_candidate(rows)
    revenue = extract_revenue_candidate(rows)

    assert employee["employee_count"] == 42
    assert revenue["revenue_won"] == 5_500_000_000


def test_extract_employee_and_revenue_candidates_with_spacing_and_particles():
    rows = [
        {
            "title": "Alpha profile",
            "description": "직원 수는 1,234명이고 매출액은 1.5조원입니다.",
            "link": "https://example.com/profile",
        }
    ]

    employee = extract_employee_candidate(rows)
    revenue = extract_revenue_candidate(rows)

    assert employee["employee_count"] == 1234
    assert revenue["revenue_won"] == 1_500_000_000_000


def test_extract_important_news_falls_back_to_phase2_web_rows():
    rows = [
        {
            "query": "Alpha update",
            "title": "Alpha signs MOU",
            "description": "AI platform partnership",
            "link": "https://example.com/mou",
        }
    ]

    result = extract_important_news(pd.DataFrame(), rows)

    assert len(result) == 1
    assert result[0]["title"] == "Alpha signs MOU"
    assert result[0]["link"] == "https://example.com/mou"


def test_extract_important_news_uses_web_when_raw_news_has_no_signal():
    news_df = pd.DataFrame(
        [
            {
                "title": "Alpha general update",
                "description": "No concrete company event",
                "link": "https://example.com/general",
                "pub_date": "2026-05-10T09:00:00+09:00",
            }
        ]
    )
    web_rows = [
        {
            "query": "Alpha news",
            "title": "Alpha supply deal",
            "description": "new MOU",
            "link": "https://example.com/mou",
        }
    ]

    result = extract_important_news(news_df, web_rows)

    assert len(result) == 1
    assert result[0]["link"] == "https://example.com/mou"


def test_dedupe_web_rows_keeps_same_link_with_different_snippets():
    rows = [
        {
            "title": "Alpha profile",
            "description": "직원수 20명",
            "link": "https://example.com/profile",
        },
        {
            "title": "Alpha profile",
            "description": "매출액 30억원",
            "link": "https://example.com/profile",
        },
        {
            "title": "Alpha profile",
            "description": "매출액 30억원",
            "link": "https://example.com/profile",
        },
    ]

    result = dedupe_web_rows(rows)

    assert len(result) == 2


def test_parse_korean_money_to_won_handles_common_units():
    assert parse_korean_money_to_won("1.5", "조원") == 1_500_000_000_000
    assert parse_korean_money_to_won("120", "억원") == 12_000_000_000
    assert parse_korean_money_to_won("3,000", "백만원") == 3_000_000_000


def test_collect_future_value_phase2_research_summarizes_candidate():
    candidates_df = pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "name": "Alpha",
                "price": 3000,
                "market_cap_eok": 800,
                "theme_categories": "AI/data_center",
            }
        ]
    )
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "title": "Alpha AI 공급 계약",
                "description": "대형 고객과 계약",
                "link": "https://example.com/news",
                "naver_link": "",
                "pub_date": "2026-05-10T09:00:00+09:00",
            }
        ]
    )

    summary_df, web_df = collect_future_value_phase2_research(
        candidates_df,
        FakeNaverClient(),
        raw_news_df,
        top_n=1,
        web_max_items=1,
    )

    assert len(summary_df) == 1
    assert summary_df.loc[0, "employee_count"] == 25
    assert summary_df.loc[0, "revenue_eok"] == 120.0
    assert summary_df.loc[0, "important_news_count"] == 1
    assert len(web_df) == 8


def test_collect_future_value_phase2_research_prefers_dart_revenue():
    candidates_df = pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "name": "Alpha",
                "price": 3000,
                "market_cap_eok": 800,
                "theme_categories": "AI/data_center",
            }
        ]
    )
    dart_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "dart_bsns_year": "2025",
                "revenue": 99_000_000_000,
            }
        ]
    )

    summary_df, _ = collect_future_value_phase2_research(
        candidates_df,
        FakeNaverClient(),
        pd.DataFrame(),
        top_n=1,
        web_max_items=1,
        dart_df=dart_df,
    )

    assert summary_df.loc[0, "revenue_eok"] == 990.0
    assert summary_df.loc[0, "revenue_source"] == "OpenDART 2025"
    assert "web_revenue_only" not in summary_df.loc[0, "phase2_flags"]
