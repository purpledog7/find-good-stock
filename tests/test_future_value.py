from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from src.future_value import (
    analyze_future_value_news,
    build_future_value_news_window,
    build_future_value_universe,
    build_future_value_news_queries,
    is_excluded_future_value_name,
    match_themes_in_text,
    calculate_future_value_score,
    score_future_value_news_candidates,
    select_future_value_candidates,
)


def test_build_future_value_universe_filters_kosdaq_low_price_and_excludes_spac():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "AlphaTech",
                "market": "KOSDAQ",
                "price": 3_000,
                "market_cap": 80_000_000_000,
                "sector": "벤처기업부",
                "industry": "소프트웨어 개발 및 공급업",
            },
            {
                "code": "000002",
                "name": "HighPrice",
                "market": "KOSDAQ",
                "price": 5_500,
                "market_cap": 90_000_000_000,
                "sector": "벤처기업부",
                "industry": "로봇 제조업",
            },
            {
                "code": "000003",
                "name": "KospiRobot",
                "market": "KOSPI",
                "price": 2_000,
                "market_cap": 100_000_000_000,
                "sector": "일반",
                "industry": "로봇 제조업",
            },
            {
                "code": "000004",
                "name": "Beta스팩",
                "market": "KOSDAQ",
                "price": 2_000,
                "market_cap": 50_000_000_000,
                "sector": "스팩",
                "industry": "기업인수목적",
            },
        ]
    )

    result = build_future_value_universe(snapshot_df, "2026-05-11", max_price=5_000)
    eligible = result[result["future_value_eligible"]]

    assert eligible["code"].tolist() == ["000001"]
    assert "IT/software" in eligible.loc[eligible.index[0], "theme_categories"]
    assert eligible.loc[eligible.index[0], "naver_finance_url"].endswith("code=000001")
    assert "AlphaTech IR" in eligible.loc[eligible.index[0], "research_queries"]
    assert result.set_index("code").loc["000002", "filter_reason"] == "price_gt_max"
    assert "excluded_spac_or_shell" in result.set_index("code").loc["000004", "filter_reason"]


def test_score_future_value_news_candidates_adds_news_theme_evidence():
    candidates_df = pd.DataFrame(
        [
            {
                "date": "2026-05-11",
                "market_date": "2026-05-11",
                "rank": 1,
                "code": "000001",
                "name": "Alpha",
                "market": "KOSDAQ",
                "price": 3_000,
                "market_cap": 80_000_000_000,
                "sector": "",
                "industry": "기타 제조업",
                "future_value_eligible": True,
                "filter_reason": "pass",
                "theme_categories": "",
                "theme_evidence": "",
                "static_theme_categories": "",
                "static_theme_evidence": "",
            }
        ]
    )
    end_dt = datetime(2026, 5, 11, 12, tzinfo=ZoneInfo("Asia/Seoul"))
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "news_rank": 1,
                "title": "Alpha AI robot platform contract",
                "description": "Alpha expands automation and data center software",
                "link": "https://example.com/a",
                "naver_link": "https://n.news.naver.com/a",
                "description_truncated": False,
                "pub_date": end_dt.isoformat(),
                "keyword_flags": "",
            }
        ]
    )

    scored = score_future_value_news_candidates(
        candidates_df,
        raw_news_df,
        analysis_start_dt=datetime(2026, 2, 11, tzinfo=ZoneInfo("Asia/Seoul")),
        analysis_end_dt=end_dt,
    )
    selected = select_future_value_candidates(scored)

    assert selected["code"].tolist() == ["000001"]
    assert "AI/data_center" in selected.loc[0, "theme_categories"]
    assert "robot/automation" in selected.loc[0, "theme_categories"]
    assert selected.loc[0, "theme_news_count"] == 1
    assert "Alpha AI robot platform contract" in selected.loc[0, "key_news_titles"]


def test_analyze_future_value_news_ignores_unrelated_company_name():
    end_dt = datetime(2026, 5, 11, 12, tzinfo=ZoneInfo("Asia/Seoul"))
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "news_rank": 1,
                "title": "Other AI robot news",
                "description": "No target company mention",
                "link": "https://example.com/a",
                "naver_link": "",
                "description_truncated": False,
                "pub_date": end_dt.isoformat(),
                "keyword_flags": "",
            }
        ]
    )

    result = analyze_future_value_news(
        raw_news_df,
        analysis_start_dt=datetime(2026, 2, 11, tzinfo=ZoneInfo("Asia/Seoul")),
        analysis_end_dt=end_dt,
    ).set_index("code")

    assert result.loc["000001", "relevant_news_count"] == 0
    assert result.loc["000001", "theme_news_count"] == 0


def test_build_future_value_news_window_uses_lookback_and_caps_future():
    start_dt, end_dt = build_future_value_news_window(
        "2026-05-11",
        lookback_days=3,
        now=datetime(2026, 5, 10, 15, 0, tzinfo=ZoneInfo("Asia/Seoul")),
    )

    assert start_dt.isoformat() == "2026-05-08T00:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-10T15:00:00+09:00"


def test_short_ascii_theme_keywords_do_not_match_inside_unrelated_words():
    matches = match_themes_in_text("mobility security space")

    assert "it_software" not in matches
    assert "ai_data_center" not in matches
    assert not is_excluded_future_value_name("Alpha Space")
    assert is_excluded_future_value_name("Alpha SPAC")


def test_theme_matching_tolerates_punctuation_between_words():
    matches = match_themes_in_text("Alpha data-center AI 플랫폼")

    assert "ai_data_center" in matches


def test_build_future_value_news_queries_adds_theme_queries():
    queries = build_future_value_news_queries("Alpha")

    assert queries[0] == "Alpha"
    assert "Alpha AI" in queries
    assert "Alpha 로봇" in queries
    assert "Alpha 양자" in queries


def test_calculate_future_value_score_tolerates_missing_optional_columns():
    result = calculate_future_value_score(pd.DataFrame([{"code": "000001"}]))

    assert result.tolist() == [0.0]
