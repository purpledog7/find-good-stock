import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN, AVG_TRADING_VALUE_EOK_COLUMN
from src.criteria import FilterCriteria
from src.profiles import ScanProfile
from src.recommender import build_recommendations, save_raw_news_markdown, scan_profiles


def sample_row(code, per, pbr, roe, market_cap=100_000_000_000, liquidity=2_000_000_000):
    return {
        "date": "2026-05-06",
        "code": code,
        "name": f"name-{code}",
        "market": "KOSPI",
        "sector": "제조업",
        "industry": "테스트 산업",
        "price": 10_000,
        "market_cap": market_cap,
        "per": per,
        "pbr": pbr,
        "eps": 1000,
        "bps": 10_000,
        "estimated_roe": roe,
        AVG_TRADING_VALUE_COLUMN: liquidity,
    }


def test_scan_profiles_adds_profile_rank_and_profile_score():
    df = pd.DataFrame(
        [
            sample_row("000001", per=6, pbr=0.6, roe=12),
            sample_row("000002", per=13, pbr=1.1, roe=12),
        ]
    )
    profiles = [
        ScanProfile(
            name="test",
            description="test",
            criteria=FilterCriteria(max_per=12, max_pbr=1.2, min_estimated_roe=8),
        )
    ]

    result = scan_profiles(df, profiles)

    assert result["code"].tolist() == ["000001"]
    assert result.loc[0, "profile"] == "test"
    assert result.loc[0, "profile_rank"] == 1
    assert result.loc[0, "profile_score"] > 0
    assert result.loc[0, "sector"] == "제조업"
    assert result.loc[0, "market_cap_eok"] == 1000.0
    assert result.loc[0, AVG_TRADING_VALUE_EOK_COLUMN] == 20.0


def test_build_recommendations_rewards_multiple_profile_matches():
    candidates = pd.DataFrame(
        [
            {
                **sample_row("000001", per=6, pbr=0.6, roe=12),
                "profile": "balanced",
                "profile_rank": 1,
                "profile_score": 80.0,
                "market_cap_eok": 1000.0,
                AVG_TRADING_VALUE_EOK_COLUMN: 20.0,
            },
            {
                **sample_row("000001", per=6, pbr=0.6, roe=12),
                "profile": "deep_value",
                "profile_rank": 1,
                "profile_score": 78.0,
                "market_cap_eok": 1000.0,
                AVG_TRADING_VALUE_EOK_COLUMN: 20.0,
            },
            {
                **sample_row("000002", per=5, pbr=0.5, roe=10),
                "profile": "balanced",
                "profile_rank": 2,
                "profile_score": 82.0,
                "market_cap_eok": 1000.0,
                AVG_TRADING_VALUE_EOK_COLUMN: 20.0,
            },
        ]
    )

    merged, recommendations = build_recommendations(candidates, top_n=2)

    first = recommendations.iloc[0]
    assert len(merged) == 2
    assert first["code"] == "000001"
    assert first["profile_count"] == 2
    assert first["sector"] == "제조업"
    assert first["matched_profiles"] == "balanced, deep_value"
    assert "2개 프로필" in first["selected_reason"]


def test_save_raw_news_markdown_groups_news_by_company(tmp_path):
    raw_news_df = pd.DataFrame(
        [
            {
                "code": 1.0,
                "name": "test",
                "news_rank": 1,
                "title": "title",
                "description": "description",
                "link": "https://example.com",
                "pub_date": "2026-05-06T07:00:00+09:00",
                "keyword_flags": "contract",
            }
        ]
    )
    recommendations_df = pd.DataFrame(
        [
            {
                "final_rank": 1,
                "code": 1,
                "name": "test",
                "market": "KOSPI",
                "sector": "제조업",
                "recommendation_score": 90.0,
            }
        ]
    )

    result = save_raw_news_markdown(
        raw_news_df,
        recommendations_df,
        "2026-05-06",
        tmp_path,
        pd.Timestamp("2026-05-05T16:00:00+09:00"),
        pd.Timestamp("2026-05-06T07:00:00+09:00"),
    )

    assert result.name == "2026-05-06_news_raw.md"
    assert result.exists()
    content = result.read_text(encoding="utf-8")
    assert "## 1. test (000001)" in content
    assert "### 1. title" in content
    assert "description" in content


def test_save_raw_news_markdown_uses_fallback_rank_for_invalid_news_rank(tmp_path):
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "test",
                "news_rank": "bad",
                "title": "title",
                "description": "description",
                "link": "https://example.com",
                "pub_date": "2026-05-06T07:00:00+09:00",
                "keyword_flags": "",
            }
        ]
    )
    recommendations_df = pd.DataFrame(
        [
            {
                "final_rank": 1,
                "code": "000001",
                "name": "test",
                "market": "KOSPI",
                "sector": "제조업",
                "recommendation_score": 90.0,
            }
        ]
    )

    result = save_raw_news_markdown(
        raw_news_df,
        recommendations_df,
        "2026-05-06",
        tmp_path,
        pd.Timestamp("2026-05-05T16:00:00+09:00"),
        pd.Timestamp("2026-05-06T07:00:00+09:00"),
    )

    assert "### 1. title" in result.read_text(encoding="utf-8")
