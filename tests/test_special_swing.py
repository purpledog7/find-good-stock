from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from config import KST_TIMEZONE, SPECIAL_SWING_NEWS_LOOKBACK_DAYS
from src.special_swing import (
    SPECIAL_SWING_COLUMNS,
    add_day_swing_technical_scores,
    analyze_special_news,
    apply_special_news_analysis,
    build_day_swing_ai_news_window,
    build_day_swing_eligible_mask,
    build_special_ai_news_window,
    build_special_news_analysis_window,
    build_special_stock_news_queries,
    build_special_swing_technical_candidates,
    has_community_setup_signal,
)


def make_history_row(code, date, close, volume=300_000):
    return {
        "date": date,
        "code": code,
        "market": "KOSPI",
        "open": close * 0.995,
        "high": close * 1.01,
        "low": close * 0.99,
        "close": close,
        "volume": volume,
        "trading_value": close * volume,
        "change_rate": 0,
    }


def make_special_history():
    start = datetime(2026, 3, 1)
    rows = []
    closes = (
        [10_000] * 40
        + [10_000, 10_200, 10_400, 10_600, 10_800, 11_000, 10_800, 10_600, 10_500, 10_400]
        + [10_400, 10_350, 10_420, 10_380, 10_450, 10_420, 10_430, 10_410, 10_440, 10_400]
    )
    hot_closes = [10_000] * 55 + [11_000, 11_700, 12_300, 12_900, 13_500]
    for index, close in enumerate(closes):
        date = (start + timedelta(days=index)).strftime("%Y-%m-%d")
        rows.append(make_history_row("000001", date, close, volume=320_000))
        rows.append(make_history_row("000002", date, hot_closes[index], volume=320_000))
    return pd.DataFrame(rows)


def test_build_special_swing_technical_candidates_finds_box_pullback_volume_setup():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "tech",
                "industry": "software",
                "market_cap": 120_000_000_000,
            },
            {
                "code": "000002",
                "name": "hot",
                "market": "KOSPI",
                "sector": "tech",
                "industry": "software",
                "market_cap": 120_000_000_000,
            },
        ]
    )

    result = build_special_swing_technical_candidates(
        snapshot_df,
        make_special_history(),
        signal_date="2026-05-11",
        market_date="2026-05-08",
        top_n=10,
    )

    assert result["code"].tolist() == ["000001"]
    assert result.columns.tolist() == SPECIAL_SWING_COLUMNS
    assert result.loc[0, "box_score"] >= 10
    assert result.loc[0, "pullback_score"] >= 10
    assert result.loc[0, "steady_volume_score"] >= 10
    assert result.loc[0, "five_day_trigger_score"] >= 8
    assert result.loc[0, "community_setup_score"] >= 8
    assert "box_range" in result.loc[0, "matched_conditions"]
    assert "pullback" in result.loc[0, "matched_conditions"]
    assert "volume_dryup" in result.loc[0, "matched_conditions"]
    assert "anchored_vwap_reclaim" in result.loc[0, "matched_conditions"]


def test_analyze_special_news_scores_theme_and_increasing_news_trend():
    end_dt = datetime(2026, 5, 10, 7, 30, tzinfo=ZoneInfo(KST_TIMEZONE))
    start_dt = end_dt - timedelta(days=SPECIAL_SWING_NEWS_LOOKBACK_DAYS - 1)
    raw_news_df = pd.DataFrame(
        [
            news_row("000001", "alpha AI 공급 계약", "인공지능 신규 계약", end_dt, 9, "a"),
            news_row("000001", "alpha 반도체 협력", "HBM 패키징 협력", end_dt, 4, "b"),
            news_row("000001", "alpha AI 수주", "데이터센터 공급", end_dt, 2, "c"),
            news_row("000001", "alpha AI 실적 증가", "영업이익 증가", end_dt, 1, "d"),
            news_row("000001", "alpha AI 추가 계약", "공급 확대", end_dt, 0, "e"),
            news_row("000002", "beta 소송", "관리종목 우려", end_dt, 0, "f", name="beta"),
        ]
    )

    result = analyze_special_news(raw_news_df, start_dt, end_dt).set_index("code")

    assert result.loc["000001", "news_growth_score"] >= 8
    assert result.loc["000001", "theme_score"] > 0
    assert "AI" in result.loc["000001", "theme_hits"]
    assert result.loc["000001", "positive_news_count"] >= 3
    assert result.loc["000002", "negative_news_count"] == 1
    assert result.loc["000001", "news_relevance_score"] > 0


def test_apply_special_news_analysis_returns_only_theme_growth_candidates():
    end_dt = datetime(2026, 5, 10, 7, 30, tzinfo=ZoneInfo(KST_TIMEZONE))
    start_dt = end_dt - timedelta(days=SPECIAL_SWING_NEWS_LOOKBACK_DAYS - 1)
    candidates = pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "name": "alpha",
                "technical_score": 45,
                "five_day_trigger_score": 8,
                "matched_conditions": "box_range, pullback, steady_volume",
                "risk_flags": "",
            },
            {
                "rank": 2,
                "code": "000002",
                "name": "beta",
                "technical_score": 48,
                "five_day_trigger_score": 8,
                "matched_conditions": "box_range, pullback, steady_volume",
                "risk_flags": "",
            },
        ]
    )
    raw_news_df = pd.DataFrame(
        [
            news_row("000001", "alpha AI 공급 계약", "인공지능 신규 계약", end_dt, 4, "a"),
            news_row("000001", "alpha AI 수주", "데이터센터 공급", end_dt, 2, "b"),
            news_row("000001", "alpha AI 실적 증가", "영업이익 증가", end_dt, 1, "c"),
            news_row("000001", "alpha AI 추가 계약", "공급 확대", end_dt, 0, "d"),
            news_row("000002", "beta 일반 기사", "테마 없음", end_dt, 0, "e"),
        ]
    )

    result = apply_special_news_analysis(candidates, raw_news_df, start_dt, end_dt, top_n=10)

    assert result["code"].tolist() == ["000001"]
    assert "news_growth" in result.loc[0, "matched_conditions"]
    assert "theme_news" in result.loc[0, "matched_conditions"]
    assert result.loc[0, "special_swing_score"] > result.loc[0, "technical_score"]


def test_build_special_news_analysis_window_uses_recent_five_calendar_dates_and_caps_future():
    start_dt, end_dt = build_special_news_analysis_window(
        "2026-05-11",
        now=datetime(2026, 5, 9, 15, 0, tzinfo=ZoneInfo(KST_TIMEZONE)),
    )

    assert start_dt.isoformat() == "2026-05-05T00:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-09T15:00:00+09:00"


def test_build_special_ai_news_window_uses_recent_five_calendar_dates():
    start_dt, end_dt = build_special_ai_news_window(
        "2026-05-10",
        now=datetime(2026, 5, 10, 8, 30, tzinfo=ZoneInfo(KST_TIMEZONE)),
    )

    assert start_dt.isoformat() == "2026-05-06T00:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-10T08:00:00+09:00"


def test_build_day_swing_ai_news_window_uses_post_close_to_morning_cutoff():
    start_dt, end_dt = build_day_swing_ai_news_window(
        "2026-05-08",
        "2026-05-11",
        now=datetime(2026, 5, 11, 8, 30, tzinfo=ZoneInfo(KST_TIMEZONE)),
    )

    assert start_dt.isoformat() == "2026-05-08T16:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-11T08:00:00+09:00"


def test_analyze_special_news_filters_ambiguous_company_name_noise():
    end_dt = datetime(2026, 5, 10, 7, 30, tzinfo=ZoneInfo(KST_TIMEZONE))
    start_dt = end_dt - timedelta(days=SPECIAL_SWING_NEWS_LOOKBACK_DAYS - 1)
    raw_news_df = pd.DataFrame(
        [
            news_row("000001", "선진국 AI 투자 확대", "선진국 반도체 지원", end_dt, 1, "a", name="선진"),
            news_row("000001", "선진화 정책 발표", "선진시장 로봇 투자", end_dt, 0, "b", name="선진"),
        ]
    )

    result = analyze_special_news(raw_news_df, start_dt, end_dt).set_index("code")

    assert result.loc["000001", "relevant_news_count_5d"] == 0
    assert result.loc["000001", "noisy_news_count_5d"] == 2
    assert result.loc["000001", "theme_score"] == 0


def test_analyze_special_news_penalizes_one_day_news_spike():
    end_dt = datetime(2026, 5, 10, 7, 30, tzinfo=ZoneInfo(KST_TIMEZONE))
    start_dt = end_dt - timedelta(days=SPECIAL_SWING_NEWS_LOOKBACK_DAYS - 1)
    raw_news_df = pd.DataFrame(
        [
            news_row(
                "000001",
                f"alpha AI 공급계약 {index}",
                "alpha 실적 증가",
                end_dt,
                2,
                str(index),
            )
            for index in range(8)
        ]
    )

    result = analyze_special_news(raw_news_df, start_dt, end_dt).set_index("code")

    assert result.loc["000001", "max_daily_news_share"] == 1.0
    assert result.loc["000001", "news_concentration_penalty"] >= 6
    assert result.loc["000001", "direct_catalyst_score"] > 0


def test_analyze_special_news_scores_primary_fresh_news_and_duplicate_story_penalty():
    end_dt = datetime(2026, 5, 10, 7, 30, tzinfo=ZoneInfo(KST_TIMEZONE))
    start_dt = end_dt - timedelta(days=SPECIAL_SWING_NEWS_LOOKBACK_DAYS - 1)
    raw_news_df = pd.DataFrame(
        [
            news_row("000001", f"alpha AI MOU {index}", "alpha AI MOU", end_dt, index % 2, str(index))
            for index in range(4)
        ]
    )

    result = analyze_special_news(raw_news_df, start_dt, end_dt).set_index("code")

    assert result.loc["000001", "primary_news_count_5d"] == 4
    assert result.loc["000001", "primary_news_score"] > 0
    assert result.loc["000001", "news_freshness_score"] >= 4
    assert result.loc["000001", "duplicate_story_count_5d"] >= 3
    assert result.loc["000001", "duplicate_story_penalty"] >= 5


def test_has_community_setup_signal_accepts_single_strong_modern_setup():
    setups = pd.DataFrame(
        [
            {"community_setup_score": 7.5, "vcp_score": 14},
            {"community_setup_score": 7.5, "avwap_reclaim_score": 12},
            {"community_setup_score": 7.5, "relative_strength_score": 12},
            {"community_setup_score": 7.5},
        ]
    )

    assert has_community_setup_signal(setups).tolist() == [True, True, True, False]


def test_add_day_swing_technical_scores_adds_orb_vwap_rvol_setup_stack():
    result = add_day_swing_technical_scores(
        pd.DataFrame(
            [
                {
                    "trading_value_today": 8_000_000_000,
                    "avg_trading_value_20d": 3_500_000_000,
                    "technical_score": 45,
                    "steady_volume_score": 12,
                    "reclaim_score": 9,
                    "breakout_ready_score": 10,
                    "range_contraction_score": 9,
                    "tight_base_score": 10,
                    "pocket_pivot_score": 9,
                    "relative_strength_score": 12,
                    "accumulation_5d": 2,
                    "return_1d": 2,
                    "return_5d": 3,
                    "rsi14": 56,
                    "day_range_pct": 4,
                    "adr_20d": 6,
                    "volume_ratio_20d": 1.8,
                    "trading_value_ratio_20d": 1.7,
                    "box_position_pct": 78,
                    "close_position_in_range": 72,
                    "close_vs_20d_high_pct": -4,
                    "price_vs_vwap20_pct": 0.8,
                    "price_vs_avwap_pct": 1.2,
                    "price_vs_ma20_pct": 1.0,
                    "ema20_extension_pct": 3,
                }
            ]
        )
    )

    row = result.iloc[0]
    assert row["day_orb_readiness_score"] >= 8
    assert row["day_vwap_reclaim_score"] >= 8
    assert row["day_rvol_score"] >= 8
    assert row["day_setup_score"] >= 40
    assert row["day_technical_score"] >= 50
    assert "orb_ready" in row["matched_conditions"]
    assert "vwap_reclaim" in row["matched_conditions"]
    assert "rvol_confirmed" in row["matched_conditions"]


def test_build_day_swing_eligible_mask_tolerates_missing_exclude_swing():
    mask = build_day_swing_eligible_mask(
        pd.DataFrame(
            [
                {
                    "market_cap": 120_000_000_000,
                    "avg_trading_value_20d": 3_500_000_000,
                    "trading_value_today": 8_000_000_000,
                    "price": 10_000,
                    "return_1d": 2,
                    "return_5d": 3,
                    "return_20d": 8,
                    "rsi14": 56,
                    "day_technical_score": 55,
                    "steady_volume_score": 12,
                    "day_rvol_score": 12,
                    "day_setup_score": 45,
                    "morning_entry_bias_score": 16,
                    "day_orb_readiness_score": 12,
                    "day_vwap_reclaim_score": 12,
                    "breakout_ready_score": 10,
                    "reclaim_score": 9,
                    "pocket_pivot_score": 9,
                    "relative_strength_score": 12,
                    "day_gap_risk_penalty": 4,
                }
            ]
        )
    )

    assert mask.tolist() == [True]


def test_build_special_stock_news_queries_adds_theme_queries():
    queries = build_special_stock_news_queries("alpha")

    assert queries[0] == "alpha"
    assert "alpha AI" in queries
    assert "alpha 반도체" in queries
    assert "alpha 비만치료제" in queries


def news_row(code, title, description, end_dt, days_ago, suffix, name="alpha"):
    pub_dt = end_dt - timedelta(days=days_ago)
    return {
        "code": code,
        "name": name,
        "news_rank": 1,
        "title": title,
        "description": description,
        "link": f"https://example.com/{suffix}",
        "naver_link": f"https://n.news.naver.com/{suffix}",
        "description_truncated": False,
        "pub_date": pub_dt.isoformat(),
        "keyword_flags": "",
    }
