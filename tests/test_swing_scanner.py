from datetime import datetime, timedelta

import pytest
import pandas as pd

from src.swing_scanner import (
    SWING_CANDIDATE_COLUMNS,
    build_swing_candidates,
    calculate_bb_squeeze_score,
    calculate_anchored_vwap_score,
    calculate_average_discount_score,
    calculate_ema_trend_score,
    calculate_pocket_pivot_score,
    calculate_rsi_score,
    calculate_value_score,
    get_krx_tick_size,
    round_to_tick,
)


def make_history_row(code, date, close, volume, trading_value):
    return {
        "date": date,
        "code": code,
        "market": "KOSPI",
        "open": close * 0.98,
        "high": close * 1.01,
        "low": close * 0.97,
        "close": close,
        "volume": volume,
        "trading_value": trading_value,
        "change_rate": 0,
    }


def make_history():
    start = datetime(2026, 4, 1)
    rows = []
    for index in range(21):
        date = (start + timedelta(days=index)).strftime("%Y-%m-%d")
        if index < 20:
            rows.append(make_history_row("000001", date, 10_000, 250_000, 2_500_000_000))
            rows.append(make_history_row("000002", date, 10_000, 250_000, 2_500_000_000))
        else:
            rows.append(make_history_row("000001", date, 10_600, 800_000, 8_000_000_000))
            rows.append(make_history_row("000002", date, 10_600, 800_000, 8_000_000_000))
    return pd.DataFrame(rows)


VALUE_FIELDS = {
    "per": 8.0,
    "pbr": 0.9,
    "eps": 1200,
    "bps": 12_000,
    "estimated_roe": 10.0,
}


def test_build_swing_candidates_scores_breakout_and_trade_plan():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
            {
                "code": "000002",
                "name": "small",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 40_000_000_000,
                **VALUE_FIELDS,
            },
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history(),
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]
    assert result.columns.tolist() == SWING_CANDIDATE_COLUMNS
    assert result.loc[0, "rank"] == 1
    assert "event_pivot" in result.loc[0, "matched_setups"]
    assert result.loc[0, "tick_size"] == 10
    assert result.loc[0, "entry_price"] == 10_600
    assert result.loc[0, "add_price_1"] == 10_170
    assert result.loc[0, "add_price_2"] == 9_750
    assert result.loc[0, "add_price_3"] == 9_540
    assert result.loc[0, "half_take_profit_price"] == 11_030
    assert result.loc[0, "full_take_profit_price"] == 11_350
    assert result.loc[0, "review_date"] == "2026-05-12"
    assert result.loc[0, "review_date_3d"] == "2026-05-12"
    assert result.loc[0, "review_date_5d"] == "2026-05-14"
    assert result.loc[0, "event_pivot_score"] > 0
    assert result.loc[0, "volume_breakout_score"] > 0
    assert "relative_return_5d" in result.columns
    assert "risk_penalty" in result.columns
    assert result.loc[0, "value_score"] > 0
    assert "value_score" in result.columns
    assert "pocket_pivot_score" in result.columns
    assert "bb_squeeze_score" in result.columns
    assert "anchored_vwap_score" in result.columns
    assert "price_vs_avwap_pct" in result.columns
    assert "accumulation_score" in result.columns
    assert "undervaluation_score" in result.columns
    assert "value_trap_penalty" in result.columns
    assert "rsi14" in result.columns
    assert "ema_trend_score" in result.columns
    assert "ema20_extension_pct" in result.columns
    assert "ema50_extension_pct" in result.columns
    assert "average_discount_score" in result.columns
    assert "price_vs_vwap20_pct" in result.columns


def test_build_swing_candidates_requires_value_anchor():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "expensive",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 80.0,
                "pbr": 8.0,
                "eps": 100,
                "bps": 1_000,
                "estimated_roe": 2.0,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result.empty


def test_build_swing_candidates_does_not_use_roe_alone_as_value_anchor():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "quality-expensive",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 40.0,
                "pbr": 4.0,
                "eps": 1000,
                "bps": 5_000,
                "estimated_roe": 20.0,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result.empty


def test_value_score_rewards_low_per_low_pbr_and_roe():
    df = pd.DataFrame(
        [
            {"per": 6, "pbr": 0.7, "estimated_roe": 14, "eps": 1000, "bps": 10_000},
            {"per": 30, "pbr": 4.0, "estimated_roe": 2, "eps": -100, "bps": 10_000},
        ]
    )

    result = calculate_value_score(df)

    assert result.iloc[0] > result.iloc[1]
    assert result.iloc[0] >= 20


def test_average_discount_score_rewards_price_below_average_not_far_above():
    df = pd.DataFrame(
        [
            {
                "price_vs_ma20_pct": -4.0,
                "price_vs_ma50_pct": -7.0,
                "price_vs_vwap20_pct": -5.0,
                "price_vs_vwap50_pct": -8.0,
                "close_position_in_range": 65,
                "rsi14": 52,
                "return_1d": 1.0,
                "return_20d": -6.0,
            },
            {
                "price_vs_ma20_pct": 8.0,
                "price_vs_ma50_pct": 10.0,
                "price_vs_vwap20_pct": 7.0,
                "price_vs_vwap50_pct": 9.0,
                "close_position_in_range": 70,
                "rsi14": 62,
                "return_1d": 3.0,
                "return_5d": 8.0,
                "return_20d": 18.0,
            },
            {
                "price_vs_ma20_pct": -9.0,
                "price_vs_ma50_pct": -12.0,
                "price_vs_vwap20_pct": -10.0,
                "price_vs_vwap50_pct": -13.0,
                "close_position_in_range": 5,
                "rsi14": 32,
                "return_1d": -3.0,
                "return_5d": -16.0,
                "return_20d": -14.0,
            },
        ]
    )

    result = calculate_average_discount_score(df)

    assert result.iloc[0] > result.iloc[1]
    assert result.iloc[0] >= 8
    assert result.iloc[0] > result.iloc[2]


def test_build_swing_candidates_prioritizes_price_below_average_when_value_matches():
    start = datetime(2026, 3, 1)
    rows = []
    for index in range(60):
        date = (start + timedelta(days=index)).strftime("%Y-%m-%d")
        discount_close = 11_000 if index < 35 else 10_200
        premium_close = 10_000 if index < 55 else 10_200
        if index >= 55:
            discount_close = [10_000, 10_100, 10_200, 10_300, 10_500][index - 55]
            premium_close = [10_100, 10_200, 10_300, 10_400, 10_500][index - 55]
        rows.append(make_history_row("000001", date, discount_close, 300_000, discount_close * 300_000))
        rows.append(make_history_row("000002", date, premium_close, 300_000, premium_close * 300_000))
    history = pd.DataFrame(rows)
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "discounted",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
            {
                "code": "000002",
                "name": "premium",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=2,
    )

    assert result["code"].iloc[0] == "000001"
    assert result.loc[0, "average_discount_score"] > result.loc[1, "average_discount_score"]
    assert "average_discount" in result.loc[0, "setup_tags"]


def test_build_swing_candidates_prefers_peer_discount_when_setups_match():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "cheap",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 6.0,
                "pbr": 0.6,
                "eps": 1600,
                "bps": 16_000,
                "estimated_roe": 10.0,
            },
            {
                "code": "000002",
                "name": "less-cheap",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 10.0,
                "pbr": 0.95,
                "eps": 1200,
                "bps": 12_000,
                "estimated_roe": 10.0,
            },
            {
                "code": "000003",
                "name": "peer-anchor",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 16.0,
                "pbr": 1.5,
                "eps": 1000,
                "bps": 10_000,
                "estimated_roe": 10.0,
            },
        ]
    )
    history = make_history()
    history_000003 = history[history["code"] == "000001"].copy()
    history_000003["code"] = "000003"
    history = pd.concat([history, history_000003], ignore_index=True)

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=5,
    )

    assert result["code"].iloc[0] == "000001"
    assert result.loc[0, "undervaluation_score"] > result.loc[1, "undervaluation_score"]
    assert "sector_discount" in result.loc[0, "setup_tags"]


def test_build_swing_candidates_excludes_value_trap_with_negative_earnings():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "cheap-trap",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                "per": 4.0,
                "pbr": 0.4,
                "eps": -100,
                "bps": 12_000,
                "estimated_roe": -1.0,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=5,
    )

    assert result.empty


def test_pocket_pivot_score_requires_volume_above_recent_down_volume():
    df = pd.DataFrame(
        [
            {
                "pocket_pivot_volume_ratio": 1.4,
                "return_1d": 3.0,
                "price": 10_500,
                "ma10": 10_200,
                "ma20": 10_000,
                "close_position_in_range": 75,
            },
            {
                "pocket_pivot_volume_ratio": 0.8,
                "return_1d": 3.0,
                "price": 10_500,
                "ma10": 10_200,
                "ma20": 10_000,
                "close_position_in_range": 75,
            },
        ]
    )

    result = calculate_pocket_pivot_score(df)

    assert result.iloc[0] > 0
    assert result.iloc[1] == 0


def test_bb_squeeze_score_requires_tight_band_and_near_ma20():
    df = pd.DataFrame(
        [
            {
                "bb_width_percentile_60": 20,
                "bb_width_pct": 8,
                "price": 10_200,
                "ma20": 10_000,
                "close_position_in_range": 70,
                "trading_value_ratio_20d": 1.5,
            },
            {
                "bb_width_percentile_60": 80,
                "bb_width_pct": 30,
                "price": 10_200,
                "ma20": 10_000,
                "close_position_in_range": 70,
                "trading_value_ratio_20d": 1.5,
            },
        ]
    )

    result = calculate_bb_squeeze_score(df)

    assert result.iloc[0] > 0
    assert result.iloc[1] == 0


def test_rsi_and_ema_scores_reward_constructive_trend_not_overbought():
    df = pd.DataFrame(
        [
            {
                "rsi14": 58,
                "price": 10_500,
                "ema10": 10_300,
                "ema20": 10_100,
                "ema50": 9_900,
                "return_5d": 6,
            },
            {
                "rsi14": 82,
                "price": 10_500,
                "ema10": 10_000,
                "ema20": 10_200,
                "ema50": 10_300,
                "return_5d": 28,
            },
        ]
    )

    assert calculate_rsi_score(df).tolist() == [8.0, 0.0]
    assert calculate_ema_trend_score(df).iloc[0] > calculate_ema_trend_score(df).iloc[1]


def test_anchored_vwap_score_rewards_support_near_recent_low_anchor():
    df = pd.DataFrame(
        [
            {
                "price_vs_avwap_pct": 2.0,
                "price": 10_200,
                "ma20": 10_000,
                "close_position_in_range": 70,
                "return_5d": 5,
            },
            {
                "price_vs_avwap_pct": 18.0,
                "price": 11_800,
                "ma20": 10_000,
                "close_position_in_range": 70,
                "return_5d": 30,
            },
        ]
    )

    result = calculate_anchored_vwap_score(df)

    assert result.iloc[0] > 0
    assert result.iloc[1] == 0


def test_build_swing_candidates_excludes_overheated_single_day_move():
    history = make_history()
    history.loc[history["code"] == "000001", "close"] = 10_000
    latest_index = history[history["code"] == "000001"].index[-1]
    history.loc[latest_index, "close"] = 12_000
    history.loc[latest_index, "high"] = 12_100
    history.loc[latest_index, "low"] = 11_500

    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "hot",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result.empty


def test_build_swing_candidates_excludes_overextended_chase_setup():
    history = make_history()
    overheated_indexes = history[history["code"] == "000002"].tail(5).index
    for index, close in zip(overheated_indexes, [10_400, 11_000, 11_600, 12_100, 12_400]):
        history.loc[index, "open"] = close * 0.98
        history.loc[index, "high"] = close * 1.01
        history.loc[index, "low"] = close * 0.97
        history.loc[index, "close"] = close
        history.loc[index, "volume"] = 900_000
        history.loc[index, "trading_value"] = 9_000_000_000

    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "normal",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
            {
                "code": "000002",
                "name": "too_hot",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]


def test_build_swing_candidates_requires_enough_history_for_20d_metrics():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "short",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )
    short_history = make_history()[lambda df: df["code"] == "000001"].head(20)

    result = build_swing_candidates(
        snapshot_df,
        short_history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result.empty


def test_build_swing_candidates_handles_string_numeric_history_values():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )
    history = make_history()[lambda df: df["code"] == "000001"].copy()
    for column in ["open", "high", "low", "close", "volume", "trading_value"]:
        history[column] = history[column].astype(str)

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]
    assert result.loc[0, "entry_price"] == 10_600


def test_build_swing_candidates_ignores_zero_price_rows_from_non_trading_days():
    history = make_history()[lambda df: df["code"] == "000001"].copy()
    zero_rows = history.tail(3).copy()
    zero_rows[["open", "high", "low", "close"]] = 0
    zero_rows["date"] = pd.to_datetime(zero_rows["date"]) + pd.Timedelta(days=30)
    zero_rows["date"] = zero_rows["date"].dt.strftime("%Y-%m-%d")
    history = pd.concat([history, zero_rows], ignore_index=True)
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]
    assert result.loc[0, "return_5d"] > -100


def test_build_swing_candidates_handles_mixed_type_history_codes():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )
    history = make_history()[lambda df: df["code"] == "000001"].copy()
    history.loc[history.index[:10], "code"] = 1
    history.loc[history.index[10:], "code"] = "000001"

    result = build_swing_candidates(
        snapshot_df,
        history,
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]


def test_build_swing_candidates_requires_at_least_one_matched_setup():
    history = make_history()
    history.loc[:, "open"] = 10_000
    history.loc[:, "high"] = 10_100
    history.loc[:, "low"] = 9_900
    history.loc[:, "close"] = 10_000
    history.loc[:, "volume"] = 400_000
    history.loc[:, "trading_value"] = 4_000_000_000
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "flat",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        history[history["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result.empty


def test_build_swing_candidates_parses_string_false_exclude_swing_as_false():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": 1,
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
                "exclude_swing": "false",
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]


def test_build_swing_candidates_deduplicates_snapshot_codes():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": 1,
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
            {
                "code": "000001",
                "name": "alpha-duplicate",
                "market": "KOSPI",
                "sector": "중복",
                "industry": "중복",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            },
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-07",
        market_date="2026-05-06",
        top_n=30,
    )

    assert result["code"].tolist() == ["000001"]
    assert result.loc[0, "name"] == "alpha"


def test_build_swing_candidates_default_review_date_skips_fixed_holiday():
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 100_000_000_000,
                **VALUE_FIELDS,
            }
        ]
    )

    result = build_swing_candidates(
        snapshot_df,
        make_history()[lambda df: df["code"] == "000001"],
        signal_date="2026-05-01",
        market_date="2026-04-30",
        top_n=30,
    )

    assert result.loc[0, "review_date"] == "2026-05-07"
    assert result.loc[0, "review_date_3d"] == "2026-05-07"
    assert result.loc[0, "review_date_5d"] == "2026-05-11"


def test_krx_tick_size_uses_price_bands():
    assert get_krx_tick_size(1_999) == 1
    assert get_krx_tick_size(2_000) == 5
    assert get_krx_tick_size(5_000) == 10
    assert get_krx_tick_size(20_000) == 50
    assert get_krx_tick_size(50_000) == 100
    assert get_krx_tick_size(200_000) == 500
    assert get_krx_tick_size(500_000) == 1_000


def test_round_to_tick_uses_buy_down_sell_up_and_nearest_entry():
    prices = pd.Series([10_176, 11_020.1, 10_604])

    assert round_to_tick(prices, mode="down").tolist() == [10_170, 11_020, 10_600]
    assert round_to_tick(prices, mode="up").tolist() == [10_180, 11_030, 10_610]
    assert round_to_tick(prices, mode="nearest").tolist() == [10_180, 11_020, 10_600]

    with pytest.raises(ValueError):
        round_to_tick(prices, mode="bad")
