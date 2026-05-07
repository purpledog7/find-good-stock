from datetime import datetime, timedelta

import pytest
import pandas as pd

from src.swing_scanner import (
    SWING_CANDIDATE_COLUMNS,
    build_swing_candidates,
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
            },
            {
                "code": "000002",
                "name": "small",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "market_cap": 40_000_000_000,
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
    assert result.loc[0, "event_pivot_score"] > 0
    assert result.loc[0, "volume_breakout_score"] > 0
    assert "relative_return_5d" in result.columns
    assert "risk_penalty" in result.columns


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
            },
            {
                "code": "000001",
                "name": "alpha-duplicate",
                "market": "KOSPI",
                "sector": "중복",
                "industry": "중복",
                "market_cap": 100_000_000_000,
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
