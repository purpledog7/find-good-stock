import pandas as pd
from datetime import datetime, timedelta

from src.swing_backtester import classify_outcome, run_swing_backtest
from tests.test_swing_scanner import make_history


def test_classify_outcome_prioritizes_profit_targets():
    assert classify_outcome(True, True, True) == "full_take_profit"
    assert classify_outcome(True, False, True) == "half_take_profit"
    assert classify_outcome(False, False, True) == "drawdown_10"
    assert classify_outcome(False, False, False) == "timeout"


def test_run_swing_backtest_returns_expected_columns():
    history_df = make_history()
    extra_rows = []
    start = datetime(2026, 4, 22)
    for index in range(38):
        date = (start + timedelta(days=index)).strftime("%Y-%m-%d")
        extra_rows.append(
            {
                "date": date,
                "code": "000001",
                "market": "KOSPI",
                "open": 10_000,
                "high": 10_700,
                "low": 9_800,
                "close": 10_300,
                "volume": 800_000,
                "trading_value": 8_000_000_000,
                "change_rate": 0,
            }
        )
    history_df = pd.concat([history_df[history_df["code"] == "000001"], pd.DataFrame(extra_rows)])
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

    result = run_swing_backtest(snapshot_df, history_df, top_n=1, lookback_signals=3)

    assert set(["signal_date", "code", "outcome", "max_return_3d"]).issubset(result.columns)


def test_run_swing_backtest_uses_four_future_rows_for_three_day_review():
    history_df = make_history()
    start = datetime(2026, 4, 22)
    extra_rows = []
    for index in range(8):
        date = (start + timedelta(days=index)).strftime("%Y-%m-%d")
        is_signal_setup_day = index == 3
        is_fourth_future_day = index == 7
        extra_rows.append(
            {
                "date": date,
                "code": "000001",
                "market": "KOSPI",
                "open": 10_000 if not is_signal_setup_day else 10_100,
                "high": 11_400 if is_fourth_future_day else 10_706 if is_signal_setup_day else 10_200,
                "low": 9_900 if not is_signal_setup_day else 10_282,
                "close": 10_600 if is_signal_setup_day else 10_000,
                "volume": 800_000,
                "trading_value": 8_000_000_000,
                "change_rate": 0,
            }
        )
    history_df = pd.concat([history_df[history_df["code"] == "000001"], pd.DataFrame(extra_rows)])
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

    result = run_swing_backtest(snapshot_df, history_df, top_n=1, lookback_signals=1)

    assert not result.empty
    assert result.iloc[-1]["outcome"] == "full_take_profit"
