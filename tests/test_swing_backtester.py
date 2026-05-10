import pandas as pd
from datetime import datetime, timedelta

from src.swing_backtester import classify_outcome, evaluate_candidate, run_swing_backtest
from tests.test_swing_scanner import VALUE_FIELDS, make_history


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
                **VALUE_FIELDS,
            }
        ]
    )

    result = run_swing_backtest(snapshot_df, history_df, top_n=1, lookback_signals=3)

    assert set(
        [
            "signal_date",
            "code",
            "outcome",
            "outcome_3d",
            "outcome_5d",
            "max_return_3d",
            "max_return_5d",
            "review_date_3d",
            "review_date_5d",
        ]
    ).issubset(result.columns)


def test_evaluate_candidate_tracks_three_and_five_day_windows():
    candidate = pd.Series(
        {
            "date": "2026-05-11",
            "rank": 1,
            "code": "000001",
            "name": "alpha",
            "entry_price": 10_000,
            "review_date": "2026-05-14",
            "review_date_3d": "2026-05-14",
            "review_date_5d": "2026-05-18",
            "swing_score": 80,
            "matched_setups": "average_discount_pullback",
        }
    )
    history_df = pd.DataFrame(
        [
            {"date": "2026-05-11", "code": "000001", "high": 10_200, "low": 9_900},
            {"date": "2026-05-12", "code": "000001", "high": 10_300, "low": 9_850},
            {"date": "2026-05-13", "code": "000001", "high": 10_500, "low": 9_800},
            {"date": "2026-05-14", "code": "000001", "high": 10_600, "low": 9_750},
            {"date": "2026-05-15", "code": "000001", "high": 10_700, "low": 9_700},
            {"date": "2026-05-18", "code": "000001", "high": 11_000, "low": 9_650},
        ]
    )
    history_df["date"] = pd.to_datetime(history_df["date"])

    result = evaluate_candidate(
        candidate,
        history_df,
        ["2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14"],
        ["2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15", "2026-05-18"],
        "2026-05-08",
    )

    assert result["max_return_3d"] == 6.0
    assert result["max_return_5d"] == 10.0
    assert result["outcome_3d"] == "half_take_profit"
    assert result["outcome_5d"] == "full_take_profit"
    assert result["outcome"] == "half_take_profit"
