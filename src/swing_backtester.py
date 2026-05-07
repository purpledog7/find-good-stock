from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import CSV_ENCODING
from src.stock_codes import normalize_stock_code, normalize_stock_code_series
from src.swing_scanner import build_swing_candidates


BACKTEST_COLUMNS = [
    "signal_date",
    "market_date",
    "rank",
    "code",
    "name",
    "entry_price",
    "max_return_3d",
    "min_return_3d",
    "hit_half_take_profit",
    "hit_full_take_profit",
    "hit_drawdown_10",
    "outcome",
    "swing_score",
    "matched_setups",
]


def run_swing_backtest(
    snapshot_df: pd.DataFrame,
    history_df: pd.DataFrame,
    top_n: int = 10,
    lookback_signals: int = 20,
) -> pd.DataFrame:
    if snapshot_df.empty or history_df.empty:
        return pd.DataFrame(columns=BACKTEST_COLUMNS)

    prepared = history_df.copy()
    prepared["date"] = pd.to_datetime(prepared["date"])
    trading_dates = sorted(prepared["date"].dt.strftime("%Y-%m-%d").unique())
    if len(trading_dates) < 25:
        return pd.DataFrame(columns=BACKTEST_COLUMNS)

    signal_indices = list(range(20, len(trading_dates) - 4))
    signal_indices = signal_indices[-lookback_signals:]
    rows: list[dict] = []

    for index in signal_indices:
        market_date = trading_dates[index]
        signal_date = trading_dates[index + 1]
        review_date = trading_dates[index + 4]
        history_until_date = prepared[prepared["date"] <= pd.Timestamp(market_date)].copy()
        candidates = build_swing_candidates(
            snapshot_df=snapshot_df,
            history_df=history_until_date,
            signal_date=signal_date,
            market_date=market_date,
            top_n=top_n,
            review_date=review_date,
        )
        for _, candidate in candidates.iterrows():
            rows.append(evaluate_candidate(candidate, prepared, trading_dates[index + 1 : index + 5], market_date))

    if not rows:
        return pd.DataFrame(columns=BACKTEST_COLUMNS)
    return pd.DataFrame(rows)[BACKTEST_COLUMNS]


def evaluate_candidate(
    candidate: pd.Series,
    history_df: pd.DataFrame,
    future_dates: list[str],
    market_date: str,
) -> dict:
    code = normalize_stock_code(candidate["code"])
    entry_price = float(candidate["entry_price"])
    future = history_df[
        (normalize_stock_code_series(history_df["code"]) == code)
        & (history_df["date"].dt.strftime("%Y-%m-%d").isin(future_dates))
    ].sort_values("date")

    if future.empty or entry_price <= 0:
        max_return = 0.0
        min_return = 0.0
    else:
        max_return = ((pd.to_numeric(future["high"], errors="coerce").max() / entry_price) - 1) * 100
        min_return = ((pd.to_numeric(future["low"], errors="coerce").min() / entry_price) - 1) * 100

    hit_half = bool(max_return >= 4)
    hit_full = bool(max_return >= 7)
    hit_drawdown = bool(min_return <= -10)
    outcome = classify_outcome(hit_half, hit_full, hit_drawdown)

    return {
        "signal_date": candidate["date"],
        "market_date": market_date,
        "rank": candidate["rank"],
        "code": code,
        "name": candidate["name"],
        "entry_price": candidate["entry_price"],
        "max_return_3d": round(max_return, 2),
        "min_return_3d": round(min_return, 2),
        "hit_half_take_profit": hit_half,
        "hit_full_take_profit": hit_full,
        "hit_drawdown_10": hit_drawdown,
        "outcome": outcome,
        "swing_score": candidate["swing_score"],
        "matched_setups": candidate["matched_setups"],
    }


def classify_outcome(hit_half: bool, hit_full: bool, hit_drawdown: bool) -> str:
    if hit_full:
        return "full_take_profit"
    if hit_half:
        return "half_take_profit"
    if hit_drawdown:
        return "drawdown_10"
    return "timeout"


def save_swing_backtest(
    backtest_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_backtest.csv"
    backtest_df.to_csv(path, index=False, encoding=CSV_ENCODING)
    return path
