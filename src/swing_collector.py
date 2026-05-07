from __future__ import annotations

import time
from typing import Callable

import pandas as pd

from config import MARKETS, REQUEST_SLEEP_SECONDS, SWING_HISTORY_TRADING_DAYS
from src.collector import (
    call_with_retry,
    coerce_numeric,
    find_latest_market_date,
    get_recent_trading_dates,
    normalize_ticker_frame,
    require_pykrx,
    to_output_date,
)
from src.stock_codes import normalize_stock_code_series


ProgressCallback = Callable[[str], None] | None

OHLCV_COLUMN_MAP = {
    "시가": "open",
    "고가": "high",
    "저가": "low",
    "종가": "close",
    "거래량": "volume",
    "거래대금": "trading_value",
    "등락률": "change_rate",
}

OHLCV_COLUMNS = [
    "date",
    "code",
    "market",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "trading_value",
    "change_rate",
]


def collect_swing_source_data(
    reference_date: str | None = None,
    history_days: int = SWING_HISTORY_TRADING_DAYS,
    progress: ProgressCallback = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str, list[str]]:
    emit_progress(progress, "1/3 스윙 기준 거래일 확인 중...")
    market_date = find_latest_market_date(reference_date, progress)
    run_date = to_output_date(market_date)
    emit_progress(progress, f"스윙 기준 거래일: {run_date}")

    emit_progress(progress, f"2/3 최근 거래일 {history_days}개 확인 중...")
    trading_dates = get_recent_trading_dates(
        market_date,
        history_days,
        progress,
    )

    emit_progress(progress, "3/3 스윙용 시세/시총 데이터 수집 중...")
    snapshot_df = pd.concat(
        [collect_swing_market_snapshot(market, market_date, progress) for market in MARKETS],
        ignore_index=True,
    )
    history_df = collect_ohlcv_history(trading_dates, progress)
    return snapshot_df, history_df, run_date, trading_dates


def collect_swing_market_snapshot(
    market: str,
    date: str,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    stock_api = require_pykrx()
    emit_progress(progress, f"  {market} 스윙용 시총/종목명 수집 중...")
    cap_df = normalize_ticker_frame(
        call_with_retry(stock_api.get_market_cap, date, market=market)
    )
    cap_df = cap_df.rename(columns={"종가": "price", "시가총액": "market_cap"})
    required_columns = ["code", "price", "market_cap"]
    for column in required_columns:
        if column not in cap_df.columns:
            return pd.DataFrame(columns=["code", "name", "market", "price", "market_cap"])

    result = cap_df[required_columns].copy()
    result["name"] = result["code"].map(stock_api.get_market_ticker_name)
    result["market"] = market
    result = coerce_numeric(result, ["price", "market_cap"])
    return result[["code", "name", "market", "price", "market_cap"]]


def collect_ohlcv_history(
    trading_dates: list[str],
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    stock_api = require_pykrx()
    frames: list[pd.DataFrame] = []

    for index, date in enumerate(trading_dates, start=1):
        emit_progress(
            progress,
            f"  OHLCV 수집 중 ({index}/{len(trading_dates)}): {to_output_date(date)}",
        )
        for market in MARKETS:
            raw_df = normalize_ticker_frame(
                call_with_retry(stock_api.get_market_ohlcv, date, market=market)
            )
            frame = normalize_ohlcv_frame(raw_df, date, market)
            if not frame.empty:
                frames.append(frame)
            time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    return pd.concat(frames, ignore_index=True)


def normalize_ohlcv_frame(df: pd.DataFrame, date: str, market: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=OHLCV_COLUMNS)

    result = df.rename(columns=OHLCV_COLUMN_MAP).copy()
    required_columns = ["code", "open", "high", "low", "close", "volume", "trading_value"]
    for column in required_columns:
        if column not in result.columns:
            return pd.DataFrame(columns=OHLCV_COLUMNS)

    if "change_rate" not in result.columns:
        result["change_rate"] = pd.NA

    result = result[required_columns + ["change_rate"]].copy()
    result["code"] = normalize_stock_code_series(result["code"])
    result["date"] = to_output_date(date)
    result["market"] = market
    result = coerce_numeric(
        result,
        ["open", "high", "low", "close", "volume", "trading_value", "change_rate"],
    )
    return result[OHLCV_COLUMNS]


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
