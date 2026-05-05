from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from config import (
    LOOKBACK_TRADING_DAYS,
    MARKETS,
    REQUEST_SLEEP_SECONDS,
    RETRY_COUNT,
    RETRY_SLEEP_SECONDS,
)

DATE_INPUT_FORMATS = ("%Y%m%d", "%Y-%m-%d")
_stock_api: Any | None = None


def collect_all_stock_data(reference_date: str | None = None) -> tuple[pd.DataFrame, str]:
    market_date = find_latest_market_date(reference_date)
    trading_dates = get_recent_trading_dates(market_date, LOOKBACK_TRADING_DAYS)

    snapshots = [collect_market_snapshot(market, market_date) for market in MARKETS]
    snapshot_df = pd.concat(snapshots, ignore_index=True)

    avg_trading_value_df = collect_average_trading_value_20d(trading_dates)
    result = snapshot_df.merge(avg_trading_value_df, on="code", how="left")
    result["date"] = to_output_date(market_date)

    return result, to_output_date(market_date)


def find_latest_market_date(reference_date: str | None = None) -> str:
    stock_api = require_pykrx()
    current = parse_date(reference_date) if reference_date else datetime.now()

    for offset in range(14):
        date_str = (current - timedelta(days=offset)).strftime("%Y%m%d")
        ohlcv = call_with_retry(stock_api.get_market_ohlcv, date_str, market="KOSPI")
        if not ohlcv.empty:
            return date_str
        time.sleep(REQUEST_SLEEP_SECONDS)

    raise RuntimeError("최근 14일 안에서 거래 데이터가 있는 기준일을 찾지 못했어.")


def get_recent_trading_dates(base_date: str, count: int) -> list[str]:
    stock_api = require_pykrx()
    current = parse_date(base_date)
    dates: list[str] = []

    for offset in range(90):
        if len(dates) >= count:
            break

        date_str = (current - timedelta(days=offset)).strftime("%Y%m%d")
        ohlcv = call_with_retry(stock_api.get_market_ohlcv, date_str, market="KOSPI")
        if not ohlcv.empty:
            dates.append(date_str)
        time.sleep(REQUEST_SLEEP_SECONDS)

    if len(dates) < count:
        raise RuntimeError(f"최근 거래일 {count}개를 찾지 못했어. 찾은 개수: {len(dates)}")

    return dates


def collect_market_snapshot(market: str, date: str) -> pd.DataFrame:
    stock_api = require_pykrx()

    cap_df = normalize_ticker_frame(
        call_with_retry(stock_api.get_market_cap, date, market=market)
    )
    fundamental_df = normalize_ticker_frame(
        call_with_retry(stock_api.get_market_fundamental, date, market=market)
    )

    cap_df = cap_df.rename(columns={"종가": "price", "시가총액": "market_cap"})
    fundamental_df = fundamental_df.rename(
        columns={"PER": "per", "PBR": "pbr", "EPS": "eps", "BPS": "bps"}
    )

    cap_columns = ["code", "price", "market_cap"]
    fundamental_columns = ["code", "per", "pbr", "eps", "bps"]

    merged = cap_df[cap_columns].merge(
        fundamental_df[fundamental_columns],
        on="code",
        how="left",
    )
    merged["name"] = merged["code"].map(stock_api.get_market_ticker_name)
    merged["market"] = market

    numeric_columns = ["price", "market_cap", "per", "pbr", "eps", "bps"]
    merged = coerce_numeric(merged, numeric_columns)
    merged["estimated_roe"] = calculate_estimated_roe(merged["eps"], merged["bps"])

    return merged[
        [
            "code",
            "name",
            "market",
            "price",
            "market_cap",
            "per",
            "pbr",
            "eps",
            "bps",
            "estimated_roe",
        ]
    ]


def collect_average_trading_value_20d(trading_dates: list[str]) -> pd.DataFrame:
    stock_api = require_pykrx()
    frames: list[pd.DataFrame] = []

    for date in trading_dates:
        for market in MARKETS:
            ohlcv = normalize_ticker_frame(
                call_with_retry(stock_api.get_market_ohlcv, date, market=market)
            )
            if ohlcv.empty or "거래대금" not in ohlcv.columns:
                continue

            frame = ohlcv[["code", "거래대금"]].rename(
                columns={"거래대금": "trading_value"}
            )
            frame = coerce_numeric(frame, ["trading_value"])
            frames.append(frame)
            time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame(columns=["code", "avg_trading_value_20d"])

    combined = pd.concat(frames, ignore_index=True)
    return (
        combined.groupby("code", as_index=False)["trading_value"]
        .mean()
        .rename(columns={"trading_value": "avg_trading_value_20d"})
    )


def calculate_estimated_roe(eps: pd.Series, bps: pd.Series) -> pd.Series:
    eps_numeric = pd.to_numeric(eps, errors="coerce")
    bps_numeric = pd.to_numeric(bps, errors="coerce")
    return (eps_numeric / bps_numeric.where(bps_numeric > 0) * 100).round(2)


def normalize_ticker_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["code"])

    normalized = df.copy().reset_index()
    first_column = normalized.columns[0]
    normalized = normalized.rename(columns={first_column: "code", "티커": "code"})
    normalized["code"] = normalized["code"].astype(str).str.zfill(6)
    return normalized


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def call_with_retry(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    last_error: Exception | None = None
    for attempt in range(RETRY_COUNT):
        try:
            return func(*args, **kwargs)
        except Exception as error:  # pragma: no cover - depends on remote service
            last_error = error
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_SLEEP_SECONDS * (attempt + 1))

    raise RuntimeError(f"데이터 수집 중 오류가 반복됐어: {last_error}") from last_error


def parse_date(value: str) -> datetime:
    for date_format in DATE_INPUT_FORMATS:
        try:
            return datetime.strptime(value, date_format)
        except ValueError:
            continue
    raise ValueError("날짜는 YYYY-MM-DD 또는 YYYYMMDD 형식으로 입력해줘.")


def to_output_date(value: str) -> str:
    return parse_date(value).strftime("%Y-%m-%d")


def require_pykrx() -> Any:
    if not os.getenv("KRX_ID") or not os.getenv("KRX_PW"):
        raise RuntimeError(
            "현재 pykrx의 KRX 데이터 조회에는 `KRX_ID`와 `KRX_PW` 환경변수가 필요해. "
            "환경변수를 설정한 뒤 다시 실행해줘."
        )

    global _stock_api
    if _stock_api is None:
        try:
            from pykrx import stock as imported_stock
        except ImportError as error:  # pragma: no cover - dependency absence
            raise RuntimeError(
                "pykrx가 설치되어 있지 않아. 먼저 `pip install -r requirements.txt`를 실행해줘."
            ) from error
        _stock_api = imported_stock

    return _stock_api
