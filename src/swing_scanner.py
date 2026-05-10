from __future__ import annotations

import math

import pandas as pd

from config import SWING_TOP_N
from src.stock_codes import normalize_stock_code, normalize_stock_code_series
from src.trading_calendar import add_trading_days


SWING_CANDIDATE_COLUMNS = [
    "date",
    "market_date",
    "rank",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "price",
    "tick_size",
    "market_cap",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    "earnings_yield",
    "book_discount_pct",
    "sector_per_median",
    "sector_pbr_median",
    "per_vs_sector_pct",
    "pbr_vs_sector_pct",
    "trading_value_today",
    "avg_trading_value_20d",
    "return_1d",
    "return_3d",
    "return_5d",
    "return_20d",
    "market_return_3d",
    "market_return_5d",
    "market_return_20d",
    "market_positive_rate_1d",
    "relative_return_5d",
    "relative_return_20d",
    "volume_ratio_20d",
    "trading_value_ratio_20d",
    "day_range_pct",
    "adr_20d",
    "close_position_in_range",
    "ma5",
    "ma10",
    "ma20",
    "ma50",
    "ema10",
    "ema20",
    "ema50",
    "rsi14",
    "ema20_extension_pct",
    "ema50_extension_pct",
    "vwap20",
    "vwap50",
    "price_vs_ma20_pct",
    "price_vs_ma50_pct",
    "price_vs_vwap20_pct",
    "price_vs_vwap50_pct",
    "low_20d",
    "high_10d",
    "high_20d",
    "high_50d",
    "close_vs_20d_high_pct",
    "bb_width_pct",
    "bb_width_percentile_60",
    "avwap_from_20d_low",
    "price_vs_avwap_pct",
    "accumulation_5d",
    "pocket_pivot_volume_ratio",
    "matched_setups",
    "setup_tags",
    "risk_flags",
    "market_risk_flags",
    "news_risk_flags",
    "event_pivot_score",
    "volume_breakout_score",
    "contraction_score",
    "darvas_breakout_score",
    "pullback_ladder_score",
    "pocket_pivot_score",
    "bb_squeeze_score",
    "anchored_vwap_score",
    "accumulation_score",
    "relative_strength_score",
    "value_score",
    "undervaluation_score",
    "average_discount_score",
    "rsi_score",
    "ema_trend_score",
    "setup_bonus",
    "risk_penalty",
    "value_trap_penalty",
    "news_risk_penalty",
    "swing_score",
    "entry_price",
    "add_price_1",
    "add_price_2",
    "add_price_3",
    "half_take_profit_price",
    "full_take_profit_price",
    "review_date",
    "review_date_3d",
    "review_date_5d",
]

MIN_SWING_MARKET_CAP = 50_000_000_000
MIN_AVG_TRADING_VALUE_20D = 2_000_000_000
MIN_TODAY_TRADING_VALUE = 3_000_000_000
MIN_PRICE = 1_000


def build_swing_candidates(
    snapshot_df: pd.DataFrame,
    history_df: pd.DataFrame,
    signal_date: str,
    market_date: str,
    top_n: int = SWING_TOP_N,
    review_date: str | None = None,
    review_date_5d: str | None = None,
) -> pd.DataFrame:
    if snapshot_df.empty or history_df.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)

    metrics_df = calculate_swing_metrics(history_df)
    if metrics_df.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)
    metrics_df["code"] = normalize_stock_code_series(metrics_df["code"])

    snapshot_columns = [
        "code",
        "name",
        "market",
        "sector",
        "industry",
        "market_cap",
        "per",
        "pbr",
        "eps",
        "bps",
        "estimated_roe",
        "market_risk_flags",
        "exclude_swing",
    ]
    snapshot = ensure_columns(snapshot_df, snapshot_columns)[snapshot_columns].copy()
    snapshot["code"] = normalize_stock_code_series(snapshot["code"])
    snapshot = snapshot[snapshot["code"] != ""].drop_duplicates("code", keep="first")
    result = metrics_df.merge(snapshot, on="code", how="left", suffixes=("", "_snapshot"))
    result["market"] = result["market_snapshot"].fillna(result.get("market", ""))
    result = result.drop(columns=["market_snapshot"], errors="ignore")
    result = result.fillna({"name": "", "market": "", "sector": "", "industry": ""})
    result["market_risk_flags"] = result["market_risk_flags"].fillna("")
    result["news_risk_flags"] = ""
    result["news_risk_penalty"] = 0
    result["exclude_swing"] = result["exclude_swing"].apply(parse_bool_like)
    result = add_value_context_columns(result)

    result = apply_swing_hard_filters(result)
    if result.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)

    result = add_trade_plan_columns(result, signal_date, review_date, review_date_5d)
    result = score_swing_candidates(result)
    result = result[result["matched_setups"].astype(str).str.strip() != ""].copy()
    if result.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)

    result["date"] = signal_date
    result["market_date"] = market_date
    result["average_discount_anchor"] = calculate_average_discount_anchor(result)
    result = result.sort_values(
        by=[
            "average_discount_anchor",
            "average_discount_score",
            "swing_score",
            "undervaluation_score",
            "trading_value_today",
            "return_1d",
        ],
        ascending=[False, False, False, False, False, False],
    ).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)

    return ensure_columns(result.head(top_n).copy(), SWING_CANDIDATE_COLUMNS)[
        SWING_CANDIDATE_COLUMNS
    ]


def calculate_swing_metrics(history_df: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    prepared = history_df.copy()
    if "code" not in prepared.columns:
        return pd.DataFrame()
    prepared["code"] = normalize_stock_code_series(prepared["code"])
    prepared = prepared[prepared["code"] != ""].copy()
    prepared["date"] = pd.to_datetime(prepared["date"])
    numeric_columns = ["open", "high", "low", "close", "volume", "trading_value"]
    for column in numeric_columns:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    prepared = prepared[
        (prepared["open"] > 0)
        & (prepared["high"] > 0)
        & (prepared["low"] > 0)
        & (prepared["close"] > 0)
        & (prepared["volume"] >= 0)
        & (prepared["trading_value"] >= 0)
    ].copy()

    for code, group in prepared.sort_values(["code", "date"]).groupby("code"):
        if len(group) < 21:
            continue
        rows.append(calculate_code_metrics(normalize_stock_code(code), group.reset_index(drop=True)))

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return add_market_regime_columns(result)


def calculate_code_metrics(code: str, group: pd.DataFrame) -> dict:
    latest = group.iloc[-1]
    close = pd.to_numeric(group["close"], errors="coerce")
    high = pd.to_numeric(group["high"], errors="coerce")
    low = pd.to_numeric(group["low"], errors="coerce")
    volume = pd.to_numeric(group["volume"], errors="coerce")
    trading_value = pd.to_numeric(group["trading_value"], errors="coerce")

    latest_close = close.iloc[-1]
    latest_high = high.iloc[-1]
    latest_low = low.iloc[-1]
    latest_volume = volume.iloc[-1]
    latest_trading_value = trading_value.iloc[-1]

    day_range = safe_divide(latest_high - latest_low, latest_close) * 100
    close_position = safe_divide(latest_close - latest_low, latest_high - latest_low) * 100
    avg_volume_20d = volume.tail(20).mean()
    avg_trading_value_20d = trading_value.tail(20).mean()
    high_10d = high.tail(10).max()
    high_20d = high.tail(20).max()
    high_50d = high.tail(50).max()
    low_20d = low.tail(20).min()
    prev_high_20d = high.iloc[:-1].tail(20).max()
    range_pct = safe_divide_series(high - low, close) * 100
    ma20 = close.tail(20).mean()
    ma50 = close.tail(50).mean()
    ema10 = calculate_ema(close, 10)
    ema20 = calculate_ema(close, 20)
    ema50 = calculate_ema(close, 50)
    rsi14 = calculate_rsi(close, 14)
    ema20_extension_pct = (safe_divide(latest_close, ema20) - 1) * 100
    ema50_extension_pct = (safe_divide(latest_close, ema50) - 1) * 100
    vwap20 = calculate_period_vwap(volume, trading_value, 20)
    vwap50 = calculate_period_vwap(volume, trading_value, 50)
    price_vs_ma20_pct = (safe_divide(latest_close, ma20) - 1) * 100
    price_vs_ma50_pct = (safe_divide(latest_close, ma50) - 1) * 100
    price_vs_vwap20_pct = (safe_divide(latest_close, vwap20) - 1) * 100
    price_vs_vwap50_pct = (safe_divide(latest_close, vwap50) - 1) * 100
    bb_width_series = safe_divide_series(
        close.rolling(20).std() * 4,
        close.rolling(20).mean(),
    ) * 100
    bb_width_pct = bb_width_series.iloc[-1]
    bb_width_sample = bb_width_series.dropna().tail(60)
    bb_width_percentile_60 = (
        (bb_width_sample <= bb_width_pct).mean() * 100
        if not bb_width_sample.empty and not pd.isna(bb_width_pct)
        else 100
    )
    avwap_from_20d_low = calculate_anchored_vwap_from_low(close, high, low, volume, 20)
    price_vs_avwap_pct = (safe_divide(latest_close, avwap_from_20d_low) - 1) * 100
    prev_close = close.shift(1)
    highest_down_volume_10d = volume.where(close < prev_close).iloc[:-1].tail(10).max()
    pocket_pivot_volume_ratio = safe_divide(latest_volume, highest_down_volume_10d)
    accumulation_5d = calculate_accumulation_change(close, high, low, volume)

    return {
        "code": code,
        "market": latest.get("market", ""),
        "price": latest_close,
        "trading_value_today": latest_trading_value,
        "volume_today": latest_volume,
        "avg_volume_20d": avg_volume_20d,
        "avg_volume_5d": volume.tail(5).mean(),
        "avg_trading_value_20d": avg_trading_value_20d,
        "return_1d": pct_return(close, 1),
        "return_3d": pct_return(close, 3),
        "return_5d": pct_return(close, 5),
        "return_20d": pct_return(close, 20),
        "volume_ratio_20d": safe_divide(latest_volume, avg_volume_20d),
        "trading_value_ratio_20d": safe_divide(latest_trading_value, avg_trading_value_20d),
        "day_range_pct": day_range,
        "adr_20d": range_pct.tail(20).mean(),
        "avg_range_5d": range_pct.tail(5).mean(),
        "avg_range_20d": range_pct.tail(20).mean(),
        "close_position_in_range": close_position,
        "ma5": close.tail(5).mean(),
        "ma10": close.tail(10).mean(),
        "ma20": ma20,
        "ma50": ma50,
        "ema10": ema10,
        "ema20": ema20,
        "ema50": ema50,
        "rsi14": rsi14,
        "ema20_extension_pct": ema20_extension_pct,
        "ema50_extension_pct": ema50_extension_pct,
        "vwap20": vwap20,
        "vwap50": vwap50,
        "price_vs_ma20_pct": price_vs_ma20_pct,
        "price_vs_ma50_pct": price_vs_ma50_pct,
        "price_vs_vwap20_pct": price_vs_vwap20_pct,
        "price_vs_vwap50_pct": price_vs_vwap50_pct,
        "low_10d": low.tail(10).min(),
        "low_20d": low_20d,
        "high_10d": high_10d,
        "high_20d": high_20d,
        "high_50d": high_50d,
        "prev_high_20d": prev_high_20d,
        "close_vs_20d_high_pct": (safe_divide(latest_close, high_20d) - 1) * 100,
        "bb_width_pct": bb_width_pct,
        "bb_width_percentile_60": bb_width_percentile_60,
        "avwap_from_20d_low": avwap_from_20d_low,
        "price_vs_avwap_pct": price_vs_avwap_pct,
        "accumulation_5d": accumulation_5d,
        "pocket_pivot_volume_ratio": pocket_pivot_volume_ratio,
    }


def add_market_regime_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    market_stats = (
        result.groupby("market")
        .agg(
            market_return_3d=("return_3d", "median"),
            market_return_5d=("return_5d", "median"),
            market_return_20d=("return_20d", "median"),
            market_positive_rate_1d=("return_1d", lambda values: (values > 0).mean() * 100),
        )
        .reset_index()
    )
    result = result.merge(market_stats, on="market", how="left")
    result["relative_return_5d"] = result["return_5d"] - result["market_return_5d"]
    result["relative_return_20d"] = result["return_20d"] - result["market_return_20d"]
    return result


def add_value_context_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in ["per", "pbr", "eps", "bps", "estimated_roe"]:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    per = result["per"]
    pbr = result["pbr"]
    result["earnings_yield"] = (safe_divide_series(pd.Series(1.0, index=result.index), per) * 100).where(per > 0, 0)
    result["book_discount_pct"] = ((1 - pbr) * 100).where(pbr > 0, 0)

    group_key = first_non_empty_series(
        result.get("industry", pd.Series("", index=result.index)),
        result.get("sector", pd.Series("", index=result.index)),
        result.get("market", pd.Series("", index=result.index)),
    )
    market_key = result.get("market", pd.Series("", index=result.index)).fillna("").astype(str)

    result["sector_per_median"] = valuation_median_with_fallback(per, group_key, market_key)
    result["sector_pbr_median"] = valuation_median_with_fallback(pbr, group_key, market_key)
    result["per_vs_sector_pct"] = (
        (safe_divide_series(per, result["sector_per_median"]) - 1) * 100
    ).where((per > 0) & (result["sector_per_median"] > 0), 0)
    result["pbr_vs_sector_pct"] = (
        (safe_divide_series(pbr, result["sector_pbr_median"]) - 1) * 100
    ).where((pbr > 0) & (result["sector_pbr_median"] > 0), 0)
    result["undervaluation_score"] = calculate_undervaluation_score(result)
    result["value_trap_penalty"] = calculate_value_trap_penalty(result)
    return result


def first_non_empty_series(*series_values: pd.Series) -> pd.Series:
    if not series_values:
        return pd.Series(dtype="object")

    result = pd.Series("", index=series_values[0].index, dtype="object")
    for values in series_values:
        cleaned = values.fillna("").astype(str).str.strip()
        result = result.mask(result == "", cleaned)
    return result


def valuation_median_with_fallback(
    values: pd.Series,
    group_key: pd.Series,
    market_key: pd.Series,
) -> pd.Series:
    positive_values = values.where(values > 0)
    group_median = positive_values.groupby(group_key).transform("median")
    group_count = positive_values.groupby(group_key).transform("count")
    market_median = positive_values.groupby(market_key).transform("median")
    global_median = positive_values.median()

    if pd.isna(global_median):
        global_median = 0

    return group_median.where(group_count >= 3, market_median).fillna(global_median)


def apply_swing_hard_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    numeric_columns = [
        "price",
        "market_cap",
        "trading_value_today",
        "avg_trading_value_20d",
        "return_1d",
        "return_3d",
        "return_5d",
        "ema20_extension_pct",
        "ema50_extension_pct",
        "rsi14",
        "per",
        "pbr",
        "eps",
        "bps",
        "estimated_roe",
        "undervaluation_score",
        "value_trap_penalty",
        "per_vs_sector_pct",
        "pbr_vs_sector_pct",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    value_anchor = (
        (result["undervaluation_score"] >= 8)
        & (
            ((result["per"] > 0) & (result["per"] <= 18))
            | ((result["pbr"] > 0) & (result["pbr"] <= 1.5))
            | (result["per_vs_sector_pct"] <= -15)
            | (result["pbr_vs_sector_pct"] <= -15)
        )
    )
    quality_anchor = (
        (result["eps"] > 0)
        & (result["bps"] > 0)
        & (result["estimated_roe"] >= 4)
        & (result["value_trap_penalty"] <= 10)
    )
    not_chasing = (
        (result["return_1d"] <= 10)
        & (result["return_3d"] <= 20)
        & (result["return_5d"] <= 18)
        & (result["ema20_extension_pct"] <= 15)
        & (result["ema50_extension_pct"] <= 30)
        & (
            (result["rsi14"] <= 78)
            | ((result["return_5d"] <= 8) & (result["ema20_extension_pct"] <= 8))
        )
    )
    mask = (
        (result["market_cap"] >= MIN_SWING_MARKET_CAP)
        & (result["avg_trading_value_20d"] >= MIN_AVG_TRADING_VALUE_20D)
        & (result["trading_value_today"] >= MIN_TODAY_TRADING_VALUE)
        & (result["price"] >= MIN_PRICE)
        & (result["return_1d"] >= -7)
        & not_chasing
        & value_anchor
        & quality_anchor
        & (~result["exclude_swing"])
    )
    return result[mask].copy()


def add_trade_plan_columns(
    df: pd.DataFrame,
    signal_date: str,
    review_date: str | None = None,
    review_date_5d: str | None = None,
) -> pd.DataFrame:
    result = df.copy()
    entry_price = pd.to_numeric(result["price"], errors="coerce")
    result["tick_size"] = entry_price.apply(get_krx_tick_size).astype("Int64")
    result["entry_price"] = round_to_tick(entry_price, mode="nearest")
    result["add_price_1"] = round_to_tick(entry_price * 0.96, mode="down")
    result["add_price_2"] = round_to_tick(entry_price * 0.92, mode="down")
    result["add_price_3"] = round_to_tick(entry_price * 0.90, mode="down")
    result["half_take_profit_price"] = round_to_tick(entry_price * 1.04, mode="up")
    result["full_take_profit_price"] = round_to_tick(entry_price * 1.07, mode="up")
    review_date_3d = review_date or add_trading_days(signal_date, 3)
    result["review_date"] = review_date_3d
    result["review_date_3d"] = review_date_3d
    result["review_date_5d"] = review_date_5d or add_trading_days(signal_date, 5)
    return result


def score_swing_candidates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if "undervaluation_score" not in result.columns or "value_trap_penalty" not in result.columns:
        result = add_value_context_columns(result)
    result["value_score"] = calculate_value_score(result)
    result["event_pivot_score"] = calculate_event_pivot_score(result)
    result["average_discount_score"] = calculate_average_discount_score(result)
    result["volume_breakout_score"] = calculate_volume_breakout_score(result)
    result["contraction_score"] = calculate_contraction_score(result)
    result["darvas_breakout_score"] = calculate_darvas_breakout_score(result)
    result["pullback_ladder_score"] = result.apply(calculate_pullback_ladder_score, axis=1)
    result["pocket_pivot_score"] = calculate_pocket_pivot_score(result)
    result["bb_squeeze_score"] = calculate_bb_squeeze_score(result)
    result["anchored_vwap_score"] = calculate_anchored_vwap_score(result)
    result["accumulation_score"] = calculate_accumulation_score(result)
    result["relative_strength_score"] = calculate_relative_strength_score(result)
    result["rsi_score"] = calculate_rsi_score(result)
    result["ema_trend_score"] = calculate_ema_trend_score(result)
    result["average_discount_anchor"] = calculate_average_discount_anchor(result)
    result["risk_penalty"] = result.apply(calculate_risk_penalty, axis=1)
    result["matched_setups"] = result.apply(build_matched_setups, axis=1)
    result["setup_tags"] = result.apply(build_setup_tags, axis=1)
    result["risk_flags"] = result.apply(build_risk_flags, axis=1)
    result["setup_bonus"] = result["matched_setups"].apply(count_csv_items).clip(upper=4) * 3
    result["swing_score"] = (
        result[
            [
                "value_score",
                "event_pivot_score",
                "average_discount_score",
                "volume_breakout_score",
                "contraction_score",
                "darvas_breakout_score",
                "pullback_ladder_score",
                "pocket_pivot_score",
                "bb_squeeze_score",
                "anchored_vwap_score",
                "accumulation_score",
                "relative_strength_score",
                "rsi_score",
                "ema_trend_score",
                "setup_bonus",
            ]
        ].sum(axis=1)
        - result["risk_penalty"]
        - result["value_trap_penalty"]
    ).clip(lower=0).round(2)

    round_columns = [
        "price",
        "trading_value_today",
        "avg_trading_value_20d",
        "return_1d",
        "return_3d",
        "return_5d",
        "return_20d",
        "market_return_3d",
        "market_return_5d",
        "market_return_20d",
        "market_positive_rate_1d",
        "relative_return_5d",
        "relative_return_20d",
        "volume_ratio_20d",
        "trading_value_ratio_20d",
        "day_range_pct",
        "adr_20d",
        "close_position_in_range",
        "ma5",
        "ma10",
        "ma20",
        "ma50",
        "low_20d",
        "high_10d",
        "high_20d",
        "high_50d",
        "close_vs_20d_high_pct",
        "bb_width_pct",
        "bb_width_percentile_60",
        "avwap_from_20d_low",
        "price_vs_avwap_pct",
        "accumulation_5d",
        "pocket_pivot_volume_ratio",
        "earnings_yield",
        "book_discount_pct",
        "sector_per_median",
        "sector_pbr_median",
        "per_vs_sector_pct",
        "pbr_vs_sector_pct",
        "ema10",
        "ema20",
        "ema50",
        "rsi14",
        "ema20_extension_pct",
        "ema50_extension_pct",
        "vwap20",
        "vwap50",
        "price_vs_ma20_pct",
        "price_vs_ma50_pct",
        "price_vs_vwap20_pct",
        "price_vs_vwap50_pct",
        "value_score",
        "undervaluation_score",
        "average_discount_score",
        "event_pivot_score",
        "volume_breakout_score",
        "contraction_score",
        "darvas_breakout_score",
        "pullback_ladder_score",
        "pocket_pivot_score",
        "bb_squeeze_score",
        "anchored_vwap_score",
        "accumulation_score",
        "relative_strength_score",
        "rsi_score",
        "ema_trend_score",
        "setup_bonus",
        "risk_penalty",
        "value_trap_penalty",
    ]
    for column in round_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").round(2)

    return result


def calculate_value_score(df: pd.DataFrame) -> pd.Series:
    per = pd.to_numeric(df["per"], errors="coerce")
    pbr = pd.to_numeric(df["pbr"], errors="coerce")
    roe = pd.to_numeric(df["estimated_roe"], errors="coerce").fillna(0)
    eps = pd.to_numeric(df["eps"], errors="coerce")
    bps = pd.to_numeric(df["bps"], errors="coerce")
    undervaluation_score = (
        pd.to_numeric(df["undervaluation_score"], errors="coerce").fillna(0)
        if "undervaluation_score" in df.columns
        else calculate_undervaluation_score(df)
    )

    per_score = ((18 - per) / 18 * 6).where(per > 0, 0).clip(lower=0, upper=6)
    pbr_score = ((1.8 - pbr) / 1.8 * 6).where(pbr > 0, 0).clip(lower=0, upper=6)
    roe_score = (roe / 15 * 6).clip(lower=0, upper=6)
    positive_book_score = ((eps > 0) & (bps > 0)).astype(int) * 2
    return (
        undervaluation_score
        + per_score
        + pbr_score
        + roe_score
        + positive_book_score
    ).round(2)


def calculate_undervaluation_score(df: pd.DataFrame) -> pd.Series:
    per = pd.to_numeric(df["per"], errors="coerce")
    pbr = pd.to_numeric(df["pbr"], errors="coerce")
    roe = pd.to_numeric(df["estimated_roe"], errors="coerce").fillna(0)
    earnings_yield = (
        pd.to_numeric(df["earnings_yield"], errors="coerce").fillna(0)
        if "earnings_yield" in df.columns
        else (safe_divide_series(pd.Series(1.0, index=df.index), per) * 100).where(per > 0, 0)
    )
    sector_per = (
        pd.to_numeric(df["sector_per_median"], errors="coerce")
        if "sector_per_median" in df.columns
        else pd.Series(18.0, index=df.index)
    )
    sector_pbr = (
        pd.to_numeric(df["sector_pbr_median"], errors="coerce")
        if "sector_pbr_median" in df.columns
        else pd.Series(1.8, index=df.index)
    )

    absolute_per_score = ((18 - per) / 18 * 8).where(per > 0, 0).clip(lower=0, upper=8)
    absolute_pbr_score = ((1.5 - pbr) / 1.5 * 8).where(pbr > 0, 0).clip(lower=0, upper=8)
    sector_per_score = (
        ((sector_per - per) / sector_per * 6)
        .where((per > 0) & (sector_per > 0), 0)
        .clip(lower=0, upper=6)
    )
    sector_pbr_score = (
        ((sector_pbr - pbr) / sector_pbr * 6)
        .where((pbr > 0) & (sector_pbr > 0), 0)
        .clip(lower=0, upper=6)
    )
    earnings_yield_score = (earnings_yield / 12 * 4).clip(lower=0, upper=4)
    roe_quality_score = ((roe - 4) / 12 * 4).clip(lower=0, upper=4)
    return (
        absolute_per_score
        + absolute_pbr_score
        + sector_per_score
        + sector_pbr_score
        + earnings_yield_score
        + roe_quality_score
    ).round(2)


def calculate_average_discount_anchor(df: pd.DataFrame) -> pd.Series:
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")
    price_vs_ma50 = numeric_series(df, "price_vs_ma50_pct")
    price_vs_vwap20 = numeric_series(df, "price_vs_vwap20_pct")
    price_vs_vwap50 = numeric_series(df, "price_vs_vwap50_pct")
    return (
        (price_vs_ma20 <= -0.5)
        | (price_vs_ma50 <= -0.5)
        | (price_vs_vwap20 <= -0.5)
        | (price_vs_vwap50 <= -0.5)
    )


def calculate_average_discount_score(df: pd.DataFrame) -> pd.Series:
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")
    price_vs_ma50 = numeric_series(df, "price_vs_ma50_pct")
    price_vs_vwap20 = numeric_series(df, "price_vs_vwap20_pct")
    price_vs_vwap50 = numeric_series(df, "price_vs_vwap50_pct")
    close_position = numeric_series(df, "close_position_in_range")
    rsi = numeric_series(df, "rsi14", 50)
    return_1d = numeric_series(df, "return_1d")
    return_5d = numeric_series(df, "return_5d")
    return_20d = numeric_series(df, "return_20d")

    short_average_discount = (
        ((-price_vs_ma20).clip(lower=0, upper=12) / 12 * 4)
        + ((-price_vs_vwap20).clip(lower=0, upper=12) / 12 * 4)
    )
    medium_average_discount = (
        ((-price_vs_ma50).clip(lower=0, upper=18) / 18 * 3)
        + ((-price_vs_vwap50).clip(lower=0, upper=18) / 18 * 3)
    )
    constructive_rsi = ((rsi >= 38) & (rsi <= 68)).astype(int) * 2
    close_recovery = (close_position >= 45).astype(int) * 2
    not_deep_breakdown = (
        (price_vs_ma20 >= -18)
        & (price_vs_vwap20 >= -18)
        & (price_vs_ma50 >= -30)
        & (price_vs_vwap50 >= -30)
        & (return_20d >= -25)
        & (return_1d >= -7)
    ).astype(int) * 2
    anchor_bonus = calculate_average_discount_anchor(df).astype(int) * 2
    weak_rebound_penalty = (
        ((rsi < 35).astype(int) * 4)
        + ((close_position < 25).astype(int) * 4)
        + ((return_5d <= -15).astype(int) * 3)
        + ((price_vs_vwap20 <= -18).astype(int) * 2)
    )

    return (
        short_average_discount
        + medium_average_discount
        + constructive_rsi
        + close_recovery
        + not_deep_breakdown
        + anchor_bonus
        - weak_rebound_penalty
    ).clip(lower=0).round(2)


def calculate_value_trap_penalty(df: pd.DataFrame) -> pd.Series:
    per = pd.to_numeric(df["per"], errors="coerce")
    pbr = pd.to_numeric(df["pbr"], errors="coerce")
    eps = pd.to_numeric(df["eps"], errors="coerce")
    bps = pd.to_numeric(df["bps"], errors="coerce")
    roe = pd.to_numeric(df["estimated_roe"], errors="coerce").fillna(0)
    return_20d = numeric_series(df, "return_20d")
    price = numeric_series(df, "price")
    ma50 = numeric_series(df, "ma50")

    penalty = pd.Series(0.0, index=df.index)
    penalty += (eps <= 0).astype(int) * 8
    penalty += (bps <= 0).astype(int) * 8
    penalty += (roe < 0).astype(int) * 10
    penalty += ((roe >= 0) & (roe < 4)).astype(int) * 5
    penalty += (((per > 0) & (per <= 3)) | ((pbr > 0) & (pbr <= 0.35))).astype(int) * 4
    penalty += ((return_20d <= -20) & (price < ma50)).astype(int) * 5
    return penalty.round(2)


def calculate_event_pivot_score(df: pd.DataFrame) -> pd.Series:
    return_1d = pd.to_numeric(df["return_1d"], errors="coerce").fillna(0)
    trading_ratio = pd.to_numeric(df["trading_value_ratio_20d"], errors="coerce").fillna(0)
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)
    active = (return_1d >= 2) & (trading_ratio >= 1.8) & (close_position >= 60)
    score = (
        ((return_1d.clip(lower=0, upper=12) / 12) * 10)
        + ((trading_ratio.clip(lower=0, upper=5) / 5) * 10)
        + ((close_position.clip(lower=0, upper=100) / 100) * 5)
    )
    return score.where(active, 0).round(2)


def calculate_volume_breakout_score(df: pd.DataFrame) -> pd.Series:
    trading_ratio = pd.to_numeric(df["trading_value_ratio_20d"], errors="coerce").fillna(0)
    volume_ratio = pd.to_numeric(df["volume_ratio_20d"], errors="coerce").fillna(0)
    blended_ratio = (trading_ratio * 0.65) + (volume_ratio * 0.35)
    return (((blended_ratio - 1).clip(lower=0, upper=4) / 4) * 20).round(2)


def calculate_contraction_score(df: pd.DataFrame) -> pd.Series:
    avg_range_5d = pd.to_numeric(df["avg_range_5d"], errors="coerce")
    avg_range_20d = pd.to_numeric(df["avg_range_20d"], errors="coerce")
    avg_volume_5d = pd.to_numeric(df["avg_volume_5d"], errors="coerce")
    avg_volume_20d = pd.to_numeric(df["avg_volume_20d"], errors="coerce")
    price = pd.to_numeric(df["price"], errors="coerce")
    ma20 = pd.to_numeric(df["ma20"], errors="coerce")
    high_20d = pd.to_numeric(df["high_20d"], errors="coerce")

    range_ratio = safe_divide_series(avg_range_5d, avg_range_20d)
    volume_ratio = safe_divide_series(avg_volume_5d, avg_volume_20d)
    high_proximity = safe_divide_series(price, high_20d)
    range_score = ((1 - range_ratio).clip(lower=0, upper=0.5) / 0.5) * 8
    volume_dryup_score = ((1 - volume_ratio).clip(lower=0, upper=0.5) / 0.5) * 5
    high_score = ((high_proximity - 0.9).clip(lower=0, upper=0.1) / 0.1) * 2
    trend_bonus = (price > ma20).astype(int) * 3
    return (range_score + volume_dryup_score + high_score + trend_bonus).round(2)


def calculate_darvas_breakout_score(df: pd.DataFrame) -> pd.Series:
    price = pd.to_numeric(df["price"], errors="coerce")
    prev_high = pd.to_numeric(df["prev_high_20d"], errors="coerce")
    trading_ratio = pd.to_numeric(df["trading_value_ratio_20d"], errors="coerce").fillna(0)
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)

    breakout_ratio = safe_divide_series(price, prev_high)
    near_or_breakout = breakout_ratio >= 0.98
    score = (
        ((breakout_ratio - 0.96).clip(lower=0, upper=0.08) / 0.08) * 10
        + ((trading_ratio - 1).clip(lower=0, upper=3) / 3) * 6
        + (close_position.clip(lower=0, upper=100) / 100) * 4
    )
    return score.where(near_or_breakout, 0).round(2)


def calculate_pullback_ladder_score(row: pd.Series) -> float:
    price = float_or_zero(row.get("price"))
    ma5 = float_or_zero(row.get("ma5"))
    ma20 = float_or_zero(row.get("ma20"))
    low_10d = float_or_zero(row.get("low_10d"))
    return_5d = float_or_zero(row.get("return_5d"))
    if price <= 0 or ma20 <= 0:
        return 0.0

    support_scores = [
        support_proximity_score(price * 0.96, [ma5, ma20, low_10d]),
        support_proximity_score(price * 0.92, [ma20, low_10d]),
        support_proximity_score(price * 0.90, [ma20, low_10d]),
    ]
    trend_score = 4 if price >= ma20 else 0
    momentum_score = 3 if 0 <= return_5d <= 18 else 0
    return round(sum(support_scores) + trend_score + momentum_score, 2)


def calculate_pocket_pivot_score(df: pd.DataFrame) -> pd.Series:
    volume_ratio = pd.to_numeric(df["pocket_pivot_volume_ratio"], errors="coerce").fillna(0)
    return_1d = pd.to_numeric(df["return_1d"], errors="coerce").fillna(0)
    price = pd.to_numeric(df["price"], errors="coerce")
    ma10 = pd.to_numeric(df["ma10"], errors="coerce")
    ma20 = pd.to_numeric(df["ma20"], errors="coerce")
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)

    active = (
        (volume_ratio >= 1.0)
        & (return_1d > 0)
        & (price >= ma10 * 0.98)
        & (price >= ma20 * 0.96)
        & (close_position >= 55)
    )
    score = (
        ((volume_ratio - 1).clip(lower=0, upper=2.5) / 2.5) * 8
        + (return_1d.clip(lower=0, upper=8) / 8) * 4
        + (close_position.clip(lower=0, upper=100) / 100) * 3
    )
    return score.where(active, 0).round(2)


def calculate_bb_squeeze_score(df: pd.DataFrame) -> pd.Series:
    width_percentile = pd.to_numeric(df["bb_width_percentile_60"], errors="coerce").fillna(100)
    width_pct = pd.to_numeric(df["bb_width_pct"], errors="coerce").fillna(100)
    price = pd.to_numeric(df["price"], errors="coerce")
    ma20 = pd.to_numeric(df["ma20"], errors="coerce")
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)
    trading_ratio = pd.to_numeric(df["trading_value_ratio_20d"], errors="coerce").fillna(0)

    active = (width_percentile <= 40) & (price >= ma20 * 0.98) & (close_position >= 50)
    squeeze_score = ((40 - width_percentile).clip(lower=0, upper=40) / 40) * 8
    width_quality_score = ((18 - width_pct).clip(lower=0, upper=18) / 18) * 3
    trigger_score = ((trading_ratio - 0.8).clip(lower=0, upper=1.7) / 1.7) * 4
    return (squeeze_score + width_quality_score + trigger_score).where(active, 0).round(2)


def calculate_anchored_vwap_score(df: pd.DataFrame) -> pd.Series:
    price_vs_avwap = pd.to_numeric(df["price_vs_avwap_pct"], errors="coerce")
    price = pd.to_numeric(df["price"], errors="coerce")
    ma20 = pd.to_numeric(df["ma20"], errors="coerce")
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)
    return_5d = pd.to_numeric(df["return_5d"], errors="coerce").fillna(0)

    active = (
        (price_vs_avwap >= -1.0)
        & (price_vs_avwap <= 10.0)
        & (price >= ma20 * 0.97)
        & (close_position >= 55)
        & (return_5d > 0)
        & (return_5d <= 25)
    )
    proximity_score = ((10 - price_vs_avwap.abs()).clip(lower=0, upper=10) / 10) * 7
    trend_score = (price >= ma20).astype(int) * 3
    close_score = (close_position.clip(lower=0, upper=100) / 100) * 2
    return (proximity_score + trend_score + close_score).where(active, 0).round(2)


def calculate_accumulation_score(df: pd.DataFrame) -> pd.Series:
    accumulation = pd.to_numeric(df["accumulation_5d"], errors="coerce").fillna(0)
    close_position = pd.to_numeric(df["close_position_in_range"], errors="coerce").fillna(0)
    volume_ratio = pd.to_numeric(df["volume_ratio_20d"], errors="coerce").fillna(0)
    active = (accumulation > 0) & (close_position >= 55)
    score = (
        (accumulation.clip(lower=0, upper=4) / 4) * 6
        + (close_position.clip(lower=0, upper=100) / 100) * 3
        + ((volume_ratio - 0.8).clip(lower=0, upper=2.2) / 2.2) * 3
    )
    return score.where(active, 0).round(2)


def calculate_relative_strength_score(df: pd.DataFrame) -> pd.Series:
    relative_5d = pd.to_numeric(df["relative_return_5d"], errors="coerce")
    relative_20d = pd.to_numeric(df["relative_return_20d"], errors="coerce")
    rank_5d = relative_5d.rank(pct=True).fillna(0)
    rank_20d = relative_20d.rank(pct=True).fillna(0)
    return ((rank_5d * 0.45 + rank_20d * 0.55) * 10).round(2)


def calculate_rsi_score(df: pd.DataFrame) -> pd.Series:
    rsi = pd.to_numeric(df["rsi14"], errors="coerce").fillna(50)
    score = (8 - ((rsi - 58).abs() / 18 * 8)).clip(lower=0, upper=8)
    return score.where((rsi >= 38) & (rsi <= 74), 0).round(2)


def calculate_ema_trend_score(df: pd.DataFrame) -> pd.Series:
    price = pd.to_numeric(df["price"], errors="coerce")
    ema10 = pd.to_numeric(df["ema10"], errors="coerce")
    ema20 = pd.to_numeric(df["ema20"], errors="coerce")
    ema50 = pd.to_numeric(df["ema50"], errors="coerce")
    return_5d = pd.to_numeric(df["return_5d"], errors="coerce").fillna(0)

    trend_score = (price >= ema20).astype(int) * 3
    alignment_score = (ema10 >= ema20).astype(int) * 3
    intermediate_score = (ema20 >= ema50).astype(int) * 2
    pullback_quality = ((return_5d >= -4) & (return_5d <= 18)).astype(int) * 2
    return (trend_score + alignment_score + intermediate_score + pullback_quality).round(2)


def support_proximity_score(price: float, supports: list[float]) -> float:
    valid_supports = [support for support in supports if support > 0]
    if not valid_supports or price <= 0:
        return 0.0

    min_distance_pct = min(abs(price - support) / support * 100 for support in valid_supports)
    if min_distance_pct <= 1.5:
        return 4.0
    if min_distance_pct <= 3:
        return 2.5
    if min_distance_pct <= 5:
        return 1.0
    return 0.0


def calculate_risk_penalty(row: pd.Series) -> float:
    penalty = 0.0
    if str(row.get("market_risk_flags", "")).strip():
        penalty += 20
    if float_or_zero(row.get("market_return_3d")) <= -2:
        penalty += 5
    if float_or_zero(row.get("market_positive_rate_1d")) <= 35:
        penalty += 5
    if float_or_zero(row.get("day_range_pct")) >= 12:
        penalty += 5
    if float_or_zero(row.get("close_position_in_range")) <= 40:
        penalty += 5
    if float_or_zero(row.get("return_5d")) >= 25:
        penalty += 10
    if float_or_zero(row.get("return_1d")) >= 12:
        penalty += 5
    if float_or_zero(row.get("return_3d")) >= 20:
        penalty += 8
    if float_or_zero(row.get("ema20_extension_pct")) >= 15:
        penalty += 10
    if float_or_zero(row.get("ema50_extension_pct")) >= 30:
        penalty += 8
    if float_or_zero(row.get("trading_value_ratio_20d")) >= 8:
        penalty += 5
    if safe_divide(float_or_zero(row.get("price")), float_or_zero(row.get("ma20"))) >= 1.25:
        penalty += 5
    if float_or_zero(row.get("adr_20d")) >= 12:
        penalty += 5
    if float_or_zero(row.get("bb_width_pct")) >= 30:
        penalty += 5
    if float_or_zero(row.get("return_20d")) >= 45:
        penalty += 5
    if float_or_zero(row.get("rsi14")) >= 78:
        penalty += 5
    if float_or_zero(row.get("rsi14")) <= 35:
        penalty += 5
    if float_or_zero(row.get("price_vs_ma20_pct")) <= -18 and float_or_zero(row.get("price_vs_vwap20_pct")) <= -18:
        penalty += 5
    if float_or_zero(row.get("price_vs_ma50_pct")) <= -30 and float_or_zero(row.get("return_20d")) <= -25:
        penalty += 8
    return penalty


def build_matched_setups(row: pd.Series) -> str:
    setups: list[str] = []
    if float_or_zero(row.get("event_pivot_score")) >= 12:
        setups.append("event_pivot")
    if bool(row.get("average_discount_anchor")) and float_or_zero(row.get("average_discount_score")) >= 6:
        setups.append("average_discount_pullback")
    if float_or_zero(row.get("contraction_score")) >= 10:
        setups.append("vcp_squeeze")
    if float_or_zero(row.get("darvas_breakout_score")) >= 12:
        setups.append("darvas_breakout")
    if float_or_zero(row.get("pullback_ladder_score")) >= 9:
        setups.append("pullback_ladder")
    if float_or_zero(row.get("pocket_pivot_score")) >= 9:
        setups.append("pocket_pivot")
    if float_or_zero(row.get("bb_squeeze_score")) >= 9:
        setups.append("bb_squeeze")
    if float_or_zero(row.get("anchored_vwap_score")) >= 8:
        setups.append("anchored_vwap_support")
    return ", ".join(setups)


def build_setup_tags(row: pd.Series) -> str:
    tags: list[str] = []
    if float_or_zero(row.get("volume_breakout_score")) >= 10:
        tags.append("volume_expansion")
    if float_or_zero(row.get("close_vs_20d_high_pct")) >= -3:
        tags.append("near_20d_high")
    if float_or_zero(row.get("return_3d")) > 0 and float_or_zero(row.get("return_5d")) > 0:
        tags.append("short_momentum")
    if float_or_zero(row.get("price")) >= float_or_zero(row.get("ma5")) >= float_or_zero(row.get("ma20")):
        tags.append("ma_alignment")
    if float_or_zero(row.get("value_score")) >= 16:
        tags.append("undervalued_quality")
    if float_or_zero(row.get("accumulation_score")) >= 7:
        tags.append("accumulation")
    if float_or_zero(row.get("anchored_vwap_score")) >= 8:
        tags.append("avwap_support")
    if float_or_zero(row.get("relative_strength_score")) >= 7:
        tags.append("relative_strength")
    if float_or_zero(row.get("undervaluation_score")) >= 14:
        tags.append("undervalued")
    if bool(row.get("average_discount_anchor")) and float_or_zero(row.get("average_discount_score")) >= 6:
        tags.append("average_discount")
    if float_or_zero(row.get("price_vs_ma20_pct")) <= -0.5:
        tags.append("below_ma20")
    if float_or_zero(row.get("price_vs_ma50_pct")) <= -0.5:
        tags.append("below_ma50")
    if float_or_zero(row.get("price_vs_vwap20_pct")) <= -0.5:
        tags.append("below_vwap20")
    if float_or_zero(row.get("price_vs_vwap50_pct")) <= -0.5:
        tags.append("below_vwap50")
    if float_or_zero(row.get("per_vs_sector_pct")) <= -15 or float_or_zero(row.get("pbr_vs_sector_pct")) <= -15:
        tags.append("sector_discount")
    if float_or_zero(row.get("rsi_score")) >= 5:
        tags.append("constructive_rsi")
    if float_or_zero(row.get("ema_trend_score")) >= 7:
        tags.append("ema_trend")
    return ", ".join(tags)


def build_risk_flags(row: pd.Series) -> str:
    flags: list[str] = []
    market_risk_flags = str(row.get("market_risk_flags", "")).strip()
    if market_risk_flags:
        flags.extend([flag.strip() for flag in market_risk_flags.split(",") if flag.strip()])
    if float_or_zero(row.get("market_return_3d")) <= -2:
        flags.append("weak_market_3d")
    if float_or_zero(row.get("market_positive_rate_1d")) <= 35:
        flags.append("weak_market_breadth")
    if float_or_zero(row.get("day_range_pct")) >= 12:
        flags.append("wide_intraday_range")
    if float_or_zero(row.get("close_position_in_range")) <= 40:
        flags.append("weak_close")
    if float_or_zero(row.get("return_5d")) >= 25:
        flags.append("extended_5d")
    if float_or_zero(row.get("return_1d")) >= 12:
        flags.append("hot_single_day")
    if float_or_zero(row.get("return_3d")) >= 20:
        flags.append("extended_3d")
    if float_or_zero(row.get("ema20_extension_pct")) >= 15:
        flags.append("far_above_ema20")
    if float_or_zero(row.get("ema50_extension_pct")) >= 30:
        flags.append("far_above_ema50")
    if float_or_zero(row.get("trading_value_ratio_20d")) >= 8:
        flags.append("possible_overheat")
    if safe_divide(float_or_zero(row.get("price")), float_or_zero(row.get("ma20"))) >= 1.25:
        flags.append("far_above_ma20")
    if float_or_zero(row.get("adr_20d")) >= 12:
        flags.append("high_adr")
    if float_or_zero(row.get("bb_width_pct")) >= 30:
        flags.append("wide_bollinger_band")
    if float_or_zero(row.get("return_20d")) >= 45:
        flags.append("extended_20d")
    if float_or_zero(row.get("rsi14")) >= 78:
        flags.append("overbought_rsi")
    if float_or_zero(row.get("rsi14")) <= 35:
        flags.append("weak_rsi")
    if float_or_zero(row.get("price_vs_ma20_pct")) <= -18 and float_or_zero(row.get("price_vs_vwap20_pct")) <= -18:
        flags.append("deep_below_20d_average")
    if float_or_zero(row.get("price_vs_ma50_pct")) <= -30 and float_or_zero(row.get("return_20d")) <= -25:
        flags.append("deep_downtrend_discount")
    if float_or_zero(row.get("value_trap_penalty")) >= 8:
        flags.append("value_trap_quality")
    return ", ".join(dict.fromkeys(flags))


def pct_return(close: pd.Series, periods: int) -> float:
    if len(close) <= periods:
        return 0.0
    base = close.iloc[-(periods + 1)]
    latest = close.iloc[-1]
    return (safe_divide(latest, base) - 1) * 100


def calculate_ema(close: pd.Series, periods: int) -> float:
    values = pd.to_numeric(close, errors="coerce").dropna()
    if values.empty:
        return 0.0
    return float(values.ewm(span=periods, adjust=False).mean().iloc[-1])


def calculate_rsi(close: pd.Series, periods: int = 14) -> float:
    values = pd.to_numeric(close, errors="coerce").dropna()
    if len(values) <= periods:
        return 50.0

    delta = values.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.ewm(alpha=1 / periods, adjust=False, min_periods=periods).mean().iloc[-1]
    avg_loss = losses.ewm(alpha=1 / periods, adjust=False, min_periods=periods).mean().iloc[-1]

    if pd.isna(avg_gain) or pd.isna(avg_loss):
        return 50.0
    if avg_loss == 0:
        return 100.0 if avg_gain > 0 else 50.0

    relative_strength = avg_gain / avg_loss
    return float(100 - (100 / (1 + relative_strength)))


def calculate_accumulation_change(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
) -> float:
    high_low_range = high - low
    multiplier = ((close - low) - (high - close)) / high_low_range.where(high_low_range != 0)
    money_flow_volume = multiplier.fillna(0) * volume.fillna(0)
    adl = money_flow_volume.cumsum()
    if len(adl) <= 5:
        return 0.0
    avg_volume_20d = volume.tail(20).mean()
    return safe_divide(adl.iloc[-1] - adl.iloc[-6], avg_volume_20d)


def calculate_anchored_vwap_from_low(
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    lookback: int,
) -> float:
    if close.empty or high.empty or low.empty or volume.empty:
        return 0.0

    anchor_index = low.tail(lookback).idxmin()
    typical_price = (high + low + close) / 3
    anchored_price = typical_price.loc[anchor_index:]
    anchored_volume = volume.loc[anchor_index:]
    return safe_divide((anchored_price * anchored_volume).sum(), anchored_volume.sum())


def calculate_period_vwap(
    volume: pd.Series,
    trading_value: pd.Series,
    periods: int,
) -> float:
    recent_volume = pd.to_numeric(volume, errors="coerce").tail(periods)
    recent_trading_value = pd.to_numeric(trading_value, errors="coerce").tail(periods)
    return safe_divide(recent_trading_value.sum(), recent_volume.sum())


def safe_divide(numerator, denominator) -> float:
    try:
        if pd.isna(numerator) or pd.isna(denominator) or denominator == 0:
            return 0.0
        return float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return 0.0


def safe_divide_series(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    denominator = denominator.where(denominator != 0)
    return numerator / denominator


def float_or_zero(value) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def numeric_series(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def get_krx_tick_size(price) -> int:
    value = float_or_zero(price)
    if value < 2_000:
        return 1
    if value < 5_000:
        return 5
    if value < 20_000:
        return 10
    if value < 50_000:
        return 50
    if value < 200_000:
        return 100
    if value < 500_000:
        return 500
    return 1_000


def round_to_tick(series: pd.Series, mode: str) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")

    def round_value(value):
        if pd.isna(value):
            return pd.NA
        tick_size = get_krx_tick_size(value)
        if mode == "down":
            return int(math.floor(value / tick_size) * tick_size)
        if mode == "up":
            return int(math.ceil(value / tick_size) * tick_size)
        if mode == "nearest":
            return int(math.floor((value / tick_size) + 0.5) * tick_size)
        raise ValueError("mode must be one of: down, up, nearest")

    return values.apply(round_value).astype("Int64")


def parse_bool_like(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y", "exclude", "제외"}


def count_csv_items(value: str) -> int:
    if not value:
        return 0
    return len([item for item in str(value).split(",") if item.strip()])


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result
