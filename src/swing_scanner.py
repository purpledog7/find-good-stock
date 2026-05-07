from __future__ import annotations

import math

import pandas as pd

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
    "trading_value_today",
    "avg_trading_value_20d",
    "return_1d",
    "return_3d",
    "return_5d",
    "market_return_3d",
    "market_return_5d",
    "market_positive_rate_1d",
    "relative_return_5d",
    "volume_ratio_20d",
    "trading_value_ratio_20d",
    "day_range_pct",
    "close_position_in_range",
    "ma5",
    "ma20",
    "high_10d",
    "high_20d",
    "close_vs_20d_high_pct",
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
    "relative_strength_score",
    "setup_bonus",
    "risk_penalty",
    "news_risk_penalty",
    "swing_score",
    "entry_price",
    "add_price_1",
    "add_price_2",
    "add_price_3",
    "half_take_profit_price",
    "full_take_profit_price",
    "review_date",
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
    top_n: int = 30,
    review_date: str | None = None,
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

    result = apply_swing_hard_filters(result)
    if result.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)

    result = add_trade_plan_columns(result, signal_date, review_date)
    result = score_swing_candidates(result)
    result = result[result["matched_setups"].astype(str).str.strip() != ""].copy()
    if result.empty:
        return pd.DataFrame(columns=SWING_CANDIDATE_COLUMNS)

    result["date"] = signal_date
    result["market_date"] = market_date
    result = result.sort_values(
        by=["swing_score", "trading_value_today", "return_1d"],
        ascending=[False, False, False],
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
    prev_high_20d = high.iloc[:-1].tail(20).max()
    range_pct = safe_divide_series(high - low, close) * 100

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
        "volume_ratio_20d": safe_divide(latest_volume, avg_volume_20d),
        "trading_value_ratio_20d": safe_divide(latest_trading_value, avg_trading_value_20d),
        "day_range_pct": day_range,
        "avg_range_5d": range_pct.tail(5).mean(),
        "avg_range_20d": range_pct.tail(20).mean(),
        "close_position_in_range": close_position,
        "ma5": close.tail(5).mean(),
        "ma20": close.tail(20).mean(),
        "low_10d": low.tail(10).min(),
        "high_10d": high_10d,
        "high_20d": high_20d,
        "prev_high_20d": prev_high_20d,
        "close_vs_20d_high_pct": (safe_divide(latest_close, high_20d) - 1) * 100,
    }


def add_market_regime_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    market_stats = (
        result.groupby("market")
        .agg(
            market_return_3d=("return_3d", "median"),
            market_return_5d=("return_5d", "median"),
            market_positive_rate_1d=("return_1d", lambda values: (values > 0).mean() * 100),
        )
        .reset_index()
    )
    result = result.merge(market_stats, on="market", how="left")
    result["relative_return_5d"] = result["return_5d"] - result["market_return_5d"]
    return result


def apply_swing_hard_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    numeric_columns = [
        "price",
        "market_cap",
        "trading_value_today",
        "avg_trading_value_20d",
        "return_1d",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")

    mask = (
        (result["market_cap"] >= MIN_SWING_MARKET_CAP)
        & (result["avg_trading_value_20d"] >= MIN_AVG_TRADING_VALUE_20D)
        & (result["trading_value_today"] >= MIN_TODAY_TRADING_VALUE)
        & (result["price"] >= MIN_PRICE)
        & (result["return_1d"] >= -7)
        & (result["return_1d"] <= 15)
        & (~result["exclude_swing"])
    )
    return result[mask].copy()


def add_trade_plan_columns(
    df: pd.DataFrame,
    signal_date: str,
    review_date: str | None = None,
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
    result["review_date"] = review_date or add_trading_days(signal_date, 3)
    return result


def score_swing_candidates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["event_pivot_score"] = calculate_event_pivot_score(result)
    result["volume_breakout_score"] = calculate_volume_breakout_score(result)
    result["contraction_score"] = calculate_contraction_score(result)
    result["darvas_breakout_score"] = calculate_darvas_breakout_score(result)
    result["pullback_ladder_score"] = result.apply(calculate_pullback_ladder_score, axis=1)
    result["relative_strength_score"] = result["relative_return_5d"].rank(pct=True).fillna(0) * 10
    result["risk_penalty"] = result.apply(calculate_risk_penalty, axis=1)
    result["matched_setups"] = result.apply(build_matched_setups, axis=1)
    result["setup_tags"] = result.apply(build_setup_tags, axis=1)
    result["risk_flags"] = result.apply(build_risk_flags, axis=1)
    result["setup_bonus"] = result["matched_setups"].apply(count_csv_items).clip(upper=3) * 5
    result["swing_score"] = (
        result[
            [
                "event_pivot_score",
                "volume_breakout_score",
                "contraction_score",
                "darvas_breakout_score",
                "pullback_ladder_score",
                "relative_strength_score",
                "setup_bonus",
            ]
        ].sum(axis=1)
        - result["risk_penalty"]
    ).clip(lower=0).round(2)

    round_columns = [
        "price",
        "trading_value_today",
        "avg_trading_value_20d",
        "return_1d",
        "return_3d",
        "return_5d",
        "market_return_3d",
        "market_return_5d",
        "market_positive_rate_1d",
        "relative_return_5d",
        "volume_ratio_20d",
        "trading_value_ratio_20d",
        "day_range_pct",
        "close_position_in_range",
        "ma5",
        "ma20",
        "high_10d",
        "high_20d",
        "close_vs_20d_high_pct",
        "event_pivot_score",
        "volume_breakout_score",
        "contraction_score",
        "darvas_breakout_score",
        "pullback_ladder_score",
        "relative_strength_score",
        "setup_bonus",
        "risk_penalty",
    ]
    for column in round_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").round(2)

    return result


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
    if float_or_zero(row.get("trading_value_ratio_20d")) >= 8:
        penalty += 5
    if safe_divide(float_or_zero(row.get("price")), float_or_zero(row.get("ma20"))) >= 1.25:
        penalty += 5
    return penalty


def build_matched_setups(row: pd.Series) -> str:
    setups: list[str] = []
    if float_or_zero(row.get("event_pivot_score")) >= 12:
        setups.append("event_pivot")
    if float_or_zero(row.get("contraction_score")) >= 10:
        setups.append("vcp_squeeze")
    if float_or_zero(row.get("darvas_breakout_score")) >= 12:
        setups.append("darvas_breakout")
    if float_or_zero(row.get("pullback_ladder_score")) >= 9:
        setups.append("pullback_ladder")
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
    if float_or_zero(row.get("trading_value_ratio_20d")) >= 8:
        flags.append("possible_overheat")
    if safe_divide(float_or_zero(row.get("price")), float_or_zero(row.get("ma20"))) >= 1.25:
        flags.append("far_above_ma20")
    return ", ".join(dict.fromkeys(flags))


def pct_return(close: pd.Series, periods: int) -> float:
    if len(close) <= periods:
        return 0.0
    base = close.iloc[-(periods + 1)]
    latest = close.iloc[-1]
    return (safe_divide(latest, base) - 1) * 100


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
