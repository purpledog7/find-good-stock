from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from config import (
    KST_TIMEZONE,
    SPECIAL_SWING_NEWS_CUTOFF_HOUR,
    SPECIAL_SWING_NEWS_CUTOFF_MINUTE,
    SPECIAL_SWING_NEWS_LOOKBACK_DAYS,
    SPECIAL_SWING_TOP_N,
)
from src.stock_codes import normalize_stock_code, normalize_stock_code_series
from src.swing_scanner import (
    add_trade_plan_columns,
    calculate_swing_metrics,
    ensure_columns,
    numeric_series,
    parse_bool_like,
    safe_divide_series,
)


SPECIAL_SWING_COLUMNS = [
    "date",
    "market_date",
    "rank",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "price",
    "market_cap",
    "trading_value_today",
    "avg_trading_value_20d",
    "return_1d",
    "return_3d",
    "return_5d",
    "return_20d",
    "volume_ratio_20d",
    "trading_value_ratio_20d",
    "volume_consistency_ratio",
    "day_range_pct",
    "adr_20d",
    "box_range_pct",
    "box_position_pct",
    "close_vs_20d_high_pct",
    "price_vs_ma20_pct",
    "price_vs_vwap20_pct",
    "bb_width_pct",
    "bb_width_percentile_60",
    "rsi14",
    "box_score",
    "pullback_score",
    "steady_volume_score",
    "breakout_ready_score",
    "range_contraction_score",
    "volume_dryup_score",
    "reclaim_score",
    "vcp_score",
    "pocket_pivot_score",
    "avwap_reclaim_score",
    "relative_strength_score",
    "tight_base_score",
    "community_setup_score",
    "five_day_trigger_score",
    "technical_risk_penalty",
    "technical_score",
    "news_count_5d",
    "unique_news_count_5d",
    "recent_3d_news_count",
    "previous_3d_news_count",
    "recent_5d_news_count",
    "previous_5d_news_count",
    "news_slope",
    "news_growth_score",
    "news_relevance_score",
    "primary_news_score",
    "news_freshness_score",
    "direct_catalyst_score",
    "news_concentration_penalty",
    "duplicate_story_penalty",
    "theme_breadth_penalty",
    "relevant_news_count_5d",
    "primary_news_count_5d",
    "duplicate_story_count_5d",
    "noisy_news_count_5d",
    "max_daily_news_share",
    "positive_news_count",
    "negative_news_count",
    "theme_hits",
    "theme_score",
    "catalyst_score",
    "news_sentiment_hint",
    "news_daily_counts",
    "special_swing_score",
    "matched_conditions",
    "risk_flags",
    "tick_size",
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

SPECIAL_SWING_AUDIT_COLUMNS = SPECIAL_SWING_COLUMNS + [
    "special_swing_eligible",
    "filter_reason",
]

SPECIAL_NEWS_ANALYSIS_COLUMNS = [
    "code",
    "news_count_5d",
    "unique_news_count_5d",
    "recent_3d_news_count",
    "previous_3d_news_count",
    "recent_5d_news_count",
    "previous_5d_news_count",
    "news_slope",
    "news_growth_score",
    "news_relevance_score",
    "primary_news_score",
    "news_freshness_score",
    "direct_catalyst_score",
    "news_concentration_penalty",
    "duplicate_story_penalty",
    "theme_breadth_penalty",
    "relevant_news_count_5d",
    "primary_news_count_5d",
    "duplicate_story_count_5d",
    "noisy_news_count_5d",
    "max_daily_news_share",
    "positive_news_count",
    "negative_news_count",
    "theme_hits",
    "theme_score",
    "catalyst_score",
    "news_sentiment_hint",
    "news_daily_counts",
]

MIN_SPECIAL_MARKET_CAP = 30_000_000_000
MIN_SPECIAL_AVG_TRADING_VALUE_20D = 1_000_000_000
MIN_SPECIAL_TODAY_TRADING_VALUE = 500_000_000
MIN_SPECIAL_PRICE = 1_000
SPECIAL_FILTER_NUMERIC_COLUMNS = [
    "price",
    "market_cap",
    "trading_value_today",
    "avg_trading_value_20d",
    "return_1d",
    "return_5d",
    "return_20d",
    "rsi14",
]

THEME_KEYWORDS = {
    "AI": ["AI", "인공지능", "생성형", "챗봇", "데이터센터", "엔비디아"],
    "semiconductor": ["반도체", "HBM", "D램", "낸드", "파운드리", "EUV", "패키징"],
    "obesity": ["비만", "GLP-1", "위고비", "마운자로", "펩타이드", "당뇨"],
    "bio": ["바이오", "신약", "임상", "FDA", "기술이전", "품목허가"],
    "robot": ["로봇", "휴머노이드", "자동화", "스마트팩토리"],
    "power": ["전력", "전선", "변압기", "전력망", "송전", "배전"],
    "nuclear": ["원전", "원자력", "SMR", "소형모듈원전"],
    "defense": ["방산", "방위산업", "무기", "수출계약"],
    "battery": ["2차전지", "배터리", "양극재", "음극재", "전고체"],
    "shipbuilding": ["조선", "선박", "LNG선", "해운", "수주잔고"],
}

POSITIVE_NEWS_KEYWORDS = [
    "수주",
    "계약",
    "공급",
    "승인",
    "허가",
    "임상",
    "기술이전",
    "흑자전환",
    "증가",
    "성장",
    "투자",
    "증설",
    "협력",
    "MOU",
    "실적",
    "영업이익",
]

NEGATIVE_NEWS_KEYWORDS = [
    "유상증자",
    "전환사채",
    "CB",
    "BW",
    "적자",
    "손실",
    "횡령",
    "배임",
    "소송",
    "거래정지",
    "관리종목",
    "투자주의",
    "투자경고",
    "투자위험",
    "단기과열",
    "상장폐지",
    "감사의견",
    "하향",
]

DIRECT_CATALYST_KEYWORDS = [
    "수주",
    "공급계약",
    "공급 계약",
    "계약",
    "공시",
    "실적",
    "매출",
    "영업이익",
    "흑자전환",
    "승인",
    "허가",
    "임상",
    "기술이전",
    "MOU",
    "증설",
    "투자",
]

AMBIGUOUS_NAME_FALSE_POSITIVES = {
    "선진": ["선진국", "선진화", "선진시장", "선진사례", "선진입"],
    "AP위성": ["AP통신", "AP 뉴스", "AP뉴스", "Associated Press"],
}

POSITIVE_NEWS_KEYWORDS.extend(
    [
        "수주",
        "계약",
        "공급",
        "승인",
        "허가",
        "임상",
        "기술이전",
        "흑자전환",
        "증가",
        "성장",
        "투자",
        "증설",
        "협력",
        "실적",
        "영업이익",
    ]
)

NEGATIVE_NEWS_KEYWORDS.extend(
    [
        "유상증자",
        "전환사채",
        "적자",
        "손실",
        "횡령",
        "배임",
        "소송",
        "거래정지",
        "관리종목",
        "투자주의",
        "투자경고",
        "투자위험",
        "단기과열",
        "상장폐지",
        "감사의견",
        "하향",
    ]
)

SPECIAL_QUERY_TERMS = [
    "주식",
    "공시",
    "계약",
    "수주",
    "실적",
    "AI",
    "반도체",
    "비만치료제",
    "로봇",
    "원전",
    "방산",
]


def build_special_swing_technical_candidates(
    snapshot_df: pd.DataFrame,
    history_df: pd.DataFrame,
    signal_date: str,
    market_date: str,
    top_n: int = SPECIAL_SWING_TOP_N,
    review_date: str | None = None,
    review_date_5d: str | None = None,
) -> pd.DataFrame:
    evaluated_df = build_special_swing_technical_universe(
        snapshot_df=snapshot_df,
        history_df=history_df,
        signal_date=signal_date,
        market_date=market_date,
        review_date=review_date,
        review_date_5d=review_date_5d,
    )
    return select_special_swing_technical_candidates(evaluated_df, top_n)


def build_special_swing_technical_universe(
    snapshot_df: pd.DataFrame,
    history_df: pd.DataFrame,
    signal_date: str,
    market_date: str,
    review_date: str | None = None,
    review_date_5d: str | None = None,
) -> pd.DataFrame:
    if snapshot_df.empty or history_df.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_AUDIT_COLUMNS)

    metrics_df = calculate_swing_metrics(history_df)
    if metrics_df.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_AUDIT_COLUMNS)
    metrics_df["code"] = normalize_stock_code_series(metrics_df["code"])

    snapshot_columns = [
        "code",
        "name",
        "market",
        "sector",
        "industry",
        "market_cap",
        "exclude_swing",
    ]
    snapshot = ensure_columns(snapshot_df, snapshot_columns)[snapshot_columns].copy()
    snapshot["code"] = normalize_stock_code_series(snapshot["code"])
    snapshot = snapshot[snapshot["code"] != ""].drop_duplicates("code", keep="first")

    result = metrics_df.merge(snapshot, on="code", how="left", suffixes=("", "_snapshot"))
    result["market"] = result["market_snapshot"].fillna(result.get("market", ""))
    result = result.drop(columns=["market_snapshot"], errors="ignore")
    result = result.fillna({"name": "", "market": "", "sector": "", "industry": ""})
    result["exclude_swing"] = result["exclude_swing"].apply(parse_bool_like)
    result = coerce_special_filter_numeric_columns(result)

    result = add_special_technical_scores(result)
    result = add_trade_plan_columns(result, signal_date, review_date, review_date_5d)
    result["date"] = signal_date
    result["market_date"] = market_date
    result["special_swing_eligible"] = build_special_swing_eligible_mask(result)
    result["filter_reason"] = result.apply(build_special_filter_reason, axis=1)
    result = sort_special_technical_candidates(result).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)

    return normalize_special_audit_columns(result.copy())


def select_special_swing_technical_candidates(
    evaluated_df: pd.DataFrame,
    top_n: int = SPECIAL_SWING_TOP_N,
) -> pd.DataFrame:
    if evaluated_df.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_COLUMNS)

    result = evaluated_df.copy()
    if "special_swing_eligible" in result.columns:
        result = result[result["special_swing_eligible"].apply(parse_bool_like)].copy()
    else:
        result = result[build_special_swing_eligible_mask(result)].copy()

    if result.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_COLUMNS)

    result = sort_special_technical_candidates(result).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    return normalize_special_columns(result.head(top_n).copy())


def sort_special_technical_candidates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in [
        "special_swing_eligible",
        "technical_score",
        "community_setup_score",
        "five_day_trigger_score",
        "breakout_ready_score",
        "steady_volume_score",
        "trading_value_today",
    ]:
        if column not in result.columns:
            result[column] = 0
    return result.sort_values(
        by=[
            "special_swing_eligible",
            "technical_score",
            "community_setup_score",
            "five_day_trigger_score",
            "breakout_ready_score",
            "steady_volume_score",
            "trading_value_today",
        ],
        ascending=[False, False, False, False, False, False, False],
    )


def apply_special_swing_hard_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = coerce_special_filter_numeric_columns(df)
    mask = build_special_hard_filter_mask(result)
    return result[mask].copy()


def coerce_special_filter_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in SPECIAL_FILTER_NUMERIC_COLUMNS:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def build_special_hard_filter_mask(df: pd.DataFrame) -> pd.Series:
    result = coerce_special_filter_numeric_columns(df)
    return (
        (result["market_cap"] >= MIN_SPECIAL_MARKET_CAP)
        & (result["avg_trading_value_20d"] >= MIN_SPECIAL_AVG_TRADING_VALUE_20D)
        & (result["trading_value_today"] >= MIN_SPECIAL_TODAY_TRADING_VALUE)
        & (result["price"] >= MIN_SPECIAL_PRICE)
        & (result["return_1d"].between(-8, 8))
        & (result["return_5d"].between(-15, 12))
        & (result["return_20d"].between(-30, 35))
        & (result["rsi14"].between(30, 70))
        & (~result["exclude_swing"].apply(parse_bool_like))
    )


def build_special_setup_filter_mask(df: pd.DataFrame) -> pd.Series:
    return (
        (numeric_series(df, "box_score") >= 10)
        & (numeric_series(df, "pullback_score") >= 10)
        & (numeric_series(df, "steady_volume_score") >= 10)
        & (numeric_series(df, "five_day_trigger_score") >= 8)
        & has_community_setup_signal(df)
        & (numeric_series(df, "technical_score") >= 30)
    )


def build_special_swing_eligible_mask(df: pd.DataFrame) -> pd.Series:
    return build_special_hard_filter_mask(df) & build_special_setup_filter_mask(df)


def build_special_filter_reason(row: pd.Series) -> str:
    reasons: list[str] = []
    if safe_row_number(row, "market_cap") < MIN_SPECIAL_MARKET_CAP:
        reasons.append("market_cap_lt_min")
    if safe_row_number(row, "avg_trading_value_20d") < MIN_SPECIAL_AVG_TRADING_VALUE_20D:
        reasons.append("avg_trading_value_lt_min")
    if safe_row_number(row, "trading_value_today") < MIN_SPECIAL_TODAY_TRADING_VALUE:
        reasons.append("today_trading_value_lt_min")
    if safe_row_number(row, "price") < MIN_SPECIAL_PRICE:
        reasons.append("price_lt_min")
    if not (-8 <= safe_row_number(row, "return_1d") <= 8):
        reasons.append("return_1d_out_of_range")
    if not (-15 <= safe_row_number(row, "return_5d") <= 12):
        reasons.append("return_5d_out_of_range")
    if not (-30 <= safe_row_number(row, "return_20d") <= 35):
        reasons.append("return_20d_out_of_range")
    if not (30 <= safe_row_number(row, "rsi14") <= 70):
        reasons.append("rsi14_out_of_range")
    if parse_bool_like(row.get("exclude_swing", False)):
        reasons.append("excluded_by_rule")
    if safe_row_number(row, "box_score") < 10:
        reasons.append("weak_box")
    if safe_row_number(row, "pullback_score") < 10:
        reasons.append("weak_pullback")
    if safe_row_number(row, "steady_volume_score") < 10:
        reasons.append("weak_steady_volume")
    if safe_row_number(row, "five_day_trigger_score") < 8:
        reasons.append("weak_3_5d_trigger")
    if not row_has_community_setup_signal(row):
        reasons.append("weak_community_setup")
    if safe_row_number(row, "technical_score") < 30:
        reasons.append("technical_score_lt_min")
    return "pass" if not reasons else ", ".join(dict.fromkeys(reasons))


def row_has_community_setup_signal(row: pd.Series) -> bool:
    return bool(
        (safe_row_number(row, "community_setup_score") >= 8)
        or (safe_row_number(row, "vcp_score") >= 14)
        or (safe_row_number(row, "tight_base_score") >= 11)
        or (safe_row_number(row, "avwap_reclaim_score") >= 12)
        or (safe_row_number(row, "pocket_pivot_score") >= 10)
        or (safe_row_number(row, "relative_strength_score") >= 12)
    )


def safe_row_number(row: pd.Series, column: str) -> float:
    value = pd.to_numeric(pd.Series([row.get(column)]), errors="coerce").iloc[0]
    if pd.isna(value):
        return float("-inf")
    return float(value)


def add_special_technical_scores(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    price = numeric_series(result, "price")
    low_20d = numeric_series(result, "low_20d")
    high_20d = numeric_series(result, "high_20d")
    avg_volume_5d = numeric_series(result, "avg_volume_5d")
    avg_volume_20d = numeric_series(result, "avg_volume_20d")

    result["box_range_pct"] = ((safe_divide_series(high_20d, low_20d) - 1) * 100).fillna(0)
    result["box_position_pct"] = (
        safe_divide_series(price - low_20d, high_20d - low_20d) * 100
    ).fillna(0)
    result["volume_consistency_ratio"] = safe_divide_series(avg_volume_5d, avg_volume_20d).fillna(0)

    result["box_score"] = calculate_box_score(result)
    result["pullback_score"] = calculate_pullback_score(result)
    result["steady_volume_score"] = calculate_steady_volume_score(result)
    result["breakout_ready_score"] = calculate_breakout_ready_score(result)
    result["range_contraction_score"] = calculate_range_contraction_score(result)
    result["volume_dryup_score"] = calculate_volume_dryup_score(result)
    result["reclaim_score"] = calculate_reclaim_score(result)
    result["vcp_score"] = calculate_vcp_score(result)
    result["pocket_pivot_score"] = calculate_pocket_pivot_score(result)
    result["avwap_reclaim_score"] = calculate_avwap_reclaim_score(result)
    result["relative_strength_score"] = calculate_relative_strength_score(result)
    result["tight_base_score"] = calculate_tight_base_score(result)
    result["community_setup_score"] = calculate_community_setup_score(result)
    result["five_day_trigger_score"] = calculate_five_day_trigger_score(result)
    result["technical_risk_penalty"] = calculate_special_technical_risk_penalty(result)
    result["technical_score"] = (
        result["box_score"]
        + result["pullback_score"]
        + result["steady_volume_score"]
        + result["breakout_ready_score"]
        + result["five_day_trigger_score"]
        + result["community_setup_score"]
        - result["technical_risk_penalty"]
    ).clip(lower=0)
    result["matched_conditions"] = result.apply(build_technical_condition_text, axis=1)
    result["risk_flags"] = result.apply(build_special_risk_flags, axis=1)
    return round_special_numeric_columns(result)


def calculate_box_score(df: pd.DataFrame) -> pd.Series:
    box_range = numeric_series(df, "box_range_pct")
    bb_percentile = numeric_series(df, "bb_width_percentile_60", 100)
    box_position = numeric_series(df, "box_position_pct")
    return_20d = numeric_series(df, "return_20d")

    return (
        box_range.between(6, 25).astype(int) * 8
        + box_range.between(8, 18).astype(int) * 4
        + (bb_percentile <= 55).astype(int) * 5
        + box_position.between(25, 82).astype(int) * 4
        + (return_20d.abs() <= 12).astype(int) * 3
    )


def calculate_pullback_score(df: pd.DataFrame) -> pd.Series:
    close_vs_high = numeric_series(df, "close_vs_20d_high_pct")
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")
    price_vs_vwap20 = numeric_series(df, "price_vs_vwap20_pct")
    rsi = numeric_series(df, "rsi14", 50)
    return_5d = numeric_series(df, "return_5d")

    return (
        close_vs_high.between(-18, -3).astype(int) * 8
        + price_vs_ma20.between(-10, 2).astype(int) * 4
        + price_vs_vwap20.between(-10, 2).astype(int) * 4
        + rsi.between(38, 58).astype(int) * 3
        + return_5d.between(-12, 4).astype(int) * 3
    )


def calculate_steady_volume_score(df: pd.DataFrame) -> pd.Series:
    avg_trading_value = numeric_series(df, "avg_trading_value_20d")
    volume_ratio = numeric_series(df, "volume_ratio_20d")
    trading_value_ratio = numeric_series(df, "trading_value_ratio_20d")
    consistency = numeric_series(df, "volume_consistency_ratio")

    liquidity_part = (avg_trading_value / 5_000_000_000).clip(0, 1) * 6
    return (
        liquidity_part
        + volume_ratio.between(0.6, 2.5).astype(int) * 5
        + trading_value_ratio.between(0.6, 2.5).astype(int) * 4
        + consistency.between(0.75, 1.8).astype(int) * 5
    )


def calculate_breakout_ready_score(df: pd.DataFrame) -> pd.Series:
    close_vs_high = numeric_series(df, "close_vs_20d_high_pct")
    box_position = numeric_series(df, "box_position_pct")
    accumulation = numeric_series(df, "accumulation_5d")
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")

    return (
        close_vs_high.between(-12, -3).astype(int) * 5
        + box_position.between(45, 85).astype(int) * 5
        + (accumulation > 0).astype(int) * 3
        + (price_vs_ma20 >= -5).astype(int) * 2
    )


def calculate_range_contraction_score(df: pd.DataFrame) -> pd.Series:
    day_range = numeric_series(df, "day_range_pct")
    adr = numeric_series(df, "adr_20d")
    avg_range_5d = numeric_series(df, "avg_range_5d")
    bb_percentile = numeric_series(df, "bb_width_percentile_60", 100)
    box_range = numeric_series(df, "box_range_pct")

    return (
        ((day_range > 0) & (day_range <= adr * 0.8)).astype(int) * 4
        + ((avg_range_5d > 0) & (avg_range_5d <= adr * 0.9)).astype(int) * 3
        + (bb_percentile <= 35).astype(int) * 5
        + (bb_percentile.between(35.01, 55)).astype(int) * 3
        + box_range.between(8, 18).astype(int) * 3
    )


def calculate_volume_dryup_score(df: pd.DataFrame) -> pd.Series:
    volume_ratio = numeric_series(df, "volume_ratio_20d")
    trading_value_ratio = numeric_series(df, "trading_value_ratio_20d")
    consistency = numeric_series(df, "volume_consistency_ratio")
    trading_value_today = numeric_series(df, "trading_value_today")
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")

    return (
        volume_ratio.between(0.55, 1.2).astype(int) * 5
        + trading_value_ratio.between(0.55, 1.4).astype(int) * 4
        + consistency.between(0.75, 1.35).astype(int) * 4
        + (trading_value_today >= 2_000_000_000).astype(int) * 3
        + price_vs_ma20.between(-8, 2).astype(int) * 2
    )


def calculate_reclaim_score(df: pd.DataFrame) -> pd.Series:
    price = numeric_series(df, "price")
    ma5 = numeric_series(df, "ma5")
    ma10 = numeric_series(df, "ma10")
    price_vs_ma5_pct = ((safe_divide_series(price, ma5) - 1) * 100).fillna(0)
    price_vs_ma10_pct = ((safe_divide_series(price, ma10) - 1) * 100).fillna(0)
    price_vs_vwap20 = numeric_series(df, "price_vs_vwap20_pct")
    return_1d = numeric_series(df, "return_1d")
    return_5d = numeric_series(df, "return_5d")
    close_position = numeric_series(df, "close_position_in_range")

    return (
        (price_vs_ma5_pct >= -0.3).astype(int) * 4
        + (price_vs_ma10_pct >= -1.0).astype(int) * 3
        + (price_vs_vwap20 >= -2.0).astype(int) * 3
        + ((return_1d > 0) & (return_5d <= 2)).astype(int) * 4
        + (close_position >= 55).astype(int) * 2
    )


def calculate_vcp_score(df: pd.DataFrame) -> pd.Series:
    bb_percentile = numeric_series(df, "bb_width_percentile_60", 100)
    avg_range_5d = numeric_series(df, "avg_range_5d")
    avg_range_20d = numeric_series(df, "avg_range_20d")
    box_range = numeric_series(df, "box_range_pct")
    close_vs_high = numeric_series(df, "close_vs_20d_high_pct")
    volume_ratio = numeric_series(df, "volume_ratio_20d")
    range_contraction = numeric_series(df, "range_contraction_score")
    volume_dryup = numeric_series(df, "volume_dryup_score")

    tightening = (avg_range_5d > 0) & (avg_range_20d > 0) & (avg_range_5d <= avg_range_20d * 0.9)
    return (
        (range_contraction >= 8).astype(int) * 5
        + (volume_dryup >= 8).astype(int) * 5
        + (bb_percentile <= 35).astype(int) * 4
        + tightening.astype(int) * 3
        + box_range.between(6, 18).astype(int) * 3
        + close_vs_high.between(-12, -2).astype(int) * 3
        + volume_ratio.between(0.55, 1.2).astype(int) * 2
    )


def calculate_pocket_pivot_score(df: pd.DataFrame) -> pd.Series:
    pocket_ratio = numeric_series(df, "pocket_pivot_volume_ratio")
    return_1d = numeric_series(df, "return_1d")
    close_position = numeric_series(df, "close_position_in_range")
    volume_ratio = numeric_series(df, "volume_ratio_20d")
    price = numeric_series(df, "price")
    ma10 = numeric_series(df, "ma10")
    price_vs_ma10_pct = ((safe_divide_series(price, ma10) - 1) * 100).fillna(0)
    trading_value = numeric_series(df, "trading_value_today")

    return (
        (pocket_ratio >= 1).astype(int) * 6
        + (return_1d > 0).astype(int) * 3
        + (close_position >= 60).astype(int) * 3
        + volume_ratio.between(0.9, 2.8).astype(int) * 2
        + (price_vs_ma10_pct >= -1).astype(int) * 2
        + (trading_value >= 2_000_000_000).astype(int) * 2
    )


def calculate_avwap_reclaim_score(df: pd.DataFrame) -> pd.Series:
    price_vs_avwap = numeric_series(df, "price_vs_avwap_pct")
    price_vs_vwap20 = numeric_series(df, "price_vs_vwap20_pct")
    price_vs_ma20 = numeric_series(df, "price_vs_ma20_pct")
    return_1d = numeric_series(df, "return_1d")
    close_position = numeric_series(df, "close_position_in_range")

    return (
        price_vs_avwap.between(-2, 6).astype(int) * 6
        + price_vs_vwap20.between(-3, 3).astype(int) * 4
        + price_vs_ma20.between(-3, 3).astype(int) * 3
        + (return_1d >= -1).astype(int) * 2
        + (close_position >= 50).astype(int) * 2
    )


def calculate_relative_strength_score(df: pd.DataFrame) -> pd.Series:
    relative_5d = numeric_series(df, "relative_return_5d")
    relative_20d = numeric_series(df, "relative_return_20d")
    close_vs_high = numeric_series(df, "close_vs_20d_high_pct")
    return_20d = numeric_series(df, "return_20d")
    market_positive_rate = numeric_series(df, "market_positive_rate_1d", 50)

    return (
        (relative_5d >= 0).astype(int) * 4
        + (relative_20d >= 0).astype(int) * 5
        + (close_vs_high >= -12).astype(int) * 4
        + return_20d.between(-5, 25).astype(int) * 2
        + (market_positive_rate >= 40).astype(int) * 2
    )


def calculate_tight_base_score(df: pd.DataFrame) -> pd.Series:
    avg_range_5d = numeric_series(df, "avg_range_5d")
    avg_range_20d = numeric_series(df, "avg_range_20d")
    day_range = numeric_series(df, "day_range_pct")
    adr = numeric_series(df, "adr_20d")
    bb_percentile = numeric_series(df, "bb_width_percentile_60", 100)
    return_3d = numeric_series(df, "return_3d")
    consistency = numeric_series(df, "volume_consistency_ratio")

    return (
        ((avg_range_5d > 0) & (avg_range_20d > 0) & (avg_range_5d <= avg_range_20d * 0.9)).astype(int) * 4
        + (bb_percentile <= 35).astype(int) * 5
        + ((day_range > 0) & (adr > 0) & (day_range <= adr * 0.9)).astype(int) * 3
        + (return_3d.abs() <= 4).astype(int) * 3
        + consistency.between(0.75, 1.35).astype(int) * 3
    )


def calculate_community_setup_score(df: pd.DataFrame) -> pd.Series:
    return (
        numeric_series(df, "vcp_score") * 0.35
        + numeric_series(df, "tight_base_score") * 0.25
        + numeric_series(df, "avwap_reclaim_score") * 0.20
        + numeric_series(df, "pocket_pivot_score") * 0.10
        + numeric_series(df, "relative_strength_score") * 0.10
    ).clip(upper=30).round(2)


def has_community_setup_signal(df: pd.DataFrame) -> pd.Series:
    return (
        (numeric_series(df, "community_setup_score") >= 8)
        | (numeric_series(df, "vcp_score") >= 14)
        | (numeric_series(df, "tight_base_score") >= 11)
        | (numeric_series(df, "avwap_reclaim_score") >= 12)
        | (numeric_series(df, "pocket_pivot_score") >= 10)
        | (numeric_series(df, "relative_strength_score") >= 12)
    )


def calculate_five_day_trigger_score(df: pd.DataFrame) -> pd.Series:
    return (
        numeric_series(df, "range_contraction_score")
        + numeric_series(df, "volume_dryup_score")
        + numeric_series(df, "reclaim_score")
        + numeric_series(df, "breakout_ready_score").clip(upper=10)
    ).clip(upper=40)


def calculate_special_technical_risk_penalty(df: pd.DataFrame) -> pd.Series:
    return_1d = numeric_series(df, "return_1d")
    return_5d = numeric_series(df, "return_5d")
    return_20d = numeric_series(df, "return_20d")
    rsi = numeric_series(df, "rsi14", 50)
    adr = numeric_series(df, "adr_20d")
    day_range = numeric_series(df, "day_range_pct")
    volume_ratio = numeric_series(df, "volume_ratio_20d")
    box_position = numeric_series(df, "box_position_pct")
    close_vs_high = numeric_series(df, "close_vs_20d_high_pct")

    return (
        (return_1d > 8).astype(int) * 4
        + (return_5d > 12).astype(int) * 6
        + (return_20d > 35).astype(int) * 6
        + (rsi > 70).astype(int) * 5
        + (adr > 12).astype(int) * 4
        + (day_range > 12).astype(int) * 4
        + (volume_ratio > 3.5).astype(int) * 5
        + (box_position > 92).astype(int) * 3
        + (close_vs_high > -1).astype(int) * 3
    )


def analyze_special_news(
    raw_news_df: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> pd.DataFrame:
    if raw_news_df.empty:
        return pd.DataFrame(columns=SPECIAL_NEWS_ANALYSIS_COLUMNS)

    prepared = raw_news_df.copy()
    prepared["code"] = normalize_stock_code_series(prepared["code"])
    rows: list[dict] = []
    for code, group in prepared.groupby("code", dropna=False):
        normalized_code = normalize_stock_code(code)
        if not normalized_code:
            continue
        rows.append(analyze_special_news_group(normalized_code, group, analysis_start_dt, analysis_end_dt))

    if not rows:
        return pd.DataFrame(columns=SPECIAL_NEWS_ANALYSIS_COLUMNS)
    return pd.DataFrame(rows, columns=SPECIAL_NEWS_ANALYSIS_COLUMNS)


def apply_special_news_analysis(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
    top_n: int = SPECIAL_SWING_TOP_N,
) -> pd.DataFrame:
    result = score_special_news_candidates(
        candidates_df,
        raw_news_df,
        analysis_start_dt,
        analysis_end_dt,
    )
    if result.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_COLUMNS)

    eligible = result[
        (numeric_series(result, "technical_score") >= 30)
        & (numeric_series(result, "five_day_trigger_score", 8) >= 8)
        & (numeric_series(result, "relevant_news_count_5d") >= 2)
        & (numeric_series(result, "news_relevance_score") >= 8)
        & ((numeric_series(result, "primary_news_score") >= 3) | (numeric_series(result, "direct_catalyst_score") >= 8))
        & (numeric_series(result, "news_growth_score") >= 8)
        & (numeric_series(result, "news_freshness_score") >= 4)
        & (numeric_series(result, "theme_score") > 0)
        & (numeric_series(result, "theme_breadth_penalty") <= 6)
        & ((numeric_series(result, "catalyst_score") >= 5) | (numeric_series(result, "direct_catalyst_score") >= 8))
        & (numeric_series(result, "negative_news_count") <= numeric_series(result, "positive_news_count"))
    ].copy()
    eligible = sort_special_news_scored_candidates(eligible)
    eligible["rank"] = range(1, len(eligible) + 1)
    return normalize_special_columns(eligible.head(top_n).copy())


def score_special_news_candidates(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> pd.DataFrame:
    if candidates_df.empty:
        return pd.DataFrame(columns=SPECIAL_SWING_COLUMNS)

    result = candidates_df.copy()
    result["code"] = normalize_stock_code_series(result["code"])
    result = result.drop(
        columns=[column for column in SPECIAL_NEWS_ANALYSIS_COLUMNS if column != "code"],
        errors="ignore",
    )
    news_df = analyze_special_news(raw_news_df, analysis_start_dt, analysis_end_dt)
    result = result.merge(news_df, on="code", how="left", suffixes=("", "_news"))
    result = fill_missing_special_news_columns(result)
    result["matched_conditions"] = result.apply(append_news_condition_text, axis=1)
    result["risk_flags"] = result.apply(append_news_risk_flags, axis=1)
    result["special_swing_score"] = (
        numeric_series(result, "technical_score")
        + numeric_series(result, "five_day_trigger_score")
        + numeric_series(result, "news_growth_score")
        + numeric_series(result, "news_relevance_score")
        + numeric_series(result, "primary_news_score")
        + numeric_series(result, "news_freshness_score")
        + numeric_series(result, "direct_catalyst_score")
        + numeric_series(result, "theme_score")
        + numeric_series(result, "catalyst_score")
        - (numeric_series(result, "negative_news_count") * 6)
        - numeric_series(result, "news_concentration_penalty")
        - numeric_series(result, "duplicate_story_penalty")
        - numeric_series(result, "theme_breadth_penalty")
        - (numeric_series(result, "noisy_news_count_5d") * 1.5)
    ).clip(lower=0).round(2)

    result = sort_special_news_scored_candidates(result).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    return normalize_special_columns(result.copy())


def sort_special_news_scored_candidates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in [
        "special_swing_score",
        "news_growth_score",
        "news_relevance_score",
        "primary_news_score",
        "news_freshness_score",
        "direct_catalyst_score",
        "news_concentration_penalty",
        "duplicate_story_penalty",
        "theme_breadth_penalty",
        "theme_score",
        "technical_score",
        "five_day_trigger_score",
        "negative_news_count",
        "trading_value_today",
    ]:
        if column not in result.columns:
            result[column] = 0
    return result.sort_values(
        by=[
            "special_swing_score",
            "direct_catalyst_score",
            "primary_news_score",
            "news_relevance_score",
            "news_freshness_score",
            "news_growth_score",
            "theme_score",
            "technical_score",
            "five_day_trigger_score",
            "duplicate_story_penalty",
            "theme_breadth_penalty",
            "negative_news_count",
            "trading_value_today",
        ],
        ascending=[False, False, False, False, False, False, False, False, False, True, True, True, False],
    )


def analyze_special_news_group(
    code: str,
    group: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> dict:
    timezone = analysis_end_dt.tzinfo or ZoneInfo(KST_TIMEZONE)
    parsed_rows = []
    for _, row in group.iterrows():
        pub_dt = parse_news_timestamp(row.get("pub_date"), timezone)
        if pub_dt is None:
            continue
        if not (analysis_start_dt <= pub_dt <= analysis_end_dt):
            continue
        parsed_rows.append((row, pub_dt))

    all_unique_rows = dedupe_news_rows(parsed_rows)
    company_name = first_non_empty_value(group.get("name", pd.Series(dtype="object")))
    unique_rows = [
        (row, pub_dt)
        for row, pub_dt in all_unique_rows
        if is_relevant_special_news(company_name, row)
    ]
    noisy_count = len(all_unique_rows) - len(unique_rows)
    window_dates = build_news_count_dates(analysis_end_dt, SPECIAL_SWING_NEWS_LOOKBACK_DAYS)
    daily_counts = {date.isoformat(): 0 for date in window_dates}
    for _, pub_dt in unique_rows:
        key = pub_dt.date().isoformat()
        if key in daily_counts:
            daily_counts[key] += 1

    count_values = list(daily_counts.values())
    max_daily_count = max(count_values) if count_values else 0
    max_daily_share = max_daily_count / len(unique_rows) if unique_rows else 0.0
    recent_3d = sum(count_values[-3:])
    previous_3d = sum(count_values[-6:-3])
    recent_5d = sum(count_values[-5:])
    previous_5d = sum(count_values[:5])
    news_slope = calculate_news_slope(count_values)

    positive_count = count_keyword_news(unique_rows, POSITIVE_NEWS_KEYWORDS)
    negative_count = count_keyword_news(unique_rows, NEGATIVE_NEWS_KEYWORDS)
    direct_catalyst_count = count_keyword_news(unique_rows, DIRECT_CATALYST_KEYWORDS)
    primary_news_count = count_primary_company_news(company_name, unique_rows)
    story_cluster_count = count_news_story_clusters(company_name, unique_rows)
    duplicate_story_count = max(0, len(unique_rows) - story_cluster_count)
    theme_hits, theme_news_count = find_theme_hits(unique_rows)
    news_relevance_score = calculate_news_relevance_score(len(unique_rows), noisy_count)
    concentration_penalty = calculate_news_concentration_penalty(
        len(unique_rows),
        max_daily_share,
        count_values,
    )
    primary_news_score = calculate_primary_news_score(primary_news_count, len(unique_rows))
    freshness_score = calculate_news_freshness_score(count_values)
    duplicate_story_penalty = calculate_duplicate_story_penalty(
        len(unique_rows),
        duplicate_story_count,
    )
    news_growth_score = calculate_news_growth_score(
        len(unique_rows),
        recent_3d,
        previous_3d,
        recent_5d,
        previous_5d,
        news_slope,
        count_values,
    )
    theme_score = min(20, len(theme_hits) * 6 + min(theme_news_count, 4) * 2)
    direct_catalyst_score = calculate_direct_catalyst_score(
        direct_catalyst_count,
        positive_count,
        negative_count,
    )
    theme_breadth_penalty = calculate_theme_breadth_penalty(
        theme_hits,
        theme_news_count,
        len(unique_rows),
        direct_catalyst_score,
    )
    catalyst_score = max(
        0,
        min(
            20,
            positive_count * 2
            + direct_catalyst_score * 0.7
            + primary_news_score * 0.3
            + freshness_score * 0.4
            + theme_score * 0.3
            + news_growth_score * 0.3
            - negative_count * 5
            - concentration_penalty * 0.3
            - duplicate_story_penalty * 0.2
            - theme_breadth_penalty * 0.3,
        ),
    )

    return {
        "code": code,
        "news_count_5d": len(unique_rows),
        "unique_news_count_5d": len(unique_rows),
        "recent_3d_news_count": recent_3d,
        "previous_3d_news_count": previous_3d,
        "recent_5d_news_count": recent_5d,
        "previous_5d_news_count": previous_5d,
        "news_slope": round(news_slope, 2),
        "news_growth_score": round(news_growth_score, 2),
        "news_relevance_score": round(news_relevance_score, 2),
        "primary_news_score": round(primary_news_score, 2),
        "news_freshness_score": round(freshness_score, 2),
        "direct_catalyst_score": round(direct_catalyst_score, 2),
        "news_concentration_penalty": round(concentration_penalty, 2),
        "duplicate_story_penalty": round(duplicate_story_penalty, 2),
        "theme_breadth_penalty": round(theme_breadth_penalty, 2),
        "relevant_news_count_5d": len(unique_rows),
        "primary_news_count_5d": primary_news_count,
        "duplicate_story_count_5d": duplicate_story_count,
        "noisy_news_count_5d": noisy_count,
        "max_daily_news_share": round(max_daily_share, 2),
        "positive_news_count": positive_count,
        "negative_news_count": negative_count,
        "theme_hits": ", ".join(theme_hits),
        "theme_score": round(theme_score, 2),
        "catalyst_score": round(catalyst_score, 2),
        "news_sentiment_hint": classify_news_hint(positive_count, negative_count, theme_hits),
        "news_daily_counts": ", ".join(f"{date}:{count}" for date, count in daily_counts.items()),
    }


def build_special_stock_news_queries(name: str) -> list[str]:
    cleaned = str(name).strip()
    if not cleaned:
        return []
    return [cleaned] + [f"{cleaned} {term}" for term in SPECIAL_QUERY_TERMS]


def build_fast_special_stock_news_queries(name: str) -> list[str]:
    cleaned = str(name).strip()
    if not cleaned:
        return []
    return [cleaned]


def build_special_news_analysis_window(
    signal_date: str,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    return build_special_news_window(signal_date, SPECIAL_SWING_NEWS_LOOKBACK_DAYS, now)


def build_special_ai_news_window(
    signal_date: str,
    now: datetime | None = None,
    lookback_days: int = SPECIAL_SWING_NEWS_LOOKBACK_DAYS,
) -> tuple[datetime, datetime]:
    return build_special_news_window(signal_date, lookback_days, now)


def build_special_news_window(
    signal_date: str,
    lookback_days: int,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(KST_TIMEZONE)
    current_dt = (now or datetime.now(timezone)).astimezone(timezone)
    target_date = datetime.strptime(signal_date, "%Y-%m-%d").date()
    default_end_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        SPECIAL_SWING_NEWS_CUTOFF_HOUR,
        SPECIAL_SWING_NEWS_CUTOFF_MINUTE,
        tzinfo=timezone,
    )
    end_dt = min(default_end_dt, current_dt)
    start_date = end_dt.date() - timedelta(days=max(lookback_days, 1) - 1)
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone)
    return start_dt, end_dt


def fill_missing_special_news_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = ensure_columns(df, SPECIAL_NEWS_ANALYSIS_COLUMNS)
    numeric_columns = [
        "news_count_5d",
        "unique_news_count_5d",
        "recent_3d_news_count",
        "previous_3d_news_count",
        "recent_5d_news_count",
        "previous_5d_news_count",
        "news_slope",
        "news_growth_score",
        "news_relevance_score",
        "primary_news_score",
        "news_freshness_score",
        "direct_catalyst_score",
        "news_concentration_penalty",
        "duplicate_story_penalty",
        "theme_breadth_penalty",
        "relevant_news_count_5d",
        "primary_news_count_5d",
        "duplicate_story_count_5d",
        "noisy_news_count_5d",
        "max_daily_news_share",
        "positive_news_count",
        "negative_news_count",
        "theme_score",
        "catalyst_score",
    ]
    for column in numeric_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0)
    for column in ["theme_hits", "news_sentiment_hint", "news_daily_counts"]:
        result[column] = result[column].fillna("").astype(str)
    return result


def normalize_special_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_columns(df, SPECIAL_SWING_COLUMNS)
    return normalized[SPECIAL_SWING_COLUMNS]


def normalize_special_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_columns(df, SPECIAL_SWING_AUDIT_COLUMNS)
    return normalized[SPECIAL_SWING_AUDIT_COLUMNS]


def round_special_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    round_columns = [
        "price",
        "market_cap",
        "trading_value_today",
        "avg_trading_value_20d",
        "return_1d",
        "return_3d",
        "return_5d",
        "return_20d",
        "volume_ratio_20d",
        "trading_value_ratio_20d",
        "volume_consistency_ratio",
        "day_range_pct",
        "adr_20d",
        "box_range_pct",
        "box_position_pct",
        "close_vs_20d_high_pct",
        "price_vs_ma20_pct",
        "price_vs_vwap20_pct",
        "bb_width_pct",
        "bb_width_percentile_60",
        "rsi14",
        "box_score",
        "pullback_score",
        "steady_volume_score",
        "breakout_ready_score",
        "range_contraction_score",
        "volume_dryup_score",
        "reclaim_score",
        "vcp_score",
        "pocket_pivot_score",
        "avwap_reclaim_score",
        "relative_strength_score",
        "tight_base_score",
        "community_setup_score",
        "five_day_trigger_score",
        "technical_risk_penalty",
        "technical_score",
    ]
    for column in round_columns:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce").round(2)
    return result


def build_technical_condition_text(row: pd.Series) -> str:
    conditions: list[str] = []
    if row.get("box_score", 0) >= 10:
        conditions.append("box_range")
    if row.get("pullback_score", 0) >= 10:
        conditions.append("pullback")
    if row.get("steady_volume_score", 0) >= 10:
        conditions.append("steady_volume")
    if row.get("breakout_ready_score", 0) >= 8:
        conditions.append("breakout_ready")
    if row.get("range_contraction_score", 0) >= 8:
        conditions.append("range_contraction")
    if row.get("volume_dryup_score", 0) >= 8:
        conditions.append("volume_dryup")
    if row.get("reclaim_score", 0) >= 7:
        conditions.append("ma_vwap_reclaim")
    if row.get("vcp_score", 0) >= 15:
        conditions.append("vcp_tightening")
    if row.get("pocket_pivot_score", 0) >= 10:
        conditions.append("pocket_pivot")
    if row.get("avwap_reclaim_score", 0) >= 12:
        conditions.append("anchored_vwap_reclaim")
    if row.get("relative_strength_score", 0) >= 10:
        conditions.append("relative_strength")
    if row.get("tight_base_score", 0) >= 10:
        conditions.append("tight_base")
    if row.get("community_setup_score", 0) >= 12:
        conditions.append("community_setup")
    if row.get("five_day_trigger_score", 0) >= 18:
        conditions.append("five_day_trigger")
    return ", ".join(conditions)


def append_news_condition_text(row: pd.Series) -> str:
    conditions = split_csv_text(row.get("matched_conditions", ""))
    if row.get("news_growth_score", 0) >= 8:
        conditions.append("news_growth")
    if row.get("news_relevance_score", 0) >= 14:
        conditions.append("news_relevance_ok")
    if row.get("direct_catalyst_score", 0) >= 8:
        conditions.append("direct_catalyst")
    if str(row.get("theme_hits", "")).strip():
        conditions.append("theme_news")
    if row.get("catalyst_score", 0) >= 5:
        conditions.append("catalyst_watch")
    return ", ".join(dict.fromkeys(conditions))


def build_special_risk_flags(row: pd.Series) -> str:
    flags: list[str] = []
    if row.get("adr_20d", 0) > 12:
        flags.append("high_adr")
    if row.get("volume_ratio_20d", 0) > 3.5:
        flags.append("volume_spike")
    if row.get("return_20d", 0) > 35:
        flags.append("overextended_20d")
    if row.get("box_position_pct", 0) > 92:
        flags.append("near_box_top_chase")
    if row.get("rsi14", 50) > 70:
        flags.append("rsi_overheated")
    if row.get("community_setup_score", 0) < 8:
        flags.append("weak_community_setup")
    if row.get("relative_strength_score", 0) < 6:
        flags.append("weak_relative_strength")
    if row.get("pocket_pivot_score", 0) < 4 and row.get("volume_ratio_20d", 0) > 2.8:
        flags.append("volume_without_pocket_pivot")
    return ", ".join(flags)


def append_news_risk_flags(row: pd.Series) -> str:
    flags = split_csv_text(row.get("risk_flags", ""))
    if row.get("negative_news_count", 0) > 0:
        flags.append("negative_news")
    if row.get("positive_news_count", 0) < row.get("negative_news_count", 0):
        flags.append("negative_news_dominant")
    if row.get("news_concentration_penalty", 0) >= 6:
        flags.append("news_concentration")
    if row.get("noisy_news_count_5d", 0) > row.get("relevant_news_count_5d", 0):
        flags.append("noisy_news")
    if row.get("direct_catalyst_score", 0) < 4:
        flags.append("weak_direct_catalyst")
    return ", ".join(dict.fromkeys(flags))


def split_csv_text(value) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def parse_news_timestamp(value, timezone) -> datetime | None:
    timestamp = pd.to_datetime(value, errors="coerce")
    if pd.isna(timestamp):
        return None
    dt_value = timestamp.to_pydatetime()
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone)
    return dt_value.astimezone(timezone)


def dedupe_news_rows(parsed_rows: list[tuple[pd.Series, datetime]]) -> list[tuple[pd.Series, datetime]]:
    unique_rows: list[tuple[pd.Series, datetime]] = []
    seen: set[str] = set()
    for row, pub_dt in sorted(parsed_rows, key=lambda item: item[1], reverse=True):
        key = build_news_dedupe_key(row, pub_dt)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append((row, pub_dt))
    return unique_rows


def build_news_dedupe_key(row: pd.Series, pub_dt: datetime) -> str:
    link = str(row.get("link") or row.get("naver_link") or "").strip().lower()
    if link:
        return link
    title = normalize_news_text(row.get("title", ""))
    return f"{title}:{pub_dt.date().isoformat()}"


def first_non_empty_value(values: pd.Series) -> str:
    for value in values.dropna().astype(str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return ""


def is_relevant_special_news(company_name: str, row: pd.Series) -> bool:
    name = str(company_name or "").strip()
    if not name:
        return True

    raw_text = f"{row.get('title', '')} {row.get('description', '')}"
    normalized_text = normalize_news_text(raw_text)
    normalized_name = normalize_news_text(name)
    if normalized_name not in normalized_text:
        return False

    false_positive_patterns = AMBIGUOUS_NAME_FALSE_POSITIVES.get(name, [])
    if any(pattern.casefold() in raw_text.casefold() for pattern in false_positive_patterns):
        direct_contexts = [f"{name} ", f"{name},", f"{name}(", f"{name}은", f"{name}는"]
        return any(context in raw_text for context in direct_contexts)

    return True


def count_primary_company_news(
    company_name: str,
    unique_rows: list[tuple[pd.Series, datetime]],
) -> int:
    return sum(1 for row, _ in unique_rows if is_primary_company_news(company_name, row))


def is_primary_company_news(company_name: str, row: pd.Series) -> bool:
    name = str(company_name or "").strip()
    if not name:
        return False
    return normalize_news_text(name) in normalize_news_text(row.get("title", ""))


def count_news_story_clusters(
    company_name: str,
    unique_rows: list[tuple[pd.Series, datetime]],
) -> int:
    story_keys = {
        key
        for row, _ in unique_rows
        if (key := build_news_story_key(company_name, row))
    }
    return len(story_keys)


def build_news_story_key(company_name: str, row: pd.Series) -> str:
    normalized_name = normalize_news_text(company_name)
    title = normalize_news_text(row.get("title", "")).replace(normalized_name, "")
    title = re.sub(r"\d+", "#", title)
    if len(title) >= 8:
        return title[:40]

    description = normalize_news_text(row.get("description", "")).replace(normalized_name, "")
    description = re.sub(r"\d+", "#", description)
    return description[:40] if len(description) >= 8 else title


def build_news_count_dates(end_dt: datetime, days: int) -> list:
    end_date = end_dt.date()
    return [end_date - timedelta(days=offset) for offset in reversed(range(days))]


def calculate_news_slope(count_values: list[int]) -> float:
    if len(count_values) < 2:
        return 0.0
    x_values = list(range(len(count_values)))
    x_mean = sum(x_values) / len(x_values)
    y_mean = sum(count_values) / len(count_values)
    denominator = sum((x - x_mean) ** 2 for x in x_values)
    if denominator == 0:
        return 0.0
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_values, count_values))
    return numerator / denominator


def calculate_news_growth_score(
    unique_count: int,
    recent_3d: int,
    previous_3d: int,
    recent_5d: int,
    previous_5d: int,
    news_slope: float,
    count_values: list[int],
) -> float:
    score = min(unique_count, 5) * 0.8
    if unique_count >= 2:
        score += 3
    if recent_5d > previous_5d:
        score += 6
    if recent_3d >= previous_3d and recent_3d > 0:
        score += 4
    if news_slope > 0:
        score += 4
    if sum(count_values[-2:]) > 0:
        score += 3
    return min(score, 20)


def calculate_news_relevance_score(relevant_count: int, noisy_count: int) -> float:
    total_count = relevant_count + noisy_count
    if total_count <= 0:
        return 0.0
    relevance_ratio = relevant_count / total_count
    return min(20, relevance_ratio * 14 + min(relevant_count, 6))


def calculate_primary_news_score(primary_count: int, relevant_count: int) -> float:
    if relevant_count <= 0:
        return 0.0
    primary_ratio = primary_count / relevant_count
    return min(16, min(primary_count, 5) * 2 + primary_ratio * 6)


def calculate_news_freshness_score(count_values: list[int]) -> float:
    if not count_values:
        return 0.0
    recent_3d = sum(count_values[-3:])
    recent_5d = sum(count_values[-5:])
    if recent_3d == 0 and recent_5d > 0:
        return 3.0
    return min(14, recent_3d * 3 + max(0, recent_5d - recent_3d) * 1.2)


def calculate_news_concentration_penalty(
    relevant_count: int,
    max_daily_share: float,
    count_values: list[int],
) -> float:
    if relevant_count < 5:
        return 0.0

    penalty = 0.0
    if max_daily_share >= 0.85:
        penalty += 10
    elif max_daily_share >= 0.70:
        penalty += 6
    elif max_daily_share >= 0.55:
        penalty += 3

    if sum(count_values[-2:]) == 0 and max(count_values or [0]) >= 5:
        penalty += 4
    return penalty


def calculate_duplicate_story_penalty(
    relevant_count: int,
    duplicate_story_count: int,
) -> float:
    if relevant_count < 4:
        return 0.0
    duplicate_ratio = duplicate_story_count / relevant_count
    if duplicate_ratio >= 0.7:
        return 8.0
    if duplicate_ratio >= 0.5:
        return 5.0
    if duplicate_ratio >= 0.35:
        return 3.0
    return 0.0


def calculate_theme_breadth_penalty(
    theme_hits: list[str],
    theme_news_count: int,
    relevant_count: int,
    direct_catalyst_score: float,
) -> float:
    if relevant_count <= 0:
        return 0.0
    theme_ratio = theme_news_count / relevant_count
    if len(theme_hits) >= 5 and theme_ratio >= 0.8:
        return 4.0 if direct_catalyst_score >= 8 else 8.0
    if len(theme_hits) >= 4 and direct_catalyst_score < 5:
        return 4.0
    return 0.0


def calculate_direct_catalyst_score(
    direct_catalyst_count: int,
    positive_count: int,
    negative_count: int,
) -> float:
    if direct_catalyst_count <= 0:
        return max(0, min(6, positive_count * 1.5 - negative_count * 3))
    return max(0, min(20, direct_catalyst_count * 5 + positive_count - negative_count * 4))


def count_keyword_news(
    unique_rows: list[tuple[pd.Series, datetime]],
    keywords: list[str],
) -> int:
    count = 0
    for row, _ in unique_rows:
        text = normalize_news_text(f"{row.get('title', '')} {row.get('description', '')}")
        if any(normalize_news_text(keyword) in text for keyword in keywords):
            count += 1
    return count


def find_theme_hits(unique_rows: list[tuple[pd.Series, datetime]]) -> tuple[list[str], int]:
    themes: set[str] = set()
    theme_news_count = 0
    for row, _ in unique_rows:
        text = normalize_news_text(f"{row.get('title', '')} {row.get('description', '')}")
        row_has_theme = False
        for theme, keywords in THEME_KEYWORDS.items():
            if any(normalize_news_text(keyword) in text for keyword in keywords):
                themes.add(theme)
                row_has_theme = True
        if row_has_theme:
            theme_news_count += 1
    return sorted(themes), theme_news_count


def classify_news_hint(
    positive_count: int,
    negative_count: int,
    theme_hits: list[str],
) -> str:
    if negative_count > positive_count:
        return "negative"
    if positive_count > 0 and positive_count >= negative_count:
        return "positive"
    if theme_hits:
        return "theme_only"
    return "neutral"


def normalize_news_text(value) -> str:
    return "".join(str(value or "").casefold().split())
