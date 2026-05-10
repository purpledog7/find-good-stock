from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import CSV_ENCODING
from src.recommender import ensure_columns, format_markdown_text
from src.stock_codes import normalize_stock_code_series
from src.swing_scanner import count_csv_items


SWING_BUY_REVIEW_TOP_N = 5

SWING_BUY_REVIEW_COLUMNS = [
    "date",
    "market_date",
    "buy_rank",
    "source_rank",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "price",
    "entry_price",
    "buy_review_score",
    "upside_score",
    "pressed_price_score",
    "near_term_bounce_score",
    "buy_risk_penalty",
    "buy_review_eligible",
    "pressed_anchor_count",
    "pullback_timing_score",
    "technical_recovery_score",
    "liquidity_wake_score",
    "setup_bounce_score",
    "risk_flag_penalty",
    "buy_review_flags",
    "buy_review_reason",
    "swing_score",
    "undervaluation_score",
    "average_discount_score",
    "value_trap_penalty",
    "risk_penalty",
    "news_risk_penalty",
    "per",
    "pbr",
    "estimated_roe",
    "earnings_yield",
    "book_discount_pct",
    "price_vs_ma20_pct",
    "price_vs_ma50_pct",
    "price_vs_vwap20_pct",
    "price_vs_vwap50_pct",
    "rsi14",
    "close_position_in_range",
    "return_1d",
    "return_3d",
    "return_5d",
    "return_20d",
    "volume_ratio_20d",
    "trading_value_ratio_20d",
    "accumulation_score",
    "anchored_vwap_score",
    "bb_squeeze_score",
    "pullback_ladder_score",
    "relative_strength_score",
    "matched_setups",
    "setup_tags",
    "risk_flags",
    "review_date",
    "review_date_3d",
    "review_date_5d",
    "half_take_profit_price",
    "full_take_profit_price",
    "add_price_1",
    "add_price_2",
    "add_price_3",
]


def build_swing_buy_review(
    candidates_df: pd.DataFrame,
    top_n: int = SWING_BUY_REVIEW_TOP_N,
) -> pd.DataFrame:
    if candidates_df.empty:
        return pd.DataFrame(columns=SWING_BUY_REVIEW_COLUMNS)

    result = ensure_columns(candidates_df, source_columns()).copy()
    result["code"] = normalize_stock_code_series(result["code"])
    result["source_rank"] = to_numeric(result, "rank")
    result["pressed_anchor_count"] = calculate_pressed_anchor_count(result)
    result = result[result["pressed_anchor_count"] > 0].copy()
    if result.empty:
        return pd.DataFrame(columns=SWING_BUY_REVIEW_COLUMNS)

    result["upside_score"] = calculate_upside_score(result)
    result["pressed_price_score"] = calculate_pressed_price_score(result)
    result["pullback_timing_score"] = calculate_pullback_timing_score(result)
    result["technical_recovery_score"] = calculate_technical_recovery_score(result)
    result["liquidity_wake_score"] = calculate_liquidity_wake_score(result)
    result["setup_bounce_score"] = calculate_setup_bounce_score(result)
    result["near_term_bounce_score"] = calculate_near_term_bounce_score(result)
    result["risk_flag_penalty"] = result["risk_flags"].apply(score_risk_flags)
    result["buy_risk_penalty"] = calculate_buy_risk_penalty(result)
    result["buy_review_eligible"] = calculate_buy_review_eligible(result)
    if result["buy_review_eligible"].sum() >= top_n:
        result = result[result["buy_review_eligible"]].copy()

    result["buy_review_flags"] = result.apply(build_buy_review_flags, axis=1)
    result["buy_review_reason"] = result.apply(build_buy_review_reason, axis=1)
    result["buy_review_score"] = (
        result["upside_score"]
        + result["pressed_price_score"]
        + result["near_term_bounce_score"]
        - result["buy_risk_penalty"]
    ).clip(lower=0)

    round_columns = [
        "buy_review_score",
        "upside_score",
        "pressed_price_score",
        "near_term_bounce_score",
        "buy_risk_penalty",
        "pressed_anchor_count",
        "pullback_timing_score",
        "technical_recovery_score",
        "liquidity_wake_score",
        "setup_bounce_score",
        "risk_flag_penalty",
    ]
    for column in round_columns:
        result[column] = pd.to_numeric(result[column], errors="coerce").round(2)

    result = result.sort_values(
        by=[
            "buy_review_score",
            "near_term_bounce_score",
            "pressed_price_score",
            "upside_score",
            "swing_score",
        ],
        ascending=[False, False, False, False, False],
    ).head(top_n).reset_index(drop=True)
    result["buy_rank"] = range(1, len(result) + 1)

    return ensure_columns(result, SWING_BUY_REVIEW_COLUMNS)[SWING_BUY_REVIEW_COLUMNS]


def source_columns() -> list[str]:
    return sorted(
        set(SWING_BUY_REVIEW_COLUMNS)
        | {
            "rank",
            "event_pivot_score",
            "pocket_pivot_score",
            "ema_trend_score",
            "rsi_score",
        }
    )


def calculate_upside_score(df: pd.DataFrame) -> pd.Series:
    swing_score = to_numeric(df, "swing_score")
    undervaluation = to_numeric(df, "undervaluation_score")
    roe = to_numeric(df, "estimated_roe")
    earnings_yield = to_numeric(df, "earnings_yield")
    setup_count = df["matched_setups"].apply(count_csv_items)
    value_trap_penalty = to_numeric(df, "value_trap_penalty")

    return (
        (swing_score / 100 * 8).clip(lower=0, upper=8)
        + (undervaluation / 24 * 8).clip(lower=0, upper=8)
        + ((roe - 4) / 12 * 4).clip(lower=0, upper=4)
        + (earnings_yield / 10 * 3).clip(lower=0, upper=3)
        + setup_count.clip(upper=4) * 0.75
        - (value_trap_penalty * 0.35).clip(lower=0, upper=4)
    )


def calculate_pressed_price_score(df: pd.DataFrame) -> pd.Series:
    average_discount = to_numeric(df, "average_discount_score")
    price_vs_ma20 = to_numeric(df, "price_vs_ma20_pct")
    price_vs_ma50 = to_numeric(df, "price_vs_ma50_pct")
    price_vs_vwap20 = to_numeric(df, "price_vs_vwap20_pct")
    price_vs_vwap50 = to_numeric(df, "price_vs_vwap50_pct")

    discount_depth = (
        ((-price_vs_ma20).clip(lower=0, upper=12) / 12 * 3)
        + ((-price_vs_ma50).clip(lower=0, upper=18) / 18 * 3)
        + ((-price_vs_vwap20).clip(lower=0, upper=12) / 12 * 4)
        + ((-price_vs_vwap50).clip(lower=0, upper=18) / 18 * 3)
    )
    average_anchor_count = (
        (price_vs_ma20 <= -0.5).astype(int)
        + (price_vs_ma50 <= -0.5).astype(int)
        + (price_vs_vwap20 <= -0.5).astype(int)
        + (price_vs_vwap50 <= -0.5).astype(int)
    )
    too_deep_penalty = (
        (price_vs_ma20 <= -18).astype(int) * 3
        + (price_vs_vwap20 <= -18).astype(int) * 3
        + (price_vs_ma50 <= -30).astype(int) * 3
        + (price_vs_vwap50 <= -30).astype(int) * 3
    )
    return (
        (average_discount / 20 * 8).clip(lower=0, upper=8)
        + discount_depth
        + average_anchor_count.clip(upper=4)
        - too_deep_penalty
    ).clip(lower=0)


def calculate_pressed_anchor_count(df: pd.DataFrame) -> pd.Series:
    return (
        (to_numeric(df, "price_vs_ma20_pct") <= -0.5).astype(int)
        + (to_numeric(df, "price_vs_ma50_pct") <= -0.5).astype(int)
        + (to_numeric(df, "price_vs_vwap20_pct") <= -0.5).astype(int)
        + (to_numeric(df, "price_vs_vwap50_pct") <= -0.5).astype(int)
    )


def calculate_near_term_bounce_score(df: pd.DataFrame) -> pd.Series:
    accumulation = to_numeric(df, "accumulation_score")

    return (
        calculate_technical_recovery_score(df)
        + calculate_pullback_timing_score(df)
        + calculate_liquidity_wake_score(df)
        + calculate_setup_bounce_score(df)
        + (accumulation / 8 * 2).clip(lower=0, upper=2)
    )


def calculate_technical_recovery_score(df: pd.DataFrame) -> pd.Series:
    rsi = to_numeric(df, "rsi14")
    close_position = to_numeric(df, "close_position_in_range")

    rsi_score = (6 - (rsi - 48).abs() / 4).clip(lower=0, upper=6)
    close_score = (close_position / 12).clip(lower=0, upper=6)
    return rsi_score + close_score


def calculate_pullback_timing_score(df: pd.DataFrame) -> pd.Series:
    return_1d = to_numeric(df, "return_1d")
    return_3d = to_numeric(df, "return_3d")
    return_5d = to_numeric(df, "return_5d")

    return (
        ((return_3d >= -10) & (return_3d <= 2)).astype(int) * 3
        + ((return_5d >= -14) & (return_5d <= 3)).astype(int) * 3
        + ((return_1d >= -4) & (return_1d <= 2.5)).astype(int) * 2
    )


def calculate_liquidity_wake_score(df: pd.DataFrame) -> pd.Series:
    volume_ratio = to_numeric(df, "volume_ratio_20d")
    trading_ratio = to_numeric(df, "trading_value_ratio_20d")

    return (
        ((volume_ratio >= 0.8) & (volume_ratio <= 3.5)).astype(int) * 2
        + ((trading_ratio >= 0.8) & (trading_ratio <= 3.5)).astype(int) * 2
        + ((volume_ratio >= 1.15) & (volume_ratio <= 2.5)).astype(int) * 2
        + ((trading_ratio >= 1.15) & (trading_ratio <= 2.5)).astype(int) * 2
        - ((volume_ratio < 0.75) | (trading_ratio < 0.75)).astype(int) * 3
    )


def calculate_setup_bounce_score(df: pd.DataFrame) -> pd.Series:
    return df["matched_setups"].apply(score_bounce_setups)


def score_bounce_setups(value: str) -> float:
    setups = {item.strip() for item in clean_text(value).split(",") if item.strip()}
    score = 0.0
    if "average_discount_pullback" in setups:
        score += 3
    if "anchored_vwap_support" in setups:
        score += 2
    if "pullback_ladder" in setups:
        score += 2
    if "bb_squeeze" in setups:
        score += 2
    if "pocket_pivot" in setups:
        score += 1
    return score


def calculate_buy_risk_penalty(df: pd.DataFrame) -> pd.Series:
    risk_penalty = to_numeric(df, "risk_penalty")
    news_risk_penalty = to_numeric(df, "news_risk_penalty")
    rsi = to_numeric(df, "rsi14")
    close_position = to_numeric(df, "close_position_in_range")
    return_1d = to_numeric(df, "return_1d")
    return_5d = to_numeric(df, "return_5d")
    price_vs_vwap20 = to_numeric(df, "price_vs_vwap20_pct")

    flag_penalty = (
        to_numeric(df, "risk_flag_penalty")
        if "risk_flag_penalty" in df.columns
        else df["risk_flags"].apply(score_risk_flags)
    )
    return (
        (risk_penalty * 0.45).clip(lower=0, upper=12)
        + (news_risk_penalty * 0.7).clip(lower=0, upper=12)
        + flag_penalty
        + (rsi < 35).astype(int) * 4
        + (close_position < 25).astype(int) * 5
        + (return_1d < -5).astype(int) * 3
        + (return_1d > 5).astype(int) * 3
        + (return_5d < -16).astype(int) * 4
        + (price_vs_vwap20 < -18).astype(int) * 3
    )


def score_risk_flags(value: str) -> float:
    flags = {item.strip() for item in clean_text(value).split(",") if item.strip()}
    score = 0.0
    severe = {
        "investment_warning",
        "investment_risk",
        "trading_halt",
        "administrative_issue",
        "value_trap_quality",
    }
    soft = {
        "weak_close",
        "weak_rsi",
        "high_adr",
        "wide_bollinger_band",
        "wide_intraday_range",
        "weak_market_3d",
        "weak_market_breadth",
    }
    score += len(flags & severe) * 12
    score += len(flags & soft) * 3
    return score


def calculate_buy_review_eligible(df: pd.DataFrame) -> pd.Series:
    return (
        (to_numeric(df, "pressed_anchor_count") >= 2)
        & (to_numeric(df, "pressed_price_score") >= 8)
        & (to_numeric(df, "near_term_bounce_score") >= 18)
        & (to_numeric(df, "buy_risk_penalty") <= 9.5)
        & (to_numeric(df, "rsi14") >= 38)
        & (to_numeric(df, "rsi14") <= 62)
        & (to_numeric(df, "close_position_in_range") >= 35)
        & (to_numeric(df, "volume_ratio_20d") >= 0.6)
        & (to_numeric(df, "trading_value_ratio_20d") >= 0.6)
    )


def build_buy_review_flags(row: pd.Series) -> str:
    flags: list[str] = []
    if bool(row.get("buy_review_eligible")):
        flags.append("eligible")
    if float_value(row.get("pressed_price_score")) >= 14:
        flags.append("strong_price_discount")
    if float_value(row.get("near_term_bounce_score")) >= 18:
        flags.append("near_term_bounce")
    if float_value(row.get("volume_ratio_20d")) >= 1.15 and float_value(row.get("trading_value_ratio_20d")) >= 1.15:
        flags.append("volume_wake")
    if float_value(row.get("upside_score")) >= 17:
        flags.append("upside_value")
    if float_value(row.get("rsi14")) < 35:
        flags.append("weak_rsi_watch")
    if float_value(row.get("close_position_in_range")) < 25:
        flags.append("weak_close_watch")
    if float_value(row.get("buy_risk_penalty")) >= 10:
        flags.append("risk_check_required")
    return ", ".join(flags)


def build_buy_review_reason(row: pd.Series) -> str:
    parts: list[str] = []
    parts.append(
        f"상승여력 {float_value(row.get('upside_score')):.1f}, "
        f"눌림 {float_value(row.get('pressed_price_score')):.1f}, "
        f"단기반등 {float_value(row.get('near_term_bounce_score')):.1f}"
    )
    discount_notes = []
    for column, label in [
        ("price_vs_ma20_pct", "MA20"),
        ("price_vs_ma50_pct", "MA50"),
        ("price_vs_vwap20_pct", "VWAP20"),
        ("price_vs_vwap50_pct", "VWAP50"),
    ]:
        value = float_value(row.get(column))
        if value <= -0.5:
            discount_notes.append(f"{label} {value:.1f}%")
    if discount_notes:
        parts.append("평균대비 할인: " + ", ".join(discount_notes[:3]))
    parts.append(f"RSI {float_value(row.get('rsi14')):.1f}, 종가위치 {float_value(row.get('close_position_in_range')):.1f}")
    if float_value(row.get("volume_ratio_20d")) >= 1.15 and float_value(row.get("trading_value_ratio_20d")) >= 1.15:
        parts.append(
            f"거래활성: 거래량 {float_value(row.get('volume_ratio_20d')):.2f}배, "
            f"거래대금 {float_value(row.get('trading_value_ratio_20d')):.2f}배"
        )
    risk_flags = clean_text(row.get("risk_flags", ""))
    if risk_flags:
        parts.append(f"리스크: {risk_flags}")
    return " / ".join(parts)


def save_swing_buy_review(
    buy_review_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_buy_review_top5.csv"
    normalized_df = ensure_columns(buy_review_df, SWING_BUY_REVIEW_COLUMNS)
    normalized_df[SWING_BUY_REVIEW_COLUMNS].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_swing_buy_review_prompt(
    buy_review_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_buy_review_prompt.md"
    path.write_text(build_swing_buy_review_prompt(buy_review_df, signal_date), encoding="utf-8")
    return path


def build_swing_buy_review_prompt(
    buy_review_df: pd.DataFrame,
    signal_date: str,
) -> str:
    preview_columns = [
        "buy_rank",
        "source_rank",
        "code",
        "name",
        "buy_review_score",
        "upside_score",
        "pressed_price_score",
        "near_term_bounce_score",
        "buy_risk_penalty",
        "buy_review_eligible",
        "pressed_anchor_count",
        "pullback_timing_score",
        "technical_recovery_score",
        "liquidity_wake_score",
        "setup_bounce_score",
        "risk_flag_penalty",
        "price",
        "per",
        "pbr",
        "estimated_roe",
        "price_vs_ma20_pct",
        "price_vs_vwap20_pct",
        "rsi14",
        "close_position_in_range",
        "return_1d",
        "return_3d",
        "return_5d",
        "matched_setups",
        "review_date_3d",
        "review_date_5d",
        "buy_review_flags",
        "buy_review_reason",
    ]
    preview_df = ensure_columns(buy_review_df, preview_columns)[preview_columns].copy()
    for column in preview_df.columns:
        preview_df[column] = preview_df[column].apply(format_markdown_cell)

    return f"""# Swing Buy Review Top5 - {signal_date}

이 표는 투자 자문이 아니라 스윙 후보 20개를 2차로 줄인 매수 검토용 데이터야.

판단 축:
- 상승 여력: 저평가, 품질, 기존 스윙 점수, 세팅 수
- 가격 눌림: MA20/MA50, VWAP20/VWAP50 대비 현재가 할인
- 근시일 반등: RSI, 종가 위치, 단기 눌림, 거래 활성, 반등 세팅

{preview_df.to_markdown(index=False)}
"""


def format_markdown_cell(value) -> str:
    return format_markdown_text(value).replace("\n", " ").replace("|", "/")


def to_numeric(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[column], errors="coerce").fillna(0.0)


def clean_text(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    return str(value).strip()


def float_value(value) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
