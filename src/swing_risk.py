from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from config import SWING_MARKET_RISK_CACHE_PATH
from src.stock_codes import normalize_stock_code, normalize_stock_code_series


ProgressCallback = Callable[[str], None] | None

RISK_COLUMNS = ["code", "market_risk_flags", "exclude_swing"]
SEVERE_NEWS_FLAGS = {
    "trading_halt",
    "administrative_issue",
    "market_warning",
    "investment_warning",
    "investment_risk",
    "short_term_overheat",
    "disclosure_violation",
    "delisting",
    "investment_attention",
    "audit_opinion",
    "embezzlement",
    "breach_of_trust",
}


def add_market_risk_info(
    df: pd.DataFrame,
    cache_path: Path = SWING_MARKET_RISK_CACHE_PATH,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    result = df.copy()
    if "code" not in result.columns:
        return result
    result["code"] = normalize_stock_code_series(result["code"])
    if "market_risk_flags" not in result.columns:
        result["market_risk_flags"] = ""
    if "exclude_swing" not in result.columns:
        result["exclude_swing"] = False

    risk_df = load_market_risk_info(cache_path, progress)
    if risk_df.empty:
        return result

    result = result.drop(columns=["market_risk_flags", "exclude_swing"], errors="ignore")
    result = result.merge(risk_df, on="code", how="left")
    result["market_risk_flags"] = result["market_risk_flags"].fillna("")
    result["exclude_swing"] = result["exclude_swing"].apply(parse_bool)
    return result


def load_market_risk_info(
    cache_path: Path = SWING_MARKET_RISK_CACHE_PATH,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    if not cache_path.exists():
        emit_progress(
            progress,
            f"스윙 시장경보 캐시 없음: {cache_path} (있으면 code,risk_flags,exclude_swing 컬럼으로 반영)",
        )
        return pd.DataFrame(columns=RISK_COLUMNS)

    raw_df = pd.read_csv(cache_path, dtype={"code": str}, encoding="utf-8-sig")
    if "code" not in raw_df.columns:
        return pd.DataFrame(columns=RISK_COLUMNS)

    result = raw_df.copy()
    result["code"] = normalize_stock_code_series(result["code"])
    if "market_risk_flags" not in result.columns and "risk_flags" in result.columns:
        result = result.rename(columns={"risk_flags": "market_risk_flags"})
    if "market_risk_flags" not in result.columns:
        result["market_risk_flags"] = ""
    if "exclude_swing" not in result.columns:
        result["exclude_swing"] = False
    result["exclude_swing"] = result["exclude_swing"].apply(parse_bool)
    result["market_risk_flags"] = result["market_risk_flags"].fillna("").astype(str)
    return (
        result[RISK_COLUMNS]
        .groupby("code", as_index=False)
        .agg(
            market_risk_flags=("market_risk_flags", merge_flag_values),
            exclude_swing=("exclude_swing", "any"),
        )
    )


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "exclude", "제외"}


def merge_flag_values(values: pd.Series) -> str:
    flags: list[str] = []
    for value in values:
        flags.extend(
            flag.strip()
            for flag in str(value).split(",")
            if flag.strip() and flag.strip().lower() != "nan"
        )
    return ", ".join(dict.fromkeys(flags))


def apply_news_risk_info(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
) -> pd.DataFrame:
    if candidates_df.empty or raw_news_df.empty or "keyword_flags" not in raw_news_df.columns:
        result = candidates_df.copy()
        if "news_risk_flags" not in result.columns:
            result["news_risk_flags"] = ""
        if "news_risk_penalty" not in result.columns:
            result["news_risk_penalty"] = 0
        return result

    flag_rows = []
    for code, group in raw_news_df.groupby("code"):
        flags = sorted(
            {
                flag.strip()
                for value in group["keyword_flags"].dropna()
                for flag in str(value).split(",")
                if flag.strip()
            }
        )
        severe_flags = [flag for flag in flags if flag in SEVERE_NEWS_FLAGS]
        flag_rows.append(
            {
                "code": normalize_stock_code(code),
                "news_risk_flags": ", ".join(severe_flags),
                "news_risk_penalty": 15 if severe_flags else 0,
            }
        )

    risk_df = pd.DataFrame(flag_rows)
    result = candidates_df.drop(
        columns=["news_risk_flags", "news_risk_penalty"],
        errors="ignore",
    ).merge(risk_df, on="code", how="left")
    result["news_risk_flags"] = result["news_risk_flags"].fillna("")
    result["news_risk_penalty"] = result["news_risk_penalty"].fillna(0)
    result["risk_penalty"] = pd.to_numeric(result["risk_penalty"], errors="coerce").fillna(0)
    result["swing_score"] = pd.to_numeric(result["swing_score"], errors="coerce").fillna(0)
    result["risk_penalty"] = result["risk_penalty"] + result["news_risk_penalty"]
    result["swing_score"] = (result["swing_score"] - result["news_risk_penalty"]).clip(lower=0).round(2)
    result["risk_flags"] = result.apply(merge_risk_flags, axis=1)
    sort_columns = ["swing_score"]
    if "undervaluation_score" in result.columns:
        sort_columns.append("undervaluation_score")
    sort_columns.extend(["trading_value_today", "return_1d"])
    result = result.sort_values(
        by=sort_columns,
        ascending=[False] * len(sort_columns),
    ).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    return result


def merge_risk_flags(row: pd.Series) -> str:
    flags = []
    for column in ["risk_flags", "news_risk_flags"]:
        flags.extend(
            flag.strip()
            for flag in str(row.get(column, "")).split(",")
            if flag.strip() and flag.strip().lower() != "nan"
        )
    return ", ".join(dict.fromkeys(flags))


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
