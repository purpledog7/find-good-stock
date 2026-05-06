from __future__ import annotations

import pandas as pd

from config import (
    AVG_TRADING_VALUE_COLUMN,
    LIQUIDITY_FULL_SCORE_VALUE,
    MAX_PBR,
    MAX_PER,
    MAX_RAW_SCORE,
)


def score_stocks(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    if result.empty:
        result["score"] = pd.Series(dtype="float64")
        return result

    per = pd.to_numeric(result["per"], errors="coerce").fillna(MAX_PER)
    pbr = pd.to_numeric(result["pbr"], errors="coerce").fillna(MAX_PBR)
    roe = pd.to_numeric(result["estimated_roe"], errors="coerce").fillna(0)
    avg_trading_value = pd.to_numeric(
        result[AVG_TRADING_VALUE_COLUMN], errors="coerce"
    ).fillna(0)

    per_score = ((MAX_PER - per) / MAX_PER * 20).clip(lower=0, upper=20)
    pbr_score = ((MAX_PBR - pbr) / MAX_PBR * 20).clip(lower=0, upper=20)
    roe_score = (roe / 20 * 20).clip(lower=0, upper=20)
    liquidity_score = (
        avg_trading_value / LIQUIDITY_FULL_SCORE_VALUE * 10
    ).clip(lower=0, upper=10)

    raw_score = per_score + pbr_score + roe_score + liquidity_score
    result["score"] = (raw_score / MAX_RAW_SCORE * 100).round(2)
    return result
