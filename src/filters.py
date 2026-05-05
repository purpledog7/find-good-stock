from __future__ import annotations

import pandas as pd

from config import (
    MAX_PBR,
    MAX_PER,
    MIN_AVG_TRADING_VALUE_20D,
    MIN_ESTIMATED_ROE,
    MIN_MARKET_CAP,
)


REQUIRED_COLUMNS = [
    "market_cap",
    "avg_trading_value_20d",
    "per",
    "pbr",
    "estimated_roe",
]


def apply_value_filters(df: pd.DataFrame) -> pd.DataFrame:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"필터에 필요한 컬럼이 없어: {', '.join(missing_columns)}")

    mask = (
        (df["market_cap"] >= MIN_MARKET_CAP)
        & (df["avg_trading_value_20d"] >= MIN_AVG_TRADING_VALUE_20D)
        & (df["per"] > 0)
        & (df["per"] <= MAX_PER)
        & (df["pbr"] > 0)
        & (df["pbr"] <= MAX_PBR)
        & (df["estimated_roe"] >= MIN_ESTIMATED_ROE)
    )
    return df.loc[mask].copy()
