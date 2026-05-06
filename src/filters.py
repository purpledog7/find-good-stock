from __future__ import annotations

import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN
from src.criteria import DEFAULT_FILTER_CRITERIA, FilterCriteria


REQUIRED_COLUMNS = [
    "market_cap",
    AVG_TRADING_VALUE_COLUMN,
    "per",
    "pbr",
    "estimated_roe",
]


def apply_value_filters(
    df: pd.DataFrame,
    criteria: FilterCriteria = DEFAULT_FILTER_CRITERIA,
) -> pd.DataFrame:
    missing_columns = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing_columns:
        raise ValueError(f"필터에 필요한 컬럼이 없어: {', '.join(missing_columns)}")

    mask = (
        (df["market_cap"] >= criteria.min_market_cap)
        & (df[AVG_TRADING_VALUE_COLUMN] >= criteria.min_avg_trading_value)
        & (df["per"] > 0)
        & (df["per"] <= criteria.max_per)
        & (df["pbr"] > 0)
        & (df["pbr"] <= criteria.max_pbr)
        & (df["estimated_roe"] >= criteria.min_estimated_roe)
    )

    if criteria.max_market_cap is not None:
        mask &= df["market_cap"] <= criteria.max_market_cap

    return df.loc[mask].copy()
