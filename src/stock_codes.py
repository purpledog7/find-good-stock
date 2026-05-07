from __future__ import annotations

import pandas as pd


def normalize_stock_code(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "<na>"}:
        return ""
    return text.removesuffix(".0").zfill(6)


def normalize_stock_code_series(series: pd.Series) -> pd.Series:
    normalized = (
        series.astype("string")
        .fillna("")
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    normalized = normalized.mask(normalized.str.lower().isin({"nan", "none", "<na>"}), "")
    return normalized.where(normalized == "", normalized.str.zfill(6))
