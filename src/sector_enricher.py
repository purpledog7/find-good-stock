from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from src.stock_codes import normalize_stock_code_series


DEFAULT_SECTOR_CACHE_PATH = Path("data/cache/krx_desc.csv")
ProgressCallback = Callable[[str], None] | None


def add_sector_info(
    df: pd.DataFrame,
    cache_path: Path = DEFAULT_SECTOR_CACHE_PATH,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    result = df.copy()
    if "sector" not in result.columns:
        result["sector"] = ""
    if "industry" not in result.columns:
        result["industry"] = ""
    if "code" not in result.columns:
        return result
    result["code"] = normalize_stock_code_series(result["code"])

    try:
        sector_df = load_sector_info(cache_path, progress)
    except Exception as error:  # pragma: no cover - depends on network/package
        emit_progress(progress, f"업종 정보 보강 건너뜀: {error}")
        return result

    if sector_df.empty:
        return result

    result = result.drop(columns=["sector", "industry"], errors="ignore")
    return result.merge(sector_df, on="code", how="left").fillna(
        {"sector": "", "industry": ""}
    )


def load_sector_info(
    cache_path: Path = DEFAULT_SECTOR_CACHE_PATH,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    if cache_path.exists():
        return normalize_sector_frame(
            pd.read_csv(cache_path, dtype={"code": str}, encoding="utf-8-sig")
        )

    emit_progress(progress, "KRX 업종 정보 다운로드 중...")
    import FinanceDataReader as fdr

    raw_df = fdr.StockListing("KRX-DESC")
    sector_df = raw_df.rename(
        columns={
            "Code": "code",
            "Sector": "sector",
            "Industry": "industry",
        }
    )[["code", "sector", "industry"]].copy()
    sector_df = normalize_sector_frame(sector_df)

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    sector_df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return sector_df


def normalize_sector_frame(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in ["code", "sector", "industry"]:
        if column not in result.columns:
            result[column] = ""
    result["code"] = normalize_stock_code_series(result["code"])
    result = result[result["code"] != ""].copy()
    result = result.fillna({"sector": "", "industry": ""})
    return result[["code", "sector", "industry"]].drop_duplicates("code", keep="first")


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
