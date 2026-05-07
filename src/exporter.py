from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import (
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
    CSV_ENCODING,
    DART_OUTPUT_COLUMNS,
    OUTPUT_COLUMNS,
    RESULT_DIR,
)


def save_results(
    all_df: pd.DataFrame,
    top_df: pd.DataFrame,
    run_date: str,
    result_dir: Path = RESULT_DIR,
    include_summary: bool = False,
    include_dart: bool = False,
    top_n: int | None = None,
) -> tuple[Path, Path]:
    result_dir.mkdir(parents=True, exist_ok=True)

    all_path = result_dir / f"{run_date}_all.csv"
    top_count = top_n if top_n is not None else len(top_df)
    top_path = result_dir / f"{run_date}_top{top_count}.csv"

    normalize_output_columns(all_df, include_summary, include_dart).to_csv(
        all_path,
        index=False,
        encoding=CSV_ENCODING,
    )
    normalize_output_columns(top_df, include_summary, include_dart).to_csv(
        top_path,
        index=False,
        encoding=CSV_ENCODING,
    )

    return all_path, top_path


def normalize_output_columns(
    df: pd.DataFrame,
    include_summary: bool = False,
    include_dart: bool = False,
) -> pd.DataFrame:
    result = df.copy()
    result = add_display_columns(result)
    output_columns = list(OUTPUT_COLUMNS)
    if include_dart:
        output_columns.extend(DART_OUTPUT_COLUMNS)
    if include_summary:
        output_columns.append("summary")

    for column in output_columns:
        if column not in result.columns:
            result[column] = ""
    return result[output_columns]


def add_display_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["market_cap_eok"] = to_eok(result.get("market_cap"))
    result[AVG_TRADING_VALUE_EOK_COLUMN] = to_eok(result.get(AVG_TRADING_VALUE_COLUMN))
    return result


def to_eok(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="float64")
    return (pd.to_numeric(series, errors="coerce") / 100_000_000).round(2)
