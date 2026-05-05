from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import CSV_ENCODING, OUTPUT_COLUMNS, RESULT_DIR


def save_results(
    all_df: pd.DataFrame,
    top_df: pd.DataFrame,
    run_date: str,
    result_dir: Path = RESULT_DIR,
) -> tuple[Path, Path]:
    result_dir.mkdir(parents=True, exist_ok=True)

    all_path = result_dir / f"{run_date}_all.csv"
    top_path = result_dir / f"{run_date}_top20.csv"

    normalize_output_columns(all_df).to_csv(
        all_path,
        index=False,
        encoding=CSV_ENCODING,
    )
    normalize_output_columns(top_df).to_csv(
        top_path,
        index=False,
        encoding=CSV_ENCODING,
    )

    return all_path, top_path


def normalize_output_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in OUTPUT_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    return result[OUTPUT_COLUMNS]
