import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN, AVG_TRADING_VALUE_EOK_COLUMN
from src.exporter import normalize_output_columns


def test_normalize_output_columns_adds_eok_display_columns():
    df = pd.DataFrame(
        [
            {
                "date": "2026-05-06",
                "code": "000001",
                "name": "test",
                "market": "KOSPI",
                "price": 10_000,
                "market_cap": 30_448_259_800,
                "per": 10.0,
                "pbr": 1.0,
                "eps": 1000,
                "bps": 10_000,
                "estimated_roe": 10.0,
                AVG_TRADING_VALUE_COLUMN: 987_654_321,
                "score": 80.0,
            }
        ]
    )

    result = normalize_output_columns(df)

    assert result.loc[0, "market_cap_eok"] == 304.48
    assert result.loc[0, AVG_TRADING_VALUE_EOK_COLUMN] == 9.88


def test_normalize_output_columns_can_include_dart_columns():
    df = pd.DataFrame(
        [
            {
                "date": "2026-05-06",
                "rank": 1,
                "code": "000001",
                "name": "test",
                "market": "KOSPI",
                "price": 10_000,
                "market_cap": 30_448_259_800,
                "per": 10.0,
                "pbr": 1.0,
                "eps": 1000,
                "bps": 10_000,
                "estimated_roe": 10.0,
                AVG_TRADING_VALUE_COLUMN: 987_654_321,
                "score": 80.0,
            }
        ]
    )

    result = normalize_output_columns(df, include_dart=True)

    assert "operating_profit" in result.columns
    assert "debt_ratio" in result.columns
