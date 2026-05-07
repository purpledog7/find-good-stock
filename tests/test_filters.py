import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN
from src.criteria import STRICT_FILTER_CRITERIA, FilterCriteria
from src.filters import apply_value_filters


def test_apply_value_filters_keeps_threshold_boundary_rows():
    df = pd.DataFrame(
        [
            {
                "market_cap": 30_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 500_000_000,
                "per": 12.0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
                "code": "000001",
            }
        ]
    )

    result = apply_value_filters(df)

    assert result["code"].tolist() == ["000001"]


def test_apply_value_filters_handles_numeric_strings():
    df = pd.DataFrame(
        [
            {
                "market_cap": "30000000000",
                AVG_TRADING_VALUE_COLUMN: "500000000",
                "per": "12.0",
                "pbr": "1.2",
                "estimated_roe": "8.0",
                "code": "000001",
            }
        ]
    )

    result = apply_value_filters(df)

    assert result["code"].tolist() == ["000001"]


def test_apply_value_filters_removes_rows_outside_rules():
    df = pd.DataFrame(
        [
            {
                "market_cap": 29_999_999_999,
                AVG_TRADING_VALUE_COLUMN: 500_000_000,
                "per": 12.0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
                "code": "low_cap",
            },
            {
                "market_cap": 30_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 500_000_000,
                "per": 0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
                "code": "bad_per",
            },
            {
                "market_cap": 30_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 500_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "pass",
            },
        ]
    )

    result = apply_value_filters(df)

    assert result["code"].tolist() == ["pass"]


def test_apply_value_filters_supports_strict_criteria():
    df = pd.DataFrame(
        [
            {
                "market_cap": 30_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 500_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "default_only",
            },
            {
                "market_cap": 50_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 1_000_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "strict_pass",
            },
        ]
    )

    result = apply_value_filters(df, STRICT_FILTER_CRITERIA)

    assert result["code"].tolist() == ["strict_pass"]


def test_apply_value_filters_supports_max_market_cap():
    df = pd.DataFrame(
        [
            {
                "market_cap": 100_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 1_000_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "small",
            },
            {
                "market_cap": 300_000_000_000,
                AVG_TRADING_VALUE_COLUMN: 1_000_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "large",
            },
        ]
    )

    result = apply_value_filters(
        df,
        FilterCriteria(max_market_cap=200_000_000_000),
    )

    assert result["code"].tolist() == ["small"]
