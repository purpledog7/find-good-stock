import pandas as pd

from src.filters import apply_value_filters


def test_apply_value_filters_keeps_threshold_boundary_rows():
    df = pd.DataFrame(
        [
            {
                "market_cap": 30_000_000_000,
                "avg_trading_value_20d": 500_000_000,
                "per": 12.0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
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
                "avg_trading_value_20d": 500_000_000,
                "per": 12.0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
                "code": "low_cap",
            },
            {
                "market_cap": 30_000_000_000,
                "avg_trading_value_20d": 500_000_000,
                "per": 0,
                "pbr": 1.2,
                "estimated_roe": 8.0,
                "code": "bad_per",
            },
            {
                "market_cap": 30_000_000_000,
                "avg_trading_value_20d": 500_000_000,
                "per": 8.0,
                "pbr": 0.9,
                "estimated_roe": 10.0,
                "code": "pass",
            },
        ]
    )

    result = apply_value_filters(df)

    assert result["code"].tolist() == ["pass"]
