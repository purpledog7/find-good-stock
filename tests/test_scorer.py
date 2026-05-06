import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN
from src.scorer import score_stocks


def test_score_stocks_converts_v1_raw_score_to_100_point_scale():
    df = pd.DataFrame(
        [
            {
                "per": 6.0,
                "pbr": 0.6,
                "estimated_roe": 10.0,
                AVG_TRADING_VALUE_COLUMN: 2_500_000_000,
            }
        ]
    )

    result = score_stocks(df)

    assert result.loc[0, "score"] == 50.0


def test_score_stocks_caps_score_components():
    df = pd.DataFrame(
        [
            {
                "per": 0.0,
                "pbr": 0.0,
                "estimated_roe": 100.0,
                AVG_TRADING_VALUE_COLUMN: 20_000_000_000,
            }
        ]
    )

    result = score_stocks(df)

    assert result.loc[0, "score"] == 100.0
