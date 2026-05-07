import pandas as pd

from src.swing_collector import normalize_ohlcv_frame


def test_normalize_ohlcv_frame_maps_krx_columns():
    df = pd.DataFrame(
        [
            {
                "code": 1,
                "시가": 10_000,
                "고가": 10_500,
                "저가": 9_900,
                "종가": 10_300,
                "거래량": 100_000,
                "거래대금": 1_030_000_000,
                "등락률": 3.0,
            }
        ]
    )

    result = normalize_ohlcv_frame(df, "20260506", "KOSPI")

    assert result.loc[0, "date"] == "2026-05-06"
    assert result.loc[0, "code"] == "000001"
    assert result.loc[0, "market"] == "KOSPI"
    assert result.loc[0, "open"] == 10_000
    assert result.loc[0, "high"] == 10_500
    assert result.loc[0, "low"] == 9_900
    assert result.loc[0, "close"] == 10_300
    assert result.loc[0, "volume"] == 100_000
    assert result.loc[0, "trading_value"] == 1_030_000_000
