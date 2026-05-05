import pandas as pd

from src.collector import calculate_estimated_roe, normalize_ticker_frame


def test_calculate_estimated_roe_uses_eps_divided_by_bps():
    eps = pd.Series([1000, -500, 100])
    bps = pd.Series([10_000, 10_000, 0])

    result = calculate_estimated_roe(eps, bps)

    assert result.iloc[0] == 10.0
    assert result.iloc[1] == -5.0
    assert pd.isna(result.iloc[2])


def test_normalize_ticker_frame_preserves_six_digit_codes():
    df = pd.DataFrame({"종가": [1000]}, index=["123"])

    result = normalize_ticker_frame(df)

    assert result.loc[0, "code"] == "000123"
