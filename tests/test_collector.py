import pandas as pd

from src.collector import (
    calculate_estimated_roe,
    get_recent_trading_dates,
    has_meaningful_market_data,
    normalize_ticker_frame,
)


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


def test_has_meaningful_market_data_rejects_zero_only_rows():
    zero_df = pd.DataFrame({"종가": [0, 0], "거래량": [0, 0]})
    valid_df = pd.DataFrame({"종가": [0, 10_000], "거래량": [0, 1_000]})

    assert not has_meaningful_market_data(zero_df)
    assert has_meaningful_market_data(valid_df)


def test_get_recent_trading_dates_searches_beyond_90_calendar_days(monkeypatch):
    class FakeStockApi:
        def get_market_ohlcv(self, date, market):
            valid_dates = {
                "20260508",
                "20260507",
                "20260207",
            }
            if date in valid_dates:
                return pd.DataFrame({"종가": [10_000], "거래량": [1_000]})
            return pd.DataFrame({"종가": [0], "거래량": [0]})

    monkeypatch.setattr("src.collector.require_pykrx", lambda: FakeStockApi())
    monkeypatch.setattr("src.collector.time.sleep", lambda seconds: None)

    result = get_recent_trading_dates("20260508", 3)

    assert result == ["20260508", "20260507", "20260207"]
