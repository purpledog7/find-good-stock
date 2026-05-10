import pandas as pd

from src.swing_collector import collect_swing_market_snapshot, normalize_ohlcv_frame


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


def test_collect_swing_market_snapshot_adds_fundamental_value_columns(monkeypatch):
    class FakeStockApi:
        def get_market_cap(self, date, market):
            return pd.DataFrame(
                [{"종가": 10_000, "시가총액": 100_000_000_000}],
                index=["000001"],
            )

        def get_market_fundamental(self, date, market):
            return pd.DataFrame(
                [{"PER": 8.0, "PBR": 0.8, "EPS": 1000, "BPS": 10_000}],
                index=["000001"],
            )

        def get_market_ticker_name(self, code):
            return "alpha"

    monkeypatch.setattr("src.swing_collector.require_pykrx", lambda: FakeStockApi())

    result = collect_swing_market_snapshot("KOSPI", "20260506")

    assert result.loc[0, "code"] == "000001"
    assert result.loc[0, "name"] == "alpha"
    assert result.loc[0, "per"] == 8.0
    assert result.loc[0, "pbr"] == 0.8
    assert result.loc[0, "estimated_roe"] == 10.0


def test_normalize_ohlcv_frame_accepts_standard_korean_columns():
    df = pd.DataFrame(
        [
            {
                "code": "000001",
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

    assert result.loc[0, "open"] == 10_000
    assert result.loc[0, "high"] == 10_500
    assert result.loc[0, "low"] == 9_900
    assert result.loc[0, "close"] == 10_300
