import pandas as pd

from config import AVG_TRADING_VALUE_COLUMN, AVG_TRADING_VALUE_EOK_COLUMN
from src.validator import validate_results


def valid_row(code="000001", score=90.0, rank=1):
    return {
        "date": "2026-05-06",
        "rank": rank,
        "code": code,
        "name": "테스트",
        "market": "KOSPI",
        "price": 10_000,
        "market_cap": 30_000_000_000,
        "market_cap_eok": 300.0,
        "per": 10.0,
        "pbr": 1.0,
        "eps": 1000,
        "bps": 10_000,
        "estimated_roe": 10.0,
        AVG_TRADING_VALUE_COLUMN: 500_000_000,
        AVG_TRADING_VALUE_EOK_COLUMN: 5.0,
        "score": score,
    }


def test_validate_results_passes_valid_frames():
    all_df = pd.DataFrame([valid_row("000001", 90, 1), valid_row("000002", 80, 2)])
    top_df = pd.DataFrame([valid_row("000001", 90, 1)])

    report = validate_results(all_df, top_df, "2026-05-06", 20)

    assert report.passed
    assert report.errors == []


def test_validate_results_fails_filter_violations_and_top_sorting():
    all_df = pd.DataFrame([valid_row("000001", 90)])
    top_df = pd.DataFrame(
        [
            valid_row("000001", 80, 1),
            valid_row("000002", 90, 2),
            {**valid_row("000003", 70, 3), "per": 13.0},
        ]
    )

    report = validate_results(all_df, top_df, "2026-05-06", 20)

    assert not report.passed
    assert any("PER" in error for error in report.errors)
    assert any("내림차순" in error for error in report.errors)
