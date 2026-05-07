from src.trading_calendar import add_trading_days, next_trading_day


def test_next_trading_day_skips_weekend():
    assert next_trading_day("2026-05-09") == "2026-05-11"


def test_add_trading_days_skips_weekend_and_fixed_holiday():
    assert add_trading_days("2026-05-01", 3) == "2026-05-07"
