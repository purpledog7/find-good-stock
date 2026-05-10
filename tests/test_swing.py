from argparse import Namespace
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from config import KST_TIMEZONE, SWING_NEWS_LOOKBACK_DAYS, SWING_NEWS_MAX_ITEMS_DEFAULT, SWING_TOP_N
from swing import build_swing_news_window, parse_args, resolve_signal_date, validate_args, validate_signal_date


def test_build_swing_news_window_defaults_to_recent_five_day_window():
    start_dt, end_dt = build_swing_news_window(
        Namespace(news_from=None, news_to=None),
        "2026-05-07",
        now=datetime(2026, 5, 7, 8, 0, tzinfo=ZoneInfo(KST_TIMEZONE)),
    )

    assert SWING_NEWS_LOOKBACK_DAYS == 5
    assert start_dt.isoformat() == "2026-05-02T00:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-07T07:30:00+09:00"


def test_build_swing_news_window_caps_future_default_end_to_now():
    start_dt, end_dt = build_swing_news_window(
        Namespace(news_from=None, news_to=None),
        "2026-05-11",
        now=datetime(2026, 5, 8, 22, 0, tzinfo=ZoneInfo(KST_TIMEZONE)),
    )

    assert start_dt.isoformat() == "2026-05-03T22:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-08T22:00:00+09:00"


def test_parse_args_defaults_to_twenty_candidates_and_thirty_news_items(monkeypatch):
    monkeypatch.setattr("sys.argv", ["swing.py"])
    args = parse_args()

    assert args.top_n == SWING_TOP_N == 20
    assert args.news_max_items == SWING_NEWS_MAX_ITEMS_DEFAULT == 30


def test_build_swing_news_window_allows_overrides():
    start_dt, end_dt = build_swing_news_window(
        Namespace(
            news_from="2026-05-06T01:00:00+09:00",
            news_to="2026-05-06T02:00:00+09:00",
        ),
        "2026-05-07",
    )

    assert start_dt.isoformat() == "2026-05-06T01:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-06T02:00:00+09:00"


def test_resolve_signal_date_defaults_to_next_trading_day_after_market_date():
    assert resolve_signal_date(None, "2026-05-08") == "2026-05-11"


def test_resolve_signal_date_adjusts_non_trading_input():
    assert resolve_signal_date("2026-05-09", "2026-05-08") == "2026-05-11"


def test_validate_signal_date_rejects_same_or_past_market_date():
    with pytest.raises(ValueError, match="다음 거래일 이후"):
        validate_signal_date("2026-05-08", "2026-05-08")

    with pytest.raises(ValueError, match="다음 거래일 이후"):
        validate_signal_date("2026-05-07", "2026-05-08")


def test_validate_args_rejects_short_history_window():
    args = Namespace(
        top_n=30,
        history_days=20,
        news_max_items=50,
        backtest_signals=20,
    )

    with pytest.raises(ValueError, match="최소 21거래일"):
        validate_args(args)


def test_validate_args_rejects_news_count_over_naver_display_limit():
    args = Namespace(
        top_n=30,
        history_days=25,
        news_max_items=101,
        backtest_signals=20,
    )

    with pytest.raises(ValueError, match="100개 이하"):
        validate_args(args)
