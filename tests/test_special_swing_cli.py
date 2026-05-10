from argparse import Namespace

import pytest

from config import (
    SPECIAL_SWING_CANDIDATE_POOL_N,
    SPECIAL_SWING_FINAL_N,
    SPECIAL_SWING_NEWS_LOOKBACK_DAYS,
    SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT,
    SPECIAL_SWING_SHORTLIST_N,
)
from special_swing import clear_result_dir, parse_args, validate_args


def test_special_swing_parse_args_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["special_swing.py"])

    args = parse_args()

    assert args.shortlist_n == SPECIAL_SWING_SHORTLIST_N == 30
    assert args.final_n == SPECIAL_SWING_FINAL_N == 10
    assert args.candidate_pool_n == SPECIAL_SWING_CANDIDATE_POOL_N == 100
    assert args.news_max_items == SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT == 50
    assert args.news_lookback_days == SPECIAL_SWING_NEWS_LOOKBACK_DAYS == 5
    assert args.news_time_budget_seconds == 180.0
    assert args.news_request_sleep_seconds == 0.05
    assert args.news_request_timeout_seconds == 8.0
    assert args.enrich_news_metadata is False


def test_special_swing_parse_args_accepts_top_n_as_shortlist_alias(monkeypatch):
    monkeypatch.setattr("sys.argv", ["special_swing.py", "--top-n", "7"])

    args = parse_args()

    assert args.shortlist_n == 7


def test_special_swing_validate_args_rejects_pool_smaller_than_shortlist():
    args = valid_args(shortlist_n=10, candidate_pool_n=9)

    with pytest.raises(ValueError, match="candidate-pool-n"):
        validate_args(args)


def test_special_swing_validate_args_rejects_final_larger_than_shortlist():
    args = valid_args(shortlist_n=10, final_n=11)

    with pytest.raises(ValueError, match="final-n"):
        validate_args(args)


def test_special_swing_validate_args_rejects_news_count_over_naver_limit():
    args = valid_args(news_max_items=101)

    with pytest.raises(ValueError, match="100 or less"):
        validate_args(args)


def test_special_swing_validate_args_rejects_negative_news_time_budget():
    args = valid_args(news_time_budget_seconds=-1)

    with pytest.raises(ValueError, match="news-time-budget-seconds"):
        validate_args(args)


def test_special_swing_validate_args_rejects_invalid_news_lookback():
    args = valid_args(news_lookback_days=0)

    with pytest.raises(ValueError, match="news-lookback-days"):
        validate_args(args)


def test_special_swing_validate_args_rejects_invalid_request_timeout():
    args = valid_args(news_request_timeout_seconds=0)

    with pytest.raises(ValueError, match="news-request-timeout-seconds"):
        validate_args(args)


def test_clear_result_dir_removes_existing_result_files(tmp_path):
    result_dir = tmp_path / "data" / "results"
    result_dir.mkdir(parents=True)
    (result_dir / "old.csv").write_text("old", encoding="utf-8")
    nested_dir = result_dir / "old_folder"
    nested_dir.mkdir()
    (nested_dir / "old.json").write_text("old", encoding="utf-8")

    clear_result_dir(result_dir)

    assert result_dir.exists()
    assert list(result_dir.iterdir()) == []


def test_clear_result_dir_rejects_unexpected_path(tmp_path):
    unsafe_dir = tmp_path / "results"

    with pytest.raises(RuntimeError, match="unexpected result directory"):
        clear_result_dir(unsafe_dir)


def valid_args(**overrides):
    values = {
        "shortlist_n": 30,
        "final_n": 10,
        "candidate_pool_n": 100,
        "history_days": 60,
        "news_max_items": 50,
        "news_lookback_days": 5,
        "news_time_budget_seconds": 180,
        "news_request_sleep_seconds": 0.05,
        "news_request_timeout_seconds": 8,
    }
    values.update(overrides)
    return Namespace(**values)
