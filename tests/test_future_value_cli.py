from argparse import Namespace

import pandas as pd
import pytest

from config import (
    FUTURE_VALUE_MAX_PRICE,
    FUTURE_VALUE_NEWS_LOOKBACK_DAYS,
    FUTURE_VALUE_NEWS_MAX_ITEMS_DEFAULT,
    FUTURE_VALUE_PHASE2_TOP_N,
    FUTURE_VALUE_PHASE2_WEB_MAX_ITEMS_DEFAULT,
)
from future_value import clear_result_dir, parse_args, run, validate_args
from src.news_client import WebSearchItem


class FakePhase2Client:
    def search_web_documents(self, query, display=10, start=1):
        return [
            WebSearchItem(
                title=f"{query} profile",
                description="사원수 12명 매출액 30억원",
                link="https://example.com/profile",
            )
        ]


def test_future_value_parse_args_defaults(monkeypatch):
    monkeypatch.setattr("sys.argv", ["future_value.py"])

    args = parse_args()

    assert args.max_price == FUTURE_VALUE_MAX_PRICE == 5_000
    assert args.news_max_items == FUTURE_VALUE_NEWS_MAX_ITEMS_DEFAULT == 30
    assert args.news_lookback_days == FUTURE_VALUE_NEWS_LOOKBACK_DAYS == 90
    assert args.candidate_limit == 0
    assert args.skip_news is False
    assert args.include_phase2_research is False
    assert args.phase2_top_n == FUTURE_VALUE_PHASE2_TOP_N == 30
    assert args.phase2_web_max_items == FUTURE_VALUE_PHASE2_WEB_MAX_ITEMS_DEFAULT == 10


def test_future_value_validate_args_rejects_invalid_news_count():
    args = valid_args(news_max_items=101)

    with pytest.raises(ValueError, match="100 or less"):
        validate_args(args)


def test_future_value_validate_args_rejects_negative_candidate_limit():
    args = valid_args(candidate_limit=-1)

    with pytest.raises(ValueError, match="candidate-limit"):
        validate_args(args)


def test_future_value_validate_args_rejects_invalid_phase2_web_count():
    args = valid_args(phase2_web_max_items=101)

    with pytest.raises(ValueError, match="phase2-web-max-items"):
        validate_args(args)


def test_future_value_run_skip_news_writes_outputs(monkeypatch, tmp_path):
    result_dir = tmp_path / "data" / "results"
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "market": "KOSDAQ",
                "price": 3_000,
                "market_cap": 80_000_000_000,
                "sector": "벤처기업부",
                "industry": "AI 소프트웨어 개발업",
            }
        ]
    )

    monkeypatch.setattr("future_value.RESULT_DIR", result_dir)
    monkeypatch.setattr("future_value.find_latest_market_date", lambda *args, **kwargs: "20260511")
    monkeypatch.setattr("future_value.collect_market_snapshot", lambda *args, **kwargs: snapshot_df)

    run(valid_args(skip_news=True, skip_sector=True))

    assert (result_dir / "2026-05-11_future_value_candidates.csv").exists()
    assert (result_dir / "2026-05-11_future_value_candidates_by_theme.md").exists()
    assert (result_dir / "2026-05-11_future_value_research_prompt.md").exists()


def test_future_value_run_phase2_writes_research_outputs(monkeypatch, tmp_path):
    result_dir = tmp_path / "data" / "results"
    snapshot_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "market": "KOSDAQ",
                "price": 3_000,
                "market_cap": 80_000_000_000,
                "sector": "",
                "industry": "AI software",
            }
        ]
    )

    monkeypatch.setattr("future_value.RESULT_DIR", result_dir)
    monkeypatch.setattr("future_value.find_latest_market_date", lambda *args, **kwargs: "20260511")
    monkeypatch.setattr("future_value.collect_market_snapshot", lambda *args, **kwargs: snapshot_df)
    monkeypatch.setattr("future_value.NaverNewsClient.from_env", lambda *args, **kwargs: FakePhase2Client())

    run(
        valid_args(
            skip_news=True,
            skip_sector=True,
            include_phase2_research=True,
            phase2_top_n=1,
            phase2_web_max_items=1,
        )
    )

    assert (result_dir / "2026-05-11_future_value_phase2_research.csv").exists()
    assert (result_dir / "2026-05-11_future_value_phase2_summary.md").exists()
    assert (result_dir / "2026-05-11_future_value_phase2_web_raw.md").exists()
    assert (result_dir / "2026-05-11_future_value_phase2_ai_review_prompt.md").exists()


def test_clear_result_dir_only_removes_future_value_outputs(tmp_path):
    result_dir = tmp_path / "data" / "results"
    result_dir.mkdir(parents=True)
    future_file = result_dir / "2026-05-11_future_value_candidates.csv"
    other_file = result_dir / "2026-05-11_special_swing_candidates_top100.csv"
    future_file.write_text("future", encoding="utf-8")
    other_file.write_text("special", encoding="utf-8")

    clear_result_dir(result_dir, markers=("_future_value_",))

    assert not future_file.exists()
    assert other_file.exists()


def valid_args(**overrides):
    values = {
        "date": None,
        "max_price": 5_000,
        "candidate_limit": 0,
        "news_lookback_days": 90,
        "news_max_items": 30,
        "news_time_budget_seconds": 600,
        "news_request_sleep_seconds": 0.05,
        "news_request_timeout_seconds": 8,
        "skip_sector": True,
        "skip_news": False,
        "enrich_news_metadata": False,
        "include_phase2_research": False,
        "phase2_top_n": 30,
        "phase2_web_max_items": 10,
        "phase2_web_request_sleep_seconds": 0.05,
        "phase2_include_dart": False,
        "phase2_dart_year": "2025",
        "phase2_dart_report_code": "11011",
        "phase2_dart_fs_div": "CFS",
        "phase2_dart_request_sleep_seconds": 0.2,
    }
    values.update(overrides)
    return Namespace(**values)
