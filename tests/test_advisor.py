from argparse import Namespace
from types import SimpleNamespace

import pandas as pd
import pytest

import advisor
from advisor import build_news_window, validate_args


def test_build_news_window_uses_previous_day_1600_to_morning_7():
    start_dt, end_dt = build_news_window(
        Namespace(news_from=None, news_to=None),
        "2026-05-06",
    )

    assert start_dt.isoformat() == "2026-05-05T16:00:00+09:00"
    assert end_dt.isoformat() == "2026-05-06T07:00:00+09:00"


def test_validate_args_rejects_non_positive_top_n():
    args = Namespace(top_n=0, news_max_items=30)

    with pytest.raises(ValueError, match="1개 이상"):
        validate_args(args)


def test_validate_args_rejects_news_count_over_naver_display_limit():
    args = Namespace(top_n=10, news_max_items=101)

    with pytest.raises(ValueError, match="1~100개"):
        validate_args(args)


def test_run_skips_news_client_when_no_recommendations(monkeypatch, tmp_path):
    args = Namespace(
        date=None,
        top_n=10,
        profile=None,
        skip_sector=True,
        include_news=True,
        news_from=None,
        news_to=None,
        news_max_items=30,
    )
    monkeypatch.setattr(advisor, "RESULT_DIR", tmp_path)
    monkeypatch.setattr(
        advisor,
        "get_profiles",
        lambda names: [SimpleNamespace(name="test")],
    )
    monkeypatch.setattr(
        advisor,
        "collect_all_stock_data",
        lambda date, progress=None: (pd.DataFrame(), "2026-05-06"),
    )
    monkeypatch.setattr(advisor, "scan_profiles", lambda df, profiles: pd.DataFrame())
    monkeypatch.setattr(
        advisor,
        "build_recommendations",
        lambda candidates, top_n: (pd.DataFrame(), pd.DataFrame()),
    )
    monkeypatch.setattr(
        advisor.NaverNewsClient,
        "from_env",
        lambda: (_ for _ in ()).throw(AssertionError("news client should not be created")),
    )
    monkeypatch.setattr(
        advisor,
        "save_advisor_results",
        lambda candidates, recommendations, run_date, result_dir, top_n: (
            tmp_path / "candidates.csv",
            tmp_path / "recommendations.csv",
        ),
    )
    monkeypatch.setattr(
        advisor,
        "save_codex_review_prompt",
        lambda recommendations, run_date, result_dir, raw_news_path=None: tmp_path / "prompt.md",
    )

    advisor.run(args)
