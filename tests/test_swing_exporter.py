from pathlib import Path

import pandas as pd

from src.swing_scanner import SWING_CANDIDATE_COLUMNS
from src.swing_exporter import (
    build_swing_review_prompt,
    save_swing_candidates,
    save_swing_news_markdown,
)


def sample_candidates():
    return pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "제조업",
                "swing_score": 88.5,
                "event_pivot_score": 20.0,
                "volume_breakout_score": 15.0,
                "contraction_score": 5.0,
                "darvas_breakout_score": 12.0,
                "pullback_ladder_score": 10.0,
                "relative_strength_score": 8.0,
                "risk_penalty": 0,
                "matched_setups": "event_pivot, darvas_breakout",
                "setup_tags": "volume_expansion",
                "risk_flags": "",
                "price": 10_600,
                "return_1d": 6.0,
                "return_3d": 7.0,
                "return_5d": 8.0,
                "trading_value_ratio_20d": 2.9,
                "volume_ratio_20d": 3.0,
                "tick_size": 10,
                "entry_price": 10_600,
                "add_price_1": 10_170,
                "add_price_2": 9_750,
                "add_price_3": 9_540,
                "half_take_profit_price": 11_030,
                "full_take_profit_price": 11_350,
                "review_date": "2026-05-12",
            }
        ]
    )


def test_save_swing_candidates_writes_csv(tmp_path):
    path = save_swing_candidates(sample_candidates(), "2026-05-07", tmp_path)

    assert path.name == "2026-05-07_swing_candidates.csv"
    assert path.exists()
    saved_df = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    assert saved_df.columns.tolist() == SWING_CANDIDATE_COLUMNS


def test_save_swing_news_markdown_groups_news_by_candidate(tmp_path):
    raw_news_df = pd.DataFrame(
        [
            {
                "code": 1.0,
                "name": "alpha",
                "news_rank": 1,
                "title": "news title",
                "description": "news description",
                "link": "https://example.com",
                "naver_link": "https://n.news.naver.com/article/001/0000000000",
                "description_truncated": False,
                "pub_date": "2026-05-07T07:00:00+09:00",
                "keyword_flags": "",
            }
        ]
    )
    candidates = sample_candidates()
    candidates.loc[0, "code"] = 1

    path = save_swing_news_markdown(
        raw_news_df,
        candidates,
        "2026-05-07",
        tmp_path,
        pd.Timestamp("2026-05-02T00:00:00+09:00"),
        pd.Timestamp("2026-05-07T07:30:00+09:00"),
    )

    content = path.read_text(encoding="utf-8")
    assert path.name == "2026-05-07_swing_news_raw.md"
    assert "- Window: 2026-05-02T00:00:00+09:00 ~ 2026-05-07T07:30:00+09:00" in content
    assert "## 1. alpha (000001)" in content
    assert "### 1. news title" in content
    assert "- Original link: https://example.com" in content
    assert "- Naver link: https://n.news.naver.com/article/001/0000000000" in content
    assert "- Preview shortened: no" in content
    assert "Preview:" in content
    assert "news description" in content
    assert "keyword_flags" not in content


def test_save_swing_news_markdown_uses_fallback_rank_for_invalid_news_rank(tmp_path):
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "news_rank": "bad",
                "title": "news title",
                "description": "news description",
                "link": "https://example.com",
                "pub_date": "2026-05-07T07:00:00+09:00",
                "keyword_flags": "",
            }
        ]
    )

    path = save_swing_news_markdown(
        raw_news_df,
        sample_candidates(),
        "2026-05-07",
        tmp_path,
        pd.Timestamp("2026-05-05T00:00:00+09:00"),
        pd.Timestamp("2026-05-07T07:30:00+09:00"),
    )

    assert "### 1. news title" in path.read_text(encoding="utf-8")


def test_save_swing_news_markdown_tolerates_duplicate_columns(tmp_path):
    raw_news_df = pd.DataFrame(
        [
            [
                "000001",
                "alpha",
                1,
                "news title",
                "duplicate news title",
                "news description",
                "https://example.com",
                "2026-05-07T07:00:00+09:00",
                "",
            ]
        ],
        columns=[
            "code",
            "name",
            "news_rank",
            "title",
            "title",
            "description",
            "link",
            "pub_date",
            "keyword_flags",
        ],
    )
    candidates = pd.DataFrame(
        [[1, "000001", "alpha", "KOSPI", "sector-a", "sector-b", 88.5, "event_pivot"]],
        columns=[
            "rank",
            "code",
            "name",
            "market",
            "sector",
            "sector",
            "swing_score",
            "matched_setups",
        ],
    )

    path = save_swing_news_markdown(
        raw_news_df,
        candidates,
        "2026-05-07",
        tmp_path,
        pd.Timestamp("2026-05-05T00:00:00+09:00"),
        pd.Timestamp("2026-05-07T07:30:00+09:00"),
    )

    content = path.read_text(encoding="utf-8")
    assert "### 1. news title" in content
    assert "duplicate news title" not in content


def test_build_swing_review_prompt_mentions_main_and_backup_picks():
    prompt = build_swing_review_prompt(
        sample_candidates(),
        "2026-05-07",
        news_path=Path("data/results/2026-05-07_swing_news_raw.md"),
    )

    assert "Swing Review Prompt - 2026-05-07" in prompt
    assert "최종 메인 1개와 예비 2개" in prompt
    assert "KRX 호가단위" in prompt
    assert "tick_size" in prompt
    assert "event_pivot_score" in prompt
    assert "2026-05-07_swing_news_raw.md" in prompt


def test_build_swing_review_prompt_keeps_multiline_cells_inside_table_row():
    candidates = sample_candidates()
    candidates.loc[0, "risk_flags"] = "첫 줄\n둘째 줄 | 파이프"

    prompt = build_swing_review_prompt(candidates, "2026-05-07")

    assert "첫 줄 둘째 줄 / 파이프" in prompt
    assert "첫 줄\n둘째 줄" not in prompt
