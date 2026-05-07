import pandas as pd
from pathlib import Path

from config import AVG_TRADING_VALUE_EOK_COLUMN
from src.codex_prompt import build_codex_review_prompt


def test_build_codex_review_prompt_includes_candidate_table():
    df = pd.DataFrame(
        [
            {
                "final_rank": 1,
                "code": "000001",
                "name": "test",
                "market": "KOSPI",
                "recommendation_score": 90.0,
                "matched_profiles": "balanced, deep_value",
                "profile_count": 2,
                "best_score": 80.0,
                "per": 6.0,
                "pbr": 0.6,
                "estimated_roe": 12.0,
                "market_cap_eok": 1000.0,
                AVG_TRADING_VALUE_EOK_COLUMN: 20.0,
                "risk_note": "주요 정량 리스크는 낮은 편",
            }
        ]
    )

    prompt = build_codex_review_prompt(
        df,
        "2026-05-06",
        raw_news_path=Path("data/results/2026-05-06_news_raw.md"),
    )

    assert "Codex Review Prompt - 2026-05-06" in prompt
    assert "000001" in prompt
    assert "balanced, deep_value" in prompt
    assert "2026-05-06_news_raw.md" in prompt


def test_build_codex_review_prompt_keeps_multiline_cells_inside_table_row():
    df = pd.DataFrame(
        [
            {
                "final_rank": 1,
                "code": "000001",
                "name": "test",
                "market": "KOSPI",
                "recommendation_score": 90.0,
                "risk_note": "첫 줄\n둘째 줄 | 파이프",
            }
        ]
    )

    prompt = build_codex_review_prompt(df, "2026-05-06")

    assert "첫 줄 둘째 줄 / 파이프" in prompt
    assert "첫 줄\n둘째 줄" not in prompt
