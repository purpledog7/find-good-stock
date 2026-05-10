import pandas as pd

from src.special_swing_exporter import (
    build_day_swing_phase2_prompt,
    build_day_swing_phase3_prompt,
    build_special_swing_news_dataset,
    build_special_swing_phase2_prompt,
    build_special_swing_phase3_prompt,
    save_day_swing_news_dataset,
    save_day_swing_phase2_prompt,
    save_day_swing_phase3_prompt,
    save_special_swing_all_evaluated,
    save_special_swing_candidates,
    save_special_swing_news_dataset,
    save_special_swing_news_markdown,
    save_special_swing_phase2_prompt,
    save_special_swing_phase3_prompt,
)


def sample_candidates():
    return pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "name": "alpha",
                "market": "KOSPI",
                "sector": "AI",
                "special_swing_score": 88.5,
                "day_swing_score": 92.0,
                "day_technical_score": 36.0,
                "day_liquidity_score": 14.0,
                "morning_entry_bias_score": 18.0,
                "day_setup_score": 42.0,
                "day_orb_readiness_score": 10.0,
                "day_vwap_reclaim_score": 9.0,
                "day_rvol_score": 11.0,
                "day_momentum_ignition_score": 7.0,
                "day_risk_reward_score": 5.0,
                "overnight_news_score": 34.0,
                "technical_score": 45.0,
                "theme_hits": "AI, semiconductor",
                "news_growth_score": 17.0,
                "news_daily_counts": "2026-05-06:0, 2026-05-10:2",
                "matched_conditions": "box_range, pullback, steady_volume, news_growth",
                "risk_flags": "",
            }
        ]
    )


def sample_raw_news():
    return pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "alpha",
                "news_rank": 1,
                "title": "alpha AI supply contract",
                "description": "preview body",
                "link": "https://example.com/original",
                "naver_link": "https://n.news.naver.com/article/001/1",
                "description_truncated": False,
                "pub_date": "2026-05-10T07:00:00+09:00",
                "keyword_flags": "",
            }
        ]
    )


def test_save_special_swing_all_evaluated_writes_audit_csv(tmp_path):
    evaluated = sample_candidates().assign(special_swing_eligible=True, filter_reason="pass")

    path = save_special_swing_all_evaluated(evaluated, "2026-05-11", tmp_path)

    assert path.name == "2026-05-11_special_swing_all_evaluated.csv"
    saved_df = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    assert saved_df.loc[0, "special_swing_eligible"]
    assert saved_df.loc[0, "filter_reason"] == "pass"


def test_save_special_swing_candidates_writes_top100_csv(tmp_path):
    path = save_special_swing_candidates(sample_candidates(), "2026-05-11", tmp_path, candidate_count=100)

    assert path.name == "2026-05-11_special_swing_candidates_top100.csv"
    saved_df = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    assert saved_df.loc[0, "code"] == "000001"
    assert "special_swing_score" in saved_df.columns


def test_save_special_swing_news_markdown_keeps_title_link_and_preview(tmp_path):
    path = save_special_swing_news_markdown(
        sample_raw_news(),
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        pd.Timestamp("2026-05-06T00:00:00+09:00"),
        pd.Timestamp("2026-05-10T07:30:00+09:00"),
        candidate_count=100,
    )

    content = path.read_text(encoding="utf-8")
    assert path.name == "2026-05-11_special_swing_news_raw_top100.md"
    assert "filtered by publish time" in content
    assert "### 1. alpha AI supply contract" in content
    assert "- Original link: https://example.com/original" in content
    assert "- Naver link: https://n.news.naver.com/article/001/1" in content
    assert "preview body" in content


def test_build_special_swing_news_dataset_groups_candidates_and_news():
    payload = build_special_swing_news_dataset(
        sample_candidates(),
        sample_raw_news(),
        "2026-05-11",
        pd.Timestamp("2026-05-06T00:00:00+09:00"),
        pd.Timestamp("2026-05-10T07:30:00+09:00"),
    )

    assert payload["task"].startswith("Codex should score Top100")
    assert payload["candidates"][0]["candidate"]["code"] == "000001"
    assert payload["candidates"][0]["news_items"][0]["title"] == "alpha AI supply contract"
    assert "ai_adjusted_score" in payload["phase2_output_schema"]


def test_save_special_swing_news_dataset_writes_json(tmp_path):
    path = save_special_swing_news_dataset(
        sample_candidates(),
        sample_raw_news(),
        "2026-05-11",
        tmp_path,
        pd.Timestamp("2026-05-06T00:00:00+09:00"),
        pd.Timestamp("2026-05-10T07:30:00+09:00"),
        candidate_count=100,
    )

    assert path.name == "2026-05-11_special_swing_news_dataset_top100.json"
    assert "phase2_output_schema" in path.read_text(encoding="utf-8")


def test_save_day_swing_news_dataset_writes_day_file(tmp_path):
    path = save_day_swing_news_dataset(
        sample_candidates(),
        sample_raw_news(),
        "2026-05-11",
        tmp_path,
        pd.Timestamp("2026-05-08T16:00:00+09:00"),
        pd.Timestamp("2026-05-11T08:00:00+09:00"),
        candidate_count=100,
    )

    content = path.read_text(encoding="utf-8")
    assert path.name == "2026-05-11_day_swing_news_dataset_top100.json"
    assert '"strategy": "day_swing"' in content
    assert "morning entry and afternoon exit" in content
    assert "day_orb_readiness_score" in content


def test_build_special_swing_phase2_prompt_points_to_dataset_and_outputs():
    prompt = build_special_swing_phase2_prompt(
        sample_candidates(),
        "2026-05-11",
        dataset_path="data/results/2026-05-11_special_swing_news_dataset_top100.json",
        news_path="data/results/2026-05-11_special_swing_news_raw_top100.md",
        shortlist_n=30,
        candidate_count=100,
    )

    assert "Special Swing Phase 2" in prompt
    assert "special_swing_news_dataset_top100.json" in prompt
    assert "special_swing_phase2_scored_top100.csv" in prompt
    assert "special_swing_phase2_top30.json" in prompt
    assert "news_count_score" in prompt
    assert "selection_reason" in prompt


def test_build_day_swing_phase2_prompt_points_to_day_outputs():
    prompt = build_day_swing_phase2_prompt(
        sample_candidates(),
        "2026-05-11",
        dataset_path="data/results/2026-05-11_day_swing_news_dataset_top100.json",
        news_path="data/results/2026-05-11_day_swing_news_raw_top100.md",
        shortlist_n=20,
        candidate_count=100,
    )

    assert "Day Swing Phase 2" in prompt
    assert "morning entry candidate, afternoon exit" in prompt
    assert "ORB means Opening Range Breakout" in prompt
    assert "open_setup_score" in prompt
    assert "day_swing_phase2_scored_top100.csv" in prompt
    assert "morning_entry_condition" in prompt


def test_save_special_swing_phase2_prompt_writes_prompt(tmp_path):
    path = save_special_swing_phase2_prompt(
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        dataset_path="dataset.json",
        news_path="news.md",
        shortlist_n=30,
        candidate_count=100,
    )

    assert path.name == "2026-05-11_special_swing_phase2_score_top100_to_top30_prompt.md"


def test_save_day_swing_phase2_prompt_writes_prompt(tmp_path):
    path = save_day_swing_phase2_prompt(
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        dataset_path="dataset.json",
        news_path="news.md",
        shortlist_n=20,
        candidate_count=100,
    )

    assert path.name == "2026-05-11_day_swing_phase2_score_top100_to_top20_prompt.md"


def test_build_special_swing_phase3_prompt_points_to_phase2_result():
    prompt = build_special_swing_phase3_prompt(
        sample_candidates(),
        "2026-05-11",
        phase2_top10_path="data/results/2026-05-11_special_swing_phase2_top30.json",
        news_path="data/results/2026-05-11_special_swing_news_raw_top100.md",
        shortlist_n=30,
        final_n=10,
    )

    assert "Special Swing Phase 3" in prompt
    assert "four distinct stock-specialist roles plus one leader" in prompt
    assert "News catalyst specialist" in prompt
    assert "special_swing_phase3_final_top10.csv" in prompt


def test_build_day_swing_phase3_prompt_points_to_day_result():
    prompt = build_day_swing_phase3_prompt(
        sample_candidates(),
        "2026-05-11",
        phase2_top_path="data/results/2026-05-11_day_swing_phase2_top20.json",
        news_path="data/results/2026-05-11_day_swing_news_raw_top100.md",
        shortlist_n=20,
        final_n=5,
    )

    assert "Day Swing Phase 3" in prompt
    assert "morning entry and afternoon exit" in prompt
    assert "ORB readiness, VWAP reclaim, RVOL" in prompt
    assert "day_swing_phase3_final_top5.csv" in prompt
    assert "no_trade_condition" in prompt


def test_save_special_swing_phase3_prompt_writes_prompt(tmp_path):
    path = save_special_swing_phase3_prompt(
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        phase2_top10_path="phase2.json",
        news_path="news.md",
        shortlist_n=30,
        final_n=10,
    )

    assert path.name == "2026-05-11_special_swing_phase3_debate_top30_to_top10_prompt.md"


def test_save_day_swing_phase3_prompt_writes_prompt(tmp_path):
    path = save_day_swing_phase3_prompt(
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        phase2_top_path="phase2.json",
        news_path="news.md",
        shortlist_n=20,
        final_n=5,
    )

    assert path.name == "2026-05-11_day_swing_phase3_debate_top20_to_top5_prompt.md"
