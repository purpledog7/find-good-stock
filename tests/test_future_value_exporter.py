import pandas as pd

from src.future_value_exporter import (
    build_future_value_news_dataset,
    build_future_value_phase2_review_prompt,
    build_future_value_phase2_summary_markdown,
    build_future_value_research_prompt,
    save_future_value_candidates,
    save_future_value_news_markdown,
    save_future_value_phase2_csv,
    save_future_value_phase2_review_prompt,
    save_future_value_phase2_web_markdown,
    save_future_value_theme_markdown,
)


def sample_candidates():
    return pd.DataFrame(
        [
            {
                "date": "2026-05-11",
                "market_date": "2026-05-11",
                "rank": 1,
                "code": "000001",
                "name": "Alpha",
                "market": "KOSDAQ",
                "price": 3_000,
                "market_cap": 80_000_000_000,
                "market_cap_eok": 800,
                "sector": "벤처기업부",
                "industry": "소프트웨어 개발업",
                "theme_categories": "IT/software, AI/data_center",
                "theme_evidence": "IT/software: software | AI/data_center: AI",
                "static_theme_categories": "IT/software",
                "static_theme_evidence": "IT/software: software",
                "news_theme_categories": "AI/data_center",
                "news_theme_evidence": "AI/data_center: AI",
                "news_count": 1,
                "relevant_news_count": 1,
                "theme_news_count": 1,
                "key_news_titles": "Alpha AI platform",
                "key_news_links": "https://example.com/a",
                "naver_finance_url": "https://finance.naver.com/item/main.naver?code=000001",
                "research_queries": "Alpha 공식 홈페이지 | Alpha IR",
                "future_value_score": 55.5,
                "risk_flags": "",
            }
        ]
    )


def sample_raw_news():
    return pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "news_rank": 1,
                "title": "Alpha AI platform",
                "description": "Alpha develops data center AI software",
                "link": "https://example.com/a",
                "naver_link": "https://n.news.naver.com/a",
                "description_truncated": False,
                "pub_date": "2026-05-10T09:00:00+09:00",
                "keyword_flags": "",
            }
        ]
    )


def sample_phase2():
    return pd.DataFrame(
        [
            {
                "phase2_rank": 1,
                "rank": 1,
                "code": "000001",
                "name": "Alpha",
                "price": 3000,
                "market_cap_eok": 800,
                "theme_categories": "AI/data_center",
                "employee_count": 25,
                "employee_source_title": "Alpha profile",
                "employee_source_link": "https://example.com/profile",
                "revenue_won": 12_000_000_000,
                "revenue_eok": 120.0,
                "revenue_source": "naver_web",
                "revenue_source_title": "Alpha sales",
                "revenue_source_link": "https://example.com/sales",
                "revenue_per_employee_won": 480_000_000,
                "revenue_per_employee_eok": 4.8,
                "important_news_count": 1,
                "important_news_titles": "Alpha AI supply",
                "important_news_links": "https://example.com/news",
                "web_result_count": 2,
                "key_web_titles": "Alpha profile",
                "key_web_links": "https://example.com/profile",
                "phase2_confidence": "medium",
                "phase2_flags": "web_revenue_only",
                "phase2_summary": "Alpha phase2 summary",
            }
        ]
    )


def sample_phase2_web():
    return pd.DataFrame(
        [
            {
                "code": "000001",
                "name": "Alpha",
                "query": "Alpha 사원수",
                "result_rank": 1,
                "title": "Alpha profile",
                "description": "사원수 25명",
                "link": "https://example.com/profile",
            }
        ]
    )


def test_save_future_value_candidates_writes_csv(tmp_path):
    path = save_future_value_candidates(sample_candidates(), "2026-05-11", tmp_path)

    assert path.name == "2026-05-11_future_value_candidates.csv"
    saved_df = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    assert saved_df.loc[0, "code"] == "000001"
    assert "theme_categories" in saved_df.columns


def test_save_future_value_theme_markdown_groups_by_theme(tmp_path):
    path = save_future_value_theme_markdown(sample_candidates(), "2026-05-11", tmp_path, max_price=5_000)
    content = path.read_text(encoding="utf-8")

    assert path.name == "2026-05-11_future_value_candidates_by_theme.md"
    assert "## IT/software" in content
    assert "## AI/data_center" in content
    assert "Alpha (000001)" in content
    assert "Alpha AI platform" in content
    assert "Alpha IR" in content


def test_save_future_value_news_markdown_keeps_links_and_preview(tmp_path):
    path = save_future_value_news_markdown(
        sample_raw_news(),
        sample_candidates(),
        "2026-05-11",
        tmp_path,
        pd.Timestamp("2026-02-11T00:00:00+09:00"),
        pd.Timestamp("2026-05-11T23:59:59+09:00"),
    )
    content = path.read_text(encoding="utf-8")

    assert path.name == "2026-05-11_future_value_news_raw.md"
    assert "Alpha AI platform" in content
    assert "- Original link: https://example.com/a" in content
    assert "Alpha develops data center AI software" in content


def test_build_future_value_news_dataset_contains_task_and_news():
    payload = build_future_value_news_dataset(
        sample_candidates(),
        sample_raw_news(),
        "2026-05-11",
        pd.Timestamp("2026-02-11T00:00:00+09:00"),
        pd.Timestamp("2026-05-11T23:59:59+09:00"),
        max_price=5_000,
    )

    assert payload["strategy"] == "future_value"
    assert payload["universe"]["max_price"] == 5_000
    assert payload["candidates"][0]["candidate"]["code"] == "000001"
    assert payload["candidates"][0]["news_items"][0]["title"] == "Alpha AI platform"


def test_build_future_value_research_prompt_mentions_summary_output():
    prompt = build_future_value_research_prompt(
        sample_candidates(),
        "2026-05-11",
        dataset_path="dataset.json",
        news_path="news.md",
        theme_path="themes.md",
    )

    assert "Future Value Research Prompt" in prompt
    assert "future_value_summary.md" in prompt
    assert "not a trading recommendation" in prompt
    assert "official company sites" in prompt


def test_phase2_summary_markdown_contains_employee_revenue_and_news():
    content = build_future_value_phase2_summary_markdown(sample_phase2(), "2026-05-11")

    assert "Future Value Phase 2 Research Summary" in content
    assert "Employee count: 25" in content
    assert "Annual revenue: 120.0 eok KRW" in content
    assert "Alpha AI supply" in content


def test_build_phase2_review_prompt_points_to_ai_summary_output():
    prompt = build_future_value_phase2_review_prompt(
        sample_phase2(),
        "2026-05-11",
        phase2_csv_path="phase2.csv",
        phase2_summary_path="summary.md",
        phase2_web_path="web.md",
        dataset_path="dataset.json",
        news_path="news.md",
    )

    assert "Future Value Phase 2 AI Review Prompt" in prompt
    assert "future_value_phase2_ai_summary.md" in prompt
    assert "사원수" in prompt
    assert "OpenDART" in prompt


def test_save_phase2_outputs_write_files(tmp_path):
    csv_path = save_future_value_phase2_csv(sample_phase2(), "2026-05-11", tmp_path)
    web_path = save_future_value_phase2_web_markdown(
        sample_phase2_web(),
        sample_phase2(),
        "2026-05-11",
        tmp_path,
    )
    prompt_path = save_future_value_phase2_review_prompt(
        sample_phase2(),
        "2026-05-11",
        tmp_path,
        phase2_csv_path=csv_path,
        phase2_summary_path=tmp_path / "summary.md",
        phase2_web_path=web_path,
        dataset_path=tmp_path / "dataset.json",
        news_path=tmp_path / "news.md",
    )

    assert csv_path.name == "2026-05-11_future_value_phase2_research.csv"
    assert web_path.name == "2026-05-11_future_value_phase2_web_raw.md"
    assert prompt_path.name == "2026-05-11_future_value_phase2_ai_review_prompt.md"
    assert "Alpha profile" in web_path.read_text(encoding="utf-8")
    assert "future_value_phase2_ai_summary.md" in prompt_path.read_text(encoding="utf-8")
