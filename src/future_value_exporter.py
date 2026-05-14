from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from config import CSV_ENCODING, NEWS_RAW_COLUMNS
from src.future_value import (
    FUTURE_VALUE_AUDIT_COLUMNS,
    FUTURE_VALUE_COLUMNS,
    FUTURE_VALUE_THEMES,
)
from src.future_value_phase2 import PHASE2_SUMMARY_COLUMNS, PHASE2_WEB_RAW_COLUMNS
from src.recommender import ensure_columns, format_markdown_text
from src.stock_codes import normalize_stock_code
from src.swing_exporter import format_rank, is_truthy


def save_future_value_all_evaluated(
    evaluated_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_all_evaluated.csv"
    normalized_df = ensure_columns(evaluated_df, FUTURE_VALUE_AUDIT_COLUMNS)
    normalized_df[FUTURE_VALUE_AUDIT_COLUMNS].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_future_value_candidates(
    candidates_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_candidates.csv"
    normalized_df = ensure_columns(candidates_df, FUTURE_VALUE_COLUMNS)
    normalized_df[FUTURE_VALUE_COLUMNS].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_future_value_theme_markdown(
    candidates_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
    max_price: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_candidates_by_theme.md"
    path.write_text(
        build_future_value_theme_markdown(candidates_df, market_date, max_price),
        encoding="utf-8",
    )
    return path


def build_future_value_theme_markdown(
    candidates_df: pd.DataFrame,
    market_date: str,
    max_price: int,
) -> str:
    candidates_df = ensure_columns(candidates_df, FUTURE_VALUE_COLUMNS)
    lines = [
        f"# Future Value Candidates By Theme - {market_date}",
        "",
        f"- Universe: KOSDAQ stocks priced at or below {max_price:,} KRW",
        "- Purpose: research candidates only, not investment advice",
        "- Scoring: heuristic evidence strength from sector text and recent news",
        "",
    ]
    if candidates_df.empty:
        lines.extend(["No candidates found.", ""])
        return "\n".join(lines).rstrip() + "\n"

    for theme_label in theme_labels_in_order():
        theme_df = candidates_df[
            candidates_df["theme_categories"]
            .fillna("")
            .astype(str)
            .apply(lambda value: theme_label in [item.strip() for item in value.split(",")])
        ].copy()
        if theme_df.empty:
            continue

        lines.extend([f"## {theme_label}", ""])
        for _, row in theme_df.sort_values("rank").iterrows():
            lines.extend(
                [
                    f"### {format_rank(row.get('rank'), len(lines))}. {format_markdown_text(row.get('name'))} ({normalize_stock_code(row.get('code'))})",
                    "",
                    f"- Price: {format_number(row.get('price'))} KRW",
                    f"- Market cap: {format_number(row.get('market_cap_eok'))} eok KRW",
                    f"- Sector: {format_markdown_text(row.get('sector'))}",
                    f"- Industry: {format_markdown_text(row.get('industry'))}",
                    f"- Themes: {format_markdown_text(row.get('theme_categories'))}",
                    f"- Evidence: {format_markdown_text(row.get('theme_evidence'))}",
                    f"- News: {row.get('relevant_news_count', 0)} relevant / {row.get('theme_news_count', 0)} theme",
                    f"- Key titles: {format_markdown_text(row.get('key_news_titles'))}",
                    f"- Key links: {format_markdown_text(row.get('key_news_links'))}",
                    f"- Naver Finance: {format_markdown_text(row.get('naver_finance_url'))}",
                    f"- Research queries: {format_markdown_text(row.get('research_queries'))}",
                    "",
                ]
            )
    return "\n".join(lines).rstrip() + "\n"


def save_future_value_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_news_raw.md"
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    path.write_text(
        build_future_value_news_markdown(
            raw_news_df[NEWS_RAW_COLUMNS],
            candidates_df,
            market_date,
            analysis_start_dt,
            analysis_end_dt,
        ),
        encoding="utf-8",
    )
    return path


def build_future_value_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    market_date: str,
    analysis_start_dt,
    analysis_end_dt,
) -> str:
    lines = [
        f"# Future Value Raw News - {market_date}",
        "",
        "- Source: Naver News Open API",
        "- Search: latest news first, filtered by publish time",
        f"- Analysis window: {analysis_start_dt.isoformat()} ~ {analysis_end_dt.isoformat()}",
        "- Summary: none; previews are search or publisher metadata, not AI summaries",
        "",
    ]
    candidates_df = ensure_columns(candidates_df, FUTURE_VALUE_COLUMNS)
    news_by_code = {
        normalize_stock_code(code): group.sort_values(["pub_date", "news_rank"], ascending=[False, True])
        for code, group in raw_news_df.groupby("code", dropna=False)
    }

    for _, company in candidates_df.sort_values("rank").iterrows():
        code = normalize_stock_code(company.get("code"))
        name = str(company.get("name", ""))
        group = news_by_code.get(code, pd.DataFrame(columns=NEWS_RAW_COLUMNS))
        lines.extend(
            [
                f"## {format_rank(company.get('rank'), 0)}. {format_markdown_text(name)} ({code})",
                "",
                f"- Price: {company.get('price', '')}",
                f"- Themes: {company.get('theme_categories', '')}",
                f"- Evidence score: {company.get('future_value_score', '')}",
                f"- News collected: {len(group)}",
                "",
            ]
        )
        if group.empty:
            lines.extend(["No news found in the selected search.", ""])
            continue

        for fallback_rank, (_, item) in enumerate(group.iterrows(), start=1):
            title = format_markdown_text(item.get("title", "")).replace("\n", " ")
            description = format_markdown_text(item.get("description", ""))
            link = str(item.get("link", "")).strip()
            naver_link = str(item.get("naver_link", "")).strip()
            description_truncated = is_truthy(item.get("description_truncated", False))
            pub_date = str(item.get("pub_date", "")).strip()
            lines.extend(
                [
                    f"### {format_rank(item.get('news_rank'), fallback_rank)}. {title}",
                    "",
                    f"- Published: {pub_date}",
                    f"- Original link: {link}",
                    f"- Naver link: {naver_link}",
                    f"- Preview shortened: {'yes' if description_truncated else 'no'}",
                    "",
                    "Preview:",
                    "",
                    description if description else "(No preview text returned. Open the original link.)",
                    "",
                ]
            )
    return "\n".join(lines).rstrip() + "\n"


def save_future_value_news_dataset(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
    max_price: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_news_dataset.json"
    payload = build_future_value_news_dataset(
        candidates_df,
        raw_news_df,
        market_date,
        analysis_start_dt,
        analysis_end_dt,
        max_price,
    )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def build_future_value_news_dataset(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    market_date: str,
    analysis_start_dt,
    analysis_end_dt,
    max_price: int,
) -> dict:
    candidates_df = ensure_columns(candidates_df, FUTURE_VALUE_COLUMNS)
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    news_by_code = {
        normalize_stock_code(code): group.sort_values(["pub_date", "news_rank"], ascending=[False, True])
        for code, group in raw_news_df.groupby("code", dropna=False)
    }
    candidates = []
    for _, row in candidates_df.sort_values("rank").iterrows():
        code = normalize_stock_code(row.get("code"))
        group = news_by_code.get(code, pd.DataFrame(columns=NEWS_RAW_COLUMNS))
        candidates.append(
            {
                "candidate": {column: dataset_value(row.get(column)) for column in FUTURE_VALUE_COLUMNS},
                "news_items": [
                    {
                        "news_rank": dataset_value(item.get("news_rank")),
                        "title": dataset_value(item.get("title")),
                        "description": dataset_value(item.get("description")),
                        "link": dataset_value(item.get("link")),
                        "naver_link": dataset_value(item.get("naver_link")),
                        "pub_date": dataset_value(item.get("pub_date")),
                        "keyword_flags": dataset_value(item.get("keyword_flags")),
                    }
                    for _, item in group.iterrows()
                ],
            }
        )

    return {
        "market_date": market_date,
        "strategy": "future_value",
        "universe": {
            "market": "KOSDAQ",
            "max_price": max_price,
        },
        "news_window": {
            "start": analysis_start_dt.isoformat(),
            "end": analysis_end_dt.isoformat(),
        },
        "task": (
            "Review low-priced KOSDAQ future-theme candidates. Use the dataset, raw news, "
            "and source links to build a research markdown summary by theme."
        ),
        "themes": {
            key: {
                "label": value["label"],
                "keywords": value["keywords"],
            }
            for key, value in FUTURE_VALUE_THEMES.items()
        },
        "candidates": candidates,
    }


def save_future_value_research_prompt(
    candidates_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
    dataset_path: Path,
    news_path: Path,
    theme_path: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_research_prompt.md"
    path.write_text(
        build_future_value_research_prompt(
            candidates_df,
            market_date,
            dataset_path,
            news_path,
            theme_path,
        ),
        encoding="utf-8",
    )
    return path


def save_future_value_phase2_summary(
    phase2_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_phase2_summary.md"
    path.write_text(build_future_value_phase2_summary_markdown(phase2_df, market_date), encoding="utf-8")
    return path


def save_future_value_phase2_csv(
    phase2_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_phase2_research.csv"
    normalized_df = ensure_columns(phase2_df, PHASE2_SUMMARY_COLUMNS)
    normalized_df[PHASE2_SUMMARY_COLUMNS].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_future_value_phase2_web_markdown(
    web_df: pd.DataFrame,
    phase2_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_phase2_web_raw.md"
    path.write_text(build_future_value_phase2_web_markdown(web_df, phase2_df, market_date), encoding="utf-8")
    return path


def save_future_value_phase2_review_prompt(
    phase2_df: pd.DataFrame,
    market_date: str,
    result_dir: Path,
    phase2_csv_path: Path,
    phase2_summary_path: Path,
    phase2_web_path: Path,
    dataset_path: Path,
    news_path: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{market_date}_future_value_phase2_ai_review_prompt.md"
    path.write_text(
        build_future_value_phase2_review_prompt(
            phase2_df,
            market_date,
            phase2_csv_path=phase2_csv_path,
            phase2_summary_path=phase2_summary_path,
            phase2_web_path=phase2_web_path,
            dataset_path=dataset_path,
            news_path=news_path,
        ),
        encoding="utf-8",
    )
    return path


def build_future_value_phase2_summary_markdown(
    phase2_df: pd.DataFrame,
    market_date: str,
) -> str:
    phase2_df = ensure_columns(phase2_df, PHASE2_SUMMARY_COLUMNS)
    lines = [
        f"# Future Value Phase 2 Research Summary - {market_date}",
        "",
        "- Source: Naver web document search, saved Naver news previews, optional OpenDART revenue",
        "- Purpose: research summary only, not investment advice",
        "- `unknown` means no reliable value was found in collected search previews",
        "",
    ]
    if phase2_df.empty:
        lines.extend(["No phase2 research rows.", ""])
        return "\n".join(lines).rstrip() + "\n"

    for _, row in phase2_df.sort_values("phase2_rank").iterrows():
        lines.extend(
            [
                f"## {format_rank(row.get('phase2_rank'), 0)}. {format_markdown_text(row.get('name'))} ({normalize_stock_code(row.get('code'))})",
                "",
                f"- Themes: {format_markdown_text(row.get('theme_categories'))}",
                f"- Employee count: {format_markdown_text(row.get('employee_count')) or 'unknown'}",
                f"- Annual revenue: {format_markdown_text(row.get('revenue_eok')) or 'unknown'} eok KRW",
                f"- Revenue per employee: {format_markdown_text(row.get('revenue_per_employee_eok')) or 'unknown'} eok KRW/person",
                f"- Revenue source: {format_markdown_text(row.get('revenue_source')) or 'unknown'}",
                f"- Important news count: {format_markdown_text(row.get('important_news_count'))}",
                f"- Confidence: {format_markdown_text(row.get('phase2_confidence'))}",
                f"- Flags: {format_markdown_text(row.get('phase2_flags'))}",
                "",
                "Summary:",
                "",
                format_markdown_text(row.get("phase2_summary")),
                "",
                "Evidence links:",
                "",
                f"- Employee source: {format_markdown_text(row.get('employee_source_title'))} {format_markdown_text(row.get('employee_source_link'))}",
                f"- Revenue source: {format_markdown_text(row.get('revenue_source_title'))} {format_markdown_text(row.get('revenue_source_link'))}",
                f"- Important news: {format_markdown_text(row.get('important_news_titles'))}",
                f"- Important news links: {format_markdown_text(row.get('important_news_links'))}",
                f"- Web links: {format_markdown_text(row.get('key_web_links'))}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def build_future_value_phase2_review_prompt(
    phase2_df: pd.DataFrame,
    market_date: str,
    phase2_csv_path: Path,
    phase2_summary_path: Path,
    phase2_web_path: Path,
    dataset_path: Path,
    news_path: Path,
) -> str:
    phase2_df = ensure_columns(phase2_df, PHASE2_SUMMARY_COLUMNS)
    preview_columns = [
        "phase2_rank",
        "code",
        "name",
        "price",
        "theme_categories",
        "employee_count",
        "revenue_eok",
        "revenue_per_employee_eok",
        "important_news_count",
        "phase2_confidence",
        "phase2_flags",
        "key_web_titles",
    ]
    preview_df = ensure_columns(phase2_df, preview_columns)[preview_columns].head(50).copy()
    return f"""# Future Value Phase 2 AI Review Prompt - {market_date}

## Input Files

- Phase 2 CSV: `{phase2_csv_path}`
- Phase 2 summary: `{phase2_summary_path}`
- Phase 2 raw web results: `{phase2_web_path}`
- Phase 1 dataset JSON: `{dataset_path}`
- Phase 1 raw news: `{news_path}`

## Task

Use the saved files and Naver/company/disclosure search to verify each Phase 2 candidate.
This is research only, not investment advice. Do not guess when a fact is not source-backed.

For each candidate, check:
- actual employee count or latest available staff count,
- annual revenue and whether it comes from OpenDART, IR, company profile, or only a web snippet,
- revenue per employee when both values are known,
- whether the business really produces revenue rather than only a future-theme story,
- important company-specific news such as contracts, supply deals, MOU, investment, patents, approvals, revenue growth, losses, financing, lawsuits, or trading warnings,
- whether the theme link is direct, indirect, or weak.

Prefer OpenDART, company IR, company homepage, exchange disclosures, and original news pages.
Use Naver search terms such as `<company> 사원수`, `<company> 직원수`, `<company> 매출액`,
`<company> 연매출`, `<company> IR`, `<company> 사업보고서`, and `<company> 중요 뉴스`.

## Save Output

- Final markdown: `data/results/{market_date}_future_value_phase2_ai_summary.md`

## Output Format

Group by theme. For each stock include:
- company summary,
- employee count with source,
- annual revenue with source,
- revenue per employee,
- important news bullets with links,
- future-value evidence strength: strong / medium / weak,
- risks and unknowns,
- final AI judgment: deep_research / watch_only / exclude.

## Candidate Preview

{preview_df.to_markdown(index=False)}
"""


def build_future_value_phase2_web_markdown(
    web_df: pd.DataFrame,
    phase2_df: pd.DataFrame,
    market_date: str,
) -> str:
    web_df = ensure_columns(web_df, PHASE2_WEB_RAW_COLUMNS)
    phase2_df = ensure_columns(phase2_df, PHASE2_SUMMARY_COLUMNS)
    lines = [
        f"# Future Value Phase 2 Raw Web Results - {market_date}",
        "",
        "- Source: Naver web document Open API",
        "- Summary: none; descriptions are search result snippets",
        "",
    ]
    web_by_code = {
        normalize_stock_code(code): group
        for code, group in web_df.groupby("code", dropna=False)
    }
    for _, row in phase2_df.sort_values("phase2_rank").iterrows():
        code = normalize_stock_code(row.get("code"))
        group = web_by_code.get(code, pd.DataFrame(columns=PHASE2_WEB_RAW_COLUMNS))
        lines.extend([f"## {row.get('phase2_rank')}. {row.get('name')} ({code})", ""])
        if group.empty:
            lines.extend(["No web results collected.", ""])
            continue
        for index, (_, item) in enumerate(group.iterrows(), start=1):
            lines.extend(
                [
                    f"### {index}. {format_markdown_text(item.get('title'))}",
                    "",
                    f"- Query: {format_markdown_text(item.get('query'))}",
                    f"- Link: {format_markdown_text(item.get('link'))}",
                    "",
                    format_markdown_text(item.get("description")),
                    "",
                ]
            )
    return "\n".join(lines).rstrip() + "\n"


def build_future_value_research_prompt(
    candidates_df: pd.DataFrame,
    market_date: str,
    dataset_path: Path,
    news_path: Path,
    theme_path: Path,
) -> str:
    candidates_df = ensure_columns(candidates_df, FUTURE_VALUE_COLUMNS)
    preview_columns = [
        "rank",
        "code",
        "name",
        "price",
        "market_cap_eok",
        "sector",
        "industry",
        "theme_categories",
        "relevant_news_count",
        "theme_news_count",
        "future_value_score",
        "key_news_titles",
        "naver_finance_url",
        "research_queries",
    ]
    preview_df = ensure_columns(candidates_df, preview_columns)[preview_columns].head(80).copy()
    return f"""# Future Value Research Prompt - {market_date}

## Input Files

- Dataset JSON: `{dataset_path}`
- Raw news markdown: `{news_path}`
- Theme candidate markdown: `{theme_path}`

## Task

Read the dataset and raw news. Build a broad future-value research markdown, grouped by theme.
This is not a trading recommendation. Do not use ROI, PER, PBR, or ROE as the main decision rule.
Focus on whether each company appears connected to future themes: IT/software, AI/data center,
semiconductor/materials, robot/automation, space/aerospace, quantum/security, autonomous/mobility.

For each usable company, summarize:
- what the company appears to do,
- which future theme it belongs to,
- what evidence supports the theme,
- important recent news links,
- why the evidence is weak, medium, or strong,
- key risks or unknowns.

Use the saved news titles, descriptions, links, sector, industry, Naver Finance links, and research queries.
Open official company sites, IR pages, or exchange/company disclosure pages when needed.
If a fact is not in the dataset or source links, write "unknown" instead of guessing.

## Save Output

- Final markdown: `data/results/{market_date}_future_value_summary.md`

## Candidate Preview

{preview_df.to_markdown(index=False)}
"""


def theme_labels_in_order() -> list[str]:
    return [definition["label"] for definition in FUTURE_VALUE_THEMES.values()]


def format_number(value) -> str:
    try:
        if pd.isna(value):
            return ""
        numeric = float(value)
    except (TypeError, ValueError):
        return str(value or "")
    if numeric.is_integer():
        return f"{int(numeric):,}"
    return f"{numeric:,.2f}"


def dataset_value(value):
    if pd.isna(value):
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value
