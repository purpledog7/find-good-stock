from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from config import CSV_ENCODING, NEWS_RAW_COLUMNS
from src.recommender import ensure_columns, format_markdown_text
from src.special_swing import (
    DAY_SWING_AUDIT_COLUMNS,
    DAY_SWING_COLUMNS,
    SPECIAL_SWING_AUDIT_COLUMNS,
    SPECIAL_SWING_COLUMNS,
)
from src.stock_codes import normalize_stock_code
from src.swing_exporter import dataframe_to_markdown, format_rank, is_truthy


def save_special_swing_candidates(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    candidate_count: int,
    strategy_slug: str = "special_swing",
    columns: list[str] | None = None,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_{strategy_slug}_candidates_top{candidate_count}.csv"
    output_columns = columns or SPECIAL_SWING_COLUMNS
    normalized_df = ensure_columns(candidates_df, output_columns)
    normalized_df[output_columns].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_special_swing_all_evaluated(
    evaluated_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    strategy_slug: str = "special_swing",
    columns: list[str] | None = None,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_{strategy_slug}_all_evaluated.csv"
    output_columns = columns or SPECIAL_SWING_AUDIT_COLUMNS
    normalized_df = ensure_columns(evaluated_df, output_columns)
    normalized_df[output_columns].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_day_swing_candidates(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    candidate_count: int,
) -> Path:
    return save_special_swing_candidates(
        candidates_df,
        signal_date,
        result_dir,
        candidate_count,
        strategy_slug="day_swing",
        columns=DAY_SWING_COLUMNS,
    )


def save_day_swing_all_evaluated(
    evaluated_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
) -> Path:
    return save_special_swing_all_evaluated(
        evaluated_df,
        signal_date,
        result_dir,
        strategy_slug="day_swing",
        columns=DAY_SWING_AUDIT_COLUMNS,
    )


def save_special_swing_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
    candidate_count: int,
    strategy_slug: str = "special_swing",
    title: str = "Special Swing Raw News",
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_{strategy_slug}_news_raw_top{candidate_count}.md"
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    path.write_text(
        build_special_swing_news_markdown(
            raw_news_df[NEWS_RAW_COLUMNS],
            candidates_df,
            signal_date,
            analysis_start_dt,
            analysis_end_dt,
            title=title,
        ),
        encoding="utf-8",
    )
    return path


def save_day_swing_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
    candidate_count: int,
) -> Path:
    return save_special_swing_news_markdown(
        raw_news_df,
        candidates_df,
        signal_date,
        result_dir,
        analysis_start_dt,
        analysis_end_dt,
        candidate_count,
        strategy_slug="day_swing",
        title="Day Swing Raw News",
    )


def build_special_swing_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    signal_date: str,
    analysis_start_dt,
    analysis_end_dt,
    title: str = "Special Swing Raw News",
) -> str:
    lines = [
        f"# {title} - {signal_date}",
        "",
        "- Source: Naver News Open API",
        "- Search: latest news first, filtered by publish time",
        f"- Analysis window: {analysis_start_dt.isoformat()} ~ {analysis_end_dt.isoformat()}",
        "- Summary: none; previews are search or publisher metadata, not AI summaries",
        "",
    ]
    news_by_code = {
        normalize_stock_code(code): group.sort_values(["pub_date", "news_rank"], ascending=[False, True])
        for code, group in raw_news_df.groupby("code", dropna=False)
    }

    for _, company in candidates_df.sort_values("rank").iterrows():
        code = normalize_stock_code(company["code"])
        name = str(company["name"])
        rank = int(company["rank"])
        group = news_by_code.get(code, pd.DataFrame(columns=NEWS_RAW_COLUMNS))

        lines.extend(
            [
                f"## {rank}. {name} ({code})",
                "",
                f"- Market: {company.get('market', '')}",
                f"- Sector: {company.get('sector', '')}",
                f"- Special swing score: {company.get('special_swing_score', '')}",
                f"- Technical score: {company.get('technical_score', '')}",
                f"- Theme hits: {company.get('theme_hits', '')}",
                f"- News growth score: {company.get('news_growth_score', '')}",
                f"- News daily counts: {company.get('news_daily_counts', '')}",
                f"- News collected: {len(group)}",
                "",
            ]
        )

        if group.empty:
            lines.extend(["No news found in the selected news search.", ""])
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


def save_special_swing_news_dataset(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
    candidate_count: int,
    shortlist_n: int = 30,
    final_n: int = 10,
    strategy_slug: str = "special_swing",
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_{strategy_slug}_news_dataset_top{candidate_count}.json"
    payload = build_special_swing_news_dataset(
        candidates_df,
        raw_news_df,
        signal_date,
        analysis_start_dt,
        analysis_end_dt,
        candidate_count=candidate_count,
        shortlist_n=shortlist_n,
        final_n=final_n,
        strategy_slug=strategy_slug,
    )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def save_day_swing_news_dataset(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    analysis_start_dt,
    analysis_end_dt,
    candidate_count: int,
    shortlist_n: int = 20,
    final_n: int = 5,
) -> Path:
    return save_special_swing_news_dataset(
        candidates_df,
        raw_news_df,
        signal_date,
        result_dir,
        analysis_start_dt,
        analysis_end_dt,
        candidate_count,
        shortlist_n=shortlist_n,
        final_n=final_n,
        strategy_slug="day_swing",
    )


def build_special_swing_news_dataset(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    signal_date: str,
    analysis_start_dt,
    analysis_end_dt,
    candidate_count: int = 100,
    shortlist_n: int = 30,
    final_n: int = 10,
    strategy_slug: str = "special_swing",
) -> dict:
    candidate_columns = [
        "rank",
        "code",
        "name",
        "market",
        "sector",
        "industry",
        "special_swing_score",
        "technical_score",
        "community_setup_score",
        "five_day_trigger_score",
        "news_growth_score",
        "news_relevance_score",
        "primary_news_score",
        "news_freshness_score",
        "news_count_5d",
        "relevant_news_count_5d",
        "primary_news_count_5d",
        "noisy_news_count_5d",
        "direct_catalyst_score",
        "theme_score",
        "catalyst_score",
        "theme_hits",
        "matched_conditions",
        "risk_flags",
        "price",
        "entry_price",
        "half_take_profit_price",
        "full_take_profit_price",
        "review_date_3d",
        "review_date_5d",
    ]
    if strategy_slug == "day_swing":
        candidate_columns.extend(
            [
                "day_liquidity_score",
                "morning_entry_bias_score",
                "day_gap_risk_penalty",
                "day_technical_score",
                "overnight_news_score",
                "day_swing_score",
            ]
        )
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    candidates_df = ensure_columns(candidates_df, candidate_columns)
    news_by_code = {
        normalize_stock_code(code): group.sort_values(["pub_date", "news_rank"], ascending=[False, True])
        for code, group in raw_news_df.groupby("code", dropna=False)
    }

    candidates = []
    for _, row in candidates_df.sort_values("rank").iterrows():
        code = normalize_stock_code(row.get("code", ""))
        group = news_by_code.get(code, pd.DataFrame(columns=NEWS_RAW_COLUMNS))
        news_items = []
        for _, item in group.iterrows():
            news_items.append(
                {
                    "news_rank": dataset_value(item.get("news_rank")),
                    "title": dataset_value(item.get("title")),
                    "description": dataset_value(item.get("description")),
                    "link": dataset_value(item.get("link")),
                    "naver_link": dataset_value(item.get("naver_link")),
                    "pub_date": dataset_value(item.get("pub_date")),
                    "description_truncated": bool(is_truthy(item.get("description_truncated", False))),
                    "keyword_flags": dataset_value(item.get("keyword_flags")),
                }
            )

        candidates.append(
            {
                "candidate": {column: dataset_value(row.get(column)) for column in candidate_columns},
                "news_count": len(news_items),
                "news_items": news_items,
            }
        )

    return {
        "signal_date": signal_date,
        "strategy": strategy_slug,
        "news_window": {
            "start": analysis_start_dt.isoformat(),
            "end": analysis_end_dt.isoformat(),
        },
        "task": build_dataset_task(strategy_slug, candidate_count, shortlist_n, final_n),
        "phase2_output_schema": build_phase2_output_schema(strategy_slug),
        "candidates": candidates,
    }


def build_phase2_output_schema(strategy_slug: str) -> dict[str, str]:
    if strategy_slug == "day_swing":
        return {
            "news_count_score": "0~30 integer; relevant post-close-to-08:00 article count and freshness.",
            "news_quality_score": "0~20 integer; company-specific positive catalyst quality for same-day movement.",
            "news_theme_score": "0~10 integer; active theme fit likely to attract market attention today.",
            "ai_news_score": "0~60 integer; news_count_score + news_quality_score + news_theme_score.",
            "ai_risk_penalty": "0~25 integer; stale/duplicated/unrelated/negative/already-reflected news risk.",
            "ai_adjusted_score": "day_swing_score + ai_news_score - ai_risk_penalty.",
            "ai_news_summary": "Korean summary of the actual same-day catalyst.",
            "ai_risk_summary": "Korean summary of hidden day-trade risks.",
            "ai_judgment": "selected/watch_only/exclude.",
        }
    return {
        "news_count_score": "0~35 integer; recent 5-day article count and sudden growth strength. This is the largest news subscore.",
        "news_quality_score": "0~15 integer; company-specific positive catalyst quality.",
        "news_theme_score": "0~10 integer; AI/semiconductor/bio/robot/power/defense or other active theme fit.",
        "ai_news_score": "0~60 integer; news_count_score + news_quality_score + news_theme_score.",
        "ai_risk_penalty": "0~20 integer; unrelated/negative/over-reflected news risk.",
        "ai_adjusted_score": "special_swing_score + ai_news_score - ai_risk_penalty.",
        "ai_news_summary": "Korean summary of the actual news catalyst.",
        "ai_risk_summary": "Korean summary of hidden risks.",
        "ai_judgment": "selected/watch_only/exclude.",
    }


def build_dataset_task(strategy_slug: str, candidate_count: int, shortlist_n: int, final_n: int) -> str:
    if strategy_slug == "day_swing":
        return (
            f"Codex should score Top{candidate_count} one-day swing candidates, select Top{shortlist_n}, "
            f"then run a four-specialist and one-leader debate to select final Top{final_n} for morning entry and afternoon exit."
        )
    return f"Codex should score Top{candidate_count} candidates, select Top{shortlist_n}, then run a four-specialist and one-leader debate to select final Top{final_n}."


def save_special_swing_phase2_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    dataset_path: Path,
    news_path: Path,
    shortlist_n: int,
    candidate_count: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_special_swing_phase2_score_top{candidate_count}_to_top{shortlist_n}_prompt.md"
    path.write_text(
        build_special_swing_phase2_prompt(
            candidates_df,
            signal_date,
            dataset_path,
            news_path,
            shortlist_n,
            candidate_count,
        ),
        encoding="utf-8",
    )
    return path


def save_day_swing_phase2_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    dataset_path: Path,
    news_path: Path,
    shortlist_n: int,
    candidate_count: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_day_swing_phase2_score_top{candidate_count}_to_top{shortlist_n}_prompt.md"
    path.write_text(
        build_day_swing_phase2_prompt(
            candidates_df,
            signal_date,
            dataset_path,
            news_path,
            shortlist_n,
            candidate_count,
        ),
        encoding="utf-8",
    )
    return path


def build_special_swing_phase2_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    dataset_path: Path,
    news_path: Path,
    shortlist_n: int,
    candidate_count: int,
) -> str:
    preview_df = ensure_columns(candidates_df, stage_preview_columns())[stage_preview_columns()].copy()
    return f"""# Special Swing Phase 2 - Score Top{candidate_count} To Top{shortlist_n} ({signal_date})

## Input Files

- Dataset JSON: `{dataset_path}`
- Raw news markdown: `{news_path}`

## Task

Read the Top{candidate_count} dataset and raw news. Add first-pass AI news scores, then select Top{shortlist_n}.
Use the news title, preview/description, published time, and links. Do not assume facts that are not in the files.
The most important factor is whether the number of relevant news items increased suddenly during the latest 5-day window.

## Scoring Rules

- `news_count_score`: 0~35. Give the largest weight to 5-day relevant news count, latest-day concentration, and sudden growth versus earlier days.
- `news_quality_score`: 0~15. High only for recent, company-specific, actionable positive catalysts likely to matter within 3~5 trading days.
- `news_theme_score`: 0~10. Add only when the news clearly connects to active market themes such as AI, semiconductor, obesity treatment, bio, robot, power, nuclear, defense, battery, or shipbuilding.
- `ai_news_score`: 0~60. Use `news_count_score + news_quality_score + news_theme_score`.
- `ai_risk_penalty`: 0~20. Penalize unrelated theme articles, one-day duplicate news, negative news, dilution, lawsuits, and already-reflected hype.
- `ai_adjusted_score`: `special_swing_score + ai_news_score - ai_risk_penalty`.
- Select exactly Top{shortlist_n} unless fewer candidates have usable news.

## Save Output

- Scored CSV: `data/results/{signal_date}_special_swing_phase2_scored_top{candidate_count}.csv`
- Top{shortlist_n} JSON: `data/results/{signal_date}_special_swing_phase2_top{shortlist_n}.json`

## Required Scored CSV Columns

`rank, code, name, special_swing_score, news_count_score, news_quality_score, news_theme_score, ai_news_score, ai_risk_penalty, ai_adjusted_score, ai_news_summary, ai_risk_summary, ai_judgment, selection_reason, key_news_links`

## Candidate Preview

{dataframe_to_markdown(preview_df)}
"""


def build_day_swing_phase2_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    dataset_path: Path,
    news_path: Path,
    shortlist_n: int,
    candidate_count: int,
) -> str:
    preview_df = ensure_columns(candidates_df, day_stage_preview_columns())[day_stage_preview_columns()].copy()
    return f"""# Day Swing Phase 2 - Score Top{candidate_count} To Top{shortlist_n} ({signal_date})

## Input Files

- Dataset JSON: `{dataset_path}`
- Raw news markdown: `{news_path}`

## Task

Read the Top{candidate_count} one-day swing dataset and raw news. Add first-pass AI news scores, then select Top{shortlist_n}.
Use only the news title, preview/description, published time, links, and saved technical data.
This is for same-day trading: morning entry candidate, afternoon exit. Do not treat it as a 3~5 trading-day hold.
The most important factor is fresh company-specific news from the post-close to 08:00 KST window.

## Scoring Rules

- `news_count_score`: 0~30. Give high scores to multiple relevant fresh articles in the overnight window.
- `news_quality_score`: 0~20. High only for direct company catalysts likely to move price today.
- `news_theme_score`: 0~10. Add only when the news clearly connects to an active market theme.
- `ai_news_score`: 0~60. Use `news_count_score + news_quality_score + news_theme_score`.
- `ai_risk_penalty`: 0~25. Penalize stale, unrelated, duplicated, negative, or already-reflected news.
- `ai_adjusted_score`: `day_swing_score + ai_news_score - ai_risk_penalty`.
- Select exactly Top{shortlist_n} unless fewer candidates have usable news.

## Save Output

- Scored CSV: `data/results/{signal_date}_day_swing_phase2_scored_top{candidate_count}.csv`
- Top{shortlist_n} JSON: `data/results/{signal_date}_day_swing_phase2_top{shortlist_n}.json`

## Required Scored CSV Columns

`rank, code, name, day_swing_score, day_technical_score, overnight_news_score, news_count_score, news_quality_score, news_theme_score, ai_news_score, ai_risk_penalty, ai_adjusted_score, ai_news_summary, ai_risk_summary, ai_judgment, morning_entry_condition, no_trade_condition, afternoon_exit_plan, selection_reason, key_news_links`

## Candidate Preview

{dataframe_to_markdown(preview_df)}
"""


def save_special_swing_phase3_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    phase2_top10_path: Path,
    news_path: Path,
    shortlist_n: int,
    final_n: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_special_swing_phase3_debate_top{shortlist_n}_to_top{final_n}_prompt.md"
    path.write_text(
        build_special_swing_phase3_prompt(
            preview_candidates_df,
            signal_date,
            phase2_top10_path,
            news_path,
            shortlist_n,
            final_n,
        ),
        encoding="utf-8",
    )
    return path


def save_day_swing_phase3_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    phase2_top_path: Path,
    news_path: Path,
    shortlist_n: int,
    final_n: int,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_day_swing_phase3_debate_top{shortlist_n}_to_top{final_n}_prompt.md"
    path.write_text(
        build_day_swing_phase3_prompt(
            preview_candidates_df,
            signal_date,
            phase2_top_path,
            news_path,
            shortlist_n,
            final_n,
        ),
        encoding="utf-8",
    )
    return path


def build_special_swing_phase3_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    phase2_top10_path: Path,
    news_path: Path,
    shortlist_n: int,
    final_n: int,
) -> str:
    preview_df = ensure_columns(preview_candidates_df, stage_preview_columns())[stage_preview_columns()].copy()
    return f"""# Special Swing Phase 3 - Four-Specialist And Leader Debate Top{shortlist_n} To Top{final_n} ({signal_date})

## Input Files

- Phase 2 Top{shortlist_n}: `{phase2_top10_path}`
- Raw news markdown: `{news_path}`

## Task

Review the Phase 2 Top{shortlist_n}. Use four distinct stock-specialist roles plus one leader, debate the candidates, and produce the final ordered Top{final_n}.
Keep the analysis focused on a 3~5 trading-day swing. Use only the dataset, raw news, and saved Phase 2 result. Do not assume facts outside the files.

Specialist roles:
- Technical and entry timing specialist: box, pullback, VCP/tight base, VWAP/MA reclaim, trigger, stop clarity.
- News catalyst specialist: 5-day news count growth, latest news acceleration, and whether the news can move price within 3~5 trading days.
- Theme and flow specialist: AI/semiconductor/bio/robot/power/defense and other active theme strength.
- Risk specialist: negative news, dilution, lawsuits, over-reflection, liquidity traps, and poor reward/risk.
- Leader: compare all specialist opinions, challenge weak evidence, then select the final ordered Top{final_n}.

Debate process:
- Round 1: each specialist independently names preferred and rejected stocks with evidence.
- Round 2: specialists challenge weak catalysts, already-reflected hype, and poor entry timing.
- Final decision: the leader selects exactly Top{final_n} unless fewer candidates are usable.

## Save Output

- Final markdown: `data/results/{signal_date}_special_swing_phase3_final_top{final_n}.md`
- Final CSV: `data/results/{signal_date}_special_swing_phase3_final_top{final_n}.csv`

## Required Final Columns

`final_rank, code, name, final_judgment, key_catalyst, key_news_links, debate_summary, selected_reason, entry_condition, stop_condition, target_reason, key_risk`

## Required Markdown Sections

- Four specialist opinions
- Leader review
- Main debate and disagreement points
- Rejected Top{shortlist_n} candidates and rejection reasons
- Final Top{final_n} selection reasons
- 3~5 trading-day entry, stop, and target logic

## Current Rule-Based Preview

{dataframe_to_markdown(preview_df)}
"""


def build_day_swing_phase3_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    phase2_top_path: Path,
    news_path: Path,
    shortlist_n: int,
    final_n: int,
) -> str:
    preview_df = ensure_columns(preview_candidates_df, day_stage_preview_columns())[day_stage_preview_columns()].copy()
    return f"""# Day Swing Phase 3 - Four-Specialist And Leader Debate Top{shortlist_n} To Top{final_n} ({signal_date})

## Input Files

- Phase 2 Top{shortlist_n}: `{phase2_top_path}`
- Raw news markdown: `{news_path}`

## Task

Review the Phase 2 Top{shortlist_n}. Use four distinct stock-specialist roles plus one leader, debate the candidates, and produce the final ordered Top{final_n}.
Keep the analysis focused on same-day trading: morning entry and afternoon exit. Use only the dataset, raw news, and saved Phase 2 result.

Specialist roles:
- Technical and morning timing specialist: premarket setup, liquidity, reclaim, gap-chase risk, invalidation line.
- News catalyst specialist: post-close to 08:00 news freshness, directness, and whether it can move price today.
- Theme and flow specialist: active theme strength and likely market attention today.
- Risk specialist: negative news, duplicate articles, already-reflected moves, no-trade conditions, liquidity traps.
- Leader: compare all specialist opinions, challenge weak evidence, then select the final ordered Top{final_n}.

Debate process:
- Round 1: each specialist independently names preferred and rejected stocks with evidence.
- Round 2: specialists challenge weak catalysts, gap-chase risk, and unclear afternoon exit logic.
- Final decision: the leader selects exactly Top{final_n} unless fewer candidates are usable.

## Save Output

- Final markdown: `data/results/{signal_date}_day_swing_phase3_final_top{final_n}.md`
- Final CSV: `data/results/{signal_date}_day_swing_phase3_final_top{final_n}.csv`

## Required Final Columns

`final_rank, code, name, final_judgment, key_catalyst, key_news_links, debate_summary, selected_reason, morning_entry_condition, invalidation_condition, afternoon_exit_plan, no_trade_condition, key_risk`

## Required Markdown Sections

- Four specialist opinions
- Leader review
- Main debate and disagreement points
- Rejected Top{shortlist_n} candidates and rejection reasons
- Final Top{final_n} selection reasons
- Morning entry, invalidation, afternoon exit, and no-trade logic

## Current Rule-Based Preview

{dataframe_to_markdown(preview_df)}
"""


def save_special_swing_stage1_prompt(*args, **kwargs) -> Path:
    return save_special_swing_phase2_prompt(*args, **kwargs)


def build_special_swing_stage1_prompt(*args, **kwargs) -> str:
    return build_special_swing_phase2_prompt(*args, **kwargs)


def save_special_swing_stage2_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    stage1_top10_path: Path,
    news_path: Path,
    shortlist_n: int,
) -> Path:
    return save_special_swing_phase3_prompt(
        preview_candidates_df,
        signal_date,
        result_dir,
        phase2_top10_path=stage1_top10_path,
        news_path=news_path,
        shortlist_n=shortlist_n,
        final_n=shortlist_n,
    )


def build_special_swing_stage2_prompt(
    preview_candidates_df: pd.DataFrame,
    signal_date: str,
    stage1_top10_path: Path,
    news_path: Path,
    shortlist_n: int,
) -> str:
    return build_special_swing_phase3_prompt(
        preview_candidates_df,
        signal_date,
        phase2_top10_path=stage1_top10_path,
        news_path=news_path,
        shortlist_n=shortlist_n,
        final_n=shortlist_n,
    )


def stage_preview_columns() -> list[str]:
    return [
        "rank",
        "code",
        "name",
        "market",
        "sector",
        "special_swing_score",
        "technical_score",
        "community_setup_score",
        "news_growth_score",
        "news_count_5d",
        "direct_catalyst_score",
        "theme_hits",
        "matched_conditions",
        "risk_flags",
        "review_date_3d",
        "review_date_5d",
    ]


def day_stage_preview_columns() -> list[str]:
    return [
        "rank",
        "code",
        "name",
        "market",
        "sector",
        "day_swing_score",
        "day_technical_score",
        "day_liquidity_score",
        "morning_entry_bias_score",
        "overnight_news_score",
        "news_freshness_score",
        "direct_catalyst_score",
        "theme_hits",
        "matched_conditions",
        "risk_flags",
        "entry_price",
        "half_take_profit_price",
        "full_take_profit_price",
    ]


def dataset_value(value):
    if pd.isna(value):
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value.item() if hasattr(value, "item") else value
