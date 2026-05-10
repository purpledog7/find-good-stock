from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import (
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
    CSV_ENCODING,
    NEWS_RAW_COLUMNS,
    SECTOR_COLUMNS,
)
from src.exporter import add_display_columns
from src.filters import apply_value_filters
from src.profiles import ScanProfile
from src.scorer import score_stocks
from src.stock_codes import normalize_stock_code, normalize_stock_code_series


RECOMMENDATION_COLUMNS = [
    "date",
    "final_rank",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "recommendation_score",
    "selected_reason",
    "risk_note",
    "matched_profiles",
    "profile_count",
    "best_score",
    "avg_profile_score",
    "best_profile_rank",
    "price",
    "market_cap",
    "market_cap_eok",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
]

CANDIDATE_COLUMNS = [
    "date",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "profile",
    "profile_rank",
    "profile_score",
    "price",
    "market_cap",
    "market_cap_eok",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
]


def scan_profiles(
    collected_df: pd.DataFrame,
    profiles: list[ScanProfile],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    for profile in profiles:
        filtered_df = apply_value_filters(collected_df, profile.criteria)
        scored_df = score_stocks(filtered_df).sort_values(
            by="score",
            ascending=False,
            na_position="last",
        ).reset_index(drop=True)

        if scored_df.empty:
            continue

        scored_df["profile"] = profile.name
        scored_df["profile_rank"] = range(1, len(scored_df) + 1)
        scored_df["profile_score"] = scored_df["score"]
        frames.append(add_display_columns(scored_df))

    if not frames:
        return pd.DataFrame(columns=CANDIDATE_COLUMNS)

    candidates = pd.concat(frames, ignore_index=True)
    candidates = ensure_columns(candidates, SECTOR_COLUMNS)
    candidates["code"] = normalize_stock_code_series(candidates["code"])
    return candidates[CANDIDATE_COLUMNS]


def build_recommendations(
    candidates_df: pd.DataFrame,
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if candidates_df.empty:
        return candidates_df.copy(), pd.DataFrame(columns=RECOMMENDATION_COLUMNS)

    candidates_df = ensure_columns(candidates_df, SECTOR_COLUMNS)
    candidates_df["code"] = normalize_stock_code_series(candidates_df["code"])

    profile_count = candidates_df.groupby("code")["profile"].nunique()
    best_score = candidates_df.groupby("code")["profile_score"].max()
    avg_score = candidates_df.groupby("code")["profile_score"].mean()
    best_rank = candidates_df.groupby("code")["profile_rank"].min()
    matched_profiles = candidates_df.groupby("code")["profile"].apply(
        lambda values: ", ".join(sorted(set(values)))
    )

    base_rows = (
        candidates_df.sort_values(
            by=["profile_score", "profile_rank"],
            ascending=[False, True],
        )
        .drop_duplicates("code")
        .set_index("code")
    )

    merged = base_rows.copy()
    merged["matched_profiles"] = matched_profiles
    merged["profile_count"] = profile_count
    merged["best_score"] = best_score.round(2)
    merged["avg_profile_score"] = avg_score.round(2)
    merged["best_profile_rank"] = best_rank
    merged["recommendation_score"] = calculate_recommendation_score(merged)
    merged["selected_reason"] = merged.apply(build_selected_reason, axis=1)
    merged["risk_note"] = merged.apply(build_risk_note, axis=1)

    result = merged.reset_index()
    result = result.sort_values(
        by=["recommendation_score", "best_score", "profile_count"],
        ascending=[False, False, False],
    ).reset_index(drop=True)
    result["final_rank"] = range(1, len(result) + 1)

    recommendations = result.head(top_n).copy()
    return result[RECOMMENDATION_COLUMNS], recommendations[RECOMMENDATION_COLUMNS]


def calculate_recommendation_score(df: pd.DataFrame) -> pd.Series:
    best_score_part = pd.to_numeric(df["best_score"], errors="coerce").fillna(0) * 0.55
    profile_part = (pd.to_numeric(df["profile_count"], errors="coerce").fillna(0) / 4).clip(0, 1) * 20
    liquidity_part = (
        pd.to_numeric(df[AVG_TRADING_VALUE_COLUMN], errors="coerce").fillna(0)
        / 5_000_000_000
    ).clip(0, 1) * 10
    quality_part = (
        pd.to_numeric(df["estimated_roe"], errors="coerce").fillna(0) / 20
    ).clip(0, 1) * 10
    size_part = (
        pd.to_numeric(df["market_cap"], errors="coerce").fillna(0)
        / 500_000_000_000
    ).clip(0, 1) * 5

    return (best_score_part + profile_part + liquidity_part + quality_part + size_part).round(2)


def build_selected_reason(row: pd.Series) -> str:
    return (
        f"{int(row['profile_count'])}개 프로필({row['matched_profiles']})에서 포착. "
        f"최고 점수 {row['best_score']:.2f}, PER {row['per']:.2f}, "
        f"PBR {row['pbr']:.2f}, 추정 ROE {row['estimated_roe']:.2f}%."
    )


def build_risk_note(row: pd.Series) -> str:
    risks: list[str] = []
    if row["market_cap_eok"] < 1_000:
        risks.append("소형주라 가격 변동과 체결 리스크 확인 필요")
    if row[AVG_TRADING_VALUE_EOK_COLUMN] < 10:
        risks.append("거래대금이 낮아 유동성 주의")
    if row["profile_count"] == 1:
        risks.append("단일 프로필에서만 포착되어 추가 확인 필요")
    if row["estimated_roe"] < 10:
        risks.append("추정 ROE가 높지는 않아 수익성 확인 필요")
    return " / ".join(risks) if risks else "주요 정량 리스크는 낮은 편"


def save_advisor_results(
    candidates_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    run_date: str,
    result_dir: Path,
    top_n: int,
) -> tuple[Path, Path]:
    result_dir.mkdir(parents=True, exist_ok=True)
    candidates_path = result_dir / f"{run_date}_profile_candidates.csv"
    recommendations_path = result_dir / f"{run_date}_recommend{top_n}.csv"

    candidates_df.to_csv(candidates_path, index=False, encoding=CSV_ENCODING)
    recommendations_df.to_csv(recommendations_path, index=False, encoding=CSV_ENCODING)

    return candidates_path, recommendations_path


def save_raw_news_markdown(
    raw_news_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    run_date: str,
    result_dir: Path,
    start_dt,
    end_dt,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    news_path = result_dir / f"{run_date}_news_raw.md"
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    news_path.write_text(
        build_raw_news_markdown(
            raw_news_df[NEWS_RAW_COLUMNS],
            recommendations_df,
            run_date,
            start_dt,
            end_dt,
        ),
        encoding="utf-8",
    )
    return news_path


def build_raw_news_markdown(
    raw_news_df: pd.DataFrame,
    recommendations_df: pd.DataFrame,
    run_date: str,
    start_dt,
    end_dt,
) -> str:
    lines = [
        f"# Raw News - {run_date}",
        "",
        f"- Window: {start_dt.isoformat()} ~ {end_dt.isoformat()}",
        "- Source: Naver News Open API",
        "- Summary: none",
        "",
    ]

    news_by_code = {
        normalize_stock_code(code): group.sort_values("news_rank")
        for code, group in raw_news_df.groupby("code", dropna=False)
    }

    for _, company in recommendations_df.sort_values("final_rank").iterrows():
        code = normalize_stock_code(company["code"])
        name = str(company["name"])
        rank = int(company["final_rank"])
        group = news_by_code.get(code, pd.DataFrame(columns=NEWS_RAW_COLUMNS))

        lines.extend(
            [
                f"## {rank}. {name} ({code})",
                "",
                f"- Market: {company.get('market', '')}",
                f"- Sector: {company.get('sector', '')}",
                f"- Recommendation score: {company.get('recommendation_score', '')}",
                f"- News count: {len(group)}",
                "",
            ]
        )

        if group.empty:
            lines.extend(["No news found in the selected window.", ""])
            continue

        for fallback_rank, (_, item) in enumerate(group.iterrows(), start=1):
            title = format_markdown_text(item.get("title", ""))
            description = format_markdown_text(item.get("description", ""))
            link = str(item.get("link", "")).strip()
            pub_date = str(item.get("pub_date", "")).strip()

            lines.extend(
                [
                    f"### {format_rank(item.get('news_rank'), fallback_rank)}. {title}",
                    "",
                    f"- Published: {pub_date}",
                    f"- Link: {link}",
                    "",
                    description,
                    "",
                ]
            )

    return "\n".join(lines).rstrip() + "\n"


def format_markdown_text(value) -> str:
    value = scalar_value(value)
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).replace("\r\n", "\n").replace("\r", "\n").strip()


def format_rank(value, fallback: int) -> int:
    value = scalar_value(value)
    try:
        if pd.isna(value):
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def scalar_value(value):
    if isinstance(value, pd.Series):
        if value.empty:
            return ""
        non_null = value.dropna()
        if non_null.empty:
            return ""
        return non_null.iloc[0]
    return value


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.loc[:, ~df.columns.duplicated()].copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result
