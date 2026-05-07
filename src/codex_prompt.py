from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import AVG_TRADING_VALUE_EOK_COLUMN


PROMPT_COLUMNS = [
    "final_rank",
    "code",
    "name",
    "market",
    "sector",
    "industry",
    "recommendation_score",
    "matched_profiles",
    "profile_count",
    "best_score",
    "per",
    "pbr",
    "estimated_roe",
    "market_cap_eok",
    AVG_TRADING_VALUE_EOK_COLUMN,
    "news_count",
    "news_sentiment",
    "news_risk_flags",
    "news_titles",
    "risk_note",
]


def save_codex_review_prompt(
    recommendations_df: pd.DataFrame,
    run_date: str,
    result_dir: Path,
    raw_news_path: Path | None = None,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    prompt_path = result_dir / f"{run_date}_codex_review_prompt.md"
    prompt_path.write_text(
        build_codex_review_prompt(recommendations_df, run_date, raw_news_path),
        encoding="utf-8",
    )
    return prompt_path


def build_codex_review_prompt(
    recommendations_df: pd.DataFrame,
    run_date: str,
    raw_news_path: Path | None = None,
) -> str:
    recommendations_df = ensure_prompt_columns(recommendations_df)
    preview_df = recommendations_df[PROMPT_COLUMNS].copy()
    markdown_table = dataframe_to_markdown(preview_df)
    raw_news_section = ""
    if raw_news_path is not None:
        raw_news_section = (
            "\n## 원본 뉴스 CSV\n\n"
            f"- `{raw_news_path}`\n"
            "- 뉴스 제목/요약만으로 부족하면 이 CSV를 읽고 종목별 원문 뉴스 목록을 함께 검토해.\n"
        )

    return f"""# Codex Review Prompt - {run_date}

아래 표는 `advisor.py`가 여러 저평가 프로필로 후보를 모은 뒤 recommendation_score로 정렬한 결과야.

너의 역할:
- 이 결과를 투자 추천이 아니라 후보 스크리닝 관점으로 검토해.
- 최종 20개를 유지하되, 정량 지표상 주의해야 할 종목을 표시해.
- PER/PBR/추정 ROE/시총/거래대금/매칭 프로필을 근거로 간단히 분석해.
- 업종과 최근 뉴스 리스크 키워드가 있으면 함께 반영해.
- 한 업종이나 소형주에 과하게 몰려 있으면 리스크로 적어.
- 텔레그램으로 보낼 수 있게 간결한 한국어 메시지 형태로 작성해.

주의:
- 외부 뉴스나 실시간 이슈를 모르면 모른다고 말해.
- 매수/매도 지시처럼 쓰지 말고, 추가 검토 후보라고 표현해.
- 정량 데이터만으로 확신하지 말고 재무제표와 공시 확인 필요성을 남겨.

## 추천 후보

{markdown_table}
{raw_news_section}
"""


def ensure_prompt_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in PROMPT_COLUMNS:
        if column not in result.columns:
            result[column] = ""
    return result


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for _, row in df.iterrows():
        values = [format_markdown_cell(row[column]) for column in df.columns]
        lines.append("| " + " | ".join(values) + " |")

    return "\n".join(lines)


def format_markdown_cell(value) -> str:
    if pd.isna(value):
        return ""
    return str(value).replace("|", "/")
