from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import AVG_TRADING_VALUE_EOK_COLUMN


PROMPT_COLUMNS = [
    "final_rank",
    "code",
    "name",
    "market",
    "recommendation_score",
    "matched_profiles",
    "profile_count",
    "best_score",
    "per",
    "pbr",
    "estimated_roe",
    "market_cap_eok",
    AVG_TRADING_VALUE_EOK_COLUMN,
    "risk_note",
]


def save_codex_review_prompt(
    recommendations_df: pd.DataFrame,
    run_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    prompt_path = result_dir / f"{run_date}_codex_review_prompt.md"
    prompt_path.write_text(
        build_codex_review_prompt(recommendations_df, run_date),
        encoding="utf-8",
    )
    return prompt_path


def build_codex_review_prompt(recommendations_df: pd.DataFrame, run_date: str) -> str:
    preview_df = recommendations_df[PROMPT_COLUMNS].copy()
    markdown_table = dataframe_to_markdown(preview_df)

    return f"""# Codex Review Prompt - {run_date}

아래 표는 `advisor.py`가 여러 저평가 프로필로 후보를 모은 뒤 recommendation_score로 정렬한 결과야.

너의 역할:
- 이 결과를 투자 추천이 아니라 후보 스크리닝 관점으로 검토해.
- 최종 20개를 유지하되, 정량 지표상 주의해야 할 종목을 표시해.
- PER/PBR/추정 ROE/시총/거래대금/매칭 프로필을 근거로 간단히 분석해.
- 한 업종이나 소형주에 과하게 몰려 있으면 리스크로 적어.
- 텔레그램으로 보낼 수 있게 간결한 한국어 메시지 형태로 작성해.

주의:
- 외부 뉴스나 실시간 이슈를 모르면 모른다고 말해.
- 매수/매도 지시처럼 쓰지 말고, 추가 검토 후보라고 표현해.
- 정량 데이터만으로 확신하지 말고 재무제표와 공시 확인 필요성을 남겨.

## 추천 후보

{markdown_table}
"""


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
