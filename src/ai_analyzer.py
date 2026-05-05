from __future__ import annotations

import pandas as pd


def add_ai_summary(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    result["ai_summary"] = result.apply(build_summary, axis=1)
    return result


def build_summary(row: pd.Series) -> str:
    per = row.get("per")
    pbr = row.get("pbr")
    roe = row.get("estimated_roe")
    avg_trading_value = row.get("avg_trading_value_20d")

    summary: list[str] = []

    if per <= 8 and pbr <= 0.8:
        summary.append("PER과 PBR이 모두 낮아 가격 부담이 작은 편임.")
    else:
        summary.append("저PER, 저PBR 조건을 통과해 저평가 후보로 볼 수 있음.")

    if roe >= 15:
        summary.append("추정 ROE가 높아 자기자본 대비 이익력이 양호함.")
    else:
        summary.append("추정 ROE가 MVP 기준을 충족함.")

    if avg_trading_value < 1_000_000_000:
        summary.append("다만 거래대금이 낮아 유동성 리스크 점검이 필요함.")
    else:
        summary.append("거래대금도 최소 유동성 기준을 충족함.")

    return " ".join(summary)
