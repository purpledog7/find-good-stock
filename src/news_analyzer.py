from __future__ import annotations

from datetime import datetime
from typing import Callable

import pandas as pd

from config import NEWS_RAW_COLUMNS
from src.news_client import NaverNewsClient, NewsItem


ProgressCallback = Callable[[str], None] | None

NEGATIVE_KEYWORDS = {
    "유상증자": "capital_raise",
    "전환사채": "convertible_bond",
    "CB": "convertible_bond",
    "BW": "bond_with_warrant",
    "적자": "loss",
    "손실": "loss",
    "횡령": "embezzlement",
    "배임": "breach_of_trust",
    "소송": "lawsuit",
    "거래정지": "trading_halt",
    "감사의견": "audit_opinion",
    "하향": "downgrade",
    "급락": "sharp_drop",
}

POSITIVE_KEYWORDS = {
    "수주": "order_win",
    "계약": "contract",
    "흑자전환": "turnaround",
    "증가": "increase",
    "성장": "growth",
    "상향": "upgrade",
    "배당": "dividend",
    "자사주": "buyback",
}


def enrich_news_info(
    df: pd.DataFrame,
    client: NaverNewsClient,
    start_dt: datetime,
    end_dt: datetime,
    max_items: int,
    progress: ProgressCallback = None,
) -> pd.DataFrame:
    enriched_df, _ = collect_news_info(
        df,
        client,
        start_dt,
        end_dt,
        max_items,
        progress,
    )
    return enriched_df


def collect_news_info(
    df: pd.DataFrame,
    client: NaverNewsClient,
    start_dt: datetime,
    end_dt: datetime,
    max_items: int,
    progress: ProgressCallback = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    result = df.copy()
    summary_rows: list[dict] = []
    raw_rows: list[dict] = []

    for index, row in result.iterrows():
        code = str(row["code"]).zfill(6)
        name = str(row["name"])
        emit_progress(progress, f"뉴스 검색 중 ({len(summary_rows) + 1}/{len(result)}): {name}")
        try:
            items = client.search_recent_news(
                name,
                start_dt=start_dt,
                end_dt=end_dt,
                display=max_items,
            )
        except RuntimeError as error:
            emit_progress(progress, f"  뉴스 검색 실패: {error}")
            items = []

        summary_rows.append(summarize_news_items(items))
        raw_rows.extend(build_raw_news_rows(code, name, items))

    news_df = pd.DataFrame(summary_rows, index=result.index)
    raw_news_df = pd.DataFrame(raw_rows, columns=NEWS_RAW_COLUMNS)
    return pd.concat([result, news_df], axis=1), raw_news_df


def summarize_news_items(items: list[NewsItem]) -> dict:
    risk_flags = sorted(find_keyword_flags(items, NEGATIVE_KEYWORDS))
    positive_flags = sorted(find_keyword_flags(items, POSITIVE_KEYWORDS))
    news_sentiment = classify_sentiment(risk_flags, positive_flags)
    titles = [item.title for item in items[:5]]

    return {
        "news_count": len(items),
        "news_sentiment": news_sentiment,
        "news_risk_flags": ", ".join(risk_flags),
        "news_titles": " | ".join(titles),
        "news_summary": build_news_summary(len(items), news_sentiment, risk_flags, positive_flags),
    }


def find_keyword_flags(items: list[NewsItem], keyword_map: dict[str, str]) -> set[str]:
    flags: set[str] = set()
    for item in items:
        text = f"{item.title} {item.description}"
        for keyword, flag in keyword_map.items():
            if keyword in text:
                flags.add(flag)
    return flags


def build_raw_news_rows(code: str, name: str, items: list[NewsItem]) -> list[dict]:
    rows: list[dict] = []
    for index, item in enumerate(items, start=1):
        rows.append(
            {
                "code": code,
                "name": name,
                "news_rank": index,
                "title": item.title,
                "description": item.description,
                "link": item.link,
                "pub_date": item.pub_date.isoformat(),
                "keyword_flags": ", ".join(sorted(find_item_keyword_flags(item))),
            }
        )
    return rows


def find_item_keyword_flags(item: NewsItem) -> set[str]:
    text = f"{item.title} {item.description}"
    flags: set[str] = set()
    for keyword, flag in {**NEGATIVE_KEYWORDS, **POSITIVE_KEYWORDS}.items():
        if keyword in text:
            flags.add(flag)
    return flags


def classify_sentiment(risk_flags: list[str], positive_flags: list[str]) -> str:
    if risk_flags and not positive_flags:
        return "negative"
    if positive_flags and not risk_flags:
        return "positive"
    if risk_flags and positive_flags:
        return "mixed"
    return "neutral"


def build_news_summary(
    news_count: int,
    sentiment: str,
    risk_flags: list[str],
    positive_flags: list[str],
) -> str:
    if news_count == 0:
        return "분석 기간 내 확인된 뉴스 없음."

    if sentiment == "negative":
        return f"부정 리스크 키워드 확인: {', '.join(risk_flags)}."
    if sentiment == "positive":
        return f"긍정 키워드 확인: {', '.join(positive_flags)}."
    if sentiment == "mixed":
        return (
            f"긍정/부정 키워드가 함께 확인됨. 긍정: {', '.join(positive_flags)} / "
            f"리스크: {', '.join(risk_flags)}."
        )
    return "뉴스는 있으나 명확한 긍정/부정 키워드는 적음."


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
