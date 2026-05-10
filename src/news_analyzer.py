from __future__ import annotations

import time
from datetime import datetime
from typing import Callable

import pandas as pd

from config import NEWS_OUTPUT_COLUMNS, NEWS_RAW_COLUMNS
from src.news_client import NaverNewsClient, NewsItem
from src.stock_codes import normalize_stock_code


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
    "관리종목": "administrative_issue",
    "투자주의": "market_warning",
    "투자경고": "investment_warning",
    "투자위험": "investment_risk",
    "단기과열": "short_term_overheat",
    "불성실공시": "disclosure_violation",
    "상장폐지": "delisting",
    "환기종목": "investment_attention",
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
    start_dt: datetime | None,
    end_dt: datetime | None,
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
    start_dt: datetime | None,
    end_dt: datetime | None,
    max_items: int,
    progress: ProgressCallback = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    result = df.copy().drop(columns=NEWS_OUTPUT_COLUMNS, errors="ignore")
    summary_rows: list[dict] = []
    raw_rows: list[dict] = []

    for index, row in result.iterrows():
        code = normalize_stock_code(row["code"])
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

        items = filter_relevant_stock_news(name, code, items)
        summary_rows.append(summarize_news_items(items))
        raw_rows.extend(build_raw_news_rows(code, name, items))

    news_df = pd.DataFrame(summary_rows, index=result.index)
    raw_news_df = pd.DataFrame(raw_rows, columns=NEWS_RAW_COLUMNS)
    return pd.concat([result, news_df], axis=1), raw_news_df


def collect_raw_news_info(
    df: pd.DataFrame,
    client: NaverNewsClient,
    start_dt: datetime | None,
    end_dt: datetime | None,
    max_items: int,
    progress: ProgressCallback = None,
    enhanced_queries: bool = False,
    query_builder: Callable[[str], list[str]] | None = None,
    enrich_metadata: bool = True,
    deadline: float | None = None,
) -> pd.DataFrame:
    raw_rows: list[dict] = []

    for index, row in enumerate(df.itertuples(index=False), start=1):
        if deadline is not None and time.monotonic() >= deadline:
            emit_progress(progress, "  news time budget reached; continuing with collected news")
            break
        code = normalize_stock_code(getattr(row, "code"))
        name = str(getattr(row, "name"))
        emit_progress(progress, f"뉴스 검색 중 ({index}/{len(df)}): {name}")
        try:
            if enhanced_queries:
                queries = query_builder(name) if query_builder is not None else build_stock_news_queries(name)
                items = client.search_recent_news_multi(
                    queries,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    display=max_items,
                    enrich_metadata=enrich_metadata,
                )
            else:
                items = client.search_recent_news(
                    name,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    display=max_items,
                    enrich_metadata=enrich_metadata,
                )
        except RuntimeError as error:
            emit_progress(progress, f"  뉴스 검색 실패: {error}")
            items = []

        raw_rows.extend(build_raw_news_rows(code, name, items))

    return pd.DataFrame(raw_rows, columns=NEWS_RAW_COLUMNS)


def build_stock_news_queries(name: str) -> list[str]:
    return [
        name,
        f"{name} 주식",
        f"{name} 공시",
        f"{name} 계약",
        f"{name} 실적",
    ]


def filter_relevant_stock_news(
    name: str,
    code: str,
    items: list[NewsItem],
) -> list[NewsItem]:
    return [item for item in items if is_relevant_stock_news(name, code, item)]


def is_relevant_stock_news(name: str, code: str, item: NewsItem) -> bool:
    normalized_name = normalize_news_token(name)
    normalized_code = normalize_stock_code(code)
    text = normalize_news_token(f"{item.title} {item.description} {item.link} {item.naver_link}")
    if normalized_name and normalized_name in text:
        return True
    if normalized_code and normalized_code in text:
        return True
    return False


def normalize_news_token(value: str) -> str:
    return "".join(str(value).lower().split())


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
                "naver_link": item.naver_link,
                "description_truncated": is_truncated_preview(item.description),
                "pub_date": item.pub_date.isoformat(),
                "keyword_flags": ", ".join(sorted(find_item_keyword_flags(item))),
            }
        )
    return rows


def is_truncated_preview(value: str) -> bool:
    text = str(value or "").strip()
    return text.endswith("...") or text.endswith("…")


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
