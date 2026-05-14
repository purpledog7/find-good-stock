from __future__ import annotations

import re
from typing import Callable

import pandas as pd

from config import NEWS_RAW_COLUMNS
from src.news_client import NaverNewsClient, WebSearchItem
from src.stock_codes import normalize_stock_code, normalize_stock_code_series


ProgressCallback = Callable[[str], None] | None

PHASE2_SUMMARY_COLUMNS = [
    "phase2_rank",
    "rank",
    "code",
    "name",
    "price",
    "market_cap_eok",
    "theme_categories",
    "employee_count",
    "employee_source_title",
    "employee_source_link",
    "revenue_won",
    "revenue_eok",
    "revenue_source",
    "revenue_source_title",
    "revenue_source_link",
    "revenue_per_employee_won",
    "revenue_per_employee_eok",
    "important_news_count",
    "important_news_titles",
    "important_news_links",
    "web_result_count",
    "key_web_titles",
    "key_web_links",
    "phase2_confidence",
    "phase2_flags",
    "phase2_summary",
]

PHASE2_WEB_RAW_COLUMNS = [
    "code",
    "name",
    "query",
    "result_rank",
    "title",
    "description",
    "link",
]

IMPORTANT_NEWS_KEYWORDS = [
    "수주",
    "계약",
    "공급",
    "MOU",
    "투자",
    "증설",
    "양산",
    "특허",
    "인증",
    "승인",
    "실적",
    "매출",
    "흑자",
    "AI",
    "로봇",
    "우주",
    "양자",
    "반도체",
    "자율주행",
]

WEB_IMPORTANT_NEWS_KEYWORDS = [
    keyword
    for keyword in IMPORTANT_NEWS_KEYWORDS
    if keyword not in {"AI", "로봇", "우주", "양자", "반도체", "자율주행", "매출"}
]

RISK_NEWS_KEYWORDS = [
    "유상증자",
    "전환사채",
    "CB",
    "BW",
    "적자",
    "손실",
    "거래정지",
    "관리종목",
    "투자주의",
    "투자경고",
    "소송",
    "횡령",
    "배임",
]


def collect_future_value_phase2_research(
    candidates_df: pd.DataFrame,
    client: NaverNewsClient,
    raw_news_df: pd.DataFrame,
    top_n: int = 30,
    web_max_items: int = 10,
    dart_df: pd.DataFrame | None = None,
    progress: ProgressCallback = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = candidates_df.copy()
    if top_n > 0:
        candidates = candidates.head(top_n).copy()
    if candidates.empty:
        return (
            pd.DataFrame(columns=PHASE2_SUMMARY_COLUMNS),
            pd.DataFrame(columns=PHASE2_WEB_RAW_COLUMNS),
        )

    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    raw_news_df["code"] = normalize_stock_code_series(raw_news_df["code"])
    dart_df = normalize_dart_frame(dart_df)

    summary_rows: list[dict] = []
    web_rows: list[dict] = []
    for index, row in enumerate(candidates.itertuples(index=False), start=1):
        code = normalize_stock_code(getattr(row, "code"))
        name = str(getattr(row, "name", "") or "")
        emit_progress(progress, f"phase2 web research ({index}/{len(candidates)}): {name}")
        candidate_web_rows: list[dict] = []
        for query in build_phase2_web_queries(name):
            items = client.search_web_documents(query, display=web_max_items)
            query_rows = build_phase2_web_rows(code, name, query, items)
            candidate_web_rows.extend(query_rows)

        candidate_web_rows = dedupe_web_rows(candidate_web_rows)
        web_rows.extend(candidate_web_rows)
        news_group = raw_news_df[raw_news_df["code"] == code]
        dart_row = dart_df[dart_df["code"] == code].head(1)
        summary_rows.append(
            summarize_phase2_candidate(
                pd.Series(row._asdict()),
                candidate_web_rows,
                news_group,
                dart_row.iloc[0] if not dart_row.empty else None,
                phase2_rank=index,
            )
        )

    return (
        pd.DataFrame(summary_rows, columns=PHASE2_SUMMARY_COLUMNS),
        pd.DataFrame(web_rows, columns=PHASE2_WEB_RAW_COLUMNS),
    )


def build_phase2_web_queries(name: str) -> list[str]:
    cleaned = str(name or "").strip()
    if not cleaned:
        return []
    return [
        f"{cleaned} 사원수",
        f"{cleaned} 직원수",
        f"{cleaned} 매출액",
        f"{cleaned} 연매출",
        f"{cleaned} 회사소개",
        f"{cleaned} IR",
        f"{cleaned} 사업보고서",
        f"{cleaned} 중요 뉴스",
    ]


def build_phase2_web_rows(
    code: str,
    name: str,
    query: str,
    items: list[WebSearchItem],
) -> list[dict]:
    rows: list[dict] = []
    for index, item in enumerate(items, start=1):
        rows.append(
            {
                "code": code,
                "name": name,
                "query": query,
                "result_rank": index,
                "title": item.title,
                "description": item.description,
                "link": item.link,
            }
        )
    return rows


def summarize_phase2_candidate(
    candidate: pd.Series,
    web_rows: list[dict],
    news_group: pd.DataFrame,
    dart_row: pd.Series | None,
    phase2_rank: int,
) -> dict:
    employee = extract_employee_candidate(web_rows)
    web_revenue = extract_revenue_candidate(web_rows)
    dart_revenue = extract_dart_revenue(dart_row)
    revenue = dart_revenue if dart_revenue is not None else web_revenue
    important_news = extract_important_news(news_group, web_rows)
    employee_count = employee["employee_count"]
    revenue_won = revenue["revenue_won"] if revenue else None
    revenue_per_employee = None
    if employee_count and revenue_won:
        revenue_per_employee = int(revenue_won / employee_count)

    flags = build_phase2_flags(employee_count, revenue, important_news)
    confidence = classify_phase2_confidence(employee_count, revenue, important_news)
    key_web_rows = web_rows[:5]
    row = {
        "phase2_rank": phase2_rank,
        "rank": candidate.get("rank", ""),
        "code": normalize_stock_code(candidate.get("code")),
        "name": candidate.get("name", ""),
        "price": candidate.get("price", ""),
        "market_cap_eok": candidate.get("market_cap_eok", ""),
        "theme_categories": candidate.get("theme_categories", ""),
        "employee_count": employee_count or "",
        "employee_source_title": employee["source_title"],
        "employee_source_link": employee["source_link"],
        "revenue_won": revenue_won or "",
        "revenue_eok": to_eok(revenue_won),
        "revenue_source": revenue["source"] if revenue else "",
        "revenue_source_title": revenue["source_title"] if revenue else "",
        "revenue_source_link": revenue["source_link"] if revenue else "",
        "revenue_per_employee_won": revenue_per_employee or "",
        "revenue_per_employee_eok": to_eok(revenue_per_employee),
        "important_news_count": len(important_news),
        "important_news_titles": " | ".join(item["title"] for item in important_news[:5]),
        "important_news_links": " | ".join(item["link"] for item in important_news[:5]),
        "web_result_count": len(web_rows),
        "key_web_titles": " | ".join(clean_oneline(item.get("title", "")) for item in key_web_rows),
        "key_web_links": " | ".join(clean_oneline(item.get("link", "")) for item in key_web_rows),
        "phase2_confidence": confidence,
        "phase2_flags": ", ".join(flags),
    }
    row["phase2_summary"] = build_phase2_summary(row)
    return row


def extract_employee_candidate(web_rows: list[dict]) -> dict:
    patterns = [
        re.compile(
            r"(?:사원\s*수|직원\s*수|종업원\s*수|임직원\s*수|사원|직원|임직원)"
            r"\s*(?:은|는|이|가)?\s*[:：]?\s*([0-9,]+)\s*명"
        ),
        re.compile(r"([0-9,]+)\s*명\s*(?:규모|임직원|직원|사원)"),
    ]
    for row in web_rows:
        text = row_text(row)
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            count = parse_int(match.group(1))
            if count and 1 <= count <= 200_000:
                return {
                    "employee_count": count,
                    "source_title": clean_oneline(row.get("title", "")),
                    "source_link": clean_oneline(row.get("link", "")),
                }
    return {"employee_count": None, "source_title": "", "source_link": ""}


def extract_revenue_candidate(web_rows: list[dict]) -> dict | None:
    patterns = [
        re.compile(
            r"(?:연매출|매출\s*액|매출)"
            r"\s*(?:은|는|이|가)?\s*[:：]?\s*([0-9,.]+)\s*(조원|억원|억\s*원|억|백만원|만원|원)"
        ),
        re.compile(r"([0-9,.]+)\s*(조원|억원|억\s*원|억|백만원|만원|원)\s*(?:매출|매출액|연매출)"),
    ]
    for row in web_rows:
        text = row_text(row)
        for pattern in patterns:
            match = pattern.search(text)
            if not match:
                continue
            revenue_won = parse_korean_money_to_won(match.group(1), match.group(2))
            if revenue_won and revenue_won > 0:
                return {
                    "revenue_won": revenue_won,
                    "source": "naver_web",
                    "source_title": clean_oneline(row.get("title", "")),
                    "source_link": clean_oneline(row.get("link", "")),
                }
    return None


def extract_dart_revenue(dart_row: pd.Series | None) -> dict | None:
    if dart_row is None:
        return None
    revenue = pd.to_numeric(dart_row.get("revenue"), errors="coerce")
    if pd.isna(revenue) or revenue <= 0:
        return None
    year = str(dart_row.get("dart_bsns_year", "") or "")
    return {
        "revenue_won": int(revenue),
        "source": f"OpenDART {year}".strip(),
        "source_title": "OpenDART financial statement",
        "source_link": "",
    }


def extract_important_news(
    news_group: pd.DataFrame,
    web_rows: list[dict] | None = None,
) -> list[dict]:
    rows = []
    for _, row in news_group.iterrows():
        text = row_text(row)
        is_important = any(keyword.casefold() in text.casefold() for keyword in IMPORTANT_NEWS_KEYWORDS)
        is_risk = any(keyword.casefold() in text.casefold() for keyword in RISK_NEWS_KEYWORDS)
        if not is_important and not is_risk:
            continue
        rows.append(
            {
                "title": clean_oneline(row.get("title", "")),
                "link": clean_oneline(row.get("link", "") or row.get("naver_link", "")),
                "is_risk": is_risk,
                "pub_date": str(row.get("pub_date", "")),
            }
        )
    rows = sorted(rows, key=lambda item: item["pub_date"], reverse=True)
    if rows:
        return dedupe_important_news(rows)

    for row in web_rows or []:
        query = clean_oneline(row.get("query", ""))
        text = row_text(row)
        is_news_query = "중요 뉴스" in query
        is_important = any(keyword.casefold() in text.casefold() for keyword in WEB_IMPORTANT_NEWS_KEYWORDS)
        is_risk = any(keyword.casefold() in text.casefold() for keyword in RISK_NEWS_KEYWORDS)
        if not is_news_query and not is_important and not is_risk:
            continue
        rows.append(
            {
                "title": clean_oneline(row.get("title", "")),
                "link": clean_oneline(row.get("link", "")),
                "is_risk": is_risk,
                "pub_date": "",
            }
        )
    return dedupe_important_news(rows)


def build_phase2_flags(employee_count, revenue: dict | None, important_news: list[dict]) -> list[str]:
    flags: list[str] = []
    if not employee_count:
        flags.append("missing_employee_count")
    if not revenue:
        flags.append("missing_revenue")
    elif revenue.get("source") == "naver_web":
        flags.append("web_revenue_only")
    if not important_news:
        flags.append("no_important_news")
    if any(item.get("is_risk") for item in important_news):
        flags.append("risk_news_found")
    return flags


def classify_phase2_confidence(employee_count, revenue: dict | None, important_news: list[dict]) -> str:
    score = 0
    if employee_count:
        score += 1
    if revenue:
        score += 2 if revenue.get("source", "").startswith("OpenDART") else 1
    if important_news:
        score += 1
    if score >= 4:
        return "strong"
    if score >= 2:
        return "medium"
    return "weak"


def build_phase2_summary(row: dict) -> str:
    employee = f"{row['employee_count']}명" if row.get("employee_count") else "unknown"
    revenue = f"{row['revenue_eok']}억원" if row.get("revenue_eok") != "" else "unknown"
    per_employee = (
        f"{row['revenue_per_employee_eok']}억원/명"
        if row.get("revenue_per_employee_eok") != ""
        else "unknown"
    )
    news_count = row.get("important_news_count", 0)
    flags = row.get("phase2_flags", "")
    return (
        f"{row.get('name', '')}({row.get('code', '')}) phase2: "
        f"employees={employee}, annual_revenue={revenue}, revenue_per_employee={per_employee}, "
        f"important_news={news_count}, confidence={row.get('phase2_confidence', '')}, flags={flags}."
    )


def normalize_dart_frame(dart_df: pd.DataFrame | None) -> pd.DataFrame:
    if dart_df is None or dart_df.empty:
        return pd.DataFrame(columns=["code", "revenue", "dart_bsns_year"])
    result = dart_df.copy()
    if "code" not in result.columns:
        result["code"] = ""
    result["code"] = normalize_stock_code_series(result["code"])
    return result


def dedupe_web_rows(rows: list[dict]) -> list[dict]:
    result = []
    seen: set[str] = set()
    for row in rows:
        key = "|".join(
            [
                clean_oneline(row.get("link", "")),
                clean_oneline(row.get("title", "")),
                clean_oneline(row.get("description", "")),
            ]
        )
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def dedupe_important_news(rows: list[dict]) -> list[dict]:
    result = []
    seen: set[str] = set()
    for row in rows:
        key = row.get("link") or row.get("title")
        if key in seen:
            continue
        seen.add(key)
        result.append(row)
    return result


def parse_korean_money_to_won(number_text: str, unit_text: str) -> int | None:
    try:
        number = float(str(number_text).replace(",", "").strip())
    except ValueError:
        return None
    unit = str(unit_text or "").replace(" ", "")
    if unit == "조원":
        return int(number * 1_000_000_000_000)
    if unit in {"억원", "억"}:
        return int(number * 100_000_000)
    if unit == "백만원":
        return int(number * 1_000_000)
    if unit == "만원":
        return int(number * 10_000)
    if unit == "원":
        return int(number)
    return None


def parse_int(value) -> int | None:
    try:
        return int(str(value).replace(",", "").strip())
    except ValueError:
        return None


def to_eok(value) -> float | str:
    if value is None or value == "":
        return ""
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return ""
    return round(float(numeric) / 100_000_000, 2)


def row_text(row) -> str:
    return f"{row.get('title', '')} {row.get('description', '')}"


def clean_oneline(value) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.loc[:, ~df.columns.duplicated()].copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
