from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from config import (
    FUTURE_VALUE_MAX_PRICE,
    FUTURE_VALUE_NEWS_LOOKBACK_DAYS,
    KST_TIMEZONE,
    NEWS_RAW_COLUMNS,
)
from src.stock_codes import normalize_stock_code, normalize_stock_code_series


FUTURE_VALUE_COLUMNS = [
    "date",
    "market_date",
    "rank",
    "code",
    "name",
    "market",
    "price",
    "market_cap",
    "market_cap_eok",
    "per",
    "pbr",
    "eps",
    "bps",
    "estimated_roe",
    "sector",
    "industry",
    "theme_categories",
    "theme_evidence",
    "static_theme_categories",
    "static_theme_evidence",
    "news_theme_categories",
    "news_theme_evidence",
    "news_count",
    "relevant_news_count",
    "theme_news_count",
    "key_news_titles",
    "key_news_links",
    "naver_finance_url",
    "research_queries",
    "future_value_score",
    "risk_flags",
]

FUTURE_VALUE_AUDIT_COLUMNS = FUTURE_VALUE_COLUMNS + [
    "future_value_eligible",
    "filter_reason",
]

FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS = [
    "code",
    "news_count",
    "relevant_news_count",
    "theme_news_count",
    "news_theme_categories",
    "news_theme_evidence",
    "key_news_titles",
    "key_news_links",
]

FUTURE_VALUE_THEMES = {
    "it_software": {
        "label": "IT/software",
        "keywords": [
            "IT",
            "software",
            "cloud",
            "SaaS",
            "ERP",
            "platform",
            "solution",
            "digital",
            "핀테크",
            "소프트웨어",
            "클라우드",
            "플랫폼",
            "솔루션",
            "디지털",
        ],
    },
    "ai_data_center": {
        "label": "AI/data_center",
        "keywords": [
            "AI",
            "artificial intelligence",
            "LLM",
            "generative",
            "data center",
            "GPU",
            "NPU",
            "machine learning",
            "인공지능",
            "생성형",
            "데이터센터",
            "엔비디아",
            "머신러닝",
            "딥러닝",
        ],
    },
    "semiconductor_materials": {
        "label": "semiconductor/materials",
        "keywords": [
            "semiconductor",
            "HBM",
            "EUV",
            "fabless",
            "foundry",
            "wafer",
            "반도체",
            "소부장",
            "팹리스",
            "파운드리",
            "웨이퍼",
            "패키징",
            "후공정",
        ],
    },
    "robot_automation": {
        "label": "robot/automation",
        "keywords": [
            "robot",
            "robotics",
            "automation",
            "humanoid",
            "smart factory",
            "로봇",
            "로보틱스",
            "자동화",
            "휴머노이드",
            "스마트팩토리",
            "협동로봇",
        ],
    },
    "space_aerospace": {
        "label": "space/aerospace",
        "keywords": [
            "space",
            "aerospace",
            "satellite",
            "launch vehicle",
            "UAM",
            "우주",
            "항공",
            "항공우주",
            "위성",
            "발사체",
            "드론",
        ],
    },
    "quantum_security": {
        "label": "quantum/security",
        "keywords": [
            "quantum",
            "cybersecurity",
            "security",
            "encryption",
            "PQC",
            "양자",
            "양자컴퓨터",
            "양자암호",
            "보안",
            "사이버보안",
            "암호",
        ],
    },
    "autonomous_mobility": {
        "label": "autonomous/mobility",
        "keywords": [
            "autonomous",
            "mobility",
            "ADAS",
            "lidar",
            "sensor",
            "EV",
            "자율주행",
            "모빌리티",
            "라이다",
            "센서",
            "전기차",
        ],
    },
}

EXCLUDED_NAME_KEYWORDS = [
    "스팩",
    "기업인수목적",
    "special purpose acquisition",
    "SPAC",
]


def build_future_value_universe(
    snapshot_df: pd.DataFrame,
    market_date: str,
    max_price: int = FUTURE_VALUE_MAX_PRICE,
) -> pd.DataFrame:
    if snapshot_df.empty:
        return pd.DataFrame(columns=FUTURE_VALUE_AUDIT_COLUMNS)

    result = ensure_columns(
        snapshot_df,
        [
            "code",
            "name",
            "market",
            "price",
            "market_cap",
            "per",
            "pbr",
            "eps",
            "bps",
            "estimated_roe",
            "sector",
            "industry",
        ],
    ).copy()
    result["code"] = normalize_stock_code_series(result["code"])
    result = result[result["code"] != ""].drop_duplicates("code", keep="first")
    result = coerce_numeric_columns(
        result,
        ["price", "market_cap", "per", "pbr", "eps", "bps", "estimated_roe"],
    )
    result["market"] = result["market"].fillna("").astype(str)
    result["date"] = market_date
    result["market_date"] = market_date
    result["market_cap_eok"] = (result["market_cap"] / 100_000_000).round(2)

    static_matches = result.apply(match_static_themes, axis=1)
    result["static_theme_categories"] = static_matches.apply(format_theme_categories)
    result["static_theme_evidence"] = static_matches.apply(format_theme_evidence)
    result["theme_categories"] = result["static_theme_categories"]
    result["theme_evidence"] = result["static_theme_evidence"]
    result["news_theme_categories"] = ""
    result["news_theme_evidence"] = ""
    result["news_count"] = 0
    result["relevant_news_count"] = 0
    result["theme_news_count"] = 0
    result["key_news_titles"] = ""
    result["key_news_links"] = ""
    result["naver_finance_url"] = result["code"].apply(build_naver_finance_url)
    result["research_queries"] = result.apply(build_research_queries, axis=1)
    result["risk_flags"] = ""

    result["future_value_eligible"] = build_future_value_hard_filter_mask(result, max_price)
    result["filter_reason"] = result.apply(
        lambda row: build_future_value_filter_reason(row, max_price),
        axis=1,
    )
    result["future_value_score"] = calculate_future_value_score(result)
    result = sort_future_value_candidates(result).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    return normalize_future_value_audit_columns(result)


def build_future_value_hard_filter_mask(df: pd.DataFrame, max_price: int) -> pd.Series:
    return (
        df["market"].str.upper().eq("KOSDAQ")
        & pd.to_numeric(df["price"], errors="coerce").between(1, max_price)
        & ~df["name"].fillna("").astype(str).apply(is_excluded_future_value_name)
    )


def build_future_value_filter_reason(row: pd.Series, max_price: int) -> str:
    reasons: list[str] = []
    if str(row.get("market", "")).upper() != "KOSDAQ":
        reasons.append("not_kosdaq")
    price = safe_number(row.get("price"))
    if price <= 0:
        reasons.append("price_missing")
    elif price > max_price:
        reasons.append("price_gt_max")
    if is_excluded_future_value_name(str(row.get("name", ""))):
        reasons.append("excluded_spac_or_shell")
    return "pass" if not reasons else ", ".join(reasons)


def score_future_value_news_candidates(
    candidates_df: pd.DataFrame,
    raw_news_df: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> pd.DataFrame:
    if candidates_df.empty:
        return pd.DataFrame(columns=FUTURE_VALUE_COLUMNS)

    result = ensure_columns(candidates_df, FUTURE_VALUE_AUDIT_COLUMNS).copy()
    result["code"] = normalize_stock_code_series(result["code"])
    news_df = analyze_future_value_news(raw_news_df, analysis_start_dt, analysis_end_dt)
    result = result.drop(
        columns=[column for column in FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS if column != "code"],
        errors="ignore",
    )
    result = result.merge(news_df, on="code", how="left")
    result = fill_missing_future_value_news_columns(result)
    result["theme_categories"] = result.apply(combine_static_and_news_categories, axis=1)
    result["theme_evidence"] = result.apply(combine_static_and_news_evidence, axis=1)
    result["future_value_score"] = calculate_future_value_score(result)
    result = sort_future_value_candidates(result).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    return normalize_future_value_audit_columns(result)


def select_future_value_candidates(
    evaluated_df: pd.DataFrame,
    candidate_limit: int = 0,
) -> pd.DataFrame:
    if evaluated_df.empty:
        return pd.DataFrame(columns=FUTURE_VALUE_COLUMNS)

    result = ensure_columns(evaluated_df, FUTURE_VALUE_AUDIT_COLUMNS).copy()
    result = result[result["future_value_eligible"].apply(parse_bool_like)].copy()
    result = result[result["theme_categories"].fillna("").astype(str).str.strip() != ""].copy()
    if result.empty:
        return pd.DataFrame(columns=FUTURE_VALUE_COLUMNS)

    result = sort_future_value_candidates(result).reset_index(drop=True)
    if candidate_limit > 0:
        result = result.head(candidate_limit).copy()
    result["rank"] = range(1, len(result) + 1)
    return normalize_future_value_columns(result)


def analyze_future_value_news(
    raw_news_df: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> pd.DataFrame:
    if raw_news_df.empty:
        return pd.DataFrame(columns=FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS)

    prepared = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS).copy()
    prepared["code"] = normalize_stock_code_series(prepared["code"])
    rows: list[dict] = []
    for code, group in prepared.groupby("code", dropna=False):
        normalized_code = normalize_stock_code(code)
        if not normalized_code:
            continue
        rows.append(
            analyze_future_value_news_group(
                normalized_code,
                group,
                analysis_start_dt,
                analysis_end_dt,
            )
        )
    if not rows:
        return pd.DataFrame(columns=FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS)
    return pd.DataFrame(rows, columns=FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS)


def analyze_future_value_news_group(
    code: str,
    group: pd.DataFrame,
    analysis_start_dt: datetime,
    analysis_end_dt: datetime,
) -> dict:
    timezone = analysis_end_dt.tzinfo or ZoneInfo(KST_TIMEZONE)
    company_name = first_non_empty_value(group.get("name", pd.Series(dtype="object")))
    parsed_rows: list[tuple[pd.Series, datetime]] = []
    for _, row in group.iterrows():
        pub_dt = parse_news_timestamp(row.get("pub_date"), timezone)
        if pub_dt is None:
            continue
        if analysis_start_dt <= pub_dt <= analysis_end_dt:
            parsed_rows.append((row, pub_dt))

    unique_rows = dedupe_news_rows(parsed_rows)
    relevant_rows = [
        (row, pub_dt)
        for row, pub_dt in unique_rows
        if is_relevant_future_value_news(company_name, row)
    ]
    theme_matches: dict[str, set[str]] = {}
    theme_rows: list[tuple[pd.Series, datetime]] = []
    for row, pub_dt in relevant_rows:
        row_matches = match_themes_in_text(
            f"{row.get('title', '')} {row.get('description', '')}"
        )
        if not row_matches:
            continue
        theme_rows.append((row, pub_dt))
        for category, terms in row_matches.items():
            theme_matches.setdefault(category, set()).update(terms)

    key_rows = sorted(theme_rows, key=lambda item: item[1], reverse=True)[:5]
    return {
        "code": code,
        "news_count": len(unique_rows),
        "relevant_news_count": len(relevant_rows),
        "theme_news_count": len(theme_rows),
        "news_theme_categories": format_theme_categories(theme_matches),
        "news_theme_evidence": format_theme_evidence(theme_matches),
        "key_news_titles": " | ".join(
            clean_oneline(row.get("title", "")) for row, _ in key_rows
        ),
        "key_news_links": " | ".join(
            clean_oneline(row.get("link", "") or row.get("naver_link", ""))
            for row, _ in key_rows
        ),
    }


def build_future_value_news_window(
    market_date: str,
    lookback_days: int = FUTURE_VALUE_NEWS_LOOKBACK_DAYS,
    now: datetime | None = None,
) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(KST_TIMEZONE)
    current_dt = (now or datetime.now(timezone)).astimezone(timezone)
    target_date = datetime.strptime(market_date, "%Y-%m-%d").date()
    default_end_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        23,
        59,
        59,
        tzinfo=timezone,
    )
    end_dt = min(default_end_dt, current_dt)
    start_date = end_dt.date() - timedelta(days=max(lookback_days, 1) - 1)
    start_dt = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone)
    return start_dt, end_dt


def build_future_value_news_queries(name: str) -> list[str]:
    cleaned = str(name).strip()
    if not cleaned:
        return []
    return [
        cleaned,
        f"{cleaned} 주식",
        f"{cleaned} AI",
        f"{cleaned} 로봇",
        f"{cleaned} 우주",
        f"{cleaned} 양자",
        f"{cleaned} 반도체",
        f"{cleaned} 소프트웨어",
    ]


def build_naver_finance_url(code: str) -> str:
    normalized = normalize_stock_code(code)
    if not normalized:
        return ""
    return f"https://finance.naver.com/item/main.naver?code={normalized}"


def build_research_queries(row: pd.Series) -> str:
    name = str(row.get("name", "") or "").strip()
    code = normalize_stock_code(row.get("code"))
    if not name:
        return ""
    queries = [
        f"{name} 공식 홈페이지",
        f"{name} IR",
        f"{name} 사업보고서",
        f"{name} 회사소개",
    ]
    if code:
        queries.append(f"{name} {code}")
    return " | ".join(queries)


def match_static_themes(row: pd.Series) -> dict[str, set[str]]:
    text = " ".join(
        [
            str(row.get("name", "")),
            str(row.get("sector", "")),
            str(row.get("industry", "")),
        ]
    )
    return match_themes_in_text(text)


def match_themes_in_text(text: str) -> dict[str, set[str]]:
    raw_text = str(text or "")
    normalized_text = normalize_theme_text(raw_text)
    if not normalized_text:
        return {}

    matches: dict[str, set[str]] = {}
    for category, definition in FUTURE_VALUE_THEMES.items():
        for keyword in definition["keywords"]:
            if keyword_matches_text(raw_text, normalized_text, keyword):
                matches.setdefault(category, set()).add(str(keyword))
    return matches


def format_theme_categories(matches) -> str:
    if not isinstance(matches, dict) or not matches:
        return ""
    labels = [FUTURE_VALUE_THEMES[category]["label"] for category in sorted(matches)]
    return ", ".join(labels)


def format_theme_evidence(matches) -> str:
    if not isinstance(matches, dict) or not matches:
        return ""
    parts = []
    for category in sorted(matches):
        label = FUTURE_VALUE_THEMES[category]["label"]
        terms = ", ".join(sorted(str(term) for term in matches[category]))
        parts.append(f"{label}: {terms}")
    return " | ".join(parts)


def combine_static_and_news_categories(row: pd.Series) -> str:
    return combine_csv_values(row.get("static_theme_categories", ""), row.get("news_theme_categories", ""))


def combine_static_and_news_evidence(row: pd.Series) -> str:
    return combine_pipe_values(row.get("static_theme_evidence", ""), row.get("news_theme_evidence", ""))


def calculate_future_value_score(df: pd.DataFrame) -> pd.Series:
    static_count = text_column(df, "static_theme_categories").apply(count_csv_values)
    news_count = text_column(df, "news_theme_categories").apply(count_csv_values)
    relevant_news = numeric_column(df, "relevant_news_count")
    theme_news = numeric_column(df, "theme_news_count")
    market_cap = numeric_column(df, "market_cap")
    size_hint = (market_cap / 100_000_000).clip(lower=0, upper=500) / 100
    return (
        static_count * 12
        + news_count * 18
        + relevant_news.clip(upper=10) * 1.5
        + theme_news.clip(upper=8) * 3
        + size_hint
    ).round(2)


def sort_future_value_candidates(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    for column in ["future_value_eligible", "future_value_score", "theme_news_count", "relevant_news_count", "market_cap"]:
        if column not in result.columns:
            result[column] = 0
    return result.sort_values(
        by=[
            "future_value_eligible",
            "future_value_score",
            "theme_news_count",
            "relevant_news_count",
            "market_cap",
            "price",
            "code",
        ],
        ascending=[False, False, False, False, False, True, True],
        na_position="last",
    )


def is_relevant_future_value_news(company_name: str, row: pd.Series) -> bool:
    name = str(company_name or "").strip()
    if not name:
        return True
    text = normalize_theme_text(f"{row.get('title', '')} {row.get('description', '')}")
    return normalize_theme_text(name) in text


def dedupe_news_rows(rows: list[tuple[pd.Series, datetime]]) -> list[tuple[pd.Series, datetime]]:
    result: list[tuple[pd.Series, datetime]] = []
    seen: set[str] = set()
    for row, pub_dt in rows:
        key = build_news_key(row, pub_dt)
        if key in seen:
            continue
        seen.add(key)
        result.append((row, pub_dt))
    return result


def build_news_key(row: pd.Series, pub_dt: datetime) -> str:
    link = str(row.get("link", "") or row.get("naver_link", "")).strip()
    if link:
        return link
    return f"{normalize_theme_text(row.get('title', ''))}:{pub_dt.date().isoformat()}"


def parse_news_timestamp(value, timezone) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone)
    return parsed.astimezone(timezone)


def first_non_empty_value(values: pd.Series) -> str:
    for value in values.dropna().astype(str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return ""


def fill_missing_future_value_news_columns(df: pd.DataFrame) -> pd.DataFrame:
    result = ensure_columns(df, FUTURE_VALUE_NEWS_ANALYSIS_COLUMNS)
    for column in ["news_count", "relevant_news_count", "theme_news_count"]:
        result[column] = pd.to_numeric(result[column], errors="coerce").fillna(0).astype(int)
    for column in ["news_theme_categories", "news_theme_evidence", "key_news_titles", "key_news_links"]:
        result[column] = result[column].fillna("").astype(str)
    return result


def normalize_future_value_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_columns(df, FUTURE_VALUE_COLUMNS)
    return normalized[FUTURE_VALUE_COLUMNS]


def normalize_future_value_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = ensure_columns(df, FUTURE_VALUE_AUDIT_COLUMNS)
    return normalized[FUTURE_VALUE_AUDIT_COLUMNS]


def ensure_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.loc[:, ~df.columns.duplicated()].copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result


def coerce_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def numeric_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(0, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(0)


def text_column(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series("", index=df.index, dtype="object")
    return df[column].fillna("").astype(str)


def is_excluded_future_value_name(name: str) -> bool:
    raw_name = str(name or "")
    normalized_name = normalize_theme_text(raw_name)
    return any(keyword_matches_text(raw_name, normalized_name, keyword) for keyword in EXCLUDED_NAME_KEYWORDS)


def normalize_theme_text(value) -> str:
    return re.sub(r"[\W_]+", "", str(value or "").casefold())


def keyword_matches_text(raw_text: str, normalized_text: str, keyword: str) -> bool:
    keyword_text = str(keyword or "")
    normalized_keyword = normalize_theme_text(keyword_text)
    if not normalized_keyword:
        return False
    if is_short_ascii_keyword(keyword_text):
        pattern = rf"(?<![A-Za-z0-9]){re.escape(keyword_text)}(?![A-Za-z0-9])"
        return re.search(pattern, raw_text, flags=re.IGNORECASE) is not None
    return normalized_keyword in normalized_text


def is_short_ascii_keyword(value: str) -> bool:
    stripped = str(value or "").strip()
    return 1 <= len(stripped) <= 4 and stripped.isascii() and stripped.replace("-", "").isalnum()


def clean_oneline(value) -> str:
    return str(value or "").replace("\r", " ").replace("\n", " ").strip()


def combine_csv_values(*values) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in str(value or "").split(","):
            cleaned = item.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                items.append(cleaned)
    return ", ".join(items)


def combine_pipe_values(*values) -> str:
    items: list[str] = []
    seen: set[str] = set()
    for value in values:
        for item in str(value or "").split("|"):
            cleaned = item.strip()
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                items.append(cleaned)
    return " | ".join(items)


def count_csv_values(value) -> int:
    return len([item for item in str(value or "").split(",") if item.strip()])


def parse_bool_like(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}


def safe_number(value) -> float:
    try:
        if pd.isna(value):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0
