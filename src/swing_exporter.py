from __future__ import annotations

from pathlib import Path

import pandas as pd

from config import CSV_ENCODING, NEWS_RAW_COLUMNS
from src.recommender import ensure_columns, format_markdown_text
from src.stock_codes import normalize_stock_code
from src.swing_scanner import SWING_CANDIDATE_COLUMNS


def save_swing_candidates(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_candidates.csv"
    normalized_df = ensure_columns(candidates_df, SWING_CANDIDATE_COLUMNS)
    normalized_df[SWING_CANDIDATE_COLUMNS].to_csv(path, index=False, encoding=CSV_ENCODING)
    return path


def save_swing_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    start_dt,
    end_dt,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_news_raw.md"
    raw_news_df = ensure_columns(raw_news_df, NEWS_RAW_COLUMNS)
    path.write_text(
        build_swing_news_markdown(
            raw_news_df[NEWS_RAW_COLUMNS],
            candidates_df,
            signal_date,
            start_dt,
            end_dt,
        ),
        encoding="utf-8",
    )
    return path


def build_swing_news_markdown(
    raw_news_df: pd.DataFrame,
    candidates_df: pd.DataFrame,
    signal_date: str,
    start_dt,
    end_dt,
) -> str:
    window_text = (
        "latest available news, no date filter"
        if start_dt is None and end_dt is None
        else f"{start_dt.isoformat() if start_dt is not None else 'unbounded'} ~ "
        f"{end_dt.isoformat() if end_dt is not None else 'unbounded'}"
    )
    lines = [
        f"# Swing Raw News - {signal_date}",
        "",
        f"- Window: {window_text}",
        "- Source: Naver News Open API",
        "- Summary: none; descriptions are search or publisher metadata previews, not AI summaries or full article bodies",
        "- Link policy: Original link is preferred; Naver link is kept separately when available",
        "",
    ]
    news_by_code = {
        normalize_stock_code(code): group.sort_values("news_rank")
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
                f"- Swing score: {company.get('swing_score', '')}",
                f"- Matched setups: {company.get('matched_setups', '')}",
                f"- News count: {len(group)}",
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


def save_swing_review_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    result_dir: Path,
    news_path: Path | None = None,
    backtest_path: Path | None = None,
) -> Path:
    result_dir.mkdir(parents=True, exist_ok=True)
    path = result_dir / f"{signal_date}_swing_review_prompt.md"
    path.write_text(
        build_swing_review_prompt(candidates_df, signal_date, news_path, backtest_path),
        encoding="utf-8",
    )
    return path


def build_swing_review_prompt(
    candidates_df: pd.DataFrame,
    signal_date: str,
    news_path: Path | None = None,
    backtest_path: Path | None = None,
) -> str:
    preview_columns = [
        "rank",
        "code",
        "name",
        "market",
        "sector",
        "swing_score",
        "value_score",
        "undervaluation_score",
        "average_discount_score",
        "value_trap_penalty",
        "event_pivot_score",
        "volume_breakout_score",
        "contraction_score",
        "darvas_breakout_score",
        "pullback_ladder_score",
        "pocket_pivot_score",
        "bb_squeeze_score",
        "anchored_vwap_score",
        "accumulation_score",
        "relative_strength_score",
        "rsi_score",
        "ema_trend_score",
        "risk_penalty",
        "matched_setups",
        "setup_tags",
        "risk_flags",
        "per",
        "pbr",
        "estimated_roe",
        "earnings_yield",
        "book_discount_pct",
        "per_vs_sector_pct",
        "pbr_vs_sector_pct",
        "market_return_3d",
        "market_positive_rate_1d",
        "price",
        "vwap20",
        "vwap50",
        "price_vs_ma20_pct",
        "price_vs_ma50_pct",
        "price_vs_vwap20_pct",
        "price_vs_vwap50_pct",
        "avwap_from_20d_low",
        "price_vs_avwap_pct",
        "ema10",
        "ema20",
        "ema20_extension_pct",
        "ema50_extension_pct",
        "rsi14",
        "tick_size",
        "return_1d",
        "return_3d",
        "return_5d",
        "trading_value_ratio_20d",
        "volume_ratio_20d",
        "entry_price",
        "add_price_1",
        "add_price_2",
        "add_price_3",
        "half_take_profit_price",
        "full_take_profit_price",
        "review_date",
        "review_date_3d",
        "review_date_5d",
    ]
    preview_df = ensure_columns(candidates_df, preview_columns)[preview_columns].copy()
    news_section = ""
    if news_path is not None:
        news_section = (
            "\n## 원본 뉴스 MD\n\n"
            f"- `{news_path}`\n"
            "- 후보별 최근 뉴스 원문 목록이야. 제목, 설명, 링크, 발행시각을 직접 읽고 촉매 강도를 판단해.\n"
        )
    backtest_section = ""
    if backtest_path is not None:
        backtest_section = (
            "\n## 간이 백테스트 CSV\n\n"
            f"- `{backtest_path}`\n"
            "- 최근 과거 신호에서 3/5거래일 안에 +4%, +7%, -10%를 찍었는지 확인한 자료야.\n"
        )

    return f"""# Swing Review Prompt - {signal_date}

아래 후보는 저평가, 상대강도, 거래량, 변동성 수축, AVWAP, RSI/EMA 추세 엔진이 KOSPI/KOSDAQ 전체를 스캔해서 만든 Top 후보야.

너의 역할:
- 투자 추천이 아니라 스윙 후보 검토 관점으로 분석해.
- 최종 메인 1개와 예비 2개를 골라.
- 3~5거래일 안에 +4% 반익절, +7% 전량익절 가능성이 있는지 보수적으로 판단해.
- 전일 종가 기준 진입이 이미 늦었으면 제외해.
- -4%, -8%, -10% 물타기 가격대가 지지선 관점에서 말이 되는지 검토해.
- 진입/물타기/익절 가격은 KRX 호가단위에 맞춘 값이야.
- 뉴스 원문을 읽고 촉매가 강한지, 단순 홍보성/노이즈인지 구분해.
- 위험 뉴스, 과열, 거래대금 부족, 긴 윗꼬리, 급등 후 피로감은 강하게 감점해.

매매 구조:
- 1차 진입: 700만원
- -4% 물타기: 300만원
- -8% 물타기: 300만원
- -10% 물타기: 200만원
- +4% 반익절
- +7% 전량익절
- 3거래일/5거래일 후 재검토

출력 형식:
1. 메인 후보 1개: 종목명, 코드, 선택 이유, 진입 보류 조건, 핵심 리스크
2. 예비 후보 2개: 각각 짧은 이유
3. 제외해야 할 상위 후보가 있으면 이유
4. 텔레그램으로 보낼 수 있는 짧은 요약문

## 스윙 후보

{dataframe_to_markdown(preview_df)}
{news_section}
{backtest_section}
"""


def dataframe_to_markdown(df: pd.DataFrame) -> str:
    headers = [str(column) for column in df.columns]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in df.iterrows():
        values = [format_markdown_table_cell(row[column]) for column in df.columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def format_markdown_table_cell(value) -> str:
    return (
        format_markdown_text(value)
        .replace("\r\n", " ")
        .replace("\r", " ")
        .replace("\n", " ")
        .replace("|", "/")
    )


def format_rank(value, fallback: int) -> int:
    try:
        if pd.isna(value):
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def is_truthy(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}
