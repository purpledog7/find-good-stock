from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd

from config import (
    APP_VERSION,
    FUTURE_VALUE_CANDIDATE_LIMIT,
    FUTURE_VALUE_MAX_PRICE,
    FUTURE_VALUE_NEWS_LOOKBACK_DAYS,
    FUTURE_VALUE_NEWS_MAX_ITEMS_DEFAULT,
    FUTURE_VALUE_NEWS_TIME_BUDGET_SECONDS,
    FUTURE_VALUE_PHASE2_TOP_N,
    FUTURE_VALUE_PHASE2_WEB_MAX_ITEMS_DEFAULT,
    NEWS_RAW_COLUMNS,
    RESULT_DIR,
)
from src.collector import collect_market_snapshot, find_latest_market_date, to_output_date
from src.future_value import (
    build_future_value_news_queries,
    build_future_value_news_window,
    build_future_value_universe,
    score_future_value_news_candidates,
    select_future_value_candidates,
)
from src.future_value_exporter import (
    save_future_value_all_evaluated,
    save_future_value_candidates,
    save_future_value_news_dataset,
    save_future_value_news_markdown,
    save_future_value_phase2_csv,
    save_future_value_phase2_review_prompt,
    save_future_value_phase2_summary,
    save_future_value_phase2_web_markdown,
    save_future_value_research_prompt,
    save_future_value_theme_markdown,
)
from src.future_value_phase2 import collect_future_value_phase2_research
from src.news_analyzer import collect_raw_news_info
from src.news_client import NaverNewsClient
from src.sector_enricher import add_sector_info


RESULT_MARKERS = (
    "_future_value_",
    "future_value_codex_last_message",
)


def main() -> None:
    args = parse_args()
    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    validate_args(args)
    print_progress(f"find-good-stock v{APP_VERSION}")
    print_progress("future value scan started")

    market_date_raw = find_latest_market_date(args.date, progress=print_progress)
    market_date = to_output_date(market_date_raw)
    print_progress(f"market date: {market_date}")

    print_progress("collecting KOSDAQ snapshot")
    snapshot_df = collect_market_snapshot("KOSDAQ", market_date_raw, progress=print_progress)
    if not args.skip_sector:
        print_progress("enriching sector info")
        snapshot_df = add_sector_info(snapshot_df, progress=print_progress)

    print_progress(f"building low-price universe: max_price={args.max_price:,}")
    evaluated_df = build_future_value_universe(
        snapshot_df,
        market_date=market_date,
        max_price=args.max_price,
    )
    hard_pool_df = evaluated_df[evaluated_df["future_value_eligible"].apply(parse_bool_like)].copy()
    print_progress(f"low-price KOSDAQ pool count: {len(hard_pool_df):,}")

    news_start_dt, news_end_dt = build_future_value_news_window(
        market_date,
        lookback_days=args.news_lookback_days,
    )
    if args.skip_news or hard_pool_df.empty:
        raw_news_df = pd.DataFrame(columns=NEWS_RAW_COLUMNS)
        scored_df = evaluated_df
    else:
        print_progress(
            f"collecting raw news: {news_start_dt.isoformat()} ~ {news_end_dt.isoformat()}"
        )
        raw_news_df = collect_raw_news_info(
            hard_pool_df,
            NaverNewsClient.from_env(
                request_sleep_seconds=args.news_request_sleep_seconds,
                resolve_page_metadata=args.enrich_news_metadata,
                request_timeout_seconds=args.news_request_timeout_seconds,
            ),
            start_dt=news_start_dt,
            end_dt=news_end_dt,
            max_items=args.news_max_items,
            progress=print_progress,
            enhanced_queries=True,
            query_builder=build_future_value_news_queries,
            enrich_metadata=args.enrich_news_metadata,
            deadline=build_deadline(args.news_time_budget_seconds),
        )
        scored_df = score_future_value_news_candidates(
            evaluated_df,
            raw_news_df,
            analysis_start_dt=news_start_dt,
            analysis_end_dt=news_end_dt,
        )

    candidates_df = select_future_value_candidates(
        scored_df,
        candidate_limit=args.candidate_limit,
    )
    print_progress(f"future-value evidence candidates: {len(candidates_df):,}")

    print_progress(f"clearing previous future-value outputs: {RESULT_DIR}")
    clear_result_dir(RESULT_DIR, markers=RESULT_MARKERS)

    all_evaluated_path = save_future_value_all_evaluated(scored_df, market_date, RESULT_DIR)
    candidates_path = save_future_value_candidates(candidates_df, market_date, RESULT_DIR)
    theme_path = save_future_value_theme_markdown(
        candidates_df,
        market_date,
        RESULT_DIR,
        max_price=args.max_price,
    )
    news_path = save_future_value_news_markdown(
        raw_news_df,
        candidates_df,
        market_date,
        RESULT_DIR,
        news_start_dt,
        news_end_dt,
    )
    dataset_path = save_future_value_news_dataset(
        candidates_df,
        raw_news_df,
        market_date,
        RESULT_DIR,
        news_start_dt,
        news_end_dt,
        max_price=args.max_price,
    )
    prompt_path = save_future_value_research_prompt(
        candidates_df,
        market_date,
        RESULT_DIR,
        dataset_path=dataset_path,
        news_path=news_path,
        theme_path=theme_path,
    )

    phase2_csv_path = None
    phase2_review_prompt_path = None
    phase2_summary_path = None
    phase2_web_path = None
    if args.include_phase2_research and not candidates_df.empty:
        print_progress(f"running phase2 Naver web research: top_n={args.phase2_top_n}")
        dart_df = None
        if args.phase2_include_dart:
            print_progress("collecting OpenDART revenue for phase2 candidates")
            from src.dart_client import DartClient

            dart_df = DartClient.from_env(
                request_sleep_seconds=args.phase2_dart_request_sleep_seconds,
            ).fetch_metrics_for_stock_codes(
                candidates_df["code"].tolist()
                if args.phase2_top_n <= 0
                else candidates_df.head(args.phase2_top_n)["code"].tolist(),
                bsns_year=args.phase2_dart_year,
                reprt_code=args.phase2_dart_report_code,
                fs_div=args.phase2_dart_fs_div,
                progress=print_progress,
            )
        phase2_df, phase2_web_df = collect_future_value_phase2_research(
            candidates_df,
            NaverNewsClient.from_env(
                request_sleep_seconds=args.phase2_web_request_sleep_seconds,
                resolve_page_metadata=False,
                request_timeout_seconds=args.news_request_timeout_seconds,
            ),
            raw_news_df,
            top_n=args.phase2_top_n,
            web_max_items=args.phase2_web_max_items,
            dart_df=dart_df,
            progress=print_progress,
        )
        phase2_csv_path = save_future_value_phase2_csv(phase2_df, market_date, RESULT_DIR)
        phase2_summary_path = save_future_value_phase2_summary(phase2_df, market_date, RESULT_DIR)
        phase2_web_path = save_future_value_phase2_web_markdown(
            phase2_web_df,
            phase2_df,
            market_date,
            RESULT_DIR,
        )
        phase2_review_prompt_path = save_future_value_phase2_review_prompt(
            phase2_df,
            market_date,
            RESULT_DIR,
            phase2_csv_path=phase2_csv_path,
            phase2_summary_path=phase2_summary_path,
            phase2_web_path=phase2_web_path,
            dataset_path=dataset_path,
            news_path=news_path,
        )

    print(f"market date: {market_date}")
    print(f"low-price KOSDAQ pool count: {len(hard_pool_df)}")
    print(f"future-value evidence candidates: {len(candidates_df)}")
    print(f"future value all evaluated CSV: {all_evaluated_path}")
    print(f"future value candidates CSV: {candidates_path}")
    print(f"future value theme MD: {theme_path}")
    print(f"future value raw news MD: {news_path}")
    print(f"future value dataset JSON: {dataset_path}")
    print(f"future value research prompt: {prompt_path}")
    if phase2_csv_path is not None:
        print(f"future value phase2 research CSV: {phase2_csv_path}")
        print(f"future value phase2 summary MD: {phase2_summary_path}")
        print(f"future value phase2 raw web MD: {phase2_web_path}")
        print(f"future value phase2 AI review prompt: {phase2_review_prompt_path}")


def clear_result_dir(result_dir: Path, markers: tuple[str, ...] | None = None) -> None:
    if result_dir.name != "results" or result_dir.parent.name != "data":
        raise RuntimeError(f"refusing to clear unexpected result directory: {result_dir}")

    result_dir.mkdir(parents=True, exist_ok=True)
    for entry in result_dir.iterdir():
        if markers and not any(marker in entry.name for marker in markers):
            continue
        if entry.is_symlink() or entry.is_file():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry)


def build_deadline(seconds: float) -> float | None:
    if seconds <= 0:
        return None
    return time.monotonic() + seconds


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build low-priced KOSDAQ future-theme research candidates."
    )
    parser.add_argument(
        "--date",
        help="Market reference date. Accepts YYYY-MM-DD or YYYYMMDD. Defaults to the latest trading date.",
    )
    parser.add_argument(
        "--max-price",
        type=int,
        default=FUTURE_VALUE_MAX_PRICE,
        help=f"Maximum stock price in KRW. Default is {FUTURE_VALUE_MAX_PRICE}.",
    )
    parser.add_argument(
        "--candidate-limit",
        type=int,
        default=FUTURE_VALUE_CANDIDATE_LIMIT,
        help="Limit final candidates after evidence filtering. Use 0 for no limit.",
    )
    parser.add_argument(
        "--news-lookback-days",
        type=int,
        default=FUTURE_VALUE_NEWS_LOOKBACK_DAYS,
        help=f"Recent calendar-day window for raw news. Default is {FUTURE_VALUE_NEWS_LOOKBACK_DAYS}.",
    )
    parser.add_argument(
        "--news-max-items",
        type=int,
        default=FUTURE_VALUE_NEWS_MAX_ITEMS_DEFAULT,
        help=f"Naver latest-news search count per stock. Default is {FUTURE_VALUE_NEWS_MAX_ITEMS_DEFAULT}.",
    )
    parser.add_argument(
        "--news-time-budget-seconds",
        type=float,
        default=FUTURE_VALUE_NEWS_TIME_BUDGET_SECONDS,
        help=f"Overall news collection time budget in seconds. Use 0 to disable. Default is {FUTURE_VALUE_NEWS_TIME_BUDGET_SECONDS:g}.",
    )
    parser.add_argument(
        "--news-request-sleep-seconds",
        type=float,
        default=0.05,
        help="Sleep between Naver news API requests. Default is 0.05.",
    )
    parser.add_argument(
        "--news-request-timeout-seconds",
        type=float,
        default=8.0,
        help="Per-request Naver news API timeout in seconds. Default is 8.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="Skip sector enrichment.",
    )
    parser.add_argument(
        "--skip-news",
        action="store_true",
        help="Skip Naver news collection and use only name/sector/industry theme evidence.",
    )
    parser.add_argument(
        "--enrich-news-metadata",
        action="store_true",
        help="Try to enrich truncated news metadata. Slower; default is off.",
    )
    parser.add_argument(
        "--include-phase2-research",
        action="store_true",
        help="Run second-stage Naver web research for employee count, revenue hints, and important news summary.",
    )
    parser.add_argument(
        "--phase2-top-n",
        type=int,
        default=FUTURE_VALUE_PHASE2_TOP_N,
        help=f"Candidate count for second-stage research. Use 0 for all candidates. Default is {FUTURE_VALUE_PHASE2_TOP_N}.",
    )
    parser.add_argument(
        "--phase2-web-max-items",
        type=int,
        default=FUTURE_VALUE_PHASE2_WEB_MAX_ITEMS_DEFAULT,
        help=f"Naver web result count per phase2 query. Default is {FUTURE_VALUE_PHASE2_WEB_MAX_ITEMS_DEFAULT}.",
    )
    parser.add_argument(
        "--phase2-web-request-sleep-seconds",
        type=float,
        default=0.05,
        help="Sleep between Naver web API requests in phase2. Default is 0.05.",
    )
    parser.add_argument(
        "--phase2-include-dart",
        action="store_true",
        help="Use OpenDART revenue as phase2 revenue source when DART_API_KEY is available.",
    )
    parser.add_argument(
        "--phase2-dart-year",
        default=str(datetime.now().year - 1),
        help="OpenDART business year for phase2 revenue. Default is previous calendar year.",
    )
    parser.add_argument(
        "--phase2-dart-report-code",
        default="11011",
        help="OpenDART report code for phase2 revenue. 11011 is annual report.",
    )
    parser.add_argument(
        "--phase2-dart-fs-div",
        default="CFS",
        help="OpenDART financial statement division for phase2 revenue. Default is CFS.",
    )
    parser.add_argument(
        "--phase2-dart-request-sleep-seconds",
        type=float,
        default=0.2,
        help="Sleep between OpenDART requests in phase2. Default is 0.2.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.max_price <= 0:
        raise ValueError("max-price must be at least 1")
    if args.candidate_limit < 0:
        raise ValueError("candidate-limit must be 0 or greater")
    if args.news_lookback_days <= 0:
        raise ValueError("news-lookback-days must be at least 1")
    if args.news_max_items <= 0:
        raise ValueError("news-max-items must be at least 1")
    if args.news_max_items > 100:
        raise ValueError("news-max-items must be 100 or less")
    if args.news_time_budget_seconds < 0:
        raise ValueError("news-time-budget-seconds must be 0 or greater")
    if args.news_request_sleep_seconds < 0:
        raise ValueError("news-request-sleep-seconds must be 0 or greater")
    if args.news_request_timeout_seconds <= 0:
        raise ValueError("news-request-timeout-seconds must be greater than 0")
    if args.phase2_top_n < 0:
        raise ValueError("phase2-top-n must be 0 or greater")
    if args.phase2_web_max_items <= 0:
        raise ValueError("phase2-web-max-items must be at least 1")
    if args.phase2_web_max_items > 100:
        raise ValueError("phase2-web-max-items must be 100 or less")
    if args.phase2_web_request_sleep_seconds < 0:
        raise ValueError("phase2-web-request-sleep-seconds must be 0 or greater")
    if args.phase2_dart_request_sleep_seconds < 0:
        raise ValueError("phase2-dart-request-sleep-seconds must be 0 or greater")


def parse_bool_like(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        return False
    return str(value).strip().casefold() in {"1", "true", "yes", "y"}


def print_progress(message: str) -> None:
    print(message, flush=True)


if __name__ == "__main__":
    main()
