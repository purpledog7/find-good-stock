from __future__ import annotations

import argparse
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

from config import (
    APP_VERSION,
    NEWS_RAW_COLUMNS,
    RESULT_DIR,
    SPECIAL_SWING_CANDIDATE_POOL_N,
    SPECIAL_SWING_FINAL_N,
    SPECIAL_SWING_HISTORY_TRADING_DAYS,
    SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT,
    SPECIAL_SWING_NEWS_LOOKBACK_DAYS,
    SPECIAL_SWING_SHORTLIST_N,
)
from src.collector import to_output_date
from src.news_analyzer import collect_raw_news_info
from src.news_client import NaverNewsClient
from src.sector_enricher import add_sector_info
from src.special_swing import (
    build_special_ai_news_window,
    build_fast_special_stock_news_queries,
    build_special_swing_technical_universe,
    score_special_news_candidates,
    select_special_swing_technical_candidates,
)
from src.special_swing_exporter import (
    save_special_swing_all_evaluated,
    save_special_swing_candidates,
    save_special_swing_news_dataset,
    save_special_swing_news_markdown,
    save_special_swing_phase2_prompt,
    save_special_swing_phase3_prompt,
)
from src.swing_collector import collect_swing_source_data
from src.swing_risk import add_market_risk_info
from src.trading_calendar import add_trading_days, next_trading_day


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
    print_progress("special swing scan started")

    snapshot_df, history_df, market_date, _ = collect_swing_source_data(
        args.date,
        history_days=args.history_days,
        progress=print_progress,
    )
    signal_date = resolve_signal_date(args.signal_date, market_date)
    validate_signal_date(signal_date, market_date)
    review_date_3d = add_trading_days(signal_date, 3)
    review_date_5d = add_trading_days(signal_date, 5)

    print_progress(f"signal date: {signal_date}")
    print_progress(f"market date: {market_date}")
    print_progress(f"3 trading-day review: {review_date_3d}")
    print_progress(f"5 trading-day review: {review_date_5d}")

    if not args.skip_sector:
        print_progress("enriching sector info")
        snapshot_df = add_sector_info(snapshot_df, progress=print_progress)
    snapshot_df = add_market_risk_info(snapshot_df, progress=print_progress)

    print_progress("evaluating all stocks for special swing setup")
    evaluated_df = build_special_swing_technical_universe(
        snapshot_df=snapshot_df,
        history_df=history_df,
        market_date=market_date,
        signal_date=signal_date,
        review_date=review_date_3d,
        review_date_5d=review_date_5d,
    )
    print_progress(f"all evaluated stock count: {len(evaluated_df):,}")

    print_progress("building technical candidate pool")
    pool_df = select_special_swing_technical_candidates(
        evaluated_df,
        top_n=args.candidate_pool_n,
    )
    print_progress(f"technical pool count: {len(pool_df):,}")

    news_start_dt, news_end_dt = build_special_ai_news_window(
        signal_date,
        lookback_days=args.news_lookback_days,
    )
    news_deadline = build_deadline(args.news_time_budget_seconds)
    if pool_df.empty:
        raw_news_df = pd.DataFrame(columns=NEWS_RAW_COLUMNS)
        scored_candidates_df = pool_df
    else:
        print_progress(f"collecting {args.news_lookback_days}-day raw news for technical pool")
        raw_news_df = collect_raw_news_info(
            pool_df,
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
            query_builder=build_fast_special_stock_news_queries,
            enrich_metadata=args.enrich_news_metadata,
            deadline=news_deadline,
        )
        scored_candidates_df = score_special_news_candidates(
            pool_df,
            raw_news_df,
            analysis_start_dt=news_start_dt,
            analysis_end_dt=news_end_dt,
        )

    print_progress(f"clearing result directory: {RESULT_DIR}")
    clear_result_dir(RESULT_DIR)

    all_evaluated_path = save_special_swing_all_evaluated(
        evaluated_df,
        signal_date,
        RESULT_DIR,
    )
    candidates_path = save_special_swing_candidates(
        scored_candidates_df,
        signal_date,
        RESULT_DIR,
        candidate_count=args.candidate_pool_n,
    )
    news_path = save_special_swing_news_markdown(
        raw_news_df,
        scored_candidates_df,
        signal_date,
        RESULT_DIR,
        news_start_dt,
        news_end_dt,
        candidate_count=args.candidate_pool_n,
    )
    dataset_path = save_special_swing_news_dataset(
        scored_candidates_df,
        raw_news_df,
        signal_date,
        RESULT_DIR,
        news_start_dt,
        news_end_dt,
        candidate_count=args.candidate_pool_n,
        shortlist_n=args.shortlist_n,
        final_n=args.final_n,
    )
    phase2_prompt_path = save_special_swing_phase2_prompt(
        scored_candidates_df,
        signal_date,
        RESULT_DIR,
        dataset_path=dataset_path,
        news_path=news_path,
        shortlist_n=args.shortlist_n,
        candidate_count=args.candidate_pool_n,
    )
    phase3_prompt_path = save_special_swing_phase3_prompt(
        scored_candidates_df.head(args.shortlist_n),
        signal_date,
        RESULT_DIR,
        phase2_top10_path=RESULT_DIR / f"{signal_date}_special_swing_phase2_top{args.shortlist_n}.json",
        news_path=news_path,
        shortlist_n=args.shortlist_n,
        final_n=args.final_n,
    )

    print(f"signal date: {signal_date}")
    print(f"market date: {market_date}")
    print(f"special swing all evaluated CSV: {all_evaluated_path}")
    print(f"special swing Top{args.candidate_pool_n} CSV: {candidates_path}")
    print(f"special swing Top{args.candidate_pool_n} raw news MD: {news_path}")
    print(f"special swing Top{args.candidate_pool_n} news dataset JSON: {dataset_path}")
    print(f"special swing AI phase2 prompt: {phase2_prompt_path}")
    print(f"special swing AI phase3 prompt: {phase3_prompt_path}")


def clear_result_dir(result_dir: Path) -> None:
    if result_dir.name != "results" or result_dir.parent.name != "data":
        raise RuntimeError(f"refusing to clear unexpected result directory: {result_dir}")

    result_dir.mkdir(parents=True, exist_ok=True)
    for entry in result_dir.iterdir():
        if entry.is_symlink() or entry.is_file():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build special swing Top100 candidates and Codex AI analysis prompts."
    )
    parser.add_argument(
        "--date",
        help="Market reference date. Accepts YYYY-MM-DD or YYYYMMDD. Defaults to the latest trading date.",
    )
    parser.add_argument(
        "--signal-date",
        help="Planned entry date. Accepts YYYY-MM-DD or YYYYMMDD. Defaults to the next trading day after market date.",
    )
    parser.add_argument(
        "--shortlist-n",
        type=int,
        default=SPECIAL_SWING_SHORTLIST_N,
        help=f"AI shortlist count to request in prompts. Default is {SPECIAL_SWING_SHORTLIST_N}.",
    )
    parser.add_argument(
        "--final-n",
        type=int,
        default=SPECIAL_SWING_FINAL_N,
        help=f"Final debate pick count. Default is {SPECIAL_SWING_FINAL_N}.",
    )
    parser.add_argument(
        "--top-n",
        dest="shortlist_n",
        type=int,
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--candidate-pool-n",
        type=int,
        default=SPECIAL_SWING_CANDIDATE_POOL_N,
        help=f"Technical candidate pool before news analysis. Default is {SPECIAL_SWING_CANDIDATE_POOL_N}.",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=SPECIAL_SWING_HISTORY_TRADING_DAYS,
        help=f"Trading-day history window. Default is {SPECIAL_SWING_HISTORY_TRADING_DAYS}.",
    )
    parser.add_argument(
        "--news-max-items",
        type=int,
        default=SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT,
        help=f"Naver latest-news search count per stock. Default is {SPECIAL_SWING_NEWS_MAX_ITEMS_DEFAULT}.",
    )
    parser.add_argument(
        "--news-lookback-days",
        type=int,
        default=SPECIAL_SWING_NEWS_LOOKBACK_DAYS,
        help=f"Recent calendar-day window for raw news. Default is {SPECIAL_SWING_NEWS_LOOKBACK_DAYS}.",
    )
    parser.add_argument(
        "--news-time-budget-seconds",
        type=float,
        default=180.0,
        help="Overall news collection time budget in seconds. Use 0 to disable. Default is 180.",
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
        "--enrich-news-metadata",
        action="store_true",
        help="Try to enrich truncated news metadata. Slower; default is off.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.shortlist_n <= 0:
        raise ValueError("shortlist-n must be at least 1")
    if args.final_n <= 0:
        raise ValueError("final-n must be at least 1")
    if args.final_n > args.shortlist_n:
        raise ValueError("final-n must be less than or equal to shortlist-n")
    if args.candidate_pool_n < args.shortlist_n:
        raise ValueError("candidate-pool-n must be greater than or equal to shortlist-n")
    if args.history_days < 21:
        raise ValueError("history-days must be at least 21")
    if args.news_max_items <= 0:
        raise ValueError("news-max-items must be at least 1")
    if args.news_max_items > 100:
        raise ValueError("news-max-items must be 100 or less")
    if args.news_lookback_days <= 0:
        raise ValueError("news-lookback-days must be at least 1")
    if args.news_time_budget_seconds < 0:
        raise ValueError("news-time-budget-seconds must be 0 or greater")
    if args.news_request_sleep_seconds < 0:
        raise ValueError("news-request-sleep-seconds must be 0 or greater")
    if args.news_request_timeout_seconds <= 0:
        raise ValueError("news-request-timeout-seconds must be greater than 0")


def build_deadline(seconds: float) -> float | None:
    if seconds <= 0:
        return None
    return time.monotonic() + seconds


def resolve_signal_date(value: str | None, market_date: str) -> str:
    if value:
        return next_trading_day(to_output_date(value), include_current=True)
    return next_trading_day(market_date, include_current=False)


def validate_signal_date(signal_date: str, market_date: str) -> None:
    if signal_date <= market_date:
        raise ValueError("signal-date must be after the market reference date")


def print_progress(message: str) -> None:
    print(message, flush=True)


if __name__ == "__main__":
    main()
