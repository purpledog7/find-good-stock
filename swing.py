from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from config import (
    APP_VERSION,
    KST_TIMEZONE,
    RESULT_DIR,
    SWING_BACKTEST_HISTORY_TRADING_DAYS,
    SWING_HISTORY_TRADING_DAYS,
    SWING_NEWS_MAX_ITEMS_DEFAULT,
    SWING_TOP_N,
)
from src.collector import to_output_date
from src.news_analyzer import collect_raw_news_info
from src.news_client import NaverNewsClient, parse_datetime
from src.sector_enricher import add_sector_info
from src.swing_backtester import run_swing_backtest, save_swing_backtest
from src.swing_collector import collect_swing_source_data
from src.swing_exporter import (
    save_swing_candidates,
    save_swing_news_markdown,
    save_swing_review_prompt,
)
from src.swing_scanner import build_swing_candidates
from src.swing_risk import add_market_risk_info, apply_news_risk_info
from src.trading_calendar import add_trading_days, next_trading_day


def main() -> None:
    args = parse_args()

    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"오류: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    validate_args(args)
    print_progress(f"find-good-stock v{APP_VERSION}")
    print_progress("스윙 후보 데이터 준비를 시작해.")

    history_days = (
        max(args.history_days, SWING_BACKTEST_HISTORY_TRADING_DAYS)
        if args.include_backtest
        else args.history_days
    )
    snapshot_df, history_df, market_date, _ = collect_swing_source_data(
        args.date,
        history_days=history_days,
        progress=print_progress,
    )
    signal_date = resolve_signal_date(args.signal_date, market_date)
    validate_signal_date(signal_date, market_date)
    review_date = add_trading_days(signal_date, 3)
    print_progress(f"진입 예정일: {signal_date}")
    print_progress(f"시세 기준 거래일: {market_date}")
    print_progress(f"3거래일 후 재검토일: {review_date}")

    if not args.skip_sector:
        print_progress("업종 정보 보강 중...")
        snapshot_df = add_sector_info(snapshot_df, progress=print_progress)
    snapshot_df = add_market_risk_info(snapshot_df, progress=print_progress)

    print_progress("4개 스윙 엔진으로 후보 계산 중...")
    candidates_df = build_swing_candidates(
        snapshot_df=snapshot_df,
        history_df=history_df,
        signal_date=signal_date,
        market_date=market_date,
        top_n=args.top_n,
        review_date=review_date,
    )
    print_progress(f"스윙 후보 수: {len(candidates_df):,}")

    raw_news_df = None
    news_path = None
    if args.include_news and not candidates_df.empty:
        start_dt, end_dt = build_swing_news_window(args, signal_date)
        print_progress(f"스윙 뉴스 수집 중: {start_dt.isoformat()} ~ {end_dt.isoformat()}")
        raw_news_df = collect_raw_news_info(
            candidates_df,
            NaverNewsClient.from_env(),
            start_dt=start_dt,
            end_dt=end_dt,
            max_items=args.news_max_items,
            progress=print_progress,
            enhanced_queries=True,
        )
        candidates_df = apply_news_risk_info(candidates_df, raw_news_df)
        news_path = save_swing_news_markdown(
            raw_news_df,
            candidates_df,
            signal_date,
            RESULT_DIR,
            start_dt,
            end_dt,
        )

    backtest_path = None
    if args.include_backtest:
        print_progress("간이 스윙 백테스트 계산 중...")
        backtest_df = run_swing_backtest(
            snapshot_df=snapshot_df,
            history_df=history_df,
            top_n=min(args.top_n, 10),
            lookback_signals=args.backtest_signals,
        )
        backtest_path = save_swing_backtest(backtest_df, signal_date, RESULT_DIR)

    candidates_path = save_swing_candidates(candidates_df, signal_date, RESULT_DIR)
    prompt_path = save_swing_review_prompt(
        candidates_df,
        signal_date,
        RESULT_DIR,
        news_path=news_path,
        backtest_path=backtest_path,
    )

    print(f"진입 예정일: {signal_date}")
    print(f"시세 기준 거래일: {market_date}")
    print(f"스윙 후보 CSV: {candidates_path}")
    if news_path is not None:
        print(f"스윙 뉴스 MD: {news_path}")
    if backtest_path is not None:
        print(f"스윙 백테스트 CSV: {backtest_path}")
    print(f"스윙 리뷰 프롬프트: {prompt_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="KOSPI/KOSDAQ 전체에서 3~4일 스윙 후보 데이터를 준비해."
    )
    parser.add_argument(
        "--date",
        help="시세 기준 거래일. YYYY-MM-DD 또는 YYYYMMDD 형식. 없으면 최근 거래일을 사용해.",
    )
    parser.add_argument(
        "--signal-date",
        help="진입 예정일. YYYY-MM-DD 또는 YYYYMMDD 형식. 없으면 시세 기준 거래일 다음 거래일을 사용해.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=SWING_TOP_N,
        help=f"스윙 후보 개수. 기본값은 {SWING_TOP_N}개야.",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=SWING_HISTORY_TRADING_DAYS,
        help=f"스윙 계산에 사용할 최근 거래일 개수. 기본값은 {SWING_HISTORY_TRADING_DAYS}개야.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="FinanceDataReader 기반 업종 정보 보강을 건너뛰어.",
    )
    parser.add_argument(
        "--include-news",
        action="store_true",
        help="후보별 최근 2일 뉴스 원문을 MD로 저장해.",
    )
    parser.add_argument(
        "--news-from",
        help="뉴스 시작 시각. ISO 형식. 없으면 진입 예정일 2일 전 00:00 KST를 사용해.",
    )
    parser.add_argument(
        "--news-to",
        help="뉴스 종료 시각. ISO 형식. 없으면 진입 예정일 당일 07:30 KST를 사용해.",
    )
    parser.add_argument(
        "--news-max-items",
        type=int,
        default=SWING_NEWS_MAX_ITEMS_DEFAULT,
        help=f"종목별 네이버 뉴스 검색 개수. 1~100개, 기본값은 {SWING_NEWS_MAX_ITEMS_DEFAULT}개야.",
    )
    parser.add_argument(
        "--include-backtest",
        action="store_true",
        help="최근 과거 신호의 3거래일 성과를 간이 백테스트 CSV로 저장해.",
    )
    parser.add_argument(
        "--backtest-signals",
        type=int,
        default=20,
        help="간이 백테스트에 사용할 과거 신호일 개수. 기본값은 20개야.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.top_n <= 0:
        raise ValueError("스윙 후보 개수는 1개 이상이어야 해.")
    if args.history_days < 21:
        raise ValueError("스윙 계산에는 최소 21거래일 데이터가 필요해.")
    if args.news_max_items <= 0:
        raise ValueError("뉴스 검색 개수는 1개 이상이어야 해.")
    if args.news_max_items > 100:
        raise ValueError("뉴스 검색 개수는 종목당 100개 이하로 입력해야 해.")
    if args.backtest_signals <= 0:
        raise ValueError("백테스트 신호일 개수는 1개 이상이어야 해.")


def resolve_signal_date(value: str | None, market_date: str) -> str:
    if value:
        return next_trading_day(to_output_date(value), include_current=True)
    return next_trading_day(market_date, include_current=False)


def validate_signal_date(signal_date: str, market_date: str) -> None:
    if signal_date <= market_date:
        raise ValueError("진입 예정일은 시세 기준 거래일 다음 거래일 이후여야 해.")


def build_swing_news_window(
    args: argparse.Namespace,
    signal_date: str,
) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(KST_TIMEZONE)
    target_date = datetime.strptime(signal_date, "%Y-%m-%d").date()
    start_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        tzinfo=timezone,
    ) - timedelta(days=2)
    end_dt = datetime(
        target_date.year,
        target_date.month,
        target_date.day,
        7,
        30,
        tzinfo=timezone,
    )
    if args.news_from:
        start_dt = parse_datetime(args.news_from)
    if args.news_to:
        end_dt = parse_datetime(args.news_to)
    if start_dt > end_dt:
        raise ValueError("뉴스 시작 시각은 종료 시각보다 빠르거나 같아야 해.")
    return start_dt, end_dt


def print_progress(message: str) -> None:
    print(message, flush=True)


if __name__ == "__main__":
    main()
