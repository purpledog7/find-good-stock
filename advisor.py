from __future__ import annotations

import argparse
import sys

from config import APP_VERSION, NEWS_MAX_ITEMS_DEFAULT, RESULT_DIR, TOP_N
from src.collector import collect_all_stock_data
from src.codex_prompt import save_codex_review_prompt
from src.news_analyzer import collect_raw_news_info
from src.news_client import NaverNewsClient, default_news_window, parse_datetime
from src.profiles import get_profiles
from src.recommender import (
    build_recommendations,
    save_advisor_results,
    save_raw_news_markdown,
    scan_profiles,
)
from src.sector_enricher import add_sector_info


def main() -> None:
    args = parse_args()

    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"오류: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    validate_args(args)
    profiles = get_profiles(args.profile)
    print_progress(f"find-good-stock v{APP_VERSION}")
    print_progress("여러 프로필 기반 추천 스캔을 시작해.")
    print_progress("사용 프로필: " + ", ".join(profile.name for profile in profiles))

    collected_df, run_date = collect_all_stock_data(args.date, progress=print_progress)
    if not args.skip_sector:
        print_progress("업종 정보 보강 중...")
        collected_df = add_sector_info(collected_df, progress=print_progress)

    print_progress("프로필별 후보 추출 중...")
    candidates_df = scan_profiles(collected_df, profiles)
    print_progress(f"프로필별 후보 행 수: {len(candidates_df):,}")

    print_progress("후보 통합 및 추천 점수 계산 중...")
    merged_candidates_df, recommendations_df = build_recommendations(
        candidates_df,
        top_n=args.top_n,
    )
    print_progress(f"통합 후보 종목 수: {len(merged_candidates_df):,}")
    print_progress(f"최종 추천 종목 수: {len(recommendations_df):,}")
    raw_news_df = None

    if args.include_news and not recommendations_df.empty:
        start_dt, end_dt = build_news_window(args, run_date)
        print_progress(
            f"최근 뉴스 보강 중: {start_dt.isoformat()} ~ {end_dt.isoformat()}"
        )
        news_client = NaverNewsClient.from_env()
        raw_news_df = collect_raw_news_info(
            recommendations_df,
            news_client,
            start_dt=start_dt,
            end_dt=end_dt,
            max_items=args.news_max_items,
            progress=print_progress,
        )

    candidates_path, recommendations_path = save_advisor_results(
        merged_candidates_df,
        recommendations_df,
        run_date,
        RESULT_DIR,
        args.top_n,
    )
    raw_news_md_path = None
    if raw_news_df is not None:
        raw_news_md_path = save_raw_news_markdown(
            raw_news_df,
            recommendations_df,
            run_date,
            RESULT_DIR,
            start_dt,
            end_dt,
        )
    prompt_path = save_codex_review_prompt(
        recommendations_df,
        run_date,
        RESULT_DIR,
        raw_news_path=raw_news_md_path,
    )

    print(f"기준일: {run_date}")
    print(f"후보 결과: {candidates_path}")
    print(f"추천 결과: {recommendations_path}")
    if raw_news_md_path is not None:
        print(f"원본 뉴스 MD: {raw_news_md_path}")
    print(f"Codex 리뷰 프롬프트: {prompt_path}")


def print_progress(message: str) -> None:
    print(message, flush=True)


def validate_args(args: argparse.Namespace) -> None:
    if args.top_n <= 0:
        raise ValueError("최종 추천 개수는 1개 이상이어야 해.")
    if args.news_max_items <= 0 or args.news_max_items > 100:
        raise ValueError("뉴스 검색 개수는 종목당 1~100개 사이여야 해.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="여러 저평가 프로필을 실행하고 최종 추천 후보를 CSV로 저장해."
    )
    parser.add_argument(
        "--date",
        help="기준일. YYYY-MM-DD 또는 YYYYMMDD 형식. 없으면 최근 거래일을 사용해.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=TOP_N,
        help=f"최종 추천 개수. 기본값은 {TOP_N}개야.",
    )
    parser.add_argument(
        "--profile",
        action="append",
        help="사용할 프로필 이름. 여러 번 지정할 수 있어. 없으면 전체 프로필을 사용해.",
    )
    parser.add_argument(
        "--skip-sector",
        action="store_true",
        help="FinanceDataReader 기반 업종 정보 보강을 건너뛰어.",
    )
    parser.add_argument(
        "--include-news",
        action="store_true",
        help="최종 추천 종목의 전날 16:00부터 당일 07:00까지 네이버 뉴스를 원문 MD로 저장해.",
    )
    parser.add_argument(
        "--news-from",
        help="뉴스 시작 시각. ISO 형식. 없으면 기준일 전날 16:00 KST를 사용해.",
    )
    parser.add_argument(
        "--news-to",
        help="뉴스 종료 시각. ISO 형식. 없으면 기준일 당일 07:00 KST를 사용해.",
    )
    parser.add_argument(
        "--news-max-items",
        type=int,
        default=NEWS_MAX_ITEMS_DEFAULT,
        help=f"종목별 네이버 뉴스 검색 개수. 1~100개, 기본값은 {NEWS_MAX_ITEMS_DEFAULT}개야.",
    )
    return parser.parse_args()


def build_news_window(args: argparse.Namespace, run_date: str):
    start_dt, end_dt = default_news_window(run_date=run_date)
    if args.news_from:
        start_dt = parse_datetime(args.news_from)
    if args.news_to:
        end_dt = parse_datetime(args.news_to)
    if start_dt > end_dt:
        raise ValueError("뉴스 시작 시각은 종료 시각보다 빠르거나 같아야 해.")
    return start_dt, end_dt


if __name__ == "__main__":
    main()
