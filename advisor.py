from __future__ import annotations

import argparse
import sys

from config import APP_VERSION, RESULT_DIR, TOP_N
from src.collector import collect_all_stock_data
from src.codex_prompt import save_codex_review_prompt
from src.profiles import get_profiles
from src.recommender import build_recommendations, save_advisor_results, scan_profiles


def main() -> None:
    args = parse_args()

    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"오류: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    profiles = get_profiles(args.profile)
    print_progress(f"find-good-stock v{APP_VERSION}")
    print_progress("여러 프로필 기반 추천 스캔을 시작해.")
    print_progress("사용 프로필: " + ", ".join(profile.name for profile in profiles))

    collected_df, run_date = collect_all_stock_data(args.date, progress=print_progress)

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

    candidates_path, recommendations_path = save_advisor_results(
        merged_candidates_df,
        recommendations_df,
        run_date,
        RESULT_DIR,
    )
    prompt_path = save_codex_review_prompt(recommendations_df, run_date, RESULT_DIR)

    print(f"기준일: {run_date}")
    print(f"후보 결과: {candidates_path}")
    print(f"추천 결과: {recommendations_path}")
    print(f"Codex 리뷰 프롬프트: {prompt_path}")


def print_progress(message: str) -> None:
    print(message, flush=True)


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
    return parser.parse_args()


if __name__ == "__main__":
    main()
