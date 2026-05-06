from __future__ import annotations

import argparse
import sys
from datetime import datetime

from config import DART_OUTPUT_COLUMNS, TOP_N
from src.ai_analyzer import add_summary
from src.collector import collect_all_stock_data
from src.criteria import (
    DEFAULT_FILTER_CRITERIA,
    STRICT_FILTER_CRITERIA,
    FilterCriteria,
    update_criteria,
)
from src.exporter import save_results
from src.filters import apply_value_filters
from src.scorer import score_stocks
from src.validator import (
    print_validation_report,
    raise_if_invalid,
    validate_results,
    validate_saved_csv,
)


def main() -> None:
    args = parse_args()

    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"오류: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    criteria = build_filter_criteria(args)
    print_progress("저평가 주식 스캔을 시작해.")
    collected_df, run_date = collect_all_stock_data(args.date, progress=print_progress)

    print_progress("필터 적용 중...")
    print_filter_criteria(criteria)
    filtered_df = apply_value_filters(collected_df, criteria)
    print_progress(f"필터 통과: {len(filtered_df):,}개 종목")

    print_progress("점수 계산 및 정렬 중...")
    scored_df = score_stocks(filtered_df).sort_values(
        by="score",
        ascending=False,
        na_position="last",
    ).reset_index(drop=True)
    scored_df["rank"] = range(1, len(scored_df) + 1)

    all_df = scored_df.copy()
    top_df = scored_df.head(args.top_n).copy()

    if args.include_dart:
        print_progress(f"상위 {len(top_df)}개 OpenDART 재무제표 보강 중...")
        from src.dart_client import DartClient

        dart_client = DartClient.from_env()
        dart_df = dart_client.fetch_metrics_for_stock_codes(
            top_df["code"].tolist(),
            bsns_year=args.dart_year,
            reprt_code=args.dart_report_code,
            fs_div=args.dart_fs_div,
            progress=print_progress,
        )
        top_df = top_df.merge(dart_df, on="code", how="left")
        all_df = ensure_columns(all_df, DART_OUTPUT_COLUMNS)
        top_df = ensure_columns(top_df, DART_OUTPUT_COLUMNS)

    if args.include_summary:
        print_progress(f"상위 {args.top_n}개 규칙 기반 요약 생성 중...")
        top_df = add_summary(top_df)

    print_progress("저장 전 데이터 검증 중...")
    pre_save_report = validate_results(
        all_df,
        top_df,
        run_date,
        args.top_n,
        include_summary=args.include_summary,
        include_dart=args.include_dart,
        criteria=criteria,
    )
    print_validation_report(pre_save_report, print_progress)
    raise_if_invalid(pre_save_report)

    print_progress("CSV 저장 중...")
    all_path, top_path = save_results(
        all_df,
        top_df,
        run_date,
        include_summary=args.include_summary,
        include_dart=args.include_dart,
    )

    print_progress("저장된 CSV 검증 중...")
    saved_report = validate_saved_csv(
        all_path,
        top_path,
        run_date,
        args.top_n,
        include_summary=args.include_summary,
        include_dart=args.include_dart,
        criteria=criteria,
    )
    print_validation_report(saved_report, print_progress)
    raise_if_invalid(saved_report)

    print(f"기준일: {run_date}")
    print(f"수집 종목 수: {len(collected_df)}")
    print(f"필터 통과 종목 수: {len(scored_df)}")
    print(f"전체 결과: {all_path}")
    print(f"Top {args.top_n} 결과: {top_path}")


def print_progress(message: str) -> None:
    print(message, flush=True)


def print_filter_criteria(criteria: FilterCriteria) -> None:
    print_progress(
        "필터 기준: "
        f"시총 {criteria.min_market_cap / 100_000_000:.0f}억 이상, "
        f"60거래일 평균 거래대금 {criteria.min_avg_trading_value / 100_000_000:.0f}억 이상, "
        f"PER 0 초과 {criteria.max_per:g} 이하, "
        f"PBR 0 초과 {criteria.max_pbr:g} 이하, "
        f"추정 ROE {criteria.min_estimated_roe:g}% 이상"
    )


def build_filter_criteria(args: argparse.Namespace) -> FilterCriteria:
    criteria = STRICT_FILTER_CRITERIA if args.strict else DEFAULT_FILTER_CRITERIA
    criteria = update_criteria(
        criteria,
        min_market_cap=to_won(args.min_market_cap_eok),
        min_avg_trading_value=to_won(args.min_avg_trading_value_eok),
        max_per=args.max_per,
        max_pbr=args.max_pbr,
        min_estimated_roe=args.min_estimated_roe,
    )

    if criteria.min_market_cap <= 0:
        raise ValueError("최소 시가총액은 0보다 커야 해.")
    if criteria.min_avg_trading_value <= 0:
        raise ValueError("최소 평균 거래대금은 0보다 커야 해.")
    if criteria.max_per <= 0 or criteria.max_pbr <= 0:
        raise ValueError("PER/PBR 상한은 0보다 커야 해.")
    if criteria.min_estimated_roe < 0:
        raise ValueError("최소 추정 ROE는 0 이상이어야 해.")

    return criteria


def to_won(eok_value: float | None) -> int | None:
    if eok_value is None:
        return None
    return int(eok_value * 100_000_000)


def ensure_columns(df, columns):
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            result[column] = ""
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="KOSPI/KOSDAQ 저평가 후보 종목을 점수화해서 CSV로 저장해."
    )
    parser.add_argument(
        "--date",
        help="기준일. YYYY-MM-DD 또는 YYYYMMDD 형식. 없으면 최근 거래일을 사용해.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=TOP_N,
        help=f"상위 결과 개수. 기본값은 {TOP_N}개야.",
    )
    parser.add_argument(
        "--include-summary",
        action="store_true",
        help="상위 결과에 규칙 기반 summary 컬럼을 추가해.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="더 보수적인 필터 기준을 사용해. 시총 500억, 거래대금 10억, PER 10, PBR 1.0, ROE 10 기준이야.",
    )
    parser.add_argument("--min-market-cap-eok", type=float, help="최소 시가총액. 억 원 단위야.")
    parser.add_argument(
        "--min-avg-trading-value-eok",
        type=float,
        help="최소 60거래일 평균 거래대금. 억 원 단위야.",
    )
    parser.add_argument("--max-per", type=float, help="PER 상한이야.")
    parser.add_argument("--max-pbr", type=float, help="PBR 상한이야.")
    parser.add_argument("--min-estimated-roe", type=float, help="최소 추정 ROE야.")
    parser.add_argument(
        "--include-dart",
        action="store_true",
        help="상위 결과에 OpenDART 연간 재무제표 항목을 보강해.",
    )
    parser.add_argument(
        "--dart-year",
        default=str(datetime.now().year - 1),
        help="OpenDART 사업연도. 기본값은 전년도야.",
    )
    parser.add_argument(
        "--dart-report-code",
        default="11011",
        help="OpenDART 보고서 코드. 11011은 사업보고서야.",
    )
    parser.add_argument(
        "--dart-fs-div",
        default="CFS",
        choices=("CFS", "OFS"),
        help="OpenDART 재무제표 구분. CFS는 연결, OFS는 별도야.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
