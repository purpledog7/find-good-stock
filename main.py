from __future__ import annotations

import argparse
import sys

from config import TOP_N
from src.ai_analyzer import add_ai_summary
from src.collector import collect_all_stock_data
from src.exporter import save_results
from src.filters import apply_value_filters
from src.scorer import score_stocks


def main() -> None:
    args = parse_args()

    try:
        run(args)
    except (RuntimeError, ValueError) as error:
        print(f"오류: {error}", file=sys.stderr)
        raise SystemExit(1) from None


def run(args: argparse.Namespace) -> None:
    collected_df, run_date = collect_all_stock_data(args.date)
    filtered_df = apply_value_filters(collected_df)
    scored_df = score_stocks(filtered_df).sort_values(
        by="score",
        ascending=False,
        na_position="last",
    )

    all_df = scored_df.copy()
    all_df["ai_summary"] = ""

    top_df = scored_df.head(args.top_n).copy()
    if args.skip_summary:
        top_df["ai_summary"] = ""
    else:
        top_df = add_ai_summary(top_df)

    all_path, top_path = save_results(all_df, top_df, run_date)

    print(f"기준일: {run_date}")
    print(f"수집 종목 수: {len(collected_df)}")
    print(f"필터 통과 종목 수: {len(scored_df)}")
    print(f"전체 결과: {all_path}")
    print(f"Top {args.top_n} 결과: {top_path}")


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
        "--skip-summary",
        action="store_true",
        help="규칙 기반 ai_summary 생성을 건너뛰어.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    main()
