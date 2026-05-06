from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config import (
    AVG_TRADING_VALUE_COLUMN,
    AVG_TRADING_VALUE_EOK_COLUMN,
    CSV_ENCODING,
    DART_OUTPUT_COLUMNS,
    LOOKBACK_TRADING_DAYS,
    MARKETS,
    OUTPUT_COLUMNS,
)
from src.criteria import DEFAULT_FILTER_CRITERIA, FilterCriteria


@dataclass(frozen=True)
class ValidationReport:
    errors: list[str]
    warnings: list[str]

    @property
    def passed(self) -> bool:
        return not self.errors


def validate_results(
    all_df: pd.DataFrame,
    top_df: pd.DataFrame,
    expected_date: str,
    top_n: int,
    include_summary: bool = False,
    include_dart: bool = False,
    criteria: FilterCriteria = DEFAULT_FILTER_CRITERIA,
) -> ValidationReport:
    errors: list[str] = []
    warnings: list[str] = []

    validate_frame(
        "전체 결과",
        all_df,
        expected_date,
        include_summary,
        include_dart,
        criteria,
        errors,
        warnings,
    )
    validate_frame(
        "Top 결과",
        top_df,
        expected_date,
        include_summary,
        include_dart,
        criteria,
        errors,
        warnings,
    )
    validate_top_frame(top_df, top_n, errors)

    return ValidationReport(errors=errors, warnings=warnings)


def validate_saved_csv(
    all_path: Path,
    top_path: Path,
    expected_date: str,
    top_n: int,
    include_summary: bool = False,
    include_dart: bool = False,
    criteria: FilterCriteria = DEFAULT_FILTER_CRITERIA,
) -> ValidationReport:
    all_df = pd.read_csv(all_path, dtype={"code": str}, encoding=CSV_ENCODING)
    top_df = pd.read_csv(top_path, dtype={"code": str}, encoding=CSV_ENCODING)
    return validate_results(
        all_df,
        top_df,
        expected_date,
        top_n,
        include_summary,
        include_dart,
        criteria,
    )


def raise_if_invalid(report: ValidationReport) -> None:
    if report.passed:
        return
    raise RuntimeError("데이터 검증 실패:\n" + "\n".join(f"- {error}" for error in report.errors))


def print_validation_report(report: ValidationReport, progress) -> None:
    if report.passed:
        progress("검증 통과: 필수 컬럼, 필터 조건, 점수 범위, 정렬 상태 OK")
    else:
        progress("검증 실패:")
        for error in report.errors:
            progress(f"  오류: {error}")

    for warning in report.warnings:
        progress(f"  주의: {warning}")


def validate_frame(
    label: str,
    df: pd.DataFrame,
    expected_date: str,
    include_summary: bool,
    include_dart: bool,
    criteria: FilterCriteria,
    errors: list[str],
    warnings: list[str],
) -> None:
    expected_columns = list(OUTPUT_COLUMNS)
    if include_dart:
        expected_columns.extend(DART_OUTPUT_COLUMNS)
    if include_summary:
        expected_columns.append("summary")

    missing_columns = [column for column in expected_columns if column not in df.columns]
    if missing_columns:
        errors.append(f"{label}에 필수 컬럼이 없어: {', '.join(missing_columns)}")
        return

    if df.empty:
        warnings.append(f"{label}가 비어 있어. 필터가 너무 엄격하거나 원천 데이터가 부족할 수 있어.")
        return

    validate_codes(label, df, errors)
    validate_dates(label, df, expected_date, errors)
    validate_markets(label, df, errors)
    validate_numeric_rules(label, df, errors, warnings)
    validate_display_columns(label, df, errors)
    validate_filter_rules(label, df, criteria, errors)
    validate_ranks(label, df, errors)
    validate_scores(label, df, errors)
    if include_dart:
        validate_dart_columns(label, df, warnings)


def validate_top_frame(top_df: pd.DataFrame, top_n: int, errors: list[str]) -> None:
    if len(top_df) > top_n:
        errors.append(f"Top 결과가 요청 개수보다 많아: {len(top_df)} > {top_n}")

    if "score" in top_df.columns and not top_df.empty:
        scores = pd.to_numeric(top_df["score"], errors="coerce")
        if not scores.is_monotonic_decreasing:
            errors.append("Top 결과가 score 내림차순으로 정렬되어 있지 않아.")


def validate_codes(label: str, df: pd.DataFrame, errors: list[str]) -> None:
    codes = df["code"].astype(str)
    invalid_code_count = (~codes.str.fullmatch(r"\d{6}")).sum()
    if invalid_code_count:
        errors.append(f"{label}에 6자리 종목코드가 아닌 값이 {invalid_code_count}개 있어.")

    duplicate_count = int(codes.duplicated().sum())
    if duplicate_count:
        errors.append(f"{label}에 중복 종목코드가 {duplicate_count}개 있어.")


def validate_dates(
    label: str,
    df: pd.DataFrame,
    expected_date: str,
    errors: list[str],
) -> None:
    dates = set(df["date"].astype(str).dropna())
    if dates != {expected_date}:
        errors.append(f"{label} 날짜가 기준일과 달라: {sorted(dates)}")


def validate_markets(label: str, df: pd.DataFrame, errors: list[str]) -> None:
    invalid_markets = sorted(set(df["market"].dropna()) - set(MARKETS))
    if invalid_markets:
        errors.append(f"{label}에 알 수 없는 시장 값이 있어: {invalid_markets}")


def validate_numeric_rules(
    label: str,
    df: pd.DataFrame,
    errors: list[str],
    warnings: list[str],
) -> None:
    numeric_columns = [
        "rank",
        "price",
        "market_cap",
        "market_cap_eok",
        "per",
        "pbr",
        "eps",
        "bps",
        "estimated_roe",
        AVG_TRADING_VALUE_COLUMN,
        AVG_TRADING_VALUE_EOK_COLUMN,
        "score",
    ]
    numeric_df = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    for column in numeric_columns:
        missing_count = int(numeric_df[column].isna().sum())
        if missing_count:
            warnings.append(f"{label}의 {column} 결측/비숫자 값이 {missing_count}개 있어.")

    non_positive_checks = ["price", "market_cap", AVG_TRADING_VALUE_COLUMN]
    for column in non_positive_checks:
        bad_count = int((numeric_df[column] <= 0).sum())
        if bad_count:
            errors.append(f"{label}의 {column} 값이 0 이하인 행이 {bad_count}개 있어.")


def validate_filter_rules(
    label: str,
    df: pd.DataFrame,
    criteria: FilterCriteria,
    errors: list[str],
) -> None:
    market_cap = pd.to_numeric(df["market_cap"], errors="coerce")
    avg_trading_value = pd.to_numeric(df[AVG_TRADING_VALUE_COLUMN], errors="coerce")
    per = pd.to_numeric(df["per"], errors="coerce")
    pbr = pd.to_numeric(df["pbr"], errors="coerce")
    estimated_roe = pd.to_numeric(df["estimated_roe"], errors="coerce")

    rule_counts = {
        f"시가총액 {criteria.min_market_cap:,} 미만": int(
            (market_cap < criteria.min_market_cap).sum()
        ),
        f"{LOOKBACK_TRADING_DAYS}거래일 평균 거래대금 {criteria.min_avg_trading_value:,} 미만": int(
            (avg_trading_value < criteria.min_avg_trading_value).sum()
        ),
        f"PER 조건 위반(0 초과, {criteria.max_per} 이하)": int(
            ((per <= 0) | (per > criteria.max_per)).sum()
        ),
        f"PBR 조건 위반(0 초과, {criteria.max_pbr} 이하)": int(
            ((pbr <= 0) | (pbr > criteria.max_pbr)).sum()
        ),
        f"추정 ROE {criteria.min_estimated_roe}% 미만": int(
            (estimated_roe < criteria.min_estimated_roe).sum()
        ),
    }

    for rule, count in rule_counts.items():
        if count:
            errors.append(f"{label}에 {rule}인 행이 {count}개 있어.")

    if criteria.max_market_cap is not None:
        count = int((market_cap > criteria.max_market_cap).sum())
        if count:
            errors.append(
                f"{label}에 시가총액 {criteria.max_market_cap:,} 초과인 행이 {count}개 있어."
            )


def validate_display_columns(label: str, df: pd.DataFrame, errors: list[str]) -> None:
    market_cap = pd.to_numeric(df["market_cap"], errors="coerce")
    market_cap_eok = pd.to_numeric(df["market_cap_eok"], errors="coerce")
    expected_market_cap_eok = (market_cap / 100_000_000).round(2)
    bad_market_cap_count = int(
        (market_cap_eok.sub(expected_market_cap_eok).abs() > 0.01).sum()
    )
    if bad_market_cap_count:
        errors.append(f"{label}의 market_cap_eok 값이 market_cap과 맞지 않는 행이 {bad_market_cap_count}개 있어.")

    avg_trading_value = pd.to_numeric(df[AVG_TRADING_VALUE_COLUMN], errors="coerce")
    avg_trading_value_eok = pd.to_numeric(df[AVG_TRADING_VALUE_EOK_COLUMN], errors="coerce")
    expected_avg_trading_value_eok = (avg_trading_value / 100_000_000).round(2)
    bad_avg_trading_value_count = int(
        (avg_trading_value_eok.sub(expected_avg_trading_value_eok).abs() > 0.01).sum()
    )
    if bad_avg_trading_value_count:
        errors.append(
            f"{label}의 {AVG_TRADING_VALUE_EOK_COLUMN} 값이 {AVG_TRADING_VALUE_COLUMN}과 맞지 않는 행이 "
            f"{bad_avg_trading_value_count}개 있어."
        )


def validate_scores(label: str, df: pd.DataFrame, errors: list[str]) -> None:
    scores = pd.to_numeric(df["score"], errors="coerce")
    bad_count = int(((scores < 0) | (scores > 100)).sum())
    if bad_count:
        errors.append(f"{label}에 score가 0~100 범위를 벗어난 행이 {bad_count}개 있어.")


def validate_ranks(label: str, df: pd.DataFrame, errors: list[str]) -> None:
    ranks = pd.to_numeric(df["rank"], errors="coerce")
    if ranks.isna().any():
        errors.append(f"{label}에 rank가 비어 있거나 숫자가 아닌 행이 있어.")
        return

    if int(ranks.duplicated().sum()):
        errors.append(f"{label}에 중복 rank가 있어.")

    if not ranks.is_monotonic_increasing:
        errors.append(f"{label}의 rank가 오름차순으로 정렬되어 있지 않아.")


def validate_dart_columns(label: str, df: pd.DataFrame, warnings: list[str]) -> None:
    for column in DART_OUTPUT_COLUMNS:
        missing_count = int(df[column].isna().sum() + (df[column].astype(str) == "").sum())
        if missing_count:
            warnings.append(f"{label}의 {column} 값이 비어 있는 행이 {missing_count}개 있어.")
