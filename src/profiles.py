from __future__ import annotations

from dataclasses import dataclass

from src.criteria import DEFAULT_FILTER_CRITERIA, STRICT_FILTER_CRITERIA, FilterCriteria


@dataclass(frozen=True)
class ScanProfile:
    name: str
    description: str
    criteria: FilterCriteria


SCAN_PROFILES = [
    ScanProfile(
        name="balanced",
        description="기본 저평가 후보",
        criteria=DEFAULT_FILTER_CRITERIA,
    ),
    ScanProfile(
        name="conservative",
        description="시총과 유동성을 더 보수적으로 본 후보",
        criteria=STRICT_FILTER_CRITERIA,
    ),
    ScanProfile(
        name="deep_value",
        description="PER/PBR이 특히 낮은 딥밸류 후보",
        criteria=FilterCriteria(
            min_market_cap=30_000_000_000,
            min_avg_trading_value=500_000_000,
            max_per=7.0,
            max_pbr=0.8,
            min_estimated_roe=6.0,
        ),
    ),
    ScanProfile(
        name="quality_value",
        description="ROE가 높고 가격 부담이 과하지 않은 후보",
        criteria=FilterCriteria(
            min_market_cap=30_000_000_000,
            min_avg_trading_value=500_000_000,
            max_per=15.0,
            max_pbr=1.5,
            min_estimated_roe=15.0,
        ),
    ),
    ScanProfile(
        name="liquid_value",
        description="시총과 거래대금이 충분한 유동성 중심 후보",
        criteria=FilterCriteria(
            min_market_cap=100_000_000_000,
            min_avg_trading_value=2_000_000_000,
            max_per=12.0,
            max_pbr=1.2,
            min_estimated_roe=8.0,
        ),
    ),
    ScanProfile(
        name="small_cap_value",
        description="시총 300억~2000억 사이의 소형 저평가 후보",
        criteria=FilterCriteria(
            min_market_cap=30_000_000_000,
            max_market_cap=200_000_000_000,
            min_avg_trading_value=500_000_000,
            max_per=10.0,
            max_pbr=1.0,
            min_estimated_roe=8.0,
        ),
    ),
    ScanProfile(
        name="low_pbr_focus",
        description="PBR이 낮은 자산가치 중심 후보",
        criteria=FilterCriteria(
            min_market_cap=30_000_000_000,
            min_avg_trading_value=500_000_000,
            max_per=15.0,
            max_pbr=0.7,
            min_estimated_roe=5.0,
        ),
    ),
]


def get_profiles(names: list[str] | None = None) -> list[ScanProfile]:
    if not names:
        return list(SCAN_PROFILES)

    profile_by_name = {profile.name: profile for profile in SCAN_PROFILES}
    missing_names = [name for name in names if name not in profile_by_name]
    if missing_names:
        available = ", ".join(profile_by_name)
        raise ValueError(f"알 수 없는 프로필이 있어: {', '.join(missing_names)}. 사용 가능: {available}")

    return [profile_by_name[name] for name in names]
