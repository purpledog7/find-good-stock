from __future__ import annotations

from dataclasses import dataclass, replace

from config import (
    MAX_PBR,
    MAX_PER,
    MIN_AVG_TRADING_VALUE_20D,
    MIN_ESTIMATED_ROE,
    MIN_MARKET_CAP,
    STRICT_MAX_PBR,
    STRICT_MAX_PER,
    STRICT_MIN_AVG_TRADING_VALUE,
    STRICT_MIN_ESTIMATED_ROE,
    STRICT_MIN_MARKET_CAP,
)


@dataclass(frozen=True)
class FilterCriteria:
    min_market_cap: int = MIN_MARKET_CAP
    max_market_cap: int | None = None
    min_avg_trading_value: int = MIN_AVG_TRADING_VALUE_20D
    max_per: float = MAX_PER
    max_pbr: float = MAX_PBR
    min_estimated_roe: float = MIN_ESTIMATED_ROE


DEFAULT_FILTER_CRITERIA = FilterCriteria()

STRICT_FILTER_CRITERIA = FilterCriteria(
    min_market_cap=STRICT_MIN_MARKET_CAP,
    min_avg_trading_value=STRICT_MIN_AVG_TRADING_VALUE,
    max_per=STRICT_MAX_PER,
    max_pbr=STRICT_MAX_PBR,
    min_estimated_roe=STRICT_MIN_ESTIMATED_ROE,
)


def update_criteria(criteria: FilterCriteria, **overrides) -> FilterCriteria:
    clean_overrides = {
        key: value for key, value in overrides.items() if value is not None
    }
    return replace(criteria, **clean_overrides)
