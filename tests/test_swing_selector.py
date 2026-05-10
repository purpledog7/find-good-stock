import pandas as pd

from src.swing_selector import (
    SWING_BUY_REVIEW_COLUMNS,
    build_swing_buy_review,
    build_swing_buy_review_prompt,
    save_swing_buy_review,
)


def sample_candidates():
    return pd.DataFrame(
        [
            {
                "date": "2026-05-11",
                "market_date": "2026-05-08",
                "rank": 1,
                "code": "000001",
                "name": "pressed",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "price": 10_000,
                "entry_price": 10_000,
                "swing_score": 72,
                "undervaluation_score": 20,
                "average_discount_score": 16,
                "value_trap_penalty": 0,
                "risk_penalty": 0,
                "news_risk_penalty": 0,
                "per": 7,
                "pbr": 0.7,
                "estimated_roe": 12,
                "earnings_yield": 14,
                "book_discount_pct": 30,
                "price_vs_ma20_pct": -4,
                "price_vs_ma50_pct": -6,
                "price_vs_vwap20_pct": -5,
                "price_vs_vwap50_pct": -8,
                "rsi14": 48,
                "close_position_in_range": 64,
                "return_1d": 0.8,
                "return_3d": -3,
                "return_5d": -5,
                "return_20d": -6,
                "volume_ratio_20d": 1.4,
                "trading_value_ratio_20d": 1.6,
                "accumulation_score": 5,
                "anchored_vwap_score": 8,
                "bb_squeeze_score": 3,
                "pullback_ladder_score": 10,
                "relative_strength_score": 4,
                "matched_setups": "average_discount_pullback, anchored_vwap_support, pullback_ladder",
                "setup_tags": "average_discount, undervalued",
                "risk_flags": "",
                "review_date": "2026-05-14",
                "half_take_profit_price": 10_400,
                "full_take_profit_price": 10_700,
                "add_price_1": 9_600,
                "add_price_2": 9_200,
                "add_price_3": 9_000,
            },
            {
                "date": "2026-05-11",
                "market_date": "2026-05-08",
                "rank": 2,
                "code": "000002",
                "name": "strong_not_pressed",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "price": 12_000,
                "entry_price": 12_000,
                "swing_score": 82,
                "undervaluation_score": 20,
                "average_discount_score": 4,
                "value_trap_penalty": 0,
                "risk_penalty": 0,
                "news_risk_penalty": 0,
                "per": 7,
                "pbr": 0.7,
                "estimated_roe": 12,
                "earnings_yield": 14,
                "book_discount_pct": 30,
                "price_vs_ma20_pct": 6,
                "price_vs_ma50_pct": 7,
                "price_vs_vwap20_pct": 5,
                "price_vs_vwap50_pct": 6,
                "rsi14": 62,
                "close_position_in_range": 75,
                "return_1d": 3,
                "return_3d": 7,
                "return_5d": 8,
                "return_20d": 15,
                "volume_ratio_20d": 2.2,
                "trading_value_ratio_20d": 2.3,
                "accumulation_score": 3,
                "anchored_vwap_score": 2,
                "bb_squeeze_score": 0,
                "pullback_ladder_score": 5,
                "relative_strength_score": 8,
                "matched_setups": "event_pivot",
                "setup_tags": "relative_strength",
                "risk_flags": "",
                "review_date": "2026-05-14",
            },
            {
                "date": "2026-05-11",
                "market_date": "2026-05-08",
                "rank": 3,
                "code": "000003",
                "name": "weak_close",
                "market": "KOSPI",
                "sector": "제조업",
                "industry": "테스트",
                "price": 8_000,
                "entry_price": 8_000,
                "swing_score": 70,
                "undervaluation_score": 22,
                "average_discount_score": 18,
                "value_trap_penalty": 0,
                "risk_penalty": 10,
                "news_risk_penalty": 0,
                "per": 6,
                "pbr": 0.6,
                "estimated_roe": 11,
                "earnings_yield": 16,
                "book_discount_pct": 40,
                "price_vs_ma20_pct": -9,
                "price_vs_ma50_pct": -11,
                "price_vs_vwap20_pct": -10,
                "price_vs_vwap50_pct": -12,
                "rsi14": 32,
                "close_position_in_range": 8,
                "return_1d": -4,
                "return_3d": -9,
                "return_5d": -16,
                "return_20d": -15,
                "volume_ratio_20d": 1.1,
                "trading_value_ratio_20d": 1.1,
                "accumulation_score": 1,
                "anchored_vwap_score": 8,
                "bb_squeeze_score": 0,
                "pullback_ladder_score": 8,
                "relative_strength_score": 1,
                "matched_setups": "average_discount_pullback, anchored_vwap_support",
                "setup_tags": "average_discount",
                "risk_flags": "weak_close, weak_rsi",
                "review_date": "2026-05-14",
            },
        ]
    )


def test_build_swing_buy_review_prioritizes_pressed_bounce_candidate():
    result = build_swing_buy_review(sample_candidates(), top_n=3)

    assert result.columns.tolist() == SWING_BUY_REVIEW_COLUMNS
    assert result.loc[0, "code"] == "000001"
    assert "000002" not in result["code"].tolist()
    assert "near_term_bounce" in result.loc[0, "buy_review_flags"]
    assert "volume_wake" in result.loc[0, "buy_review_flags"]
    assert result.loc[0, "buy_review_eligible"]
    assert result.loc[0, "pressed_anchor_count"] == 4
    assert result.loc[0, "liquidity_wake_score"] > 0


def test_build_swing_buy_review_penalizes_weak_close_and_weak_rsi():
    result = build_swing_buy_review(sample_candidates(), top_n=3)
    weak = result[result["code"] == "000003"].iloc[0]

    assert weak["buy_risk_penalty"] > result.loc[0, "buy_risk_penalty"]
    assert "risk_check_required" in weak["buy_review_flags"]


def test_build_swing_buy_review_filters_ineligible_when_enough_eligible():
    candidates = sample_candidates()
    clone = candidates.iloc[[0]].copy()
    clone.loc[:, "code"] = "000004"
    clone.loc[:, "name"] = "pressed-two"
    clone.loc[:, "rank"] = 4
    candidates = pd.concat([candidates, clone], ignore_index=True)

    result = build_swing_buy_review(candidates, top_n=2)

    assert result["code"].tolist() == ["000001", "000004"]
    assert "000003" not in result["code"].tolist()


def test_build_swing_buy_review_reason_omits_empty_nan_risk_flags():
    candidates = sample_candidates()
    candidates.loc[0, "risk_flags"] = pd.NA

    result = build_swing_buy_review(candidates, top_n=1)

    assert "nan" not in result.loc[0, "buy_review_reason"].lower()


def test_save_swing_buy_review_writes_csv(tmp_path):
    review = build_swing_buy_review(sample_candidates(), top_n=2)

    path = save_swing_buy_review(review, "2026-05-11", tmp_path)

    saved = pd.read_csv(path, dtype={"code": str}, encoding="utf-8-sig")
    assert path.name == "2026-05-11_swing_buy_review_top5.csv"
    assert saved["code"].tolist() == ["000001", "000003"]


def test_build_swing_buy_review_prompt_contains_three_judgment_axes():
    review = build_swing_buy_review(sample_candidates(), top_n=2)

    prompt = build_swing_buy_review_prompt(review, "2026-05-11")

    assert "상승 여력" in prompt
    assert "가격 눌림" in prompt
    assert "근시일 반등" in prompt
    assert "pressed" in prompt
