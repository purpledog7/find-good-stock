import pandas as pd

from src.swing_risk import add_market_risk_info, apply_news_risk_info


def test_add_market_risk_info_loads_optional_cache(tmp_path):
    cache_path = tmp_path / "risk.csv"
    cache_path.write_text(
        "code,risk_flags,exclude_swing\n000001,investment_warning,true\n",
        encoding="utf-8-sig",
    )
    df = pd.DataFrame([{"code": 1}, {"code": "000002"}])

    result = add_market_risk_info(df, cache_path=cache_path)

    assert result.loc[0, "market_risk_flags"] == "investment_warning"
    assert result.loc[0, "code"] == "000001"
    assert bool(result.loc[0, "exclude_swing"]) is True
    assert result.loc[1, "market_risk_flags"] == ""


def test_add_market_risk_info_merges_duplicate_cache_rows(tmp_path):
    cache_path = tmp_path / "risk.csv"
    cache_path.write_text(
        "code,risk_flags,exclude_swing\n"
        "000001,investment_warning,false\n"
        "000001,short_term_overheat,true\n",
        encoding="utf-8-sig",
    )
    df = pd.DataFrame([{"code": "000001"}])

    result = add_market_risk_info(df, cache_path=cache_path)

    assert len(result) == 1
    assert result.loc[0, "market_risk_flags"] == "investment_warning, short_term_overheat"
    assert bool(result.loc[0, "exclude_swing"]) is True


def test_apply_news_risk_info_penalizes_severe_news_flags():
    candidates_df = pd.DataFrame(
        [
            {
                "rank": 1,
                "code": "000001",
                "risk_flags": "",
                "risk_penalty": 0,
                "swing_score": 80,
                "trading_value_today": 5_000_000_000,
                "return_1d": 3.0,
            }
        ]
    )
    raw_news_df = pd.DataFrame(
        [
            {
                "code": "000001",
                "keyword_flags": "investment_warning, contract",
            }
        ]
    )

    result = apply_news_risk_info(candidates_df, raw_news_df)

    assert result.loc[0, "news_risk_flags"] == "investment_warning"
    assert result.loc[0, "risk_penalty"] == 15
    assert result.loc[0, "swing_score"] == 65
    assert "investment_warning" in result.loc[0, "risk_flags"]


def test_apply_news_risk_info_keeps_penalty_column_when_news_is_empty():
    candidates_df = pd.DataFrame([{"code": "000001", "swing_score": 80}])
    raw_news_df = pd.DataFrame(columns=["code", "keyword_flags"])

    result = apply_news_risk_info(candidates_df, raw_news_df)

    assert result.loc[0, "news_risk_flags"] == ""
    assert result.loc[0, "news_risk_penalty"] == 0
