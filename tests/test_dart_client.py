from src.dart_client import extract_financial_metrics, parse_amount


def test_parse_amount_handles_commas_and_parentheses():
    assert parse_amount("1,234") == 1234
    assert parse_amount("(1,234)") == -1234
    assert parse_amount("") is None


def test_extract_financial_metrics_calculates_ratios():
    rows = [
        {"sj_div": "IS", "account_nm": "매출액", "thstrm_amount": "1,000"},
        {"sj_div": "IS", "account_nm": "영업이익", "thstrm_amount": "150"},
        {"sj_div": "IS", "account_nm": "당기순이익", "thstrm_amount": "100"},
        {"sj_div": "BS", "account_nm": "부채총계", "thstrm_amount": "400"},
        {"sj_div": "BS", "account_nm": "자본총계", "thstrm_amount": "800"},
    ]

    result = extract_financial_metrics(rows)

    assert result["revenue"] == 1000
    assert result["operating_profit"] == 150
    assert result["net_income"] == 100
    assert result["debt_ratio"] == 50.0
    assert result["operating_margin"] == 15.0
