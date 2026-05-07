import pandas as pd

from src.dart_client import DartClient, extract_financial_metrics, normalize_corp_codes, parse_amount


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


def test_normalize_corp_codes_handles_numeric_stock_codes_and_duplicates():
    df = pd.DataFrame(
        [
            {"corp_code": 123, "corp_name": "A", "stock_code": 1, "modify_date": "20260101"},
            {"corp_code": "00000456", "corp_name": "B", "stock_code": "000001", "modify_date": "20260102"},
        ]
    )

    result = normalize_corp_codes(df)

    assert len(result) == 1
    assert result.loc[0, "corp_code"] == "00000123"
    assert result.loc[0, "stock_code"] == "000001"


def test_dart_client_normalizes_cached_corp_codes(tmp_path):
    cache_path = tmp_path / "corp_codes.csv"
    pd.DataFrame(
        [{"corp_code": 123, "corp_name": "A", "stock_code": 1, "modify_date": "20260101"}]
    ).to_csv(cache_path, index=False, encoding="utf-8-sig")
    client = DartClient("key", cache_path=cache_path)

    corp_codes = client.get_corp_codes()

    assert client.find_corp_code(corp_codes, 1.0) == "00000123"
