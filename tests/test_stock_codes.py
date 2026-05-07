import pandas as pd

from src.stock_codes import normalize_stock_code, normalize_stock_code_series


def test_normalize_stock_code_handles_numeric_and_missing_values():
    assert normalize_stock_code(1) == "000001"
    assert normalize_stock_code(1.0) == "000001"
    assert normalize_stock_code("000001") == "000001"
    assert normalize_stock_code(None) == ""


def test_normalize_stock_code_series_handles_numeric_and_missing_values():
    result = normalize_stock_code_series(pd.Series([1, 1.0, "000002", None, "nan"]))

    assert result.tolist() == ["000001", "000001", "000002", "", ""]
