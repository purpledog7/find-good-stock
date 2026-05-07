import pandas as pd

from src.sector_enricher import add_sector_info


def test_add_sector_info_uses_cached_sector_file(tmp_path):
    cache_path = tmp_path / "krx_desc.csv"
    pd.DataFrame(
        [
            {
                "code": 1,
                "sector": "제조업",
                "industry": "테스트 산업",
            },
            {
                "code": "000001",
                "sector": "중복",
                "industry": "중복 산업",
            }
        ]
    ).to_csv(cache_path, index=False, encoding="utf-8-sig")
    df = pd.DataFrame([{"code": "000001", "name": "테스트"}])

    result = add_sector_info(df, cache_path=cache_path)

    assert len(result) == 1
    assert result.loc[0, "sector"] == "제조업"
    assert result.loc[0, "industry"] == "테스트 산업"
