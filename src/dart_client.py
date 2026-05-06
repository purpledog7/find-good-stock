from __future__ import annotations

import io
import os
import time
import zipfile
from pathlib import Path
from typing import Callable
from xml.etree import ElementTree

import pandas as pd
import requests

from config import RETRY_COUNT, RETRY_SLEEP_SECONDS


CORP_CODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
FINANCIAL_STATEMENT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
DEFAULT_CACHE_PATH = Path("data/cache/corp_codes.csv")

ProgressCallback = Callable[[str], None] | None


class DartClient:
    def __init__(
        self,
        api_key: str,
        cache_path: Path = DEFAULT_CACHE_PATH,
        request_sleep_seconds: float = 0.2,
    ) -> None:
        self.api_key = api_key
        self.cache_path = cache_path
        self.request_sleep_seconds = request_sleep_seconds
        self._corp_codes: pd.DataFrame | None = None

    @classmethod
    def from_env(cls) -> "DartClient":
        api_key = os.getenv("DART_API_KEY")
        if not api_key:
            raise RuntimeError(
                "OpenDART 보강을 쓰려면 `DART_API_KEY` 환경변수가 필요해."
            )
        return cls(api_key)

    def fetch_metrics_for_stock_codes(
        self,
        stock_codes: list[str],
        bsns_year: str,
        reprt_code: str = "11011",
        fs_div: str = "CFS",
        progress: ProgressCallback = None,
    ) -> pd.DataFrame:
        rows: list[dict] = []
        corp_codes = self.get_corp_codes(progress)

        for index, stock_code in enumerate(stock_codes, start=1):
            emit_progress(
                progress,
                f"  OpenDART 조회 중 ({index}/{len(stock_codes)}): {stock_code}",
            )
            corp_code = self.find_corp_code(corp_codes, stock_code)
            if not corp_code:
                rows.append(empty_metrics(stock_code, bsns_year))
                continue

            try:
                statement_rows = self.fetch_financial_statement(
                    corp_code,
                    bsns_year,
                    reprt_code,
                    fs_div,
                )
                metrics = extract_financial_metrics(statement_rows)
                metrics.update(
                    {
                        "code": stock_code,
                        "dart_corp_code": corp_code,
                        "dart_bsns_year": bsns_year,
                    }
                )
                rows.append(metrics)
            except RuntimeError:
                rows.append(empty_metrics(stock_code, bsns_year, corp_code))

            time.sleep(self.request_sleep_seconds)

        return pd.DataFrame(rows)

    def get_corp_codes(self, progress: ProgressCallback = None) -> pd.DataFrame:
        if self._corp_codes is not None:
            return self._corp_codes

        if self.cache_path.exists():
            self._corp_codes = pd.read_csv(
                self.cache_path,
                dtype={"corp_code": str, "stock_code": str},
            )
            return self._corp_codes

        emit_progress(progress, "  OpenDART 기업코드 목록 다운로드 중...")
        response = request_with_retry(CORP_CODE_URL, {"crtfc_key": self.api_key})
        corp_codes = parse_corp_code_response(response.content)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        corp_codes.to_csv(self.cache_path, index=False, encoding="utf-8-sig")
        self._corp_codes = corp_codes
        return corp_codes

    def find_corp_code(self, corp_codes: pd.DataFrame, stock_code: str) -> str | None:
        code = str(stock_code).zfill(6)
        matches = corp_codes.loc[corp_codes["stock_code"] == code, "corp_code"]
        if matches.empty:
            return None
        return str(matches.iloc[0]).zfill(8)

    def fetch_financial_statement(
        self,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ) -> list[dict]:
        response = request_with_retry(
            FINANCIAL_STATEMENT_URL,
            {
                "crtfc_key": self.api_key,
                "corp_code": corp_code,
                "bsns_year": bsns_year,
                "reprt_code": reprt_code,
                "fs_div": fs_div,
            },
        )
        payload = response.json()
        status = payload.get("status")
        if status != "000":
            message = payload.get("message", "OpenDART 조회 실패")
            raise RuntimeError(f"OpenDART 오류({status}): {message}")
        return payload.get("list", [])


def request_with_retry(url: str, params: dict) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(RETRY_COUNT):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt < RETRY_COUNT - 1:
                time.sleep(RETRY_SLEEP_SECONDS * (attempt + 1))

    raise RuntimeError(f"OpenDART 요청 실패: {last_error}") from last_error


def parse_corp_code_response(content: bytes) -> pd.DataFrame:
    xml_bytes = extract_xml_bytes(content)
    root = ElementTree.fromstring(xml_bytes)
    rows: list[dict] = []

    for item in root.findall("list"):
        stock_code = (item.findtext("stock_code") or "").strip()
        if not stock_code:
            continue

        rows.append(
            {
                "corp_code": (item.findtext("corp_code") or "").strip().zfill(8),
                "corp_name": (item.findtext("corp_name") or "").strip(),
                "stock_code": stock_code.zfill(6),
                "modify_date": (item.findtext("modify_date") or "").strip(),
            }
        )

    return pd.DataFrame(rows)


def extract_xml_bytes(content: bytes) -> bytes:
    if zipfile.is_zipfile(io.BytesIO(content)):
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            xml_name = archive.namelist()[0]
            return archive.read(xml_name)
    return content


def extract_financial_metrics(rows: list[dict]) -> dict:
    revenue = find_amount(rows, {"매출액", "수익(매출액)", "영업수익"}, "IS")
    operating_profit = find_amount(rows, {"영업이익"}, "IS")
    net_income = find_amount(rows, {"당기순이익", "당기순이익(손실)"}, "IS")
    liabilities = find_amount(rows, {"부채총계"}, "BS")
    equity = find_amount(rows, {"자본총계"}, "BS")

    debt_ratio = None
    if liabilities is not None and equity and equity > 0:
        debt_ratio = round(liabilities / equity * 100, 2)

    operating_margin = None
    if operating_profit is not None and revenue and revenue > 0:
        operating_margin = round(operating_profit / revenue * 100, 2)

    return {
        "revenue": revenue,
        "operating_profit": operating_profit,
        "net_income": net_income,
        "debt_ratio": debt_ratio,
        "operating_margin": operating_margin,
    }


def find_amount(
    rows: list[dict],
    account_names: set[str],
    statement_division: str,
) -> int | None:
    for row in rows:
        if row.get("sj_div") != statement_division:
            continue
        account_name = normalize_account_name(row.get("account_nm", ""))
        if account_name in account_names:
            return parse_amount(row.get("thstrm_amount"))
    return None


def normalize_account_name(value: str) -> str:
    return str(value).replace(" ", "").strip()


def parse_amount(value) -> int | None:
    if value is None:
        return None

    text = str(value).replace(",", "").strip()
    if not text:
        return None
    if text.startswith("(") and text.endswith(")"):
        text = "-" + text[1:-1]

    try:
        return int(float(text))
    except ValueError:
        return None


def empty_metrics(
    stock_code: str,
    bsns_year: str,
    corp_code: str | None = None,
) -> dict:
    return {
        "code": stock_code,
        "dart_corp_code": corp_code or "",
        "dart_bsns_year": bsns_year,
        "revenue": None,
        "operating_profit": None,
        "net_income": None,
        "debt_ratio": None,
        "operating_margin": None,
    }


def emit_progress(progress: ProgressCallback, message: str) -> None:
    if progress is not None:
        progress(message)
