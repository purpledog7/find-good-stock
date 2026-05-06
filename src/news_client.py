from __future__ import annotations

import html
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Callable
from zoneinfo import ZoneInfo

import requests

from config import KST_TIMEZONE, RETRY_COUNT, RETRY_SLEEP_SECONDS


NAVER_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"
ProgressCallback = Callable[[str], None] | None


@dataclass(frozen=True)
class NewsItem:
    title: str
    description: str
    link: str
    pub_date: datetime


class NaverNewsClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        request_sleep_seconds: float = 0.2,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.request_sleep_seconds = request_sleep_seconds

    @classmethod
    def from_env(cls) -> "NaverNewsClient":
        client_id = os.getenv("NAVER_CLIENT_ID")
        client_secret = os.getenv("NAVER_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "뉴스 보강을 쓰려면 `NAVER_CLIENT_ID`와 `NAVER_CLIENT_SECRET` 환경변수가 필요해."
            )
        return cls(client_id, client_secret)

    def search_recent_news(
        self,
        query: str,
        start_dt: datetime,
        end_dt: datetime,
        display: int = 20,
    ) -> list[NewsItem]:
        payload = self.request_news(query, display)
        items = [parse_news_item(item) for item in payload.get("items", [])]
        filtered_items = [
            item
            for item in items
            if start_dt <= item.pub_date.astimezone(start_dt.tzinfo) <= end_dt
        ]
        time.sleep(self.request_sleep_seconds)
        return filtered_items

    def request_news(self, query: str, display: int) -> dict:
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        params = {
            "query": query,
            "display": min(max(display, 1), 100),
            "start": 1,
            "sort": "date",
        }

        last_error: Exception | None = None
        for attempt in range(RETRY_COUNT):
            try:
                response = requests.get(
                    NAVER_NEWS_URL,
                    headers=headers,
                    params=params,
                    timeout=20,
                )
                response.raise_for_status()
                return response.json()
            except requests.RequestException as error:
                last_error = error
                if attempt < RETRY_COUNT - 1:
                    time.sleep(RETRY_SLEEP_SECONDS * (attempt + 1))

        raise RuntimeError(f"네이버 뉴스 요청 실패: {last_error}") from last_error


def default_news_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(KST_TIMEZONE)
    end_dt = (now or datetime.now(timezone)).astimezone(timezone)
    start_dt = (end_dt - timedelta(days=1)).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    return start_dt, end_dt


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo(KST_TIMEZONE))
    return parsed


def parse_news_item(item: dict) -> NewsItem:
    return NewsItem(
        title=clean_html(item.get("title", "")),
        description=clean_html(item.get("description", "")),
        link=item.get("originallink") or item.get("link", ""),
        pub_date=parsedate_to_datetime(item["pubDate"]),
    )


def clean_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", "", value)
    return html.unescape(text).strip()
