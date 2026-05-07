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
        target_count = min(max(display, 1), 100)
        page_size = 100
        start = 1
        collected: list[NewsItem] = []

        while len(collected) < target_count and start <= 1000:
            payload = self.request_news(query, page_size, start=start)
            items = parse_news_items(payload.get("items", []))
            if not items:
                break

            reached_older_news = False
            for item in items:
                pub_date = item.pub_date.astimezone(start_dt.tzinfo)
                if start_dt <= pub_date <= end_dt:
                    collected.append(item)
                    if len(collected) >= target_count:
                        break
                elif pub_date < start_dt:
                    reached_older_news = True

            if reached_older_news or len(items) < page_size:
                break

            start += page_size
            time.sleep(self.request_sleep_seconds)

        time.sleep(self.request_sleep_seconds)
        return collected[:target_count]

    def search_recent_news_multi(
        self,
        queries: list[str],
        start_dt: datetime,
        end_dt: datetime,
        display: int = 20,
    ) -> list[NewsItem]:
        target_count = min(max(display, 1), 100)
        collected: list[NewsItem] = []
        seen_keys: set[str] = set()

        for query in queries:
            items = self.search_recent_news(
                query,
                start_dt=start_dt,
                end_dt=end_dt,
                display=target_count,
            )
            for item in items:
                key = item.link or f"{item.title}:{item.pub_date.isoformat()}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                collected.append(item)

        collected = sorted(collected, key=lambda item: item.pub_date, reverse=True)
        return collected[:target_count]

    def request_news(self, query: str, display: int, start: int = 1) -> dict:
        headers = {
            "X-Naver-Client-Id": self.client_id,
            "X-Naver-Client-Secret": self.client_secret,
        }
        params = {
            "query": query,
            "display": min(max(display, 1), 100),
            "start": min(max(start, 1), 1000),
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


def default_news_window(
    now: datetime | None = None,
    run_date: str | None = None,
) -> tuple[datetime, datetime]:
    timezone = ZoneInfo(KST_TIMEZONE)
    current_dt = (now or datetime.now(timezone)).astimezone(timezone)

    if run_date is not None:
        target_date = datetime.strptime(run_date, "%Y-%m-%d").date()
        end_dt = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            7,
            tzinfo=timezone,
        )
    else:
        scheduled_end = current_dt.replace(hour=7, minute=0, second=0, microsecond=0)
        end_dt = current_dt if current_dt < scheduled_end else scheduled_end

    start_date = end_dt.date() - timedelta(days=1)
    start_dt = datetime(
        start_date.year,
        start_date.month,
        start_date.day,
        16,
        tzinfo=timezone,
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


def parse_news_items(items: list[dict]) -> list[NewsItem]:
    parsed_items: list[NewsItem] = []
    for item in items:
        try:
            parsed_items.append(parse_news_item(item))
        except (KeyError, TypeError, ValueError, IndexError):
            continue
    return parsed_items


def clean_html(value) -> str:
    text = re.sub(r"<[^>]+>", "", str(value or ""))
    return html.unescape(text).strip()
