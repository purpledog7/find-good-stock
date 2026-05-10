from __future__ import annotations

import html
import os
import re
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
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
    naver_link: str = ""


@dataclass(frozen=True)
class PageMetadata:
    title: str = ""
    description: str = ""


class NaverNewsClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        request_sleep_seconds: float = 0.2,
        resolve_page_metadata: bool = True,
        request_timeout_seconds: float = 8.0,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.request_sleep_seconds = request_sleep_seconds
        self.resolve_page_metadata = resolve_page_metadata
        self.request_timeout_seconds = request_timeout_seconds
        self._page_metadata_cache: dict[str, PageMetadata] = {}

    @classmethod
    def from_env(
        cls,
        request_sleep_seconds: float = 0.2,
        resolve_page_metadata: bool = True,
        request_timeout_seconds: float = 8.0,
    ) -> "NaverNewsClient":
        client_id = os.getenv("NAVER_CLIENT_ID")
        client_secret = os.getenv("NAVER_CLIENT_SECRET")
        if not client_id or not client_secret:
            raise RuntimeError(
                "뉴스 보강을 쓰려면 `NAVER_CLIENT_ID`와 `NAVER_CLIENT_SECRET` 환경변수가 필요해."
            )
        return cls(
            client_id,
            client_secret,
            request_sleep_seconds=request_sleep_seconds,
            resolve_page_metadata=resolve_page_metadata,
            request_timeout_seconds=request_timeout_seconds,
        )

    def search_recent_news(
        self,
        query: str,
        start_dt: datetime | None,
        end_dt: datetime | None,
        display: int = 20,
        enrich_metadata: bool = True,
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
                if start_dt is None and end_dt is None:
                    collected.append(
                        self.maybe_enrich_item_metadata(item, enrich_metadata)
                    )
                    if len(collected) >= target_count:
                        break
                    continue

                timezone = (start_dt or end_dt).tzinfo
                pub_date = item.pub_date.astimezone(timezone) if timezone else item.pub_date
                if (start_dt is None or pub_date >= start_dt) and (
                    end_dt is None or pub_date <= end_dt
                ):
                    collected.append(
                        self.maybe_enrich_item_metadata(item, enrich_metadata)
                    )
                    if len(collected) >= target_count:
                        break
                elif start_dt is not None and pub_date < start_dt:
                    reached_older_news = True

            if reached_older_news or len(items) < page_size:
                break

            start += page_size
            time.sleep(self.request_sleep_seconds)

        time.sleep(self.request_sleep_seconds)
        return collected[:target_count]

    def maybe_enrich_item_metadata(self, item: NewsItem, enabled: bool) -> NewsItem:
        if not enabled:
            return item
        return self.enrich_item_metadata(item)

    def enrich_item_metadata(self, item: NewsItem) -> NewsItem:
        if not self.resolve_page_metadata:
            return item
        if not looks_truncated_text(item.title):
            return item

        metadata = self.fetch_page_metadata(item.link or item.naver_link)
        title = choose_better_text(item.title, metadata.title, require_untruncated=True)
        description = choose_better_text(
            item.description,
            metadata.description,
            require_untruncated=False,
        )
        if title == item.title and item.naver_link and item.naver_link != item.link:
            fallback_metadata = self.fetch_page_metadata(item.naver_link)
            title = choose_better_text(
                item.title,
                fallback_metadata.title,
                require_untruncated=True,
            )
            description = choose_better_text(
                description,
                fallback_metadata.description,
                require_untruncated=False,
            )
        if title == item.title and description == item.description:
            return item
        return replace(item, title=title, description=description)

    def fetch_page_metadata(self, url: str) -> PageMetadata:
        url = clean_url(url)
        if not url:
            return PageMetadata()
        if url in self._page_metadata_cache:
            return self._page_metadata_cache[url]

        metadata = PageMetadata()
        try:
            response = requests.get(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (compatible; find-good-stock/0.5; "
                        "+https://example.invalid/metadata)"
                    )
                },
                timeout=5,
            )
            content_type = response.headers.get("Content-Type", "").lower()
            if response.ok and ("html" in content_type or not content_type):
                parser = PageMetadataParser()
                parser.feed(response.text)
                metadata = parser.to_metadata()
        except (requests.RequestException, ValueError):
            metadata = PageMetadata()

        self._page_metadata_cache[url] = metadata
        return metadata

    def search_recent_news_multi(
        self,
        queries: list[str],
        start_dt: datetime | None,
        end_dt: datetime | None,
        display: int = 20,
        enrich_metadata: bool = True,
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
                enrich_metadata=False,
            )
            for item in items:
                key = (
                    item.link
                    or item.naver_link
                    or f"{item.title}:{item.pub_date.isoformat()}"
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                collected.append(item)

        collected = sorted(collected, key=lambda item: item.pub_date, reverse=True)
        limited = collected[:target_count]
        if not enrich_metadata:
            return limited
        return [self.enrich_item_metadata(item) for item in limited]

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
                    timeout=self.request_timeout_seconds,
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
    original_link = clean_url(item.get("originallink"))
    naver_link = clean_url(item.get("link"))
    return NewsItem(
        title=clean_html(item.get("title", "")),
        description=clean_html(item.get("description", "")),
        link=original_link or naver_link,
        pub_date=parsedate_to_datetime(item["pubDate"]),
        naver_link=naver_link,
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


def clean_url(value) -> str:
    return html.unescape(str(value or "")).strip()


def looks_truncated_text(value: str) -> bool:
    text = str(value or "").strip()
    return text.endswith("...") or text.endswith("..") or text.endswith("…")


def choose_better_text(current: str, candidate: str, require_untruncated: bool) -> str:
    current = clean_html(current)
    candidate = clean_html(candidate).replace("\n", " ")
    if not candidate:
        return current
    if require_untruncated and looks_truncated_text(candidate):
        return current
    if len(candidate) <= len(current.rstrip(".…")):
        return current
    return candidate


class PageMetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.meta: dict[str, str] = {}
        self.title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag: str, attrs) -> None:
        tag = tag.lower()
        if tag == "title":
            self._in_title = True
            return
        if tag != "meta":
            return

        values = {str(key).lower(): str(value or "") for key, value in attrs}
        key = values.get("property") or values.get("name")
        content = values.get("content", "")
        if key and content:
            self.meta[key.lower()] = content

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() == "title":
            self._in_title = False

    def to_metadata(self) -> PageMetadata:
        title = (
            self.meta.get("og:title")
            or self.meta.get("twitter:title")
            or " ".join(self.title_parts)
        )
        description = (
            self.meta.get("og:description")
            or self.meta.get("twitter:description")
            or self.meta.get("description")
        )
        return PageMetadata(
            title=clean_html(title).replace("\n", " "),
            description=clean_html(description).replace("\n", " "),
        )
