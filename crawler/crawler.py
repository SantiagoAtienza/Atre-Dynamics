import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import settings

HEADERS = {"User-Agent": "Mozilla/5.0 (LeadRadar/1.0)"}
RETRYABLE_CODES = {403, 408, 425, 429, 500, 502, 503, 504}


def _make_session(proxy=None):
    session = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=0.4,
        status_forcelist=sorted(RETRYABLE_CODES),
        allowed_methods=frozenset({"GET", "HEAD"}),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.headers.update(HEADERS)
    if proxy:
        session.proxies.update({"http": proxy, "https": proxy})
    return session


def _delay_sleep(delay_range: tuple[float, float] | None):
    if not delay_range:
        return
    minimum, maximum = delay_range
    if maximum < minimum:
        minimum, maximum = maximum, minimum
    time.sleep(random.uniform(max(0.0, minimum), max(0.0, maximum)))


def crawl_url(
    url: str,
    timeout: int = 10,
    retries: int = 2,
    proxy: str | None = None,
    delay_range: tuple[float, float] | None = None,
):
    """Fetch one URL safely and return details without raising."""
    session = _make_session(proxy=proxy)
    error_message = ""
    status_code = 0

    for attempt in range(retries + 1):
        _delay_sleep(delay_range)
        try:
            response = session.get(url, timeout=timeout)
            status_code = int(response.status_code)
            if status_code in {403, 429}:
                error_message = f"http_{status_code}"
                if attempt < retries:
                    wait_seconds = 0.8 + attempt * 1.4
                    time.sleep(wait_seconds)
                    continue
                return {
                    "url": url,
                    "html": None,
                    "status_code": status_code,
                    "error": error_message,
                }

            if status_code >= 400:
                error_message = f"http_{status_code}"
                if attempt < retries and status_code in RETRYABLE_CODES:
                    time.sleep(0.5 + attempt)
                    continue
                return {
                    "url": url,
                    "html": None,
                    "status_code": status_code,
                    "error": error_message,
                }

            return {
                "url": url,
                "html": response.text,
                "status_code": status_code,
                "error": "",
            }
        except requests.RequestException as error:
            error_message = str(error)
            if attempt < retries:
                time.sleep(0.6 + attempt)
                continue

    return {
        "url": url,
        "html": None,
        "status_code": status_code,
        "error": error_message or "request_failed",
    }


def fetch_many_details(
    urls: list[str],
    concurrency: int = 8,
    timeout: int = 10,
    delay_range: tuple[float, float] | None = None,
):
    """Fetch many URLs concurrently. Returns dict url -> response details."""
    details: dict[str, dict[str, Any]] = {}
    if not urls:
        return details

    safe_concurrency = max(1, min(int(concurrency), max(2, len(urls))))
    with ThreadPoolExecutor(max_workers=safe_concurrency) as executor:
        futures = {
            executor.submit(
                crawl_url,
                url,
                timeout,
                2,
                settings.LR_PROXY or None,
                delay_range,
            ): url
            for url in urls
        }
        for future in as_completed(futures):
            url = futures[future]
            try:
                details[url] = future.result()
            except Exception as error:
                details[url] = {
                    "url": url,
                    "html": None,
                    "status_code": 0,
                    "error": str(error),
                }
    return details


def fetch_many(urls: list[str], concurrency=8, timeout=10):
    """Compatibility helper: returns dict url -> html."""
    details = fetch_many_details(
        urls=urls,
        concurrency=concurrency,
        timeout=timeout,
        delay_range=(settings.DELAY_MIN, settings.DELAY_MAX),
    )
    return {url: item.get("html") for url, item in details.items()}
