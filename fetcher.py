"""Polite sequential fetcher for DIYAudio/XenForo thread pages."""

from __future__ import annotations

import re
import socket
import time
from html import unescape
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from storage import page_file_path, prepare_thread_storage, write_manifest


LogCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]

USER_AGENT = (
    "DIYAudioThreadDistiller/0.1 StageA-FetchOnly "
    "(polite sequential archive; contact: local-user)"
)
TEMPORARY_HTTP_STATUSES = {408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    body: bytes


def normalize_thread_base_url(thread_url: str) -> str:
    """Return a likely XenForo base thread URL without page/query/fragment parts."""
    parsed = urlparse(thread_url.strip())
    if not parsed.scheme:
        parsed = urlparse("https://" + thread_url.strip())

    path = re.sub(r"/page-\d+/?$", "/", parsed.path, flags=re.IGNORECASE)
    path = re.sub(r"/+", "/", path).rstrip("/") + "/"

    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def page_url(base_url: str, page_number: int) -> str:
    """Build a page URL for XenForo-style thread pagination."""
    if page_number == 1:
        return base_url
    return urljoin(base_url, f"page-{page_number}")


def detect_page_count(html: str, base_url: str) -> int:
    """Detect the largest page number linked by XenForo pagination.

    DIYAudio currently uses XenForo-style thread pagination. This intentionally
    checks a few common patterns, but the selectors/regexes may need adjustment
    after testing against live DIYAudio pages if their markup differs.
    """
    page_numbers = {1}

    for match in re.finditer(r"(?:/|%2F)page-(\d+)", html, flags=re.IGNORECASE):
        page_numbers.add(int(match.group(1)))

    for match in re.finditer(r"[?&]page=(\d+)", html, flags=re.IGNORECASE):
        page_numbers.add(int(match.group(1)))

    for match in re.finditer(
        r'class="[^"]*(?:pageNav-page|pageNav-jump)[^"]*"[^>]*>\s*<a[^>]*>\s*(\d+)\s*</a>',
        html,
        flags=re.IGNORECASE,
    ):
        page_numbers.add(int(match.group(1)))

    # XenForo often exposes canonical/next/last links. Keep this broad because
    # DIYAudio markup should be verified during the first live test pass.
    for href in re.findall(r'href=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        href = unescape(href)
        absolute = urljoin(base_url, href)
        parsed = urlparse(absolute)
        query_page = parse_qs(parsed.query).get("page", [])
        for value in query_page:
            if value.isdigit():
                page_numbers.add(int(value))
        path_match = re.search(r"/page-(\d+)/?$", parsed.path, flags=re.IGNORECASE)
        if path_match:
            page_numbers.add(int(path_match.group(1)))

    return max(page_numbers)


def fetch_thread(
    thread_url: str,
    output_root: Path,
    *,
    force_refetch: bool = False,
    delay_seconds: float = 2.0,
    retries: int = 3,
    timeout_seconds: float = 30.0,
    log: LogCallback | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Fetch all detected thread pages and return the manifest path."""
    logger = log or (lambda message: None)
    reporter = progress or (lambda current, total: None)

    original_url = thread_url.strip()
    if not original_url:
        raise ValueError("Enter a DIYAudio thread URL before starting.")

    base_url = normalize_thread_base_url(original_url)
    thread_dir, raw_dir = prepare_thread_storage(output_root, base_url)
    logger(f"Thread folder: {thread_dir}")
    logger(f"Normalized base URL: {base_url}")

    first_page_path = page_file_path(raw_dir, 1)
    page_entries: list[dict] = []

    if first_page_path.exists() and not force_refetch:
        logger("Using cached page_001.html for page-count detection.")
        first_html = first_page_path.read_text(encoding="utf-8", errors="replace")
        page_entries.append(_cached_entry(1, base_url, first_page_path))
    else:
        logger("Fetching page 1 for page-count detection.")
        response = _fetch_with_retries(base_url, retries, timeout_seconds, logger)
        first_page_path.write_bytes(response.body)
        logger(f"Saved page 1: {_display_path(thread_dir, first_page_path)}")
        first_html = response.body.decode("utf-8", errors="replace")
        page_entries.append(_fetched_entry(1, response.url, first_page_path, response.status))

    detected_page_count = detect_page_count(first_html, base_url)
    logger(f"Detected {detected_page_count} page(s).")
    reporter(1, detected_page_count)

    for page_number in range(2, detected_page_count + 1):
        target_url = page_url(base_url, page_number)
        output_path = page_file_path(raw_dir, page_number)

        if output_path.exists() and not force_refetch:
            logger(f"Skipping cached page {page_number}: {output_path.name}")
            page_entries.append(_cached_entry(page_number, target_url, output_path))
            reporter(page_number, detected_page_count)
            continue

        logger(f"Waiting {delay_seconds:.1f}s before page {page_number}.")
        time.sleep(delay_seconds)

        try:
            logger(f"Fetching page {page_number}: {target_url}")
            response = _fetch_with_retries(target_url, retries, timeout_seconds, logger)
            output_path.write_bytes(response.body)
            logger(f"Saved page {page_number}: {_display_path(thread_dir, output_path)}")
            page_entries.append(
                _fetched_entry(page_number, response.url, output_path, response.status)
            )
        except Exception as exc:
            logger(f"Page {page_number} failed: {exc}")
            page_entries.append(_failed_entry(page_number, target_url, output_path, exc))

        reporter(page_number, detected_page_count)

    manifest_path = write_manifest(
        thread_dir,
        original_thread_url=original_url,
        normalized_base_url=base_url,
        detected_page_count=detected_page_count,
        page_entries=page_entries,
    )
    logger(f"Manifest written: {manifest_path}")
    return manifest_path


def _fetch_with_retries(
    url: str,
    retries: int,
    timeout_seconds: float,
    logger: LogCallback,
) -> HttpResponse:
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            request = Request(url, headers={"User-Agent": USER_AGENT})
            with urlopen(request, timeout=timeout_seconds) as response:
                return HttpResponse(
                    url=response.geturl(),
                    status=response.status,
                    body=response.read(),
                )
        except HTTPError as exc:
            last_error = exc
            if exc.code not in TEMPORARY_HTTP_STATUSES or attempt == retries:
                raise RuntimeError(f"HTTP {exc.code}: {exc.reason}") from exc
            logger(f"Temporary HTTP {exc.code}; retry {attempt + 1}/{retries}.")
        except (URLError, TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(str(exc)) from exc
            logger(f"Temporary network error; retry {attempt + 1}/{retries}: {exc}")

        time.sleep(min(2**attempt, 10))

    raise RuntimeError(str(last_error) if last_error else "Unknown fetch error")


def _fetched_entry(page_number: int, url: str, output_path: Path, status: int) -> dict:
    return {
        "page_number": page_number,
        "url": url,
        "output_file_path": str(output_path),
        "http_status": status,
        "fetch_status": "fetched",
        "error_message": None,
    }


def _cached_entry(page_number: int, url: str, output_path: Path) -> dict:
    return {
        "page_number": page_number,
        "url": url,
        "output_file_path": str(output_path),
        "http_status": None,
        "fetch_status": "skipped_cached",
        "error_message": None,
    }


def _failed_entry(page_number: int, url: str, output_path: Path, error: Exception) -> dict:
    return {
        "page_number": page_number,
        "url": url,
        "output_file_path": str(output_path),
        "http_status": None,
        "fetch_status": "failed",
        "error_message": str(error),
    }


def _display_path(thread_dir: Path, output_path: Path) -> str:
    try:
        return str(output_path.relative_to(thread_dir))
    except ValueError:
        return str(output_path)
