"""Storage helpers for fetched DIYAudio thread HTML."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse


def default_threads_root() -> Path:
    """Return the default parent folder for stored thread fetches."""
    return Path.cwd() / "data" / "threads"


def safe_thread_folder_name(thread_url: str) -> str:
    """Build a conservative folder name from a thread URL."""
    parsed = urlparse(thread_url)
    path_parts = [part for part in parsed.path.split("/") if part]

    useful_part = ""
    for part in reversed(path_parts):
        if not re.fullmatch(r"page-\d+", part, flags=re.IGNORECASE):
            useful_part = part
            break

    if not useful_part:
        useful_part = parsed.netloc or "thread"

    useful_part = unquote(useful_part)
    useful_part = re.sub(r"\.[a-z0-9]+$", lambda m: m.group(0), useful_part, flags=re.IGNORECASE)
    safe = re.sub(r"[^A-Za-z0-9._-]+", "_", useful_part).strip("._-")
    safe = re.sub(r"_+", "_", safe)
    return safe[:100] or "thread"


def prepare_thread_storage(output_root: Path, thread_url: str) -> tuple[Path, Path]:
    """Create and return the thread folder and raw HTML folder."""
    thread_dir = output_root / safe_thread_folder_name(thread_url)
    raw_dir = thread_dir / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)
    return thread_dir, raw_dir


def page_file_path(raw_dir: Path, page_number: int) -> Path:
    """Return the destination path for a raw HTML page."""
    return raw_dir / f"page_{page_number:03d}.html"


def write_manifest(
    thread_dir: Path,
    *,
    original_thread_url: str,
    normalized_base_url: str | None,
    detected_page_count: int,
    page_entries: list[dict],
) -> Path:
    """Write fetch_manifest.json next to the raw folder."""
    fetched_page_count = sum(
        1
        for entry in page_entries
        if entry.get("fetch_status") in {"fetched", "skipped_cached"}
    )
    downloaded_page_count = sum(1 for entry in page_entries if entry.get("fetch_status") == "fetched")
    cached_page_count = sum(
        1 for entry in page_entries if entry.get("fetch_status") == "skipped_cached"
    )
    failed_page_count = sum(1 for entry in page_entries if entry.get("fetch_status") == "failed")

    manifest = {
        "original_thread_url": original_thread_url,
        "normalized_base_url": normalized_base_url,
        "detected_page_count": detected_page_count,
        "fetched_page_count": fetched_page_count,
        "downloaded_page_count": downloaded_page_count,
        "cached_page_count": cached_page_count,
        "failed_page_count": failed_page_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "pages": page_entries,
    }

    manifest_path = thread_dir / "fetch_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path
