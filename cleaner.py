"""Stage C deterministic cleaner for parsed forum posts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


LogCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]

SOURCE_FIELDS = [
    "page_number",
    "post_index_on_page",
    "post_id",
    "post_number",
    "post_url",
    "author",
    "date_text",
    "date_text_raw",
    "date_iso",
    "date_parse_status",
    "body_raw_html",
    "body_text",
    "quotes_text",
    "links",
    "images",
    "source_page_path",
    "parser_warnings",
]

REQUIRED_LIST_FIELDS = ["quotes_text", "links", "images", "parser_warnings"]
ACKNOWLEDGEMENT_TEXTS = {
    "+1",
    "following",
    "great",
    "nice",
    "thanks",
    "thank you",
    "thx",
}
TECHNICAL_UNIT_PATTERN = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:v|mv|a|ma|w|mw|ohm|r|k|kohm|uf|nf|pf|hz|khz|mhz|db)\b",
    flags=re.IGNORECASE,
)
COMPONENT_PATTERN = re.compile(
    r"\b(?:r|c|q|d|u|ic|lm|ne|opa|tl|bc|bd|irf|2n|1n)\d+[a-z0-9-]*\b",
    flags=re.IGNORECASE,
)


def clean_thread_posts(
    thread_folder: Path,
    *,
    log: LogCallback | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Clean posts_raw.json in a thread folder and return posts_clean.json."""
    logger = log or (lambda message: None)
    reporter = progress or (lambda current, total: None)

    thread_folder = thread_folder.expanduser().resolve()
    source_path = thread_folder / "posts_raw.json"
    logger(f"Loading posts_raw.json: {source_path}")

    if not source_path.exists():
        raise FileNotFoundError(f"Missing posts_raw.json in {thread_folder}")

    raw_data = json.loads(source_path.read_text(encoding="utf-8"))
    raw_posts = raw_data.get("posts", [])
    if not isinstance(raw_posts, list):
        raise ValueError("posts_raw.json has no posts list.")

    logger(f"Total posts loaded: {len(raw_posts)}")

    cleaned_posts: list[dict] = []
    for index, raw_post in enumerate(raw_posts, start=1):
        if not isinstance(raw_post, dict):
            cleaned_posts.append(_clean_invalid_post(raw_post, index))
        else:
            cleaned_posts.append(_clean_post(raw_post))

        if index == 1 or index == len(raw_posts) or index % 25 == 0:
            logger(f"Cleaning progress: {index}/{len(raw_posts)}")
        reporter(index, len(raw_posts))

    summary = _build_summary(cleaned_posts)
    output = {
        "thread_metadata": raw_data.get("thread") or raw_data.get("thread_metadata") or {},
        "source_file": "posts_raw.json",
        "cleaner_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_post_count": len(cleaned_posts),
        "posts": cleaned_posts,
        "cleaner_summary": summary,
    }

    output_path = thread_folder / "posts_clean.json"
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    logger(f"Total posts cleaned: {summary['posts_cleaned']}")
    logger(f"Noise/empty posts: {summary['empty_or_noise_posts']}")
    logger(f"Output written: {output_path}")
    return output_path


def _clean_post(raw_post: dict) -> dict:
    cleaner_warnings: list[str] = []
    cleaned = {field: raw_post[field] for field in SOURCE_FIELDS if field in raw_post}

    for field in ["body_text", "quotes_text", "links", "images"]:
        if field not in raw_post:
            cleaner_warnings.append(f"Missing {field}")

    quotes_text = _safe_list(raw_post.get("quotes_text"), "quotes_text", cleaner_warnings)
    links = _safe_list(raw_post.get("links"), "links", cleaner_warnings)
    images = _safe_list(raw_post.get("images"), "images", cleaner_warnings)

    body_text = raw_post.get("body_text") or ""
    if not isinstance(body_text, str):
        cleaner_warnings.append("body_text was not a string")
        body_text = str(body_text)

    body_text_clean = normalize_text(body_text)
    body_text_no_quotes = remove_known_quotes(body_text_clean, quotes_text, cleaner_warnings)
    body_text_no_quotes = normalize_text(body_text_no_quotes)

    quote_count = len(quotes_text)
    link_count = len(links)
    image_count = len(images)
    is_empty_or_noise = _is_empty_or_noise(
        body_text_clean=body_text_clean,
        body_text_no_quotes=body_text_no_quotes,
        quote_count=quote_count,
        link_count=link_count,
        image_count=image_count,
        cleaner_warnings=cleaner_warnings,
    )

    cleaned.update(
        {
            "body_text_clean": body_text_clean,
            "body_text_no_quotes": body_text_no_quotes,
            "word_count": _word_count(body_text_clean),
            "char_count": len(body_text_clean),
            "line_count": _line_count(body_text_clean),
            "quote_count": quote_count,
            "link_count": link_count,
            "image_count": image_count,
            "has_quotes": quote_count > 0,
            "has_links": link_count > 0,
            "has_images": image_count > 0,
            "is_empty_or_noise": is_empty_or_noise,
            "cleaner_warnings": cleaner_warnings,
        }
    )
    return cleaned


def _clean_invalid_post(raw_post: object, index: int) -> dict:
    text = normalize_text(str(raw_post))
    return {
        "post_index_on_page": index,
        "body_text": text,
        "body_text_clean": text,
        "body_text_no_quotes": text,
        "word_count": _word_count(text),
        "char_count": len(text),
        "line_count": _line_count(text),
        "quote_count": 0,
        "link_count": 0,
        "image_count": 0,
        "has_quotes": False,
        "has_links": False,
        "has_images": False,
        "is_empty_or_noise": not bool(text),
        "cleaner_warnings": ["Raw post was not an object"],
    }


def normalize_text(text: str) -> str:
    """Normalize whitespace while preserving meaningful line breaks."""
    normalized = text.replace("\r\n", "\n").replace("\r", "\n")
    normalized = "\n".join(re.sub(r"[ \t]+", " ", line).strip() for line in normalized.split("\n"))
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def remove_known_quotes(
    body_text: str,
    quotes_text: list,
    cleaner_warnings: list[str],
) -> str:
    """Remove exact known quote text where it is safe to do so."""
    result = body_text
    for quote in quotes_text:
        if not isinstance(quote, str):
            cleaner_warnings.append("quotes_text contained a non-string value")
            continue

        quote_clean = normalize_text(quote)
        if not quote_clean:
            continue

        if quote_clean in result:
            result = result.replace(quote_clean, "")
        else:
            compact_result = re.sub(r"\s+", " ", result)
            compact_quote = re.sub(r"\s+", " ", quote_clean)
            if compact_quote and compact_quote in compact_result:
                cleaner_warnings.append("Quote text matched only after whitespace compaction")
            else:
                cleaner_warnings.append("Quote text could not be removed exactly")
    return result


def _safe_list(value: object, field_name: str, cleaner_warnings: list[str]) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    cleaner_warnings.append(f"{field_name} was not a list")
    return []


def _word_count(text: str) -> int:
    return len(re.findall(r"\b[\w.+#/-]+\b", text, flags=re.UNICODE))


def _line_count(text: str) -> int:
    return len(text.splitlines()) if text else 0


def _is_empty_or_noise(
    *,
    body_text_clean: str,
    body_text_no_quotes: str,
    quote_count: int,
    link_count: int,
    image_count: int,
    cleaner_warnings: list[str],
) -> bool:
    candidate = body_text_no_quotes or body_text_clean
    normalized = normalize_text(candidate).lower()
    compact = re.sub(r"\s+", " ", normalized).strip()

    if link_count > 0 or image_count > 0:
        return False
    if not compact:
        if quote_count > 0:
            cleaner_warnings.append("Quote-only post or no own body text detected")
        return True
    if quote_count > 0 and body_text_clean and not normalize_text(body_text_no_quotes):
        cleaner_warnings.append("Quote-only post or no own body text detected")
        return True
    if compact in ACKNOWLEDGEMENT_TEXTS:
        return True
    if _has_technical_signal(compact):
        return False
    if _emoji_or_symbol_only(compact):
        return True
    return False


def _has_technical_signal(text: str) -> bool:
    if re.search(r"\d", text):
        return True
    if TECHNICAL_UNIT_PATTERN.search(text):
        return True
    if COMPONENT_PATTERN.search(text):
        return True
    if re.search(r"\b(?:mosfet|transistor|diode|capacitor|resistor|opamp|op-amp|dac|amp|psu)\b", text):
        return True
    return False


def _emoji_or_symbol_only(text: str) -> bool:
    if not text:
        return False
    without_common_smiles = re.sub(r"[:;=8xX][-']?[)(DPpOo/\\|]+", "", text)
    without_symbols = re.sub(r"[\W_]+", "", without_common_smiles, flags=re.UNICODE)
    return without_symbols == ""


def _build_summary(posts: list[dict]) -> dict:
    return {
        "posts_cleaned": len(posts),
        "posts_with_quotes": sum(1 for post in posts if post.get("has_quotes")),
        "posts_with_links": sum(1 for post in posts if post.get("has_links")),
        "posts_with_images": sum(1 for post in posts if post.get("has_images")),
        "empty_or_noise_posts": sum(1 for post in posts if post.get("is_empty_or_noise")),
        "total_words": sum(int(post.get("word_count") or 0) for post in posts),
        "total_links": sum(int(post.get("link_count") or 0) for post in posts),
        "total_images": sum(int(post.get("image_count") or 0) for post in posts),
    }
