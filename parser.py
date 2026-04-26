"""Stage B parser for fetched DIYAudio/XenForo thread HTML."""

from __future__ import annotations

import copy
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.parse import urljoin

try:
    from bs4 import BeautifulSoup, NavigableString, Tag
except ImportError as exc:  # pragma: no cover - exercised when dependency is absent.
    BeautifulSoup = None  # type: ignore[assignment]
    NavigableString = str  # type: ignore[assignment,misc]
    Tag = object  # type: ignore[assignment,misc]
    _BS4_IMPORT_ERROR = exc
else:
    _BS4_IMPORT_ERROR = None


LogCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]

# DIYAudio currently runs XenForo. Keep all selectors here so the first live
# parsing test can tune them without disturbing the rest of the parser.
POST_SELECTORS = [
    "article.message--post",
    "article.message",
    "div.message--post",
]
BODY_SELECTORS = [
    ".message-body .bbWrapper",
    ".message-userContent .bbWrapper",
    ".message-content .bbWrapper",
    ".message-body",
    ".message-content",
]
AUTHOR_SELECTORS = [
    ".message-name a",
    ".message-name",
    "a.username",
    ".username",
]
DATE_SELECTORS = [
    "time.u-dt",
    "time",
    ".message-attribution-main time",
]
POST_NUMBER_SELECTORS = [
    "a[href*='#post-']",
    "a[href*='/posts/']",
    ".message-attribution-opposite a",
]
QUOTE_SELECTORS = [
    ".bbCodeBlock--quote",
    ".bbCodeBlock.bbCodeBlock--quote",
    "blockquote",
]


@dataclass(frozen=True)
class PageSource:
    page_number: int
    page_url: str
    html_path: Path


def parse_thread_folder(
    thread_folder: Path,
    *,
    log: LogCallback | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Parse fetched raw pages in a thread folder and return posts_raw.json."""
    if _BS4_IMPORT_ERROR is not None:
        raise RuntimeError(
            "BeautifulSoup is required for Stage B parsing. Install project "
            "dependencies into .venv with: .venv/bin/pip install -r requirements.txt"
        ) from _BS4_IMPORT_ERROR

    logger = log or (lambda message: None)
    reporter = progress or (lambda current, total: None)

    thread_folder = thread_folder.expanduser().resolve()
    manifest_path = thread_folder / "fetch_manifest.json"
    logger(f"Loading manifest: {manifest_path}")

    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing fetch_manifest.json in {thread_folder}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    normalized_base_url = manifest.get("normalized_base_url") or manifest.get(
        "original_thread_url", ""
    )
    page_sources = _page_sources_from_manifest(thread_folder, manifest)

    posts: list[dict] = []
    parser_warnings: list[str] = []

    for index, page_source in enumerate(page_sources, start=1):
        try:
            logger(f"Parsing page {page_source.page_number}: {page_source.html_path}")
            html = page_source.html_path.read_text(encoding="utf-8", errors="replace")
            page_posts = _parse_page(html, page_source, normalized_base_url)
            posts.extend(page_posts)
            logger(f"Found {len(page_posts)} post(s) on page {page_source.page_number}.")
        except Exception as exc:
            warning = f"Page {page_source.page_number} failed to parse: {exc}"
            parser_warnings.append(warning)
            logger(warning)
        reporter(index, len(page_sources))

    output = {
        "thread": {
            "original_thread_url": manifest.get("original_thread_url"),
            "normalized_base_url": manifest.get("normalized_base_url"),
            "detected_page_count": manifest.get("detected_page_count"),
            "fetched_page_count": manifest.get("fetched_page_count"),
        },
        "parser_timestamp": datetime.now(timezone.utc).isoformat(),
        "source_manifest_path": str(manifest_path),
        "total_post_count": len(posts),
        "parser_warnings": parser_warnings,
        "posts": posts,
    }

    output_path = thread_folder / "posts_raw.json"
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")
    logger(f"Total posts extracted: {len(posts)}")
    logger(f"Output written: {output_path}")
    return output_path


def _page_sources_from_manifest(thread_folder: Path, manifest: dict) -> list[PageSource]:
    page_sources: list[PageSource] = []
    for page in manifest.get("pages", []):
        fetch_status = page.get("fetch_status")
        if fetch_status not in {"fetched", "skipped_cached"}:
            continue

        output_path = Path(page.get("output_file_path", ""))
        if not output_path.is_absolute():
            output_path = thread_folder / output_path

        page_sources.append(
            PageSource(
                page_number=int(page.get("page_number", len(page_sources) + 1)),
                page_url=page.get("url") or manifest.get("normalized_base_url") or "",
                html_path=output_path,
            )
        )
    return page_sources


def _parse_page(
    html: str,
    page_source: PageSource,
    normalized_base_url: str,
) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    post_nodes = _select_posts(soup)
    posts: list[dict] = []

    for post_index, post_node in enumerate(post_nodes, start=1):
        posts.append(
            _parse_post(
                post_node,
                page_source=page_source,
                post_index_on_page=post_index,
                normalized_base_url=normalized_base_url,
            )
        )
    return posts


def _select_posts(soup: BeautifulSoup) -> list[Tag]:
    seen: set[int] = set()
    posts: list[Tag] = []
    for selector in POST_SELECTORS:
        for node in soup.select(selector):
            identity = id(node)
            if identity not in seen:
                seen.add(identity)
                posts.append(node)
    return posts


def _parse_post(
    post_node: Tag,
    *,
    page_source: PageSource,
    post_index_on_page: int,
    normalized_base_url: str,
) -> dict:
    warnings: list[str] = []
    body_node = _first_selected(post_node, BODY_SELECTORS) or post_node
    body_raw_html = str(body_node)

    post_id = _extract_post_id(post_node)
    post_url = _extract_post_url(post_node, page_source.page_url, normalized_base_url, post_id)
    post_number = _extract_post_number(post_node, post_url)
    author = _extract_author(post_node)
    date_info = _extract_date_info(post_node)
    quotes_text = _extract_quotes_text(body_node)
    body_text = _extract_body_text_without_quotes(body_node)
    links = _extract_links(body_node, page_source.page_url or normalized_base_url)
    images = _extract_images(body_node, page_source.page_url or normalized_base_url)

    for field_name, value in [
        ("post_id", post_id),
        ("post_number", post_number),
        ("post_url", post_url),
        ("author", author),
        ("date_iso", date_info["date_iso"]),
    ]:
        if not value:
            warnings.append(f"Missing {field_name}")

    return {
        "page_number": page_source.page_number,
        "post_index_on_page": post_index_on_page,
        "post_id": post_id,
        "post_number": post_number,
        "post_url": post_url,
        "author": author,
        "date_text_raw": date_info["date_text_raw"],
        "date_iso": date_info["date_iso"],
        "date_parse_status": date_info["date_parse_status"],
        "body_raw_html": body_raw_html,
        "body_text": body_text,
        "quotes_text": quotes_text,
        "links": links,
        "images": images,
        "source_page_path": str(page_source.html_path),
        "parser_warnings": warnings,
    }


def _first_selected(node: Tag, selectors: list[str]) -> Tag | None:
    for selector in selectors:
        selected = node.select_one(selector)
        if selected is not None:
            return selected
    return None


def _extract_post_id(post_node: Tag) -> str:
    for attr_name in ("data-content", "id"):
        attr_value = post_node.get(attr_name)
        if attr_value:
            attr_text = str(attr_value)
            match = re.search(r"post[-_]?(\d+)", attr_text, flags=re.IGNORECASE)
            if match:
                return match.group(1)
            return attr_text

    anchor = post_node.select_one("a[id^='post-'], a[name^='post-']")
    if anchor:
        anchor_id = anchor.get("id") or anchor.get("name") or ""
        match = re.search(r"post[-_]?(\d+)", str(anchor_id), flags=re.IGNORECASE)
        return match.group(1) if match else str(anchor_id)
    return ""


def _extract_post_url(
    post_node: Tag,
    page_url: str,
    normalized_base_url: str,
    post_id: str,
) -> str:
    if post_id:
        generated_url = urljoin(normalized_base_url, f"post-{post_id}")
        canonical_post_path = f"/post-{post_id}"

        for selector in POST_NUMBER_SELECTORS:
            for anchor in post_node.select(selector):
                href = str(anchor.get("href") or "")
                if not href or "/reactions" in href:
                    continue

                absolute_url = urljoin(page_url or normalized_base_url, href)
                if canonical_post_path in absolute_url:
                    return absolute_url

        return generated_url

    return page_url or normalized_base_url


def _extract_post_number(post_node: Tag, post_url: str) -> str:
    for selector in POST_NUMBER_SELECTORS:
        for anchor in post_node.select(selector):
            text = _clean_text(anchor.get_text(" ", strip=True))
            if text.startswith("#") or text.isdigit():
                return text

    match = re.search(r"#post-(\d+)", post_url)
    if match:
        return ""
    return ""


def _extract_author(post_node: Tag) -> str:
    data_author = post_node.get("data-author")
    if data_author:
        return _clean_text(str(data_author))

    author_node = _first_selected(post_node, AUTHOR_SELECTORS)
    if author_node:
        return _clean_text(author_node.get_text(" ", strip=True))
    return ""


def _extract_date_info(post_node: Tag) -> dict:
    date_node = _first_selected(post_node, DATE_SELECTORS)
    if not date_node:
        return {
            "date_text_raw": "",
            "date_iso": None,
            "date_parse_status": "missing",
        }

    title = date_node.get("title")
    datetime_value = date_node.get("datetime")
    text = _clean_text(date_node.get_text(" ", strip=True))
    raw_text = _clean_text(str(title or text or datetime_value or ""))

    if datetime_value:
        normalized = _normalize_datetime_value(str(datetime_value))
        if normalized:
            return {
                "date_text_raw": raw_text,
                "date_iso": normalized,
                "date_parse_status": "datetime_attr",
            }

    return {
        "date_text_raw": raw_text,
        "date_iso": None,
        "date_parse_status": "unparsed" if raw_text else "missing",
    }


def _normalize_datetime_value(value: str) -> str | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    parse_value = cleaned[:-1] + "+00:00" if cleaned.endswith("Z") else cleaned
    try:
        parsed = datetime.fromisoformat(parse_value)
    except ValueError:
        return cleaned

    return parsed.isoformat()


def _extract_quotes_text(body_node: Tag) -> list[str]:
    quotes: list[str] = []
    for quote_node in _quote_nodes(body_node):
        text = _clean_text(quote_node.get_text("\n", strip=True))
        if text:
            quotes.append(text)
    return quotes


def _extract_body_text_without_quotes(body_node: Tag) -> str:
    body_copy = copy.deepcopy(body_node)
    for quote_node in _quote_nodes(body_copy):
        quote_node.decompose()
    return _extract_readable_text(body_copy)


def _quote_nodes(body_node: Tag) -> list[Tag]:
    seen: set[int] = set()
    nodes: list[Tag] = []
    for selector in QUOTE_SELECTORS:
        for quote_node in body_node.select(selector):
            identity = id(quote_node)
            if identity not in seen:
                seen.add(identity)
                nodes.append(quote_node)
    return nodes


def _extract_links(body_node: Tag, base_url: str) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()
    for anchor in body_node.select("a[href]"):
        href = str(anchor.get("href", "")).strip()
        if not href or href.startswith(("javascript:", "mailto:")):
            continue
        absolute = urljoin(base_url, href)
        if absolute not in seen:
            seen.add(absolute)
            links.append(absolute)
    return links


def _extract_images(body_node: Tag, base_url: str) -> list[str]:
    images: list[str] = []
    seen: set[str] = set()
    for image in body_node.select("img"):
        src = image.get("data-src") or image.get("src")
        if not src:
            continue
        absolute = urljoin(base_url, str(src).strip())
        if absolute not in seen:
            seen.add(absolute)
            images.append(absolute)
    return images


def _extract_readable_text(node: Tag) -> str:
    """Extract text without turning inline formatting tags into line breaks."""
    node_copy = copy.deepcopy(node)
    for br in node_copy.select("br"):
        br.replace_with(NavigableString("\n"))

    block_tags = [
        "blockquote",
        "div",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "li",
        "ol",
        "p",
        "pre",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    ]
    for block in node_copy.find_all(block_tags):
        block.insert_before(NavigableString("\n"))
        block.insert_after(NavigableString("\n"))

    return _clean_text(node_copy.get_text(" ", strip=False))


def _clean_text(text: str) -> str:
    lines = [re.sub(r"\s+", " ", line).strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line)
