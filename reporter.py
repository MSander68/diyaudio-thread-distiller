"""Stage E deterministic Markdown report generation for scored forum posts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Callable


LogCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]

TOP_N_GLOBAL = 50
TOP_N_NON_OP = 25
TOP_N_OP = 25
TOP_N_CATEGORY = 20
EXCERPT_LONG_CHARS = 500
EXCERPT_SHORT_CHARS = 220

REPORT_SECTIONS = [
    "opening_post",
    "top_technical_posts",
    "high_value_non_op_posts",
    "thread_starter_posts",
    "measurement_relevant_posts",
    "build_schematic_relevant_posts",
    "fault_debug_relevant_posts",
    "score_distribution",
    "low_content_noise_posts",
]


def generate_technical_report(
    thread_folder: Path,
    top_n: int = TOP_N_GLOBAL,
    *,
    log: LogCallback | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Generate technical_report.md from posts_scored.json and return its path."""
    logger = log or (lambda message: None)
    reporter = progress or (lambda current, total: None)

    thread_folder = thread_folder.expanduser().resolve()
    source_path = thread_folder / "posts_scored.json"
    logger(f"Loading posts_scored.json: {source_path}")

    if not source_path.exists():
        raise FileNotFoundError(f"Missing posts_scored.json in {thread_folder}")

    scored_data = json.loads(source_path.read_text(encoding="utf-8"))
    posts = scored_data.get("posts", [])
    if not isinstance(posts, list):
        raise ValueError("posts_scored.json has no posts list.")

    top_n = max(1, int(top_n))
    logger(f"Total posts loaded: {len(posts)}")
    logger(f"Top N value: {top_n}")
    reporter(1, 4)

    scored_posts = [post for post in posts if isinstance(post, dict)]
    ranked_posts = sorted(
        enumerate(scored_posts),
        key=lambda item: _safe_int(item[1].get("technical_score"), 0),
        reverse=True,
    )
    reporter(2, 4)

    generated_timestamp = datetime.now(timezone.utc).isoformat()
    summary = _summary_from_data(scored_data, scored_posts)
    thread_starter_author = _thread_starter_author(scored_data, scored_posts)
    report_text = _build_markdown_report(
        scored_data=scored_data,
        opening_post=scored_posts[0] if scored_posts else None,
        ranked_posts=[post for _, post in ranked_posts],
        thread_starter_author=thread_starter_author,
        summary=summary,
        top_n=top_n,
        generated_timestamp=generated_timestamp,
        log=logger,
    )
    reporter(3, 4)

    report_path = thread_folder / "technical_report.md"
    report_path.write_text(report_text, encoding="utf-8")

    manifest_path = _write_manifest(
        thread_folder=thread_folder,
        generated_timestamp=generated_timestamp,
        top_n=top_n,
        total_posts=len(scored_posts),
        scoring_profile=_scoring_profile(scored_data),
        summary=summary,
    )
    reporter(4, 4)

    logger(f"Report generated: {report_path}")
    logger(f"Report manifest written: {manifest_path}")
    return report_path


def _build_markdown_report(
    *,
    scored_data: dict,
    opening_post: dict | None,
    ranked_posts: list[dict],
    thread_starter_author: str,
    summary: dict,
    top_n: int,
    generated_timestamp: str,
    log: LogCallback,
) -> str:
    thread_metadata = scored_data.get("thread_metadata") or scored_data.get("thread") or {}
    if not isinstance(thread_metadata, dict):
        thread_metadata = {}

    selected_posts = ranked_posts[:top_n]
    non_op_posts = [
        post
        for post in ranked_posts
        if not _is_thread_starter_post(post, thread_starter_author)
    ][:TOP_N_NON_OP]
    op_posts = [
        post
        for post in ranked_posts
        if _is_thread_starter_post(post, thread_starter_author)
    ][:TOP_N_OP]

    lines = [
        "# DIYAudio Thread Technical Report",
        "",
        "## Thread",
        f"- Title or thread URL: {_thread_title_or_url(thread_metadata)}",
        f"- Original URL: {_md_text(thread_metadata.get('original_thread_url') or '')}",
        f"- Normalized base URL: {_md_text(thread_metadata.get('normalized_base_url') or '')}",
        f"- Detected pages: {_md_text(thread_metadata.get('detected_page_count', ''))}",
        f"- Total posts: {_md_text(scored_data.get('total_post_count') or len(ranked_posts))}",
        f"- Scoring profile: {_md_text(_scoring_profile(scored_data))}",
        f"- Report generated timestamp: {_md_text(generated_timestamp)}",
        "",
        "## Scoring note",
        "This report is generated from deterministic rule-based scoring.",
        "It ranks posts by likely technical density.",
        "It does not verify correctness and does not replace the original thread.",
        "Posts by the thread starter receive a small context boost because the OP often "
        "provides design intent, corrections, and project status. This does not verify "
        "correctness and does not force OP posts to the top.",
        "",
        "## Table of Contents",
        "- [Thread Starter / Opening Post](#thread-starter--opening-post)",
        "- [Top Technical Posts](#top-technical-posts)",
        "- [High-Value Non-OP Posts](#high-value-non-op-posts)",
        "- [Thread Starter Posts](#thread-starter-posts)",
        "- [Measurement-Relevant Posts](#measurement-relevant-posts)",
        "- [Build / Schematic-Relevant Posts](#build--schematic-relevant-posts)",
        "- [Fault / Debug-Relevant Posts](#fault--debug-relevant-posts)",
        "- [Score Distribution](#score-distribution)",
        "- [Low-Content / Noise Posts](#low-content--noise-posts)",
        "",
        "## Score overview",
        f"- Total posts: {_md_text(summary['total_posts'])}",
        f"- Average score: {_md_text(summary['average_score'])}",
        f"- Min score: {_md_text(summary['min_score'])}",
        f"- Max score: {_md_text(summary['max_score'])}",
        f"- Posts above 50: {_md_text(summary['posts_with_score_above_50'])}",
        f"- Posts above 30: {_md_text(summary['posts_with_score_above_30'])}",
        "",
    ]

    lines.extend(_opening_post_section(opening_post))
    lines.extend(["## Top Technical Posts", ""])

    for rank, post in enumerate(selected_posts, start=1):
        if not post.get("post_url"):
            log(f"Warning: missing post_url for post {post.get('post_number') or rank}")
        lines.extend(_top_post_lines(rank, post))

    lines.extend(_compact_posts_section(
        heading="High-Value Non-OP Posts",
        posts=non_op_posts,
        include_boost=False,
        note=(
            "These posts are shown separately so strong contributor posts are not buried "
            "when the thread starter is very active."
        ),
    ))
    lines.extend(_compact_posts_section(
        heading="Thread Starter Posts",
        posts=op_posts,
        include_boost=True,
    ))
    lines.extend(
        _marker_section(
            heading="Measurement-Relevant Posts",
            posts=ranked_posts,
            predicate=lambda post: _marker(post, "has_measurement_terms"),
        )
    )
    lines.extend(
        _marker_section(
            heading="Build / Schematic-Relevant Posts",
            posts=ranked_posts,
            predicate=lambda post: _marker(post, "has_schematic_terms")
            or _marker(post, "has_build_terms"),
        )
    )
    lines.extend(
        _marker_section(
            heading="Fault / Debug-Relevant Posts",
            posts=ranked_posts,
            predicate=lambda post: _marker(post, "has_fault_terms"),
        )
    )
    lines.extend(_score_distribution_section(ranked_posts))

    noise_count = sum(1 for post in ranked_posts if post.get("is_empty_or_noise"))
    lines.extend(
        [
            "## Low-Content / Noise Posts",
            "",
            f"- Number of posts marked empty_or_noise: {noise_count}",
            "",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _opening_post_section(post: dict | None) -> list[str]:
    lines = ["## Thread Starter / Opening Post", ""]
    if post is None:
        lines.extend(["- No opening post found.", ""])
        return lines

    reasons = post.get("score_reasons")
    if not isinstance(reasons, list):
        reasons = []

    lines.extend(
        [
            f"- Post number: {_md_text(post.get('post_number') or '(unknown post)')}",
            f"- Author: {_md_text(post.get('author') or '')}",
            f"- Date: {_md_text(post.get('date_iso') or post.get('date_text_raw') or '')}",
            f"- Final score: {_final_score(post)}",
            f"- Base technical score: {_base_score(post)}",
            f"- Author/context boost: {_author_context_boost(post)}",
            f"- Link: {_link_text(post.get('post_url'))}",
            "- Score reasons:",
        ]
    )
    if reasons:
        lines.extend(f"  - {_md_text(reason)}" for reason in reasons)
    else:
        lines.append("  - none recorded")

    lines.extend(["", "Excerpt:", f"> {_md_text(_excerpt(post, limit=700))}", ""])
    return lines


def _top_post_lines(rank: int, post: dict) -> list[str]:
    post_number = post.get("post_number") or "(unknown post)"
    score = _final_score(post)
    boost = _author_context_boost(post)
    reasons = post.get("score_reasons")
    if not isinstance(reasons, list):
        reasons = []

    lines = [
        f"### Rank {rank} - Post {_md_text(post_number)} - Final Score {score}",
        f"- Author: {_md_text(post.get('author') or '')}",
        f"- Date: {_md_text(post.get('date_iso') or post.get('date_text_raw') or '')}",
        f"- Base technical score: {_base_score(post)}",
        f"- Link: {_link_text(post.get('post_url'))}",
        f"- Reasons: {_compact_reasons(reasons)}",
    ]
    if boost:
        lines.insert(4, f"- Author/context boost: {boost}")

    excerpt = _excerpt(post, limit=EXCERPT_LONG_CHARS)
    lines.extend(["", "Excerpt:", f"> {_md_text(excerpt)}", ""])
    return lines


def _compact_posts_section(
    *,
    heading: str,
    posts: list[dict],
    include_boost: bool,
    note: str | None = None,
) -> list[str]:
    lines = [f"## {heading}", ""]
    if note:
        lines.extend([note, ""])
    if not posts:
        lines.extend(["- None", ""])
        return lines

    for post in posts:
        parts = [
            _md_text(post.get("post_number") or "(unknown post)"),
            f"Final {_final_score(post)}",
            f"Base {_base_score(post)}",
        ]
        if include_boost:
            parts.append(f"Boost {_author_context_boost(post)}")
        parts.extend(
            [
                _md_text(post.get("author") or ""),
                _link_text(post.get("post_url")),
                _md_text(_excerpt(post, limit=EXCERPT_SHORT_CHARS)),
                f"Reasons: {_compact_reasons(post.get('score_reasons'))}",
            ]
        )
        lines.append("- " + " | ".join(parts))
    lines.append("")
    return lines


def _marker_section(
    *,
    heading: str,
    posts: list[dict],
    predicate: Callable[[dict], bool],
) -> list[str]:
    lines = [f"## {heading}", ""]
    selected = [post for post in posts if predicate(post)][:TOP_N_CATEGORY]
    if not selected:
        lines.extend(["- None", ""])
        return lines

    for post in selected:
        lines.append(
            "- "
            f"{_md_text(post.get('post_number') or '(unknown post)')} | "
            f"Score {_final_score(post)} | "
            f"{_md_text(post.get('author') or '')} | "
            f"{_link_text(post.get('post_url'))} | "
            f"{_md_text(_excerpt(post, limit=EXCERPT_SHORT_CHARS))}"
        )
    lines.append("")
    return lines


def _score_distribution_section(posts: list[dict]) -> list[str]:
    buckets = [
        ("90\u2013100", 90, 100),
        ("80\u201389", 80, 89),
        ("70\u201379", 70, 79),
        ("60\u201369", 60, 69),
        ("50\u201359", 50, 59),
        ("40\u201349", 40, 49),
        ("30\u201339", 30, 39),
        ("20\u201329", 20, 29),
        ("10\u201319", 10, 19),
        ("0\u20139", 0, 9),
    ]
    lines = [
        "## Score Distribution",
        "",
        "| Score range | Post count |",
        "|---|---:|",
    ]
    for label, minimum, maximum in buckets:
        count = sum(1 for post in posts if minimum <= _final_score(post) <= maximum)
        lines.append(f"| {label} | {count} |")
    lines.append("")
    return lines


def _summary_from_data(scored_data: dict, posts: list[dict]) -> dict:
    scorer_summary = scored_data.get("scorer_summary")
    scores = [_safe_int(post.get("technical_score"), 0) for post in posts]
    total_posts = len(posts)

    computed = {
        "total_posts": total_posts,
        "average_score": round(sum(scores) / total_posts, 2) if total_posts else 0,
        "min_score": min(scores) if scores else 0,
        "max_score": max(scores) if scores else 0,
        "posts_with_score_above_50": sum(1 for score in scores if score > 50),
        "posts_with_score_above_30": sum(1 for score in scores if score > 30),
    }

    if not isinstance(scorer_summary, dict):
        return computed

    return {
        "total_posts": scorer_summary.get("posts_scored", computed["total_posts"]),
        "average_score": scorer_summary.get("average_score", computed["average_score"]),
        "min_score": scorer_summary.get("min_score", computed["min_score"]),
        "max_score": scorer_summary.get("max_score", computed["max_score"]),
        "posts_with_score_above_50": scorer_summary.get(
            "posts_with_score_above_50",
            computed["posts_with_score_above_50"],
        ),
        "posts_with_score_above_30": scorer_summary.get(
            "posts_with_score_above_30",
            computed["posts_with_score_above_30"],
        ),
    }


def _write_manifest(
    *,
    thread_folder: Path,
    generated_timestamp: str,
    top_n: int,
    total_posts: int,
    scoring_profile: str,
    summary: dict,
) -> Path:
    manifest = {
        "source_file": "posts_scored.json",
        "report_file": "technical_report.md",
        "html_report_file": None,
        "generated_timestamp": generated_timestamp,
        "top_n": top_n,
        "top_n_global": top_n,
        "top_n_non_op": TOP_N_NON_OP,
        "top_n_op": TOP_N_OP,
        "top_n_category": TOP_N_CATEGORY,
        "total_posts": total_posts,
        "scoring_profile": scoring_profile,
        "max_score": summary["max_score"],
        "average_score": summary["average_score"],
        "report_sections": REPORT_SECTIONS,
    }
    manifest_path = thread_folder / "report_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def _thread_title_or_url(thread_metadata: dict) -> str:
    for key in ("title", "thread_title", "normalized_base_url", "original_thread_url"):
        value = thread_metadata.get(key)
        if value:
            return _md_text(value)
    return ""


def _scoring_profile(scored_data: dict) -> str:
    return (
        scored_data.get("scorer_profile_display_name")
        or scored_data.get("scorer_profile")
        or ""
    )


def _marker(post: dict, key: str) -> bool:
    markers = post.get("technical_markers")
    return isinstance(markers, dict) and bool(markers.get(key))


def _thread_starter_author(scored_data: dict, posts: list[dict]) -> str:
    value = scored_data.get("thread_starter_author")
    if isinstance(value, str) and value.strip():
        return value.strip()
    if posts:
        author = posts[0].get("author")
        if isinstance(author, str):
            return author.strip()
        if author is not None:
            return str(author).strip()
    return ""


def _is_thread_starter_post(post: dict, thread_starter_author: str) -> bool:
    value = post.get("is_thread_starter_author")
    if isinstance(value, bool):
        return value
    if not thread_starter_author:
        return False
    author = post.get("author")
    if not isinstance(author, str):
        author = "" if author is None else str(author)
    return author.strip().casefold() == thread_starter_author.casefold()


def _final_score(post: dict) -> int:
    return _safe_int(post.get("technical_score"), 0)


def _base_score(post: dict) -> int:
    return _safe_int(post.get("base_technical_score"), _final_score(post))


def _author_context_boost(post: dict) -> int:
    return _safe_int(post.get("author_context_boost"), 0)


def _compact_reasons(reasons: object) -> str:
    if not isinstance(reasons, list) or not reasons:
        return "none recorded"
    return ", ".join(_md_text(reason) for reason in reasons)


def _excerpt(post: dict, *, limit: int) -> str:
    for field in ("body_text_no_quotes", "body_text_clean", "body_text"):
        value = post.get(field)
        if isinstance(value, str) and value.strip():
            text = re.sub(r"\s+", " ", value).strip()
            break
    else:
        text = ""

    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def _link_text(url: object) -> str:
    if isinstance(url, str) and url.strip():
        return f"<{escape(url.strip(), quote=False)}>"
    return "(missing link)"


def _md_text(value: object) -> str:
    return escape(str(value), quote=False)


def _safe_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
