"""Stage D deterministic technical scoring for cleaned forum posts."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


LogCallback = Callable[[str], None]
ProgressCallback = Callable[[int, int], None]

DEFAULT_SCORING_PROFILE = "electronics_amplifier_basic"

MARKER_KEYS = [
    "has_numbers",
    "has_units",
    "has_component_refs",
    "has_measurement_terms",
    "has_fault_terms",
    "has_build_terms",
    "has_schematic_terms",
    "has_audio_terms",
    "has_power_terms",
]

NUMBER_PATTERN = re.compile(
    r"(?<![A-Za-z])[-+]?\d{1,3}(?:,\d{3})*(?:\.\d+)?(?:\s*[A-Za-z]+)?\b|"
    r"(?<![A-Za-z])[-+]?\d+(?:\.\d+)?(?:\s*[A-Za-z]+)?\b"
)
UNIT_PATTERN = re.compile(
    r"(?:[-+]?\d[\d,]*(?:\.\d+)?\s*(?:mV|uA|µA|mA|mW|kΩ|uF|µF|nF|pF|"
    r"kHz|MHz|dBV|dBu|ohms?|Hz|dB|V|A|W|Ω|R|%|°C|C)(?=$|[^A-Za-z0-9_]))|"
    r"(?:\b(?:mV|uA|µA|mA|mW|kΩ|uF|µF|nF|pF|kHz|MHz|dBV|dBu|THD|"
    r"ohms?|Hz|dB|V|A|W|Ω|R|%|°C|C)\b)",
    flags=re.IGNORECASE,
)
COMPONENT_REF_PATTERN = re.compile(
    r"\b(?:R|C|Q|D|U|IC|TR|VR|F|L|T)\d+\b",
    flags=re.IGNORECASE,
)

MEASUREMENT_PATTERN = re.compile(
    r"\b(?:measured|measurement|meter|DMM|scope|oscilloscope|FFT|THD|distortion|"
    r"voltage|current|resistance|impedance|continuity|reading|rails?)\b",
    flags=re.IGNORECASE,
)
FAULT_PATTERN = re.compile(
    r"\b(?:short|shorted|blown|failed|failure|fault|thermal|hot|overheating|"
    r"oscillation|unstable|smoke|burned|burnt|exploded|damage)\b",
    flags=re.IGNORECASE,
)
BUILD_PATTERN = re.compile(
    r"\b(?:built|rebuild|replaced|swapped|soldered|populated|assembled|board|PCB|"
    r"capacitor|resistor|transistor|diode|heatsink|transformer|supply)\b",
    flags=re.IGNORECASE,
)
SCHEMATIC_PATTERN = re.compile(
    r"\b(?:schematic|circuit|topology|layout|trace|net|rail|ground|feedback|"
    r"compensation|bias|input stage|output stage)\b",
    flags=re.IGNORECASE,
)
AUDIO_PATTERN = re.compile(
    r"\b(?:amplifier|amp|speaker|subwoofer|driver|frequency response|FR|crossover|"
    r"gain|noise|hum|channel|stereo|mono)\b",
    flags=re.IGNORECASE,
)
POWER_PATTERN = re.compile(
    r"\b(?:power supply|transformer|toroidal|EI|rail|fuse|mains|rectifier|"
    r"capacitor bank|voltage rail)\b",
    flags=re.IGNORECASE,
)
SOCIAL_PATTERN = re.compile(
    r"\b(?:thanks|thank you|nice work|looks good|great|following)\b",
    flags=re.IGNORECASE,
)
WORD_PATTERN = re.compile(r"\b[\w.+#/-]+\b", flags=re.UNICODE)

SCORING_PROFILES = {
    "electronics_amplifier_basic": {
        "display_name": "Electronics / amplifier-basic",
        "marker_patterns": {
            "has_numbers": NUMBER_PATTERN,
            "has_units": UNIT_PATTERN,
            "has_component_refs": COMPONENT_REF_PATTERN,
            "has_measurement_terms": MEASUREMENT_PATTERN,
            "has_fault_terms": FAULT_PATTERN,
            "has_build_terms": BUILD_PATTERN,
            "has_schematic_terms": SCHEMATIC_PATTERN,
            "has_audio_terms": AUDIO_PATTERN,
            "has_power_terms": POWER_PATTERN,
        },
        "positive_scoring_rules": [
            ("has_numbers", 8, "contains numeric values"),
            ("has_units", 10, "contains electronics/audio units"),
            ("has_component_refs", 12, "contains component references"),
            ("has_measurement_terms", 12, "contains measurement terms"),
            ("has_fault_terms", 10, "contains fault/debugging terms"),
            ("has_build_terms", 8, "contains build/rebuild terms"),
            ("has_schematic_terms", 8, "contains schematic/circuit terms"),
            ("has_audio_terms", 5, "contains audio terms"),
            ("has_power_terms", 8, "contains power supply terms"),
        ],
        "social_pattern": SOCIAL_PATTERN,
    }
}


def score_thread_posts(
    thread_folder: Path,
    *,
    log: LogCallback | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Score posts_clean.json in a thread folder and return posts_scored.json."""
    logger = log or (lambda message: None)
    reporter = progress or (lambda current, total: None)

    thread_folder = thread_folder.expanduser().resolve()
    source_path = thread_folder / "posts_clean.json"
    logger(f"Loading posts_clean.json: {source_path}")

    if not source_path.exists():
        raise FileNotFoundError(f"Missing posts_clean.json in {thread_folder}")

    clean_data = json.loads(source_path.read_text(encoding="utf-8"))
    clean_posts = clean_data.get("posts", [])
    if not isinstance(clean_posts, list):
        raise ValueError("posts_clean.json has no posts list.")

    profile = SCORING_PROFILES[DEFAULT_SCORING_PROFILE]
    logger(f"Scoring profile: {profile['display_name']} ({DEFAULT_SCORING_PROFILE})")
    logger(f"Total posts loaded: {len(clean_posts)}")

    thread_starter_author = _thread_starter_author(clean_posts)
    logger(f"Thread starter author: {thread_starter_author or '(unknown)'}")

    scored_posts: list[dict] = []
    for index, clean_post in enumerate(clean_posts, start=1):
        if isinstance(clean_post, dict):
            scored_posts.append(
                _score_post(
                    clean_post,
                    profile=profile,
                    thread_starter_author=thread_starter_author,
                )
            )
        else:
            scored_posts.append(_score_invalid_post(clean_post))

        if index == 1 or index == len(clean_posts) or index % 25 == 0:
            logger(f"Scoring progress: {index}/{len(clean_posts)}")
        reporter(index, len(clean_posts))

    summary = _build_summary(scored_posts)
    output = {
        "thread_metadata": clean_data.get("thread_metadata") or clean_data.get("thread") or {},
        "source_file": "posts_clean.json",
        "scorer_profile": DEFAULT_SCORING_PROFILE,
        "scorer_profile_display_name": profile["display_name"],
        "thread_starter_author": thread_starter_author,
        "scorer_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_post_count": len(scored_posts),
        "posts": scored_posts,
        "scorer_summary": summary,
    }

    output_path = thread_folder / "posts_scored.json"
    output_path.write_text(json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8")

    logger(f"Total posts scored: {summary['posts_scored']}")
    logger("Top 5 posts by score:")
    top_posts = sorted(
        scored_posts,
        key=lambda item: item.get("technical_score", 0),
        reverse=True,
    )[:5]
    for post in top_posts:
        logger(
            "  "
            f"{post.get('post_number') or '(no post number)'} | "
            f"{post.get('author') or '(unknown author)'} | "
            f"score {post.get('technical_score', 0)}"
        )
    logger(f"Output written: {output_path}")
    return output_path


def _score_post(post: dict, *, profile: dict, thread_starter_author: str) -> dict:
    scored = dict(post)
    text = _scoring_text(post)
    word_count = _safe_int(post.get("word_count"), _word_count(text))
    is_thread_starter_author = _same_author(post.get("author"), thread_starter_author)

    markers = {
        marker_key: bool(pattern.search(text))
        for marker_key, pattern in profile["marker_patterns"].items()
    }

    base_score, reasons = _calculate_score(
        profile=profile,
        markers=markers,
        word_count=word_count,
        has_images=_truthy_count_or_flag(post, "images", "image_count", "has_images"),
        has_links=_truthy_count_or_flag(post, "links", "link_count", "has_links"),
        is_empty_or_noise=bool(post.get("is_empty_or_noise")),
        text=text,
    )
    author_context_boost, author_reasons = _author_context_boost(
        is_thread_starter_author=is_thread_starter_author,
        is_empty_or_noise=bool(post.get("is_empty_or_noise")),
        base_technical_score=base_score,
    )
    final_score = max(0, min(100, base_score + author_context_boost))

    scored["technical_markers"] = markers
    scored["is_thread_starter_author"] = is_thread_starter_author
    scored["base_technical_score"] = base_score
    scored["author_context_boost"] = author_context_boost
    scored["technical_score"] = final_score
    scored["score_reasons"] = reasons + author_reasons
    return scored


def _score_invalid_post(post: object) -> dict:
    text = str(post)
    markers = {key: False for key in MARKER_KEYS}
    return {
        "body_text": text,
        "technical_markers": markers,
        "is_thread_starter_author": False,
        "base_technical_score": 0,
        "author_context_boost": 0,
        "technical_score": 0,
        "score_reasons": ["post was not an object"],
    }


def _thread_starter_author(posts: list) -> str:
    for post in posts:
        if isinstance(post, dict):
            author = post.get("author")
            if isinstance(author, str):
                return author.strip()
            if author is not None:
                return str(author).strip()
            return ""
    return ""


def _same_author(author: object, thread_starter_author: str) -> bool:
    if not thread_starter_author:
        return False
    if not isinstance(author, str):
        author = "" if author is None else str(author)
    return author.strip().casefold() == thread_starter_author.strip().casefold()


def _author_context_boost(
    *,
    is_thread_starter_author: bool,
    is_empty_or_noise: bool,
    base_technical_score: int,
) -> tuple[int, list[str]]:
    if not is_thread_starter_author:
        return 0, []

    boost = 0
    reasons: list[str] = []
    if not is_empty_or_noise:
        boost += 8
        reasons.append("author is thread starter")
    if base_technical_score >= 25:
        boost += 5
        reasons.append("thread starter post contains technical content")
    return min(boost, 13), reasons


def _scoring_text(post: dict) -> str:
    for field in ("body_text_no_quotes", "body_text_clean", "body_text"):
        value = post.get(field)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _calculate_score(
    *,
    profile: dict,
    markers: dict[str, bool],
    word_count: int,
    has_images: bool,
    has_links: bool,
    is_empty_or_noise: bool,
    text: str,
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []

    for marker_key, points, reason in profile["positive_scoring_rules"]:
        if markers[marker_key]:
            score += points
            reasons.append(reason)

    if has_images:
        score += 5
        reasons.append("contains images")
    if has_links:
        score += 5
        reasons.append("contains links")
    if word_count >= 50:
        score += 3
        reasons.append("word count >= 50")
    if word_count >= 150:
        score += 5
        reasons.append("word count >= 150")

    any_marker = any(markers.values())
    if is_empty_or_noise:
        score -= 30
        reasons.append("empty/noise post penalty")
    if word_count < 10 and not any_marker:
        score -= 10
        reasons.append("short low-content post penalty")
    if _mostly_social_praise(text, word_count, social_pattern=profile["social_pattern"]):
        score -= 8
        reasons.append("mostly social/praise-only post penalty")

    return max(0, min(100, score)), reasons


def _truthy_count_or_flag(
    post: dict,
    list_field: str,
    count_field: str,
    flag_field: str,
) -> bool:
    value = post.get(list_field)
    if isinstance(value, list) and value:
        return True
    return _safe_int(post.get(count_field), 0) > 0 or bool(post.get(flag_field))


def _mostly_social_praise(text: str, word_count: int, *, social_pattern: re.Pattern) -> bool:
    compact = re.sub(r"\s+", " ", text).strip()
    if not compact or word_count > 8:
        return False
    if not social_pattern.search(compact):
        return False

    remainder = social_pattern.sub(" ", compact)
    remainder_words = [word.lower() for word in WORD_PATTERN.findall(remainder)]
    generic_words = {
        "all",
        "and",
        "for",
        "it",
        "man",
        "mate",
        "so",
        "the",
        "this",
        "very",
        "you",
    }
    return all(word in generic_words for word in remainder_words)


def _word_count(text: str) -> int:
    return len(WORD_PATTERN.findall(text))


def _safe_int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_summary(posts: list[dict]) -> dict:
    scores = [_safe_int(post.get("technical_score"), 0) for post in posts]
    post_count = len(posts)
    marker_count = lambda key: sum(
        1 for post in posts if post.get("technical_markers", {}).get(key)
    )
    posts_with_images = sum(
        1 for post in posts if _truthy_count_or_flag(post, "images", "image_count", "has_images")
    )
    posts_with_links = sum(
        1 for post in posts if _truthy_count_or_flag(post, "links", "link_count", "has_links")
    )
    return {
        "posts_scored": post_count,
        "average_score": round(sum(scores) / post_count, 2) if post_count else 0,
        "max_score": max(scores) if scores else 0,
        "min_score": min(scores) if scores else 0,
        "posts_with_score_above_50": sum(1 for score in scores if score > 50),
        "posts_with_score_above_30": sum(1 for score in scores if score > 30),
        "posts_with_numbers": marker_count("has_numbers"),
        "posts_with_units": marker_count("has_units"),
        "posts_with_component_refs": marker_count("has_component_refs"),
        "posts_with_measurement_terms": marker_count("has_measurement_terms"),
        "posts_with_fault_terms": marker_count("has_fault_terms"),
        "posts_with_images": posts_with_images,
        "posts_with_links": posts_with_links,
    }
