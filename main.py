"""Entry point for the DIYAudio Thread Distiller app."""

from __future__ import annotations

import argparse
from pathlib import Path

from cleaner import clean_thread_posts
from gui import run_app
from parser import parse_thread_folder
from reporter import generate_technical_report
from scorer import score_thread_posts


def main() -> None:
    arg_parser = argparse.ArgumentParser(
        description="DIYAudio Thread Distiller fetch/parse tool."
    )
    actions = arg_parser.add_mutually_exclusive_group()
    actions.add_argument(
        "--parse",
        metavar="THREAD_FOLDER",
        help="Parse a fetched thread folder and write posts_raw.json.",
    )
    actions.add_argument(
        "--clean",
        metavar="THREAD_FOLDER",
        help="Clean parsed posts in a thread folder and write posts_clean.json.",
    )
    actions.add_argument(
        "--score",
        metavar="THREAD_FOLDER",
        help="Score cleaned posts in a thread folder and write posts_scored.json.",
    )
    actions.add_argument(
        "--report",
        metavar="THREAD_FOLDER",
        help="Generate a Markdown technical report from posts_scored.json.",
    )
    arg_parser.add_argument(
        "--report-top-n",
        metavar="COUNT",
        type=int,
        default=50,
        help="Number of ranked posts to include in --report output. Default: 50.",
    )
    args = arg_parser.parse_args()

    if args.parse:
        output_path = parse_thread_folder(Path(args.parse), log=print)
        print(f"Posts written: {output_path}")
        return

    if args.clean:
        output_path = clean_thread_posts(Path(args.clean), log=print)
        print(f"Clean posts written: {output_path}")
        return

    if args.score:
        output_path = score_thread_posts(Path(args.score), log=print)
        print(f"Scored posts written: {output_path}")
        return

    if args.report:
        output_path = generate_technical_report(
            Path(args.report),
            top_n=args.report_top_n,
            log=print,
        )
        print(f"Technical report written: {output_path}")
        return

    run_app()


if __name__ == "__main__":
    main()
