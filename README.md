# DIYAudio Thread Distiller

A small Python GUI tool for extracting, scoring, and reporting technically useful posts from large DIYAudio forum threads.
The goal is not to replace reading the original thread.

The goal is to help users find the posts that are most likely worth reading first.

## Source code status
The current beta is distributed as executable builds only.
Source code is not published at this stage while licensing, forum policy, responsible-use safeguards.
The goal is to ensure the tool is used responsibly, avoids unnecessary forum load, and remains aligned with the DIYAudio community.

## Status

Early MVP / beta. Linux and Windows avaiable 
Includes:
- DIYAudio thread fetcher
- post parser and cleaner
- rule-based technical scoring
- compact Markdown and HTML reports
- improved URL normalization
- improved unit/link scoring

Current scoring profile is electronics/amplifier-oriented

## DIYAudio community
For now I am keeping the beta executable-only while the responsible-use model, licensing, and possible forum-side contribution.
This is partly because large threads can involve many page requests, so I want to avoid encouraging uncontrolled scraping or forks with unsafe fetch behavior.
The tool is intended to help the DIYAudio community, so I want the next step to be aligned with the forum owner/moderators before deciding how the source and future versions should be handled.

## Responsible fetching
The tool is designed to fetch one thread at a time with a polite delay between page requests.
Large threads may take a long time to download. This is intentional.

Planned safeguards include:
- configurable but bounded request delay
- large-thread warning
- cache/resume behavior
- no parallel fetching
- abort on repeated HTTP errors
- honest User-Agent identification

# Changelog

## v0.2-beta

- Added compact HTML report
- Added robust DIYAudio URL normalization
- Added thread-starter context boost
- Added high-value non-OP report section
- Improved unit detection
- Improved internal link scoring
- Added Windows and Linux executable builds

## v0.1-mvp
- Initial fetch / parse / clean / score / report pipeline

## What it does
Given one DIYAudio thread URL, the tool can:

1. Fetch all thread pages politely and save the raw HTML locally.
2. Parse individual forum posts into structured JSON.
3. Clean and normalize post text.
4. Apply deterministic rule-based technical scoring.
5. Generate a human-readable technical report.

  report_manifest.json
