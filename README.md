# DIYAudio Thread Distiller

A small Python GUI tool for extracting, scoring, and reporting technically useful posts from large DIYAudio forum threads.

The goal is not to replace reading the original thread.

The goal is to help users find the posts that are most likely worth reading first.

## Status

Early MVP / beta. Linux and Windows avaiable 

certutil -hashfile DIYAudioThreadDistiller.exe SHA1 hash of DIYAudioThreadDistiller.exe:
d30d8007c1da9bc480a51bbce794f97fd9660677

The tool currently works for selected DIYAudio threads and is intended for testing and feedback.

## What it does

Given one DIYAudio thread URL, the tool can:

1. Fetch all thread pages politely and save the raw HTML locally.
2. Parse individual forum posts into structured JSON.
3. Clean and normalize post text.
4. Apply deterministic rule-based technical scoring.
5. Generate a human-readable technical report.

## Current output

For each processed thread, the tool creates files such as:

```text
data/threads/<thread-name>/
  raw/
    page_001.html
    page_002.html
    ...
  fetch_manifest.json
  posts_raw.json
  posts_clean.json
  posts_scored.json
  technical_report.md
  report_manifest.json
