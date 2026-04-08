#!/usr/bin/env python3
"""
Preprocessing pipeline for FinLLM-India news corpus (Economic Times).

INPUT  : data/raw/news/*.txt  (+ matching .json sidecars)
OUTPUT : data/processed/news/*.txt  (+ matching .json sidecars)
         data/processed/news/skipped.log
         data/processed/news/duplicates.log
         data/processed/news/preprocessing_report_news.json

Steps per document:
  1. Date filter  — drop if publish date outside 2019-01-01 – 2024-12-31
  2. Clean        — strip ET boilerplate footers, encoding artifacts, short lines
  3. Dedup paras  — ET HTML duplicates paragraphs; keep first occurrence
  4. Quality      — skip if < MIN_WORDS after cleaning
  5. Deduplicate  — same URL hash → already handled by scraper; same title+date → keep longest
"""

import json
import re
import shutil
from datetime import date
from pathlib import Path

from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
IN_DIR   = BASE_DIR / "data" / "raw" / "news"
OUT_DIR  = BASE_DIR / "data" / "processed" / "news"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SKIP_LOG    = OUT_DIR / "skipped.log"
DUP_LOG     = OUT_DIR / "duplicates.log"
REPORT_PATH = OUT_DIR / "preprocessing_report_news.json"

# ── Constants ─────────────────────────────────────────────────────────────────
START_DATE   = date(2019, 1, 1)
END_DATE     = date(2024, 12, 31)
MIN_WORDS    = 150

# ET boilerplate patterns — matched as substrings of stripped lines (lowercase)
ET_BOILERPLATE = [
    "catch all the business news",
    "subscribe to the economic times prime",
    "read the et epaper online",
    "read more news on",
    "originally published on",
    "download the economic times",
    "follow us on",
    "click here to",
    "also read:",
    "also read :",
    "et now",
    "watch live",
    "join the community",
    "for all latest",
    "get live share market",
    "disclaimer:",
    "disclaimer :",
    "(this story has been published",
    "this article has been published",
]

# Encoding artifact patterns produced by mojibake of curly quotes / em-dashes
_ARTIFACT = re.compile(r"[\ufffd\u0093\u0094\u0096\u0097\u0092\u0091]|\\ufffd|���+")

# Pure numeric line
_PURE_NUMERIC = re.compile(r"^\s*[\d\s,.\-]+\s*$")


def _is_boilerplate(line: str) -> bool:
    ll = line.lower()
    return any(bp in ll for bp in ET_BOILERPLATE)


def _parse_date(s: str) -> date | None:
    if not s:
        return None
    try:
        parts = s[:10].split("-")
        return date(int(parts[0]), int(parts[1]), int(parts[2]))
    except (ValueError, IndexError):
        return None


def clean_text(raw: str) -> str:
    # Fix encoding artifacts first
    text = _ARTIFACT.sub("'", raw)

    lines = text.splitlines()
    seen_paras: set[str] = set()
    cleaned: list[str] = []

    for line in lines:
        line = line.strip()

        # Drop boilerplate
        if _is_boilerplate(line):
            continue

        # Drop pure numeric
        if _PURE_NUMERIC.match(line) and line:
            continue

        # Blank line — keep (for paragraph spacing) but don't dedup
        if not line:
            cleaned.append(line)
            continue

        # Deduplicate paragraphs (ET repeats them verbatim)
        norm = re.sub(r"\s+", " ", line).lower()
        if norm in seen_paras:
            continue
        seen_paras.add(norm)

        cleaned.append(line)

    # Collapse 3+ blank lines → one
    result: list[str] = []
    blank_run = 0
    for line in cleaned:
        if line == "":
            blank_run += 1
            if blank_run <= 1:
                result.append(line)
        else:
            blank_run = 0
            result.append(line)

    return "\n".join(result).strip()


def _dedup_key(meta: dict) -> str:
    title_norm = re.sub(r"\s+", " ", meta.get("title", "")).lower().strip()
    return f"{meta.get('date', '')}|{title_norm}"


def _log(path: Path, msg: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(msg + "\n")


def main() -> None:
    SKIP_LOG.unlink(missing_ok=True)
    DUP_LOG.unlink(missing_ok=True)

    all_txts = sorted(p for p in IN_DIR.glob("ET_*.txt"))
    print(f"Found {len(all_txts):,} TXT files in {IN_DIR}")
    print(f"Output → {OUT_DIR}\n")

    stats = {
        "attempted":       0,
        "skipped_date":    0,
        "skipped_quality": 0,
        "duplicates":      0,
        "processed":       0,
        "total_words":     0,
    }

    candidates: dict[str, list] = {}

    for txt_path in tqdm(all_txts, desc="Cleaning", unit="doc"):
        stats["attempted"] += 1

        json_path = txt_path.with_suffix(".json")
        if not json_path.exists():
            _log(SKIP_LOG, f"SKIP\t{txt_path.name}\tno sidecar JSON")
            stats["skipped_quality"] += 1
            continue

        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)

        # Date filter — use the trafilatura-extracted date, not the sitemap lastmod
        pub_date = _parse_date(meta.get("date", ""))
        if pub_date is None or not (START_DATE <= pub_date <= END_DATE):
            _log(SKIP_LOG, f"DATE\t{txt_path.name}\t{meta.get('date','?')}")
            stats["skipped_date"] += 1
            continue

        raw = txt_path.read_text(encoding="utf-8", errors="replace")
        text = clean_text(raw)
        word_count = len(text.split())

        if word_count < MIN_WORDS:
            _log(SKIP_LOG, f"SHORT\t{txt_path.name}\t{word_count} words")
            stats["skipped_quality"] += 1
            continue

        key = _dedup_key(meta)
        if key not in candidates:
            candidates[key] = []
        candidates[key].append((txt_path, json_path, meta, text, word_count))

    # ── Resolve duplicates, write output ──────────────────────────────────────
    print(f"\nWriting deduplicated output...")

    for key, group in candidates.items():
        group.sort(key=lambda x: x[4], reverse=True)
        chosen_txt, chosen_json, meta, text, word_count = group[0]

        for dup_txt, *_ in group[1:]:
            _log(DUP_LOG, f"DUP\t{dup_txt.name}\tkept {chosen_txt.name}")
            stats["duplicates"] += 1

        out_txt  = OUT_DIR / chosen_txt.name
        out_json = OUT_DIR / chosen_json.name
        out_txt.write_text(text, encoding="utf-8")
        shutil.copy2(chosen_json, out_json)

        stats["processed"] += 1
        stats["total_words"] += word_count

    avg_words = stats["total_words"] // max(stats["processed"], 1)
    skipped_total = stats["skipped_date"] + stats["skipped_quality"]

    report = {
        **stats,
        "skipped_total":    skipped_total,
        "avg_words_per_doc": avg_words,
        "input_dir":  str(IN_DIR),
        "output_dir": str(OUT_DIR),
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("=" * 48)
    print(f"  Attempted      : {stats['attempted']:,}")
    print(f"  Processed      : {stats['processed']:,}")
    print(f"  Skipped (date) : {stats['skipped_date']:,}")
    print(f"  Skipped (short): {stats['skipped_quality']:,}")
    print(f"  Duplicates     : {stats['duplicates']:,}")
    print(f"  Avg words      : {avg_words:,}")
    print(f"  Total words    : {stats['total_words']:,}")
    print(f"  Output         → {OUT_DIR}")
    print("=" * 48)


if __name__ == "__main__":
    main()
