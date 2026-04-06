#!/usr/bin/env python3
"""
Preprocessing pipeline for earnings call transcripts.

INPUT  : data/raw/transcripts/*.txt  (+ matching .json sidecars)
OUTPUT : data/processed/transcripts/*.txt  (+ matching .json sidecars)
         data/processed/transcripts/skipped.log
         data/processed/transcripts/duplicates.log
         data/processed/transcripts/preprocessing_report_transcripts.json
"""

import json
import os
import re
import shutil
from pathlib import Path

from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
IN_DIR   = BASE_DIR / "data" / "raw" / "transcripts"
OUT_DIR  = BASE_DIR / "data" / "processed" / "transcripts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SKIP_LOG    = OUT_DIR / "skipped.log"
DUP_LOG     = OUT_DIR / "duplicates.log"
REPORT_PATH = OUT_DIR / "preprocessing_report_transcripts.json"

# ── Constants ─────────────────────────────────────────────────────────────────
MIN_CHARS     = 30    # drop lines shorter than this
MIN_WORDS_DOC = 300   # skip entire document if below this after cleaning

OPERATOR_BOILERPLATE = [
    "ladies and gentlemen",
    "press star",
    "touchstone",
    "please note this conference",
    "is being recorded",
    "i now hand",
    "over to you",
    "thank you operator",
    "next question",
    "please go ahead",
    "you may press",
    "question queue",
    "listen only mode",
    "conference is now",
]

_PURE_NUMERIC = re.compile(r"^\s*[\d\s,.\-]+\s*$")


# ── Cleaning ──────────────────────────────────────────────────────────────────
def _is_operator_boilerplate(line: str) -> bool:
    ll = line.lower()
    return any(phrase in ll for phrase in OPERATOR_BOILERPLATE)


def _is_short_transition(line: str) -> bool:
    """
    Lines under 8 words that are pure speaker hand-offs.
    e.g. "Thank you. Over to you sir." / "Thank you. Next question please."
    """
    words = line.split()
    if len(words) >= 8:
        return False
    ll = line.lower()
    transition_words = {"thank", "you", "over", "sir", "madam", "please",
                        "next", "okay", "ok", "sure", "right", "yes", "no"}
    non_transition = [w.strip(".,!?") for w in words
                      if w.strip(".,!?").lower() not in transition_words]
    return len(non_transition) == 0


def _is_speaker_label(line: str) -> bool:
    """
    Detect pure speaker label lines — name/role with no sentence after it.
    e.g. "Operator:", "Rahul Jain - CFO:", "Analyst:"
    Heuristic: ends with ':' or is all-caps short token.
    """
    stripped = line.strip()
    if stripped.endswith(":"):
        return True
    if len(stripped.split()) <= 4 and stripped.replace(" ", "").isupper():
        return True
    return False


def clean_text(raw: str) -> str:
    """
    Apply all cleaning steps in order. Returns cleaned text string.
    """
    lines = raw.splitlines()
    cleaned = []

    for line in lines:
        line = line.strip()

        # Step 1: operator boilerplate
        if _is_operator_boilerplate(line):
            continue

        # Step 2: short transition lines (under 8 words, hand-off only)
        if line and _is_short_transition(line):
            continue

        # Step 3: too short (keep blank lines for collapse step)
        if line != "" and len(line) < MIN_CHARS:
            continue

        # Step 4: purely numeric
        if line and _PURE_NUMERIC.match(line):
            continue

        cleaned.append(line)

    # Step 5: collapse 3+ consecutive blank lines → single blank line
    result = []
    blank_count = 0
    for line in cleaned:
        if line == "":
            blank_count += 1
            if blank_count <= 1:
                result.append(line)
        else:
            blank_count = 0
            result.append(line)

    return "\n".join(result).strip()


def word_count(text: str) -> int:
    """
    Count words excluding pure speaker label lines so they don't
    artificially inflate the count toward the quality threshold.
    """
    count = 0
    for line in text.splitlines():
        if not _is_speaker_label(line):
            count += len(line.split())
    return count


# ── Deduplication key ─────────────────────────────────────────────────────────
def _dedup_key(meta: dict) -> str:
    return f"{meta.get('ticker', '')}|{meta.get('date', '')}"


# ── Logging ───────────────────────────────────────────────────────────────────
def _log(path: Path, message: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(message + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    SKIP_LOG.unlink(missing_ok=True)
    DUP_LOG.unlink(missing_ok=True)

    all_txts = sorted(
        f for f in IN_DIR.glob("*.txt")
        if f.stem not in ("failed_downloads", "skipped")
    )
    print(f"Found {len(all_txts)} transcript .txt files in {IN_DIR}")
    print(f"Output → {OUT_DIR}\n")

    stats = {
        "attempted":   0,
        "skipped":     0,
        "duplicates":  0,
        "processed":   0,
        "total_words": 0,
    }

    # ── Pass 1: extract, clean, quality filter, collect candidates ────────────
    # candidates[dedup_key] = (txt_path, json_path, meta, cleaned_text, wc)
    candidates: dict[str, tuple] = {}

    for txt_path in tqdm(all_txts, desc="Cleaning", unit="file"):
        stats["attempted"] += 1

        json_path = txt_path.with_suffix(".json")
        if not json_path.exists():
            _log(SKIP_LOG, f"SKIP\t{txt_path.name}\tno sidecar JSON")
            stats["skipped"] += 1
            continue

        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)

        raw = txt_path.read_text(encoding="utf-8", errors="ignore")
        text = clean_text(raw)
        wc   = word_count(text)

        if wc < MIN_WORDS_DOC:
            _log(SKIP_LOG, f"SKIP\t{txt_path.name}\tonly {wc} words after cleaning")
            stats["skipped"] += 1
            continue

        key = _dedup_key(meta)
        if key not in candidates:
            candidates[key] = (txt_path, json_path, meta, text, wc)
        else:
            # Keep the one with higher word count
            existing = candidates[key]
            if wc > existing[4]:
                _log(DUP_LOG,
                     f"DISCARD\t{existing[0].name}\tkept={txt_path.name}\treason=lower word count ({existing[4]} < {wc})")
                stats["duplicates"] += 1
                candidates[key] = (txt_path, json_path, meta, text, wc)
            else:
                _log(DUP_LOG,
                     f"DISCARD\t{txt_path.name}\tkept={existing[0].name}\treason=lower word count ({wc} <= {existing[4]})")
                stats["duplicates"] += 1

    # ── Pass 2: write output ──────────────────────────────────────────────────
    print(f"\nWriting {len(candidates)} deduplicated transcripts...")

    for txt_path, json_path, meta, text, wc in tqdm(
        candidates.values(), desc="Writing", unit="file"
    ):
        out_txt  = OUT_DIR / txt_path.name
        out_json = OUT_DIR / json_path.name

        out_txt.write_text(text, encoding="utf-8")
        shutil.copy2(json_path, out_json)

        stats["processed"]   += 1
        stats["total_words"] += wc

    # ── Report ────────────────────────────────────────────────────────────────
    avg_words = (
        round(stats["total_words"] / stats["processed"], 1)
        if stats["processed"] > 0 else 0
    )

    report = {
        "total_attempted":       stats["attempted"],
        "successfully_processed": stats["processed"],
        "duplicates_removed":    stats["duplicates"],
        "skipped_low_quality":   stats["skipped"],
        "total_word_count":      stats["total_words"],
        "average_words_per_doc": avg_words,
    }

    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("─" * 44)
    print(f"  Attempted             : {report['total_attempted']}")
    print(f"  Successfully processed: {report['successfully_processed']}")
    print(f"  Duplicates removed    : {report['duplicates_removed']}")
    print(f"  Skipped (low quality) : {report['skipped_low_quality']}")
    print(f"  Total words           : {report['total_word_count']:,}")
    print(f"  Avg words / doc       : {report['average_words_per_doc']:,}")
    print(f"  Report → {REPORT_PATH}")
    print("─" * 44)


if __name__ == "__main__":
    main()
