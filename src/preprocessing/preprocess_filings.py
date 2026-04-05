#!/usr/bin/env python3
"""
Preprocessing pipeline for FinLLM-India filings corpus.

INPUT  : data/raw/filings/*.pdf  (+ matching .json sidecars)
OUTPUT : data/processed/*.txt    (+ matching .json sidecars)
         data/processed/duplicates.log
         data/processed/skipped.log
         data/processed/preprocessing_report.json

Steps per document:
  1. Extract text from PDF via PyMuPDF (page by page)
  2. Clean: strip boilerplate, short lines, pure numerics, excess blanks
  3. Deduplicate: same ticker + date + filing_type → keep NSE, drop BSE
  4. Quality filter: skip if < 200 words after cleaning
  5. Write .txt + copy .json sidecar
"""

import json
import os
import re
import shutil
from pathlib import Path

import fitz  # PyMuPDF
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parents[2]
IN_DIR      = BASE_DIR / "data" / "raw" / "filings"
OUT_DIR     = BASE_DIR / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

DUP_LOG     = OUT_DIR / "duplicates.log"
SKIP_LOG    = OUT_DIR / "skipped.log"
REPORT_PATH = OUT_DIR / "preprocessing_report.json"

# ── Constants ─────────────────────────────────────────────────────────────────
MIN_LINE_LEN  = 40       # lines shorter than this are dropped
MIN_WORDS     = 200      # documents with fewer words after cleaning are skipped

BOILERPLATE = [
    "BSE Limited",
    "National Stock Exchange",
    "Bombay Stock Exchange",
    "Kind Attn",
    "Sub:",
    "Ref:",
    "To,",
    "Dear Sir",
    "Dear Madam",
    "Yours faithfully",
    "Yours truly",
    "Thanking you",
    "ISIN",
]
# Pre-lowercase for case-insensitive matching
_BOILERPLATE_LOWER = [p.lower() for p in BOILERPLATE]


# ── Extraction ────────────────────────────────────────────────────────────────
def extract_text(pdf_path: Path) -> str:
    """Extract raw text from all pages of a PDF using PyMuPDF."""
    pages = []
    try:
        with fitz.open(str(pdf_path)) as doc:
            for page in doc:
                pages.append(page.get_text())
    except Exception as exc:
        raise RuntimeError(f"PDF extraction failed: {exc}") from exc
    return "\n".join(pages)


# ── Cleaning ──────────────────────────────────────────────────────────────────
_PURE_NUMERIC = re.compile(r"^\s*[\d\s,.\-]+\s*$")


def _is_boilerplate(line: str) -> bool:
    ll = line.lower()
    return any(bp in ll for bp in _BOILERPLATE_LOWER)


def clean_text(raw: str) -> str:
    """
    Apply cleaning steps in order:
      1. Strip leading/trailing whitespace from each line
      2. Drop purely numeric lines (page numbers, standalone figures)
      3. Drop lines shorter than MIN_LINE_LEN characters
      4. Drop lines containing boilerplate phrases
      5. Collapse 3+ consecutive blank lines into one blank line
    """
    lines = raw.splitlines()
    cleaned = []
    for line in lines:
        line = line.strip()

        # Step 2: purely numeric
        if _PURE_NUMERIC.match(line) and line != "":
            continue

        # Step 3: too short (keep blank lines for now — handled in step 5)
        if line != "" and len(line) < MIN_LINE_LEN:
            continue

        # Step 4: boilerplate
        if _is_boilerplate(line):
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


# ── Deduplication ─────────────────────────────────────────────────────────────
def _dedup_key(meta: dict) -> str:
    """Canonical key: ticker + date + filing_type (exchange-independent)."""
    return f"{meta['ticker']}|{meta['date']}|{meta['filing_type']}"


# ── Logging helpers ───────────────────────────────────────────────────────────
def _log(path: Path, message: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(message + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    # Clear old logs
    DUP_LOG.unlink(missing_ok=True)
    SKIP_LOG.unlink(missing_ok=True)

    # Collect all PDFs
    all_pdfs = sorted(IN_DIR.glob("*.pdf"))
    print(f"Found {len(all_pdfs)} PDFs in {IN_DIR}")
    print(f"Output → {OUT_DIR}\n")

    # ── Pass 1: extract + clean, build candidate map ──────────────────────────
    # candidates[dedup_key] = list of (pdf_path, meta, cleaned_text)
    # We collect all first, then resolve duplicates before writing.
    candidates: dict[str, list] = {}

    stats = {
        "attempted":   0,
        "skipped":     0,
        "duplicates":  0,
        "processed":   0,
        "total_words": 0,
    }

    for pdf_path in tqdm(all_pdfs, desc="Extracting", unit="pdf"):
        stats["attempted"] += 1

        # Load sidecar
        json_path = pdf_path.with_suffix(".json")
        if not json_path.exists():
            _log(SKIP_LOG, f"SKIP\t{pdf_path.name}\tno sidecar JSON")
            stats["skipped"] += 1
            continue

        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)

        # Extract
        try:
            raw = extract_text(pdf_path)
        except RuntimeError as exc:
            _log(SKIP_LOG, f"SKIP\t{pdf_path.name}\t{exc}")
            stats["skipped"] += 1
            continue

        # Clean
        text = clean_text(raw)
        word_count = len(text.split())

        # Quality filter
        if word_count < MIN_WORDS:
            _log(SKIP_LOG, f"SKIP\t{pdf_path.name}\tonly {word_count} words after cleaning")
            stats["skipped"] += 1
            continue

        key = _dedup_key(meta)
        if key not in candidates:
            candidates[key] = []
        candidates[key].append((pdf_path, json_path, meta, text, word_count))

    # ── Pass 2: resolve duplicates, write output ───────────────────────────────
    print(f"\nWriting deduplicated output...")

    for key, entries in tqdm(candidates.items(), desc="Writing", unit="doc"):
        if len(entries) == 1:
            chosen = entries[0]
            discarded = []
        else:
            # Prefer NSE; among same exchange prefer first seen
            nse = [e for e in entries if e[2]["exchange"] == "NSE"]
            chosen = nse[0] if nse else entries[0]
            discarded = [e for e in entries if e is not chosen]

        for d in discarded:
            stats["duplicates"] += 1
            _log(DUP_LOG,
                 f"DISCARD\t{d[0].name}\tkept={chosen[0].name}\treason=duplicate({key})")

        pdf_path, json_path, meta, text, word_count = chosen

        # Output filename: same stem as PDF but .txt
        out_stem = pdf_path.stem
        out_txt  = OUT_DIR / f"{out_stem}.txt"
        out_json = OUT_DIR / f"{out_stem}.json"

        out_txt.write_text(text, encoding="utf-8")
        shutil.copy2(json_path, out_json)

        stats["processed"]   += 1
        stats["total_words"] += word_count

    # ── Report ─────────────────────────────────────────────────────────────────
    avg_words = (
        round(stats["total_words"] / stats["processed"], 1)
        if stats["processed"] > 0 else 0
    )

    report = {
        "total_pdfs_attempted":    stats["attempted"],
        "successfully_processed":  stats["processed"],
        "duplicates_removed":      stats["duplicates"],
        "skipped_low_quality":     stats["skipped"],
        "total_word_count":        stats["total_words"],
        "average_words_per_doc":   avg_words,
    }

    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("─" * 42)
    print(f"  PDFs attempted        : {report['total_pdfs_attempted']}")
    print(f"  Successfully processed: {report['successfully_processed']}")
    print(f"  Duplicates removed    : {report['duplicates_removed']}")
    print(f"  Skipped (low quality) : {report['skipped_low_quality']}")
    print(f"  Total words           : {report['total_word_count']:,}")
    print(f"  Avg words / doc       : {report['average_words_per_doc']:,}")
    print(f"  Report → {REPORT_PATH}")
    print(f"  Dup log → {DUP_LOG}")
    print(f"  Skip log → {SKIP_LOG}")
    print("─" * 42)


if __name__ == "__main__":
    main()
