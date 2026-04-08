#!/usr/bin/env python3
"""
Preprocessing pipeline for SEBI circulars corpus.

INPUT  : data/raw/sebi/*.txt  (+ matching .json sidecars)
OUTPUT : data/processed/sebi/*.txt  (+ matching .json sidecars)
         data/processed/sebi/skipped.log
         data/processed/sebi/duplicates.log
         data/processed/sebi/preprocessing_report_sebi.json
"""

import json
import re
import shutil
from pathlib import Path

from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
IN_DIR   = BASE_DIR / "data" / "raw" / "sebi"
OUT_DIR  = BASE_DIR / "data" / "processed" / "sebi"
OUT_DIR.mkdir(parents=True, exist_ok=True)

SKIP_LOG    = OUT_DIR / "skipped.log"
DUP_LOG     = OUT_DIR / "duplicates.log"
REPORT_PATH = OUT_DIR / "preprocessing_report_sebi.json"

# ── Constants ─────────────────────────────────────────────────────────────────
MIN_LINE_LEN  = 30    # lines shorter than this are dropped
MIN_WORDS_DOC = 150   # skip entire document if below this after cleaning

# Regex patterns for SEBI-specific boilerplate
_PAGE_HEADER   = re.compile(r"^Page\s+\d+\s+of\s+\d+\s*$", re.I)
_CIRC_REF      = re.compile(r"^SEBI/[A-Z/]+/\d{4}/\d+")     # e.g. SEBI/HO/CFD/...
_PURE_NUMERIC  = re.compile(r"^\s*[\d\s,.\-]+\s*$")

BOILERPLATE_FRAGMENTS = [
    "dear sir",
    "dear madam",
    "madam/sir",
    "sir/madam",
    "yours faithfully",
    "yours truly",
    "yours sincerely",
    "thanking you",
    "to,",
    "all stock exchanges",
    "all recognized stock exchanges",
    "all registered",
    "all depositories",
    "all mutual funds",
    "all asset management",
    "all merchant bankers",
    "all clearing corporations",
    "all trading members",
    "all market infrastructure",
]


def _is_boilerplate(line: str) -> bool:
    ll = line.lower().strip()
    if _PAGE_HEADER.match(line.strip()):
        return True
    if _CIRC_REF.match(line.strip()):
        return True
    return any(ll == frag or ll.startswith(frag) for frag in BOILERPLATE_FRAGMENTS)


def clean_text(raw: str) -> str:
    lines = raw.splitlines()
    cleaned = []
    for line in lines:
        line = line.strip()
        if _PURE_NUMERIC.match(line) and line != "":
            continue
        if line != "" and len(line) < MIN_LINE_LEN:
            continue
        if _is_boilerplate(line):
            continue
        cleaned.append(line)

    # Collapse 3+ consecutive blank lines → single blank line
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
    """Canonical key: title + date (catches same circular with different filenames)."""
    title_norm = re.sub(r"\s+", " ", meta.get("title", "")).lower().strip()
    return f"{meta.get('date', '')}|{title_norm}"


# ── Logging helpers ───────────────────────────────────────────────────────────
def _log(path: Path, message: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(message + "\n")


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    SKIP_LOG.unlink(missing_ok=True)
    DUP_LOG.unlink(missing_ok=True)

    all_txts = sorted(IN_DIR.glob("*.txt"))
    # Exclude log files
    all_txts = [p for p in all_txts if not p.name.endswith(".log")]
    print(f"Found {len(all_txts)} TXT files in {IN_DIR}")
    print(f"Output → {OUT_DIR}\n")

    candidates: dict[str, list] = {}
    stats = {
        "attempted":   0,
        "skipped":     0,
        "duplicates":  0,
        "processed":   0,
        "total_words": 0,
    }

    for txt_path in tqdm(all_txts, desc="Cleaning", unit="doc"):
        stats["attempted"] += 1

        json_path = txt_path.with_suffix(".json")
        if not json_path.exists():
            _log(SKIP_LOG, f"SKIP\t{txt_path.name}\tno sidecar JSON")
            stats["skipped"] += 1
            continue

        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)

        raw = txt_path.read_text(encoding="utf-8", errors="replace")
        text = clean_text(raw)
        word_count = len(text.split())

        if word_count < MIN_WORDS_DOC:
            _log(SKIP_LOG, f"SKIP\t{txt_path.name}\tonly {word_count} words after cleaning")
            stats["skipped"] += 1
            continue

        key = _dedup_key(meta)
        if key not in candidates:
            candidates[key] = []
        candidates[key].append((txt_path, json_path, meta, text, word_count))

    # ── Resolve duplicates, write output ──────────────────────────────────────
    print(f"\nWriting deduplicated output...")

    for key, group in candidates.items():
        # Keep the entry with the most words
        group.sort(key=lambda x: x[4], reverse=True)
        chosen_txt, chosen_json, meta, text, word_count = group[0]

        if len(group) > 1:
            for dup_txt, _, _, _, _ in group[1:]:
                _log(DUP_LOG, f"DUP\t{dup_txt.name}\tkept {chosen_txt.name}")
                stats["duplicates"] += 1

        out_txt  = OUT_DIR / chosen_txt.name
        out_json = OUT_DIR / chosen_json.name

        out_txt.write_text(text, encoding="utf-8")
        shutil.copy2(chosen_json, out_json)

        stats["processed"] += 1
        stats["total_words"] += word_count

    avg_words = stats["total_words"] // max(stats["processed"], 1)

    report = {
        **stats,
        "avg_words_per_doc": avg_words,
        "input_dir":  str(IN_DIR),
        "output_dir": str(OUT_DIR),
    }
    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("=" * 44)
    print(f"  Attempted   : {stats['attempted']}")
    print(f"  Processed   : {stats['processed']}")
    print(f"  Skipped     : {stats['skipped']}")
    print(f"  Duplicates  : {stats['duplicates']}")
    print(f"  Avg words   : {avg_words:,}")
    print(f"  Output      → {OUT_DIR}")
    print("=" * 44)


if __name__ == "__main__":
    main()
