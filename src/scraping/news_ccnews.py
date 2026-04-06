#!/usr/bin/env python3
"""
Indian Financial News — CC-News filter (FinLLM-India Phase 1).

SOURCE : HuggingFace dataset stanford-oval/ccnews (streaming)
GOAL   : Filter 600M+ articles down to Indian financial news only.

OUTPUT : data/raw/news/   (shared with news_scraper.py output)
         data/raw/news/ccnews_report.json
"""

import json
import re
from collections import defaultdict
from pathlib import Path

from datasets import load_dataset
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "news"
OUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_PATH = OUT_DIR / "ccnews_report.json"

# ── Config ────────────────────────────────────────────────────────────────────
YEARS = [2019, 2020, 2021, 2022, 2023, 2024]

INDIAN_FINANCE_DOMAINS = [
    "moneycontrol.com",
    "economictimes.indiatimes.com",
    "business-standard.com",
    "livemint.com",
    "financialexpress.com",
    "thehindubusinessline.com",
    "bloombergquint.com",
    "ndtvprofit.com",
    "zeebiz.com",
    "cnbctv18.com",
]

FINANCE_KEYWORDS = [
    "nse", "bse", "nifty", "sensex", "sebi", "equity", "rupee",
    "rbi", "earnings", "quarterly results", "crore", "lakh",
    "stock market", "share price", "ipo", "mutual fund",
]

MIN_WORDS = 150
PRINT_EVERY = 1000   # print progress every N articles saved per year


# ── Helpers ───────────────────────────────────────────────────────────────────
def _domain_short(domain: str) -> str:
    """'economictimes.indiatimes.com' → 'economictimes'"""
    return domain.split(".")[0]


def _contains_keyword(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in FINANCE_KEYWORDS)


def _is_indian_finance_domain(domain: str) -> bool:
    d = (domain or "").lower()
    return any(fd in d for fd in INDIAN_FINANCE_DOMAINS)


def _safe_date(record: dict) -> str:
    """Return YYYY-MM-DD from the record's date field, or 'unknown'."""
    raw = record.get("date", "") or ""
    # Already ISO format or starts with YYYY-MM-DD
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(raw))
    return m.group(1) if m else "unknown"


# ── Per-year processing ───────────────────────────────────────────────────────
def process_year(year: int, global_stats: dict) -> None:
    print(f"\n{'─' * 44}")
    print(f"  Year {year} — loading stream...")

    try:
        ds = load_dataset(
            "stanford-oval/ccnews",
            name=str(year),
            split="train",
            streaming=True,
        )
    except Exception as exc:
        print(f"  [ERROR] Could not load year {year}: {exc}")
        return

    year_examined = 0
    year_saved    = 0
    idx           = 0   # per-year file index

    for record in ds:
        year_examined += 1
        global_stats["total_examined"] += 1

        domain   = (record.get("domain") or "").strip()
        language = (record.get("language") or "").strip()
        text     = (record.get("text") or "").strip()
        title    = (record.get("title") or "").strip()
        url      = (record.get("url") or "").strip()

        # Step 1: domain filter
        if not _is_indian_finance_domain(domain):
            continue

        # Step 2: language filter
        if language != "en":
            continue

        # Step 3: keyword filter
        if not _contains_keyword(text):
            continue

        # Step 4: quality filter
        words = text.split()
        if len(words) < MIN_WORDS:
            continue

        # Step 5: save
        date       = _safe_date(record)
        dom_short  = _domain_short(domain)
        idx       += 1
        filename   = f"{dom_short}_{date}_{idx:06d}"

        txt_path  = OUT_DIR / f"{filename}.txt"
        json_path = OUT_DIR / f"{filename}.json"

        # Text file: title + blank line + body
        txt_path.write_text(f"{title}\n\n{text}", encoding="utf-8")

        # JSON sidecar
        meta = {
            "title":      title,
            "domain":     domain,
            "date":       date,
            "source_url": url,
            "word_count": len(words),
            "year":       year,
        }
        json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        year_saved += 1
        global_stats["total_saved"] += 1
        global_stats["by_domain"][domain] += 1
        global_stats["by_year"][str(year)] += 1
        global_stats["total_words"] += len(words)

        if year_saved % PRINT_EVERY == 0:
            print(f"    [{year}] examined={year_examined:,}  saved={year_saved:,}")

    print(f"  Year {year} done — examined={year_examined:,}  saved={year_saved:,}")
    global_stats["year_examined"][str(year)] = year_examined


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    global_stats: dict = {
        "total_examined": 0,
        "total_saved":    0,
        "total_words":    0,
        "by_domain":      defaultdict(int),
        "by_year":        defaultdict(int),
        "year_examined":  {},
    }

    print(f"CC-News Indian Finance Filter")
    print(f"Years : {YEARS}")
    print(f"Output: {OUT_DIR}\n")

    for year in YEARS:
        process_year(year, global_stats)

    # ── Final report ──────────────────────────────────────────────────────────
    report = {
        "total_articles_examined": global_stats["total_examined"],
        "total_articles_saved":    global_stats["total_saved"],
        "total_word_count":        global_stats["total_words"],
        "breakdown_by_domain":     dict(sorted(
            global_stats["by_domain"].items(), key=lambda x: -x[1]
        )),
        "breakdown_by_year":       dict(global_stats["by_year"]),
        "articles_examined_by_year": dict(global_stats["year_examined"]),
    }

    REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("═" * 44)
    print(f"  Total examined : {report['total_articles_examined']:,}")
    print(f"  Total saved    : {report['total_articles_saved']:,}")
    print(f"  Total words    : {report['total_word_count']:,}")
    print()
    print("  By domain:")
    for domain, count in report["breakdown_by_domain"].items():
        print(f"    {domain:40s} {count:6,}")
    print()
    print("  By year:")
    for yr, count in sorted(report["breakdown_by_year"].items()):
        examined = report["articles_examined_by_year"].get(yr, "?")
        print(f"    {yr}  saved={count:6,}  examined={examined:,}" if isinstance(examined, int)
              else f"    {yr}  saved={count:6,}")
    print()
    print(f"  Report → {REPORT_PATH}")
    print("═" * 44)


if __name__ == "__main__":
    main()
