#!/usr/bin/env python3
"""
Indian Financial News Scraper — FinLLM-India Phase 1.

Step 1: Parse RSS feeds from Moneycontrol, Economic Times, Business Standard,
        and Google News to collect article URLs.
Step 2: Fetch full article text via trafilatura.
Step 3: Filter by word count and Indian-finance keywords.
Step 4: Save .txt + .json sidecar to data/raw/news/

Output : data/raw/news/
Logs   : data/raw/news/failed_downloads.log
"""

import json
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import trafilatura
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "news"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FAIL_LOG = OUT_DIR / "failed_downloads.log"

# ── RSS Feed Sources ──────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("moneycontrol",     "https://www.moneycontrol.com/rss/marketreports.xml"),
    ("moneycontrol",     "https://www.moneycontrol.com/rss/business.xml"),
    ("economictimes",    "https://economictimes.indiatimes.com/markets/rss.cms"),
    ("economictimes",    "https://economictimes.indiatimes.com/industry/rss.cms"),
    ("businessstandard", "https://www.business-standard.com/rss/markets-106.rss"),
    ("businessstandard", "https://www.business-standard.com/rss/economy-policy-10202.rss"),
    ("googlenews",       "https://news.google.com/rss/search?q=NSE+BSE+Nifty+Sensex+earnings+India&hl=en-IN&gl=IN&ceid=IN:en"),
    ("googlenews",       "https://news.google.com/rss/search?q=SEBI+India+stock+market&hl=en-IN&gl=IN&ceid=IN:en"),
    ("googlenews",       "https://news.google.com/rss/search?q=Indian+economy+RBI+inflation+GDP+quarterly+results&hl=en-IN&gl=IN&ceid=IN:en"),
]

# ── Filter Keywords ───────────────────────────────────────────────────────────
FINANCE_KEYWORDS = [
    "nse", "bse", "nifty", "sensex", "sebi", "equity",
    "stock", "shares", "rupee", "rbi", "earnings", "quarterly", "crore", "lakh",
]

# ── Constants ─────────────────────────────────────────────────────────────────
MIN_WORDS  = 100
FETCH_DELAY = 1   # seconds between article fetches

# ── Source label from domain ──────────────────────────────────────────────────
_DOMAIN_SOURCE = {
    "moneycontrol.com":          "moneycontrol",
    "economictimes.indiatimes.com": "economictimes",
    "business-standard.com":    "businessstandard",
    "news.google.com":          "googlenews",
}


def _source_label(url: str) -> str:
    host = urlparse(url).netloc.lower().lstrip("www.")
    for domain, label in _DOMAIN_SOURCE.items():
        if domain in host:
            return label
    # Fallback: use first part of host
    return host.split(".")[0]


def _slug(url: str, max_len: int = 60) -> str:
    """Last path segment of a URL, cleaned to alphanumeric + hyphens."""
    path = urlparse(url).path.rstrip("/")
    segment = path.rsplit("/", 1)[-1] if "/" in path else path
    slug = re.sub(r"[^a-zA-Z0-9\-]", "-", segment)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:max_len] or "article"


def _parse_date(entry) -> str:
    """Extract YYYY-MM-DD from a feedparser entry."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            try:
                return datetime(*t[:3]).strftime("%Y-%m-%d")
            except Exception:
                pass
    return datetime.today().strftime("%Y-%m-%d")


def _log_fail(url: str, reason: str) -> None:
    with open(FAIL_LOG, "a", encoding="utf-8") as f:
        f.write(f"FAIL\t{url}\t{reason}\n")


def _contains_finance_keyword(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in FINANCE_KEYWORDS)


# ── Step 1: Parse all RSS feeds ───────────────────────────────────────────────
def collect_urls() -> list[tuple[str, str, str, str]]:
    """
    Returns a deduplicated list of (url, title, date, source) tuples.
    """
    seen: set[str] = set()
    articles: list[tuple[str, str, str, str]] = []
    feeds_ok = 0
    feeds_failed = 0

    print(f"Step 1: Parsing {len(RSS_FEEDS)} RSS feeds...")

    for source, feed_url in RSS_FEEDS:
        feed = feedparser.parse(feed_url)

        if feed.bozo and not feed.entries:
            print(f"  [WARN] Feed failed or empty: {feed_url}")
            _log_fail(feed_url, "feed parse error or empty")
            feeds_failed += 1
            continue

        feeds_ok += 1
        added = 0
        for entry in feed.entries:
            url   = getattr(entry, "link", "").strip()
            title = getattr(entry, "title", "").strip()
            date  = _parse_date(entry)
            if not url or url in seen:
                continue
            seen.add(url)
            articles.append((url, title, date, source))
            added += 1

        print(f"  {source:20s} → {added:4d} URLs  ({feed_url[:60]}...)")

    print(f"\n  Feeds OK: {feeds_ok}  |  Failed: {feeds_failed}")
    print(f"  Total unique article URLs: {len(articles)}\n")
    return articles


# ── Step 2–4: Fetch, filter, save ────────────────────────────────────────────
def process_articles(articles: list[tuple[str, str, str, str]]) -> dict:
    stats = {"saved": 0, "skipped_short": 0, "skipped_keyword": 0, "failed": 0}

    print(f"Step 2: Fetching {len(articles)} articles...\n")

    for url, title, date, source in tqdm(articles, desc="Fetching", unit="art"):
        time.sleep(FETCH_DELAY)

        # Fetch
        try:
            downloaded = trafilatura.fetch_url(url)
        except Exception as exc:
            _log_fail(url, f"fetch exception: {exc}")
            stats["failed"] += 1
            continue

        if not downloaded:
            _log_fail(url, "trafilatura.fetch_url returned None")
            stats["failed"] += 1
            continue

        # Extract text
        text = trafilatura.extract(downloaded)
        if not text:
            _log_fail(url, "trafilatura.extract returned None")
            stats["failed"] += 1
            continue

        # Word count filter
        words = text.split()
        if len(words) < MIN_WORDS:
            stats["skipped_short"] += 1
            continue

        # Keyword filter
        if not _contains_finance_keyword(text):
            stats["skipped_keyword"] += 1
            continue

        # Build filename
        src_label = _source_label(url)
        slug      = _slug(url)
        filename  = f"{src_label}_{date}_{slug}"

        txt_path  = OUT_DIR / f"{filename}.txt"
        json_path = OUT_DIR / f"{filename}.json"

        # Write text
        txt_path.write_text(text, encoding="utf-8")

        # Write sidecar
        meta = {
            "title":      title,
            "source":     src_label,
            "date":       date,
            "source_url": url,
            "word_count": len(words),
        }
        json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        stats["saved"] += 1

    return stats


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    # Clear old fail log
    FAIL_LOG.unlink(missing_ok=True)

    articles = collect_urls()

    if not articles:
        print("No article URLs found. Check feed URLs and network access.")
        return

    stats = process_articles(articles)

    total = len(articles)
    print()
    print("─" * 44)
    print(f"  Total URLs collected  : {total}")
    print(f"  Articles saved        : {stats['saved']}")
    print(f"  Skipped (< {MIN_WORDS} words) : {stats['skipped_short']}")
    print(f"  Skipped (no keywords) : {stats['skipped_keyword']}")
    print(f"  Failed (fetch/parse)  : {stats['failed']}")
    print(f"  Output → {OUT_DIR}")
    print(f"  Fail log → {FAIL_LOG}")
    print("─" * 44)


if __name__ == "__main__":
    main()
