#!/usr/bin/env python3
"""
AlphaStreet India — Earnings Call Transcripts Scraper.

Step 1: Crawl 663 listing pages to collect all transcript URLs.
Step 2: Scrape each transcript page for text + metadata.
Step 3: Save .txt + .json sidecar to data/raw/transcripts/

Output : data/raw/transcripts/
Logs   : data/raw/transcripts/failed_downloads.log
         data/raw/transcripts/skipped.log
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "transcripts"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FAIL_LOG = OUT_DIR / "failed_downloads.log"
SKIP_LOG = OUT_DIR / "skipped.log"

# ── Config ────────────────────────────────────────────────────────────────────
LISTING_BASE   = "https://alphastreet.com/india/category/transcripts/page/{}/"
TOTAL_PAGES    = 663
DELAY          = 2    # seconds between requests
RETRY_WAIT     = 30   # seconds to wait on 403/429 before retrying
MIN_WORDS      = 300  # skip transcripts shorter than this

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=str(FAIL_LOG),
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Headers ───────────────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "text/html,application/xhtml+xml,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_filename(name: str) -> str:
    """Strip characters illegal in filenames."""
    return re.sub(r'[<>:"/\\|?*\s]', "_", name)


def _fetch(url: str) -> requests.Response | None:
    """
    Fetch a URL with a 2s delay. On 403/429, wait 30s and retry once.
    Returns the Response or None on failure.
    """
    time.sleep(DELAY)
    try:
        resp = session.get(url, timeout=20)
    except requests.RequestException as exc:
        log.error("Request error [%s]: %s", url, exc)
        return None

    if resp.status_code in (403, 429):
        time.sleep(RETRY_WAIT)
        try:
            resp = session.get(url, timeout=20)
        except requests.RequestException as exc:
            log.error("Retry error [%s]: %s", url, exc)
            return None

    if not resp.ok:
        log.error("HTTP %d [%s]", resp.status_code, url)
        return None

    return resp


def _log_skip(filename: str, reason: str) -> None:
    with open(SKIP_LOG, "a", encoding="utf-8") as f:
        f.write(f"SKIP\t{filename}\t{reason}\n")


# ── Step 1: Collect transcript URLs ───────────────────────────────────────────
def collect_urls() -> list[str]:
    """
    Paginate through all listing pages and collect unique transcript URLs.
    Filters for URLs containing '/india/' and 'transcript'.
    """
    urls: set[str] = set()
    print(f"Step 1: Crawling {TOTAL_PAGES} listing pages...")

    for page_num in tqdm(range(1, TOTAL_PAGES + 1), desc="Listing pages", unit="pg"):
        listing_url = LISTING_BASE.format(page_num)
        resp = _fetch(listing_url)
        if resp is None:
            log.error("Listing page failed: %s", listing_url)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")

        # Article links are inside <h2> tags within article cards
        for h2 in soup.find_all("h2"):
            a = h2.find("a", href=True)
            if not a:
                continue
            href = a["href"]
            if "/india/" in href and "transcript" in href.lower():
                urls.add(href)

    print(f"  Collected {len(urls)} unique transcript URLs.\n")
    return sorted(urls)


# ── Step 2: Parse ticker / exchange / company from title ─────────────────────
_TICKER_RE = re.compile(
    r'\((?P<exchange>NSE|BSE)\s*:\s*(?P<ticker>[A-Z0-9\-&]+)\)',
    re.IGNORECASE,
)


def _parse_title(title: str) -> tuple[str, str, str]:
    """
    Extract (company_name, ticker, exchange) from a title like:
      "Reliance Industries (NSE: RELIANCE) Q2 FY24 Earnings Call Transcript"
    Returns ('', '', '') if not found.
    """
    m = _TICKER_RE.search(title)
    if not m:
        return "", "", ""
    exchange = m.group("exchange").upper()
    ticker   = m.group("ticker").upper()
    # Company name = everything before the opening bracket
    company  = title[:m.start()].strip().rstrip("-—–").strip()
    return company, ticker, exchange


def _parse_date(soup: BeautifulSoup) -> str:
    """
    Try <time datetime="..."> first, then any <time> text, then meta tags.
    Returns YYYY-MM-DD or empty string.
    """
    time_tag = soup.find("time")
    if time_tag:
        dt_attr = time_tag.get("datetime", "")
        if dt_attr:
            return dt_attr[:10]   # ISO datetime → date only
        # Try parsing visible text
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(time_tag.get_text(strip=True), fmt).strftime("%Y-%m-%d")
            except ValueError:
                pass

    # Fallback: Open Graph / meta
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") + meta.get("name", "")
        if "published_time" in prop or "date" in prop.lower():
            content = meta.get("content", "")[:10]
            if re.match(r"\d{4}-\d{2}-\d{2}", content):
                return content

    return ""


def _extract_transcript_text(soup: BeautifulSoup) -> str:
    """
    Extract all <p> text from the main article body.
    Tries common WordPress content div classes in order.
    """
    for cls in ("entry-content", "post-content", "article-content", "content"):
        body = soup.find(class_=cls)
        if body:
            paras = body.find_all("p")
            if paras:
                return "\n\n".join(p.get_text(separator=" ", strip=True) for p in paras)

    # Fallback: all <p> tags on page that are reasonably long
    paras = [
        p.get_text(separator=" ", strip=True)
        for p in soup.find_all("p")
        if len(p.get_text(strip=True)) > 60
    ]
    return "\n\n".join(paras)


# ── Step 3: Scrape + save one transcript ─────────────────────────────────────
def scrape_transcript(url: str, stats: dict) -> None:
    stats["attempted"] += 1

    slug = url.rstrip("/").rsplit("/", 1)[-1]

    resp = _fetch(url)
    if resp is None:
        stats["failed"] += 1
        return

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else slug

    # Ticker / exchange / company
    company, ticker, exchange = _parse_title(title)

    # Date
    filing_date = _parse_date(soup)

    # Transcript text
    text = _extract_transcript_text(soup)
    word_count = len(text.split())

    # Quality filter
    if word_count < MIN_WORDS:
        _log_skip(slug, f"only {word_count} words")
        stats["skipped"] += 1
        return

    # Build filename
    date_str  = filing_date or "unknown"
    exch_str  = exchange or "UNKNOWN"
    tick_str  = ticker or _safe_filename(company[:20]) or "UNKNOWN"
    filename  = _safe_filename(f"{exch_str}_{tick_str}_{date_str}_{slug}")

    txt_path  = OUT_DIR / f"{filename}.txt"
    json_path = OUT_DIR / f"{filename}.json"

    # Write text
    txt_path.write_text(text, encoding="utf-8")

    # Write sidecar
    meta = {
        "company_name": company,
        "ticker":       ticker,
        "exchange":     exchange,
        "date":         filing_date,
        "source_url":   url,
        "title":        title,
    }
    json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    stats["saved"] += 1


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    stats = {"attempted": 0, "saved": 0, "skipped": 0, "failed": 0}

    # Step 1: collect all URLs
    transcript_urls = collect_urls()

    if not transcript_urls:
        print("No transcript URLs found. Check the listing page structure.")
        return

    # Step 2 + 3: scrape and save
    print(f"Step 2: Scraping {len(transcript_urls)} transcripts...")
    for url in tqdm(transcript_urls, desc="Transcripts", unit="t"):
        scrape_transcript(url, stats)

    # Summary
    print()
    print("─" * 40)
    print(f"  Attempted : {stats['attempted']}")
    print(f"  Saved     : {stats['saved']}")
    print(f"  Skipped   : {stats['skipped']}")
    print(f"  Failed    : {stats['failed']}")
    print(f"  Output    → {OUT_DIR}")
    print(f"  Fail log  → {FAIL_LOG}")
    print(f"  Skip log  → {SKIP_LOG}")
    print("─" * 40)


if __name__ == "__main__":
    main()
