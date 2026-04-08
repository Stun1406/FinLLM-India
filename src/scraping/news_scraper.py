#!/usr/bin/env python3
"""
Indian financial news scraper for FinLLM-India.

Sources (scraping allowed per robots.txt):
  1. Economic Times — User-agent: * Allow: /  (sitemap 2010-present)
     MoneyControl dropped: Akamai WAF blocks sitemap XMLs despite robots.txt Allow.

robots.txt checked 2026-04-08.

OUTPUT : data/raw/news/
  ET_{date}_{hash8}.txt  +  ET_{date}_{hash8}.json
  MC_{date}_{hash8}.txt  +  MC_{date}_{hash8}.json
  failed.log
  scrape_report.json
"""

import hashlib
import json
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from xml.etree import ElementTree as ETree

import requests
import trafilatura
from tqdm import tqdm

# ── Configuration ─────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "news"
OUT_DIR.mkdir(parents=True, exist_ok=True)

START_DATE   = date(2019, 1, 1)
END_DATE     = date(2024, 12, 31)
MAX_ARTICLES = 60_000      # stop once this many articles are saved
WORKERS      = 4           # parallel fetchers
MIN_WORDS    = 150         # drop articles shorter than this
REQ_DELAY    = (0.4, 1.2)  # per-worker sleep range (seconds)
TIMEOUT      = 20
MAX_RETRIES  = 2

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Economic Times ─────────────────────────────────────────────────────────────
# Sitemap pattern: YYYY-MonthName-{1|2}.xml, two halves per month
ET_SITEMAP_BASE = (
    "https://economictimes.indiatimes.com/etstatic/sitemaps/et/news"
)
MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

# Blacklist: URL path prefixes for clearly non-financial content
ET_BLACKLIST = [
    "/news/sports/",
    "/panache/",                    # lifestyle
    "/news/politics-and-nation/",
    "/news/international/",
    "/news/defence/",
    "/news/science/",
    "/news/education/",
    "/news/environment/",
    "/slideshows/",
    "/videoshow/",
    "/podcasts/",
    "/prime/",                      # paywalled
    "/hindi/",
    "/news/elections/",
    "/news/company/corporate-trends/",
]

# ── XML namespace maps ─────────────────────────────────────────────────────────
NS_SM   = "http://www.sitemaps.org/schemas/sitemap/0.9"
NS_NEWS = "http://www.google.com/schemas/sitemap-news/0.9"


# ── Shared state (thread-safe) ─────────────────────────────────────────────────
_lock          = threading.Lock()
_saved_count   = 0
_failed_count  = 0
_skipped_count = 0


# ── Helpers ───────────────────────────────────────────────────────────────────

def _url_hash(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:8]


def _sleep():
    time.sleep(random.uniform(*REQ_DELAY))


def _fetch_raw(url: str) -> requests.Response | None:
    """GET with retries; returns None on permanent failure."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code == 200:
                return r
            if r.status_code in (403, 404, 410):
                return None
        except requests.RequestException:
            pass
        if attempt < MAX_RETRIES:
            time.sleep(2 ** attempt + random.uniform(0, 1))
    return None


def _already_saved(url: str) -> bool:
    return bool(list(OUT_DIR.glob(f"*_{_url_hash(url)}.txt")))


def _parse_date(s: str) -> date | None:
    """Parse ISO-8601 date string → date object."""
    if not s:
        return None
    try:
        return datetime.fromisoformat(s[:10]).date()
    except ValueError:
        return None


def _in_range(d: date | None) -> bool:
    if d is None:
        return True  # unknown date: include and let trafilatura decide
    return START_DATE <= d <= END_DATE


def _is_blacklisted(url: str, blacklist: list[str]) -> bool:
    path = "/" + url.split("/", 3)[-1].split("?")[0]
    return any(path.startswith(b) or b in path for b in blacklist)


def _log_fail(url: str, reason: str) -> None:
    with _lock:
        with open(OUT_DIR / "failed.log", "a", encoding="utf-8") as f:
            f.write(f"{url}\t{reason}\n")


def _save_article(prefix: str, url: str, text: str, meta: dict) -> None:
    h     = _url_hash(url)
    dstr  = meta.get("date", "0000-00-00")
    stem  = f"{prefix}_{dstr}_{h}"
    (OUT_DIR / f"{stem}.txt").write_text(text, encoding="utf-8")
    (OUT_DIR / f"{stem}.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )


# ── Sitemap parsing ───────────────────────────────────────────────────────────

def _parse_urlset(xml_text: str) -> list[tuple[str, str]]:
    """
    Parse a sitemap <urlset> XML.
    Returns list of (url, lastmod_str).
    """
    results = []
    try:
        root = ETree.fromstring(xml_text)
    except ETree.ParseError:
        return results

    for url_el in root.findall(f"{{{NS_SM}}}url"):
        loc = url_el.findtext(f"{{{NS_SM}}}loc", "").strip()
        if not loc:
            continue
        # Try <lastmod> first, then <news:publication_date>
        lastmod = url_el.findtext(f"{{{NS_SM}}}lastmod", "").strip()
        if not lastmod:
            news_el = url_el.find(f"{{{NS_NEWS}}}news")
            if news_el is not None:
                lastmod = news_el.findtext(
                    f"{{{NS_NEWS}}}publication_date", ""
                ).strip()
        results.append((loc, lastmod))
    return results


def _parse_sitemapindex(xml_text: str) -> list[str]:
    """Parse a <sitemapindex> XML → list of child sitemap URLs."""
    results = []
    try:
        root = ETree.fromstring(xml_text)
    except ETree.ParseError:
        return results
    for sm_el in root.findall(f"{{{NS_SM}}}sitemap"):
        loc = sm_el.findtext(f"{{{NS_SM}}}loc", "").strip()
        if loc:
            results.append(loc)
    return results


# ── URL collection ────────────────────────────────────────────────────────────

def collect_et_urls() -> list[tuple[str, str, str]]:
    """
    Collect ET article URLs for START_DATE–END_DATE.
    Returns list of (url, date_str, prefix="ET").
    """
    print("  [ET] Building monthly sitemap URLs for 2019–2024...")
    sitemap_urls = []
    for yr in range(START_DATE.year, END_DATE.year + 1):
        for mo_name in MONTH_NAMES:
            for half in (1, 2):
                sitemap_urls.append(
                    f"{ET_SITEMAP_BASE}/{yr}-{mo_name}-{half}.xml"
                )

    articles = []
    seen_urls: set[str] = set()

    for sm_url in tqdm(sitemap_urls, desc="  ET sitemaps", unit="xml"):
        _sleep()
        r = _fetch_raw(sm_url)
        if r is None:
            continue
        for art_url, lastmod in _parse_urlset(r.text):
            if art_url in seen_urls:
                continue
            d = _parse_date(lastmod)
            if not _in_range(d):
                continue
            if _is_blacklisted(art_url, ET_BLACKLIST):
                continue
            seen_urls.add(art_url)
            articles.append((art_url, lastmod[:10] if lastmod else "", "ET"))

    print(f"  [ET] {len(articles):,} candidate URLs collected")
    return articles


# ── Article fetching ──────────────────────────────────────────────────────────

def fetch_and_save(url: str, date_str: str, prefix: str) -> str:
    """
    Fetch one article, extract text, save.
    Returns one of: "saved" | "skip_exists" | "skip_short" | "fail"
    """
    global _saved_count

    with _lock:
        if _saved_count >= MAX_ARTICLES:
            return "limit"

    if _already_saved(url):
        return "skip_exists"

    _sleep()
    r = _fetch_raw(url)
    if r is None:
        _log_fail(url, "fetch_failed")
        return "fail"

    text = trafilatura.extract(
        r.text,
        include_comments=False,
        include_tables=False,
        no_fallback=False,
        favor_recall=True,
    )
    if not text:
        _log_fail(url, "extract_empty")
        return "fail"

    word_count = len(text.split())
    if word_count < MIN_WORDS:
        return "skip_short"

    # Extract metadata via trafilatura
    tmeta = trafilatura.extract_metadata(r.text)
    art_date = date_str
    if tmeta and tmeta.date:
        art_date = tmeta.date[:10]

    meta = {
        "source":    prefix,
        "url":       url,
        "date":      art_date,
        "title":     (tmeta.title if tmeta and tmeta.title else ""),
        "word_count": word_count,
    }

    _save_article(prefix, url, text, meta)

    with _lock:
        _saved_count += 1

    return "saved"


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    global _saved_count, _failed_count, _skipped_count

    print("FinLLM-India — Financial News Scraper")
    print(f"Date range  : {START_DATE} – {END_DATE}")
    print(f"Max articles: {MAX_ARTICLES:,}")
    print(f"Output      : {OUT_DIR}\n")

    # ── Step 1: Collect candidate URLs ────────────────────────────────────────
    print("Step 1: Collecting article URLs from sitemaps...")
    et_urls = collect_et_urls()

    all_urls = et_urls
    # Sort chronologically (oldest → newest)
    all_urls.sort(key=lambda x: x[1])

    # Remove URLs already downloaded
    pending = [(u, d, p) for u, d, p in all_urls if not _already_saved(u)]
    already_done = len(all_urls) - len(pending)
    _saved_count = already_done  # count toward MAX_ARTICLES

    print(f"\n  Total candidates : {len(all_urls):,}")
    print(f"  Already saved    : {already_done:,}")
    print(f"  To fetch         : {len(pending):,}")
    print(f"  Target remaining : {max(0, MAX_ARTICLES - already_done):,}\n")

    if _saved_count >= MAX_ARTICLES:
        print("Target already reached — nothing to do.")
        return

    # ── Step 2: Fetch articles ─────────────────────────────────────────────
    print("Step 2: Fetching articles...")

    saved   = already_done
    failed  = 0
    skipped = 0

    with tqdm(total=min(len(pending), MAX_ARTICLES - already_done),
              desc="Fetching", unit="art") as pbar:
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            futures = {
                pool.submit(fetch_and_save, u, d, p): (u, d, p)
                for u, d, p in pending
            }
            for fut in as_completed(futures):
                result = fut.result()
                if result == "saved":
                    saved += 1
                    pbar.update(1)
                elif result == "fail":
                    failed += 1
                elif result in ("skip_short", "skip_exists"):
                    skipped += 1
                elif result == "limit":
                    pbar.update(0)
                    # Cancel remaining — limit reached
                    for f in futures:
                        f.cancel()
                    break

    # ── Report ────────────────────────────────────────────────────────────────
    et_saved = len(list(OUT_DIR.glob("ET_*.txt")))

    report = {
        "total_saved":    saved,
        "et_articles":    et_saved,
        "failed":         failed,
        "skipped":        skipped,
        "candidate_urls": len(all_urls),
        "date_range":     f"{START_DATE} – {END_DATE}",
        "output_dir":     str(OUT_DIR),
    }
    (OUT_DIR / "scrape_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    print()
    print("=" * 48)
    print(f"  Saved (ET)   : {et_saved:,}")
    print(f"  Failed       : {failed:,}")
    print(f"  Skipped      : {skipped:,}")
    print(f"  Output       → {OUT_DIR}")
    print("=" * 48)


if __name__ == "__main__":
    main()
