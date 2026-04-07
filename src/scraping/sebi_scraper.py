#!/usr/bin/env python3
"""
SEBI Circulars Scraper — FinLLM-India Phase 1.

SOURCE : https://www.sebi.gov.in/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0

How the site works (reverse-engineered):
  - Listing page loads 25 rows; pagination is AJAX POST to
    /sebiweb/ajax/home/getnewslistinfo.jsp
  - Each row links to an HTML detail page (not a PDF directly)
  - Detail page embeds an <iframe src="../../../web/?file=https://...pdf">
  - We parse the ?file= parameter to get the real PDF URL

Flow:
  1. POST to AJAX endpoint with fromDate/toDate filter, paginate via nextValue
  2. Parse each page's HTML fragment → extract (title, date, detail_url)
  3. Fetch detail page → extract PDF URL from iframe
  4. Download PDF → data/raw/sebi/
  5. Extract text via PyMuPDF → .txt file
  6. Save .json sidecar

Output : data/raw/sebi/
Logs   : data/raw/sebi/failed_downloads.log
         data/raw/sebi/skipped.log
"""

import json
import re
import time
from datetime import datetime, date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "sebi"
OUT_DIR.mkdir(parents=True, exist_ok=True)

FAIL_LOG = OUT_DIR / "failed_downloads.log"
SKIP_LOG = OUT_DIR / "skipped.log"

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE   = date(2019, 1, 1)
END_DATE     = date(2024, 12, 31)
MIN_WORDS    = 100
REQ_DELAY    = 2
RETRY_WAIT   = 30
PAGE_SIZE    = 25   # SEBI returns 25 rows per AJAX page

BASE_URL     = "https://www.sebi.gov.in"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept":          "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection":      "keep-alive",
}


SESSION = requests.Session()
SESSION.headers.update(HEADERS)


# ── Logging ───────────────────────────────────────────────────────────────────
def _log_fail(identifier: str, reason: str) -> None:
    with open(FAIL_LOG, "a", encoding="utf-8") as f:
        f.write(f"FAIL\t{identifier}\t{reason}\n")


def _log_skip(identifier: str, reason: str) -> None:
    with open(SKIP_LOG, "a", encoding="utf-8") as f:
        f.write(f"SKIP\t{identifier}\t{reason}\n")


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def _get(url: str, stream: bool = False) -> requests.Response | None:
    for attempt in range(2):
        try:
            r = SESSION.get(url, timeout=30, stream=stream)
            if r.status_code in (403, 429):
                if attempt == 0:
                    print(f"  [{r.status_code}] Waiting {RETRY_WAIT}s...")
                    time.sleep(RETRY_WAIT)
                    continue
                _log_fail(url, f"HTTP {r.status_code} after retry")
                return None
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            if attempt == 0:
                time.sleep(RETRY_WAIT)
                continue
            _log_fail(url, str(exc))
            return None
    return None


# ── Date helpers ──────────────────────────────────────────────────────────────
def _parse_date(raw: str) -> date | None:
    raw = raw.strip()
    for fmt in ("%b %d, %Y", "%d-%b-%Y", "%d/%m/%Y", "%B %d, %Y",
                "%d %b %Y", "%Y-%m-%d", "%b %d,%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass
    # Try regex extraction for "Mar 25, 2026" style
    m = re.search(r"([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})", raw)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", "%b %d %Y").date()
        except ValueError:
            pass
    return None


# ── Filename helpers ──────────────────────────────────────────────────────────
def _circular_slug(title: str, detail_url: str) -> str:
    """Extract circular number from title or fall back to URL slug."""
    m = re.search(r"(SEBI[/\-][A-Z0-9/\-]+)", title, re.I)
    if m:
        slug = re.sub(r"[/\\]", "-", m.group(1))
        return re.sub(r"-{2,}", "-", slug).strip("-")[:60]
    # Fallback: last path segment of detail URL
    seg = urlparse(detail_url).path.rstrip("/").rsplit("/", 1)[-1]
    seg = re.sub(r"[^a-zA-Z0-9\-]", "-", seg)
    return re.sub(r"-{2,}", "-", seg).strip("-")[:60]


# ── Step 1: Paginate AJAX listing ─────────────────────────────────────────────
def collect_circulars() -> list[dict]:
    """
    Paginate SEBI circular listing via AJAX endpoint.
    nextValue=N returns rows 25N+1 to 25N+25 (0-indexed page number).
    Total: 2762 records = 111 pages. Filter to 2019-2024 date range.
    """
    circulars: list[dict] = []
    seen: set[str] = set()

    # Establish session + grab STRUTS token
    r0 = SESSION.get(
        f"{BASE_URL}/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
        timeout=30
    )
    soup0    = BeautifulSoup(r0.text, "html.parser")
    token_el = soup0.find("input", {"name": "org.apache.struts.taglib.html.TOKEN"})
    token    = token_el["value"] if token_el else ""

    # Parse first page from the GET response directly (rows 1-25)
    _parse_listing_rows(soup0, circulars, seen)

    # Total pages from "X to Y of Z records" string
    total_records = 2762  # fallback
    for t in soup0.find_all(string=re.compile(r"\d+ to \d+ of \d+ records")):
        m = re.search(r"of (\d+) records", t)
        if m:
            total_records = int(m.group(1))
            break

    total_pages = (total_records + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"Step 1: Paginating {total_records} circulars ({total_pages} pages)...")

    ajax_headers = {
        **HEADERS,
        "Accept":           "*/*",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type":     "application/x-www-form-urlencoded",
        "Referer":          f"{BASE_URL}/sebiweb/home/HomeAction.do?doListing=yes&sid=1&ssid=7&smid=0",
    }

    for page_idx in range(1, total_pages + 1):
        time.sleep(REQ_DELAY)

        try:
            r = SESSION.post(
                f"{BASE_URL}/sebiweb/ajax/home/getnewslistinfo.jsp",
                data={
                    "nextValue":  str(page_idx),
                    "next":       "n",
                    "search":     "",
                    "fromDate":   "",
                    "toDate":     "",
                    "fromYear":   "",
                    "toYear":     "",
                    "deptId":     "",
                    "sid":        "1",
                    "ssid":       "7",
                    "smid":       "0",
                    "ssidhidden": "7",
                    "intmid":     "-1",
                    "sText":      "Legal",
                    "ssText":     "Circulars",
                    "smText":     "",
                    "doDirect":   str(page_idx),
                    "org.apache.struts.taglib.html.TOKEN": token,
                },
                headers=ajax_headers,
                timeout=30,
            )
            r.raise_for_status()
        except requests.RequestException as exc:
            _log_fail(f"AJAX page {page_idx}", str(exc))
            continue

        table_html = r.text.split("#@#")[0]
        soup       = BeautifulSoup(table_html, "html.parser")
        before     = len(circulars)
        oldest     = _parse_listing_rows(soup, circulars, seen)

        print(f"  Page {page_idx:3d}/{total_pages} — +{len(circulars)-before} in range — total: {len(circulars)}")

        # Stop early once entire page is pre-2019
        if oldest is not None and oldest < START_DATE:
            print(f"  Reached pre-{START_DATE.year} records — stopping early")
            break

    print(f"\n  Total circulars collected: {len(circulars)}\n")
    return circulars


def _parse_listing_rows(soup: BeautifulSoup,
                         circulars: list,
                         seen: set) -> date | None:
    """
    Parse circular entries from a listing page soup.
    Appends to circulars in-place. Returns the oldest date seen (or None).
    """
    oldest: date | None = None

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue

        date_text   = cells[0].get_text(strip=True)
        parsed_date = _parse_date(date_text)
        if parsed_date is None:
            continue

        if oldest is None or parsed_date < oldest:
            oldest = parsed_date

        a_tag = cells[1].find("a", href=True)
        if not a_tag:
            continue

        href       = a_tag["href"]
        detail_url = href if href.startswith("http") else BASE_URL + href

        if detail_url in seen:
            continue
        seen.add(detail_url)

        title = a_tag.get("title", "").strip() or a_tag.get_text(strip=True)

        if START_DATE <= parsed_date <= END_DATE:
            circulars.append({
                "title":      title,
                "date":       parsed_date,
                "detail_url": detail_url,
            })

    return oldest


# ── Step 2: Get PDF URL from detail page ──────────────────────────────────────
def get_pdf_url(detail_url: str) -> str | None:
    """
    Fetch circular detail page, extract PDF URL from iframe.
    iframe src format: ../../../web/?file=https://www.sebi.gov.in/sebi_data/attachdocs/.../xxx.pdf
    """
    time.sleep(REQ_DELAY)
    r = _get(detail_url)
    if r is None:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Look for iframe with ?file= param
    iframe = soup.find("iframe", src=re.compile(r"\?file=", re.I))
    if iframe:
        src = iframe["src"]
        m = re.search(r"\?file=(https?://[^\s\"']+\.pdf)", src, re.I)
        if m:
            return m.group(1)

    # Fallback: any direct .pdf link
    a = soup.find("a", href=re.compile(r"sebi_data.*\.pdf", re.I))
    if a:
        href = a["href"]
        return href if href.startswith("http") else BASE_URL + href

    return None


# ── Step 3: Download PDF ──────────────────────────────────────────────────────
def download_pdf(circular: dict) -> Path | None:
    date_str = circular["date"].strftime("%Y-%m-%d")
    slug     = _circular_slug(circular["title"], circular["detail_url"])
    filename = f"SEBI_{date_str}_{slug}"
    pdf_path = OUT_DIR / f"{filename}.pdf"

    if pdf_path.exists():
        return pdf_path  # already downloaded

    pdf_url = circular.get("pdf_url")
    if not pdf_url:
        _log_fail(circular["detail_url"], "no PDF URL found on detail page")
        return None

    time.sleep(REQ_DELAY)
    r = _get(pdf_url, stream=True)
    if r is None:
        return None

    try:
        with open(pdf_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    except (requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError) as exc:
        pdf_path.unlink(missing_ok=True)
        _log_fail(pdf_url, f"download error: {exc}")
        return None

    sidecar = {
        "title":      circular["title"],
        "date":       date_str,
        "source_url": pdf_url,
        "detail_url": circular["detail_url"],
    }
    (OUT_DIR / f"{filename}.json").write_text(
        json.dumps(sidecar, indent=2), encoding="utf-8"
    )

    return pdf_path


# ── Step 4: Extract text ──────────────────────────────────────────────────────
def extract_text(pdf_path: Path) -> bool:
    txt_path = pdf_path.with_suffix(".txt")
    if txt_path.exists():
        return True

    try:
        doc  = fitz.open(str(pdf_path))
        text = "\n".join(page.get_text() for page in doc).strip()
        doc.close()
    except Exception as exc:
        _log_fail(str(pdf_path), f"PyMuPDF error: {exc}")
        return False

    words = text.split()
    if len(words) < MIN_WORDS:
        _log_skip(str(pdf_path), f"only {len(words)} words")
        return False

    txt_path.write_text(text, encoding="utf-8")
    return True


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    FAIL_LOG.unlink(missing_ok=True)
    SKIP_LOG.unlink(missing_ok=True)

    print("SEBI Circulars Scraper")
    print(f"Date range : {START_DATE} – {END_DATE}")
    print(f"Output     : {OUT_DIR}\n")

    circulars = collect_circulars()

    if not circulars:
        print("No circulars found. Check network or SEBI site structure.")
        return

    stats = {"pdf_found": 0, "downloaded": 0, "extracted": 0, "skipped": 0, "failed": 0}

    print("Step 2: Fetching detail pages for PDF URLs...\n")

    for c in tqdm(circulars, desc="Getting PDF URLs", unit="circ"):
        pdf_url = get_pdf_url(c["detail_url"])
        c["pdf_url"] = pdf_url
        if pdf_url:
            stats["pdf_found"] += 1

    print(f"\n  PDF URLs found: {stats['pdf_found']} / {len(circulars)}")

    print("\nStep 3+4: Downloading PDFs and extracting text...\n")

    for circular in tqdm(circulars, desc="Downloading", unit="doc"):
        if not circular.get("pdf_url"):
            stats["failed"] += 1
            continue

        pdf_path = download_pdf(circular)
        if pdf_path is None:
            stats["failed"] += 1
            continue

        stats["downloaded"] += 1

        if extract_text(pdf_path):
            stats["extracted"] += 1
        else:
            stats["skipped"] += 1

    print()
    print("═" * 44)
    print(f"  Total found     : {len(circulars):,}")
    print(f"  PDF URLs found  : {stats['pdf_found']:,}")
    print(f"  Downloaded      : {stats['downloaded']:,}")
    print(f"  Text extracted  : {stats['extracted']:,}")
    print(f"  Skipped (<{MIN_WORDS}w) : {stats['skipped']:,}")
    print(f"  Failed          : {stats['failed']:,}")
    print(f"  Output          → {OUT_DIR}")
    print("═" * 44)


if __name__ == "__main__":
    main()
