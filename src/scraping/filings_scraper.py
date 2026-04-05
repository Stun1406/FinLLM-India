#!/usr/bin/env python3
"""
NSE/BSE Quarterly Results Scraper for NIFTY 100 companies.
Date range : 2019-01-01 to today.
NSE        : https://www.nseindia.com/api  (session-cookie required)
BSE        : https://api.bseindia.com/BseIndiaAPI/api  (public)
Output     : data/raw/filings/  (.pdf + sidecar .json per filing)

Verified working as of April 2026 via live endpoint testing.
"""

import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
OUT_DIR  = BASE_DIR / "data" / "raw" / "filings"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = OUT_DIR / "failed_downloads.log"

# ── Config ────────────────────────────────────────────────────────────────────
DATE_FROM = date(2019, 1, 1)
DATE_TO   = date.today()
DELAY     = 2   # seconds between HTTP requests

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.ERROR,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── NIFTY 100: (NSE_SYMBOL, BSE_SCRIP_CODE, COMPANY_NAME) ────────────────────
# Verified NSE symbols via live quote-equity API (April 2026):
#   INFOSYS  → INFY       (NSE symbol is INFY, not INFOSYS)
#   ZOMATO   → ETERNAL    (Zomato Ltd renamed to Eternal Ltd, Feb 2025)
NIFTY_100 = [
    ("RELIANCE",    "500325", "Reliance Industries"),
    ("TCS",         "532540", "Tata Consultancy Services"),
    ("HDFCBANK",    "500180", "HDFC Bank"),
    ("BHARTIARTL",  "532454", "Bharti Airtel"),
    ("ICICIBANK",   "532174", "ICICI Bank"),
    ("INFY",        "500209", "Infosys"),
    ("SBIN",        "500112", "State Bank of India"),
    ("HINDUNILVR",  "500696", "Hindustan Unilever"),
    ("ITC",         "500875", "ITC"),
    ("LT",          "500510", "Larsen and Toubro"),
    ("KOTAKBANK",   "500247", "Kotak Mahindra Bank"),
    ("AXISBANK",    "532215", "Axis Bank"),
    ("ASIANPAINT",  "500820", "Asian Paints"),
    ("BAJFINANCE",  "500034", "Bajaj Finance"),
    ("WIPRO",       "507685", "Wipro"),
    ("MARUTI",      "532500", "Maruti Suzuki"),
    ("HCLTECH",     "532281", "HCL Technologies"),
    ("SUNPHARMA",   "524715", "Sun Pharmaceutical"),
    ("ADANIENT",    "512599", "Adani Enterprises"),
    ("NTPC",        "532555", "NTPC"),
    ("POWERGRID",   "532898", "Power Grid Corporation"),
    ("ULTRACEMCO",  "532538", "UltraTech Cement"),
    ("TITAN",       "500114", "Titan Company"),
    ("TECHM",       "532755", "Tech Mahindra"),
    ("BAJAJFINSV",  "532978", "Bajaj Finserv"),
    ("ONGC",        "500312", "Oil and Natural Gas Corporation"),
    ("NESTLEIND",   "500790", "Nestle India"),
    ("JSWSTEEL",    "500228", "JSW Steel"),
    ("TATAMOTORS",  "500570", "Tata Motors"),
    ("M&M",         "500520", "Mahindra and Mahindra"),
    ("HINDALCO",    "500440", "Hindalco Industries"),
    ("DRREDDY",     "500124", "Dr Reddys Laboratories"),
    ("TATASTEEL",   "500470", "Tata Steel"),
    ("COALINDIA",   "533278", "Coal India"),
    ("GRASIM",      "500300", "Grasim Industries"),
    ("DIVISLAB",    "532488", "Divis Laboratories"),
    ("BRITANNIA",   "500825", "Britannia Industries"),
    ("CIPLA",       "500087", "Cipla"),
    ("ADANIPORTS",  "532921", "Adani Ports and SEZ"),
    ("BPCL",        "500547", "Bharat Petroleum Corporation"),
    ("HDFCLIFE",    "540777", "HDFC Life Insurance"),
    ("SBILIFE",     "540719", "SBI Life Insurance"),
    ("INDUSINDBK",  "532187", "IndusInd Bank"),
    ("EICHERMOT",   "505200", "Eicher Motors"),
    ("TATACONSUM",  "500800", "Tata Consumer Products"),
    ("APOLLOHOSP",  "508869", "Apollo Hospitals Enterprise"),
    ("BAJAJ-AUTO",  "532977", "Bajaj Auto"),
    ("SBICARD",     "543066", "SBI Cards and Payment Services"),
    ("HEROMOTOCO",  "500182", "Hero MotoCorp"),
    ("VEDL",        "500295", "Vedanta"),
    ("ICICIPRULI",  "540133", "ICICI Prudential Life Insurance"),
    ("ADANIGREEN",  "541450", "Adani Green Energy"),
    ("GODREJCP",    "532424", "Godrej Consumer Products"),
    ("DABUR",       "500096", "Dabur India"),
    ("TORNTPHARM",  "500420", "Torrent Pharmaceuticals"),
    ("PIDILITIND",  "500331", "Pidilite Industries"),
    ("SIEMENS",     "500550", "Siemens"),
    ("BOSCHLTD",    "500530", "Bosch"),
    ("BERGEPAINT",  "509480", "Berger Paints India"),
    ("MARICO",      "531642", "Marico"),
    ("COLPAL",      "500830", "Colgate-Palmolive India"),
    ("HAVELLS",     "517354", "Havells India"),
    ("DLF",         "532868", "DLF"),
    ("GAIL",        "532155", "GAIL India"),
    ("IOC",         "530965", "Indian Oil Corporation"),
    ("SAIL",        "500113", "Steel Authority of India"),
    ("BANKBARODA",  "532134", "Bank of Baroda"),
    ("CANBK",       "532483", "Canara Bank"),
    ("INDIGO",      "521064", "InterGlobe Aviation"),
    ("MUTHOOTFIN",  "533398", "Muthoot Finance"),
    ("SHREECEM",    "500387", "Shree Cement"),
    ("AMBUJACEM",   "500425", "Ambuja Cements"),
    ("ACC",         "500410", "ACC"),
    ("INDUSTOWER",  "534816", "Indus Towers"),
    ("LUPIN",       "500257", "Lupin"),
    ("BIOCON",      "532523", "Biocon"),
    ("AUROPHARMA",  "524804", "Aurobindo Pharma"),
    ("NAUKRI",      "535648", "Info Edge India"),
    ("ETERNAL",     "543320", "Eternal (Zomato)"),
    ("DMART",       "540376", "Avenue Supermarts"),
    ("JUBLFOOD",    "533155", "Jubilant Foodworks"),
    ("PAGEIND",     "532827", "Page Industries"),
    ("TRENT",       "500251", "Trent"),
    ("VOLTAS",      "500575", "Voltas"),
    ("ALKEM",       "539523", "Alkem Laboratories"),
    ("TORNTPOWER",  "532779", "Torrent Power"),
    ("CUMMINSIND",  "500480", "Cummins India"),
    ("PERSISTENT",  "533179", "Persistent Systems"),
    ("MPHASIS",     "526299", "Mphasis"),
    ("COFORGE",     "532541", "Coforge"),
    ("LTIM",        "540005", "LTIMindtree"),
    ("IRCTC",       "542830", "Indian Railway Catering and Tourism"),
    ("CHOLAFIN",    "500630", "Cholamandalam Investment and Finance"),
    ("PFC",         "532810", "Power Finance Corporation"),
    ("RECLTD",      "532955", "REC Limited"),
    ("LICI",        "543526", "Life Insurance Corporation of India"),
    ("VBL",         "477251", "Varun Beverages"),
    ("TVSMOTOR",    "532343", "TVS Motor Company"),
    ("POLYCAB",     "542652", "Polycab India"),
    ("MANKIND",     "543904", "Mankind Pharma"),
    ("ICICIGI",     "540716", "ICICI Lombard General Insurance"),
    ("JIOFIN",      "543940", "Jio Financial Services"),
]

# ── HTTP Sessions ─────────────────────────────────────────────────────────────
_COMMON_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    # 'br' (brotli) intentionally excluded — requests cannot decompress brotli,
    # causing empty/garbled responses from NSE which serves br by default.
    "Accept-Encoding": "gzip, deflate",
}

nse_session = requests.Session()
nse_session.headers.update({
    **_COMMON_HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
    # No X-Requested-With — NSE treats that header as AJAX and withholds session cookies
})

bse_session = requests.Session()
bse_session.headers.update({
    **_COMMON_HEADERS,
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.bseindia.com/",
})


def _init_nse_session() -> None:
    """
    Acquire NSE session cookies by visiting browser-like pages first.
    NSE uses Akamai bot detection — the 'nsit' cookie must be present
    before any API calls or the response body will be empty (HTTP 200, 0 bytes).
    Homepage may 403; the market-data page reliably sets the full cookie set.
    """
    pages = [
        "https://www.nseindia.com/",
        "https://www.nseindia.com/market-data/live-equity-market",
    ]
    for url in pages:
        try:
            nse_session.get(url, timeout=15)
        except Exception as exc:
            log.error("NSE session init [%s]: %s", url, exc)
        time.sleep(3)

    if "nsit" not in nse_session.cookies:
        log.error("NSE session: 'nsit' cookie missing after init — API calls will fail")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _safe_filename(name: str) -> str:
    """Strip characters illegal in Windows/Linux filenames."""
    return re.sub(r'[<>:"/\\|?*\s]', "_", name)


def _save_filing(
    content: bytes,
    ext: str,
    company: str,
    ticker: str,
    exchange: str,
    filing_date: str,
    filing_type: str,
    source_url: str,
) -> bool:
    """
    Write filing bytes + sidecar .json to OUT_DIR.
    4-digit URL hash appended to prevent same-date collisions.
    Returns True on success.
    """
    stem     = _safe_filename(f"{exchange}_{ticker}_{filing_date}_{filing_type}")
    uid      = abs(hash(source_url)) % 10_000
    filename = f"{stem}_{uid:04d}.{ext}"
    filepath = OUT_DIR / filename
    try:
        filepath.write_bytes(content)
        meta = {
            "company_name": company,
            "ticker":       ticker,
            "exchange":     exchange,
            "date":         filing_date,
            "filing_type":  filing_type,
            "source_url":   source_url,
        }
        filepath.with_suffix(".json").write_text(
            json.dumps(meta, indent=2), encoding="utf-8"
        )
        return True
    except OSError as exc:
        log.error("Write error [%s]: %s", filepath, exc)
        return False


def _download(url: str, session: requests.Session) -> tuple:
    """
    Download a URL. If the server returns HTML, BeautifulSoup scans for a
    nested .pdf anchor and fetches that instead.
    Returns (content: bytes, ext: str) or (None, '') on failure.
    """
    time.sleep(DELAY)
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.error("Download failed [%s]: %s", url, exc)
        return None, ""

    ct = resp.headers.get("Content-Type", "")

    if "pdf" in ct or url.lower().endswith(".pdf"):
        return resp.content, "pdf"

    if "html" in ct:
        soup = BeautifulSoup(resp.content, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" in href.lower():
                pdf_url = href if href.startswith("http") else (
                    url.rsplit("/", 1)[0] + "/" + href.lstrip("/")
                )
                time.sleep(DELAY)
                try:
                    pdf_resp = session.get(pdf_url, timeout=30)
                    pdf_resp.raise_for_status()
                    return pdf_resp.content, "pdf"
                except requests.RequestException:
                    pass
        return resp.content, "html"

    return resp.content, "html"


def _parse_bse_date(raw: str) -> str:
    """Parse BSE ISO-style datetime string to YYYY-MM-DD."""
    # BSE DT_TM format: "2023-07-21T19:36:59.62"
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw[:10]


# ── NSE ───────────────────────────────────────────────────────────────────────
# Verified endpoint (April 2026). Returns all announcement types;
# we filter by desc='Financial Result Updates'.
_NSE_API = "https://www.nseindia.com/api/corporate-announcements"

# NSE uses exactly this desc string for quarterly result filings.
# Confirmed via live inspection across RELIANCE, TCS, HDFCBANK, WIPRO etc.
_NSE_RESULT_DESC = "financial result"

# NSE date format in an_dt field: "10-Oct-2024 16:06:39"
_NSE_DATE_FMT = "%d-%b-%Y %H:%M:%S"


def _nse_filings(symbol: str) -> list:
    """
    Single API call per company for the full 2019→today date range.
    NSE accepts multi-year ranges — no need to chunk by year.
    Returns list of dicts: {date, pdf_url}.
    """
    params = {
        "index":     "equities",
        "symbol":    symbol,
        "from_date": DATE_FROM.strftime("%d-%m-%Y"),
        "to_date":   DATE_TO.strftime("%d-%m-%Y"),
    }
    time.sleep(DELAY)
    try:
        resp = nse_session.get(
            _NSE_API, params=params, timeout=20,
            headers={"Accept": "application/json, text/plain, */*"},
        )
        resp.raise_for_status()
        items = resp.json()
    except Exception as exc:
        log.error("NSE API [%s]: %s", symbol, exc)
        return []

    if not isinstance(items, list):
        items = items.get("data", [])

    results = []
    for item in items:
        desc = (item.get("desc") or "").lower()
        if _NSE_RESULT_DESC not in desc:
            continue
        pdf_url = item.get("attchmntFile") or ""
        # NSE sometimes returns literal "-" meaning no attachment
        if not pdf_url or pdf_url.strip() == "-":
            continue
        raw_dt = item.get("an_dt") or ""
        try:
            filing_date = datetime.strptime(raw_dt.strip(), _NSE_DATE_FMT).strftime("%Y-%m-%d")
        except ValueError:
            filing_date = raw_dt[:10]
        results.append({"date": filing_date, "pdf_url": pdf_url})
    return results


def scrape_nse(symbol: str, company: str, stats: dict) -> None:
    for filing in _nse_filings(symbol):
        stats["attempted"] += 1
        content, ext = _download(filing["pdf_url"], nse_session)
        if content is None:
            stats["failed"] += 1
            log.error("NSE download failed [%s] %s", symbol, filing["pdf_url"])
            continue
        ok = _save_filing(
            content=content, ext=ext, company=company, ticker=symbol,
            exchange="NSE", filing_date=filing["date"],
            filing_type="Quarterly_Results", source_url=filing["pdf_url"],
        )
        stats["success" if ok else "failed"] += 1


# ── BSE ───────────────────────────────────────────────────────────────────────
# Verified endpoint (April 2026).
# strCat=6 returns 0 rows — the category mapping changed on BSE.
# strCat=-1 (all categories) works; we paginate all pages and filter
# for CATEGORYNAME='Result' which is BSE's current category for financial results.
# PDF files are always at AttachHis/ — AttachLive/ returns 404 for all files tested.
_BSE_API      = "https://api.bseindia.com/BseIndiaAPI/api/AnnSubCategoryGetData/w"
_BSE_PDF_BASE = "https://www.bseindia.com/xml-data/corpfiling/AttachHis/"


def _bse_filings(scrip_code: str) -> list:
    """
    Fetch all quarterly result filings for a BSE scrip code across the full
    date range. Paginates all pages (50 rows/page) and filters for
    CATEGORYNAME='Result' rows only.
    Returns list of dicts: {date, pdf_url}.
    """
    results = []
    page = 1
    while True:
        params = {
            "pageno":      str(page),
            "strCat":      "-1",                              # all categories
            "strPrevDate": DATE_FROM.strftime("%Y%m%d"),
            "strScrip":    scrip_code,
            "strSearch":   "P",
            "strToDate":   DATE_TO.strftime("%Y%m%d"),
            "strType":     "C",
            "subcategory": "-1",
        }
        time.sleep(DELAY)
        try:
            resp = bse_session.get(_BSE_API, params=params, timeout=20)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            log.error("BSE API [%s page %d]: %s", scrip_code, page, exc)
            break

        rows = payload.get("Table", [])
        if not rows:
            break

        total_pages = int(rows[0].get("TotalPageCnt", 1))

        for row in rows:
            if row.get("CATEGORYNAME") != "Result":
                continue
            attachment = row.get("ATTACHMENTNAME") or ""
            if not attachment:
                continue
            results.append({
                "date":    _parse_bse_date(row.get("DT_TM", "")),
                "pdf_url": _BSE_PDF_BASE + attachment,
            })

        if page >= total_pages:
            break
        page += 1

    return results


def scrape_bse(symbol: str, bse_code: str, company: str, stats: dict) -> None:
    for filing in _bse_filings(bse_code):
        stats["attempted"] += 1
        content, ext = _download(filing["pdf_url"], bse_session)
        if content is None:
            stats["failed"] += 1
            log.error("BSE download failed [%s] %s", bse_code, filing["pdf_url"])
            continue
        ok = _save_filing(
            content=content, ext=ext, company=company, ticker=symbol,
            exchange="BSE", filing_date=filing["date"],
            filing_type="Quarterly_Results", source_url=filing["pdf_url"],
        )
        stats["success" if ok else "failed"] += 1


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    print(f"Output dir : {OUT_DIR}")
    print(f"Date range : {DATE_FROM}  →  {DATE_TO}")
    print(f"Companies  : {len(NIFTY_100)}")
    print()

    _init_nse_session()

    stats = {"attempted": 0, "success": 0, "failed": 0}

    for symbol, bse_code, company in tqdm(NIFTY_100, desc="Companies", unit="co"):
        scrape_nse(symbol, company, stats)
        scrape_bse(symbol, bse_code, company, stats)

    print()
    print("─" * 38)
    print(f"  Attempted : {stats['attempted']}")
    print(f"  Successful: {stats['success']}")
    print(f"  Failed    : {stats['failed']}")
    print(f"  Failure log → {LOG_FILE}")
    print("─" * 38)


if __name__ == "__main__":
    main()
