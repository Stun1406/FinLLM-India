#!/usr/bin/env python3
"""
Post-processing: fix ticker/exchange/company_name in transcript JSON sidecars
and rename the matching .txt + .json files accordingly.

No re-scraping. Reads and updates files in-place.
"""

import json
import re
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parents[2]
TRANS_DIR = BASE_DIR / "data" / "raw" / "transcripts"

# ── NIFTY 100 NSE tickers ─────────────────────────────────────────────────────
NIFTY_100_NSE = {
    "RELIANCE", "TCS", "HDFCBANK", "BHARTIARTL", "ICICIBANK",
    "INFY", "SBIN", "HINDUNILVR", "ITC", "LT",
    "KOTAKBANK", "AXISBANK", "ASIANPAINT", "BAJFINANCE", "WIPRO",
    "MARUTI", "HCLTECH", "SUNPHARMA", "ADANIENT", "NTPC",
    "POWERGRID", "ULTRACEMCO", "TITAN", "TECHM", "BAJAJFINSV",
    "ONGC", "NESTLEIND", "JSWSTEEL", "TATAMOTORS", "MM",
    "HINDALCO", "DRREDDY", "TATASTEEL", "COALINDIA", "GRASIM",
    "DIVISLAB", "BRITANNIA", "CIPLA", "ADANIPORTS", "BPCL",
    "HDFCLIFE", "SBILIFE", "INDUSINDBK", "EICHERMOT", "TATACONSUM",
    "APOLLOHOSP", "BAJAJAUTO", "SBICARD", "HEROMOTOCO", "VEDL",
    "ICICIPRULI", "ADANIGREEN", "GODREJCP", "DABUR", "TORNTPHARM",
    "PIDILITIND", "SIEMENS", "BOSCHLTD", "BERGEPAINT", "MARICO",
    "COLPAL", "HAVELLS", "DLF", "GAIL", "IOC",
    "SAIL", "BANKBARODA", "CANBK", "INDIGO", "MUTHOOTFIN",
    "SHREECEM", "AMBUJACEM", "ACC", "INDUSTOWER", "LUPIN",
    "BIOCON", "AUROPHARMA", "NAUKRI", "ETERNAL", "DMART",
    "JUBLFOOD", "PAGEIND", "TRENT", "VOLTAS", "ALKEM",
    "TORNTPOWER", "CUMMINSIND", "PERSISTENT", "MPHASIS", "COFORGE",
    "LTIM", "IRCTC", "CHOLAFIN", "PFC", "RECLTD",
    "LICI", "VBL", "TVSMOTOR", "POLYCAB", "MANKIND",
    "ICICIGI", "JIOFIN",
}

# ── Regex ─────────────────────────────────────────────────────────────────────
# Matches anything in the last pair of parentheses in the title
_PAREN_RE = re.compile(r'\(([^)]+)\)[^(]*$')


def _extract_ticker(title: str) -> str:
    """
    Extract ticker from title parentheses and normalise it.
    "Bajaj Auto Ltd. (BAJAJ-AUTO) Q4 FY21..." → "BAJAJAUTO"
    Takes the last parenthesised token, removes hyphens, uppercases.
    """
    m = _PAREN_RE.search(title)
    if not m:
        return ""
    raw = m.group(1).strip()
    # Remove hyphens and spaces, uppercase
    return re.sub(r"[-\s]", "", raw).upper()


def _extract_company(title: str) -> str:
    """
    Everything before the first opening parenthesis, stripped.
    "Bajaj Auto Ltd. (BAJAJ-AUTO) Q4..." → "Bajaj Auto Ltd."
    """
    idx = title.find("(")
    if idx == -1:
        return title.strip()
    return title[:idx].strip().rstrip(".-–—").strip()


def _safe_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\s]', "_", name)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    json_files = sorted(TRANS_DIR.glob("*.json"))
    # Exclude logs
    json_files = [f for f in json_files if f.stem not in ("failed_downloads", "skipped")]

    total      = len(json_files)
    updated    = 0
    renamed    = 0
    unresolved = 0

    print(f"Processing {total} JSON sidecars in {TRANS_DIR}\n")

    for json_path in json_files:
        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)

        title = meta.get("title", "")
        if not title:
            unresolved += 1
            continue

        # Parse ticker and company
        ticker  = _extract_ticker(title)
        company = _extract_company(title)

        if not ticker:
            unresolved += 1
            continue

        # Resolve exchange
        exchange = "NSE" if ticker in NIFTY_100_NSE else "BSE"

        # Update metadata
        meta["ticker"]       = ticker
        meta["exchange"]     = exchange
        meta["company_name"] = company

        # Write updated JSON in place
        json_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        updated += 1

        # ── Rename files if they start with UNKNOWN_UNKNOWN ────────────────
        stem = json_path.stem  # e.g. UNKNOWN_UNKNOWN_2021-06-04_bajaj-auto-...
        if stem.startswith("UNKNOWN_UNKNOWN_"):
            rest     = stem[len("UNKNOWN_UNKNOWN_"):]   # "2021-06-04_bajaj-auto-..."
            new_stem = _safe_filename(f"{exchange}_{ticker}_{rest}")

            new_json = json_path.with_name(f"{new_stem}.json")
            new_txt  = json_path.with_name(f"{new_stem}.txt")
            old_txt  = json_path.with_suffix(".txt")

            # Rename JSON
            json_path.rename(new_json)

            # Rename matching .txt if it exists
            if old_txt.exists():
                old_txt.rename(new_txt)

            renamed += 1

    print("─" * 42)
    print(f"  Total sidecars     : {total}")
    print(f"  Updated            : {updated}")
    print(f"  Renamed            : {renamed}")
    print(f"  Still unresolved   : {unresolved}")
    print("─" * 42)


if __name__ == "__main__":
    main()
