#!/usr/bin/env python3
"""
Unified dataset builder for FinLLM-India.

Reads all three processed corpora, applies a final quality filter,
and writes a HuggingFace-compatible Parquet dataset.

INPUT
  data/processed/filings/    *.txt + *.json
  data/processed/transcripts/ *.txt + *.json
  data/processed/sebi/        *.txt + *.json

OUTPUT
  data/dataset/finllm_india.parquet   — single flat file
  data/dataset/build_report.json      — stats per source + overall

Schema per row
  text      : str   — cleaned document text
  source    : str   — "filings" | "transcripts" | "sebi"
  doc_type  : str   — filing_type / "earnings_call" / "sebi_circular"
  date      : str   — ISO-8601 YYYY-MM-DD
  ticker    : str   — equity ticker or "" for SEBI docs
  title     : str   — human-readable title or ""
  word_count: int
"""

import json
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parents[2]
PROC_DIR    = BASE_DIR / "data" / "processed"
OUT_DIR     = BASE_DIR / "data" / "dataset"
OUT_DIR.mkdir(parents=True, exist_ok=True)

PARQUET_OUT = OUT_DIR / "finllm_india.parquet"
REPORT_OUT  = OUT_DIR / "build_report.json"

# Final quality filter applied across all sources
MIN_WORDS = 100


# ── Loaders ───────────────────────────────────────────────────────────────────

def _load_filings() -> list[dict]:
    src_dir = PROC_DIR / "filings"
    rows = []
    txts = sorted(src_dir.glob("*.txt"))
    for txt in tqdm(txts, desc="Filings    ", unit="doc"):
        json_path = txt.with_suffix(".json")
        if not json_path.exists():
            continue
        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)
        text = txt.read_text(encoding="utf-8", errors="replace").strip()
        wc = len(text.split())
        if wc < MIN_WORDS:
            continue
        rows.append({
            "text":       text,
            "source":     "filings",
            "doc_type":   meta.get("filing_type", ""),
            "date":       meta.get("date", ""),
            "ticker":     meta.get("ticker", ""),
            "title":      meta.get("filing_type", ""),
            "word_count": wc,
        })
    return rows


def _load_transcripts() -> list[dict]:
    src_dir = PROC_DIR / "transcripts"
    rows = []
    txts = sorted(src_dir.glob("*.txt"))
    for txt in tqdm(txts, desc="Transcripts", unit="doc"):
        json_path = txt.with_suffix(".json")
        if not json_path.exists():
            continue
        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)
        text = txt.read_text(encoding="utf-8", errors="replace").strip()
        wc = len(text.split())
        if wc < MIN_WORDS:
            continue
        rows.append({
            "text":       text,
            "source":     "transcripts",
            "doc_type":   "earnings_call",
            "date":       meta.get("date", ""),
            "ticker":     meta.get("ticker", ""),
            "title":      meta.get("title", ""),
            "word_count": wc,
        })
    return rows


def _load_sebi() -> list[dict]:
    src_dir = PROC_DIR / "sebi"
    rows = []
    txts = sorted(src_dir.glob("*.txt"))
    txts = [p for p in txts if not p.name.endswith(".log")]
    for txt in tqdm(txts, desc="SEBI       ", unit="doc"):
        json_path = txt.with_suffix(".json")
        if not json_path.exists():
            continue
        with open(json_path, encoding="utf-8") as f:
            meta = json.load(f)
        text = txt.read_text(encoding="utf-8", errors="replace").strip()
        wc = len(text.split())
        if wc < MIN_WORDS:
            continue
        rows.append({
            "text":       text,
            "source":     "sebi",
            "doc_type":   "sebi_circular",
            "date":       meta.get("date", ""),
            "ticker":     "",
            "title":      meta.get("title", ""),
            "word_count": wc,
        })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("FinLLM-India — Unified Dataset Builder")
    print(f"Output → {OUT_DIR}\n")

    all_rows: list[dict] = []

    filings     = _load_filings()
    transcripts = _load_transcripts()
    sebi        = _load_sebi()

    all_rows = filings + transcripts + sebi

    df = pd.DataFrame(all_rows)

    # Sort by date then source for deterministic ordering
    df = df.sort_values(["date", "source"], ascending=True).reset_index(drop=True)

    df.to_parquet(PARQUET_OUT, index=False, engine="pyarrow")

    # ── Report ────────────────────────────────────────────────────────────────
    source_stats = {}
    for src in ("filings", "transcripts", "sebi"):
        sub = df[df["source"] == src]
        source_stats[src] = {
            "docs":       int(len(sub)),
            "total_words": int(sub["word_count"].sum()),
            "avg_words":  int(sub["word_count"].mean()) if len(sub) else 0,
        }

    report = {
        "total_docs":   len(df),
        "total_words":  int(df["word_count"].sum()),
        "avg_words":    int(df["word_count"].mean()),
        "parquet_path": str(PARQUET_OUT),
        "sources":      source_stats,
    }
    REPORT_OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print()
    print("=" * 52)
    print(f"  {'Source':<14} {'Docs':>6}  {'Avg words':>10}  {'Total words':>12}")
    print(f"  {'-'*14}  {'-'*6}  {'-'*10}  {'-'*12}")
    for src, s in source_stats.items():
        print(f"  {src:<14} {s['docs']:>6,}  {s['avg_words']:>10,}  {s['total_words']:>12,}")
    print(f"  {'TOTAL':<14} {len(df):>6,}  {report['avg_words']:>10,}  {report['total_words']:>12,}")
    print("=" * 52)
    print(f"\n  Parquet → {PARQUET_OUT}")
    print("=" * 52)


if __name__ == "__main__":
    main()
