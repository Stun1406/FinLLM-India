#!/usr/bin/env python3
"""
HuggingFace Dataset builder for FinLLM-India.

Combines all four preprocessed corpora into a single Dataset with
stratified 95/5 train/validation splits.

INPUT
  data/processed/filings/         ← NSE/BSE quarterly results
  data/processed/transcripts/     ← earnings call transcripts
  data/processed/sebi/            ← SEBI circulars
  data/processed/news/            ← Economic Times articles

OUTPUT
  data/datasets/finllm-india/     ← HuggingFace DatasetDict (Arrow)
  data/datasets/finllm-india/dataset_card.md
  data/datasets/finllm-india/dataset_report.json

Schema per record
  text       : str — full cleaned document text
  source     : str — "filings" | "transcripts" | "sebi" | "news"
  ticker     : str — equity ticker or ""
  company    : str — company name or ""
  exchange   : str — NSE / BSE / SEBI / ET or ""
  date       : str — YYYY-MM-DD or ""
  title      : str — document title or ""
  source_url : str — original URL or ""
  word_count : int — word count of text
"""

import json
from datetime import date
from pathlib import Path

from datasets import ClassLabel, Dataset, DatasetDict, Value
from tqdm import tqdm

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR  = Path(__file__).resolve().parents[2]
PROC_DIR  = BASE_DIR / "data" / "processed"
OUT_DIR   = BASE_DIR / "data" / "datasets" / "finllm-india"
OUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_WORDS = 50
SPLIT_SEED     = 42
VAL_FRACTION   = 0.05


# ── Field mappers (source → schema) ──────────────────────────────────────────

def _map_filings(meta: dict) -> dict:
    return {
        "ticker":     meta.get("ticker", ""),
        "company":    meta.get("company_name", ""),
        "exchange":   meta.get("exchange", ""),
        "date":       meta.get("date", ""),
        "title":      meta.get("filing_type", ""),
        "source_url": meta.get("source_url", ""),
    }


def _map_transcripts(meta: dict) -> dict:
    return {
        "ticker":     meta.get("ticker", ""),
        "company":    meta.get("company_name", ""),
        "exchange":   meta.get("exchange", ""),
        "date":       meta.get("date", ""),
        "title":      meta.get("title", ""),
        "source_url": meta.get("source_url", ""),
    }


def _map_sebi(meta: dict) -> dict:
    return {
        "ticker":     "",
        "company":    "",
        "exchange":   "SEBI",
        "date":       meta.get("date", ""),
        "title":      meta.get("title", ""),
        "source_url": meta.get("source_url", meta.get("detail_url", "")),
    }


def _map_news(meta: dict) -> dict:
    return {
        "ticker":     "",
        "company":    "",
        "exchange":   "ET",
        "date":       meta.get("date", ""),
        "title":      meta.get("title", ""),
        "source_url": meta.get("url", meta.get("source_url", "")),
    }


MAPPERS = {
    "filings":     _map_filings,
    "transcripts": _map_transcripts,
    "sebi":        _map_sebi,
    "news":        _map_news,
}

GLOBS = {
    "filings":     "*.txt",
    "transcripts": "*.txt",
    "sebi":        "*.txt",
    "news":        "ET_*.txt",
}


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_source(source: str) -> list[dict]:
    src_dir = PROC_DIR / source
    mapper  = MAPPERS[source]
    pattern = GLOBS[source]

    txts = sorted(p for p in src_dir.glob(pattern)
                  if not p.name.endswith(".log"))

    rows: list[dict] = []
    for txt_path in tqdm(txts, desc=f"  {source:<12}", unit="doc"):
        text = txt_path.read_text(encoding="utf-8", errors="replace").strip()
        wc   = len(text.split())
        if wc < MIN_WORDS:
            continue

        json_path = txt_path.with_suffix(".json")
        meta: dict = {}
        if json_path.exists():
            try:
                with open(json_path, encoding="utf-8") as f:
                    meta = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        mapped = mapper(meta)
        rows.append({
            "text":       text,
            "source":     source,
            "word_count": wc,
            **mapped,
        })

    return rows


# ── Dataset card ──────────────────────────────────────────────────────────────

def _write_dataset_card(
    stats: dict,
    train_size: int,
    val_size: int,
) -> None:
    lines = [
        "# FinLLM-India Dataset",
        "",
        "Domain-adapted pre-training corpus for Indian equity markets.",
        "Built from four sources covering 2019–2024.",
        "",
        "## Schema",
        "",
        "| Field | Type | Description |",
        "|---|---|---|",
        "| text | str | Full cleaned document text |",
        "| source | str | filings / transcripts / sebi / news |",
        "| ticker | str | Equity ticker (empty for sebi/news) |",
        "| company | str | Company name (empty for sebi/news) |",
        "| exchange | str | NSE / BSE / SEBI / ET |",
        "| date | str | Publication date YYYY-MM-DD |",
        "| title | str | Document title |",
        "| source_url | str | Original URL |",
        "| word_count | int | Word count of text |",
        "",
        "## Splits",
        "",
        f"| Split | Records |",
        f"|---|---|",
        f"| train | {train_size:,} |",
        f"| validation | {val_size:,} |",
        "",
        "## Sources",
        "",
        "| Source | Docs | Total words | Avg words | Date range |",
        "|---|---|---|---|---|",
    ]

    for src, s in stats.items():
        lines.append(
            f"| {src} | {s['docs']:,} | {s['total_words']:,} | "
            f"{s['avg_words']:,} | {s['date_range']} |"
        )

    lines += [
        "",
        "## Usage",
        "",
        "```python",
        "from datasets import load_from_disk",
        'ds = load_from_disk("data/datasets/finllm-india")',
        "print(ds)  # DatasetDict with train / validation splits",
        "```",
    ]

    (OUT_DIR / "dataset_card.md").write_text(
        "\n".join(lines), encoding="utf-8"
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("FinLLM-India — HuggingFace Dataset Builder")
    print(f"Output → {OUT_DIR}\n")

    # ── Load all sources ──────────────────────────────────────────────────────
    all_rows: list[dict] = []
    source_rows: dict[str, list[dict]] = {}

    for source in ("filings", "transcripts", "sebi", "news"):
        print(f"Loading {source}...")
        rows = _load_source(source)
        source_rows[source] = rows
        all_rows.extend(rows)
        print(f"  → {len(rows):,} records\n")

    print(f"Total records: {len(all_rows):,}")

    # ── Build HuggingFace Dataset ─────────────────────────────────────────────
    print("\nBuilding Dataset...")
    ds = Dataset.from_list(all_rows)

    # ── Stratified split ──────────────────────────────────────────────────────
    # train_test_split requires ClassLabel for stratification, not plain string
    print("Splitting 95/5 train/val (stratified by source)...")
    ds_labeled = ds.class_encode_column("source")
    split = ds_labeled.train_test_split(
        test_size=VAL_FRACTION,
        seed=SPLIT_SEED,
        stratify_by_column="source",
    )
    dataset_dict = DatasetDict({
        "train":      split["train"].cast_column("source", Value("string")),
        "validation": split["test"].cast_column("source", Value("string")),
    })

    train_size = len(dataset_dict["train"])
    val_size   = len(dataset_dict["validation"])
    print(f"  train:      {train_size:,}")
    print(f"  validation: {val_size:,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    print(f"\nSaving to {OUT_DIR}...")
    dataset_dict.save_to_disk(str(OUT_DIR))

    # ── Per-source stats ──────────────────────────────────────────────────────
    source_stats: dict[str, dict] = {}
    for source, rows in source_rows.items():
        if not rows:
            source_stats[source] = {
                "docs": 0, "total_words": 0, "avg_words": 0, "date_range": "N/A"
            }
            continue
        words  = [r["word_count"] for r in rows]
        dates  = sorted(r["date"] for r in rows if r["date"])
        source_stats[source] = {
            "docs":        len(rows),
            "total_words": sum(words),
            "avg_words":   sum(words) // len(words),
            "date_range":  f"{dates[0]} – {dates[-1]}" if dates else "N/A",
        }

    total_words = sum(s["total_words"] for s in source_stats.values())

    # ── Report ────────────────────────────────────────────────────────────────
    report = {
        "total_docs":   len(all_rows),
        "total_words":  total_words,
        "train_size":   train_size,
        "val_size":     val_size,
        "sources":      source_stats,
    }
    (OUT_DIR / "dataset_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )

    _write_dataset_card(source_stats, train_size, val_size)

    # ── Summary ───────────────────────────────────────────────────────────────
    print()
    print("=" * 62)
    print(f"  {'Source':<14} {'Docs':>7}  {'Avg words':>10}  {'Total words':>12}")
    print(f"  {'-'*14}  {'-'*7}  {'-'*10}  {'-'*12}")
    for src, s in source_stats.items():
        print(f"  {src:<14} {s['docs']:>7,}  {s['avg_words']:>10,}  {s['total_words']:>12,}")
    print(f"  {'TOTAL':<14} {len(all_rows):>7,}  {'':>10}  {total_words:>12,}")
    print("=" * 62)
    print(f"  train:      {train_size:,}")
    print(f"  validation: {val_size:,}")
    print(f"  Saved →     {OUT_DIR}")
    print("=" * 62)


if __name__ == "__main__":
    main()
