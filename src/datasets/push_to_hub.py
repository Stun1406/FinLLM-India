#!/usr/bin/env python3
"""
Push the FinLLM-India HuggingFace dataset to the Hub.

Usage:
    HF_TOKEN=<your_token> python src/datasets/push_to_hub.py
"""

import os
from pathlib import Path

from datasets import load_from_disk
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(Path(__file__).resolve().parents[2] / ".env")

token = os.environ.get("HF_TOKEN")
if not token:
    raise ValueError("HF_TOKEN not found — add it to .env or set it in your environment")

# Load the dataset
dataset = load_from_disk("data/datasets/finllm-india/")

# Push to hub
dataset.push_to_hub(
    repo_id="finllm-india",
    token=token,
    private=False,
)

print("Dataset pushed successfully.")
print("View at: https://huggingface.co/datasets/TaanishChauhan/finllm-india")
