# FinLLM-India

A domain-adapted large language model for Indian equity markets, built on Llama 3.1 8B with continued pre-training and QLoRA fine-tuning.

## Overview

Indian financial markets have distinct terminology, regulatory frameworks, and linguistic patterns that general-purpose LLMs handle poorly. FinLLM-India addresses this by adapting Llama 3.1 8B to the Indian financial domain through a two-stage training pipeline.

## Tasks
- Sentiment Classification
- Risk Factor Classification  
- Earnings Surprise Prediction

## Baseline
FinBERT

## Dataset Sources
- NSE/BSE corporate filings
- SEBI circulars and regulatory notices
- Earnings call transcripts
- Financial news (Moneycontrol, Economic Times, Business Standard)

## Training Pipeline
1. Continued pre-training on raw Indian financial corpus
2. QLoRA fine-tuning on labeled task-specific data

## Hardware
Developed on RTX 5050 8GB VRAM

## Stack
Python · PyTorch · HuggingFace Transformers · PEFT · BitsAndBytes · Weights & Biases

## Status
🔧 Active development

## Citation
Coming soon.

## License
Apache 2.0