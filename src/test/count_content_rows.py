# -*- coding: utf-8 -*-
"""统计 company_research 目录下 content 不为空的行数"""
import pandas as pd
from pathlib import Path

COMPANY_RESEARCH_DIR = Path(__file__).resolve().parents[2] / "data" / "company_research"
csv_files = sorted(COMPANY_RESEARCH_DIR.rglob("*.csv"))
total_non_empty = 0
for f in csv_files:
    try:
        df = pd.read_csv(f, encoding="utf-8-sig")
    except pd.errors.EmptyDataError:
        continue
    if "content" not in df.columns:
        continue
    non_empty = df["content"].apply(lambda x: pd.notna(x) and str(x).strip() != "").sum()
    total_non_empty += non_empty
    if non_empty > 0:
        print(f"{f.relative_to(COMPANY_RESEARCH_DIR)}: {non_empty} 行")
print(f"\n合计: {total_non_empty} 行 (content 不为空)")
