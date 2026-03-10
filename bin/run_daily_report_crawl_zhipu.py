# -*- coding: utf-8 -*-
"""
Daily script: crawl yesterday's Luobo reports -> append to CSV -> Zhipu extract PE/EPS -> parse.

Usage:
  python bin/run_daily_report_crawl_zhipu.py              # yesterday
  python bin/run_daily_report_crawl_zhipu.py -d 20250309  # specific date

Requires: ZHIPU_API_KEY, Chrome (for spider cookie), research_report_spider
"""
import argparse
import os
import re
import subprocess
import sys
import warnings
from datetime import datetime, timedelta
from pathlib import Path

warnings.filterwarnings("ignore")
import logging
logging.getLogger("urllib3").setLevel(logging.ERROR)

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
COMPANY_RESEARCH_DIR = DATA_DIR / "company_research"
PROMPTS_DIR = DATA_DIR / "prompts"
SPIDER_DIR = ROOT_DIR / "research_report_spider"
SYSTEM_PROMPT_PATH = PROMPTS_DIR / "PE_extract.txt"
OUTPUT_COLUMN = "extracted_forecasts"
MAX_RETRIES = 3

# CSV columns from spider (order for new rows)
SPIDER_COLUMNS = [
    "author", "content", "filename", "org_name", "original_rating",
    "pdf_link", "publish_time", "rating_adjust_mark_type", "rating_changes",
    "report_id", "save_path", "stock_code", "stock_name", "title",
]
EXPAND_COLUMNS = [
    "PE_2年前", "PE_1年前", "PE_当年", "PE_明年", "PE_后年", "PE_大后年",
    "EPS_2年前", "EPS_1年前", "EPS_当年", "EPS_明年", "EPS_后年", "EPS_大后年",
]


def _sanitize_filename(name: str) -> str:
    if not name or not isinstance(name, str):
        return ""
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    name = name.strip().strip(".") or "unnamed"
    return name[:50]


def _run_spider(target_date: str) -> Path:
    """Run spider for target_date (YYYYMMDD), return path to output CSV."""
    cwd = os.getcwd()
    try:
        os.chdir(SPIDER_DIR)
        feed_suffix = f"daily_{target_date}"
        env = os.environ.copy()
        env["REPORT_FEED_TIMESTAMP"] = feed_suffix

        cmd = [
            sys.executable, "run.py",
            "--index", "csi500",
            "-d", target_date,
            "-o", feed_suffix,
            "--delay", "5",
        ]
        ret = subprocess.run(cmd, env=env, cwd=str(SPIDER_DIR))
        if ret.returncode != 0:
            raise RuntimeError(f"Spider exited with code {ret.returncode}")

        out_path = SPIDER_DIR / f"reports_{feed_suffix}.csv"
        if not out_path.exists():
            raise FileNotFoundError(f"Spider output not found: {out_path}")
        return out_path
    finally:
        os.chdir(cwd)


def _spider_row_to_dict(row: dict) -> dict:
    """Convert spider CSV row to our CSV row format."""
    stock_code = str(row.get("stock_code", "") or "").strip()
    stock_name = str(row.get("stock_name", "") or "").strip()
    stock_name = _sanitize_filename(stock_name) or stock_code

    # pdf_link: spider may output list as string
    pdf_link = row.get("pdf_link", "")
    if isinstance(pdf_link, str) and pdf_link.startswith("["):
        pass  # keep as-is
    elif isinstance(pdf_link, list):
        pdf_link = str(pdf_link) if pdf_link else "[None]"

    return {
        "author": row.get("author", ""),
        "content": row.get("content", ""),
        "filename": row.get("filename", ""),
        "org_name": row.get("org_name", ""),
        "original_rating": row.get("original_rating", ""),
        "pdf_link": pdf_link,
        "publish_time": row.get("publish_time", ""),
        "rating_adjust_mark_type": row.get("rating_adjust_mark_type", ""),
        "rating_changes": row.get("rating_changes", ""),
        "report_id": row.get("report_id", ""),
        "save_path": row.get("save_path", ""),
        "stock_code": stock_code,
        "stock_name": stock_name,
        "title": row.get("title", ""),
    }


def _get_csv_path(stock_code: str, stock_name: str) -> Path:
    code_6 = str(stock_code).zfill(6) if stock_code else "000000"
    return COMPANY_RESEARCH_DIR / f"reports_{code_6}_{stock_name}.csv"


def _existing_report_ids(csv_path: Path) -> set:
    if not csv_path.exists():
        return set()
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig", usecols=["report_id"])
        return set(df["report_id"].astype(str).dropna().tolist())
    except Exception:
        return set()


def _append_new_reports(spider_csv: Path) -> list:
    """
    Read spider output, append new rows to company CSVs. Dedupe by report_id.
    Return list of (csv_path, list of new row indices) for Zhipu processing.
    """
    df_spider = pd.read_csv(spider_csv, encoding="utf-8-sig")
    if df_spider.empty:
        print("  No reports in spider output")
        return []

    # Group by (stock_code, stock_name)
    grouped = df_spider.groupby(
        [df_spider["stock_code"].astype(str), df_spider["stock_name"].astype(str)],
        dropna=False,
    )

    to_extract = []  # (csv_path, [(row_dict, has_content), ...])
    for (code, name), grp in grouped:
        code = str(code).strip()
        name = _sanitize_filename(str(name).strip()) or code
        csv_path = _get_csv_path(code, name)
        existing_ids = _existing_report_ids(csv_path)

        new_rows = []
        for _, r in grp.iterrows():
            rid = str(r.get("report_id", "")).strip()
            if rid in existing_ids:
                continue
            row_dict = _spider_row_to_dict(r)
            row_dict[OUTPUT_COLUMN] = ""
            for col in EXPAND_COLUMNS:
                row_dict[col] = ""
            has_content = bool(str(row_dict.get("content", "") or "").strip())
            new_rows.append((row_dict, has_content))
            existing_ids.add(rid)

        if not new_rows:
            continue

        COMPANY_RESEARCH_DIR.mkdir(parents=True, exist_ok=True)
        all_cols = SPIDER_COLUMNS + [OUTPUT_COLUMN] + EXPAND_COLUMNS
        new_df = pd.DataFrame([r[0] for r in new_rows])
        new_df = new_df[[c for c in all_cols if c in new_df.columns]]

        if csv_path.exists():
            existing = pd.read_csv(csv_path, encoding="utf-8-sig")
            for c in all_cols:
                if c not in existing.columns:
                    existing[c] = ""
            combined = pd.concat([existing, new_df], ignore_index=True)
        else:
            combined = new_df

        combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"  Appended {len(new_rows)} rows -> {csv_path.name}")

        # indices of new rows in combined (at the end)
        start_idx = len(combined) - len(new_rows)
        to_extract.append((csv_path, [(start_idx + i, has_content) for i, (_, has_content) in enumerate(new_rows)]))

    return to_extract


def _call_zhipu(user_message: str, system_instruction: str) -> str:
    from zai import ZhipuAiClient
    key = os.environ.get("ZHIPU_API_KEY")
    if not key:
        raise ValueError("Set ZHIPU_API_KEY env var")

    client = ZhipuAiClient(api_key=key)
    response = client.chat.completions.create(
        model="glm-4.7-flash",
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_message},
        ],
        thinking={"type": "enabled"},
        response_format={"type": "json_object"},
        max_tokens=4096,
        temperature=0.0,
    )
    return (response.choices[0].message.content or "").strip()


def _extract_zhipu_for_new_rows(to_extract: list, system_instruction: str) -> None:
    """Call Zhipu for new rows that have content, write extracted_forecasts."""
    for csv_path, rows_info in to_extract:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if OUTPUT_COLUMN not in df.columns:
            df[OUTPUT_COLUMN] = ""

        for idx, has_content in rows_info:
            if not has_content:
                continue
            content = df.at[idx, "content"]
            if pd.isna(content) or not str(content).strip():
                continue

            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    result = _call_zhipu(str(content).strip(), system_instruction)
                    df.at[idx, OUTPUT_COLUMN] = result
                    print(f"    [{idx}] Zhipu ok")
                    break
                except Exception as e:
                    if attempt < MAX_RETRIES:
                        print(f"    [{idx}] retry {attempt}...")
                    else:
                        df.at[idx, OUTPUT_COLUMN] = f"[ERROR] {e!s}"
                        print(f"    [{idx}] Zhipu fail: {e}")

        df.to_csv(csv_path, index=False, encoding="utf-8-sig")


def _expand_forecast_columns(csv_path: Path) -> None:
    """Run expand logic on one CSV (reuse expand_forecast_columns logic)."""
    test_dir = str(ROOT_DIR / "src" / "test")
    if test_dir not in sys.path:
        sys.path.insert(0, test_dir)
    import expand_forecast_columns as mod
    mod.process_csv(csv_path)


def main():
    parser = argparse.ArgumentParser(description="Daily: crawl Luobo -> append CSV -> Zhipu extract -> parse")
    parser.add_argument("-d", "--date", help="Target date YYYYMMDD (default: yesterday)")
    args = parser.parse_args()

    if args.date:
        target_date = str(args.date).replace("-", "")[:8]
    else:
        yesterday = datetime.now() - timedelta(days=1)
        target_date = yesterday.strftime("%Y%m%d")

    print(f"Target date: {target_date}")
    print("=" * 50)

    if not SPIDER_DIR.exists():
        print(f"Spider dir not found: {SPIDER_DIR}")
        sys.exit(1)
    if not SYSTEM_PROMPT_PATH.exists():
        print(f"Prompt not found: {SYSTEM_PROMPT_PATH}")
        sys.exit(1)

    # 1. Run spider
    print("\n[1] Crawling Luobo reports...")
    try:
        spider_csv = _run_spider(target_date)
        print(f"  Output: {spider_csv}")
    except Exception as e:
        print(f"  Spider error: {e}")
        sys.exit(1)

    # 2. Append to company CSVs
    print("\n[2] Appending to company CSVs...")
    to_extract = _append_new_reports(spider_csv)
    if not to_extract:
        print("  No new reports to process.")
        return

    # 3. Zhipu extract for new rows with content
    print("\n[3] Zhipu PE/EPS extraction...")
    system_instruction = SYSTEM_PROMPT_PATH.read_text(encoding="utf-8").strip()
    _extract_zhipu_for_new_rows(to_extract, system_instruction)

    # 4. Expand forecast columns
    print("\n[4] Parsing extracted_forecasts -> PE/EPS columns...")
    for csv_path, _ in to_extract:
        try:
            _expand_forecast_columns(csv_path)
        except Exception as e:
            print(f"  Expand error {csv_path.name}: {e}")

    print("\nDone.")


if __name__ == "__main__":
    main()
