# -*- coding: utf-8 -*-
# 遍历 data/company_research 下所有 CSV，对 content 列逐行跑豆包 LLM 提取财务预测，结果写入新列。
# 公司串行、公司内研报并行。已完全处理的公司自动跳过。
# 依赖：pip install openai pandas

import os
import sys
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple


def _log(msg: str) -> None:
    """带当前时间的日志输出。"""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# 隐藏第三方库的 warning 输出
warnings.filterwarnings("ignore")
import logging
logging.getLogger("urllib3").setLevel(logging.ERROR)

import pandas as pd
from openai import OpenAI

# 项目根目录
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
PROMPTS_DIR = DATA_DIR / "prompts"
COMPANY_RESEARCH_DIR = DATA_DIR / "company_research"
SYSTEM_PROMPT_PATH = PROMPTS_DIR / "PE_extract.txt"
OUTPUT_COLUMN = "extracted_forecasts"
MAX_RETRIES = 3
# 公司内研报并行度（高并发）
PARALLEL_WORKERS = 64


def load_system_instruction(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _is_valid_result(val) -> bool:
    """判断 extracted_forecasts 是否为有效结果（非空且非 ERROR）。"""
    if pd.isna(val):
        return False
    s = str(val).strip()
    return len(s) > 0 and not s.startswith("[ERROR]")


def is_fully_processed(csv_path: Path) -> bool:
    """检查该 CSV 是否已完全处理（所有有 content 的行都有有效结果）。"""
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return False
    if "content" not in df.columns or OUTPUT_COLUMN not in df.columns:
        return False
    for _, row in df.iterrows():
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            continue
        if not _is_valid_result(row.get(OUTPUT_COLUMN, "")):
            return False
    return True


def call_doubao(user_message: str, system_instruction: str, api_key: Optional[str] = None) -> str:
    key = api_key or os.environ.get("ARK_API_KEY_REPORT_EXTRACT")
    if not key:
        raise ValueError("请设置环境变量 ARK_API_KEY（火山引擎方舟 API Key）")

    client = OpenAI(
        base_url="https://ark.cn-beijing.volces.com/api/v3",
        api_key=key,
    )
    response = client.chat.completions.create(
        model="doubao-1-5-lite-32k-250115",
        messages=[
            {"role": "system", "content": system_instruction},
            {"role": "user", "content": user_message},
        ],
        response_format={"type": "json_object"},
        max_tokens=4096,
        temperature=0.0,
    )
    content = response.choices[0].message.content
    return (content or "").strip()


def _process_one_report(
    idx: int,
    user_text: str,
    system_instruction: str,
) -> Tuple[int, str]:
    """处理单条研报，返回 (idx, result)。"""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            result = call_doubao(user_text, system_instruction)
            return (idx, result)
        except Exception as e:
            if attempt >= MAX_RETRIES:
                return (idx, f"[ERROR] {e!s}")
    return (idx, "[ERROR] unknown")


def process_one_csv(
    csv_path: Path,
    system_instruction: str,
    company_idx: int,
    total_companies: int,
) -> None:
    """处理单个 CSV：公司内研报并行调用 LLM，结果写入 OUTPUT_COLUMN，保存回原文件。"""
    company_name = csv_path.stem
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "content" not in df.columns:
        _log(f"  [公司 {company_idx}/{total_companies}] {company_name} - 跳过（无 content 列）")
        return

    if OUTPUT_COLUMN not in df.columns:
        df[OUTPUT_COLUMN] = ""

    total_reports = len(df)
    # 收集需要处理的 (idx, user_text)
    to_process: list = []  # [(idx, user_text), ...]
    for idx in range(total_reports):
        row = df.iloc[idx]
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            df.at[idx, OUTPUT_COLUMN] = ""
            continue
        if _is_valid_result(row.get(OUTPUT_COLUMN, "")):
            continue  # 已有有效结果，跳过
        to_process.append((idx, str(content).strip()))

    skip_count = total_reports - len(to_process)
    _log(f"  [公司 {company_idx}/{total_companies}] {company_name} - 共 {total_reports} 条研报，待处理 {len(to_process)} 条（跳过 {skip_count} 条）")
    if not to_process:
        _log(f"  [公司 {company_idx}/{total_companies}] {company_name} - 已全部完成，跳过")
        return

    workers = min(PARALLEL_WORKERS, len(to_process))
    done = 0
    ok_count = 0
    err_count = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_process_one_report, idx, text, system_instruction): idx
            for idx, text in to_process
        }
        for future in as_completed(futures):
            idx, result = future.result()
            df.at[idx, OUTPUT_COLUMN] = result
            done += 1
            if result.startswith("[ERROR]"):
                err_count += 1
            else:
                ok_count += 1
            if done % 20 == 0 or done == len(to_process):
                _log(f"    进度 {done}/{len(to_process)}（成功 {ok_count}，失败 {err_count}）")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    _log(f"  [公司 {company_idx}/{total_companies}] {company_name} - 已写回（成功 {ok_count}，失败 {err_count}）")


def main():
    if not COMPANY_RESEARCH_DIR.exists():
        _log(f"目录不存在: {COMPANY_RESEARCH_DIR}")
        sys.exit(1)
    if not SYSTEM_PROMPT_PATH.exists():
        _log(f"找不到 prompt 文件: {SYSTEM_PROMPT_PATH}")
        sys.exit(1)

    system_instruction = load_system_instruction(SYSTEM_PROMPT_PATH)
    csv_files = sorted(COMPANY_RESEARCH_DIR.rglob("*.csv"))
    if not csv_files:
        _log(f"未找到 CSV 文件: {COMPANY_RESEARCH_DIR}")
        return

    total_companies = len(csv_files)
    to_run = [p for p in csv_files if not is_fully_processed(p)]
    skipped = total_companies - len(to_run)
    _log(f"共找到 {total_companies} 个公司（CSV），已完全处理跳过 {skipped} 个，待处理 {len(to_run)} 个。\n")
    for i, csv_path in enumerate(to_run, 1):
        rel = csv_path.relative_to(COMPANY_RESEARCH_DIR)
        _log(f"[公司 {i}/{len(to_run)}] {rel}")
        try:
            process_one_csv(csv_path, system_instruction, company_idx=i, total_companies=len(to_run))
        except Exception as e:
            _log(f"  处理失败: {e}")
        print()

    _log("全部完成。")


if __name__ == "__main__":
    main()
