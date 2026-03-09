# -*- coding: utf-8 -*-
# 遍历 data/company_research 下所有 CSV，对 content 列逐行跑豆包 LLM 提取财务预测，结果写入新列。
# 依赖：pip install openai pandas

import os
import sys
import warnings
from pathlib import Path
from typing import Optional

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


def load_system_instruction(path: Path) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


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


def process_one_csv(
    csv_path: Path,
    system_instruction: str,
    company_idx: int,
    total_companies: int,
) -> None:
    """处理单个 CSV：对 content 列逐行调用 LLM，结果写入 OUTPUT_COLUMN，保存回原文件。"""
    company_name = csv_path.parent.name
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "content" not in df.columns:
        print(f"  [公司 {company_idx}/{total_companies}] {company_name} - 跳过（无 content 列）")
        return

    if OUTPUT_COLUMN not in df.columns:
        df[OUTPUT_COLUMN] = ""

    total_reports = len(df)
    print(f"  [公司 {company_idx}/{total_companies}] {company_name} - 共 {total_reports} 条研报")
    for idx in range(total_reports):
        row = df.iloc[idx]
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            df.at[idx, OUTPUT_COLUMN] = ""
            print(f"    [研报 {idx + 1}/{total_reports}] 跳过空 content")
            continue

        user_text = str(content).strip()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = call_doubao(user_text, system_instruction)
                df.at[idx, OUTPUT_COLUMN] = result
                print(f"    [研报 {idx + 1}/{total_reports}] 成功")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"    [研报 {idx + 1}/{total_reports}] 第 {attempt} 次失败，重试中...")
                else:
                    df.at[idx, OUTPUT_COLUMN] = f"[ERROR] {e!s}"
                    print(f"    [研报 {idx + 1}/{total_reports}] 失败（已重试 {MAX_RETRIES} 次）: {e}")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  [公司 {company_idx}/{total_companies}] {company_name} - 已写回")


def main():
    if not COMPANY_RESEARCH_DIR.exists():
        print(f"目录不存在: {COMPANY_RESEARCH_DIR}")
        sys.exit(1)
    if not SYSTEM_PROMPT_PATH.exists():
        print(f"找不到 prompt 文件: {SYSTEM_PROMPT_PATH}")
        sys.exit(1)

    system_instruction = load_system_instruction(SYSTEM_PROMPT_PATH)
    csv_files = sorted(COMPANY_RESEARCH_DIR.rglob("*.csv"))
    if not csv_files:
        print(f"未找到 CSV 文件: {COMPANY_RESEARCH_DIR}")
        return

    total_companies = len(csv_files)
    print(f"共找到 {total_companies} 个公司（CSV），开始处理。\n")
    for i, csv_path in enumerate(csv_files, 1):
        print(f"[公司 {i}/{total_companies}] {csv_path.relative_to(COMPANY_RESEARCH_DIR)}")
        try:
            process_one_csv(csv_path, system_instruction, company_idx=i, total_companies=total_companies)
        except Exception as e:
            print(f"  处理失败: {e}")
        print()

    print("全部完成。")


if __name__ == "__main__":
    main()
