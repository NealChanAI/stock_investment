# 遍历 data/company_research 下所有 CSV，对 content 列逐行跑 LLM 提取财务预测，结果写入新列。
# 在 stock_investment 或 stock_investment_311 环境下运行。依赖：pip install google-genai pandas

import os
import sys
import warnings
from pathlib import Path
from typing import Optional

# 隐藏第三方库的 warning 输出
warnings.filterwarnings("ignore")
import logging
logging.getLogger("google").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

import pandas as pd
from google import genai
from google.genai import types

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


def call_gemini(user_message: str, system_instruction: str, api_key: Optional[str] = None) -> str:
    key = api_key or os.environ.get("GEMINI_API_KEY_STOCK_INVESTMENT")
    if not key:
        raise ValueError("请设置 GEMINI_API_KEY 或 GEMINI_API_KEY_STOCK_INVESTMENT")

    client = genai.Client(api_key=key)
    contents = [types.Content(role="user", parts=[types.Part.from_text(text=user_message)])]
    config = types.GenerateContentConfig(
        system_instruction=[types.Part.from_text(text=system_instruction)],
        thinking_config=types.ThinkingConfig(thinking_budget=-1),
        response_mime_type="application/json",
        temperature=0.0,
    )
    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
        contents=contents,
        config=config,
    )
    return (response.text or "").strip()


def process_one_csv(csv_path: Path, system_instruction: str) -> None:
    """处理单个 CSV：对 content 列逐行调用 LLM，结果写入 OUTPUT_COLUMN，保存回原文件。"""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "content" not in df.columns:
        print(f"  跳过（无 content 列）: {csv_path}")
        return

    if OUTPUT_COLUMN not in df.columns:
        df[OUTPUT_COLUMN] = ""

    total = len(df)
    for idx in range(total):
        row = df.iloc[idx]
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            df.at[idx, OUTPUT_COLUMN] = ""
            print(f"  [{idx + 1}/{total}] 跳过空 content")
            continue

        user_text = str(content).strip()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = call_gemini(user_text, system_instruction)
                df.at[idx, OUTPUT_COLUMN] = result
                print(f"  [{idx + 1}/{total}] 成功")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"  [{idx + 1}/{total}] 第 {attempt} 次失败，重试中。LLM 输入（前 500 字）:\n{user_text[:500]}\n...")
                else:
                    df.at[idx, OUTPUT_COLUMN] = f"[ERROR] {e!s}"
                    print(f"  [{idx + 1}/{total}] 失败（已重试 {MAX_RETRIES} 次）。LLM 输入（前 500 字）:\n{user_text[:500]}\n...\n错误: {e}")

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  已写回: {csv_path}")


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

    print(f"共找到 {len(csv_files)} 个 CSV，开始处理。\n")
    for i, csv_path in enumerate(csv_files, 1):
        print(f"[{i}/{len(csv_files)}] {csv_path.relative_to(COMPANY_RESEARCH_DIR)}")
        try:
            process_one_csv(csv_path, system_instruction)
        except Exception as e:
            print(f"  处理失败: {e}")
        print()

    print("全部完成。")


if __name__ == "__main__":
    main()
