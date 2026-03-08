# 在 stock_investment 或 stock_investment_311 环境下运行均可。
# 读取研报 CSV 的 content 列，逐行用 Gemini 提取财务预测，结果写回新列。
# 依赖：pip install google-genai pandas

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

# 中国中免研报 CSV
CSV_PATH = COMPANY_RESEARCH_DIR / "中国中免" / "reports_601888_中国中免.csv"
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


def main():
    if not CSV_PATH.exists():
        print(f"找不到文件: {CSV_PATH}")
        sys.exit(1)
    if not SYSTEM_PROMPT_PATH.exists():
        print(f"找不到 prompt 文件: {SYSTEM_PROMPT_PATH}")
        sys.exit(1)

    system_instruction = load_system_instruction(SYSTEM_PROMPT_PATH)
    df = pd.read_csv(CSV_PATH, encoding="utf-8-sig")
    if "content" not in df.columns:
        print("CSV 中缺少 content 列")
        sys.exit(1)

    if OUTPUT_COLUMN not in df.columns:
        df[OUTPUT_COLUMN] = ""

    total = len(df)
    for idx in range(total):
        row = df.iloc[idx]
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            df.at[idx, OUTPUT_COLUMN] = ""
            print(f"[{idx + 1}/{total}] 跳过空 content")
            continue

        user_text = str(content).strip()
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = call_gemini(user_text, system_instruction)
                df.at[idx, OUTPUT_COLUMN] = result
                print(f"[{idx + 1}/{total}] 成功")
                break
            except Exception as e:
                if attempt < MAX_RETRIES:
                    print(f"[{idx + 1}/{total}] 第 {attempt} 次失败，重试中。LLM 输入（前 500 字）:\n{user_text[:500]}\n...")
                else:
                    df.at[idx, OUTPUT_COLUMN] = f"[ERROR] {e!s}"
                    print(f"[{idx + 1}/{total}] 失败（已重试 {MAX_RETRIES} 次）。LLM 输入（前 500 字）:\n{user_text[:500]}\n...\n错误: {e}")

    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"已写回 {CSV_PATH}，新增列: {OUTPUT_COLUMN}")


if __name__ == "__main__":
    main()
