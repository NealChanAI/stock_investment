# -*- coding: utf-8 -*-
# 将 extracted_forecasts 的 JSON 解析后拆分为 12 个字段：
# 2年前、1年前、当年、明年、后年、大后年 的 PE 和 EPS（以研报 publish_time 为基准年）。
# 用法：python expand_forecast_columns.py [csv_path]
# 不传参数则处理 data/company_research 下所有 CSV。

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
COMPANY_RESEARCH_DIR = ROOT_DIR / "data" / "company_research"
EXTRACTED_COLUMN = "extracted_forecasts"
PUBLISH_TIME_COLUMN = "publish_time"

# 12 个新列：(相对年份偏移, 列名)
# 2年前=-2, 1年前=-1, 当年=0, 明年=1, 后年=2, 大后年=3
TIME_LABELS = [
    (-2, "2年前"),
    (-1, "1年前"),
    (0, "当年"),
    (1, "明年"),
    (2, "后年"),
    (3, "大后年"),
]
NEW_COLUMNS = [f"PE_{label}" for _, label in TIME_LABELS] + [
    f"EPS_{label}" for _, label in TIME_LABELS
]


def parse_publish_year(publish_time: Any) -> Optional[int]:
    """从 publish_time 解析年份。支持 2025-12-08、2025 等格式。"""
    if pd.isna(publish_time):
        return None
    s = str(publish_time).strip()
    if not s:
        return None
    m = re.match(r"(\d{4})", s)
    return int(m.group(1)) if m else None


def parse_forecasts_json(raw: Any) -> Optional[Dict[int, Dict[str, float]]]:
    """
    解析 extracted_forecasts，返回 {year: {pe: x, eps: y}, ...}。
    解析失败返回 None。
    """
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s or s.startswith("[ERROR]"):
        return None

    # 处理 CSV 中可能存在的双引号转义
    s = s.replace('""', '"')

    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return None

    forecasts = data.get("forecasts")
    if not isinstance(forecasts, list):
        return None

    result = {}
    for item in forecasts:
        if not isinstance(item, dict):
            continue
        year = item.get("year")
        if year is None:
            continue
        try:
            year = int(year)
        except (TypeError, ValueError):
            continue

        pe = item.get("pe")
        eps = item.get("eps")
        if pe is not None and not isinstance(pe, (int, float)):
            pe = None
        if eps is not None and not isinstance(eps, (int, float)):
            eps = None

        result[year] = {"pe": pe, "eps": eps}

    return result if result else None


def expand_row(row: pd.Series) -> Dict[str, Any]:
    """根据单行数据生成 12 个新列的值。有值填数值，无值留空（NaN）。"""
    out = {col: pd.NA for col in NEW_COLUMNS}

    base_year = parse_publish_year(row.get(PUBLISH_TIME_COLUMN))
    if base_year is None:
        return out

    forecasts = parse_forecasts_json(row.get(EXTRACTED_COLUMN))
    if not forecasts:
        return out

    for offset, label in TIME_LABELS:
        target_year = base_year + offset
        data = forecasts.get(target_year)
        if not data:
            continue
        if data.get("pe") is not None:
            out[f"PE_{label}"] = float(data["pe"])
        if data.get("eps") is not None:
            out[f"EPS_{label}"] = float(data["eps"])

    return out


def process_csv(csv_path: Path) -> None:
    """处理单个 CSV：解析 JSON 并新增 12 列，写回原文件。"""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")

    if EXTRACTED_COLUMN not in df.columns:
        print(f"  跳过：无 {EXTRACTED_COLUMN} 列")
        return
    if PUBLISH_TIME_COLUMN not in df.columns:
        print(f"  跳过：无 {PUBLISH_TIME_COLUMN} 列")
        return

    rows = []
    for idx in range(len(df)):
        row = df.iloc[idx]
        expanded = expand_row(row)
        rows.append(expanded)

    for col in NEW_COLUMNS:
        df[col] = [r[col] for r in rows]

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"  已写入 {len(NEW_COLUMNS)} 个新列")


def main():
    if len(sys.argv) >= 2:
        csv_path = Path(sys.argv[1]).resolve()
        if not csv_path.exists():
            print(f"文件不存在: {csv_path}")
            sys.exit(1)
        print(f"处理: {csv_path}")
        process_csv(csv_path)
        return

    if not COMPANY_RESEARCH_DIR.exists():
        print(f"目录不存在: {COMPANY_RESEARCH_DIR}")
        sys.exit(1)

    csv_files = sorted(COMPANY_RESEARCH_DIR.rglob("*.csv"))
    print(f"共 {len(csv_files)} 个 CSV，开始处理...")
    for p in csv_files:
        print(p.relative_to(COMPANY_RESEARCH_DIR))
        try:
            process_csv(p)
        except Exception as e:
            print(f"  错误: {e}")
    print("全部完成。")


if __name__ == "__main__":
    main()
