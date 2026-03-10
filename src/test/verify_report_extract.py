# -*- coding: utf-8 -*-
# 验证 data/company_research 下所有 CSV：content 不为空的行是否都有有效的 extracted_forecasts。
# 依赖：pip install pandas

import sys
from pathlib import Path
from typing import List, Tuple

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
COMPANY_RESEARCH_DIR = ROOT_DIR / "data" / "company_research"
OUTPUT_COLUMN = "extracted_forecasts"


def _is_valid_result(val) -> bool:
    """判断 extracted_forecasts 是否为有效结果（非空且非 ERROR）。"""
    if pd.isna(val):
        return False
    s = str(val).strip()
    return len(s) > 0 and not s.startswith("[ERROR]")


def verify_one_csv(csv_path: Path) -> Tuple[int, int, List[Tuple[int, str]]]:
    """
    验证单个 CSV。
    返回 (有 content 的行数, 有有效结果的行数, 问题列表 [(行号, 问题描述), ...])。
    """
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception as e:
        return (0, 0, [(0, f"读取失败: {e}")])

    if "content" not in df.columns:
        return (0, 0, [(0, "无 content 列")])
    if OUTPUT_COLUMN not in df.columns:
        return (0, 0, [(0, "无 extracted_forecasts 列")])

    has_content = 0
    has_valid = 0
    issues = []

    for idx in range(len(df)):
        row = df.iloc[idx]
        content = row["content"]
        if pd.isna(content) or str(content).strip() == "":
            continue

        has_content += 1
        extracted = row.get(OUTPUT_COLUMN, "")
        if _is_valid_result(extracted):
            has_valid += 1
        else:
            if pd.isna(extracted) or str(extracted).strip() == "":
                issues.append((idx + 1, "缺失"))
            elif str(extracted).strip().startswith("[ERROR]"):
                issues.append((idx + 1, f"ERROR: {str(extracted)[:80]}..."))
            else:
                issues.append((idx + 1, "无效"))

    return (has_content, has_valid, issues)


def main():
    if not COMPANY_RESEARCH_DIR.exists():
        print(f"目录不存在: {COMPANY_RESEARCH_DIR}")
        sys.exit(1)

    csv_files = sorted(COMPANY_RESEARCH_DIR.rglob("*.csv"))
    if not csv_files:
        print(f"未找到 CSV 文件: {COMPANY_RESEARCH_DIR}")
        return

    total_content = 0
    total_valid = 0
    files_with_issues = []

    for csv_path in csv_files:
        rel = csv_path.relative_to(COMPANY_RESEARCH_DIR)
        has_content, has_valid, issues = verify_one_csv(csv_path)

        total_content += has_content
        total_valid += has_valid

        if issues:
            files_with_issues.append((rel, has_content, has_valid, issues))

    print("=" * 60)
    print("验证结果汇总")
    print("=" * 60)
    print(f"CSV 文件总数: {len(csv_files)}")
    print(f"content 不为空的行数: {total_content}")
    print(f"有有效 extracted_forecasts 的行数: {total_valid}")
    print(f"缺失/失败的行数: {total_content - total_valid}")
    print()

    if total_content == total_valid:
        print("✓ 验证通过：所有 content 不为空的数据都有有效的提取结果。")
        return

    print("✗ 验证未通过，以下文件存在问题：")
    print()
    for rel, has_content, has_valid, issues in files_with_issues:
        print(f"  {rel}")
        print(f"    有 content: {has_content}, 有效结果: {has_valid}, 问题: {len(issues)} 条")
        for row_num, desc in issues[:10]:  # 每文件最多展示 10 条
            print(f"      行 {row_num}: {desc}")
        if len(issues) > 10:
            print(f"      ... 还有 {len(issues) - 10} 条")
        print()


if __name__ == "__main__":
    main()
