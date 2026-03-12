# -*- coding: utf-8 -*-
"""检查沪深300中哪些股票缺少 stock_info 或数据不完整"""
import os
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
HS300 = ROOT / "data" / "hs300_stocks.csv"
INFO_DIR = ROOT / "data" / "company_stock_info"

def main():
    df = pd.read_csv(HS300)
    codes = df["code"].str.replace(r"^(sh|sz)\.", "", regex=True).tolist()
    code_to_name = dict(zip(
        df["code"].str.replace(r"^(sh|sz)\.", "", regex=True),
        df["code_name"]
    ))

    no_file = []       # 无文件
    empty_or_bad = []  # 有文件但空或行数极少
    stale = []         # 数据过旧（最新日期早于2025-01-01）

    for code in codes:
        fp = INFO_DIR / f"{code}.csv"
        if not fp.exists():
            no_file.append((code, code_to_name[code]))
            continue
        try:
            sub = pd.read_csv(fp, usecols=["date"])
        except Exception:
            empty_or_bad.append((code, code_to_name[code], "读取失败"))
            continue
        if len(sub) == 0:
            empty_or_bad.append((code, code_to_name[code], "0行"))
            continue
        if len(sub) < 100:
            empty_or_bad.append((code, code_to_name[code], f"{len(sub)}行"))
            continue
        latest = pd.to_datetime(sub["date"]).max()
        if latest < pd.Timestamp("2025-01-01"):
            stale.append((code, code_to_name[code], str(latest.date()), len(sub)))

    print("=" * 60)
    print("沪深300 stock_info 检查结果")
    print("=" * 60)
    print(f"\n【1】无 stock_info 文件（共 {len(no_file)} 只）:")
    for code, name in sorted(no_file, key=lambda x: x[0]):
        print(f"  {code} {name}")

    print(f"\n【2】有文件但数据异常/过少（共 {len(empty_or_bad)} 只）:")
    for code, name, reason in sorted(empty_or_bad, key=lambda x: x[0]):
        print(f"  {code} {name} - {reason}")

    print(f"\n【3】数据过旧（最新日期早于2025-01-01，共 {len(stale)} 只）:")
    for code, name, last_date, rows in sorted(stale, key=lambda x: x[0]):
        print(f"  {code} {name} - 最新:{last_date} 共{rows}行")

    total_issue = len(no_file) + len(empty_or_bad) + len(stale)
    print(f"\n合计需关注: {total_issue} 只")

if __name__ == "__main__":
    main()
