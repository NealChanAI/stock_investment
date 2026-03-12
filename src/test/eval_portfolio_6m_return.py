# -*- coding: utf-8 -*-
"""
评估组合在买入后 6 个月的收益率（等权）。
用法: python src/test/eval_portfolio_6m_return.py
"""
import pandas as pd
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
STOCK_DIR = ROOT_DIR / "data" / "company_stock_info"

# 2016-01-04 选中的 11 只股票
PORTFOLIO = [
    "600312",  # 平高电气
    "600352",  # 浙江龙盛
    "600369",  # 西南证券
    "600483",  # 福能股份
    "601099",  # 太平洋
    "002202",  # 金风科技
    "600398",  # 海澜之家
    "002080",  # 中材科技
    "000400",  # 许继电气
    "000728",  # 国元证券
    "300115",  # 长盈精密
]

BUY_DATE = "2016-01-04"
END_DATE = "2016-07-01"  # 6 个月后第一个交易日


def get_close_on_date(code: str, date: str):
    csv_path = STOCK_DIR / f"{code}.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "date" not in df.columns or "close" not in df.columns:
            return None
        row = df[df["date"] == date]
        if row.empty:
            return None
        return float(row["close"].iloc[0])
    except Exception:
        return None


def main():
    results = []
    for code in PORTFOLIO:
        p_buy = get_close_on_date(code, BUY_DATE)
        p_end = get_close_on_date(code, END_DATE)
        if p_buy is None or p_end is None:
            print(f"{code}: 缺少数据 (buy={p_buy}, end={p_end})")
            continue
        ret = (p_end / p_buy) - 1
        results.append({"code": code, "p_buy": p_buy, "p_end": p_end, "return": ret})
        print(f"{code}: {p_buy:.2f} -> {p_end:.2f}, 收益率 {ret*100:.2f}%")

    if not results:
        print("无有效数据")
        return

    df = pd.DataFrame(results)
    equal_weight_ret = df["return"].mean()
    print(f"\n等权组合 6 个月收益率: {equal_weight_ret*100:.2f}%")


if __name__ == "__main__":
    main()
