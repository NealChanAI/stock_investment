# -*- coding: utf-8 -*-
"""诊断智飞生物(300122)未出现在 2020-01-02 评估中的原因。不依赖 baostock。"""
import json
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPANY_RESEARCH = ROOT / "data" / "company_research"
COMPANY_STOCK_INFO = ROOT / "data" / "company_stock_info"


def _parse_forecasts_json(raw):
    if pd.isna(raw):
        return None
    s = str(raw).strip().replace('""', '"')
    if not s or s.startswith("[ERROR]"):
        return None
    try:
        data = json.loads(s)
    except Exception:
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
        pe, eps = item.get("pe"), item.get("eps")
        if pe is not None and not isinstance(pe, (int, float)):
            pe = None
        if eps is not None and not isinstance(eps, (int, float)):
            eps = None
        result[year] = {"pe": pe, "eps": eps}
    return result if result else None


def _compute_g(forecasts):
    years = sorted([y for y in forecasts.keys() if forecasts[y].get("eps") or forecasts[y].get("pe")])
    if len(years) < 2:
        return None
    y1, y2 = years[0], years[-1]
    n = y2 - y1
    if n <= 0:
        return None
    d1, d2 = forecasts[y1], forecasts[y2]
    eps1, eps2 = d1.get("eps"), d2.get("eps")
    pe1, pe2 = d1.get("pe"), d2.get("pe")
    if eps1 and eps2 and eps1 > 0 and eps2 > 0:
        ratio = eps2 / eps1
    elif pe1 and pe2 and pe1 > 0 and pe2 > 0:
        ratio = pe1 / pe2
    else:
        return None
    try:
        g = (ratio ** (1.0 / n)) - 1
        return float(g) if not isinstance(g, complex) else float(g.real)
    except (ValueError, TypeError):
        return None


def main():
    code = "300122"
    as_of_date = "2020-01-02"

    print("=" * 60)
    print(f"诊断 智飞生物({code}) 未出现在 {as_of_date} 评估中的原因")
    print("=" * 60)

    # 1. 行情数据
    csv_path = COMPANY_STOCK_INFO / f"{code}.csv"
    if not csv_path.exists():
        print("\n1. 行情: 文件不存在 -> get_pe_info 返回 None")
    else:
        df_stock = pd.read_csv(csv_path, encoding="utf-8-sig")
        row = df_stock[df_stock["date"] == as_of_date]
        if row.empty:
            print("\n1. 行情: 无 2020-01-02 数据 -> get_pe_info 返回 None")
        else:
            pe = row["peTTM"].values[0]
            print(f"\n1. 行情: 有数据, peTTM={pe:.2f} -> get_pe_info 正常")

    # 2. 研报
    candidates = list(COMPANY_RESEARCH.glob(f"reports_{code}_*.csv"))
    if not candidates:
        print("\n2. 研报: 无 reports_300122_*.csv -> get_recent_predict_peTTM 返回 None")
        return
    csv_research = candidates[0]
    df = pd.read_csv(csv_research, encoding="utf-8-sig")
    if "extracted_forecasts" not in df.columns or "publish_time" not in df.columns:
        print("\n2. 研报: 缺少 extracted_forecasts 或 publish_time 列")
        return

    as_of_ts = pd.to_datetime(as_of_date)
    start_90 = as_of_ts - pd.Timedelta(days=90)
    start_360 = as_of_ts - pd.Timedelta(days=360)
    df["_pt"] = pd.to_datetime(df["publish_time"], errors="coerce")

    mask_90 = (df["_pt"] <= as_of_ts) & (df["_pt"] >= start_90)
    mask_360 = (df["_pt"] <= as_of_ts) & (df["_pt"] >= start_360)

    n_90 = mask_90.sum()
    n_360 = mask_360.sum()
    print(f"\n2. 研报: 90天内 {n_90} 篇, 360天内 {n_360} 篇")

    if n_360 == 0:
        print("   -> 无有效研报窗口 -> get_recent_predict_peTTM 返回 None")
        return

    # 实际逻辑：90天有匹配时只用90天，不fallback到360天
    df_in = df.loc[mask_90].copy() if mask_90.any() else df.loc[mask_360].copy()
    print(f"   窗口内最早: {df_in['_pt'].min()}, 最晚: {df_in['_pt'].max()}")

    # 3. 解析 extracted_forecasts
    growth_rates = []
    for _, row in df_in.iterrows():
        f = _parse_forecasts_json(row.get("extracted_forecasts"))
        if f:
            g = _compute_g(f)
            if g is not None:
                growth_rates.append(g)

    if not growth_rates:
        print("\n3. 解析: 所有研报的 extracted_forecasts 均无法解析出有效 g")
        print("   -> get_recent_predict_peTTM 返回 None -> get_stock_info 返回 None")
        # 关键：60天内有匹配时只用60天窗口，不会fallback到365天
        print("\n   【根本原因】90天窗口内有研报时，不会放宽到360天。")
        print("   90天内唯一一篇的 extracted_forecasts 只有单年且 eps/pe 均为 null，")
        print("   无法计算增长率 g（需要至少2个年份的有效 pe 或 eps）。")
        sample = df_in.iloc[0]
        raw = sample.get("extracted_forecasts")
        print(f"\n   90天内该篇 publish_time={sample['publish_time']}")
        print(f"   extracted_forecasts 前300字符: {str(raw)[:300]}...")
        return

    mean_g = sum(growth_rates) / len(growth_rates)
    print(f"\n3. 解析: 有效 g 的研报 {len(growth_rates)} 篇, mean_g={mean_g:.4f}")
    print("   -> get_recent_predict_peTTM 应返回有效 -> 理论上 get_stock_info 不应为 None")

    print("\n结论: 若行情和研报均正常，需在已安装 baostock 的环境中运行 get_stock_info 做完整验证。")


if __name__ == "__main__":
    main()
