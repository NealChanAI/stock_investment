# -*- coding: utf-8 -*-
"""
计算上海医药(601607)在 2020-01-02 时的增长率 g。
输出中间详细的研报结果和最后的 g。
"""
import json
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
COMPANY_RESEARCH = ROOT / "data" / "company_research"


def _parse_forecasts_json(raw):
    """解析 extracted_forecasts JSON，返回 {year: {pe, eps}, ...}"""
    if pd.isna(raw):
        return None
    s = str(raw).strip().replace('""', '"')
    if not s or s.startswith("[ERROR]"):
        return None
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
        pe, eps = item.get("pe"), item.get("eps")
        if pe is not None and not isinstance(pe, (int, float)):
            pe = None
        if eps is not None and not isinstance(eps, (int, float)):
            eps = None
        result[year] = {"pe": pe, "eps": eps}
    return result if result else None


def _compute_g_from_forecasts(forecasts: dict):
    """
    从 forecasts {year: {pe, eps}} 计算 EPS 年化增长率 g（小数形式）。
    优先用 EPS，无则用 PE：g = (PE_start/PE_end)^(1/n) - 1
    """
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
        method = "EPS"
    elif pe1 and pe2 and pe1 > 0 and pe2 > 0:
        ratio = pe1 / pe2
        method = "PE"
    else:
        return None
    try:
        g = (ratio ** (1.0 / n)) - 1
        return (float(g) if not isinstance(g, complex) else float(g.real), method, y1, y2, n)
    except (ValueError, TypeError):
        return None


def main():
    code = "601607"
    as_of_date = "2020-01-02"
    lookback_days = 90

    print("=" * 70)
    print(f"上海医药({code}) 在 {as_of_date} 时的增长率 g 计算")
    print("=" * 70)

    csv_path = list(COMPANY_RESEARCH.glob(f"reports_{code}_*.csv"))
    if not csv_path:
        print(f"未找到研报文件 reports_{code}_*.csv")
        return
    csv_path = csv_path[0]

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "extracted_forecasts" not in df.columns or "publish_time" not in df.columns:
        print("研报文件缺少必要列")
        return

    as_of_ts = pd.to_datetime(as_of_date)
    start_90 = as_of_ts - pd.Timedelta(days=lookback_days)
    start_360 = as_of_ts - pd.Timedelta(days=360)
    df["_pt"] = pd.to_datetime(df["publish_time"], errors="coerce")

    mask_90 = (df["_pt"] <= as_of_ts) & (df["_pt"] >= start_90)
    mask_360 = (df["_pt"] <= as_of_ts) & (df["_pt"] >= start_360)

    n_90 = mask_90.sum()
    n_360 = mask_360.sum()
    print(f"\n时间窗口: 90天=[{start_90.date()}, {as_of_date}], 360天=[{start_360.date()}, {as_of_date}]")
    print(f"90天内研报数: {n_90}, 360天内研报数: {n_360}")

    # 实际逻辑：90天有匹配时只用90天
    df_in = df.loc[mask_90].copy() if mask_90.any() else df.loc[mask_360].copy()
    window_used = "90天" if mask_90.any() else "360天"
    print(f"实际使用窗口: {window_used} (共 {len(df_in)} 篇研报)")
    print()

    growth_rates = []
    details = []

    for idx, (_, row) in enumerate(df_in.iterrows(), 1):
        pub_time = row.get("publish_time", "")
        org = row.get("org_name", "")
        title = str(row.get("title", ""))[:50] + "..." if len(str(row.get("title", ""))) > 50 else str(row.get("title", ""))
        raw = row.get("extracted_forecasts")

        f = _parse_forecasts_json(raw)
        if not f:
            details.append({
                "idx": idx,
                "publish_time": pub_time,
                "org": org,
                "title": title,
                "forecasts": "解析失败或为空",
                "g": None,
            })
            continue

        # 格式化 forecasts
        f_str = ", ".join([f"{y}: eps={v.get('eps')} pe={v.get('pe')}" for y, v in sorted(f.items())])
        res = _compute_g_from_forecasts(f)
        if res:
            g_val, method, y1, y2, n = res
            growth_rates.append(g_val)
            details.append({
                "idx": idx,
                "publish_time": pub_time,
                "org": org,
                "title": title,
                "forecasts": f_str,
                "g": g_val,
                "method": method,
                "years": f"{y1}-{y2} (n={n})",
            })
        else:
            details.append({
                "idx": idx,
                "publish_time": pub_time,
                "org": org,
                "title": title,
                "forecasts": f_str,
                "g": None,
            })

    # 输出每篇研报详情
    print("-" * 70)
    print("研报详情（按 publish_time 排序）")
    print("-" * 70)
    for d in details:
        print(f"\n【研报 {d['idx']}】")
        print(f"  发布日: {d['publish_time']}  机构: {d['org']}")
        print(f"  标题: {d['title']}")
        print(f"  extracted_forecasts: {d['forecasts']}")
        if d.get("g") is not None:
            print(f"  -> g = {d['g']:.4f} ({d['g']*100:.2f}%)  [方法: {d['method']}, 年份: {d['years']}]")
        else:
            print(f"  -> 无法计算 g (需至少2个年份的有效 eps 或 pe)")

    # 最终 g
    print()
    print("=" * 70)
    if growth_rates:
        mean_g = sum(growth_rates) / len(growth_rates)
        print(f"有效研报数: {len(growth_rates)} 篇")
        print(f"各研报 g 值: {[f'{x:.4f}' for x in growth_rates]}")
        print(f"最终 mean_g (增长率 g) = {mean_g:.4f} = {mean_g*100:.2f}%")
    else:
        print("无有效研报可计算 g -> get_recent_predict_peTTM 返回 None")
    print("=" * 70)


if __name__ == "__main__":
    main()
