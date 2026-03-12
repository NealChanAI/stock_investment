# -*- coding: utf-8 -*-
"""
严格按 价值投资系统建构举隅.txt 进行回测。
- 初始资金 100 万
- 从 2016 年第一个交易日开始
- 资金分 20 份，每半年再平衡
- 输出每次选股列表和操作收益

用法: python backtest_value_investment.py [--start-year 2016] [--end-year 2026]
"""
import argparse
import io
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR / "src" / "main"))

from stock_analysis import get_stock_info, get_sw_industry
from value_investment_system import ValueInvestmentSystem
from stock_info_extract import load_all_codes, STOCK_LIST_FILE
from industry_sector_config import is_stock_in_sectors

STOCK_DIR = ROOT_DIR / "data" / "company_stock_info"
LOG_DIR = ROOT_DIR / "log"
ALLOWED_SECTORS = ["医药", "食品", "消费"]  # 仅在此板块内买卖
TD_PATH = STOCK_DIR / "trading_dates.csv"
INITIAL_CAPITAL = 1_000_000
MAX_STOCKS = 20
REBALANCE_MONTHS = 6


def load_trading_dates():
    df = pd.read_csv(TD_PATH, encoding="utf-8-sig")
    return df["date"].astype(str).tolist()


def get_first_trading_day_of_month(dates: list, year: int, month: int) -> str:
    prefix = f"{year}-{month:02d}"
    for d in dates:
        if d.startswith(prefix):
            return d
    return None


def get_rebalance_dates(dates: list, start_year: int, end_year: int) -> list:
    """每半年第一个交易日：1月、7月"""
    min_date = f"{start_year}-01-01"
    out = []
    for y in range(start_year, end_year + 1):
        for m in [1, 7]:
            d = get_first_trading_day_of_month(dates, y, m)
            if d and d >= min_date:
                out.append(d)
    return sorted(out)


def get_close(code: str, date: str) -> float:
    """code 可为 sh.600312 或 600312"""
    c = str(code).replace("sh.", "").replace("sz.", "").strip()
    if len(c) > 6:
        c = c[-6:]
    p = STOCK_DIR / f"{c}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, encoding="utf-8-sig")
    row = df[df["date"] == date]
    if row.empty:
        return None
    return float(row["close"].iloc[0])


def run_evaluate_silent(system, stock_codes, target_date, stock_records):
    """静默评估，返回 DataFrame"""
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        df = system.evaluate_stock_list(stock_codes, target_date, stock_records)
        return df
    finally:
        sys.stdout = old_stdout


def run_select_best_silent(system, stock_codes, target_date, evaluation_df, stock_records):
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        best = system.select_best_stocks(
            stock_codes, target_date, evaluation_df=evaluation_df, stock_records=stock_records
        )
        return best
    finally:
        sys.stdout = old_stdout


def main():
    parser = argparse.ArgumentParser(description="价值投资系统回测")
    parser.add_argument("--start-year", type=int, default=2016, help="回测开始年份")
    parser.add_argument("--end-year", type=int, default=2026, help="回测结束年份")
    parser.add_argument("-o", "--output", type=str, help="将日志输出到指定文件（UTF-8）")
    args = parser.parse_args()
    dates = load_trading_dates()
    rebalance_dates = get_rebalance_dates(dates, args.start_year, args.end_year)
    if not rebalance_dates:
        print("无再平衡日期")
        return

    class TeeWriter:
        def __init__(self, filepath):
            self.file = open(filepath, "w", encoding="utf-8")
            self.stdout = sys.stdout
        def write(self, data):
            self.stdout.write(data)
            self.file.write(data)
            self.file.flush()
        def flush(self):
            self.stdout.flush()
            self.file.flush()
        def close(self):
            self.file.close()

    tee = None
    if args.output:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        output_path = LOG_DIR / Path(args.output).name
        tee = TeeWriter(output_path)
        print(f"日志保存至: {output_path}")
    if tee:
        sys.stdout = tee

    try:
        stock_records = load_all_codes(STOCK_LIST_FILE)
        # 限定：仅从医药、食品、消费板块中买卖
        filtered = []
        for r in stock_records:
            code = r.get("code", "")
            simple = str(code).replace("sh.", "").replace("sz.", "").strip()
            if len(simple) > 6:
                simple = simple[-6:]
            if len(simple) == 6 and simple.isdigit():
                industry = get_sw_industry(simple)
                if is_stock_in_sectors(industry, ALLOWED_SECTORS):
                    filtered.append(r)
        stock_records = filtered
        stock_codes = [r["code"] for r in stock_records]
        print(f"板块限定: 仅 {ALLOWED_SECTORS}，候选股票 {len(stock_codes)} 只")

        system = ValueInvestmentSystem()
        capital = INITIAL_CAPITAL
        positions = {}  # code -> {"shares": int, "cost": float, "stock_name": str, "industry": str}
        cash = capital
        prev_total_assets = INITIAL_CAPITAL

        print("=" * 80)
        print("价值投资系统回测 - 严格按《价值投资系统建构举隅》")
        print("=" * 80)
        print(f"初始资金: {INITIAL_CAPITAL/10000:.0f} 万")
        print(f"资金分 {MAX_STOCKS} 份，每半年再平衡")
        print()

        for i, rb_date in enumerate(rebalance_dates):
            print("=" * 80)
            print(f"【再平衡 {i+1}】日期: {rb_date}")
            print("=" * 80)

            # 1. 计算当前总资产
            portfolio_value = 0
            for code, pos in list(positions.items()):
                price = get_close(code, rb_date)
                if price is None:
                    price = pos.get("last_price", pos["cost"])
                pos["last_price"] = price
                pos["market_value"] = pos["shares"] * price
                portfolio_value += pos["market_value"]

            total_assets = cash + portfolio_value
            prev_total = capital if i == 0 else None
            if prev_total is not None and i > 0:
                prev_total = None  # 上一期总资产在上一轮已记录

            # 2. 检查卖出条件，卖出不符合条件的
            to_sell = []
            for code, pos in list(positions.items()):
                info = get_stock_info(code, rb_date)
                if info is None:
                    continue
                sell, reason = system.check_sell_conditions(info)
                if sell:
                    to_sell.append((code, pos, reason))

            for code, pos, reason in to_sell:
                price = get_close(code, rb_date) or pos["cost"]
                proceeds = pos["shares"] * price
                cost_basis = pos["shares"] * pos["cost"]
                pnl = proceeds - cost_basis
                pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0
                cash += proceeds
                del positions[code]
                print(f"  卖出 {code} {pos['stock_name']}: {reason}")
                print(f"    买入价: {pos['cost']:.2f} 卖出价: {price:.2f} 卖出金额: {proceeds:,.0f} 元  {'盈利' if pnl >= 0 else '亏损'}: {pnl:+,.0f} 元 ({pnl_pct:+.1f}%)")

            # 重新计算总资产
            portfolio_value = sum(p["shares"] * (get_close(c, rb_date) or p["cost"]) for c, p in positions.items())
            total_assets = cash + portfolio_value

            # 3. 按文档：再平衡是"卖出部分占用资金高的个股，买入部分占用资金低的个股"，
            #    在现有持仓内调整仓位，而非每期换一批新股票。换股需收益差>35%且谨慎操作。
            #    建仓：仅当空仓时，选出最满意的20只建仓。
            target_per_slot = total_assets / MAX_STOCKS
            n_hold = len(positions)

            eval_df = None  # 用于选股列表展示 PE/G/预估收益
            if n_hold == 0:
                # 建仓：空仓时选出最满意的股票
                eval_df = run_evaluate_silent(system, stock_codes, rb_date, stock_records)
                best = run_select_best_silent(system, stock_codes, rb_date, eval_df, stock_records)
                if len(best) == 0:
                    print("  本期无可选股票，保持空仓")
                else:
                    for s in best:
                        code = s["stock_code"]
                        price = get_close(code, rb_date)
                        if price is None or price <= 0:
                            continue
                        shares = int(target_per_slot / price / 100) * 100
                        if shares <= 0:
                            continue
                        cost = shares * price
                        if cost > cash:
                            shares = int(cash / price / 100) * 100
                            if shares <= 0:
                                continue
                            cost = shares * price
                        cash -= cost
                        positions[code] = {
                            "shares": shares, "cost": price,
                            "stock_name": s.get("stock_name", ""), "industry": s.get("industry", ""),
                        }
                        if len(positions) >= MAX_STOCKS:
                            break
            else:
                # 有持仓：换股（酌情）-> 加仓（若有空位）-> 再平衡
                # 文档：当新标的远远优于持有标的(收益差>35%)时酌情换股
                system.portfolio = [
                    {"stock_code": c, "industry": p.get("industry", "未知")}
                    for c, p in positions.items()
                ]
                eval_df = run_evaluate_silent(system, stock_codes, rb_date, stock_records)
                best = run_select_best_silent(system, stock_codes, rb_date, eval_df, stock_records)

                # 换股：若存在收益差>35%的换入标的，执行一次换股（文档：不要轻易尝试，故每期最多1次）
                best_swap = None
                for curr_code, pos in list(positions.items()):
                    curr_info = get_stock_info(curr_code, rb_date)
                    if curr_info is None:
                        continue
                    curr_dict = {
                        "stock_code": curr_code,
                        "pettm_at_date": curr_info.get("pettm_at_date"),
                        "mean_pettm_5y": curr_info.get("mean_pettm_5y"),
                        "mean_e_growth_rate": curr_info.get("mean_e_growth_rate", 0),
                        "industry": pos.get("industry", "未知"),
                    }
                    for s in best:
                        if s["stock_code"] in positions:
                            continue
                        new_dict = {
                            "stock_code": s["stock_code"],
                            "pettm_at_date": s.get("pe_now"),
                            "mean_pettm_5y": s.get("pe_mean"),
                            "mean_e_growth_rate": (s.get("growth_rate") or 0) / 100,
                            "industry": s.get("industry", "未知"),
                        }
                        ok, _ = system.check_swap_conditions(curr_dict, new_dict)
                        if ok:
                            ret_curr = system.calculate_target_return(
                                curr_dict["pettm_at_date"], curr_dict["mean_pettm_5y"],
                                system.limit_g_credible(curr_dict.get("mean_e_growth_rate", 0))
                            )
                            ret_new = system.calculate_target_return(
                                new_dict["pettm_at_date"], new_dict["mean_pettm_5y"],
                                system.limit_g_credible(new_dict.get("mean_e_growth_rate", 0))
                            )
                            diff = ret_new - ret_curr
                            if best_swap is None or diff > best_swap[2]:
                                best_swap = (curr_code, s, diff)
                if best_swap:
                    curr_code, new_s, diff = best_swap
                    pos = positions[curr_code]
                    code_new = new_s["stock_code"]
                    price_out = get_close(curr_code, rb_date) or pos["cost"]
                    proceeds = pos["shares"] * price_out
                    cost_basis = pos["shares"] * pos["cost"]
                    pnl = proceeds - cost_basis
                    pnl_pct = (pnl / cost_basis * 100) if cost_basis > 0 else 0
                    cash += proceeds
                    del positions[curr_code]
                    print(f"  换股 {curr_code} {pos['stock_name']} -> {code_new} {new_s.get('stock_name','')}: 收益差={diff*100:.1f}%>35%")
                    print(f"    卖出 {curr_code}: 买入价 {pos['cost']:.2f} 卖出价 {price_out:.2f} 卖出金额 {proceeds:,.0f} 元  {'盈利' if pnl >= 0 else '亏损'}: {pnl:+,.0f} 元 ({pnl_pct:+.1f}%)")
                    price_new = get_close(code_new, rb_date)
                    if price_new and price_new > 0 and cash >= target_per_slot * 0.5:
                        buy_shares = int(target_per_slot / price_new / 100) * 100
                        if buy_shares > 0:
                            cost = min(buy_shares * price_new, cash)
                            buy_shares = int(cost / price_new / 100) * 100
                            if buy_shares > 0:
                                cost = buy_shares * price_new
                                cash -= cost
                                positions[code_new] = {
                                    "shares": buy_shares, "cost": price_new,
                                    "stock_name": new_s.get("stock_name", ""),
                                    "industry": new_s.get("industry", ""),
                                }
                    system.portfolio = [
                        {"stock_code": c, "industry": p.get("industry", "未知")}
                        for c, p in positions.items()
                    ]
                    best = run_select_best_silent(system, stock_codes, rb_date, eval_df, stock_records)

                # 加仓：若持仓<20且有空位，买入符合条件的新股票（换股后 best 已重算）
                if len(positions) < MAX_STOCKS and cash > target_per_slot * 0.5:
                    system.portfolio = [
                        {"stock_code": c, "industry": p.get("industry", "未知")}
                        for c, p in positions.items()
                    ]
                    for s in (best or []):
                        if len(positions) >= MAX_STOCKS or cash < target_per_slot * 0.5:
                            break
                        code = s["stock_code"]
                        if code in positions:
                            continue
                        price = get_close(code, rb_date)
                        if price is None or price <= 0:
                            continue
                        shares = int(target_per_slot / price / 100) * 100
                        if shares <= 0:
                            continue
                        cost = min(shares * price, cash)
                        buy_shares = int(cost / price / 100) * 100
                        if buy_shares <= 0:
                            continue
                        cost = buy_shares * price
                        cash -= cost
                        positions[code] = {
                            "shares": buy_shares, "cost": price,
                            "stock_name": s.get("stock_name", ""), "industry": s.get("industry", ""),
                        }
                        print(f"  加仓 {code} {s.get('stock_name', '')}: {buy_shares} 股 (空位补入)")
                for code, pos in list(positions.items()):
                    price = get_close(code, rb_date) or pos["cost"]
                    if price <= 0:
                        continue
                    target_shares = max(100, int(target_per_slot / price / 100) * 100)
                    if pos["shares"] > target_shares:
                        sell_shares = pos["shares"] - target_shares
                        sell_proceeds = sell_shares * price
                        sell_cost = sell_shares * pos["cost"]
                        pnl = sell_proceeds - sell_cost
                        pnl_pct = (pnl / sell_cost * 100) if sell_cost > 0 else 0
                        cash += sell_proceeds
                        pos["shares"] = target_shares
                        print(f"  再平衡卖出 {code} {pos['stock_name']}: {sell_shares} 股 (超配)  卖出价: {price:.2f} 成本价: {pos['cost']:.2f}  {'盈利' if pnl >= 0 else '亏损'}: {pnl:+,.0f} 元 ({pnl_pct:+.1f}%)")
                    elif pos["shares"] < target_shares:
                        need_shares = target_shares - pos["shares"]
                        buy_shares = min(need_shares, int(cash / price / 100) * 100)
                        if buy_shares > 0:
                            cost = buy_shares * price
                            cash -= cost
                            old_total = pos["shares"] * pos["cost"]
                            pos["shares"] += buy_shares
                            pos["cost"] = (old_total + cost) / pos["shares"]
                            print(f"  再平衡买入 {code} {pos['stock_name']}: {buy_shares} 股 (低配)")

            # 6. 输出选股列表（含 PE、历史PE均值、G、预估收益）
            print(f"\n  选股列表 (共 {len(positions)} 只):")
            eval_lookup = eval_df.set_index("stock_code") if eval_df is not None and not eval_df.empty else None
            for code, pos in positions.items():
                base = f"    {code} {pos['stock_name']}: {pos['shares']} 股 @ {pos['cost']:.2f} = {pos['shares']*pos['cost']:,.0f} 元"
                if eval_lookup is not None and code in eval_lookup.index:
                    row = eval_lookup.loc[code]
                    pe = row.get("pe_now")
                    pe_mean = row.get("pe_mean")
                    g = row.get("growth_rate")
                    tr = row.get("target_return")
                    extras = []
                    if pe is not None and not (isinstance(pe, float) and pd.isna(pe)):
                        extras.append(f"PE:{pe:.1f}")
                    if pe_mean is not None and not (isinstance(pe_mean, float) and pd.isna(pe_mean)):
                        extras.append(f"历史PE:{pe_mean:.1f}")
                    if g is not None and not (isinstance(g, float) and pd.isna(g)):
                        extras.append(f"G:{g:.1f}%")
                    if tr is not None and not (isinstance(tr, float) and pd.isna(tr)) and tr > -99:
                        extras.append(f"预估收益:{tr:.1f}%")
                    if extras:
                        base += "  [" + " ".join(extras) + "]"
                print(base)

            # 7. 计算本期收益
            portfolio_value = sum(p["shares"] * (get_close(c, rb_date) or p["cost"]) for c, p in positions.items())
            total_assets = cash + portfolio_value

            period_ret = (total_assets - prev_total_assets) / prev_total_assets
            cum_ret = (total_assets - INITIAL_CAPITAL) / INITIAL_CAPITAL

            print(f"\n  总资产: {total_assets:,.0f} 元 (现金 {cash:,.0f} + 持仓 {portfolio_value:,.0f})")
            print(f"  本期收益率: {period_ret*100:.2f}%")
            print(f"  累计收益率: {cum_ret*100:.2f}%")
            print()

            prev_total_assets = total_assets
            capital = total_assets

        print("=" * 80)
        print("回测结束")
        print("=" * 80)
        print(f"最终总资产: {capital:,.0f} 元")
        print(f"累计收益率: {(capital/INITIAL_CAPITAL-1)*100:.2f}%")
    finally:
        if tee:
            sys.stdout = tee.stdout
            tee.close()


if __name__ == "__main__":
    main()
