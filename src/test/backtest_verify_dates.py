# -*- coding: utf-8 -*-
"""
导出指定日期的沪深300+中证500（医药、食品、消费板块）全量评估数据，
用于验证选股和再平衡逻辑。

用法: python backtest_verify_dates.py [--dates 2020-01-02,2020-07-01] [-o output.xlsx]
"""
import argparse
import io
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DIR / "src" / "main"))

from stock_analysis import get_stock_info, get_sw_industry
from value_investment_system import ValueInvestmentSystem
from stock_info_extract import load_all_codes
from industry_sector_config import is_stock_in_sectors

DATA_DIR = ROOT_DIR / "data"
LOG_DIR = ROOT_DIR / "log"
STOCK_LIST_FILE = DATA_DIR / "hs300_zz500_stocks.csv"
HS300_FILE = DATA_DIR / "hs300_stocks.csv"
ZZ500_FILE = DATA_DIR / "zz500_stocks.csv"
ALLOWED_SECTORS = ["医药", "食品", "消费"]
DEFAULT_DATES = ["2020-01-02", "2020-07-01"]


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
    """静默选股，返回选中的股票列表"""
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
    parser = argparse.ArgumentParser(description="导出指定日期的全量评估数据，用于验证选股逻辑")
    parser.add_argument(
        "--dates",
        type=str,
        default=",".join(DEFAULT_DATES),
        help="逗号分隔的日期列表，如 2020-01-02,2020-07-01",
    )
    parser.add_argument(
        "-o", "--output",
        type=str,
        default="data/backtest_verify_2020.xlsx",
        help="输出 Excel 文件路径",
    )
    args = parser.parse_args()

    dates = [d.strip() for d in args.dates.split(",") if d.strip()]
    if not dates:
        print("请指定至少一个日期")
        sys.exit(1)

    if not STOCK_LIST_FILE.exists():
        print(f"股票列表不存在: {STOCK_LIST_FILE}")
        print("请先运行合并脚本或确保 data/hs300_zz500_stocks.csv 存在")
        sys.exit(1)

    print(f"加载股票池: {STOCK_LIST_FILE}")
    stock_records = load_all_codes(str(STOCK_LIST_FILE))

    # 限定：仅医药、食品、消费
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
    print()

    # 加载指数成分用于标注来源
    hs300_codes = set()
    zz500_codes = set()
    if HS300_FILE.exists():
        df_h = pd.read_csv(HS300_FILE)
        hs300_codes = set(df_h["code"].astype(str).str.replace(r"^(sh|sz)\.", "", regex=True).tolist())
    if ZZ500_FILE.exists():
        df_z = pd.read_csv(ZZ500_FILE)
        zz500_codes = set(df_z["code"].astype(str).str.replace(r"^(sh|sz)\.", "", regex=True).tolist())

    def _index_source(code):
        c = str(code).replace("sh.", "").replace("sz.", "").strip()
        if len(c) > 6:
            c = c[-6:]
        both = c in hs300_codes and c in zz500_codes
        in_h = c in hs300_codes
        in_z = c in zz500_codes
        if both:
            return "沪深300+中证500"
        if in_h:
            return "沪深300"
        if in_z:
            return "中证500"
        return ""

    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    predict_log = LOG_DIR / f"verify_{output_path.stem}_predict.log"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    system = ValueInvestmentSystem(config={"predict_log_path": str(predict_log)})
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for date in dates:
            print(f"评估日期: {date}")
            eval_df = run_evaluate_silent(system, stock_codes, date, stock_records)
            if eval_df.empty:
                print(f"  无评估结果，跳过")
                continue

            # 添加上市来源列
            eval_df.insert(2, "指数来源", eval_df["stock_code"].apply(_index_source))

            # 全量评估结果
            sheet_all = f"{date}_全量"
            eval_df.to_excel(writer, sheet_name=sheet_all[:31], index=False)
            print(f"  全量: {len(eval_df)} 只 -> Sheet [{sheet_all[:31]}]")

            # 选中的 20 只（best 来自 eval_df，已含指数来源列）
            best = run_select_best_silent(system, stock_codes, date, eval_df, stock_records)
            if best:
                best_df = pd.DataFrame(best)
                if "指数来源" not in best_df.columns:
                    best_df.insert(2, "指数来源", best_df["stock_code"].apply(_index_source))
                sheet_sel = f"{date}_选中{len(best)}只"
                best_df.to_excel(writer, sheet_name=sheet_sel[:31], index=False)
                print(f"  选中: {len(best)} 只 -> Sheet [{sheet_sel[:31]}]")
            print()

    print(f"已保存至: {output_path}")


if __name__ == "__main__":
    main()
