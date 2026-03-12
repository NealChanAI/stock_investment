# -*- coding: utf-8 -*-
"""
按「沪深300」成分股逐只爬取研报，每只股票保存到 data/company_research 目录。

- 日期范围：2010-01-01 至当天
- 请求间隔：单次爬取内 DOWNLOAD_DELAY=6 秒；每完成一只股票后间隔 25 秒再爬下一只，降低被封风险
- 输出文件：data/company_research/reports_{股票代码}_{股票名称}.csv，名称中的非法文件字符会替换为下划线

使用前请激活环境：conda activate research_report_spider
运行方式：python run_hs300_by_stock.py
可选参数：--delay 6 --between 25 --start 20100101 --end 20260307
"""
import os
import re
import shutil
import sys
import time
import subprocess
from datetime import datetime
from pathlib import Path

# 默认参数
DEFAULT_START = "20100101"
DEFAULT_END = datetime.now().strftime("%Y%m%d")
DEFAULT_DOWNLOAD_DELAY = 6   # 单次爬取内，每请求间隔（秒）
DEFAULT_BETWEEN_DELAY = 25  # 每完成一只股票后，等待多少秒再爬下一只

# 项目根目录、company_research 目录
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT_DIR = SCRIPT_DIR.parent
COMPANY_RESEARCH_DIR = ROOT_DIR / "data" / "company_research"


def _sanitize_filename(name):
    """将股票名称中不可做文件名的字符替换为下划线"""
    if not name or not isinstance(name, str):
        return ""
    # Windows / Linux 文件名非法字符
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    name = name.strip().strip(".") or "未命名"
    return name[:50]  # 避免过长


def get_hs300_list():
    """获取沪深300成分股列表，返回 [(code, name), ...]，code 为 6 位数字代码"""
    try:
        import akshare as ak
    except ImportError:
        print("请先安装 akshare: pip install akshare")
        sys.exit(1)
    # 沪深300 指数代码 000300
    df = ak.index_stock_cons_csindex(symbol="000300")
    # 列名：成分券代码、成分券名称
    code_col = "成分券代码"
    name_col = "成分券名称"
    if code_col not in df.columns:
        code_col = [c for c in df.columns if "代码" in c][0]
    if name_col not in df.columns:
        name_col = [c for c in df.columns if "名称" in c][0]
    out = []
    for _, row in df.iterrows():
        raw_code = str(row[code_col]).strip()
        # 取 6 位代码（如 000300.SH -> 000300）
        code = raw_code.split(".")[0].strip()
        if len(code) == 6 and code.isdigit():
            name = str(row.get(name_col, "")).strip() or code
            name = _sanitize_filename(name)
            out.append((code, name))
    return out


def main():
    import argparse
    parser = argparse.ArgumentParser(description="沪深300 成分股逐只爬取研报，保存到 data/company_research")
    parser.add_argument("--start", default=DEFAULT_START, help="开始日期 YYYYMMDD，默认 20100101")
    parser.add_argument("--end", default=DEFAULT_END, help="结束日期 YYYYMMDD，默认今天")
    parser.add_argument("--delay", type=float, default=DEFAULT_DOWNLOAD_DELAY,
                        help="单次爬取内请求间隔秒数，默认 6")
    parser.add_argument("--between", type=float, default=DEFAULT_BETWEEN_DELAY,
                        help="每只股票爬完后等待秒数，默认 25")
    parser.add_argument("--limit", type=int, default=0,
                        help="仅爬前 N 只股票（调试用），0 表示全部")
    parser.add_argument("--skip", type=int, default=0,
                        help="跳过前 N 只股票（用于断点续跑），默认 0")
    args = parser.parse_args()

    project_dir = str(SCRIPT_DIR)
    os.chdir(project_dir)

    print("正在获取沪深300成分股列表……")
    stocks = get_hs300_list()
    total = len(stocks)
    print("共 {} 只成分股。日期范围: {} ~ {} | 请求间隔: {} 秒 | 每只间隔: {} 秒".format(
          total, args.start, args.end, args.delay, args.between))
    print("输出目录: {}".format(COMPANY_RESEARCH_DIR))

    if args.skip > 0:
        stocks = stocks[args.skip:]
        print("已跳过前 {} 只，本次将爬取 {} 只".format(args.skip, len(stocks)))
    if args.limit > 0:
        stocks = stocks[: args.limit]
        print("限制仅爬前 {} 只".format(len(stocks)))

    COMPANY_RESEARCH_DIR.mkdir(parents=True, exist_ok=True)

    n_stocks = len(stocks)
    for i, (code, name) in enumerate(stocks):
        nth = i + 1
        out_suffix = "{}_{}".format(code, name)
        out_file = "reports_{}.csv".format(out_suffix)
        dest_path = COMPANY_RESEARCH_DIR / out_file
        print("\n" + "=" * 60)
        print("[{}/{}] 正在爬取: {} ({}) -> {}".format(nth, n_stocks, name, code, out_file))
        print("=" * 60)

        cmd = [
            sys.executable, "run.py",
            "-s", code,
            "--start", args.start,
            "--end", args.end,
            "--delay", str(args.delay),
            "-o", out_suffix,
        ]
        ret = subprocess.run(cmd, cwd=project_dir)
        if ret.returncode != 0:
            print("[警告] 本只股票爬取异常退出，returncode={}".format(ret.returncode))

        # 将爬取结果复制到 company_research
        src_path = SCRIPT_DIR / out_file
        if src_path.exists():
            shutil.copy2(src_path, dest_path)
            print("  已保存到 {}".format(dest_path))
        else:
            print("  [注意] 未找到输出文件 {}".format(src_path))

        # 每只之间间隔，最后一只可不等
        if i < len(stocks) - 1:
            print("\n[间隔] 等待 {} 秒后爬取下一只……".format(int(args.between)))
            time.sleep(args.between)

    print("\n全部完成。每只股票研报已保存到 data/company_research/reports_{代码}_{名称}.csv。")


if __name__ == "__main__":
    main()
