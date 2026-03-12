# -*- coding: utf-8 -*-
"""
将历史 PE/PB、股价、行业信息存储到 data/company_stock_info，每公司一 CSV。
支持日增更新：已有数据则只拉取缺失日期。
交易系统所需特征均可从日频 date/peTTM/pbMRQ/open/close 等计算得出。

用法：
  python src/main/build_company_stock_info.py                    # 全量拉取（默认 zz500）
  python src/main/build_company_stock_info.py --incremental     # 日增更新
  python src/main/build_company_stock_info.py --stock-list data/self_selected_stocks.csv
  python src/main/build_company_stock_info.py --workers 32      # 并发线程数（默认 16）
"""

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple, Any

import pandas as pd

DEFAULT_WORKERS = 1
MAX_FETCH_RETRIES = 3
RETRY_DELAY_SEC = 2

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
COMPANY_STOCK_INFO_DIR = DATA_DIR / "company_stock_info"
DEFAULT_STOCK_LIST = DATA_DIR / "zz500_stocks.csv"
START_DATE = "2010-01-01"

# 日频 CSV 列（与 Baostock 字段对应）
DAILY_COLUMNS = [
    "date", "open", "high", "low", "close",
    "volume", "amount", "turn",
    "peTTM", "pbMRQ",
]
# Baostock 请求字段
BAOSTOCK_FIELDS = "date,code,open,high,low,close,volume,amount,turn,peTTM,pbMRQ"


def _ensure_src_path():
    src = ROOT_DIR / "src" / "main"
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def _code_to_simple(code: str) -> str:
    """sh.600004 -> 600004"""
    s = str(code).strip()
    if "." in s:
        return s.split(".")[-1]
    return s


def _code_to_bs(code: str) -> str:
    """确保带交易所前缀，供 Baostock 使用"""
    _ensure_src_path()
    from stock_analysis import add_stock_prefix
    s = _code_to_simple(code)
    if len(s) == 6 and s.isdigit():
        return add_stock_prefix(s)
    if "." in str(code):
        return str(code).strip()
    return add_stock_prefix(s)


def _load_stock_list(csv_path: Path):
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "code" not in df.columns:
        raise ValueError(f"Stock list missing code column: {csv_path}")
    codes = df["code"].astype(str).str.strip().dropna().unique().tolist()
    names = {}
    if "code_name" in df.columns:
        for _, row in df.iterrows():
            codes_raw = str(row["code"]).strip()
            names[codes_raw] = str(row.get("code_name", "")).strip()
    return codes, names


def _get_industry(simple_code: str) -> str:
    """获取股票申万一级行业（如：商贸零售、食品饮料）。"""
    _ensure_src_path()
    try:
        from stock_analysis import get_sw_industry
        return get_sw_industry(simple_code)
    except Exception:
        pass
    return ""


def _get_predict_data(simple_code: str) -> tuple:
    """获取研报预测的 mean_e_growth_rate 和 report_infos，失败返回 (None, '')。build 时允许调 API。"""
    _ensure_src_path()
    try:
        from stock_analysis import get_recent_predict_peTTM
        res = get_recent_predict_peTTM(simple_code, from_api=True)
        if res:
            g = res.get("mean_e_growth_rate")
            info = res.get("report_infos", "")
            return (float(g) if g is not None and not (isinstance(g, float) and pd.isna(g)) else None, info or "")
    except Exception:
        pass
    return (None, "")


def _save_trading_dates(bs_mod, today: str):
    """拉取交易日历并保存到 trading_dates.csv"""
    try:
        rs = bs_mod.query_trade_dates(start_date=START_DATE, end_date=today)
        if rs.error_code != "0":
            return
        dates = []
        while rs.next():
            row = rs.get_row_data()
            if row[1] == "1":
                dates.append(row[0])
        if dates:
            df = pd.DataFrame({"date": sorted(set(dates))})
            df.to_csv(COMPANY_STOCK_INFO_DIR / "trading_dates.csv", index=False, encoding="utf-8-sig")
    except Exception:
        pass


def _fetch_one_stock_baostock(bs_mod, code_bs: str, start_date: str, end_date: str):
    """Fetch daily data for one symbol. Retry on Baostock data errors (decompress/encoding)."""
    last_err = None
    for attempt in range(MAX_FETCH_RETRIES):
        try:
            rs = bs_mod.query_history_k_data_plus(
                code_bs,
                BAOSTOCK_FIELDS,
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3",
            )
            if rs.error_code != "0":
                return None
            rows = []
            while rs.next():
                rows.append(rs.get_row_data())
            if not rows:
                return None
            df = pd.DataFrame(rows, columns=rs.fields)
            for c in ["open", "high", "low", "close", "volume", "amount", "turn", "peTTM", "pbMRQ"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            df = df.sort_values("date").reset_index(drop=True)
            return df
        except Exception as e:
            last_err = e
            if attempt < MAX_FETCH_RETRIES - 1:
                time.sleep(RETRY_DELAY_SEC)
    return None


def _get_existing_max_date(csv_path: Path) -> Optional[str]:
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig", nrows=0)
        if "date" not in df.columns:
            return None
        df = pd.read_csv(csv_path, encoding="utf-8-sig", usecols=["date"])
        if df.empty:
            return None
        return df["date"].max()
    except Exception:
        return None


def _save_daily_csv(df: pd.DataFrame, csv_path: Path, incremental: bool):
    out_cols = [c for c in DAILY_COLUMNS if c in df.columns]
    work = df.copy()
    if "code" in work.columns:
        work = work.drop(columns=["code"])
    work = work[[c for c in out_cols if c in work.columns]]
    if incremental and csv_path.exists():
        existing = pd.read_csv(csv_path, encoding="utf-8-sig")
        existing_dates = set(existing["date"].astype(str))
        new_rows = work[~work["date"].astype(str).isin(existing_dates)]
        if new_rows.empty:
            return 0
        combined = pd.concat([existing, new_rows], ignore_index=True)
        combined = combined.sort_values("date").drop_duplicates(subset=["date"], keep="last")
        combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
        return len(new_rows)
    work.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return len(work)


def _ensure_meta_row(meta_path: Path, code: str, code_name: str, industry: str,
                    mean_e_growth_rate: float = None, report_infos: str = ""):
    row = {"code": code, "code_name": code_name, "industry": industry}
    if mean_e_growth_rate is not None:
        row["mean_e_growth_rate"] = mean_e_growth_rate
    if report_infos:
        row["report_infos"] = str(report_infos)[:2000]
    if meta_path.exists():
        meta = pd.read_csv(meta_path, encoding="utf-8-sig")
        for c in ["mean_e_growth_rate", "report_infos"]:
            if c not in meta.columns and c in row:
                meta[c] = None
        mask = meta["code"].astype(str) == str(code)
        if mask.any():
            for k, v in row.items():
                meta.loc[mask, k] = v
        else:
            meta = pd.concat([meta, pd.DataFrame([row])], ignore_index=True)
    else:
        meta = pd.DataFrame([row])
    meta.to_csv(meta_path, index=False, encoding="utf-8-sig")


def _process_one_stock(
    task: dict,
) -> Tuple[str, str, str, Any, str, int, bool, Optional[str]]:
    """
    处理单只股票。每线程需独立 login/logout Baostock。
    返回: (code_raw, code_name, industry, mean_g, report_infos, n_rows, success, error_msg)
    """
    import baostock as bs_mod

    code_raw = task["code_raw"]
    simple = task["simple"]
    code_bs = task["code_bs"]
    code_name = task["code_name"]
    csv_path = task["csv_path"]
    start_date = task["start_date"]
    today = task["today"]
    incremental = task["incremental"]
    skip_industry = task["skip_industry"]
    skip_predict = task["skip_predict"]
    idx = task["idx"]
    total = task["total"]

    lg = bs_mod.login()
    if lg.error_code != "0":
        return (code_raw, code_name, "", None, "", 0, False, f"Baostock login failed: {lg.error_msg}")

    try:
        df = _fetch_one_stock_baostock(bs_mod, code_bs, start_date, today)
        if df is None or df.empty:
            return (code_raw, code_name, "", None, "", 0, False, "no data")

        n = _save_daily_csv(df, csv_path, incremental)
        industry = ""
        mean_g, report_infos = None, ""
        if not skip_industry:
            industry = _get_industry(simple)
        if not skip_predict:
            mean_g, report_infos = _get_predict_data(simple)
        return (code_raw, code_name, industry, mean_g, report_infos, n, True, None)
    except Exception as e:
        return (code_raw, code_name, "", None, "", 0, False, str(e))
    finally:
        bs_mod.logout()


def main():
    parser = argparse.ArgumentParser(description="Build company_stock_info local data")
    parser.add_argument("--stock-list", type=str, default=str(DEFAULT_STOCK_LIST), help="stock list CSV")
    parser.add_argument("--incremental", action="store_true", help="incremental update, fetch missing dates only")
    parser.add_argument("--skip-industry", action="store_true", help="skip industry fetch (save time)")
    parser.add_argument("--skip-predict", action="store_true", help="skip predict fetch (save time)")
    parser.add_argument("--limit", type=int, default=0, help="process first N symbols (0=all, for testing)")
    parser.add_argument("--skip", type=int, default=0, help="skip first N symbols, resume from (N+1)th (e.g. --skip 242)")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"concurrent workers (default {DEFAULT_WORKERS})")
    args = parser.parse_args()

    stock_list_path = Path(args.stock_list)
    if not stock_list_path.is_absolute():
        stock_list_path = ROOT_DIR / stock_list_path
    if not stock_list_path.exists():
        print(f"Stock list not found: {stock_list_path}")
        sys.exit(1)

    COMPANY_STOCK_INFO_DIR.mkdir(parents=True, exist_ok=True)
    meta_path = COMPANY_STOCK_INFO_DIR / "meta.csv"

    codes, names = _load_stock_list(stock_list_path)
    if args.skip > 0:
        codes = codes[args.skip:]
        print(f"Skipped first {args.skip} symbols, processing from #{args.skip + 1} (--skip={args.skip})")
    if args.limit > 0:
        codes = codes[: args.limit]
        print(f"Processing first {len(codes)} symbols (--limit={args.limit})")
    print(f"{len(codes)} symbols, mode: {'incremental' if args.incremental else 'full'}, workers: {args.workers}\n")

    _ensure_src_path()
    import baostock as bs_mod
    lg = bs_mod.login()
    if lg.error_code != "0":
        print(f"Baostock login failed: {lg.error_msg}")
        sys.exit(1)

    today = datetime.now().strftime("%Y-%m-%d")
    _save_trading_dates(bs_mod, today)
    bs_mod.logout()

    # 若需行业信息，预先构建申万行业映射缓存，避免多线程并发构建
    if not args.skip_industry:
        from stock_analysis import get_sw_industry
        _ = get_sw_industry("000001")  # 触发缓存构建或加载

    # 构建任务列表
    tasks: List[dict] = []
    skip_count = 0
    for i, code_raw in enumerate(codes, 1):
        simple = _code_to_simple(code_raw)
        code_bs = _code_to_bs(code_raw)
        code_name = names.get(code_raw, "")
        csv_path = COMPANY_STOCK_INFO_DIR / f"{simple}.csv"

        start_date = START_DATE
        if args.incremental:
            max_d = _get_existing_max_date(csv_path)
            if max_d:
                next_d = (datetime.strptime(max_d, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                if next_d > today:
                    skip_count += 1
                    continue
                start_date = next_d

        tasks.append({
            "code_raw": code_raw,
            "simple": simple,
            "code_bs": code_bs,
            "code_name": code_name,
            "csv_path": csv_path,
            "start_date": start_date,
            "today": today,
            "incremental": args.incremental,
            "skip_industry": args.skip_industry,
            "skip_predict": args.skip_predict,
            "idx": i + args.skip,
            "total": len(codes) + args.skip,
        })

    if skip_count > 0:
        print(f"Skipped (already up to date): {skip_count}\n")

    ok, fail = skip_count, 0
    meta_updates: List[Tuple[str, str, str, Any, str]] = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_process_one_stock, t): t for t in tasks}
        for future in as_completed(futures):
            task = futures[future]
            simple = task["simple"]
            code_name = task["code_name"]
            idx, total = task["idx"], task["total"]
            try:
                code_raw, code_name, industry, mean_g, report_infos, n_rows, success, err = future.result()
                if success:
                    meta_updates.append((code_raw, code_name, industry, mean_g, report_infos))
                    print(f"  [{idx}/{total}] {simple} {code_name} - wrote {n_rows} rows")
                    ok += 1
                else:
                    print(f"  [{idx}/{total}] {simple} {code_name} - {err or 'no data'}")
                    fail += 1
            except Exception as e:
                print(f"  [{idx}/{total}] {simple} {code_name} - error: {e}")
                fail += 1

    # 批量更新 meta
    if meta_updates:
        meta = pd.read_csv(meta_path, encoding="utf-8-sig") if meta_path.exists() else pd.DataFrame()
        for c in ["mean_e_growth_rate", "report_infos"]:
            if c not in meta.columns:
                meta[c] = None
        for code_raw, code_name, industry, mean_g, report_infos in meta_updates:
            row = {"code": code_raw, "code_name": code_name, "industry": industry}
            if mean_g is not None:
                row["mean_e_growth_rate"] = mean_g
            if report_infos:
                row["report_infos"] = str(report_infos)[:2000]
            if not meta.empty and "code" in meta.columns:
                mask = meta["code"].astype(str) == str(code_raw)
                if mask.any():
                    for k, v in row.items():
                        meta.loc[mask, k] = v
                    continue
            meta = pd.concat([meta, pd.DataFrame([row])], ignore_index=True)
        meta.to_csv(meta_path, index=False, encoding="utf-8-sig")

    print(f"\nDone: ok {ok}, fail {fail}")
    print(f"Data dir: {COMPANY_STOCK_INFO_DIR}")


if __name__ == "__main__":
    main()
