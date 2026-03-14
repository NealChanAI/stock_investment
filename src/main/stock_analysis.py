"""
zhongmian stock analysis
"""

import json
import pandas as pd
import baostock as bs
import akshare as ak
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
COMPANY_STOCK_INFO_DIR = ROOT_DIR / "data" / "company_stock_info"
COMPANY_RESEARCH_DIR = ROOT_DIR / "data" / "company_research"
SW_INDUSTRY_MAP_PATH = COMPANY_STOCK_INFO_DIR / "sw_industry_map.csv"


def _build_sw_industry_map() -> dict:
    """从 akshare 申万接口构建 股票代码->申万一级行业 映射并缓存到文件。"""
    result = {}
    try:
        first_df = ak.sw_index_first_info()
        for _, row in first_df.iterrows():
            code = str(row.get("行业代码", "")).strip()
            if not code:
                continue
            try:
                cons_df = ak.sw_index_third_cons(symbol=code)
                if cons_df.empty or "股票代码" not in cons_df.columns or "申万1级" not in cons_df.columns:
                    continue
                for _, r in cons_df.iterrows():
                    stock_code = str(r.get("股票代码", "")).strip()
                    industry = str(r.get("申万1级", "")).strip()
                    if stock_code and industry:
                        simple = stock_code.replace(".SH", "").replace(".SZ", "").split(".")[-1]
                        if len(simple) == 6 and simple.isdigit():
                            result[simple] = industry
            except Exception:
                continue
        if result:
            pd.DataFrame([{"code": k, "industry": v} for k, v in result.items()]).to_csv(
                SW_INDUSTRY_MAP_PATH, index=False, encoding="utf-8-sig"
            )
    except Exception:
        pass
    return result


def get_sw_industry(simple_code: str) -> str:
    """
    获取股票申万一级行业（如：商贸零售、食品饮料）。
    优先从 sw_industry_map.csv 缓存读取，若无则从 akshare 申万接口构建映射。
    """
    simple = str(simple_code).strip().replace(".SH", "").replace(".SZ", "")
    if "." in simple:
        simple = simple.split(".")[-1]
    if len(simple) != 6 or not simple.isdigit():
        return ""
    # 1. 从 sw_industry_map.csv 缓存读取
    if SW_INDUSTRY_MAP_PATH.exists():
        try:
            cache_df = pd.read_csv(SW_INDUSTRY_MAP_PATH, encoding="utf-8-sig")
            cache_df["_simple"] = cache_df["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6).str[-6:]
            match = cache_df[cache_df["_simple"] == simple]
            if not match.empty:
                return str(match["industry"].iloc[0]).strip()
        except Exception:
            pass
    # 3. 构建映射并查找
    mapping = _build_sw_industry_map()
    return mapping.get(simple, "")


def baostock_login():
    """baostock login"""
    lg = bs.login()
    if lg.error_code != '0':
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)
        return None
    return lg

def baostock_logout(lg):
    """baostock logout"""
    if lg is not None:
        bs.logout()
    return None

def get_pettm_mean(pettm_df):
    """
    从peTTM数据中计算peTTM的均值
    
    Args:
        pettm_df (pandas.DataFrame): 包含 date 和 peTTM 列的 DataFrame，通常来自 get_history_pettm_data 函数
    
    Returns:
        float: peTTM的均值，如果数据为空或没有有效数据则返回 None
    """
    if pettm_df is None or pettm_df.empty:
        print('输入的peTTM数据为空')
        return None
    
    # 复制数据，避免修改原始数据
    df = pettm_df.copy()
    
    # 确保peTTM列存在
    if 'peTTM' not in df.columns:
        print('数据中不存在 peTTM 列')
        return None
    
    # 将peTTM转换为数值类型
    df['peTTM'] = pd.to_numeric(df['peTTM'], errors='coerce')
    
    # 过滤掉peTTM为空或NaN的数据
    valid_pettm = df['peTTM'].dropna()
    
    if valid_pettm.empty:
        print('没有有效的peTTM数据')
        return None
    
    # 计算均值
    mean_value = valid_pettm.mean()
    
    # 转换为Python原生float类型
    mean_value = float(mean_value)
    
    # print(f'peTTM均值: {mean_value:.4f} (基于 {len(valid_pettm)} 条有效数据)')
    return mean_value

def get_history_pettm_data(stock_code, end_date, period="10Y"):
    """
    获取股票代码的历史 peTTM 数据
    
    Args:
        stock_code (str): 股票代码，格式如 "sh.601888" 或 "sz.000001"
        end_date (str): 结束日期，格式为 YYYY-MM-DD
        period: 时间跨度，可以是字符串如 "10Y" 或整数如 10，默认 "10Y"（10年）
    
    Returns:
        pandas.DataFrame: 包含 date 和 peTTM 列的 DataFrame，如果出错返回 None
    """
    period_years = parse_period(period)
    last_trading_date = get_last_trading_date_before(end_date)
    if last_trading_date is None:
        df_wide = _load_pettm_from_local(stock_code, "2000-01-01", end_date)
        if df_wide is not None and not df_wide.empty:
            before = df_wide[df_wide["date"] <= end_date]["date"]
            last_trading_date = str(before.max()) if not before.empty else None
    if last_trading_date is None:
        return None
    last_date = datetime.strptime(last_trading_date, '%Y-%m-%d')
    start_year = last_date.year - period_years
    try:
        start_date = datetime(start_year, last_date.month, last_date.day).strftime('%Y-%m-%d')
    except ValueError:
        start_date = datetime(start_year, last_date.month, 28).strftime('%Y-%m-%d')
    result = _load_pettm_from_local(stock_code, start_date, last_trading_date)
    if result is None:
        return None
    return result[["date", "peTTM"]] if "pbMRQ" in result.columns else result

def get_current_pettm_and_mean(stock_code, period="10Y", end_date=None):
    """
    获取股票指定日期最近交易日的peTTM和均值peTTM
    
    该函数会：
    1. 调用 get_history_pettm_data 函数，end_date 传指定日期（如果未指定则使用今天）
    2. 计算peTTM的均值
    3. 返回距指定日期最近一个交易日的peTTM和均值peTTM
    
    Args:
        stock_code (str): 股票代码，格式如 "sh.601888" 或 "sz.000001"
        period: 时间跨度，可以是字符串如 "10Y" 或整数如 10，默认 "10Y"（10年）
        end_date (str): 结束日期，格式为 YYYY-MM-DD，如果为 None 则默认使用今天
    
    Returns:
        dict: 包含以下键的字典：
            - last_trading_date (str): 最近一个交易日的日期，格式为 YYYY-MM-DD
            - last_pettm (float): 最近一个交易日的peTTM
            - mean_pettm (float): peTTM的均值
            如果获取失败，返回 None
    """
    # 如果未指定结束日期，则使用今天
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # 静默模式，不打印详细信息
    # print(f'正在获取股票 {stock_code} 的peTTM和均值...')
    # print(f'结束日期: {end_date}')
    # print(f'周期: {period}\n')
    
    # 调用 get_history_pettm_data 获取历史数据
    # 静默模式：临时关闭标准输出
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        result = get_history_pettm_data(stock_code, end_date, period)
    finally:
        sys.stdout = old_stdout
    
    if result is None or result.empty:
        return None
    
    # 确保按日期排序
    result = result.sort_values('date').reset_index(drop=True)
    
    # 获取最后一条数据（最近一个交易日的peTTM）
    last_record = result.iloc[-1]
    last_trading_date = last_record['date']
    last_pettm = last_record['peTTM']
    
    # 将peTTM转换为数值类型
    if pd.isna(last_pettm):
        # print(f'{stock_code}: 最近一个交易日的peTTM数据为空')
        return None
    
    # 转换为Python原生float类型
    last_pettm = float(last_pettm)
    
    # 计算均值（静默模式）
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    try:
        mean_pettm = get_pettm_mean(result)
    finally:
        sys.stdout = old_stdout
    
    if mean_pettm is None:
        print(f'{stock_code}: 计算peTTM均值失败')
        return None
    
    # get_pettm_mean 已经返回Python原生float类型，无需再次转换
    
    # 返回结果
    result_dict = {
        'last_trading_date': str(last_trading_date),  # 确保日期是字符串
        'last_pettm': last_pettm,
        'mean_pettm': mean_pettm
    }
    
    return result_dict

def _get_last_trading_date_from_local(end_date: str) -> Optional[str]:
    """从 company_stock_info/trading_dates.csv 读取最近交易日。"""
    td_path = COMPANY_STOCK_INFO_DIR / "trading_dates.csv"
    if not td_path.exists():
        return None
    try:
        df = pd.read_csv(td_path, encoding="utf-8-sig")
        if "date" not in df.columns or df.empty:
            return None
        dates = df[df["date"] <= end_date]["date"].astype(str).tolist()
        return max(dates) if dates else None
    except Exception:
        return None


def get_last_trading_date_before(end_date):
    """
    获取指定日期之前最近的上一个交易日。仅从本地 trading_dates.csv 读取。
    """
    try:
        datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        return None
    return _get_last_trading_date_from_local(end_date)

def get_trading_date(start_date='2018-01-01', end_date=None):
    """
    获取指定日期范围内的交易日。仅从本地 trading_dates.csv 读取。
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    td_path = COMPANY_STOCK_INFO_DIR / "trading_dates.csv"
    if not td_path.exists():
        return None
    try:
        df = pd.read_csv(td_path, encoding="utf-8-sig")
        mask = (df["date"] >= start_date) & (df["date"] <= end_date)
        return df.loc[mask, "date"].astype(str).tolist()
    except Exception:
        return None


def _load_pettm_from_local(stock_code: str, start_date: str, end_date: str):
    """从 company_stock_info 本地 CSV 加载 PE/PB 数据，若无则返回 None。"""
    simple = str(stock_code).strip().split(".")[-1] if "." in str(stock_code) else str(stock_code).strip()
    if len(simple) != 6 or not simple.isdigit():
        return None
    csv_path = COMPANY_STOCK_INFO_DIR / f"{simple}.csv"
    if not csv_path.exists():
        return None
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "date" not in df.columns or "peTTM" not in df.columns or "pbMRQ" not in df.columns:
            return None
        df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
        if df.empty:
            return None
        df["peTTM"] = pd.to_numeric(df["peTTM"], errors="coerce")
        df["pbMRQ"] = pd.to_numeric(df["pbMRQ"], errors="coerce")
        df = df.sort_values("date").reset_index(drop=True)
        return df[["date", "peTTM", "pbMRQ"]]
    except Exception:
        return None


def get_history_pettm_data(stock_code, start_date='2000-01-01', end_date=None):
    """
    获取 stock 的历史 peTTM 数据。优先从 data/company_stock_info 本地 CSV 读取。
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    return _load_pettm_from_local(stock_code, start_date, end_date)


def get_pe_info(stock_code, target_date=None, period=["10Y", "5Y"]):
    """
    获取指定交易日的peTTM和以改天为节点往前推N年的平均peTTM。仅使用本地数据。
    """
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # 先拉取数据，用较宽 end_date 以覆盖 target_date
    end_wide = (datetime.strptime(target_date, '%Y-%m-%d') + timedelta(days=30)).strftime('%Y-%m-%d')
    pettm_df = get_history_pettm_data(stock_code, start_date='2010-01-01', end_date=end_wide)
    if pettm_df is None or pettm_df.empty:
        return None

    # 从本地数据推导最近交易日
    before = pettm_df[pettm_df['date'] <= target_date]['date']
    last_trading_date = before.max() if not before.empty else None
    if last_trading_date is None:
        return None
    last_trading_date = str(last_trading_date)

    # 获取目标日期的peTTM、pbMRQ
    target_date_mask = pettm_df['date'] == last_trading_date
    if not target_date_mask.any():
        print(f'目标日期 {last_trading_date} 不在数据中')
        return None
    
    pettm_at_date = pettm_df.loc[target_date_mask, 'peTTM'].values[0]
    pbmrq_at_date = pettm_df.loc[target_date_mask, 'pbMRQ'].values[0]
    
    # 确保日期列是datetime类型
    pettm_df['date'] = pd.to_datetime(pettm_df['date'])
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
    # 当前最近交易日（用于后续剔除“当前值”本身）
    current_trade_date_dt = datetime.strptime(last_trading_date, '%Y-%m-%d')
    
    # 计算最近10年的开始日期（更准确的年份计算）
    start_year_10y = target_date_obj.year - 10
    try:
        start_date_10y = datetime(start_year_10y, target_date_obj.month, target_date_obj.day)
    except ValueError:
        # 处理闰年2月29日的情况
        start_date_10y = datetime(start_year_10y, target_date_obj.month, 28)
    
    # 筛选最近10年的数据
    pettm_10y = pettm_df[
        (pettm_df['date'] >= start_date_10y) & 
        (pettm_df['date'] <= target_date_obj)
    ].copy()

    # pettm_10y.to_csv(f"{stock_code.replace('.', '_')}_pettm_10y.csv", index=False, encoding="utf-8-sig")
    # print(f"已将最近10年peTTM数据保存至 {stock_code.replace('.', '_')}_pettm_10y.csv")
    
    # 过滤掉NaN值并计算均值
    valid_pettm_10y = pettm_10y['peTTM'].dropna()
    if valid_pettm_10y.empty:
        mean_pettm_10y = None
        print(f'最近10年没有有效的peTTM数据')
    else:
        mean_pettm_10y = float(valid_pettm_10y.mean())
        # print(f'最近10年平均peTTM: {mean_pettm_10y:.4f} (基于 {len(valid_pettm_10y)} 条有效数据)')

    # 计算最近10年（不含当前最近交易日）的历史最低 peTTM
    min_pettm_10y_excl_current = None
    hist_10y_excl = pettm_10y[pettm_10y['date'] < current_trade_date_dt].copy()
    valid_hist_10y_excl = hist_10y_excl['peTTM'].dropna()
    if not valid_hist_10y_excl.empty:
        min_pettm_10y_excl_current = float(valid_hist_10y_excl.min())
    
    # 计算最近5年的开始日期（更准确的年份计算）
    start_year_5y = target_date_obj.year - 5
    try:
        start_date_5y = datetime(start_year_5y, target_date_obj.month, target_date_obj.day)
    except ValueError:
        # 处理闰年2月29日的情况
        start_date_5y = datetime(start_year_5y, target_date_obj.month, 28)
    
    # 筛选最近5年的数据
    pettm_5y = pettm_df[
        (pettm_df['date'] >= start_date_5y) & 
        (pettm_df['date'] <= target_date_obj)
    ].copy()
    
    # 过滤掉NaN值并计算均值
    valid_pettm_5y = pettm_5y['peTTM'].dropna()
    if valid_pettm_5y.empty:
        mean_pettm_5y = None
        print(f'最近5年没有有效的peTTM数据')
    else:
        mean_pettm_5y = float(valid_pettm_5y.mean())
        # print(f'最近5年平均peTTM: {mean_pettm_5y:.4f} (基于 {len(valid_pettm_5y)} 条有效数据)')

    # 计算最近5年（不含当前最近交易日）的历史最低 peTTM
    min_pettm_5y_excl_current = None
    hist_5y_excl = pettm_5y[pettm_5y['date'] < current_trade_date_dt].copy()
    valid_hist_5y_excl = hist_5y_excl['peTTM'].dropna()
    if not valid_hist_5y_excl.empty:
        min_pettm_5y_excl_current = float(valid_hist_5y_excl.min())
    
    # pbMRQ统计：均值
    valid_pbmrq_10y = pettm_10y['pbMRQ'].dropna()
    if valid_pbmrq_10y.empty:
        mean_pbmrq_10y = None
        print(f'最近10年没有有效的pbMRQ数据')
    else:
        mean_pbmrq_10y = float(valid_pbmrq_10y.mean())
        # print(f'最近10年平均pbMRQ: {mean_pbmrq_10y:.4f} (基于 {len(valid_pbmrq_10y)} 条有效数据)')

    valid_pbmrq_5y = pettm_5y['pbMRQ'].dropna()
    if valid_pbmrq_5y.empty:
        mean_pbmrq_5y = None
        print(f'最近5年没有有效的pbMRQ数据')
    else:
        mean_pbmrq_5y = float(valid_pbmrq_5y.mean())
        # print(f'最近5年平均pbMRQ: {mean_pbmrq_5y:.4f} (基于 {len(valid_pbmrq_5y)} 条有效数据)')

    # pbMRQ统计：最近10年 / 5年（不含当前最近交易日）的历史最低 PB
    min_pbmrq_10y_excl_current = None
    hist_pb_10y_excl = pettm_10y[pettm_10y['date'] < current_trade_date_dt].copy()
    valid_hist_pb_10y_excl = hist_pb_10y_excl['pbMRQ'].dropna()
    if not valid_hist_pb_10y_excl.empty:
        min_pbmrq_10y_excl_current = float(valid_hist_pb_10y_excl.min())

    min_pbmrq_5y_excl_current = None
    hist_pb_5y_excl = pettm_5y[pettm_5y['date'] < current_trade_date_dt].copy()
    valid_hist_pb_5y_excl = hist_pb_5y_excl['pbMRQ'].dropna()
    if not valid_hist_pb_5y_excl.empty:
        min_pbmrq_5y_excl_current = float(valid_hist_pb_5y_excl.min())

    return {
        'stock_code': stock_code,
        'target_date': target_date,
        'pettm_at_date': float(pettm_at_date),
        'mean_pettm_10y': mean_pettm_10y,
        'mean_pettm_5y': mean_pettm_5y,
        'min_pettm_10y_excl_current': min_pettm_10y_excl_current,
        'min_pettm_5y_excl_current': min_pettm_5y_excl_current,
        'pbmrq_at_date': float(pbmrq_at_date),
        'mean_pbmrq_10y': mean_pbmrq_10y,
        'mean_pbmrq_5y': mean_pbmrq_5y,
        'min_pbmrq_10y_excl_current': min_pbmrq_10y_excl_current,
        'min_pbmrq_5y_excl_current': min_pbmrq_5y_excl_current
    }


def _load_predict_from_local(stock_code: str):
    """从 meta.csv 读取 mean_e_growth_rate 等预测数据，若无则返回 None。"""
    simple = str(stock_code).strip().split(".")[-1] if "." in str(stock_code) else str(stock_code).strip()
    if len(simple) != 6:
        return None
    meta_path = COMPANY_STOCK_INFO_DIR / "meta.csv"
    if not meta_path.exists():
        return None
    try:
        meta = pd.read_csv(meta_path, encoding="utf-8-sig")
        meta["_simple"] = meta["code"].astype(str).str.split(".").str[-1]
        match = meta[meta["_simple"] == simple]
        if match.empty:
            return None
        row = match.iloc[0]
        g = row.get("mean_e_growth_rate", None)
        if g is None or (isinstance(g, float) and pd.isna(g)):
            g = None
        else:
            try:
                g = float(g)
            except (ValueError, TypeError):
                g = None
        return {
            "stock_code": simple,
            "stock_name": str(row.get("code_name", "")),
            "mean_e_growth_rate": g,
            "report_infos": str(row.get("report_infos", "")),
            "industry": str(row.get("industry", "")),
        }
    except Exception:
        return None


def _parse_forecasts_json(raw) -> Optional[dict]:
    """解析 extracted_forecasts JSON，返回 {year: {pe, eps}, ...}"""
    if pd.isna(raw):
        return None
    s = str(raw).strip()
    if not s or s.startswith("[ERROR]"):
        return None
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
        pe, eps = item.get("pe"), item.get("eps")
        if pe is not None and not isinstance(pe, (int, float)):
            pe = None
        if eps is not None and not isinstance(eps, (int, float)):
            eps = None
        result[year] = {"pe": pe, "eps": eps}
    return result if result else None


def _compute_g_from_forecasts(forecasts: dict) -> Optional[float]:
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
    elif pe1 and pe2 and pe1 > 0 and pe2 > 0:
        ratio = pe1 / pe2
    else:
        return None
    try:
        g = (ratio ** (1.0 / n)) - 1
        return float(g) if not isinstance(g, complex) else float(g.real)
    except (ValueError, TypeError):
        return None


def _load_predict_from_company_research(
    stock_code: str, as_of_date: str, lookback_days: int = 90
) -> Optional[dict]:
    """
    从 company_research 研报 CSV 中，筛选 publish_time <= as_of_date 且在 lookback_days 内的报告，
    解析 extracted_forecasts 计算 mean_e_growth_rate。
    优先使用最近 90 天内的研报，若无则放宽到 360 天。
    """
    simple = str(stock_code).strip().split(".")[-1] if "." in str(stock_code) else str(stock_code).strip()
    if len(simple) != 6:
        return None
    code_6 = simple.zfill(6)
    if not COMPANY_RESEARCH_DIR.exists():
        return None
    candidates = list(COMPANY_RESEARCH_DIR.glob(f"reports_{code_6}_*.csv"))
    if not candidates:
        candidates = [f for f in COMPANY_RESEARCH_DIR.glob("reports_*.csv") if len(f.stem.split("_")) >= 2 and f.stem.split("_")[1] == code_6]
    if not candidates:
        return None
    csv_path = candidates[0]
    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        return None
    if "extracted_forecasts" not in df.columns or "publish_time" not in df.columns:
        return None
    as_of_ts = pd.to_datetime(as_of_date)
    start_ts = as_of_ts - pd.Timedelta(days=lookback_days)
    wider_start = as_of_ts - pd.Timedelta(days=360)  # 90天内无有效研报时放宽到360天
    df["_pt"] = pd.to_datetime(df["publish_time"], errors="coerce")
    mask_90 = (df["_pt"] <= as_of_ts) & (df["_pt"] >= start_ts)
    mask = mask_90
    if not mask.any():
        mask = (df["_pt"] <= as_of_ts) & (df["_pt"] >= wider_start)
    window_used = "90天" if mask_90.any() else "360天"
    df = df.loc[mask].copy()
    if df.empty:
        return None
    growth_rates = []
    report_details = []
    stock_name = ""
    for _, row in df.iterrows():
        f = _parse_forecasts_json(row.get("extracted_forecasts"))
        g = _compute_g_from_forecasts(f) if f else None
        if g is not None:
            growth_rates.append(g)
        # 记录每篇研报详情（含解析失败或仅1年无法算g的）
        f_str = ", ".join([f"{y}: eps={v.get('eps')} pe={v.get('pe')}" for y, v in sorted(f.items())]) if f else "解析失败"
        report_details.append({
            "publish_time": str(row.get("publish_time", "")),
            "org_name": str(row.get("org_name", "")),
            "title": (str(row.get("title", "")) or "")[:80],
            "forecasts": f_str,
            "g": round(g, 4) if g is not None else None,
        })
        if not stock_name and pd.notna(row.get("stock_name")):
            stock_name = str(row["stock_name"])
    if not growth_rates:
        return None
    mean_g = sum(growth_rates) / len(growth_rates)
    industry = ""
    meta_path = COMPANY_STOCK_INFO_DIR / "meta.csv"
    if meta_path.exists():
        try:
            meta = pd.read_csv(meta_path, encoding="utf-8-sig")
            meta["_simple"] = meta["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6).str[-6:]
            match = meta[meta["_simple"] == code_6]
            if not match.empty:
                industry = str(match["industry"].iloc[0])
        except Exception:
            pass
    return {
        "stock_code": simple,
        "stock_name": stock_name or "",
        "mean_e_growth_rate": mean_g,
        "report_infos": f"company_research {len(growth_rates)} reports ({window_used})",
        "industry": industry,
        "report_details": report_details,
        "g_window": window_used,
    }


def get_recent_predict_peTTM(stock_code, as_of_date=None, lookback_days=90, from_api=False):
    """
    获取研报预测的盈利增长率 g。
    - from_api=True: 从 akshare 拉取
    - as_of_date 有值: 从 company_research 按日期筛选研报计算 g（支持回测）
    - 否则: 从 meta.csv 读取
    """
    if not from_api:
        date_to_use = as_of_date or datetime.now().strftime("%Y-%m-%d")
        res = _load_predict_from_company_research(stock_code, date_to_use, lookback_days)
        if res is not None:
            return res
        if not as_of_date:
            local = _load_predict_from_local(stock_code)
            if local is not None:
                return local
        return None
    report_df = ak.stock_research_report_em(symbol=stock_code)
    
    # 检查接口返回的所有列，查找年份相关的列
    all_columns = report_df.columns.tolist()
    
    # 查找年份相关的列（2025-2029）
    year_columns = {}
    for col in all_columns:
        for year in ['2025', '2026', '2027', '2028', '2029']:
            if year in str(col) and '盈利预测' in str(col) and '市盈率' in str(col):
                year_columns[year] = col
                break
    
    # 确定要使用的列
    base_columns = ['股票代码', '股票简称', '机构', '报告PDF链接', '日期']
    selected_year_columns = []
    column_mapping = {}
    
    # 优先使用2028年数据（如果存在），否则使用2027年
    if '2028' in year_columns:
        # 使用2026和2028年计算增长率（3年）
        selected_year_columns = ['2025', '2026', '2027', '2028']
        column_mapping = {
            year_columns['2025']: 'predict_peTTM_2025',
            year_columns['2026']: 'predict_peTTM_2026',
            year_columns['2027']: 'predict_peTTM_2027',
            year_columns['2028']: 'predict_peTTM_2028'
        }
        use_2028 = True
        start_year = '2026'
        end_year = '2028'
    elif '2027' in year_columns:
        # 使用2025和2027年计算增长率（2年，开平方根得到年化）
        selected_year_columns = ['2025', '2026', '2027']
        column_mapping = {
            year_columns['2025']: 'predict_peTTM_2025',
            year_columns['2026']: 'predict_peTTM_2026',
            year_columns['2027']: 'predict_peTTM_2027'
        }
        use_2028 = False
        start_year = '2025'
        end_year = '2027'
    else:
        # 如果没有找到预期的年份列，返回None
        print(f"警告: 股票 {stock_code} 未找到2025-2027年的预测市盈率数据")
        return None
    
    # 选择需要的列
    needed_columns = base_columns + [year_columns[y] for y in selected_year_columns if y in year_columns]
    report_df = report_df[needed_columns].copy()
    
    # 重命名列
    rename_dict = {}
    for old_col, new_col in column_mapping.items():
        rename_dict[old_col] = new_col
    rename_dict.update({
        '股票代码': 'stock_code',
        '股票简称': 'stock_name',
        '机构': 'institution',
        '报告PDF链接': 'report_pdf_link',
        '日期': 'date'
    })
    report_df = report_df.rename(columns=rename_dict)

    # 确保日期列为 datetime 类型，便于按日期过滤
    report_df['date'] = pd.to_datetime(report_df['date'])
    report_df = report_df.sort_values(by='date', ascending=False).reset_index(drop=True)

    # ---- 按日期过滤研报（支持回测） ----
    if as_of_date is not None:
        # 回测模式：只使用 as_of_date 之前、且在 lookback_days 天窗口内的研报
        as_of_ts = pd.to_datetime(as_of_date)
        start_ts = as_of_ts - pd.Timedelta(days=lookback_days)
        mask = (report_df['date'] <= as_of_ts) & (report_df['date'] >= start_ts)
        filtered_df = report_df.loc[mask].copy()

        # 如果窗口内没有研报，尝试放宽窗口到 360 天；仍然没有则返回 None
        if filtered_df.empty:
            wider_start_ts = as_of_ts - pd.Timedelta(days=360)
            mask = (report_df['date'] <= as_of_ts) & (report_df['date'] >= wider_start_ts)
            filtered_df = report_df.loc[mask].copy()

        if filtered_df.empty:
            print(f"警告: 股票 {stock_code} 在 {as_of_date} 之前 {lookback_days}~360 天内无研报预测数据")
            return None

        report_df = filtered_df.reset_index(drop=True)
    else:
        # 实时模式：使用最近一段时间（约 30 天内的一组研报）
        dates_lst = report_df['date'].to_list()
        idx = -1
        for i in range(len(dates_lst)):
            if i == 0:
                continue
            if (dates_lst[i - 1] - dates_lst[i]).days > 30:
                idx = i
                break

        # 如果找到分界点，则只保留最近的一组研报；否则保留全部
        if idx != -1:
            report_df = report_df.iloc[:idx].reset_index(drop=True)
    
    # 根据使用的年份确定需要检查的列
    if use_2028:
        # 使用2025和2028年计算（3年）
        required_cols = ['predict_peTTM_2025', 'predict_peTTM_2028']
        report_df = report_df.dropna(subset=required_cols).reset_index(drop=True)
        
        def calculate_e_growth_rate(row):
            """计算每股净利润增长率（3年期：2025到2028）"""
            pe_start = row['predict_peTTM_2025']
            pe_end = row['predict_peTTM_2028']
            
            if isinstance(pe_start, complex) or isinstance(pe_end, complex):
                return float('nan')
            
            try:
                pe_start_val = float(pe_start)
                pe_end_val = float(pe_end)
            except (ValueError, TypeError):
                return float('nan')
            
            if pe_start_val > 0 and pe_end_val > 0:
                ratio = pe_start_val / pe_end_val
                # 3年期，开立方根
                result = (ratio ** (1.0/3)) - 1
                if isinstance(result, complex):
                    return result.real
                return float(result)
            else:
                return float('nan')
    else:
        # 使用2025和2027年计算（2年，开平方根得到年化）
        required_cols = ['predict_peTTM_2025', 'predict_peTTM_2026', 'predict_peTTM_2027']
        report_df = report_df.dropna(subset=required_cols).reset_index(drop=True)
        
        def calculate_e_growth_rate(row):
            """计算每股净利润增长率（2年期：2025到2027，开平方根得到年化）"""
            pe2025 = row['predict_peTTM_2025']
            pe2027 = row['predict_peTTM_2027']
            
            if isinstance(pe2025, complex) or isinstance(pe2027, complex):
                return float('nan')
            
            try:
                pe2025_val = float(pe2025)
                pe2027_val = float(pe2027)
            except (ValueError, TypeError):
                return float('nan')
            
            if pe2025_val > 0 and pe2027_val > 0:
                ratio = pe2025_val / pe2027_val
                # 2年期，开平方根得到年化增长率
                result = (ratio ** 0.5) - 1
                if isinstance(result, complex):
                    return result.real
                return float(result)
            else:
                return float('nan')
    
    report_df['e_growth_rate'] = report_df.apply(calculate_e_growth_rate, axis=1)
    
    # 计算均值，跳过 NaN 值，并确保结果是实数
    valid_growth_rates = report_df['e_growth_rate'].dropna()
    if valid_growth_rates.empty:
        mean_e_growth_rate = float('nan')
    else:
        mean_e_growth_rate = valid_growth_rates.mean()
        # 确保是实数类型
        if isinstance(mean_e_growth_rate, complex):
            mean_e_growth_rate = mean_e_growth_rate.real
        mean_e_growth_rate = float(mean_e_growth_rate)

    # 每条报告的关键信息按照 \001 拼接，每条之间用 \n 拼接
    info_columns = ['institution', 'date', 'e_growth_rate', 'report_pdf_link']
    # 添加所有可用的年份列
    for year in ['2025', '2026', '2027', '2028']:
        col_name = f'predict_peTTM_{year}'
        if col_name in report_df.columns:
            info_columns.insert(-1, col_name)  # 插入到e_growth_rate之前
    def row_to_str(row):
        return '  '.join([
            str(row.get(col, "")) if row.get(col, "") is not None else "" for col in info_columns
        ])
    report_infos_str = '\n'.join([row_to_str(row) for _, row in report_df.iterrows()])

    # 获取股票所属行业信息：申万一级行业。优先从 meta.csv 读取，from_api 时若无则从申万接口构建缓存获取
    industry = ""
    simple = str(stock_code).strip().split(".")[-1] if "." in str(stock_code) else str(stock_code).strip()
    if len(simple) == 6:
        industry = get_sw_industry(simple)

    # 将最终结果保存到字典中
    res = dict()
    res['stock_code'] = stock_code
    res['stock_name'] = report_df['stock_name'].iloc[0]
    res['mean_e_growth_rate'] = mean_e_growth_rate
    res['report_infos'] = report_infos_str
    res['industry'] = industry
    return res


def get_merge_info(pe_info_dict, predict_g_dict):
    """
    合并两个字典
    """
    if not pe_info_dict or not predict_g_dict:
        return None

    # 保留 pe_info_dict 中带交易所前缀的 stock_code（如 sh.601888），
    # 仅合并预测数据中的其它字段，避免覆盖前缀
    merged = dict(pe_info_dict)
    predict_part = dict(predict_g_dict)
    predict_part.pop('stock_code', None)

    merged.update(predict_part)
    return merged


def add_stock_prefix(stock_code: str) -> str:
    """
    给6位数字的股票编码加地区前缀，上证用sh，深证用sz，创业板用sz，科创板用sh。
    规则参考A股一般约定：
        - 以60打头的为上证证券交易所（sh）
        - 以00打头的为深证证券交易所（sz）
        - 以30打头的为深证创业板（sz）
        - 以68打头的为上证科创板（sh）
    """
    stock_code = stock_code.strip()
    if not (stock_code.isdigit() and len(stock_code) == 6):
        raise ValueError(f"股票代码格式不正确: {stock_code}")

    if stock_code.startswith("60") or stock_code.startswith("68"):
        prefix = "sh"
    elif stock_code.startswith("00") or stock_code.startswith("30"):
        prefix = "sz"
    else:
        # 这里默认按sz
        prefix = "sz"
    return f"{prefix}.{stock_code}"



def get_stock_info(stock_code, target_date=None):
    if not stock_code:
        return None
    
    if len(stock_code) != 6:
        sim_stock_code = stock_code[-6:]
    else:
        sim_stock_code = stock_code
        stock_code = add_stock_prefix(stock_code) 
    pe_res = get_pe_info(stock_code, target_date)
    predict_g_res = get_recent_predict_peTTM(sim_stock_code, as_of_date=target_date)
    return get_merge_info(pe_res, predict_g_res)
    

def main():
    stock_code = 'sh.601888'
    get_stock_info(stock_code)

def test():
    res = get_recent_predict_peTTM("601888")
    print(res)

def _test():
    # 1. 登录系统 + 强制校验登录结果（核心修复1）
    lg = bs.login()
    if lg.error_code != '0':
        print(f"❌ 登录失败：{lg.error_msg}")
        return
    print(f"✅ 登录成功：{lg.error_msg}")

    etf_code = 'sh.510300'  # 华泰柏瑞沪深300ETF
    # 2. 查询ETF日线数据
    rs = bs.query_history_k_data_plus(
        etf_code,
        "date,open,high,low,close,volume,amount,turn",
        start_date='2024-01-01',
        end_date='2025-01-08',
        frequency="d",
        adjustflag="3"  # ETF专用：不复权
    )

    # 3. 校验查询请求是否成功
    if rs.error_code != '0':
        print(f"❌ 数据查询失败：{rs.error_msg}")
        bs.logout()
        return

    # 4. 读取数据（核心修复2：循环逻辑更正）
    data_list = []
    while rs.next():  # ✅ 正确写法：先判断是否有下一条，再读取
        data_list.append(rs.get_row_data())

    # 5. 空数据校验
    if not data_list:
        print(f"❌ 未查询到 {etf_code} 的数据，请检查时间范围/代码")
        bs.logout()
        return

    # 6. 构建DataFrame + 字段命名优化（核心修复3）
    columns = ["日期", "开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额", "换手率"]
    df = pd.DataFrame(data_list, columns=columns)
    
    # 7. 数据类型转换（字符串→数值，必备！否则无法做计算）
    num_cols = ["开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额", "换手率"]
    df[num_cols] = df[num_cols].astype(float)

    # 8. 打印结果
    print(f"\n✅ 成功获取 {etf_code} 数据，共【{len(df)}】条")
    print(df.head(10))  # 打印前10条

    # 9. 登出系统
    bs.logout()
    print("\n✅ 已登出Baostock系统")
    return df

def _test2():
    import akshare as ak

    # 1. 获取510300的ETF完整数据
    etf_df = ak.fund_etf_hist_sina(symbol="sh510300")  # 这里symbol写"510300"也可以（无需加sh.）
    print(f"原始数据共【{len(etf_df)}】条")

    # 2. （可选但推荐）将date列转为datetime类型，方便精准筛选
    etf_df['date'] = pd.to_datetime(etf_df['date'])

    # 3. 定义要筛选的时间范围（示例：筛选2025年12月的所有数据）
    start_filter_date = "2025-12-31"  # 起始日期
    end_filter_date = "2025-12-31"    # 结束日期

    # 4. 执行时间筛选
    etf_df_filtered = etf_df[
        (etf_df['date'] >= start_filter_date) & 
        (etf_df['date'] <= end_filter_date)
    ]

    # 5. 打印筛选结果
    print(f"\n筛选后（{start_filter_date}至{end_filter_date}）的数据共【{len(etf_df_filtered)}】条")
    print(etf_df_filtered)  # 若数据多，可改用 etf_df_filtered.tail(10) 看末尾10条

if __name__ == "__main__":
    # main()
    # test()
    # _test()
    _test2()
    
