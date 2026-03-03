from pathlib import Path
from datetime import datetime
from os import path as osp  
import pandas as pd
import os

from stock_analysis import get_stock_info

# STOCK_FILE_NAME = "sz50_stocks.csv"
# STOCK_FILE_NAME = "self_selected_stocks.csv" 
# STOCK_FILE_NAME = "hs300_stocks.csv" 
STOCK_FILE_NAME = "zz500_stocks.csv" 
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = osp.join(ROOT_DIR, "data")
STOCK_LIST_FILE = osp.join(DATA_DIR, STOCK_FILE_NAME)
OUTPUT_DIR = osp.join(DATA_DIR, "stock_analysis_results")
# 基于输入文件名生成输出文件名（去掉.csv后缀）
OUTPUT_FILE_NAME = STOCK_FILE_NAME.replace(".csv", "")


def load_all_codes(csv_path: str):
    """读取 csv 中的 stock code 列表（PE/PB 等估值分析用）"""
    if not osp.exists(csv_path):
        raise FileNotFoundError(f"找不到文件: {csv_path}")

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="gbk")

    if "code" not in df.columns:
        raise ValueError(f"文件缺少code列: {csv_path}")

    if "code_name" not in df.columns:
        df["code_name"] = ""

    df["code"] = df["code"].astype(str).str.strip()
    df["code_name"] = df["code_name"].astype(str).str.strip()

    records = (
        df[["code", "code_name"]]
        .dropna(subset=["code"])
        .drop_duplicates(subset="code", keep="first")
    )
    return records.to_dict("records")

def _calc_revenue(row, mean_pe_col):
    pe_now = row['pettm_at_date']
    pe_mean = row[mean_pe_col]
    growth = row['mean_e_growth_rate']
    
    # 1. 数据清洗：确保是数字
    try:
        pe_now = float(pe_now)
        pe_mean = float(pe_mean)
    except (ValueError, TypeError):
        return None

    # 2. 核心逻辑：处理负值和零
    # 如果最新 PE 或历史均值 PE 为负，均值回归模型失效
    if pe_now <= 0 or pe_mean <= 0:
        return -10
        
    # 3. 计算比率
    ratio = pe_mean / pe_now
    
    # 4. 计算收益率 (避免负数开根号)
    try:
        val = (ratio ** 0.5) * (1 + growth) - 1
        return val
    except Exception:
        return -10

def _calc_pb_return(row, mean_pb_col):
    """
    计算当前 PB 如果回归到均值 PB 时的涨幅
    
    Args:
        row: DataFrame的一行数据
        mean_pb_col: 均值 PB 列名，如 'mean_pbmrq_5y' 或 'mean_pbmrq_10y'
    
    Returns:
        float: PB 涨幅（小数形式，如 0.1 表示 10%），如果计算失败返回 -10
    """
    pb_now = row['pbmrq_at_date']
    pb_mean = row[mean_pb_col]
    
    # 1. 数据清洗：确保是数字
    try:
        pb_now = float(pb_now)
        pb_mean = float(pb_mean)
    except (ValueError, TypeError):
        return -10

    # 2. 核心逻辑：处理负值和零
    # 如果当前 PB 或均值 PB 为负或零，均值回归模型失效
    if pb_now <= 0 or pb_mean <= 0:
        return -10
        
    # 3. 计算涨幅：如果当前 PB 回归到均值 PB，涨幅 = (均值 PB / 当前 PB) - 1
    try:
        return_ratio = (pb_mean / pb_now) - 1
        return return_ratio
    except Exception:
        return -10

def post_process_results(result_rows):
    """
    对原始数据进行后处理
    """
    res_df = pd.DataFrame(result_rows)
    # PEG = PE / EPS growth rate
    res_df['PEG'] = res_df.apply(
        lambda row: row['pettm_at_date'] / (row['mean_e_growth_rate'] * 100)
        if row['mean_e_growth_rate'] > 0
        else -10,
        axis=1,
    )
    # 保留1位小数，但保持数值类型
    res_df['PEG'] = res_df['PEG'].round(1)
    # 5 年均值回归的收益率（基于 PE 均值回归）
    res_df['predict_revenue_5y'] = res_df.apply(lambda row: _calc_revenue(row, 'mean_pettm_5y'), axis=1)
    print(f"predict_revenue_5y: {res_df['predict_revenue_5y'].to_list()}")
    # 10 年均值回归的收益率（基于 PE 均值回归）
    res_df['predict_revenue_10y'] = res_df.apply(lambda row: _calc_revenue(row, 'mean_pettm_10y'), axis=1)
    # 5 年 PB 均值回归的涨幅
    res_df['predict_pb_return_5y'] = res_df.apply(lambda row: _calc_pb_return(row, 'mean_pbmrq_5y'), axis=1)
    # 10 年 PB 均值回归的涨幅
    res_df['predict_pb_return_10y'] = res_df.apply(lambda row: _calc_pb_return(row, 'mean_pbmrq_10y'), axis=1)
    # EPS growth rate：转为百分数，保留 2 位小数，保持数值类型（如 15.23 表示 15.23%）
    res_df['mean_e_growth_rate'] = (res_df['mean_e_growth_rate'] * 100).round(2)
    # 5 年 / 10 年均值回归的收益率（PE 均值回归）：转为百分数，保留 2 位小数
    res_df['predict_revenue_5y'] = (res_df['predict_revenue_5y'] * 100).round(2)
    res_df['predict_revenue_10y'] = (res_df['predict_revenue_10y'] * 100).round(2)
    # 5 年 / 10 年 PB 均值回归的涨幅：转为百分数，保留 2 位小数
    res_df['predict_pb_return_5y'] = (res_df['predict_pb_return_5y'] * 100).round(2)
    res_df['predict_pb_return_10y'] = (res_df['predict_pb_return_10y'] * 100).round(2)
    # 5 年 / 10 年均值回归的 PE：保留 1 位小数
    res_df['mean_pettm_5y'] = res_df['mean_pettm_5y'].round(1)
    res_df['mean_pettm_10y'] = res_df['mean_pettm_10y'].round(1)
    # 5 年 / 10 年内（不含当前最近交易日）的历史最低 PE：保留 1 位小数
    if 'min_pettm_5y_excl_current' in res_df.columns:
        res_df['min_pettm_5y_excl_current'] = pd.to_numeric(
            res_df['min_pettm_5y_excl_current'], errors='coerce'
        ).round(1)
    if 'min_pettm_10y_excl_current' in res_df.columns:
        res_df['min_pettm_10y_excl_current'] = pd.to_numeric(
            res_df['min_pettm_10y_excl_current'], errors='coerce'
        ).round(1)
    # 5 年 / 10 年内（不含当前最近交易日）的历史最低 PB：保留 2 位小数
    if 'min_pbmrq_5y_excl_current' in res_df.columns:
        res_df['min_pbmrq_5y_excl_current'] = pd.to_numeric(
            res_df['min_pbmrq_5y_excl_current'], errors='coerce'
        ).round(2)
    if 'min_pbmrq_10y_excl_current' in res_df.columns:
        res_df['min_pbmrq_10y_excl_current'] = pd.to_numeric(
            res_df['min_pbmrq_10y_excl_current'], errors='coerce'
        ).round(2)
    # 最新 PE：保留 1 位小数
    res_df['pettm_at_date'] = res_df['pettm_at_date'].round(1)
    # PB：保留 2 位小数
    res_df['pbmrq_at_date'] = res_df['pbmrq_at_date'].round(2)
    res_df['mean_pbmrq_5y'] = res_df['mean_pbmrq_5y'].round(2)
    res_df['mean_pbmrq_10y'] = res_df['mean_pbmrq_10y'].round(2)

    # ---- 5/10 年 PE/PB 谷底 ±15% 区间与是否落在区间 ----
    # 统一确保参与计算的列为数值
    for col in [
        'min_pettm_5y_excl_current', 'min_pettm_10y_excl_current',
        'min_pbmrq_5y_excl_current', 'min_pbmrq_10y_excl_current',
        'pettm_at_date', 'pbmrq_at_date'
    ]:
        if col in res_df.columns:
            res_df[col] = pd.to_numeric(res_df[col], errors='coerce')

    # 5年 PE 谷底 ±15%
    res_df['pe_5y_trough_low_15'] = (res_df['min_pettm_5y_excl_current'] * 0.85).round(1)
    res_df['pe_5y_trough_high_15'] = (res_df['min_pettm_5y_excl_current'] * 1.15).round(1)
    res_df['pe_5y_in_trough_15'] = (
        (res_df['pettm_at_date'] >= res_df['pe_5y_trough_low_15']) &
        (res_df['pettm_at_date'] <= res_df['pe_5y_trough_high_15'])
    ).astype(int)

    # 10年 PE 谷底 ±15%
    res_df['pe_10y_trough_low_15'] = (res_df['min_pettm_10y_excl_current'] * 0.85).round(1)
    res_df['pe_10y_trough_high_15'] = (res_df['min_pettm_10y_excl_current'] * 1.15).round(1)
    res_df['pe_10y_in_trough_15'] = (
        (res_df['pettm_at_date'] >= res_df['pe_10y_trough_low_15']) &
        (res_df['pettm_at_date'] <= res_df['pe_10y_trough_high_15'])
    ).astype(int)

    # 5年 PB 谷底 ±15%
    res_df['pb_5y_trough_low_15'] = (res_df['min_pbmrq_5y_excl_current'] * 0.85).round(2)
    res_df['pb_5y_trough_high_15'] = (res_df['min_pbmrq_5y_excl_current'] * 1.15).round(2)
    res_df['pb_5y_in_trough_15'] = (
        (res_df['pbmrq_at_date'] >= res_df['pb_5y_trough_low_15']) &
        (res_df['pbmrq_at_date'] <= res_df['pb_5y_trough_high_15'])
    ).astype(int)

    # 10年 PB 谷底 ±15%
    res_df['pb_10y_trough_low_15'] = (res_df['min_pbmrq_10y_excl_current'] * 0.85).round(2)
    res_df['pb_10y_trough_high_15'] = (res_df['min_pbmrq_10y_excl_current'] * 1.15).round(2)
    res_df['pb_10y_in_trough_15'] = (
        (res_df['pbmrq_at_date'] >= res_df['pb_10y_trough_low_15']) &
        (res_df['pbmrq_at_date'] <= res_df['pb_10y_trough_high_15'])
    ).astype(int)
    # stock_code格式化
    res_df['stock_code'] = res_df['stock_code'].map(lambda x: str(x))

    cols = [
        "target_date",
        "stock_code", 
        "stock_name",
        "industry",
        "mean_pettm_5y",
        "mean_pettm_10y",
        "min_pettm_5y_excl_current",
        "min_pettm_10y_excl_current",
        "min_pbmrq_5y_excl_current",
        "min_pbmrq_10y_excl_current",
        "pettm_at_date",
        "mean_e_growth_rate",
        "PEG",
        "predict_revenue_5y",
        "predict_revenue_10y",
        "pbmrq_at_date",
        "mean_pbmrq_5y",
        "mean_pbmrq_10y",
        "predict_pb_return_5y",
        "predict_pb_return_10y",
        # 下面这 8 个是 5/10 年 PE/PB 谷底 ±15% 及命中标记，整体挪到报告信息之前
        "pe_5y_trough_low_15",
        "pe_5y_trough_high_15",
        "pe_5y_in_trough_15",
        "pe_10y_trough_low_15",
        "pe_10y_trough_high_15",
        "pe_10y_in_trough_15",
        "pb_5y_trough_low_15",
        "pb_5y_trough_high_15",
        "pb_5y_in_trough_15",
        "pb_10y_trough_low_15",
        "pb_10y_trough_high_15",
        "pb_10y_in_trough_15",
        "report_infos"
    ]

    cols_zh = [
        "日期",                             # target_date
        "股票代码",                         # stock_code
        "股票名称",                         # stock_name
        "所属行业",                         # industry
        "5年均值回归的市盈率",               # mean_pettm_5y
        "10年均值回归的市盈率",              # mean_pettm_10y
        "5年内(不含当前)最低市盈率",          # min_pettm_5y_excl_current
        "10年内(不含当前)最低市盈率",         # min_pettm_10y_excl_current
        "5年内(不含当前)最低市净率",          # min_pbmrq_5y_excl_current
        "10年内(不含当前)最低市净率",         # min_pbmrq_10y_excl_current
        "最新市盈率",                        # pettm_at_date
        "预估每股净利润增长率(%)",            # mean_e_growth_rate
        "PEG",                              # PEG
        "5年均值PE回归的收益率(%)",           # predict_revenue_5y
        "10年均值PE回归的收益率(%)",          # predict_revenue_10y
        "最新市净率",                        # pbmrq_at_date
        "5年均值回归的市净率",                # mean_pbmrq_5y
        "10年均值回归的市净率",               # mean_pbmrq_10y
        "5年均值PB回归的涨幅(%)",             # predict_pb_return_5y
        "10年均值PB回归的涨幅(%)",            # predict_pb_return_10y
        # 8 个谷底 ±15% 相关字段，挪到报告信息之前
        "5年内PE谷底-15%值",                 # pe_5y_trough_low_15
        "5年内PE谷底+15%值",                 # pe_5y_trough_high_15
        "最新PE是否在5年PE谷底±15%内",        # pe_5y_in_trough_15
        "10年内PE谷底-15%值",                # pe_10y_trough_low_15
        "10年内PE谷底+15%值",                # pe_10y_trough_high_15
        "最新PE是否在10年PE谷底±15%内",       # pe_10y_in_trough_15
        "5年内PB谷底-15%值",                 # pb_5y_trough_low_15
        "5年内PB谷底+15%值",                 # pb_5y_trrough_high_15
        "最新PB是否在5年PB谷底±15%内",        # pb_5y_in_trough_15
        "10年内PB谷底-15%值",                # pb_10y_trough_low_15
        "10年内PB谷底+15%值",                # pb_10y_trrough_high_15
        "最新PB是否在10年PB谷底±15%内",       # pb_10y_in_trough_15
        "报告信息"                           # report_infos
    ]

    res_df = res_df[cols]
    res_df.columns = cols_zh

    return res_df 


def save_results(res_df, output_dir: str, file_name: str):
    """将结果保存到 Excel 文件"""
    if res_df is None:
        print("没有可保存的结果")
        return

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = osp.join(output_dir, f"{file_name}_{timestamp}.xlsx")

    # 保存为 Excel，禁用索引
    res_df.to_excel(output_path, index=False)
    print(f"结果已保存至 {output_path} (Excel)")



def process_stocks(csv_path: str = STOCK_LIST_FILE, output_dir: str = OUTPUT_DIR, file_name: str = OUTPUT_FILE_NAME):
    """
    批量处理 stock 列表，调用 get_stock_info 获取信息并保存到 CSV
    
    Args:
        csv_path: stock 列表 CSV 文件路径，默认为 STOCK_LIST_FILE
        output_dir: 输出目录，默认为OUTPUT_DIR
        file_name: 输出文件名前缀，默认为"stock_analysis"
    """
    # 加载stock列表
    print(f"正在加载stock列表: {csv_path}")
    try:
        stock_list = load_all_codes(csv_path)
        print(f"成功加载 {len(stock_list)} ")
    except Exception as e:
        print(f"加载stock列表失败: {e}")
        return
    
    # 处理每只stock
    result_rows = []
    total = len(stock_list)
    
    for idx, stock_record in enumerate(stock_list, 1):
        stock_code = stock_record.get("code", "")
        stock_name = stock_record.get("code_name", "")
        
        print(f"[{idx}/{total}] 正在处理: {stock_code} {stock_name}")
        
        try:
            # 调用get_stock_info获取stock信息
            stock_info = get_stock_info(stock_code)
            
            if stock_info is None:
                print(f"  ⚠️  {stock_code} {stock_name}: 获取信息失败，跳过")
                continue
            
            # 添加stock代码和名称到结果中
            stock_info["code"] = stock_code
            stock_info["code_name"] = stock_name
            
            result_rows.append(stock_info)
            print(f"  ✅ {stock_code} {stock_name}: 处理成功")
            
        except Exception as e:
            print(f"  ❌ {stock_code} {stock_name}: 处理出错 - {e}")
            continue
    
    # 保存结果
    print(f"\n处理完成，成功处理 {len(result_rows)}/{total} 只stock")
    if result_rows:
        processed_df = post_process_results(result_rows)
        save_results(processed_df, output_dir, file_name)
    else:
        print("没有成功处理任何stock，不保存结果")


def main():
    """主函数"""
    process_stocks()


if __name__ == "__main__":
    main()