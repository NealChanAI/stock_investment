from pathlib import Path
from datetime import datetime
from os import path as osp  
import pandas as pd
import os

from stock_analysis import get_stock_info

# STOCK_FILE_NAME = "sz50_stocks.csv"
STOCK_FILE_NAME = "hs300_stocks.csv" 
ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = osp.join(ROOT_DIR, "data")
STOCK_LIST_FILE = osp.join(DATA_DIR, STOCK_FILE_NAME)
OUTPUT_DIR = osp.join(DATA_DIR, "stock_analysis_results")


def load_all_codes(csv_path: str):
    """读取csv中的股票代码列表"""
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
    # 如果最新市盈率或历史均值市盈率为负，均值回归模型失效
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

def post_process_results(result_rows):
    """
    对原始数据进行后处理
    """
    res_df = pd.DataFrame(result_rows)
    # PEG = pettm_at_date / mean_e_growth_rate
    res_df['PEG'] = res_df.apply(lambda row: row['pettm_at_date'] / (row['mean_e_growth_rate'] * 100) if row['mean_e_growth_rate'] > 0 else -10, axis=1)
    res_df['PEG'] = res_df['PEG'].map(lambda x: f"{x:.1f}")
    # 5年均值回归的收益率
    res_df['predict_revenue_5y'] = res_df.apply(lambda row: _calc_revenue(row, 'mean_pettm_5y'), axis=1)
    print(f"predict_revenue_5y: {res_df['predict_revenue_5y'].to_list()}")
    # 10年均值回归的收益率
    res_df['predict_revenue_10y'] = res_df.apply(lambda row: _calc_revenue(row, 'mean_pettm_10y'), axis=1)
    # 每股净利润增长率格式化
    res_df['mean_e_growth_rate'] = (res_df['mean_e_growth_rate'] * 100).map(lambda x: f"{x:.2f}%")
    # 5年均值回归的收益率格式化
    res_df['predict_revenue_5y'] = (res_df['predict_revenue_5y'] * 100).map(lambda x: f"{x:.2f}%")
    # 10年均值回归的收益率
    res_df['predict_revenue_10y'] = (res_df['predict_revenue_10y'] * 100).map(lambda x: f"{x:.2f}%")
    # 5年均值回归的市盈率格式化
    res_df['mean_pettm_5y'] = res_df['mean_pettm_5y'].map(lambda x: f"{x:.1f}")
    # 10年均值回归的市盈率格式化
    res_df['mean_pettm_10y'] = res_df['mean_pettm_10y'].map(lambda x: f"{x:.1f}")
    # 最新市盈率格式化
    res_df['pettm_at_date'] = res_df['pettm_at_date'].map(lambda x: f"{x:.1f}")
    # 市净率格式化
    res_df['pbmrq_at_date'] = res_df['pbmrq_at_date'].map(lambda x: f"{x:.2f}")
    # 5年市净率格式化
    res_df['mean_pbmrq_5y'] = res_df['mean_pbmrq_5y'].map(lambda x: f"{x:.2f}")
    # 10年市净率格式化
    res_df['mean_pbmrq_10y'] = res_df['mean_pbmrq_10y'].map(lambda x: f"{x:.2f}")
    # stock_code格式化
    res_df['stock_code'] = res_df['stock_code'].map(lambda x: str(x))

    cols = [
        "target_date",
        "stock_code",
        "stock_name",
        "mean_pettm_5y",
        "mean_pettm_10y",
        "pettm_at_date",
        "mean_e_growth_rate",
        "PEG",
        "predict_revenue_5y",
        "predict_revenue_10y",
        "pbmrq_at_date",
        "mean_pbmrq_5y",
        "mean_pbmrq_10y",
        "report_infos"
    ]

    cols_zh = [
        "日期",
        "股票代码",
        "股票名称",
        "5年均值回归的市盈率",
        "10年均值回归的市盈率",
        "最新市盈率",
        "预估每股净利润增长率(%)",
        "PEG",
        "5年均值PE回归的收益率(%)",
        "10年均值PE回归的收益率(%)",
        "最新市净率",
        "5年均值回归的市净率",
        "10年均值回归的市净率",
        "报告信息"
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



def process_stocks(csv_path: str = STOCK_LIST_FILE, output_dir: str = OUTPUT_DIR, file_name: str = "stock_analysis"):
    """
    批量处理股票列表，调用get_stock_info获取信息并保存到CSV
    
    Args:
        csv_path: 股票列表CSV文件路径，默认为STOCK_LIST_FILE
        output_dir: 输出目录，默认为OUTPUT_DIR
        file_name: 输出文件名前缀，默认为"stock_analysis"
    """
    # 加载股票列表
    print(f"正在加载股票列表: {csv_path}")
    try:
        stock_list = load_all_codes(csv_path)
        print(f"成功加载 {len(stock_list)} 只股票")
    except Exception as e:
        print(f"加载股票列表失败: {e}")
        return
    
    # 处理每只股票
    result_rows = []
    total = len(stock_list)
    
    for idx, stock_record in enumerate(stock_list, 1):
        stock_code = stock_record.get("code", "")
        stock_name = stock_record.get("code_name", "")
        
        print(f"[{idx}/{total}] 正在处理: {stock_code} {stock_name}")
        
        try:
            # 调用get_stock_info获取股票信息
            stock_info = get_stock_info(stock_code)
            
            if stock_info is None:
                print(f"  ⚠️  {stock_code} {stock_name}: 获取信息失败，跳过")
                continue
            
            # 添加股票代码和名称到结果中
            stock_info["code"] = stock_code
            stock_info["code_name"] = stock_name
            
            result_rows.append(stock_info)
            print(f"  ✅ {stock_code} {stock_name}: 处理成功")
            
        except Exception as e:
            print(f"  ❌ {stock_code} {stock_name}: 处理出错 - {e}")
            continue
    
    # 保存结果
    print(f"\n处理完成，成功处理 {len(result_rows)}/{total} 只股票")
    if result_rows:
        processed_df = post_process_results(result_rows)
        save_results(processed_df, output_dir, file_name)
    else:
        print("没有成功处理任何股票，不保存结果")


def main():
    """主函数"""
    process_stocks()


if __name__ == "__main__":
    main()