"""
    simplest trade system
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

# 输入文件名配置：必须指定要处理的 Excel 文件名
INPUT_FILE_NAME = "self_selected_stocks_20251223_111026.xlsx"

"""
1. 筛选出5年均值PE回归的收益率(%)大于30% 且 PEG < 1.5 且 g > 10% 的股票 且 5年均值PB回归的涨幅(%)大于10%
2. 筛选出10年均值PE回归的收益率(%)大于30% 且 PEG < 1.5 且 g > 10% 的股票 且 10年均值PB回归的涨幅(%)大于10%
3. 筛选出PEG < 1.5 且 g > 10% 的股票 且 5年均值PE回归的收益率(%)大于30% 且 10年均值PE回归的收益率(%)大于30% 且 且 5年均值PB回归的涨幅(%)大于10%
"""


def clean_percentage(value):
    """
    清理百分比字符串，转换为浮点数
    例如: "10.08%" -> 10.08, "-10.08%" -> -10.08
    """
    if pd.isna(value):
        return None
    if isinstance(value, str):
        value = value.replace('%', '').strip()
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def filter_stocks(df):
    """
    根据规则筛选股票，返回三个规则的mask（布尔数组）
    
    规则1: 5年均值PE回归的收益率(%) > 30% 且 PEG < 1.5 且 g > 10%
    规则2: 10年均值PE回归的收益率(%) > 30% 且 PEG < 1.5 且 g > 10%
    规则3: PEG < 1.5 且 g > 10% 且 5年均值PE回归的收益率(%) > 30% 且 10年均值PE回归的收益率(%) > 30% 且 5年均值PB回归的涨幅(%) > 10%
    """
    # 清理数据
    df = df.copy()
    
    # 清理百分比列
    df['5年均值PE回归的收益率(%)_clean'] = df['5年均值PE回归的收益率(%)'].apply(clean_percentage)
    df['10年均值PE回归的收益率(%)_clean'] = df['10年均值PE回归的收益率(%)'].apply(clean_percentage)
    df['预估每股净利润增长率(%)_clean'] = df['预估每股净利润增长率(%)'].apply(clean_percentage)
    df['5年均值PB回归的涨幅(%)_clean'] = df['5年均值PB回归的涨幅(%)'].apply(clean_percentage)
    
    # PEG列转换为数值
    df['PEG_clean'] = pd.to_numeric(df['PEG'], errors='coerce')
    
    # 规则1: 5年均值PE回归的收益率(%) > 30% 且 PEG < 1.5 且 g > 10%
    rule1_mask = (
        (df['5年均值PE回归的收益率(%)_clean'] > 30) &
        (df['PEG_clean'] < 1.5) &
        (df['预估每股净利润增长率(%)_clean'] > 10)
    )
    
    # 规则2: 10年均值PE回归的收益率(%) > 30% 且 PEG < 1.5 且 g > 10%
    rule2_mask = (
        (df['10年均值PE回归的收益率(%)_clean'] > 30) &
        (df['PEG_clean'] < 1.5) &
        (df['预估每股净利润增长率(%)_clean'] > 10)
    )
    
    # 规则3: PEG < 1.5 且 g > 10% 且 5年均值PE回归的收益率(%) > 30% 且 10年均值PE回归的收益率(%) > 30% 且 5年均值PB回归的涨幅(%) > 10%
    rule3_mask = (
        (df['PEG_clean'] < 1.5) &
        (df['预估每股净利润增长率(%)_clean'] > 10) &
        (df['5年均值PE回归的收益率(%)_clean'] > 30) &
        (df['10年均值PE回归的收益率(%)_clean'] > 30) &
        (df['5年均值PB回归的涨幅(%)_clean'] > 10)
    )
    
    return rule1_mask, rule2_mask, rule3_mask


def merge_and_save_results(df, rule1_mask, rule2_mask, rule3_mask, output_dir, input_file_name):
    """
    为原始数据添加规则1/规则2/规则3三个字段，并保存到Excel文件
    
    约定：
    - 字段名为：规则1、规则2、规则3
    - 若某只股票满足对应规则，则该字段值为 1；不满足则为空（空字符串）
    - 每个股票只保留一行数据
    - 只保存满足至少一个规则的股票
    """
    # 复制原始数据
    result_df = df.copy()
    
    # 删除临时清理列
    result_df = result_df.drop(columns=[col for col in result_df.columns if col.endswith('_clean')])
    
    # 添加规则字段，初始化为空字符串（字符串类型）
    result_df['规则1'] = ""
    result_df['规则2'] = ""
    result_df['规则3'] = ""
    
    # 根据mask设置规则字段值（使用字符串"1"）
    result_df.loc[rule1_mask, '规则1'] = "1"
    result_df.loc[rule2_mask, '规则2'] = "1"
    result_df.loc[rule3_mask, '规则3'] = "1"
    
    # 只保留满足至少一个规则的股票
    has_rule = (result_df['规则1'] == "1") | (result_df['规则2'] == "1") | (result_df['规则3'] == "1")
    result_df = result_df[has_rule].copy()
    
    # 如果没有找到任何符合条件的股票
    if result_df.empty:
        print("未找到任何符合条件的股票")
        return
    
    # 确保规则字段中空字符串保持为空字符串（替换可能的NaN）
    for col in ['规则1', '规则2', '规则3']:
        result_df[col] = result_df[col].fillna("")
        # 确保值为"1"的保持为"1"，空字符串保持为空字符串
        result_df[col] = result_df[col].apply(lambda x: "1" if x == "1" or x == 1 or x == 1.0 else "")
    
    # 生成输出文件名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = input_file_name.replace('.xlsx', '').replace('.xls', '')
    output_file = output_dir / f"{base_name}_filtered_{timestamp}.xlsx"
    
    # 保存到Excel
    result_df.to_excel(output_file, index=False)
    print(f"筛选结果已保存至: {output_file}")
    print(f"共找到 {len(result_df)} 条符合条件的股票记录")
    print(f"  - 规则1: {rule1_mask.sum()} 条")
    print(f"  - 规则2: {rule2_mask.sum()} 条")
    print(f"  - 规则3: {rule3_mask.sum()} 条")


def main():
    """
    主函数：读取excel文件并筛选股票
    """
    # 获取数据目录
    data_dir = Path(__file__).parent.parent.parent / 'data' / 'stock_analysis_results'
    
    # 检查是否指定了文件名
    if INPUT_FILE_NAME is None:
        print("错误: 请在文件顶部设置 INPUT_FILE_NAME 变量来指定要处理的 Excel 文件名")
        print(f"例如: INPUT_FILE_NAME = 'stock_analysis_20251222_203658.xlsx'")
        return
    
    # 使用指定的文件名
    excel_file = data_dir / INPUT_FILE_NAME
    if not excel_file.exists():
        print(f"指定的文件不存在: {excel_file}")
        return
    
    print(f"正在读取文件: {excel_file}")
    
    # 读取excel文件
    try:
        df = pd.read_excel(excel_file)
        print(f"成功读取 {len(df)} 条股票数据")
    except Exception as e:
        print(f"读取文件失败: {e}")
        return
    
    # 筛选股票，获取三个规则的mask
    rule1_mask, rule2_mask, rule3_mask = filter_stocks(df)
    
    # 合并结果并保存
    merge_and_save_results(df, rule1_mask, rule2_mask, rule3_mask, data_dir, INPUT_FILE_NAME)


if __name__ == "__main__":
    main()