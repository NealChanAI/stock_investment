"""
测试akshare接口是否支持2028年数据
"""

import sys
import io
from pathlib import Path

# 设置输出编码为UTF-8
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 添加项目路径
sys.path.append(str(Path(__file__).resolve().parents[1]))
import akshare as ak
import pandas as pd

def test_stock_research_report_columns():
    """
    测试stock_research_report_em接口返回的列名，检查是否有2028年数据
    """
    # 使用一个常见的股票代码进行测试
    test_stock_code = "000001"  # 平安银行
    
    print("=" * 80)
    print(f"测试股票代码: {test_stock_code}")
    print("=" * 80)
    
    try:
        # 调用接口
        print("\n正在调用 ak.stock_research_report_em()...")
        report_df = ak.stock_research_report_em(symbol=test_stock_code)
        
        if report_df is None or report_df.empty:
            print("[警告] 接口返回空数据")
            return
        
        print(f"\n[成功] 成功获取数据，共 {len(report_df)} 条记录")
        
        # 检查股票代码是否一致
        if '股票代码' in report_df.columns:
            unique_codes = report_df['股票代码'].unique()
            print(f"\n股票代码唯一值: {unique_codes}")
            if len(unique_codes) > 1:
                print(f"[警告] 发现 {len(unique_codes)} 个不同的股票代码！")
                code_counts = report_df['股票代码'].value_counts()
                print("\n各股票代码的记录数:")
                for code, count in code_counts.items():
                    print(f"  {code}: {count} 条")
            else:
                print(f"[确认] 所有记录都是同一只股票: {unique_codes[0]}")
        
        # 显示所有列名
        print("\n" + "=" * 80)
        print("接口返回的所有列名:")
        print("=" * 80)
        for idx, col in enumerate(report_df.columns, 1):
            print(f"{idx:2d}. {col}")
        
        # 检查是否包含年份相关的列
        print("\n" + "=" * 80)
        print("检查年份相关列:")
        print("=" * 80)
        
        # 查找所有包含年份的列
        year_columns = {}
        for col in report_df.columns:
            col_str = str(col)
            for year in ['2024', '2025', '2026', '2027', '2028', '2029', '2030']:
                if year in col_str:
                    if year not in year_columns:
                        year_columns[year] = []
                    year_columns[year].append(col)
        
        if year_columns:
            print("\n找到的年份相关列:")
            for year in sorted(year_columns.keys()):
                print(f"\n  {year}年:")
                for col in year_columns[year]:
                    print(f"    - {col}")
        else:
            print("\n未找到年份相关列（可能列名格式不同）")
        
        # 特别检查盈利预测-市盈率相关的列
        print("\n" + "=" * 80)
        print("检查盈利预测-市盈率相关列:")
        print("=" * 80)
        
        pe_columns = [col for col in report_df.columns if '市盈率' in str(col) or 'PE' in str(col).upper()]
        if pe_columns:
            print("\n找到的市盈率相关列:")
            for col in pe_columns:
                print(f"  - {col}")
        else:
            print("\n未找到市盈率相关列")
        
        # 显示前几条数据示例（只显示关键列）
        print("\n" + "=" * 80)
        print("数据示例（前2条，仅显示年份相关列）:")
        print("=" * 80)
        
        # 选择年份相关的列进行显示
        display_cols = ['股票代码', '股票简称', '日期']
        display_cols.extend([col for col in report_df.columns if any(year in str(col) for year in ['2025', '2026', '2027', '2028'])])
        
        if display_cols:
            print(report_df[display_cols].head(2).to_string())
        
        # 特别检查2028年相关的列
        print("\n" + "=" * 80)
        print("检查2028年数据:")
        print("=" * 80)
        
        has_2028 = False
        for col in report_df.columns:
            if '2028' in str(col):
                has_2028 = True
                print(f"[找到] 2028年相关列: {col}")
                # 检查该列是否有非空数据
                non_null_count = report_df[col].notna().sum()
                print(f"       非空数据条数: {non_null_count}/{len(report_df)}")
                if non_null_count > 0:
                    # 显示一些示例值
                    sample_values = report_df[col].dropna().head(3).tolist()
                    print(f"       示例值: {sample_values}")
        
        if not has_2028:
            print("[未找到] 2028年相关的列")
            print("\n建议:")
            print("1. 当前接口可能不支持2028年数据")
            print("2. 可以继续使用2025-2027年的数据进行计算")
            print("3. 或者等待akshare接口更新")
        
        # 检查每份研报中有多少个年份有值
        print("\n" + "=" * 80)
        print("检查每份研报中的年份数据完整性:")
        print("=" * 80)
        
        pe_cols = {
            '2025': '2025-盈利预测-市盈率',
            '2026': '2026-盈利预测-市盈率',
            '2027': '2027-盈利预测-市盈率'
        }
        
        # 统计每条记录中有值的年份数量
        year_count_stats = {}
        for idx, row in report_df.iterrows():
            # 统计这条记录中有多少个年份有值
            valid_years = []
            for year, col in pe_cols.items():
                if col in report_df.columns:
                    value = row[col]
                    if pd.notna(value) and value != '':
                        try:
                            float_val = float(value)
                            if float_val > 0:  # 确保是正数
                                valid_years.append(year)
                        except (ValueError, TypeError):
                            pass
            
            year_count = len(valid_years)
            if year_count not in year_count_stats:
                year_count_stats[year_count] = {
                    'count': 0,
                    'examples': []
                }
            year_count_stats[year_count]['count'] += 1
            
            # 保存前5个示例
            if len(year_count_stats[year_count]['examples']) < 5:
                year_count_stats[year_count]['examples'].append({
                    'index': idx,
                    'years': valid_years,
                    'date': row.get('日期', ''),
                    'institution': row.get('机构', '')
                })
        
        print(f"\n统计结果（共 {len(report_df)} 条记录）:")
        print("-" * 80)
        for year_count in sorted(year_count_stats.keys(), reverse=True):
            stats = year_count_stats[year_count]
            percentage = (stats['count'] / len(report_df)) * 100
            print(f"\n有 {year_count} 个年份数据的研报: {stats['count']} 条 ({percentage:.1f}%)")
            
            if stats['examples']:
                print(f"  示例（前{min(5, len(stats['examples']))}条）:")
                for ex in stats['examples']:
                    years_str = ', '.join(ex['years'])
                    print(f"    - 日期: {ex['date']}, 机构: {ex['institution']}, 年份: {years_str}")
        
        # 检查年份组合情况
        print("\n" + "=" * 80)
        print("年份组合统计:")
        print("=" * 80)
        
        year_combinations = {}
        for idx, row in report_df.iterrows():
            valid_years = []
            for year, col in pe_cols.items():
                if col in report_df.columns:
                    value = row[col]
                    if pd.notna(value) and value != '':
                        try:
                            float_val = float(value)
                            if float_val > 0:
                                valid_years.append(year)
                        except (ValueError, TypeError):
                            pass
            
            combo_key = '-'.join(sorted(valid_years))
            if combo_key:
                year_combinations[combo_key] = year_combinations.get(combo_key, 0) + 1
        
        print("\n年份组合分布:")
        for combo, count in sorted(year_combinations.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / len(report_df)) * 100
            print(f"  {combo}: {count} 条 ({percentage:.1f}%)")
        
    except Exception as e:
        print(f"\n[错误] 测试失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_stock_research_report_columns()
