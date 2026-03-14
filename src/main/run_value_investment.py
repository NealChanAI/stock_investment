"""
价值投资系统运行脚本
从CSV文件读取股票列表，评估并生成投资建议
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import os
import sys

# 添加项目路径
sys.path.append(str(Path(__file__).resolve().parents[1]))
from value_investment_system import ValueInvestmentSystem
from stock_info_extract import load_all_codes, OUTPUT_DIR


def run_value_investment_analysis(
    stock_list_file: str,
    output_dir: str = None,
    target_date: str = None,
    config: dict = None
):
    """
    运行价值投资系统分析
    
    Args:
        stock_list_file: 股票列表CSV文件路径
        output_dir: 输出目录，如果为None则使用默认目录
        target_date: 目标日期，格式为YYYY-MM-DD，如果为None则使用今天
        config: 系统配置字典，如果为None则使用默认配置
    """
    # 设置输出目录
    if output_dir is None:
        output_dir = OUTPUT_DIR

    # 合并配置：每次评估时输出预测期股票信息到 predict_YYYYMMDD_HHMMSS.log
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    merged_config = dict(config) if config else {}
    merged_config.setdefault("predict_log_path", str(Path(output_dir) / f"predict_{timestamp}.log"))

    # 创建价值投资系统实例
    system = ValueInvestmentSystem(config=merged_config)
    
    # 加载股票列表
    print(f"正在加载股票列表: {stock_list_file}")
    try:
        stock_records = load_all_codes(stock_list_file)
        stock_codes = [record['code'] for record in stock_records]
        print(f"成功加载 {len(stock_codes)} 只股票\n")
    except Exception as e:
        print(f"加载股票列表失败: {e}")
        return
    
    # 评估所有股票
    print("=" * 80)
    print("开始评估股票...")
    print("=" * 80)
    
    results_df = system.evaluate_stock_list(stock_codes, target_date, stock_records=stock_records)
    
    if results_df.empty:
        print("没有获取到任何评估结果")
        return
    
    # 保存完整评估结果
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存所有评估结果
    all_results_file = os.path.join(output_dir, f"value_investment_all_{timestamp}.xlsx")
    results_df.to_excel(all_results_file, index=False)
    print(f"\n完整评估结果已保存至: {all_results_file}")
    
    # 筛选符合买入条件的股票
    buyable_df = results_df[results_df['can_buy'] == True].copy()
    
    if buyable_df.empty:
        print("\n没有符合买入条件的股票")
        return
    
    # 保存符合买入条件的股票
    buyable_file = os.path.join(output_dir, f"value_investment_buyable_{timestamp}.xlsx")
    buyable_df.to_excel(buyable_file, index=False)
    print(f"符合买入条件的股票已保存至: {buyable_file}")
    
    # 选出最佳股票组合
    print("\n" + "=" * 80)
    print("选出最佳股票组合...")
    print("=" * 80)
    
    best_stocks = system.select_best_stocks(stock_codes, target_date, evaluation_df=results_df, stock_records=stock_records)
    
    if best_stocks:
        # 转换为DataFrame并保存
        best_df = pd.DataFrame(best_stocks)
        best_file = os.path.join(output_dir, f"value_investment_portfolio_{timestamp}.xlsx")
        best_df.to_excel(best_file, index=False)
        print(f"\n最佳组合已保存至: {best_file}")
        
        # 打印组合摘要
        print("\n" + "=" * 80)
        print("组合摘要")
        print("=" * 80)
        summary = system.get_portfolio_summary()
        print(f"股票数量: {summary['total_stocks']}")
        print(f"行业数量: {summary['total_industries']}")
        print(f"平均目标收益率: {summary['avg_target_return']*100:.2f}%")
        print(f"平均PEG: {summary['avg_peg']:.2f}")
        print("\n行业分布:")
        for industry, count in summary['industry_distribution'].items():
            print(f"  {industry}: {count}只")
        
        # 打印选中的股票详情
        print("\n选中的股票:")
        print("-" * 80)
        for idx, stock in enumerate(best_stocks, 1):
            print(f"{idx}. {stock['stock_code']} {stock['stock_name']}")
            print(f"   行业: {stock['industry']}")
            print(f"   当前PE: {stock['pe_now']:.2f}, 历史均值PE: {stock['pe_mean']:.2f}")
            print(f"   增长率: {stock['growth_rate']:.2f}%")
            print(f"   PEG: {stock['PEG']:.2f}")
            print(f"   目标收益率: {stock['target_return']:.2f}%")
            print()
    else:
        print("\n未能选出符合条件的股票组合")
    
    # 打印统计信息
    print("\n" + "=" * 80)
    print("评估统计")
    print("=" * 80)
    print(f"总评估股票数: {len(results_df)}")
    print(f"符合买入条件: {len(buyable_df)}")
    print(f"最终选中: {len(best_stocks) if best_stocks else 0}")
    
    # 按行业统计符合买入条件的股票
    if not buyable_df.empty:
        print("\n符合买入条件的股票按行业分布:")
        industry_counts = buyable_df['industry'].value_counts()
        for industry, count in industry_counts.items():
            print(f"  {industry}: {count}只")


def main():
    """
    主函数
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='价值投资系统分析工具')
    parser.add_argument(
        '--stock-list',
        type=str,
        default=None,
        help='股票列表CSV文件路径（默认使用stock_info_extract.py中的STOCK_FILE_NAME）'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=None,
        help='输出目录（默认使用stock_info_extract.py中的OUTPUT_DIR）'
    )
    parser.add_argument(
        '--target-date',
        type=str,
        default=None,
        help='目标日期，格式为YYYY-MM-DD（默认使用今天）'
    )
    
    args = parser.parse_args()
    
    # 如果没有指定股票列表，使用默认的
    if args.stock_list is None:
        from stock_info_extract import STOCK_LIST_FILE
        stock_list_file = STOCK_LIST_FILE
    else:
        stock_list_file = args.stock_list
    
    # 运行分析
    run_value_investment_analysis(
        stock_list_file=stock_list_file,
        output_dir=args.output_dir,
        target_date=args.target_date
    )


if __name__ == "__main__":
    main()
