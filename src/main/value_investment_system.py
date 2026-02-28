"""
基于预估收益率的价值投资系统
参考文档：data/files/价值投资系统建构举隅.txt
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import os
import sys

# 添加项目路径以便导入模块
sys.path.append(str(Path(__file__).resolve().parents[1]))
from stock_analysis import get_stock_info, add_stock_prefix


class ValueInvestmentSystem:
    """
    价值投资系统类
    实现基于预估收益率的买入、卖出、换股决策
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化价值投资系统
        
        Args:
            config: 配置字典，包含系统参数。如果为None，使用默认参数
        """
        # 默认参数配置（根据文档）
        default_config = {
            # 变量参数
            'min_growth_rate': 0.10,  # g > 10% (根据券商未来三年预估)
            'max_peg': 1.2,  # PEG < 1.2
            'min_target_return': 0.35,  # 目标预估收益率 > 35%
            'max_sell_return': 0.0,  # 卖出预估收益率 <= 0%
            'max_stocks': 20,  # 组合中股票数量：20只
            'max_industries': 10,  # 行业数量：10个行业
            'max_stocks_per_industry': 5,  # 行业最大股票数量：5只
            'n_years': 3,  # n取3年，是决策公式中的n
            'max_g_credible': 0.35,  # g可信值 <= 35%
            
            # 其他配置
            'rebalance_period_months': 6,  # 再平衡周期（月）
            'use_mean_pe_5y': True,  # 使用5年PE均值还是10年PE均值
        }
        
        # 合并用户配置和默认配置
        if config:
            default_config.update(config)
        self.config = default_config
        
        # 当前持仓组合
        self.portfolio: List[Dict] = []
        
        # 组合历史记录
        self.portfolio_history: List[Dict] = []
    
    def calculate_peg(self, pe: float, g: float) -> float:
        """
        计算PEG值
        PEG = PE / (g * 100)
        
        Args:
            pe: 当前市盈率
            g: 增长率（小数形式，如0.2表示20%）
        
        Returns:
            PEG值
        """
        if g <= 0:
            return float('inf')
        return pe / (g * 100)
    
    def calculate_target_return(
        self, 
        pe_now: float, 
        pe_mean: float, 
        g: float, 
        n: Optional[int] = None
    ) -> float:
        """
        计算目标预估收益率
        公式：((PE_mean / PE_now)^(1/n)) * (1 + g) - 1
        
        Args:
            pe_now: 当前市盈率
            pe_mean: 历史平均市盈率
            g: 增长率（小数形式，如0.2表示20%）
            n: 年数，默认使用配置中的n_years
        
        Returns:
            目标预估收益率（小数形式，如0.35表示35%）
        """
        if n is None:
            n = self.config['n_years']
        
        # 数据验证
        if pe_now <= 0 or pe_mean <= 0 or g < 0:
            return -1.0  # 返回负值表示无法计算
        
        try:
            # 计算PE回归比率
            ratio = pe_mean / pe_now
            
            # 计算年化PE回归系数
            pe_regression_factor = ratio ** (1.0 / n)
            
            # 计算目标收益率
            target_return = pe_regression_factor * (1 + g) - 1
            
            return target_return
        except Exception as e:
            print(f"计算目标收益率时出错: {e}")
            return -1.0
    
    def limit_g_credible(self, g: float) -> float:
        """
        限制g的可信值，避免过于乐观的预测
        如果g > max_g_credible，则取max_g_credible
        
        Args:
            g: 原始增长率
        
        Returns:
            限制后的增长率
        """
        max_g = self.config['max_g_credible']
        return min(g, max_g)
    
    def check_buy_conditions(self, stock_info: Dict) -> Tuple[bool, List[str]]:
        """
        检查买入条件
        
        买入条件（需全部满足）：
        1. PEG < 1.2
        2. 目标预估收益率 > 35%
        3. 当前市盈率必须小于等于历史均值
        4. g > 10% (根据券商未来三年预估)
        5. 组合中股票数量: 不大于20只
        6. 未超过行业最大股票数量5只
        
        Args:
            stock_info: 股票信息字典，应包含以下字段：
                - stock_code: 股票代码
                - stock_name: 股票名称
                - industry: 所属行业
                - pettm_at_date: 当前市盈率
                - mean_pettm_5y 或 mean_pettm_10y: 历史平均PE
                - mean_e_growth_rate: 预估增长率（小数形式）
        
        Returns:
            (是否符合买入条件, 不符合的原因列表)
        """
        reasons = []
        
        # 获取必要数据
        pe_now = stock_info.get('pettm_at_date')
        pe_mean = stock_info.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else stock_info.get('mean_pettm_10y')
        g_raw = stock_info.get('mean_e_growth_rate', 0)
        
        # 数据验证
        if pe_now is None or pe_mean is None or g_raw is None:
            reasons.append("缺少必要的估值数据")
            return False, reasons
        
        # 限制g的可信值
        g = self.limit_g_credible(g_raw)
        
        # 条件1: PEG < 1.2
        peg = self.calculate_peg(pe_now, g)
        if peg >= self.config['max_peg']:
            reasons.append(f"PEG={peg:.2f} >= {self.config['max_peg']}")
        
        # 条件2: 目标预估收益率 > 35%
        target_return = self.calculate_target_return(pe_now, pe_mean, g)
        if target_return <= self.config['min_target_return']:
            reasons.append(f"目标预估收益率={target_return*100:.2f}% <= {self.config['min_target_return']*100}%")
        
        # 条件3: 当前市盈率必须小于等于历史均值
        if pe_now > pe_mean:
            reasons.append(f"当前PE={pe_now:.2f} > 历史均值PE={pe_mean:.2f}")
        
        # 条件4: g > 10%
        if g <= self.config['min_growth_rate']:
            reasons.append(f"增长率={g*100:.2f}% <= {self.config['min_growth_rate']*100}%")
        
        # 条件5: 组合中股票数量不大于20只
        if len(self.portfolio) >= self.config['max_stocks']:
            reasons.append(f"组合股票数量={len(self.portfolio)} >= {self.config['max_stocks']}")
        
        # 条件6: 未超过行业最大股票数量
        industry = stock_info.get('industry', '')
        industry_stock_count = sum(1 for s in self.portfolio if s.get('industry') == industry)
        if industry_stock_count >= self.config['max_stocks_per_industry']:
            reasons.append(f"行业'{industry}'股票数量={industry_stock_count} >= {self.config['max_stocks_per_industry']}")
        
        # 如果所有条件都满足，reasons为空
        return len(reasons) == 0, reasons
    
    def check_sell_conditions(self, stock_info: Dict) -> Tuple[bool, str]:
        """
        检查卖出条件
        
        卖出条件（AB任意一个发生即卖出）：
        A. 基本面变坏: g < 10% (根据券商未来三年预估)
        B. 个股处于极端高估或未来预估收益率降低: 卖出预估收益率 <= 0%
        
        Args:
            stock_info: 股票信息字典
        
        Returns:
            (是否应该卖出, 卖出原因)
        """
        g_raw = stock_info.get('mean_e_growth_rate', 0)
        if g_raw is None:
            return False, ""
        
        # 限制g的可信值
        g = self.limit_g_credible(g_raw)
        
        # 条件A: 基本面变坏
        if g < self.config['min_growth_rate']:
            return True, f"基本面变坏: 增长率={g*100:.2f}% < {self.config['min_growth_rate']*100}%"
        
        # 条件B: 卖出预估收益率 <= 0%
        pe_now = stock_info.get('pettm_at_date')
        pe_mean = stock_info.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else stock_info.get('mean_pettm_10y')
        
        if pe_now is not None and pe_mean is not None:
            sell_return = self.calculate_target_return(pe_now, pe_mean, g)
            if sell_return <= self.config['max_sell_return']:
                return True, f"卖出预估收益率={sell_return*100:.2f}% <= {self.config['max_sell_return']*100}%"
        
        return False, ""
    
    def check_swap_conditions(
        self, 
        current_stock: Dict, 
        new_stock: Dict
    ) -> Tuple[bool, List[str]]:
        """
        检查换股条件
        
        换股条件（需全部满足）：
        1. 换入标的与换出标的目标预估收益率之差 > 35%
        2. 换入标的应满足买入条件
        3. 换股后应满足组合限制
        
        Args:
            current_stock: 当前持有的股票信息
            new_stock: 准备换入的股票信息
        
        Returns:
            (是否符合换股条件, 不符合的原因列表)
        """
        reasons = []
        
        # 计算两个股票的目标预估收益率
        pe_now_curr = current_stock.get('pettm_at_date')
        pe_mean_curr = current_stock.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else current_stock.get('mean_pettm_10y')
        g_curr = self.limit_g_credible(current_stock.get('mean_e_growth_rate', 0))
        
        pe_now_new = new_stock.get('pettm_at_date')
        pe_mean_new = new_stock.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else new_stock.get('mean_pettm_10y')
        g_new = self.limit_g_credible(new_stock.get('mean_e_growth_rate', 0))
        
        if pe_now_curr is None or pe_mean_curr is None or pe_now_new is None or pe_mean_new is None:
            reasons.append("缺少必要的估值数据")
            return False, reasons
        
        target_return_curr = self.calculate_target_return(pe_now_curr, pe_mean_curr, g_curr)
        target_return_new = self.calculate_target_return(pe_now_new, pe_mean_new, g_new)
        
        # 条件1: 换入标的与换出标的目标预估收益率之差 > 35%
        return_diff = target_return_new - target_return_curr
        if return_diff <= self.config['min_target_return']:
            reasons.append(f"收益率差={return_diff*100:.2f}% <= {self.config['min_target_return']*100}%")
        
        # 条件2: 换入标的应满足买入条件
        # 临时移除当前股票，检查新股票是否符合买入条件
        temp_portfolio = [s for s in self.portfolio if s.get('stock_code') != current_stock.get('stock_code')]
        original_portfolio = self.portfolio
        self.portfolio = temp_portfolio
        
        can_buy, buy_reasons = self.check_buy_conditions(new_stock)
        if not can_buy:
            reasons.extend([f"换入标的买入条件不满足: {r}" for r in buy_reasons])
        
        # 恢复原组合
        self.portfolio = original_portfolio
        
        # 条件3: 换股后应满足组合限制（已在check_buy_conditions中检查）
        
        return len(reasons) == 0, reasons
    
    def add_to_portfolio(self, stock_info: Dict, position_size: Optional[float] = None):
        """
        添加股票到组合
        
        Args:
            stock_info: 股票信息字典
            position_size: 持仓金额，如果为None则平均分配
        """
        # 添加时间戳和持仓金额
        stock_info['add_date'] = datetime.now().strftime('%Y-%m-%d')
        stock_info['position_size'] = position_size
        
        self.portfolio.append(stock_info)
    
    def remove_from_portfolio(self, stock_code: str):
        """
        从组合中移除股票
        
        Args:
            stock_code: 股票代码
        """
        self.portfolio = [s for s in self.portfolio if s.get('stock_code') != stock_code]
    
    def get_portfolio_summary(self) -> Dict:
        """
        获取组合摘要信息
        
        Returns:
            包含组合统计信息的字典
        """
        if not self.portfolio:
            return {
                'total_stocks': 0,
                'total_industries': 0,
                'industry_distribution': {},
                'avg_target_return': 0,
                'avg_peg': 0
            }
        
        industries = {}
        target_returns = []
        pegs = []
        
        for stock in self.portfolio:
            industry = stock.get('industry', '未知')
            industries[industry] = industries.get(industry, 0) + 1
            
            pe_now = stock.get('pettm_at_date')
            pe_mean = stock.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else stock.get('mean_pettm_10y')
            g = self.limit_g_credible(stock.get('mean_e_growth_rate', 0))
            
            if pe_now and pe_mean:
                target_return = self.calculate_target_return(pe_now, pe_mean, g)
                if target_return > 0:
                    target_returns.append(target_return)
                
                peg = self.calculate_peg(pe_now, g)
                if peg < float('inf'):
                    pegs.append(peg)
        
        return {
            'total_stocks': len(self.portfolio),
            'total_industries': len(industries),
            'industry_distribution': industries,
            'avg_target_return': np.mean(target_returns) if target_returns else 0,
            'avg_peg': np.mean(pegs) if pegs else 0
        }
    
    def rebalance_portfolio(self, total_assets: float):
        """
        定期再平衡组合
        
        根据文档：用总资产除以20份求得当前每份资金量，卖出部分占用资金高的个股，
        买入部分占用资金低的个股，使得每份资金接近当前每份资金量。
        
        Args:
            total_assets: 总资产金额
        """
        if not self.portfolio:
            return
        
        target_position_size = total_assets / self.config['max_stocks']
        
        # 更新每只股票的持仓金额
        for stock in self.portfolio:
            current_size = stock.get('position_size', 0)
            if current_size == 0:
                # 如果没有设置持仓金额，使用目标金额
                stock['position_size'] = target_position_size
            else:
                # 调整到目标金额
                stock['position_size'] = target_position_size
        
        print(f"再平衡完成：目标每份资金={target_position_size:.2f}元")
    
    def evaluate_stock_list(
        self, 
        stock_codes: List[str], 
        target_date: Optional[str] = None
    ) -> pd.DataFrame:
        """
        评估股票列表，返回符合买入条件的股票
        
        Args:
            stock_codes: 股票代码列表
            target_date: 目标日期，格式为YYYY-MM-DD
        
        Returns:
            包含评估结果的DataFrame
        """
        results = []
        
        for idx, code in enumerate(stock_codes, 1):
            print(f"[{idx}/{len(stock_codes)}] 正在评估: {code}")
            
            try:
                # 获取股票信息
                stock_info = get_stock_info(code, target_date)
                
                if stock_info is None:
                    print(f"  ⚠️  无法获取股票信息，跳过")
                    continue
                
                # 计算关键指标
                pe_now = stock_info.get('pettm_at_date')
                pe_mean = stock_info.get('mean_pettm_5y') if self.config['use_mean_pe_5y'] else stock_info.get('mean_pettm_10y')
                g_raw = stock_info.get('mean_e_growth_rate', 0)
                g = self.limit_g_credible(g_raw) if g_raw else 0
                
                # 计算PEG和目标收益率
                peg = self.calculate_peg(pe_now, g) if pe_now and g > 0 else float('inf')
                target_return = self.calculate_target_return(pe_now, pe_mean, g) if pe_now and pe_mean else -1
                
                # 检查买入条件
                can_buy, reasons = self.check_buy_conditions(stock_info)
                
                # 构建结果记录
                result = {
                    'stock_code': code,
                    'stock_name': stock_info.get('stock_name', ''),
                    'industry': stock_info.get('industry', ''),
                    'pe_now': pe_now,
                    'pe_mean': pe_mean,
                    'growth_rate': g * 100 if g else 0,
                    'PEG': peg,
                    'target_return': target_return * 100 if target_return > 0 else -100,
                    'can_buy': can_buy,
                    'buy_reasons': '; '.join(reasons) if reasons else '符合买入条件'
                }
                
                results.append(result)
                
                status = "✅ 符合买入条件" if can_buy else f"❌ 不符合: {', '.join(reasons[:2])}"
                print(f"  {status}")
                
            except Exception as e:
                print(f"  ❌ 评估出错: {e}")
                continue
        
        # 转换为DataFrame
        df = pd.DataFrame(results)
        
        # 按目标收益率排序
        if not df.empty and 'target_return' in df.columns:
            df = df.sort_values('target_return', ascending=False).reset_index(drop=True)
        
        return df
    
    def select_best_stocks(
        self, 
        stock_codes: List[str], 
        target_date: Optional[str] = None,
        evaluation_df: Optional[pd.DataFrame] = None
    ) -> List[Dict]:
        """
        从股票列表中选出最符合买入条件的股票
        
        Args:
            stock_codes: 股票代码列表
            target_date: 目标日期
            evaluation_df: 可选的评估结果DataFrame，如果提供则跳过评估步骤
        
        Returns:
            选出的股票列表（包含完整的股票信息）
        """
        # 如果没有提供评估结果，则进行评估
        if evaluation_df is None:
            df = self.evaluate_stock_list(stock_codes, target_date)
        else:
            df = evaluation_df
        
        if df.empty:
            return []
        
        # 筛选符合买入条件的股票
        buyable_df = df[df['can_buy'] == True].copy()
        
        if buyable_df.empty:
            print("没有符合买入条件的股票")
            return []
        
        # 按行业分组，每个行业最多选择max_stocks_per_industry只
        selected_stocks = []
        industry_counts = {}
        
        for _, row in buyable_df.iterrows():
            industry = row['industry']
            if not industry:
                industry = '未知'
            
            # 检查行业股票数量限制
            if industry_counts.get(industry, 0) >= self.config['max_stocks_per_industry']:
                continue
            
            # 检查总股票数量限制
            if len(selected_stocks) >= self.config['max_stocks']:
                break
            
            # 添加到选中列表（转换为字典）
            stock_dict = row.to_dict()
            selected_stocks.append(stock_dict)
            industry_counts[industry] = industry_counts.get(industry, 0) + 1
        
        print(f"\n共选出 {len(selected_stocks)} 只股票")
        print(f"涉及 {len(industry_counts)} 个行业")
        
        return selected_stocks
    
    def save_portfolio(self, file_path: str):
        """
        保存组合到文件
        
        Args:
            file_path: 保存路径
        """
        if not self.portfolio:
            print("组合为空，无需保存")
            return
        
        # 转换为DataFrame
        df = pd.DataFrame(self.portfolio)
        
        # 保存为Excel
        df.to_excel(file_path, index=False)
        print(f"组合已保存至: {file_path}")
    
    def load_portfolio(self, file_path: str):
        """
        从文件加载组合
        
        Args:
            file_path: 文件路径
        """
        if not os.path.exists(file_path):
            print(f"文件不存在: {file_path}")
            return
        
        df = pd.read_excel(file_path)
        self.portfolio = df.to_dict('records')
        print(f"已加载 {len(self.portfolio)} 只股票到组合")


def main():
    """
    主函数示例
    """
    # 创建价值投资系统实例
    system = ValueInvestmentSystem()
    
    # 示例：评估股票列表
    # 这里使用示例股票代码，实际使用时应该从CSV文件读取
    example_stocks = ['000001', '600000', '600036', '000002', '600519']
    
    print("=" * 60)
    print("价值投资系统 - 股票评估")
    print("=" * 60)
    
    # 评估股票
    results_df = system.evaluate_stock_list(example_stocks)
    
    if not results_df.empty:
        print("\n评估结果:")
        print(results_df[['stock_code', 'stock_name', 'target_return', 'PEG', 'can_buy']].to_string())
        
        # 选出最佳股票
        print("\n" + "=" * 60)
        print("选出最佳股票")
        print("=" * 60)
        best_stocks = system.select_best_stocks(example_stocks)
        
        if best_stocks:
            print(f"\n共选出 {len(best_stocks)} 只股票:")
            for stock in best_stocks:
                print(f"  - {stock['stock_code']} {stock['stock_name']} "
                      f"(目标收益率: {stock['target_return']:.2f}%, PEG: {stock['PEG']:.2f})")


if __name__ == "__main__":
    main()
