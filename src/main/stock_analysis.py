# -*- coding:utf-8 -*-

import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import os
import re 

STOCK_CODE = "sh.601888"


def baostock_login():
    lg = bs.login()
    if lg.error_code != '0':
        print('login respond error_code:' + lg.error_code)
        print('login respond  error_msg:' + lg.error_msg)
        return None
    return lg

def baostock_logout(lg):
    if lg is not None:
        bs.logout()
    return None



def get_stock_data(lg, stock_code):
    pass 


def get_last_trading_date_before(end_date):
    """
    获取指定日期之前最近的上一个交易日
    
    Args:
        end_date (str): 日期字符串，格式为 YYYY-MM-DD
    
    Returns:
        str: 上一个交易日的日期字符串，格式为 YYYY-MM-DD，如果出错返回 None
    """
    # 将字符串转换为日期对象
    try:
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    except ValueError:
        print(f'日期格式错误: {end_date}，应为 YYYY-MM-DD 格式')
        return None
    
    # 获取指定日期之前60天的交易日数据，确保能找到上一个交易日
    start_date = (end_date_obj - timedelta(days=60)).strftime('%Y-%m-%d')
    
    rs = bs.query_trade_dates(start_date=start_date, end_date=end_date)
    if rs.error_code != '0':
        print(f'query_trade_dates respond error_code: {rs.error_code}')
        print(f'query_trade_dates respond error_msg: {rs.error_msg}')
        return None
    
    # 获取所有交易日
    trading_dates = []
    while (rs.error_code == '0') & rs.next():
        row_data = rs.get_row_data()
        if row_data[1] == '1':  # is_trading_day == '1' 表示是交易日
            trading_date = row_data[0]  # calendar_date
            # 只保留小于等于 end_date 的交易日
            if trading_date <= end_date:
                trading_dates.append(trading_date)
    
    if not trading_dates:
        print(f'在 {end_date} 之前未找到交易日')
        return None
    
    # 返回最后一个交易日（即上一个交易日）
    return sorted(trading_dates)[-1]


def parse_period(period):
    """
    解析周期参数，支持 "10Y" 格式或整数格式
    
    Args:
        period: 周期，可以是字符串如 "10Y" 或整数如 10
    
    Returns:
        int: 年份数
    """
    if isinstance(period, int):
        return period
    elif isinstance(period, str):
        # 匹配 "10Y", "5Y" 等格式
        match = re.match(r'^(\d+)Y$', period.upper())
        if match:
            return int(match.group(1))
        else:
            # 如果不是 "XY" 格式，尝试直接转换为整数
            try:
                return int(period)
            except ValueError:
                print(f'无法解析周期参数: {period}，使用默认值10年')
                return 10
    else:
        print(f'周期参数类型错误: {type(period)}，使用默认值10年')
        return 10


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
    # 登录系统
    lg = bs.login()
    if lg.error_code != '0':
        print(f'login respond error_code: {lg.error_code}')
        print(f'login respond error_msg: {lg.error_msg}')
        return None
    
    try:
        # 解析周期参数
        period_years = parse_period(period)
        print(f'查询周期: {period_years}年')
        
        # 获取距离 end_date 最近的上一个交易日
        last_trading_date = get_last_trading_date_before(end_date)
        if last_trading_date is None:
            print('无法获取上一个交易日')
            return None
        
        print(f'结束日期: {end_date}，上一个交易日: {last_trading_date}')
        
        # 计算N年前的日期（使用年份减去，更准确）
        last_date = datetime.strptime(last_trading_date, '%Y-%m-%d')
        start_year = last_date.year - period_years
        # 处理闰年2月29日的情况
        try:
            start_date = datetime(start_year, last_date.month, last_date.day).strftime('%Y-%m-%d')
        except ValueError:
            # 如果目标年份不是闰年且日期是2月29日，则使用2月28日
            start_date = datetime(start_year, last_date.month, 28).strftime('%Y-%m-%d')
        
        print(f'查询日期范围: {start_date} 至 {last_trading_date}')
        
        # 查询历史K线数据，只获取 date 和 peTTM 字段
        rs = bs.query_history_k_data_plus(
            stock_code,
            "date,peTTM",  # 只获取日期和滚动市盈率
            start_date=start_date,
            end_date=last_trading_date,
            frequency="d",  # 日线数据
            adjustflag="3"  # 不复权
        )
        
        if rs.error_code != '0':
            print(f'query_history_k_data_plus respond error_code: {rs.error_code}')
            print(f'query_history_k_data_plus respond error_msg: {rs.error_msg}')
            return None
        
        # 处理结果集
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            print('未获取到数据')
            return None
        
        # 转换为DataFrame
        result = pd.DataFrame(data_list, columns=rs.fields)
        
        # 将 peTTM 转换为数值类型
        result['peTTM'] = pd.to_numeric(result['peTTM'], errors='coerce')
        
        # 按日期排序
        result = result.sort_values('date').reset_index(drop=True)
        
        print(f'成功获取 {len(result)} 条 peTTM 数据')
        return result
        
    finally:
        # 登出系统
        bs.logout() 




def get_history_ps_data(stock_code, period):
    """
    Get the historical P/S ratio data for the given stock code and period.
    """
    pass 

def main():
    """主函数，测试获取历史 peTTM 数据"""
    # 测试获取历史 peTTM 数据
    stock_code = STOCK_CODE
    # 使用当前日期作为结束日期
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f'正在获取股票 {stock_code} 的历史 peTTM 数据...')
    print(f'结束日期: {end_date}')
    print(f'周期: 10Y\n')
    
    result = get_history_pettm_data(stock_code, end_date, period="10Y")
    
    if result is not None:
        print('\n前5条数据:')
        print(result.head())
        print('\n后5条数据:')
        print(result.tail())
        print(f'\n数据统计:')
        print(result.describe())
        
        # 保存到CSV文件
        output_dir = "data/history_pettm"
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/{stock_code.replace('.', '_')}_pettm_10y.csv"
        result.to_csv(output_file, encoding="gbk", index=False)
        print(f'\n数据已保存到: {output_file}')
    else:
        print('获取数据失败') 



if __name__ == "__main__":
    main()
