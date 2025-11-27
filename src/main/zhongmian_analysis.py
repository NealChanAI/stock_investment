"""
zhongmian stock analysis
"""

import pandas as pd
from stock_analysis import get_current_pettm_and_mean
import baostock as bs
from datetime import datetime, timedelta


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
    
    # 获取指定日期之前30天的交易日数据，确保能找到上一个交易日
    start_date = (end_date_obj - timedelta(days=30)).strftime('%Y-%m-%d')
    
    lg = baostock_login()

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
        return None
    
    baostock_logout(lg)
    # 返回最后一个交易日（即上一个交易日）
    return sorted(trading_dates)[-1]

def get_trading_date(start_date='2018-01-01', end_date=None):
    """
    获取指定日期范围内的交易日
    Args:
        start_date (str): 开始日期，格式为 YYYY-MM-DD
        end_date (str): 结束日期，格式为 YYYY-MM-DD，如果为 None 则默认使用今天
    Returns:
        list: 交易日列表
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')

    lg = baostock_login()
    try:
        # 查询交易日数据
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
        
        return trading_dates
        
    finally:
        baostock_logout(lg)


def get_history_pettm_data(stock_code, start_date='2000-01-01', end_date=None):
    """
    获取sotck的历史peTTM数据
    """
    lg = baostock_login()
    try:
        rs = bs.query_history_k_data_plus(
                stock_code,
                "date,code,peTTM,pbMRQ",  # 获取peTTM与pbMRQ
                start_date=start_date,
                end_date=end_date,
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
        
        # 将 peTTM、pbMRQ 转换为数值类型
        result['peTTM'] = pd.to_numeric(result['peTTM'], errors='coerce')
        result['pbMRQ'] = pd.to_numeric(result['pbMRQ'], errors='coerce')
        
        # 按日期排序
        result = result.sort_values('date').reset_index(drop=True)
        
        print(f'成功获取 {len(result)} 条 peTTM 数据')
        return result
    
    finally:
        baostock_logout(lg)


def get_pe_info(stock_code, target_date=None, period=["10Y", "5Y"]):
    """
    获取指定交易日的peTTM和以改天为节点往前推N年的平均peTTM
    """
    if target_date is None:
        target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    last_trading_date = get_last_trading_date_before(target_date)
    if last_trading_date is None:
        return None
    
    pettm_df = get_history_pettm_data(stock_code, start_date='2010-01-01', end_date=last_trading_date)
    if pettm_df is None:
        return 
    
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
    
    # 计算最近10年的开始日期（更准确的年份计算）
    start_year_10y = target_date_obj.year - 10
    try:
        start_date_10y = datetime(start_year_10y, target_date_obj.month, target_date_obj.day)
    except ValueError:
        # 处理闰年2月29日的情况
        start_date_10y = datetime(start_year_10y, target_date_obj.month, 28)
    
    # 筛选最近10年的数据
    # print(f'筛选最近10年的数据: {start_date_10y} 至 {target_date_obj}')
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
        print(f'最近10年平均peTTM: {mean_pettm_10y:.4f} (基于 {len(valid_pettm_10y)} 条有效数据)')
    
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
        print(f'最近5年平均peTTM: {mean_pettm_5y:.4f} (基于 {len(valid_pettm_5y)} 条有效数据)')
    
    # pbMRQ统计
    valid_pbmrq_10y = pettm_10y['pbMRQ'].dropna()
    if valid_pbmrq_10y.empty:
        mean_pbmrq_10y = None
        print(f'最近10年没有有效的pbMRQ数据')
    else:
        mean_pbmrq_10y = float(valid_pbmrq_10y.mean())
        print(f'最近10年平均pbMRQ: {mean_pbmrq_10y:.4f} (基于 {len(valid_pbmrq_10y)} 条有效数据)')

    valid_pbmrq_5y = pettm_5y['pbMRQ'].dropna()
    if valid_pbmrq_5y.empty:
        mean_pbmrq_5y = None
        print(f'最近5年没有有效的pbMRQ数据')
    else:
        mean_pbmrq_5y = float(valid_pbmrq_5y.mean())
        print(f'最近5年平均pbMRQ: {mean_pbmrq_5y:.4f} (基于 {len(valid_pbmrq_5y)} 条有效数据)')

    return {
        'target_date': target_date,
        'pettm_at_date': float(pettm_at_date),
        'mean_pettm_10y': mean_pettm_10y,
        'mean_pettm_5y': mean_pettm_5y,
        'pbmrq_at_date': float(pbmrq_at_date),
        'mean_pbmrq_10y': mean_pbmrq_10y,
        'mean_pbmrq_5y': mean_pbmrq_5y
    }


def main():
    stock_code = "sh.601888"
    res = get_pe_info(stock_code)
    print(res)


if __name__ == "__main__":
    main()
