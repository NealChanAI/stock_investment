# -*- coding:utf-8 -*-

import baostock as bs
import pandas as pd
from datetime import datetime, timedelta
import os 
import re
import sys
from io import StringIO
import matplotlib.pyplot as plt
import matplotlib
# 设置中文字体
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
matplotlib.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号 

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
    
    # 获取指定日期之前30天的交易日数据，确保能找到上一个交易日
    start_date = (end_date_obj - timedelta(days=30)).strftime('%Y-%m-%d')
    
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
    lg = baostock_login()
    
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
        baostock_logout(lg)


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
    
    print(f'peTTM均值: {mean_value:.4f} (基于 {len(valid_pettm)} 条有效数据)')
    return mean_value


def get_sz50_stocks():
    """
    获取上证50成分股列表
    
    Returns:
        pandas.DataFrame: 包含股票代码和股票名称的DataFrame，如果获取失败返回None
    """
    lg = baostock_login()
    if lg is None:
        return None
    
    try:
        # 获取上证50成分股
        rs = bs.query_sz50_stocks()
        if rs.error_code != '0':
            print(f'query_sz50_stocks error_code: {rs.error_code}')
            print(f'query_sz50_stocks error_msg: {rs.error_msg}')
            return None
        
        # 获取结果集
        sz50_stocks = []
        while (rs.error_code == '0') & rs.next():
            sz50_stocks.append(rs.get_row_data())
        
        if not sz50_stocks:
            print('未获取到上证50成分股数据')
            return None
        
        # 转换为DataFrame
        result = pd.DataFrame(sz50_stocks, columns=rs.fields)
        print(f'成功获取 {len(result)} 只上证50成分股')
        return result
        
    finally:
        baostock_logout(lg)


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
        # print(f'{stock_code}: 计算peTTM均值失败')
        return None
    
    # get_pettm_mean 已经返回Python原生float类型，无需再次转换
    
    # 返回结果
    result_dict = {
        'last_trading_date': str(last_trading_date),  # 确保日期是字符串
        'last_pettm': last_pettm,
        'mean_pettm': mean_pettm
    }
    
    # print(f'{stock_code}: 最近交易日={last_trading_date}, peTTM={last_pettm:.4f}, 均值={mean_pettm:.4f}')
    
    return result_dict


def analyze_sz50_stocks_pettm(period="10Y", end_date=None):
    """
    分析上证50成分股，找出最新peTTM小于平均peTTM的股票
    
    Args:
        period: 时间跨度，可以是字符串如 "10Y" 或整数如 10，默认 "10Y"（10年）
        end_date (str): 结束日期，格式为 YYYY-MM-DD，如果为 None 则默认使用今天
    
    Returns:
        pandas.DataFrame: 包含以下列的DataFrame：
            - code (str): 股票代码
            - code_name (str): 股票名称
            - last_pettm (float): 最新peTTM
            - mean_pettm (float): 平均peTTM
            - last_trading_date (str): 最近交易日
    """
    print('='*60)
    print('开始分析上证50成分股的peTTM...')
    print('='*60)
    
    # 获取上证50股票列表
    print('\n1. 获取上证50成分股列表...')
    sz50_stocks = get_sz50_stocks()
    if sz50_stocks is None or sz50_stocks.empty:
        return None
    
    print(f'共获取 {len(sz50_stocks)} 只股票')
    
    # 存储结果
    results = []
    
    print(f'\n2. 开始计算每只股票的peTTM...')
    print(f'共需处理 {len(sz50_stocks)} 只股票\n')
    
    for idx, row in sz50_stocks.iterrows():
        stock_code = row['code']
        stock_name = row['code_name']
        
        print(f'[{idx+1}/{len(sz50_stocks)}] 处理股票: {stock_code} ({stock_name})')
        
        # 获取peTTM数据
        pettm_data = get_current_pettm_and_mean(stock_code, period=period, end_date=end_date)
        
        if pettm_data is None:
            print(f'  └─ 跳过: 无法获取peTTM数据\n')
            continue
        
        last_pettm = pettm_data['last_pettm']
        if last_pettm < 0:
            print(f'  └─ ✗ 不符合: 最新peTTM={last_pettm:.4f} >= 平均peTTM={mean_pettm:.4f}\n')
        mean_pettm = pettm_data['mean_pettm']
        
        # 判断最新peTTM是否小于平均peTTM
        if last_pettm < mean_pettm:
            results.append({
                'code': stock_code,
                'code_name': stock_name,
                'last_pettm': last_pettm,
                'mean_pettm': mean_pettm,
                'last_trading_date': pettm_data['last_trading_date'],
                'diff': mean_pettm - last_pettm,  # 差值
                'diff_pct': (mean_pettm - last_pettm) / last_pettm * 100  # 差值百分比
            })
            print(f'  └─ ✓ 符合条件: 最新peTTM={last_pettm:.4f} < 平均peTTM={mean_pettm:.4f}\n')
        else:
            print(f'  └─ ✗ 不符合: 最新peTTM={last_pettm:.4f} >= 平均peTTM={mean_pettm:.4f}\n')
    
    # 转换为DataFrame
    if not results:
        print('\n未找到符合条件的股票')
        return None
    
    result_df = pd.DataFrame(results)
    
    # 按差值百分比排序（从大到小）
    result_df = result_df.sort_values('diff_pct', ascending=False).reset_index(drop=True)
    
    print('='*60)
    print(f'分析完成！共找到 {len(result_df)} 只符合条件的股票')
    print('='*60)
    
    return result_df


def get_month_end_pettm_data(pettm_df):
    """
    从peTTM数据中提取每个月月末的数据，并确保最后一条数据是原始数据集的最后一条
    
    Args:
        pettm_df (pandas.DataFrame): 包含 date 和 peTTM 列的 DataFrame
    
    Returns:
        pandas.DataFrame: 包含每月月末 peTTM 数据的 DataFrame，最后一条是原始数据的最后一条
    """
    if pettm_df is None or pettm_df.empty:
        print('输入的peTTM数据为空')
        return None
    
    # 复制数据，避免修改原始数据
    df = pettm_df.copy()
    
    # 将date列转换为datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 确保按日期排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 获取原始数据的最后一条记录
    last_record = df.iloc[-1:].copy()
    last_date = last_record['date'].iloc[0]
    last_date_str = last_record['date'].iloc[0].strftime('%Y-%m-%d')
    
    # 添加年月列，用于分组
    df['year_month'] = df['date'].dt.to_period('M')
    
    # 获取最后一条数据之前的所有数据（不包括最后一条）
    df_before_last = df[df['date'] < last_date].copy()
    
    # 如果只有一条数据，直接返回
    if df_before_last.empty:
        return last_record[['date', 'peTTM']]
    
    # 按年月分组，获取每组的最后一条记录（月末数据）
    df_before_last_month_end = df_before_last.groupby('year_month').last().reset_index(drop=True)
    
    # 选择需要的列
    month_end_data = df_before_last_month_end[['date', 'peTTM']].copy()
    
    # 将最后一条数据添加到结果中（确保最后一条数据是原始数据的最后一条）
    result = pd.concat([month_end_data, last_record[['date', 'peTTM']]], ignore_index=True)
    
    # 去重（如果最后一条数据恰好是它所在月份的月末数据，可能会有重复）
    # 保留最后一个（即原始数据的最后一条），确保最后一条数据一定是原始数据的最后一条
    result = result.drop_duplicates(subset=['date'], keep='last')
    
    # 确保按日期排序
    result = result.sort_values('date').reset_index(drop=True)
    
    print(f'提取了 {len(result)} 条月末数据（最后一条是原始数据的最后一条: {last_date_str}）')
    return result


def plot_pettm_line(pettm_df, stock_code, title=None, save_path=None):
    """
    绘制peTTM数据的折线图
    
    Args:
        pettm_df (pandas.DataFrame): 包含 date 和 peTTM 列的 DataFrame
        stock_code (str): 股票代码，用于标题和文件名
        title (str): 图表标题，如果为None则自动生成
        save_path (str): 保存路径，如果为None则不保存
    
    Returns:
        None
    """
    if pettm_df is None or pettm_df.empty:
        print('数据为空，无法绘图')
        return
    
    # 复制数据，避免修改原始数据
    df = pettm_df.copy()
    
    # 将date列转换为datetime类型
    df['date'] = pd.to_datetime(df['date'])
    
    # 确保按日期排序
    df = df.sort_values('date').reset_index(drop=True)
    
    # 过滤掉peTTM为空或NaN的数据
    df = df.dropna(subset=['peTTM'])
    
    if df.empty:
        print('没有有效的peTTM数据可以绘图')
        return
    
    # 创建图表
    plt.figure(figsize=(14, 8))
    
    # 绘制折线图
    plt.plot(df['date'], df['peTTM'], marker='o', markersize=4, linewidth=1.5, label='peTTM')
    
    # 设置标题
    if title is None:
        title = f'{stock_code} 历史 peTTM 趋势图'
    plt.title(title, fontsize=16, fontweight='bold')
    
    # 设置x轴标签
    plt.xlabel('日期', fontsize=12)
    
    # 设置y轴标签
    plt.ylabel('peTTM (滚动市盈率)', fontsize=12)
    
    # 添加网格
    plt.grid(True, alpha=0.3, linestyle='--')
    
    # 旋转x轴标签，避免重叠
    plt.xticks(rotation=45)
    
    # 添加图例
    plt.legend(fontsize=10)
    
    # 调整布局，避免标签被裁剪
    plt.tight_layout()
    
    # 保存图片
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        print(f'图表已保存到: {save_path}')
    
    # 显示图表
    plt.show()


def main():
    """主函数，测试获取历史 peTTM 数据并绘制图表"""
    # 测试获取历史 peTTM 数据
    stock_code = STOCK_CODE
    # # 使用当前日期作为结束日期
    # end_date = datetime.now().strftime('%Y-%m-%d')
    
    # print(f'正在获取股票 {stock_code} 的历史 peTTM 数据...')
    # print(f'结束日期: {end_date}')
    # print(f'周期: 10Y\n')
    
    # # 获取历史peTTM数据
    # result = get_history_pettm_data(stock_code, end_date, period="10Y")
    # pettm_mean = get_pettm_mean(result)
    # print(f'peTTM均值: {pettm_mean:.4f}')

    # if result is not None:
    #     print('\n前5条数据:')
    #     print(result.head())
    #     print('\n后5条数据:')
    #     print(result.tail())
    #     print(f'\n数据统计:')
    #     print(result.describe())
        
        
    #     # 提取每月月末数据
    #     print('\n' + '='*50)
    #     print('提取每月月末数据...')
    #     month_end_data = get_month_end_pettm_data(result)
        
    #     if month_end_data is not None:
    #         print('\n月末数据前5条:')
    #         print(month_end_data.head())
    #         print('\n月末数据后5条:')
    #         print(month_end_data.tail())
            
    #         # 绘制折线图
    #         print('\n' + '='*50)
    #         print('绘制折线图...')
    #         plot_pettm_line(month_end_data, stock_code, 
    #                       title=f'{stock_code} 历史 peTTM 月末数据趋势图',
    #                       save_path=None)
    #     else:
    #         print('提取月末数据失败')
    # else:
    #     print('获取数据失败') 

    # 分析上证50成分股，找出最新peTTM小于平均peTTM的股票
    result_df = analyze_sz50_stocks_pettm(period="10Y", end_date=None)
    
    if result_df is not None:
        print('\n符合条件的股票列表:')
        print('='*60)
        print(result_df[['code', 'code_name', 'last_pettm', 'mean_pettm', 'diff_pct']].to_string(index=False))
        print('='*60)
    else:
        print('分析失败或未找到符合条件的股票')

if __name__ == "__main__":
    main()
