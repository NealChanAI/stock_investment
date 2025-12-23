"""
zhongmian stock analysis
"""

import pandas as pd
import baostock as bs
import akshare as ak
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
    lg = baostock_login()
    
    try:
        # 解析周期参数
        period_years = parse_period(period)
        # print(f'查询周期: {period_years}年')
        
        # 获取距离 end_date 最近的上一个交易日
        last_trading_date = get_last_trading_date_before(end_date)
        if last_trading_date is None:
            print('无法获取上一个交易日')
            return None
        
        # print(f'结束日期: {end_date}，上一个交易日: {last_trading_date}')
        
        # 计算N年前的日期（使用年份减去，更准确）
        last_date = datetime.strptime(last_trading_date, '%Y-%m-%d')
        start_year = last_date.year - period_years
        # 处理闰年2月29日的情况
        try:
            start_date = datetime(start_year, last_date.month, last_date.day).strftime('%Y-%m-%d')
        except ValueError:
            # 如果目标年份不是闰年且日期是2月29日，则使用2月28日
            start_date = datetime(start_year, last_date.month, 28).strftime('%Y-%m-%d')
        
        # print(f'查询日期范围: {start_date} 至 {last_trading_date}')
        
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
        
        # print(f'成功获取 {len(result)} 条 peTTM 数据')
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
    
    # pbMRQ统计
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

    return {
        'stock_code': stock_code,
        'target_date': target_date,
        'pettm_at_date': float(pettm_at_date),
        'mean_pettm_10y': mean_pettm_10y,
        'mean_pettm_5y': mean_pettm_5y,
        'pbmrq_at_date': float(pbmrq_at_date),
        'mean_pbmrq_10y': mean_pbmrq_10y,
        'mean_pbmrq_5y': mean_pbmrq_5y
    }


def get_recent_predict_peTTM(stock_code):
    """
    get recent predict peTTM from akshare
    """
    report_df = ak.stock_research_report_em(symbol=stock_code)
    report_df = report_df[['股票代码', '股票简称', '2025-盈利预测-市盈率', '2026-盈利预测-市盈率', '2027-盈利预测-市盈率', '机构', '报告PDF链接', '日期']]
    report_df.columns = ['stock_code', 'stock_name', 'predict_peTTM_2025', 'predict_peTTM_2026', 'predict_peTTM_2027', 'institution', 'report_pdf_link', 'date']
    report_df = report_df.sort_values(by='date', ascending=False).reset_index(drop=True)
    
    # 根据日期对数据进行分组, 相隔超过30天的则为两个分组
    dates_lst = report_df['date'].to_list()
    idx = -1 
    for i in range(len(dates_lst)):
        if i == 0:
            continue
        if (dates_lst[i-1] - dates_lst[i]).days > 30:
            idx = i
            break
    
    report_df = report_df.iloc[:idx]
    # 剔除掉predict_peTTM_2025~2027中有任一数据为空的行
    report_df = report_df.dropna(subset=['predict_peTTM_2025', 'predict_peTTM_2026', 'predict_peTTM_2027']).reset_index(drop=True)
    
    def calculate_e_growth_rate(row):
        """计算每股净利润增长率，只有当两个预测市盈率都是正数时才进行开平方"""
        pe2025 = row['predict_peTTM_2025']
        pe2027 = row['predict_peTTM_2027']
        
        # 检查是否为复数或负数
        if isinstance(pe2025, complex) or isinstance(pe2027, complex):
            return float('nan')
        
        # 转换为浮点数并检查是否为正数
        try:
            pe2025_val = float(pe2025)
            pe2027_val = float(pe2027)
        except (ValueError, TypeError):
            return float('nan')
        
        # 只有当两个值都是正数时才进行开平方
        if pe2025_val > 0 and pe2027_val > 0:
            ratio = pe2025_val / pe2027_val
            # ratio 一定大于 0，因为两个除数都是正数
            result = (ratio ** 0.5) - 1
            # 确保结果是实数
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
    info_columns = [
        'institution', 'date', 'predict_peTTM_2025', 'predict_peTTM_2026', 'predict_peTTM_2027', 'e_growth_rate', 'report_pdf_link'
    ]
    def row_to_str(row):
        return '  '.join([
            str(row.get(col, "")) if row.get(col, "") is not None else "" for col in info_columns
        ])
    report_infos_str = '\n'.join([row_to_str(row) for _, row in report_df.iterrows()])

    # 获取股票所属行业信息
    industry = ""
    try:
        stock_detail = ak.stock_individual_info_em(symbol=stock_code)
        # 从 item/value 结构中提取“行业”
        industry_series = stock_detail.loc[stock_detail["item"] == "行业", "value"]
        if not industry_series.empty:
            industry = str(industry_series.iloc[0])
    except Exception:
        # 行业获取失败时保持为空字符串，避免中断主流程
        industry = ""

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
    predict_g_res = get_recent_predict_peTTM(sim_stock_code)
    merge_res = get_merge_info(pe_res, predict_g_res)
    print(merge_res)
    return merge_res
    

def main():
    stock_code = 'sh.601888'
    get_stock_info(stock_code)

def test():
    res = get_recent_predict_peTTM("601888")
    print(res)

if __name__ == "__main__":
    main()
    # test()
    
