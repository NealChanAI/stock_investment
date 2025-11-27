# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
Get stock trading days information through API. You can set parameters to get data for specific start and end years.
SSE data available from 1990 to present.
"""

import baostock as bs
import pandas as pd

#### Login to the system ####
lg = bs.login()
# Display login response information
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

#### Get trading dates information ####
"""
Parameter Name   Description
calendar_date    Date
is_trading_day   Is trading day (0: non-trading day; 1: trading day)
"""
rs = bs.query_trade_dates(start_date="2017-01-01", end_date="2017-06-30")
print('query_trade_dates respond error_code:' + rs.error_code)
print('query_trade_dates respond  error_msg:' + rs.error_msg)

#### Print result set ####
data_list = []
while (rs.error_code == '0') & rs.next():
    # Get one record and append it to the list
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=rs.fields)

#### Output result set to csv file ####
result.to_csv("data/trade_dates/trade_dates.csv", encoding="gbk", index=False)
print(result)

#### Logout from the system ####
bs.logout()