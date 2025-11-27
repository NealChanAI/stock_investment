# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
"""

import baostock as bs
import pandas as pd

# Login to the system
lg = bs.login()
# Show login response information
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# Query quarterly profitability indicators
"""
code: stock code, format is sh or sz. + 6 digits, or index code, e.g., sh.601398. sh: Shanghai; sz: Shenzhen. This parameter cannot be empty.
year: reporting year, defaults to current year if empty.
quarter: reporting quarter, optional, defaults to current quarter. If not empty, only 4 values are allowed: 1, 2, 3, 4.

code        Stock code
pubDate     Date the company released the financial report
statDate    The last day of the statistic period for the financial report, e.g., 2017-03-31, 2017-06-30
roeAvg      Return on average equity (%) = Net profit attributable to shareholders of the parent company / [(Opening equity attributable to parent company’s shareholders + Closing equity attributable to parent company’s shareholders) / 2] * 100%
npMargin    Net profit margin (%) = Net profit / Operating revenue * 100%
gpMargin    Gross profit margin (%) = Gross profit / Operating revenue * 100% = (Operating revenue - Cost of revenue) / Operating revenue * 100%
netProfit   Net profit (Yuan)
epsTTM      Earnings per share (TTM) = Net profit attributable to shareholders of the parent company (TTM) / Latest total share capital
MBRevenue   Main business revenue (Yuan)
totalShare  Total share capital
liqaShare   Total outstanding shares
"""
profit_list = []
rs_profit = bs.query_profit_data(code="sh.601888", year=2024, quarter=4)
while (rs_profit.error_code == '0') & rs_profit.next():
    profit_list.append(rs_profit.get_row_data())
result_profit = pd.DataFrame(profit_list, columns=rs_profit.fields)
# Print result
print(result_profit)
# Output result set to csv file
result_profit.to_csv("data/profit_data/601888.csv", encoding="gbk", index=False)

# Logout from the system
bs.logout()