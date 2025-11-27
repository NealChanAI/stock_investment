# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
Get quarterly growth ability information through API interface, can obtain data for the corresponding year and quarter through parameter settings, providing data from 2007 to the present.
"""

import baostock as bs
import pandas as pd

# Login to the system
lg = bs.login()
# Show login response information
print('login respond error_code:' + lg.error_code)
print('login respond  error_msg:' + lg.error_msg)

# Query growth ability data
"""
code: stock code, format is sh or sz. + 6 digits, or index code, e.g., sh.601398. sh: Shanghai; sz: Shenzhen. This parameter cannot be empty.
year: reporting year, defaults to current year if empty.
quarter: reporting quarter, optional, defaults to current quarter. If not empty, only 4 values are allowed: 1, 2, 3, 4.

Code explanations:
code        Stock code
pubDate     Date the company released the financial report
statDate    The last day of the statistic period for the financial report, e.g., 2017-03-31, 2017-06-30
YOYEquity   Year-on-year growth rate of net assets = (Current period net assets - Same period last year net assets) / |Same period last year net assets| * 100%
YOYAsset    Year-on-year growth rate of total assets = (Current period total assets - Same period last year total assets) / |Same period last year total assets| * 100%
YOYNI       Year-on-year growth rate of net profit = (Current period net profit - Same period last year net profit) / |Same period last year net profit| * 100%
YOYEPSBasic Year-on-year growth rate of basic EPS = (Current period basic EPS - Same period last year basic EPS) / |Same period last year basic EPS| * 100%
YOYPNI      Year-on-year growth rate of net profit attributable to parent company shareholders = (Current period attributable net profit - Same period last year attributable net profit) / |Same period last year attributable net profit| * 100%
"""

growth_list = []
rs_growth = bs.query_growth_data(code="sh.601888", year=2024, quarter=4)
while (rs_growth.error_code == '0') & rs_growth.next():
    growth_list.append(rs_growth.get_row_data())
result_growth = pd.DataFrame(growth_list, columns=rs_growth.fields)
# Print the result
print(result_growth)
# Output the result set to a csv file
result_growth.to_csv("data/growth_data/601888.csv", encoding="gbk", index=False)

# Logout from the system
bs.logout()