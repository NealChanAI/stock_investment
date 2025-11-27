# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
Get industry classification information through the API. Update frequency: every Monday. Return type: pandas DataFrame. Usage example.
"""

import baostock as bs
import pandas as pd

# Login to the system
lg = bs.login()
# Display login response information
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# Get industry classification data
"""
code: A-share stock code, sh or sz.+6-digit code, or index code, e.g., sh.601398. sh: Shanghai; sz: Shenzhen. Can be empty;
date: Query date in format XXXX-XX-XX, defaults to latest date if empty.

Parameter Name           Parameter Description
updateDate               Update date
code                     Security code
code_name                Security name
industry                 Industry
industryClassification   Industry classification type
"""
rs = bs.query_stock_industry()
# rs = bs.query_stock_basic(code_name="浦发银行")
print('query_stock_industry error_code:'+rs.error_code)
print('query_stock_industry respond  error_msg:'+rs.error_msg)

# Print result set
industry_list = []
while (rs.error_code == '0') & rs.next():
    # Get one record and append it to the list
    industry_list.append(rs.get_row_data())
result = pd.DataFrame(industry_list, columns=rs.fields)
# Output result set to csv file
result.to_csv("data/industry_data/industry_data.csv", encoding="gbk", index=False)
print(result)

# Logout from the system
bs.logout()
