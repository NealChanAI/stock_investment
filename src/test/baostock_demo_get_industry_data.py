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
import os
# 确保项目根目录下的data目录存在
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')
os.makedirs(data_dir, exist_ok=True)
result.to_csv(os.path.join(data_dir, "industry_data.csv"), encoding="utf-8", index=False)
print(result)

# Logout from the system
bs.logout()
