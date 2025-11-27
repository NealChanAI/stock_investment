# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
"""

import baostock as bs
import pandas as pd

# Login to the system
lg = bs.login()
# Show login response info
print('login respond error_code:' + lg.error_code)
print('login respond error_msg:' + lg.error_msg)

# Get historical K-line data
# Detailed indicator parameters, see "Historical Data Indicator Parameters" section
"""
Parameter Name    Parameter Description               Remarks
date              Trading date                        Format: YYYY-MM-DD
code              Security code                       Format: sh.600000. sh: Shanghai, sz: Shenzhen
open              Opening price today                 Precision: 4 decimal places; Unit: RMB yuan
high              Highest price                       Precision: 4 decimal places; Unit: RMB yuan
low               Lowest price                        Precision: 4 decimal places; Unit: RMB yuan
close             Closing price today                 Precision: 4 decimal places; Unit: RMB yuan
preclose          Previous closing price              Precision: 4 decimal places; Unit: RMB yuan
volume            Trading volume                      Unit: shares
amount            Trading amount                      Precision: 4 decimal places; Unit: RMB yuan
adjustflag        Adjustment status                   No adjustment, forward adjustment, backward adjustment
turn              Turnover rate                       Precision: 6 decimal places; Unit: %
tradestatus       Trading status                      1: Normal trading  0: Suspended
pctChg            Percentage change                   Precision: 6 decimal places
peTTM             Rolling P/E ratio                   Precision: 6 decimal places
psTTM             Rolling P/S ratio                   Precision: 6 decimal places
pcfNcfTTM         Rolling P/CF ratio                  Precision: 6 decimal places
pbMRQ             P/B ratio                          Precision: 6 decimal places
isST              Is ST                              1: Yes, 0: No
"""
rs = bs.query_history_k_data_plus(
    "sh.601888",
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turnover,tradestatus,pct_change,pe_ttm,pb_mrq,ps_ttm,pcf_ncf_ttm,is_st",
    start_date='1990-12-19', end_date='2025-11-12',
    frequency="d", adjustflag="3"  # frequency="d" for daily k-line, adjustflag="3" means no adjustment
)
print('query_history_k_data_plus respond error_code:' + rs.error_code)
print('query_history_k_data_plus respond error_msg:' + rs.error_msg)

# Print result set
data_list = []
while (rs.error_code == '0') & rs.next():
    # Get one record and append it to the list
    data_list.append(rs.get_row_data())
result = pd.DataFrame(data_list, columns=[
    "date","code","open","high","low","close","preclose","volume","amount","adjustflag","turnover","tradestatus","pct_change","pe_ttm","pb_mrq","ps_ttm","pcf_ncf_ttm","is_st"
])

# Output result set to csv file
result.to_csv("data/history_k_data/601888.csv", encoding="gbk", index=False)
print(result)

# Logout from the system
bs.logout()