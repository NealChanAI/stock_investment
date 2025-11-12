import baostock as bs
import pandas as pd

# Login to the system
lg = bs.login()
# Show login response info
print('login respond error_code:' + lg.error_code)
print('login respond error_msg:' + lg.error_msg)

# Get historical K-line data
# Detailed indicator parameters, see "Historical Data Indicator Parameters" section
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