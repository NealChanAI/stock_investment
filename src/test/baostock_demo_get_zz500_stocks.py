# -*- coding:utf-8 -*-

"""
reference: http://baostock.com/mainContent?file=pythonAPI.md
通过API接口获取中证500成分股信息，更新频率：每周一更新。
"""

import baostock as bs
import pandas as pd

# 登陆系统
lg = bs.login()
# 显示登陆返回信息
print('login respond error_code:'+lg.error_code)
print('login respond  error_msg:'+lg.error_msg)

# 获取中证500成分股
rs = bs.query_zz500_stocks()
print('query_zz500 error_code:'+rs.error_code)
print('query_zz500  error_msg:'+rs.error_msg)

# 打印结果集
zz500_stocks = []
while (rs.error_code == '0') & rs.next():
    # 获取一条记录，将记录合并在一起
    zz500_stocks.append(rs.get_row_data())
result = pd.DataFrame(zz500_stocks, columns=rs.fields)
# 结果集输出到csv文件
import os
# 确保项目根目录下的data目录存在
data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')
os.makedirs(data_dir, exist_ok=True)
result.to_csv(os.path.join(data_dir, "zz500_stocks.csv"), encoding="utf-8", index=False)

print(result)

# 登出系统
bs.logout()