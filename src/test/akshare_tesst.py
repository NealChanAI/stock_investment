from re import S
import akshare as ak
import requests


def _main():
    stock_research_report_em_df = ak.stock_research_report_em(symbol="601888")
    print(stock_research_report_em_df.columns)
    print(stock_research_report_em_df.head(2))
    print(stock_research_report_em_df[['行业']])


def main():
    df = ak.stock_gdfx_top_10_em(symbol='SH600436', date='20250630')
    print(df.columns)
    print(df.head(10))

    # stock_board_industry_summary_ths_df = ak.stock_board_industry_summary_ths()
    # print(stock_board_industry_summary_ths_df['板块'].to_list())

if __name__ == "__main__":
    _main()
