import akshare as ak
from akshare.stock.stock_zh_comparison_em import stock_zh_valuation_comparison_em


def main():
    stock_zh_valuation_comparison_em_df = stock_zh_valuation_comparison_em(symbol="SH601888")
    print(stock_zh_valuation_comparison_em_df)


if __name__ == "__main__":
    main()

