# -*- coding: utf-8 -*-

"""
将 `data/self_selected_stocks.raw.csv` 转换为与 `data/sz50_stocks.csv` 相同的格式：

目标字段：updateDate, code, code_name

- 不修改源文件 `self_selected_stocks.raw.csv`
- 在同目录下生成新文件：`data/self_selected_stocks.csv`
"""

import os
from datetime import datetime
import argparse
from typing import Optional

import pandas as pd


# 默认的源文件和输出文件路径（可按需修改）
DEFAULT_RAW_PATH = os.path.join("data", "self_selected_stocks.raw.csv")
DEFAULT_OUT_PATH = os.path.join("data", "self_selected_stocks.csv")


def add_stock_prefix(stock_code: str) -> str:
    """
    给6位数字的股票编码加地区前缀：
        - 以60打头：上证（sh）
        - 以68打头：上证科创板（sh）
        - 以00打头：深证（sz）
        - 以30打头：深证创业板（sz）
        - 其它：默认 sz
    """
    stock_code = str(stock_code).strip()
    # 补全为6位数字
    if stock_code.isdigit() and len(stock_code) < 6:
        stock_code = stock_code.zfill(6)

    if not (stock_code.isdigit() and len(stock_code) == 6):
        raise ValueError(f"股票代码格式不正确: {stock_code}")

    if stock_code.startswith("60") or stock_code.startswith("68"):
        prefix = "sh"
    elif stock_code.startswith("00") or stock_code.startswith("30"):
        prefix = "sz"
    else:
        prefix = "sz"
    return f"{prefix}.{stock_code}"


def transform_self_selected_stocks(
    raw_path: str,
    out_path: str,
    update_date: Optional[str] = None,
) -> None:
    """
    将原始自选股列表转换为标准格式 CSV。

    原始文件格式（无表头）：
        code, name

    目标文件格式（有表头）：
        updateDate, code, code_name
    """
    if update_date is None:
        update_date = datetime.now().strftime("%Y-%m-%d")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"未找到原始文件: {raw_path}")

    # 读取原始文件：无表头，每行形如 "603259,药明康德"
    df_raw = pd.read_csv(
        raw_path,
        header=None,
        names=["code", "code_name"],
        dtype={"code": str, "code_name": str},
        encoding="utf-8",
    )

    # 补全代码并添加前缀
    df_raw["code"] = df_raw["code"].astype(str).str.strip()
    df_raw["code"] = df_raw["code"].str.zfill(6)
    df_raw["code"] = df_raw["code"].apply(add_stock_prefix)

    # 构造目标 DataFrame
    df_out = df_raw.copy()
    df_out.insert(0, "updateDate", update_date)
    df_out = df_out[["updateDate", "code", "code_name"]]

    # 保存到新文件（不覆盖原始文件）
    df_out.to_csv(out_path, index=False, encoding="utf-8")
    print(f"转换完成，已生成文件：{out_path}")


def main():
    """
    命令行入口：

    示例：
        python src/test/self_selected_stocks_format.py \
            --raw data/self_selected_stocks.raw.csv \
            --out data/self_selected_stocks.csv \
            --date 2025-12-01

    参数说明：
        --raw / -r   源CSV路径（可选，默认 DEFAULT_RAW_PATH）
        --out / -o   输出CSV路径（可选，默认 DEFAULT_OUT_PATH）
        --date / -d  updateDate（可选，不传则默认为今天）
    """
    parser = argparse.ArgumentParser(
        description="将自选股原始CSV转换为标准格式（updateDate,code,code_name）"
    )
    parser.add_argument(
        "--raw",
        "-r",
        required=False,
        default=DEFAULT_RAW_PATH,
        help=f"原始CSV文件路径，默认 {DEFAULT_RAW_PATH}",
    )
    parser.add_argument(
        "--out",
        "-o",
        required=False,
        default=DEFAULT_OUT_PATH,
        help=f"输出CSV文件路径，默认 {DEFAULT_OUT_PATH}",
    )
    parser.add_argument(
        "--date",
        "-d",
        required=False,
        help="updateDate，格式 YYYY-MM-DD；不传则默认今天",
    )

    args = parser.parse_args()

    transform_self_selected_stocks(
        raw_path=args.raw,
        out_path=args.out,
        update_date=args.date,
    )


if __name__ == "__main__":
    main()


