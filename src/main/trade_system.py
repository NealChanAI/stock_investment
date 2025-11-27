from pathlib import Path
from datetime import datetime

import pandas as pd

from zhongmian_analysis import get_pe_info


def load_all_codes(csv_path: Path):
    """读取csv中的股票代码列表."""
    if not csv_path.exists():
        raise FileNotFoundError(f"找不到文件: {csv_path}")

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        df = pd.read_csv(csv_path, encoding="gbk")

    if "code" not in df.columns:
        raise ValueError(f"文件缺少code列: {csv_path}")

    if "code_name" not in df.columns:
        df["code_name"] = ""

    df["code"] = df["code"].astype(str).str.strip()
    df["code_name"] = df["code_name"].astype(str).str.strip()

    records = (
        df[["code", "code_name"]]
        .dropna(subset=["code"])
        .drop_duplicates(subset="code", keep="first")
    )
    return records.to_dict("records")


def save_results(result_rows, output_dir: Path):
    """将结果保存到CSV，优先使用utf-8-sig，失败时回退到gbk."""
    if not result_rows:
        print("没有可保存的结果")
        return

    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = output_dir / f"sz50_pe_info_{timestamp}.csv"

    result_df = pd.DataFrame(result_rows)
    try:
        result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"结果已保存至 {output_path} (utf-8-sig)")
    except UnicodeEncodeError:
        fallback_path = output_path.with_suffix(".gbk.csv")
        result_df.to_csv(fallback_path, index=False, encoding="gbk")
        print(f"utf-8 编码失败，改用 GBK 保存至 {fallback_path}")


def main():
    project_root = Path(__file__).resolve().parents[2]
    csv_path = project_root / "data" / "sz50_stocks.csv"

    try:
        stock_infos = load_all_codes(csv_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"加载股票列表失败: {exc}")
        return

    if not stock_infos:
        print("未在列表中找到任何股票代码")
        return

    results = []
    for info in stock_infos:
        code = info["code"]
        code_name = info.get("code_name", "")
        try:
            pe_info = get_pe_info(code)
        except Exception as exc:  # noqa: BLE001
            print(f"[{code} {code_name}] 获取PE信息失败: {exc}")
            continue

        if pe_info is None:
            print(f"[{code} {code_name}] 无法获取PE信息")
            continue

        print(f"[{code} {code_name}] target_date={pe_info['target_date']}, "
              f"peTTM={pe_info['pettm_at_date']}, "
              f"mean_10Y={pe_info['mean_pettm_10y']}, "
              f"mean_5Y={pe_info['mean_pettm_5y']}")

        results.append(
            {
                "date": pe_info["target_date"],
                "code": code,
                "code_name": code_name,
                "peTTM": pe_info["pettm_at_date"],
                "mean_peTTM_5Y": pe_info["mean_pettm_5y"],
                "mean_peTTM_10Y": pe_info["mean_pettm_10y"],
            }
        )

    if results:
        output_dir = project_root / "data" / "pe_info"
        save_results(results, output_dir)
    else:
        print("未获取到任何有效的PE结果，跳过保存。")


if __name__ == "__main__":
    main()