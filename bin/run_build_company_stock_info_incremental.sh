#!/bin/bash
# company_stock_info 日增更新：只拉取缺失日期的 PE/PB、股价等数据
# 用法：
#   ./bin/run_build_company_stock_info_incremental.sh
#   ./bin/run_build_company_stock_info_incremental.sh --stock-list data/self_selected_stocks.csv
#   ./bin/run_build_company_stock_info_incremental.sh --skip-industry --skip-predict  # 仅更新日线，最快

set -eu

ROOT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "${ROOT_DIR}"

# 加载工具函数（若存在）
if [[ -f "${ROOT_DIR}/bin/utils.sh" ]]; then
  source "${ROOT_DIR}/bin/utils.sh"
else
  echo_info() { echo -e "\033[32m[INFO] \033[0m$*"; }
  echo_warn() { echo -e "\033[33m[WARN] \033[0m$*"; }
  echo_error() { echo -e "\033[31m[ERROR] \033[0m$*"; }
  time_diff() {
    local start end
    start=$(date +%s)
    "$@"
    end=$(date +%s)
    echo_info "耗时: $(( (end - start) / 60 )) 分钟"
  }
fi

function check_env() {
  echo_info "========== 检查环境..."
  if ! command -v python &>/dev/null; then
    echo_error "未找到 python，请先安装或激活 conda 环境"
    exit 1
  fi
  if [[ ! -f "${ROOT_DIR}/data/zz500_stocks.csv" ]]; then
    echo_warn "默认股票列表 data/zz500_stocks.csv 不存在，请使用 --stock-list 指定"
  fi
}

function run_incremental() {
  echo_info "========== 执行日增更新..."
  python src/main/build_company_stock_info.py --incremental "$@"
}

function main() {
  echo_info "==================== 开始 company_stock_info 日增更新..."
  check_env "$@"
  time_diff run_incremental "$@"
  echo_info "==================== 日增更新完成"
}

main "$@"
