#!/usr/bin/env bash
# Daily: crawl yesterday's Luobo reports -> append CSV -> Zhipu extract -> parse.
# Usage: ./bin/run_daily_report_crawl_zhipu.sh [YYYYMMDD]
# Requires: ZHIPU_API_KEY, Chrome (for spider), conda activate research_report_spider or stock_investment

set -e
cd "$(dirname "$0")/.."
ROOT_DIR=$(pwd)
export ZHIPU_API_KEY="${ZHIPU_API_KEY:-}"

if [ -z "$ZHIPU_API_KEY" ]; then
  echo "Error: ZHIPU_API_KEY not set"
  exit 1
fi

if [ -n "$1" ]; then
  python bin/run_daily_report_crawl_zhipu.py -d "$1"
else
  python bin/run_daily_report_crawl_zhipu.py
fi
