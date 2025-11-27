# -*- coding:utf-8 -*-
"""
    Pork concept stock analysis
"""

import os 
import pandas as pd 
from stock_analysis import get_current_pettm_and_mean
from datetime import datetime


def convert_stock_code(code):
    """
    Convert stock code to baostock format
    
    Args:
        code (str): 6-digit stock code, e.g., "000048", "600073"
    
    Returns:
        str: baostock format stock code, e.g., "sz.000048", "sh.600073"
    """
    # Determine if it's Shanghai or Shenzhen
    if code.startswith('60') or code.startswith('68'):  # Shanghai main board, STAR market
        return f"sh.{code}"
    elif code.startswith('00') or code.startswith('30') or code.startswith('92'):  # Shenzhen main board, SME board, ChiNext
        return f"sz.{code}"
    else:
        # Default to Shenzhen stock
        return f"sz.{code}"


def read_stocks_from_file(file_path):
    """
    Read stock list from file
    
    Args:
        file_path (str): File path
    
    Returns:
        list: List of tuples containing (stock_code, stock_name)
    """
    stocks = []
    if not os.path.exists(file_path):
        print(f'File not found: {file_path}')
        return stocks
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:  # Skip empty lines
                continue
            
            # Split stock code and name (may be separated by space or tab)
            parts = line.split()
            if len(parts) >= 2:
                code = parts[0].strip()
                name = ' '.join(parts[1:]).strip()  # Name may contain spaces
                stocks.append((code, name))
            elif len(parts) == 1:
                # If only code without name
                stocks.append((parts[0].strip(), ''))
    
    return stocks


def main():
    """
    Main function: Read stock list and print latest peTTM and mean peTTM for each stock
    """
    # Read stock list (using relative path)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(script_dir))
    stocks_file = os.path.join(project_root, "data", "pork", "stocks.txt")
    stocks = read_stocks_from_file(stocks_file)
    
    if not stocks:
        print('No stock data read')
        return
    
    print(f'Total {len(stocks)} stocks read')
    print('='*80)
    print(f'{"No.":<6} {"Code":<12} {"Name":<20} {"Latest peTTM":<15} {"Mean peTTM":<15} {"Diff":<15}')
    print('='*80)
    
    success_count = 0
    fail_count = 0
    
    for idx, (code, name) in enumerate(stocks, 1):
        # Convert to baostock format
        stock_code = convert_stock_code(code)
        
        print(f'[{idx}/{len(stocks)}] Processing: {code} ({name})...', end=' ')
        
        # Get peTTM data
        pettm_data = get_current_pettm_and_mean(stock_code, period="10Y", end_date=None)
        
        if pettm_data is not None:
            last_pettm = pettm_data['last_pettm']
            mean_pettm = pettm_data['mean_pettm']
            diff = mean_pettm - last_pettm
            
            print(f'✓')
            print(f'{idx:<6} {code:<12} {name:<20} {last_pettm:>14.4f} {mean_pettm:>14.4f} {diff:>14.4f}')
            success_count += 1
        else:
            print(f'✗ Failed')
            print(f'{idx:<6} {code:<12} {name:<20} {"N/A":<15} {"N/A":<15} {"N/A":<15}')
            fail_count += 1
        
        print('-'*80)
    
    print('='*80)
    print(f'Processing complete! Success: {success_count}, Failed: {fail_count}')


if __name__ == "__main__":
    main()