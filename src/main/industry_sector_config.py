# -*- coding: utf-8 -*-
"""
板块与申万一级行业映射配置。

左边：板块（用户自定义分类，如医药、食品、消费）
右边：申万一级行业（来自 akshare 申万接口）

用于判断股票所属板块，例如行业限制筛选时使用。
"""

import json
from pathlib import Path
from typing import Dict, List, Optional

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "data" / "industry_sector_config.json"

# 默认映射：板块 -> 申万一级行业列表
DEFAULT_SECTOR_TO_SW_INDUSTRY: Dict[str, List[str]] = {
    "医药": ["医药生物"],
    "食品": ["食品饮料"],
    "消费": ["商贸零售", "家用电器", "社会服务", "纺织服饰", "美容护理"],
}


def load_sector_config() -> Dict[str, List[str]]:
    """从 JSON 文件加载板块配置，若文件不存在则返回默认配置。"""
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return DEFAULT_SECTOR_TO_SW_INDUSTRY.copy()


def get_sw_industries_by_sector(sector: str) -> List[str]:
    """根据板块名称获取对应的申万一级行业列表。"""
    config = load_sector_config()
    return config.get(sector, [])


def get_sector_by_sw_industry(sw_industry: str) -> Optional[str]:
    """根据申万一级行业获取所属板块，若不属于任何板块则返回 None。"""
    config = load_sector_config()
    for sector, industries in config.items():
        if sw_industry in industries:
            return sector
    return None


def is_stock_in_sectors(sw_industry: str, allowed_sectors: List[str]) -> bool:
    """
    判断股票的申万一级行业是否属于允许的板块之一。

    Args:
        sw_industry: 申万一级行业名称（如：商贸零售）
        allowed_sectors: 允许的板块列表（如：["医药", "食品", "消费"]）

    Returns:
        True 若该行业属于任一允许板块
    """
    sector = get_sector_by_sw_industry(sw_industry)
    return sector in allowed_sectors if sector else False
