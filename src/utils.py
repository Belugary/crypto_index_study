"""
工具函数模块

提供项目中常用的工具函数
"""

import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional


def print_json(data: Any, title: str = "", max_items: Optional[int] = None) -> None:
    """
    格式化打印JSON数据

    Args:
        data: 要打印的数据
        title: 标题
        max_items: 最大显示项数
    """
    if title:
        print(f"\n{'='*50}")
        print(title)
        print(f"{'='*50}")

    if isinstance(data, list) and max_items:
        if len(data) > max_items:
            print(f"显示前 {max_items} 项 (总共 {len(data)} 项):")
            data = data[:max_items]
            print(json.dumps(data, indent=2, ensure_ascii=False))
            print(f"... 还有 {len(data) - max_items} 项")
        else:
            print(json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(json.dumps(data, indent=2, ensure_ascii=False))


def get_timestamp(date_str: Optional[str] = None) -> int:
    """
    获取Unix时间戳

    Args:
        date_str: 日期字符串，格式为 'YYYY-MM-DD'，默认为当前时间

    Returns:
        Unix时间戳
    """
    if date_str:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return int(dt.timestamp())
    return int(time.time())


def format_currency(amount: float, currency: str = "USD") -> str:
    """
    格式化货币显示

    Args:
        amount: 金额
        currency: 货币类型

    Returns:
        格式化的货币字符串
    """
    if currency.upper() == "USD":
        return f"${amount:,.2f}"
    elif currency.upper() == "CNY":
        return f"¥{amount:,.2f}"
    else:
        return f"{amount:,.2f} {currency.upper()}"


def calculate_percentage_change(old_value: float, new_value: float) -> float:
    """
    计算百分比变化

    Args:
        old_value: 原值
        new_value: 新值

    Returns:
        百分比变化
    """
    if old_value == 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100


def safe_get(data: Dict, *keys: str, default: Any = None) -> Any:
    """
    安全获取嵌套字典中的值

    Args:
        data: 字典数据
        keys: 键的路径
        default: 默认值

    Returns:
        获取到的值或默认值
    """
    for key in keys:
        if isinstance(data, dict) and key in data:
            data = data[key]
        else:
            return default
    return data


def get_project_root() -> str:
    """
    获取项目的根目录路径。

    通过查找 .git 目录来确定项目根目录。

    Returns:
        str: 项目的绝对根目录路径。
    """
    current_path = os.path.abspath(os.path.dirname(__file__))
    while True:
        if ".git" in os.listdir(current_path):
            return current_path
        parent_path = os.path.dirname(current_path)
        if parent_path == current_path:
            raise FileNotFoundError("无法找到项目根目录 (未找到 .git 文件夹)")
        current_path = parent_path
