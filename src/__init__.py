"""
CoinGecko 数字货币数据分析项目

主要模块:
- api: API接口封装
- models: 数据模型
- utils: 工具函数
- analysis: 数据分析相关功能
"""

__version__ = "1.0.0"
__author__ = "Your Name"

# 导入主要类和函数
from .api.coingecko import CoinGeckoAPI

__all__ = ["CoinGeckoAPI"]
