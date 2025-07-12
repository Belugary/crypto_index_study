"""
CoinGecko 数字货币数据分析项目

主要模块:
- api: API接口封装
- downloaders: 数据下载和聚合工具
- classification: 币种分类功能
- analysis: 数据分析相关功能
- utils: 工具函数
"""

__version__ = "1.1.0"
__author__ = "Your Name"

# 导入主要类和函数
from .api.coingecko import CoinGeckoAPI, create_api_client
from .downloaders.batch_downloader import BatchDownloader, create_batch_downloader

__all__ = [
    "CoinGeckoAPI",
    "create_api_client",
    "BatchDownloader",
    "create_batch_downloader",
]
