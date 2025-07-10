"""
CoinGecko 数字货币数据分析项目

主要模块:
- api: API接口封装
- data: 数据下载和管理
- utils: 工具函数
- analysis: 数据分析相关功能
"""

__version__ = "1.1.0"
__author__ = "Your Name"

# 导入主要类和函数
from .api.coingecko import CoinGeckoAPI, create_api_client
from .data.batch_downloader import BatchDownloader, create_batch_downloader

__all__ = [
    "CoinGeckoAPI", 
    "create_api_client",
    "BatchDownloader",
    "create_batch_downloader"
]
