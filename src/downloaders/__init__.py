"""
数据获取工具集

提供各种数据下载和聚合功能，包括：
- 批量下载器：获取原始市场数据
- 每日聚合器：生成每日市场快照
"""

from .batch_downloader import BatchDownloader, create_batch_downloader
from .daily_aggregator import DailyDataAggregator, create_daily_aggregator

__all__ = [
    "BatchDownloader",
    "create_batch_downloader",
    "DailyDataAggregator",
    "create_daily_aggregator",
]
