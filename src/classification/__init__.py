"""
币种分类模块

提供各种币种分类功能，包括稳定币识别、包装币识别等。
基于 CoinGecko 官方分类数据进行精确识别。
"""

from .stablecoin_checker import StablecoinChecker
from .wrapped_coin_checker import WrappedCoinChecker

__all__ = ["StablecoinChecker", "WrappedCoinChecker"]
