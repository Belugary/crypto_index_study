"""
指数计算模块

提供各种加权方式的区块链资产指数计算功能
"""

from .market_cap_weighted import MarketCapWeightedIndexCalculator

__all__ = ["MarketCapWeightedIndexCalculator"]
