"""
币种分类模块

提供各种币种分类功能，包括稳定币识别、包装币识别等。
基于 CoinGecko 官方分类数据进行精确识别。

核心组件：
- UnifiedClassifier: 统一分类器，提供高效的批量分类功能
- ClassificationResult: 分类结果数据类

注意：旧版的 StablecoinChecker 和 WrappedCoinChecker 已移动到 legacy/ 目录存档。
"""

from .unified_classifier import ClassificationResult, UnifiedClassifier

__all__ = [
    "UnifiedClassifier",
    "ClassificationResult",
]
