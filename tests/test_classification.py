"""
分类器模块测试

测试统一分类器的基本功能和分类结果数据类。

注意：旧版的 StablecoinChecker 和 WrappedCoinChecker 相关测试已移除，
这些检查器已被 UnifiedClassifier 替代并移动到 legacy/ 目录存档。
"""

import unittest
from unittest.mock import patch

from src.classification.unified_classifier import (
    UnifiedClassifier,
    ClassificationResult,
)


class TestUnifiedClassifier(unittest.TestCase):
    """测试统一分类器功能"""

    @patch("src.classification.unified_classifier.create_batch_downloader")
    def test_unified_classifier_initialization(self, mock_downloader):
        """测试统一分类器初始化"""
        print("\n--- 测试统一分类器初始化 ---")

        classifier = UnifiedClassifier()
        self.assertIsNotNone(classifier)
        self.assertTrue(hasattr(classifier, "metadata_dir"))
        self.assertTrue(hasattr(classifier, "downloader"))
        self.assertTrue(hasattr(classifier, "_cache"))
        self.assertEqual(len(classifier._cache), 0)
        print("✅ 统一分类器初始化测试通过")

    @patch("src.classification.unified_classifier.create_batch_downloader")
    def test_unified_classifier_has_methods(self, mock_downloader):
        """测试统一分类器包含必要方法"""
        print("\n--- 测试统一分类器方法 ---")

        classifier = UnifiedClassifier()

        # 检查核心方法存在
        self.assertTrue(hasattr(classifier, "classify_coin"))
        self.assertTrue(hasattr(classifier, "classify_coins_batch"))
        self.assertTrue(hasattr(classifier, "filter_coins"))
        self.assertTrue(hasattr(classifier, "get_classification_summary"))
        self.assertTrue(callable(classifier.classify_coin))
        self.assertTrue(callable(classifier.classify_coins_batch))
        print("✅ 统一分类器方法测试通过")

    @patch("src.classification.unified_classifier.create_batch_downloader")
    def test_classification_result_dataclass(self, mock_downloader):
        """测试分类结果数据类"""
        print("\n--- 测试分类结果数据类 ---")

        # 测试默认值
        result = ClassificationResult(coin_id="test")
        self.assertEqual(result.coin_id, "test")
        self.assertFalse(result.is_stablecoin)
        self.assertFalse(result.is_wrapped_coin)
        self.assertEqual(result.confidence, "unknown")
        self.assertEqual(result.stablecoin_categories, [])
        self.assertEqual(result.wrapped_categories, [])
        self.assertEqual(result.all_categories, [])
        print("✅ 分类结果数据类测试通过")

    @patch("src.classification.unified_classifier.create_batch_downloader")
    def test_unified_classifier_cache(self, mock_downloader):
        """测试统一分类器缓存功能"""
        print("\n--- 测试统一分类器缓存 ---")

        classifier = UnifiedClassifier()

        # 测试缓存清空
        classifier._cache["test"] = ClassificationResult(coin_id="test")
        self.assertEqual(len(classifier._cache), 1)
        classifier.clear_cache()
        self.assertEqual(len(classifier._cache), 0)
        print("✅ 统一分类器缓存测试通过")


if __name__ == "__main__":
    unittest.main()
