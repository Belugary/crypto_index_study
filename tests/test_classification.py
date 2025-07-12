"""
分类器模块基本测试

测试稳定币检查器和包装币检查器的基本功能
"""

import unittest
from unittest.mock import MagicMock, patch

from src.classification.stablecoin_checker import StablecoinChecker
from src.classification.wrapped_coin_checker import WrappedCoinChecker


class TestStablecoinChecker(unittest.TestCase):
    """测试稳定币检查器基本功能"""

    @patch("src.classification.stablecoin_checker.create_batch_downloader")
    def test_stablecoin_checker_initialization(self, mock_downloader):
        """测试稳定币检查器初始化"""
        print("\n--- 测试稳定币检查器初始化 ---")

        checker = StablecoinChecker()
        self.assertIsNotNone(checker)
        self.assertTrue(hasattr(checker, "metadata_dir"))
        self.assertTrue(hasattr(checker, "downloader"))
        print("✅ 稳定币检查器初始化测试通过")

    @patch("src.classification.stablecoin_checker.create_batch_downloader")
    def test_stablecoin_checker_has_methods(self, mock_downloader):
        """测试稳定币检查器包含必要方法"""
        print("\n--- 测试稳定币检查器方法 ---")

        checker = StablecoinChecker()

        # 检查核心方法存在
        self.assertTrue(hasattr(checker, "is_stablecoin"))
        self.assertTrue(hasattr(checker, "get_all_stablecoins"))
        self.assertTrue(callable(checker.is_stablecoin))
        self.assertTrue(callable(checker.get_all_stablecoins))
        print("✅ 稳定币检查器方法测试通过")


class TestWrappedCoinChecker(unittest.TestCase):
    """测试包装币检查器基本功能"""

    @patch("src.classification.wrapped_coin_checker.create_batch_downloader")
    def test_wrapped_coin_checker_initialization(self, mock_downloader):
        """测试包装币检查器初始化"""
        print("\n--- 测试包装币检查器初始化 ---")

        checker = WrappedCoinChecker()
        self.assertIsNotNone(checker)
        self.assertTrue(hasattr(checker, "metadata_dir"))
        self.assertTrue(hasattr(checker, "downloader"))
        print("✅ 包装币检查器初始化测试通过")

    @patch("src.classification.wrapped_coin_checker.create_batch_downloader")
    def test_wrapped_coin_checker_has_methods(self, mock_downloader):
        """测试包装币检查器包含必要方法"""
        print("\n--- 测试包装币检查器方法 ---")

        checker = WrappedCoinChecker()

        # 检查核心方法存在
        self.assertTrue(hasattr(checker, "is_wrapped_coin"))
        self.assertTrue(hasattr(checker, "get_all_wrapped_coins"))
        self.assertTrue(callable(checker.is_wrapped_coin))
        self.assertTrue(callable(checker.get_all_wrapped_coins))
        print("✅ 包装币检查器方法测试通过")


class TestClassificationIntegration(unittest.TestCase):
    """测试分类器集成功能"""

    @patch("src.classification.stablecoin_checker.create_batch_downloader")
    @patch("src.classification.wrapped_coin_checker.create_batch_downloader")
    def test_classification_modules_work_together(
        self, mock_wrapped_downloader, mock_stable_downloader
    ):
        """测试分类器模块协同工作"""
        print("\n--- 测试分类器模块协同工作 ---")

        stable_checker = StablecoinChecker()
        wrapped_checker = WrappedCoinChecker()

        # 验证两个检查器都可以独立创建
        self.assertIsNotNone(stable_checker)
        self.assertIsNotNone(wrapped_checker)

        # 验证它们有不同的检查方法
        self.assertNotEqual(
            stable_checker.is_stablecoin.__name__,
            wrapped_checker.is_wrapped_coin.__name__,
        )
        print("✅ 分类器模块协同工作测试通过")


if __name__ == "__main__":
    unittest.main()
