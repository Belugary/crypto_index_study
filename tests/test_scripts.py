"""
测试 scripts/ 目录下的自动化脚本
"""

import os
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 待测试的脚本
from scripts import update_price_data
from src.updaters.price_updater import PriceDataUpdater


class TestUpdatePriceDataScript(unittest.TestCase):
    """测试价格数据更新脚本"""

    @patch("scripts.update_price_data.PriceDataUpdater")
    @patch("sys.argv", ["update_price_data.py"])  # 模拟命令行参数
    def test_main_function_calls_updater(self, MockPriceDataUpdater):
        """测试主函数是否正确调用了 PriceDataUpdater"""
        print("\n--- 测试 update_price_data.py 主流程 ---")

        # 创建一个 mock 实例
        mock_updater_instance = MockPriceDataUpdater.return_value

        # 调用脚本的主函数
        update_price_data.main()

        # 断言 PriceDataUpdater 被实例化了一次
        MockPriceDataUpdater.assert_called_once()

        # 断言 update_with_smart_strategy 方法被调用了一次
        mock_updater_instance.update_with_smart_strategy.assert_called_once()

        print("✅ 主函数成功调用了 PriceDataUpdater.update_with_smart_strategy()")


class TestPriceDataUpdaterLogic(unittest.TestCase):
    """测试 PriceDataUpdater 类的内部逻辑"""

    def test_initialization(self):
        """测试 PriceDataUpdater 是否正确初始化"""
        print("\n--- 测试 PriceDataUpdater 初始化 ---")

        with patch("src.updaters.price_updater.CoinGeckoAPI"), patch(
            "src.updaters.price_updater.create_batch_downloader"
        ), patch("src.updaters.price_updater.CoinClassifier"), patch(
            "src.updaters.price_updater.MarketDataFetcher"
        ):

            updater = PriceDataUpdater()
            self.assertIsNotNone(updater.api)
            self.assertIsNotNone(updater.downloader)
            self.assertIsNotNone(updater.classifier)
            self.assertIsNotNone(updater.market_fetcher)
            self.assertEqual(updater.stats["total_processed"], 0)
            self.assertEqual(updater.stats["native_updated"], 0)

        print("✅ PriceDataUpdater 初始化成功")

    @patch("src.updaters.price_updater.CoinGeckoAPI")
    @patch("src.updaters.price_updater.create_batch_downloader")
    @patch("src.updaters.price_updater.CoinClassifier")
    @patch("src.updaters.price_updater.MarketDataFetcher")
    @patch("src.updaters.price_updater.tqdm")
    @patch("time.sleep")
    def test_update_workflow(
        self,
        mock_sleep,
        mock_tqdm,
        MockMarketDataFetcher,
        MockCoinClassifier,
        mock_create_batch_downloader,
        MockCoinGeckoAPI,
    ):
        """测试完整的 update_with_smart_strategy 工作流"""
        print("\n--- 测试 PriceDataUpdater.update_with_smart_strategy() 工作流 ---")

        # 设置 mock 对象
        mock_api = MockCoinGeckoAPI.return_value
        mock_downloader = MagicMock()
        mock_create_batch_downloader.return_value = mock_downloader
        mock_classifier = MockCoinClassifier.return_value
        mock_market_fetcher = MockMarketDataFetcher.return_value

        # 模拟市值排名数据
        mock_coins_data = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "market_cap_rank": 2,
            },
        ]
        mock_market_fetcher.get_top_coins.return_value = mock_coins_data

        # 模拟币种分类 - 都是原生币
        mock_classifier.classify_coin.return_value = "native"

        # 创建updater实例
        updater = PriceDataUpdater()

        # Mock updater的方法
        updater.download_coin_data = MagicMock(
            return_value=(True, True)
        )  # 返回 (success, api_called)
        updater.get_existing_coin_ids = MagicMock(return_value=set())
        updater.update_metadata = MagicMock()
        updater.generate_final_report = MagicMock()

        # 运行测试（目标2个原生币）
        updater.update_with_smart_strategy(target_native_coins=2, max_search_range=10)

        # 断言关键方法被调用
        mock_market_fetcher.get_top_coins.assert_called()
        mock_classifier.classify_coin.assert_called()
        updater.download_coin_data.assert_called()
        updater.update_metadata.assert_called_once()
        updater.generate_final_report.assert_called_once()

        # 检查统计信息
        self.assertGreaterEqual(updater.stats["native_updated"], 2)

        print("✅ update_with_smart_strategy() 工作流测试成功")


class TestUpdateMetadataScript(unittest.TestCase):
    """测试 scripts/update_all_metadata.py 脚本"""

    @patch("src.updaters.metadata_updater.MetadataUpdater")
    @patch("sys.argv", ["update_all_metadata.py"])  # 模拟命令行参数
    def test_main_function_calls_updater(self, MockMetadataUpdater):
        """测试主函数是否正确调用了 MetadataUpdater"""
        print("\n--- 测试 update_all_metadata.py 主流程 ---")

        # 创建一个 mock 实例
        mock_updater_instance = MockMetadataUpdater.return_value
        mock_updater_instance.batch_update_all_metadata.return_value = {}
        mock_updater_instance.update_all_classification_lists.return_value = {}

        # 导入并调用脚本的主函数
        from scripts import update_all_metadata

        update_all_metadata.main()

        # 断言 MetadataUpdater 被实例化了一次
        MockMetadataUpdater.assert_called_once()

        # 断言相关方法被调用了
        mock_updater_instance.batch_update_all_metadata.assert_called_once()
        mock_updater_instance.update_all_classification_lists.assert_called_once()

        print("✅ 主函数成功调用了 MetadataUpdater 的相关方法")


class TestMetadataUpdaterIntegration(unittest.TestCase):
    """测试 MetadataUpdater 的集成功能"""

    def setUp(self):
        """设置测试环境"""
        with patch("src.updaters.metadata_updater.StablecoinChecker"), patch(
            "src.updaters.metadata_updater.WrappedCoinChecker"
        ), patch("src.updaters.metadata_updater.create_batch_downloader"):
            from src.updaters.metadata_updater import MetadataUpdater

            self.updater = MetadataUpdater()

    def test_get_all_coin_ids_from_data(self):
        """测试从数据目录获取所有币种 ID"""
        print("\n--- 测试 get_all_coin_ids_from_data ---")

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.glob"
        ) as mock_glob:
            # 模拟 CSV 文件
            mock_file1 = MagicMock()
            mock_file1.stem = "bitcoin"
            mock_file2 = MagicMock()
            mock_file2.stem = "ethereum"
            mock_file3 = MagicMock()
            mock_file3.stem = "cardano"

            mock_glob.return_value = [mock_file1, mock_file2, mock_file3]

            # 执行函数
            result = self.updater.get_all_coin_ids_from_data()

            # 验证结果（应该按字母顺序排序）
            self.assertEqual(result, ["bitcoin", "cardano", "ethereum"])

        print("✅ get_all_coin_ids_from_data 测试成功")

    def test_get_existing_metadata_coin_ids(self):
        """测试获取已有元数据的币种 ID"""
        print("\n--- 测试 get_existing_metadata_coin_ids ---")

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.glob"
        ) as mock_glob:
            # 模拟 JSON 文件
            mock_file1 = MagicMock()
            mock_file1.stem = "bitcoin"
            mock_file2 = MagicMock()
            mock_file2.stem = "ethereum"

            mock_glob.return_value = [mock_file1, mock_file2]

            # 执行函数
            result = self.updater.get_existing_metadata_coin_ids()

            # 验证结果
            self.assertEqual(result, {"bitcoin", "ethereum"})

        print("✅ get_existing_metadata_coin_ids 测试成功")

    @patch("src.updaters.metadata_updater.create_batch_downloader")
    @patch("time.sleep")
    def test_batch_update_all_metadata_incremental(
        self, mock_sleep, mock_create_downloader
    ):
        """测试增量更新模式"""
        print("\n--- 测试 batch_update_all_metadata (增量模式) ---")

        # Mock 方法
        self.updater.get_all_coin_ids_from_data = MagicMock(
            return_value=["bitcoin", "ethereum", "cardano"]
        )
        self.updater.get_existing_metadata_coin_ids = MagicMock(
            return_value={"bitcoin"}  # bitcoin 已存在
        )

        # 模拟下载器
        mock_downloader = MagicMock()
        mock_create_downloader.return_value = mock_downloader
        mock_downloader.batch_update_coin_metadata.return_value = {
            "ethereum": True,
            "cardano": True,
        }

        # 重新设置下载器
        self.updater.downloader = mock_downloader

        # 执行函数
        result = self.updater.batch_update_all_metadata(
            batch_size=2, delay_seconds=0.1, force_update=False
        )

        # 验证调用
        self.updater.get_all_coin_ids_from_data.assert_called_once()
        self.updater.get_existing_metadata_coin_ids.assert_called_once()

        # 验证只处理新的币种
        mock_downloader.batch_update_coin_metadata.assert_called_once_with(
            coin_ids=["ethereum", "cardano"], force=False, delay_seconds=0.1
        )

        print("✅ batch_update_all_metadata (增量模式) 测试成功")


if __name__ == "__main__":
    unittest.main()
