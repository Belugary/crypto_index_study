"""
测试 src/updaters/ 模块的核心功能,
"""

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.metadata_updater import MetadataUpdater
from src.updaters.price_updater import (
    MarketDataFetcher,
    PriceDataUpdater,
)


# TODO: CoinClassifier 已被移除，其功能由 UnifiedClassifier 提供
# 如需测试分类功能，请使用 tests/test_classification.py


class TestMarketDataFetcher(unittest.TestCase):
    """测试市场数据获取器"""

    def setUp(self):
        """设置测试环境"""
        self.mock_api = Mock()
        self.fetcher = MarketDataFetcher(self.mock_api)

    def test_get_top_coins_single_page(self):
        """测试获取少量币种（单页）"""
        print("\n--- 测试获取前5名币种 ---")

        # 模拟 API 返回数据
        mock_data = [
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin", "market_cap_rank": 1},
            {
                "id": "ethereum",
                "symbol": "eth",
                "name": "Ethereum",
                "market_cap_rank": 2,
            },
            {"id": "tether", "symbol": "usdt", "name": "Tether", "market_cap_rank": 3},
            {"id": "bnb", "symbol": "bnb", "name": "BNB", "market_cap_rank": 4},
            {"id": "solana", "symbol": "sol", "name": "Solana", "market_cap_rank": 5},
        ]
        self.mock_api.get_coins_markets.return_value = mock_data

        result = self.fetcher.get_top_coins(5)

        self.assertEqual(len(result), 5)
        self.assertEqual(result[0]["id"], "bitcoin")
        self.mock_api.get_coins_markets.assert_called_once()
        print(f"✅ 单页获取测试通过: 获取到 {len(result)} 个币种")


class TestPriceDataUpdater(unittest.TestCase):
    """测试价格数据更新器"""

    def setUp(self):
        """设置测试环境"""
        with patch("src.updaters.price_updater.CoinGeckoAPI"), patch(
            "src.updaters.price_updater.create_batch_downloader"
        ), patch("src.updaters.price_updater.MarketDataFetcher"):
            self.updater = PriceDataUpdater()

    def test_download_coin_data_new_coin(self):
        """测试新币种数据下载"""
        print("\n--- 测试新币种数据下载 ---")

        # 模拟不存在的币种文件
        with patch("pathlib.Path.exists", return_value=False):
            with patch.object(
                self.updater.downloader, "download_coin_data", return_value=True
            ):
                success, api_called = self.updater.download_coin_data(
                    "nonexistent-coin"
                )
                self.assertTrue(success)
                self.assertTrue(api_called)  # 新币种应该会调用API
        print("✅ 新币种数据下载测试通过")

    def test_get_existing_coin_ids(self):
        """测试获取已存在的币种ID"""
        print("\n--- 测试获取已存在币种ID ---")

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.glob",
            return_value=[
                Path("data/coins/bitcoin.csv"),
                Path("data/coins/ethereum.csv"),
            ],
        ):
            existing_ids = self.updater.get_existing_coin_ids()

            self.assertIn("bitcoin", existing_ids)
            self.assertIn("ethereum", existing_ids)
            print(f"✅ 获取已存在币种ID测试通过: {existing_ids}")


class TestMetadataUpdater(unittest.TestCase):
    """测试元数据更新器"""

    def setUp(self):
        """设置测试环境"""
        with patch("src.updaters.metadata_updater.UnifiedClassifier"), patch(
            "src.updaters.metadata_updater.create_batch_downloader"
        ):
            self.updater = MetadataUpdater()

    def test_get_all_coin_ids_from_data(self):
        """测试从数据目录获取币种ID"""
        print("\n--- 测试从数据目录获取币种ID ---")

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.glob",
            return_value=[
                Path("data/coins/bitcoin.csv"),
                Path("data/coins/ethereum.csv"),
                Path("data/coins/tether.csv"),
            ],
        ):
            coin_ids = self.updater.get_all_coin_ids_from_data()

            self.assertEqual(len(coin_ids), 3)
            self.assertIn("bitcoin", coin_ids)
            self.assertIn("ethereum", coin_ids)
            self.assertIn("tether", coin_ids)
            print(f"✅ 获取币种ID测试通过: {coin_ids}")

    def test_get_existing_metadata_coin_ids(self):
        """测试获取已有元数据的币种ID"""
        print("\n--- 测试获取已有元数据币种ID ---")

        with patch("pathlib.Path.exists", return_value=True), patch(
            "pathlib.Path.glob",
            return_value=[
                Path("data/metadata/coin_metadata/bitcoin.json"),
                Path("data/metadata/coin_metadata/ethereum.json"),
            ],
        ):
            existing_ids = self.updater.get_existing_metadata_coin_ids()

            self.assertEqual(len(existing_ids), 2)
            self.assertIn("bitcoin", existing_ids)
            self.assertIn("ethereum", existing_ids)
            print(f"✅ 获取已有元数据币种ID测试通过: {existing_ids}")


if __name__ == "__main__":
    print("🧪 开始测试 src/updaters/ 核心功能模块")
    print("=" * 60)

    unittest.main(verbosity=2)
