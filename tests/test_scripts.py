"""
测试 scripts/ 目录下的自动化脚本
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 待测试的脚本
from scripts import update_price_data
from scripts.update_price_data import PriceDataUpdater


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

        # 断言 run 方法被调用了一次
        mock_updater_instance.run.assert_called_once()

        print("✅ 主函数成功调用了 PriceDataUpdater.run()")


class TestPriceDataUpdaterLogic(unittest.TestCase):
    """测试 PriceDataUpdater 类的内部逻辑"""

    @patch("scripts.update_price_data.CoinGeckoAPI")
    @patch("scripts.update_price_data.create_batch_downloader")
    @patch("scripts.update_price_data.StablecoinChecker")
    @patch("scripts.update_price_data.WrappedCoinChecker")
    def setUp(
        self, MockWrappedCoinChecker, MockStablecoinChecker, mock_create_batch_downloader, MockCoinGeckoAPI
    ):
        """初始化测试环境和 mock 对象"""
        self.mock_api = MockCoinGeckoAPI.return_value
        self.mock_downloader = MagicMock()
        mock_create_batch_downloader.return_value = self.mock_downloader
        self.mock_checker = MockStablecoinChecker.return_value
        self.mock_wrapped_checker = MockWrappedCoinChecker.return_value

        self.updater = PriceDataUpdater(
            api=self.mock_api,
            downloader=self.mock_downloader,
            checker=self.mock_checker,
            wrapped_checker=self.mock_wrapped_checker,
        )
        # 为测试目的，手动添加在 run() 方法中才会创建的属性
        self.updater.updated_coins = []
        self.updater.new_coins = []

    def test_initialization(self):
        """测试 PriceDataUpdater 是否正确初始化"""
        print("\n--- 测试 PriceDataUpdater 初始化 ---")
        self.assertIsNotNone(self.updater.api)
        self.assertIsNotNone(self.updater.downloader)
        self.assertIsNotNone(self.updater.checker)
        self.assertEqual(self.updater.updated_coins, [])
        self.assertEqual(self.updater.new_coins, [])
        print("✅ PriceDataUpdater 初始化成功")

    @patch("scripts.update_price_data.tqdm")
    def test_run_workflow(self, mock_tqdm):
        """测试完整的 run 工作流"""
        print("\n--- 测试 PriceDataUpdater.run() 工作流 ---")

        # 模拟 API 返回
        self.mock_api.get_coins_markets.side_effect = [
            [
                {"id": "bitcoin", "symbol": "btc"},
                {"id": "ethereum", "symbol": "eth"},
                {"id": "tether", "symbol": "usdt"},  # 稳定币
            ],
            [],  # 第二页为空
        ]

        # 模拟稳定币检查器
        self.mock_checker.is_stablecoin.side_effect = lambda symbol: symbol == "usdt"

        # 模拟包装币检查器
        self.mock_wrapped_checker.is_wrapped_coin.side_effect = lambda coin_id: {"is_wrapped_coin": coin_id == "wrapped-bitcoin"}

        # 模拟 get_coin_last_date
        # bitcoin 最新, ethereum 过时, tether 是稳定币应跳过
        with patch.object(self.updater, "get_coin_last_date") as mock_get_last_date:
            mock_get_last_date.side_effect = [
                datetime.now().strftime("%Y-%m-%d"),  # bitcoin, 最新
                (datetime.now() - timedelta(days=5)).strftime(
                    "%Y-%m-%d"
                ),  # ethereum, 过时
            ]

            # 运行
            self.updater.run(top_n=3)

            # 断言
            # get_coins_markets 被调用
            self.mock_api.get_coins_markets.assert_called()
            # is_stablecoin 被调用
            self.mock_checker.is_stablecoin.assert_called()
            # downloader.download_coin_data 只为 ethereum 调用
            self.mock_downloader.download_coin_data.assert_called_once()
            # 检查调用参数
            call_args = self.mock_downloader.download_coin_data.call_args
            self.assertEqual(call_args[1]["coin_id"], "ethereum")

            # 检查更新列表
            self.assertIn("ethereum", self.updater.updated_coins)
            self.assertNotIn("bitcoin", self.updater.updated_coins)

            print("✅ run() 工作流测试成功")


class TestUpdateMetadataScript(unittest.TestCase):
    """测试 scripts/update_all_metadata.py 脚本"""

    def setUp(self):
        """设置测试环境"""
        self.test_coins_dir = Path("test_data/coins")
        self.test_metadata_dir = Path("test_data/metadata/coin_metadata")

    def tearDown(self):
        """清理测试环境"""
        import shutil

        if Path("test_data").exists():
            shutil.rmtree("test_data")

    @patch("scripts.update_all_metadata.Path")
    def test_get_all_coin_ids_from_data(self, mock_path):
        """测试从数据目录获取所有币种 ID"""
        from scripts.update_all_metadata import get_all_coin_ids_from_data

        print("\n--- 测试 get_all_coin_ids_from_data ---")

        # 模拟目录和文件
        mock_coins_dir = MagicMock()
        mock_path.return_value = mock_coins_dir
        mock_coins_dir.exists.return_value = True

        # 模拟 CSV 文件
        mock_file1 = MagicMock()
        mock_file1.stem = "bitcoin"
        mock_file2 = MagicMock()
        mock_file2.stem = "ethereum"
        mock_file3 = MagicMock()
        mock_file3.stem = "cardano"

        mock_coins_dir.glob.return_value = [mock_file1, mock_file2, mock_file3]

        # 执行函数
        result = get_all_coin_ids_from_data()

        # 验证结果（应该按字母顺序排序）
        self.assertEqual(result, ["bitcoin", "cardano", "ethereum"])
        mock_path.assert_called_once_with("data/coins")
        mock_coins_dir.exists.assert_called_once()
        mock_coins_dir.glob.assert_called_once_with("*.csv")

        print("✅ get_all_coin_ids_from_data 测试成功")

    @patch("scripts.update_all_metadata.Path")
    def test_get_all_coin_ids_from_data_no_directory(self, mock_path):
        """测试当数据目录不存在时的情况"""
        from scripts.update_all_metadata import get_all_coin_ids_from_data

        print("\n--- 测试 get_all_coin_ids_from_data (无目录) ---")

        # 模拟目录不存在
        mock_coins_dir = MagicMock()
        mock_path.return_value = mock_coins_dir
        mock_coins_dir.exists.return_value = False

        # 执行函数
        result = get_all_coin_ids_from_data()

        # 验证结果
        self.assertEqual(result, [])
        mock_coins_dir.exists.assert_called_once()

        print("✅ get_all_coin_ids_from_data (无目录) 测试成功")

    @patch("scripts.update_all_metadata.Path")
    def test_get_existing_metadata_coin_ids(self, mock_path):
        """测试获取已有元数据的币种 ID"""
        from scripts.update_all_metadata import get_existing_metadata_coin_ids

        print("\n--- 测试 get_existing_metadata_coin_ids ---")

        # 模拟目录和文件
        mock_metadata_dir = MagicMock()
        mock_path.return_value = mock_metadata_dir
        mock_metadata_dir.exists.return_value = True

        # 模拟 JSON 文件
        mock_file1 = MagicMock()
        mock_file1.stem = "bitcoin"
        mock_file2 = MagicMock()
        mock_file2.stem = "ethereum"

        mock_metadata_dir.glob.return_value = [mock_file1, mock_file2]

        # 执行函数
        result = get_existing_metadata_coin_ids()

        # 验证结果
        self.assertEqual(result, {"bitcoin", "ethereum"})
        mock_path.assert_called_once_with("data/metadata/coin_metadata")
        mock_metadata_dir.exists.assert_called_once()
        mock_metadata_dir.glob.assert_called_once_with("*.json")

        print("✅ get_existing_metadata_coin_ids 测试成功")

    @patch("scripts.update_all_metadata.Path")
    def test_get_existing_metadata_coin_ids_no_directory(self, mock_path):
        """测试当元数据目录不存在时的情况"""
        from scripts.update_all_metadata import get_existing_metadata_coin_ids

        print("\n--- 测试 get_existing_metadata_coin_ids (无目录) ---")

        # 模拟目录不存在
        mock_metadata_dir = MagicMock()
        mock_path.return_value = mock_metadata_dir
        mock_metadata_dir.exists.return_value = False

        # 执行函数
        result = get_existing_metadata_coin_ids()

        # 验证结果
        self.assertEqual(result, set())
        mock_metadata_dir.exists.assert_called_once()

        print("✅ get_existing_metadata_coin_ids (无目录) 测试成功")

    @patch("scripts.update_all_metadata.create_batch_downloader")
    @patch("scripts.update_all_metadata.get_existing_metadata_coin_ids")
    @patch("scripts.update_all_metadata.get_all_coin_ids_from_data")
    @patch("time.sleep")
    def test_batch_update_all_metadata_incremental(
        self,
        mock_sleep,
        mock_get_all_ids,
        mock_get_existing_ids,
        mock_create_downloader,
    ):
        """测试增量更新模式"""
        from scripts.update_all_metadata import batch_update_all_metadata

        print("\n--- 测试 batch_update_all_metadata (增量模式) ---")

        # 模拟数据
        mock_get_all_ids.return_value = ["bitcoin", "ethereum", "cardano"]
        mock_get_existing_ids.return_value = {"bitcoin"}  # bitcoin 已存在

        # 模拟下载器
        mock_downloader = MagicMock()
        mock_create_downloader.return_value = mock_downloader
        mock_downloader.batch_update_coin_metadata.return_value = {
            "ethereum": True,
            "cardano": True,
        }

        # 执行函数
        batch_update_all_metadata(batch_size=2, delay_seconds=0.1, force_update=False)

        # 验证调用
        mock_get_all_ids.assert_called_once()
        mock_get_existing_ids.assert_called_once()
        mock_create_downloader.assert_called_once()

        # 验证只处理新的币种
        mock_downloader.batch_update_coin_metadata.assert_called_once_with(
            coin_ids=["ethereum", "cardano"], force=False, delay_seconds=0.1
        )

        print("✅ batch_update_all_metadata (增量模式) 测试成功")

    @patch("scripts.update_all_metadata.create_batch_downloader")
    @patch("scripts.update_all_metadata.get_existing_metadata_coin_ids")
    @patch("scripts.update_all_metadata.get_all_coin_ids_from_data")
    @patch("time.sleep")
    def test_batch_update_all_metadata_force_update(
        self,
        mock_sleep,
        mock_get_all_ids,
        mock_get_existing_ids,
        mock_create_downloader,
    ):
        """测试强制更新模式"""
        from scripts.update_all_metadata import batch_update_all_metadata

        print("\n--- 测试 batch_update_all_metadata (强制更新) ---")

        # 模拟数据
        mock_get_all_ids.return_value = ["bitcoin", "ethereum"]
        mock_get_existing_ids.return_value = {"bitcoin"}  # bitcoin 已存在

        # 模拟下载器
        mock_downloader = MagicMock()
        mock_create_downloader.return_value = mock_downloader
        mock_downloader.batch_update_coin_metadata.return_value = {
            "bitcoin": True,
            "ethereum": True,
        }

        # 执行函数（强制更新）
        batch_update_all_metadata(batch_size=2, delay_seconds=0.1, force_update=True)

        # 验证调用
        mock_get_all_ids.assert_called_once()
        mock_get_existing_ids.assert_called_once()
        mock_create_downloader.assert_called_once()

        # 验证处理所有币种
        mock_downloader.batch_update_coin_metadata.assert_called_once_with(
            coin_ids=["bitcoin", "ethereum"], force=True, delay_seconds=0.1
        )

        print("✅ batch_update_all_metadata (强制更新) 测试成功")

    @patch("scripts.update_all_metadata.get_all_coin_ids_from_data")
    def test_batch_update_all_metadata_no_coins(self, mock_get_all_ids):
        """测试没有币种数据时的情况"""
        from scripts.update_all_metadata import batch_update_all_metadata

        print("\n--- 测试 batch_update_all_metadata (无币种) ---")

        # 模拟没有币种数据
        mock_get_all_ids.return_value = []

        # 执行函数
        batch_update_all_metadata()

        # 验证调用
        mock_get_all_ids.assert_called_once()

        print("✅ batch_update_all_metadata (无币种) 测试成功")

    @patch("scripts.update_all_metadata.StablecoinChecker")
    def test_generate_complete_stablecoin_list(self, mock_checker_class):
        """测试生成完整稳定币列表"""
        from scripts.update_all_metadata import generate_complete_stablecoin_list

        print("\n--- 测试 generate_complete_stablecoin_list ---")

        # 模拟 StablecoinChecker
        mock_checker = MagicMock()
        mock_checker_class.return_value = mock_checker

        # 模拟稳定币数据
        mock_stablecoins = [
            {
                "symbol": "usdt",
                "name": "Tether",
                "stablecoin_categories": ["fiat-backed"],
            },
            {
                "symbol": "usdc",
                "name": "USD Coin",
                "stablecoin_categories": ["fiat-backed"],
            },
        ]
        mock_checker.get_all_stablecoins.return_value = mock_stablecoins
        mock_checker.export_stablecoins_csv.return_value = True

        # 执行函数
        generate_complete_stablecoin_list()

        # 验证调用
        mock_checker_class.assert_called_once()
        mock_checker.get_all_stablecoins.assert_called_once()
        mock_checker.export_stablecoins_csv.assert_called_once()

        print("✅ generate_complete_stablecoin_list 测试成功")

    @patch("scripts.update_all_metadata.WrappedCoinChecker")
    def test_generate_complete_wrapped_coin_list(self, mock_checker_class):
        """测试生成完整包装币列表"""
        from scripts.update_all_metadata import generate_complete_wrapped_coin_list

        print("\n--- 测试 generate_complete_wrapped_coin_list ---")

        # 模拟 WrappedCoinChecker
        mock_checker = MagicMock()
        mock_checker_class.return_value = mock_checker

        # 模拟包装币数据
        mock_wrapped_coins = [
            {
                "coin_id": "wrapped-bitcoin",
                "name": "Wrapped Bitcoin",
                "symbol": "wbtc",
                "is_wrapped_coin": True,
                "confidence": "high",
                "wrapped_categories": ["Wrapped-Tokens"],
                "name_indicators": ["wrapped"],
                "symbol_patterns": ["prefix_w"],
                "special_patterns": [],
            },
            {
                "coin_id": "staked-ether",
                "name": "Staked Ether",
                "symbol": "steth",
                "is_wrapped_coin": True,
                "confidence": "very_high",
                "wrapped_categories": ["Liquid Staking Tokens"],
                "name_indicators": ["staked"],
                "symbol_patterns": ["prefix_st"],
                "special_patterns": [],
            },
        ]
        mock_checker.get_all_wrapped_coins.return_value = mock_wrapped_coins
        mock_checker.export_wrapped_coins_csv.return_value = True

        # 执行函数
        generate_complete_wrapped_coin_list()

        # 验证调用
        mock_checker_class.assert_called_once()
        mock_checker.get_all_wrapped_coins.assert_called_once()
        mock_checker.export_wrapped_coins_csv.assert_called_once()

        print("✅ generate_complete_wrapped_coin_list 测试成功")

    @patch("scripts.update_all_metadata.StablecoinChecker")
    def test_generate_complete_stablecoin_list_no_stablecoins(self, mock_checker_class):
        """测试没有稳定币时的情况"""
        from scripts.update_all_metadata import generate_complete_stablecoin_list

        print("\n--- 测试 generate_complete_stablecoin_list (无稳定币) ---")

        # 模拟 StablecoinChecker
        mock_checker = MagicMock()
        mock_checker_class.return_value = mock_checker
        mock_checker.get_all_stablecoins.return_value = []

        # 执行函数
        generate_complete_stablecoin_list()

        # 验证调用
        mock_checker_class.assert_called_once()
        mock_checker.get_all_stablecoins.assert_called_once()
        # 没有稳定币时不应该调用导出
        mock_checker.export_stablecoins_csv.assert_not_called()

        print("✅ generate_complete_stablecoin_list (无稳定币) 测试成功")


if __name__ == "__main__":
    unittest.main()
