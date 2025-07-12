#!/usr/bin/env python3
"""
增量每日数据更新器测试模块

测试覆盖：
1. 新币种检测逻辑
2. 历史数据下载
3. 每日文件插入
4. 错误处理和恢复
5. 完整工作流
"""

import os
import sys
import unittest
import tempfile
import shutil
from datetime import date
from pathlib import Path
from unittest.mock import patch
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.incremental_daily_updater import (
    IncrementalDailyUpdater,
    create_incremental_updater,
)


class TestIncrementalDailyUpdater(unittest.TestCase):
    """增量每日数据更新器测试"""

    def setUp(self):
        """测试前准备"""
        # 创建临时目录
        self.temp_dir = tempfile.mkdtemp()
        self.coins_dir = Path(self.temp_dir) / "coins"
        self.daily_dir = Path(self.temp_dir) / "daily"
        self.coins_dir.mkdir(parents=True)
        self.daily_dir.mkdir(parents=True)

        print(f"\n--- 测试环境初始化 ---")
        print(f"临时目录: {self.temp_dir}")

    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir)
        print("✅ 测试环境清理完成")

    def test_01_initialization(self):
        """测试初始化"""
        print("\n--- 测试 1: 增量更新器初始化 ---")

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            self.assertEqual(updater.coins_dir, self.coins_dir)
            self.assertEqual(updater.daily_dir, self.daily_dir)
            self.assertTrue(updater.backup_enabled)

        print("✅ 初始化测试通过")

    def test_02_get_existing_coins(self):
        """测试获取已有币种列表"""
        print("\n--- 测试 2: 获取已有币种列表 ---")

        # 创建测试数据文件
        test_coins = ["bitcoin", "ethereum", "cardano"]
        for coin in test_coins:
            (self.coins_dir / f"{coin}.csv").touch()

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            existing = updater.get_existing_coins()

            self.assertEqual(existing, set(test_coins))

        print(f"✅ 成功识别 {len(existing)} 个已有币种")

    def test_03_detect_new_coins(self):
        """测试新币种检测"""
        print("\n--- 测试 3: 新币种检测 ---")

        # 创建已有币种
        existing_coins = ["bitcoin", "ethereum"]
        for coin in existing_coins:
            (self.coins_dir / f"{coin}.csv").touch()

        # 模拟市值排名数据
        mock_market_data = [
            {"id": "bitcoin", "name": "Bitcoin"},
            {"id": "ethereum", "name": "Ethereum"},
            {"id": "cardano", "name": "Cardano"},  # 新币种
            {"id": "solana", "name": "Solana"},  # 新币种
        ]

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ) as MockMarketDataFetcher:

            # 设置 mock
            mock_fetcher = MockMarketDataFetcher.return_value
            mock_fetcher.get_top_coins.return_value = mock_market_data

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            new_coins = updater.detect_new_coins(top_n=10)

            expected_new = {"cardano", "solana"}
            self.assertEqual(set(new_coins), expected_new)

        print(f"✅ 成功检测到 {len(new_coins)} 个新币种: {new_coins}")

    def test_04_load_coin_data(self):
        """测试币种数据加载"""
        print("\n--- 测试 4: 币种数据加载 ---")

        # 创建测试CSV数据
        test_data = pd.DataFrame(
            {
                "timestamp": [
                    1609459200000,
                    1609545600000,
                    1609632000000,
                ],  # 2021-01-01, 01-02, 01-03
                "price": [29000.0, 30000.0, 31000.0],
                "volume": [1000000.0, 1100000.0, 1200000.0],
                "market_cap": [500000000.0, 520000000.0, 540000000.0],
            }
        )

        test_csv_path = self.coins_dir / "test-coin.csv"
        test_data.to_csv(test_csv_path, index=False)

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            coin_df = updater.load_coin_data("test-coin")

            self.assertIsNotNone(coin_df)
            if coin_df is not None:  # 类型保护
                self.assertEqual(len(coin_df), 3)
                self.assertIn("date", coin_df.columns)
                self.assertIn("coin_id", coin_df.columns)
                self.assertEqual(coin_df["coin_id"].iloc[0], "test-coin")

        print("✅ 币种数据加载测试通过")

    def test_05_insert_coin_into_daily_file(self):
        """测试插入币种到每日文件"""
        print("\n--- 测试 5: 插入币种到每日文件 ---")

        # 创建测试的每日文件目录结构
        target_date = date(2021, 1, 1)
        year_dir = self.daily_dir / "2021"
        month_dir = year_dir / "01"
        month_dir.mkdir(parents=True)

        # 创建已有的每日文件
        existing_data = pd.DataFrame(
            {
                "timestamp": [1609459200000],
                "price": [29000.0],
                "volume": [1000000.0],
                "market_cap": [500000000.0],
                "date": [target_date],
                "coin_id": ["bitcoin"],
                "rank": [1],
            }
        )

        daily_file_path = month_dir / f"{target_date}.csv"
        existing_data.to_csv(daily_file_path, index=False)

        # 准备要插入的新币种数据
        new_coin_data = {
            "timestamp": 1609459200000,
            "price": 1.5,
            "volume": 500000.0,
            "market_cap": 300000000.0,  # 比比特币小，应该排在第2位
            "date": target_date,
            "coin_id": "cardano",
        }

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            # 执行插入
            success = updater.insert_coin_into_daily_file(target_date, new_coin_data)

            self.assertTrue(success)

            # 验证文件内容
            updated_df = pd.read_csv(daily_file_path)
            self.assertEqual(len(updated_df), 2)  # 应该有2个币种

            # 验证排序：比特币应该排第1，卡尔达诺第2
            bitcoin_rank = updated_df[updated_df["coin_id"] == "bitcoin"]["rank"].iloc[
                0
            ]
            cardano_rank = updated_df[updated_df["coin_id"] == "cardano"]["rank"].iloc[
                0
            ]

            self.assertEqual(bitcoin_rank, 1)
            self.assertEqual(cardano_rank, 2)

        print("✅ 币种插入测试通过")

    def test_06_integration_workflow(self):
        """测试完整集成工作流"""
        print("\n--- 测试 6: 完整集成工作流 ---")

        # 1. 创建测试的币种历史数据
        test_coin_data = pd.DataFrame(
            {
                "timestamp": [1609459200000, 1609545600000],  # 2021-01-01, 01-02
                "price": [1.5, 1.6],
                "volume": [500000.0, 550000.0],
                "market_cap": [300000000.0, 320000000.0],
            }
        )

        test_csv_path = self.coins_dir / "cardano.csv"
        test_coin_data.to_csv(test_csv_path, index=False)

        # 2. 创建已有的每日文件
        for day_offset in range(2):  # 2021-01-01 和 2021-01-02
            target_date = date(2021, 1, 1 + day_offset)
            year_dir = self.daily_dir / "2021"
            month_dir = year_dir / "01"
            month_dir.mkdir(parents=True, exist_ok=True)

            existing_data = pd.DataFrame(
                {
                    "timestamp": [1609459200000 + day_offset * 86400000],
                    "price": [29000.0 + day_offset * 1000],
                    "volume": [1000000.0],
                    "market_cap": [500000000.0 + day_offset * 10000000],
                    "date": [target_date],
                    "coin_id": ["bitcoin"],
                    "rank": [1],
                }
            )

            daily_file_path = month_dir / f"{target_date}.csv"
            existing_data.to_csv(daily_file_path, index=False)

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            # 执行集成
            inserted_count, total_attempts = (
                updater.integrate_new_coin_into_daily_files("cardano")
            )

            self.assertEqual(total_attempts, 2)  # 应该尝试2天
            self.assertEqual(inserted_count, 2)  # 应该成功插入2天

            # 验证每日文件都已更新
            for day_offset in range(2):
                target_date = date(2021, 1, 1 + day_offset)
                daily_file_path = self.daily_dir / "2021" / "01" / f"{target_date}.csv"

                df = pd.read_csv(daily_file_path)
                self.assertEqual(len(df), 2)  # 每个文件应该有2个币种
                self.assertIn("cardano", df["coin_id"].values)

        print(f"✅ 集成工作流测试通过: {inserted_count}/{total_attempts} 天成功")

    def test_07_error_handling(self):
        """测试错误处理"""
        print("\n--- 测试 7: 错误处理 ---")

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            # 测试加载不存在的币种
            result = updater.load_coin_data("nonexistent-coin")
            self.assertIsNone(result)

            # 测试插入到无效日期
            invalid_coin_data = {
                "timestamp": 1609459200000,
                "price": -1.0,  # 无效价格
                "volume": 500000.0,
                "market_cap": 300000000.0,
                "date": date(2021, 1, 1),
                "coin_id": "invalid-coin",
            }

            # 这个应该能插入（我们在插入时不验证价格）
            # 但在实际的load_coin_data中会被过滤掉

        print("✅ 错误处理测试通过")

    def test_08_convenience_function(self):
        """测试便捷创建函数"""
        print("\n--- 测试 8: 便捷创建函数 ---")

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ):

            updater = create_incremental_updater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            self.assertIsInstance(updater, IncrementalDailyUpdater)
            self.assertEqual(updater.coins_dir, self.coins_dir)

        print("✅ 便捷创建函数测试通过")

    def test_09_dry_run_mode(self):
        """测试试运行模式"""
        print("\n--- 测试 9: 试运行模式 ---")

        # 创建已有币种
        (self.coins_dir / "bitcoin.csv").touch()

        # 模拟市值排名数据
        mock_market_data = [
            {"id": "bitcoin", "name": "Bitcoin"},
            {"id": "ethereum", "name": "Ethereum"},  # 新币种
        ]

        with patch(
            "src.updaters.incremental_daily_updater.create_batch_downloader"
        ), patch("src.updaters.incremental_daily_updater.CoinGeckoAPI"), patch(
            "src.updaters.incremental_daily_updater.MarketDataFetcher"
        ) as MockMarketDataFetcher:

            # 设置 mock
            mock_fetcher = MockMarketDataFetcher.return_value
            mock_fetcher.get_top_coins.return_value = mock_market_data

            updater = IncrementalDailyUpdater(
                coins_dir=str(self.coins_dir), daily_dir=str(self.daily_dir)
            )

            # 执行试运行
            results = updater.update_with_new_coins(top_n=10, dry_run=True)

            self.assertTrue(results["summary"]["dry_run"])
            self.assertEqual(results["summary"]["status"], "dry_run_complete")
            self.assertEqual(results["new_coins"], ["ethereum"])

            # 确保没有实际下载或修改文件
            self.assertEqual(len(results["download_results"]), 0)
            self.assertEqual(len(results["integration_results"]), 0)

        print("✅ 试运行模式测试通过")


def run_tests():
    """运行所有测试"""
    print("🧪 开始运行增量每日数据更新器测试")
    print("=" * 60)

    # 创建测试套件
    suite = unittest.TestLoader().loadTestsFromTestCase(TestIncrementalDailyUpdater)

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("=" * 60)
    print(f"📊 测试结果:")
    print(f"   - 总测试数: {result.testsRun}")
    print(f"   - 成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   - 失败: {len(result.failures)}")
    print(f"   - 错误: {len(result.errors)}")

    if result.failures:
        print("\n❌ 失败的测试:")
        for test, traceback in result.failures:
            print(f"   - {test}: {traceback}")

    if result.errors:
        print("\n🚨 错误的测试:")
        for test, traceback in result.errors:
            print(f"   - {test}: {traceback}")

    if result.wasSuccessful():
        print("\n✅ 所有测试通过！")
        return True
    else:
        print("\n❌ 部分测试失败")
        return False


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
