"""
指数计算模块测试
"""

import unittest
import pandas as pd
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, date
import sys
import os
from unittest.mock import patch, MagicMock

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator
from src.downloaders.daily_aggregator import DailyDataAggregator


class TestMarketCapWeightedIndexCalculatorModern(unittest.TestCase):
    """
    现代化测试：使用模拟的DailyDataAggregator和真实的计算逻辑
    """

    def setUp(self):
        """设置测试环境"""
        # 不再需要真实的文件系统
        self.mock_daily_data = self._create_mock_daily_data()

        # 初始化计算器，分类功能在测试中关闭
        self.calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=False,
            exclude_wrapped_coins=False,
        )

        # 创建一个补丁来模拟 get_daily_data 方法
        self.patcher = patch.object(
            self.calculator.daily_aggregator,
            "get_daily_data",
            side_effect=self.mock_get_daily_data,
        )
        self.mock_get_daily_data_func = self.patcher.start()

    def tearDown(self):
        """清理测试环境"""
        self.patcher.stop()

    def mock_get_daily_data(self, target_date, force_refresh=False, result_include_all=False, prefer_database=True):
        """模拟的 get_daily_data，从内存返回数据"""
        if isinstance(target_date, str):
            date_str = target_date
        else:
            date_str = target_date.isoformat()
        return self.mock_daily_data.get(date_str, pd.DataFrame())

    def _create_mock_daily_data(self):
        """创建模拟的每日汇总数据，并保存在内存中"""
        # 定义每日数据
        daily_data_map = {
            "2024-01-01": pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp("2024-01-01").timestamp() * 1000] * 3,
                    "price": [40000, 2500, 100],
                    "volume": [1e6, 5e5, 2e5],
                    "market_cap": [8e11, 3e11, 5e10],
                    "date": ["2024-01-01"] * 3,
                    "coin_id": ["bitcoin", "ethereum", "solana"],
                    "rank": [1, 2, 3],
                }
            ),
            "2024-01-02": pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp("2024-01-02").timestamp() * 1000] * 3,
                    "price": [41000, 2600, 105],
                    "volume": [1.1e6, 5.5e5, 2.2e5],
                    "market_cap": [8.2e11, 3.12e11, 5.25e10],
                    "date": ["2024-01-02"] * 3,
                    "coin_id": ["bitcoin", "ethereum", "solana"],
                    "rank": [1, 2, 3],
                }
            ),
            "2024-01-03": pd.DataFrame(
                {
                    "timestamp": [pd.Timestamp("2024-01-03").timestamp() * 1000] * 3,
                    "price": [42000, 2700, 110],
                    "volume": [1.2e6, 6e5, 2.4e5],
                    "market_cap": [8.4e11, 3.24e11, 5.5e10],
                    "date": ["2024-01-03"] * 3,
                    "coin_id": ["bitcoin", "ethereum", "solana"],
                    "rank": [1, 2, 3],
                }
            ),
        }
        return daily_data_map

    def test_calculate_index_normal(self):
        """测试正常情况下的指数计算"""
        index_df = self.calculator.calculate_index(
            start_date="2024-01-01",
            end_date="2024-01-02",
            base_date="2024-01-01",
            base_value=1000.0,
            top_n=2,
        )

        self.assertEqual(len(index_df), 2)
        self.assertAlmostEqual(index_df.iloc[0]["index_value"], 1000.0, places=5)
        self.assertGreater(index_df.iloc[1]["index_value"], 1000.0)
        self.assertEqual(index_df.iloc[0]["constituent_count"], 2)
        self.assertEqual(index_df.iloc[1]["constituent_count"], 2)

    def test_insufficient_coins_on_base_date(self):
        """测试基准日币种不足时应抛出ValueError"""
        with self.assertRaisesRegex(ValueError, "不足以满足 top_n=5 的要求"):
            self.calculator.calculate_index(
                start_date="2024-01-01",
                end_date="2024-01-02",
                base_date="2024-01-01",
                base_value=1000.0,
                top_n=5,  # 请求5个，但只有3个可用
            )

    def test_missing_data_on_current_date(self):
        """测试计算日缺少数据文件时的情况"""
        # 移除一天的模拟数据
        del self.mock_daily_data["2024-01-02"]

        index_df = self.calculator.calculate_index(
            start_date="2024-01-01",
            end_date="2024-01-03",
            base_date="2024-01-01",
            base_value=1000.0,
            top_n=2,
        )

        # 应该只计算了有数据的两天
        self.assertEqual(len(index_df), 2)
        self.assertEqual(index_df.iloc[0]["date"], date(2024, 1, 1))
        self.assertEqual(index_df.iloc[1]["date"], date(2024, 1, 3))

    def test_force_rebuild_flag(self):
        """测试 force_rebuild 标志是否正确传递以及缓存是否生效"""
        calculator = MarketCapWeightedIndexCalculator(
            force_rebuild=True, exclude_stablecoins=False, exclude_wrapped_coins=False
        )

        with patch.object(
            calculator.daily_aggregator,
            "get_daily_data",
            side_effect=self.mock_get_daily_data,
        ) as mock_get:
            # 第一次调用，应该触发强制刷新
            calculator._get_daily_data_cached(date(2024, 1, 1))

            # 验证 get_daily_data 被调用了一次，且 force_refresh=True
            mock_get.assert_called_once()
            self.assertTrue(mock_get.call_args.kwargs.get("force_refresh", False))

            # 第二次调用相同日期，应该从缓存返回，不再调用 get_daily_data
            calculator._get_daily_data_cached(date(2024, 1, 1))

            # 验证 get_daily_data 仍然只被调用了一次
            mock_get.assert_called_once()


if __name__ == "__main__":
    unittest.main()
