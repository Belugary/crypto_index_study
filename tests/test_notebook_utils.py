#!/usr/bin/env python3
"""
notebook_utils.py 测试模块

测试覆盖：
1. setup_project_imports 函数
2. get_daily_data_aggregator 函数
3. load_market_data 函数及其参数映射
4. include_all_coins 参数功能
5. force_refresh 参数功能
"""

import os
import sys
import unittest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd
from pathlib import Path

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.utils.notebook_utils import (
    setup_project_imports,
    get_daily_data_aggregator,
    load_market_data
)


class TestNotebookUtils(unittest.TestCase):
    """测试 notebook_utils 模块"""

    def setUp(self):
        """设置测试环境"""
        self.test_date = "2024-01-01"
        self.sample_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'solana'],
            'price': [50000, 3000, 100],
            'market_cap': [1000000000, 400000000, 50000000],
            'volume': [20000000, 10000000, 5000000]
        })

    def test_01_setup_project_imports(self):
        """测试项目导入路径设置"""
        print("\n--- 测试 1: 项目导入路径设置 ---")
        
        # 调用函数
        project_root = setup_project_imports()
        
        # 验证返回值
        self.assertIsInstance(project_root, Path)
        self.assertTrue(project_root.exists())
        self.assertTrue((project_root / ".git").exists())
        
        # 验证路径已添加到 sys.path
        self.assertIn(str(project_root), sys.path)
        
        print("✅ 项目导入路径设置测试通过")

    def test_02_get_daily_data_aggregator(self):
        """测试获取数据聚合器实例"""
        print("\n--- 测试 2: 获取数据聚合器实例 ---")
        
        # 调用函数
        aggregator = get_daily_data_aggregator()
        
        # 验证返回值
        self.assertIsNotNone(aggregator)
        self.assertTrue(hasattr(aggregator, 'get_daily_data'))
        
        print("✅ 数据聚合器实例获取测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_03_load_market_data_basic(self, mock_aggregator_class):
        """测试 load_market_data 基本功能"""
        print("\n--- 测试 3: load_market_data 基本功能 ---")
        
        # 设置模拟
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 调用函数
        result = load_market_data(self.test_date)
        
        # 验证调用
        mock_aggregator_class.assert_called_once()
        mock_aggregator.get_daily_data.assert_called_once_with(
            self.test_date,
            force_refresh=False,
            result_include_all=False  # 默认值
        )
        
        # 验证返回结果
        pd.testing.assert_frame_equal(result, self.sample_data)
        
        print("✅ load_market_data 基本功能测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_04_include_all_coins_parameter(self, mock_aggregator_class):
        """测试 include_all_coins 参数"""
        print("\n--- 测试 4: include_all_coins 参数 ---")
        
        # 设置模拟
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试 include_all_coins=True
        result = load_market_data(self.test_date, include_all_coins=True)
        
        # 验证调用参数
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=True  # 应该传递 True
        )
        
        # 重置模拟
        mock_aggregator.reset_mock()
        
        # 测试 include_all_coins=False
        result = load_market_data(self.test_date, include_all_coins=False)
        
        # 验证调用参数
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=False  # 应该传递 False
        )
        
        print("✅ include_all_coins 参数测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_05_force_refresh_parameter(self, mock_aggregator_class):
        """测试 force_refresh 参数"""
        print("\n--- 测试 5: force_refresh 参数 ---")
        
        # 设置模拟
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试 force_refresh=True
        result = load_market_data(self.test_date, force_refresh=True)
        
        # 验证调用参数
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=True,
            result_include_all=False
        )
        
        # 重置模拟
        mock_aggregator.reset_mock()
        
        # 测试 force_refresh=False
        result = load_market_data(self.test_date, force_refresh=False)
        
        # 验证调用参数
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=False
        )
        
        print("✅ force_refresh 参数测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_06_parameter_combinations(self, mock_aggregator_class):
        """测试参数组合"""
        print("\n--- 测试 6: 参数组合 ---")
        
        # 设置模拟
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试所有参数组合
        test_cases = [
            (True, True),   # force_refresh=True, include_all_coins=True
            (True, False),  # force_refresh=True, include_all_coins=False
            (False, True),  # force_refresh=False, include_all_coins=True
            (False, False)  # force_refresh=False, include_all_coins=False
        ]
        
        for force_refresh, include_all_coins in test_cases:
            with self.subTest(force_refresh=force_refresh, include_all_coins=include_all_coins):
                # 重置模拟
                mock_aggregator.reset_mock()
                
                # 调用函数
                result = load_market_data(
                    self.test_date, 
                    force_refresh=force_refresh,
                    include_all_coins=include_all_coins
                )
                
                # 验证调用参数
                mock_aggregator.get_daily_data.assert_called_with(
                    self.test_date,
                    force_refresh=force_refresh,
                    result_include_all=include_all_coins  # 直接映射
                )
        
        print("✅ 参数组合测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_07_parameter_mapping_correctness(self, mock_aggregator_class):
        """测试参数映射正确性"""
        print("\n--- 测试 7: 参数映射正确性 ---")
        
        # 设置模拟
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 验证参数映射逻辑
        # include_all_coins 应该直接映射到 result_include_all
        
        # 测试 include_all_coins=True → result_include_all=True
        load_market_data(self.test_date, include_all_coins=True)
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=True
        )
        
        # 重置并测试 include_all_coins=False → result_include_all=False
        mock_aggregator.reset_mock()
        load_market_data(self.test_date, include_all_coins=False)
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=False
        )
        
        print("✅ 参数映射正确性测试通过")

    def test_08_function_signature_consistency(self):
        """测试函数签名一致性"""
        print("\n--- 测试 8: 函数签名一致性 ---")
        
        # 检查函数签名
        import inspect
        
        # 获取 load_market_data 函数签名
        sig = inspect.signature(load_market_data)
        params = list(sig.parameters.keys())
        
        # 验证参数名称
        expected_params = ['date_str', 'force_refresh', 'include_all_coins']
        self.assertEqual(params, expected_params)
        
        # 验证默认值
        self.assertEqual(sig.parameters['force_refresh'].default, False)
        self.assertEqual(sig.parameters['include_all_coins'].default, False)
        
        print("✅ 函数签名一致性测试通过")


if __name__ == '__main__':
    unittest.main(verbosity=2)
