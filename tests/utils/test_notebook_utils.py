#!/usr/bin/env python3
"""
notebook_utils.py 测试模块

测试覆盖：
1. load_market_data 函数基本功能
2. include_all_coins 参数测试
3. force_refresh 参数测试
4. 参数映射正确性验证
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
import pandas as pd

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.utils.notebook_utils import load_market_data, setup_project_imports, get_daily_data_aggregator


class TestNotebookUtils(unittest.TestCase):
    """测试 notebook_utils 模块"""
    
    def setUp(self):
        """测试前准备"""
        self.test_date = "2024-12-01"
        self.sample_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'tether'],
            'price': [96513.14, 3709.91, 1.00],
            'market_cap': [1910022814269, 446851521665, 134096975603],
            'volume': [43580019720, 31961998508, 50885084855],
            'rank': [1, 2, 3],
            'date': ['2024-12-01', '2024-12-01', '2024-12-01']
        })
        
        self.native_only_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum'],
            'price': [96513.14, 3709.91],
            'market_cap': [1910022814269, 446851521665],
            'volume': [43580019720, 31961998508],
            'rank': [1, 2],
            'date': ['2024-12-01', '2024-12-01']
        })

    def test_01_setup_project_imports(self):
        """测试项目导入路径设置"""
        print("\n--- 测试 1: 项目导入路径设置 ---")
        
        project_root = setup_project_imports()
        
        # 验证返回的是 Path 对象
        self.assertIsNotNone(project_root)
        self.assertTrue(str(project_root) in sys.path)
        
        print("✅ 项目导入路径设置测试通过")

    def test_02_get_daily_data_aggregator(self):
        """测试获取数据聚合器实例"""
        print("\n--- 测试 2: 获取数据聚合器实例 ---")
        
        aggregator = get_daily_data_aggregator()
        
        # 验证返回的是正确的类型
        from src.downloaders.daily_aggregator import DailyDataAggregator
        self.assertIsInstance(aggregator, DailyDataAggregator)
        
        print("✅ 数据聚合器实例获取测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_03_load_market_data_basic(self, mock_aggregator_class):
        """测试 load_market_data 基本功能"""
        print("\n--- 测试 3: load_market_data 基本功能 ---")
        
        # 设置 mock
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
        
        # 设置 mock
        mock_aggregator = MagicMock()
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试 include_all_coins=False (默认)
        mock_aggregator.get_daily_data.return_value = self.native_only_data
        result_native = load_market_data(self.test_date, include_all_coins=False)
        
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=False
        )
        self.assertEqual(len(result_native), 2)  # 只有原生币种
        
        # 测试 include_all_coins=True
        mock_aggregator.get_daily_data.return_value = self.sample_data
        result_all = load_market_data(self.test_date, include_all_coins=True)
        
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=False,
            result_include_all=True
        )
        self.assertEqual(len(result_all), 3)  # 包含所有币种
        
        print("✅ include_all_coins 参数测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_05_force_refresh_parameter(self, mock_aggregator_class):
        """测试 force_refresh 参数"""
        print("\n--- 测试 5: force_refresh 参数 ---")
        
        # 设置 mock
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试 force_refresh=True
        load_market_data(self.test_date, force_refresh=True)
        
        mock_aggregator.get_daily_data.assert_called_with(
            self.test_date,
            force_refresh=True,
            result_include_all=False
        )
        
        # 测试 force_refresh=False (默认)
        load_market_data(self.test_date, force_refresh=False)
        
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
        
        # 设置 mock
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 测试所有参数组合
        test_cases = [
            (False, False),  # 默认值
            (True, False),   # 强制刷新，只要原生币
            (False, True),   # 不刷新，要所有币种
            (True, True),    # 强制刷新，要所有币种
        ]
        
        for force_refresh, include_all_coins in test_cases:
            with self.subTest(force_refresh=force_refresh, include_all_coins=include_all_coins):
                load_market_data(
                    self.test_date,
                    force_refresh=force_refresh,
                    include_all_coins=include_all_coins
                )
                
                mock_aggregator.get_daily_data.assert_called_with(
                    self.test_date,
                    force_refresh=force_refresh,
                    result_include_all=include_all_coins
                )
        
        print("✅ 参数组合测试通过")

    @patch('src.downloaders.daily_aggregator.DailyDataAggregator')
    def test_07_parameter_mapping_correctness(self, mock_aggregator_class):
        """测试参数映射正确性"""
        print("\n--- 测试 7: 参数映射正确性 ---")
        
        # 设置 mock
        mock_aggregator = MagicMock()
        mock_aggregator.get_daily_data.return_value = self.sample_data
        mock_aggregator_class.return_value = mock_aggregator
        
        # 验证参数映射逻辑
        # include_all_coins 应该直接对应 result_include_all
        
        # include_all_coins=False → result_include_all=False (只要原生币)
        load_market_data(self.test_date, include_all_coins=False)
        args, kwargs = mock_aggregator.get_daily_data.call_args
        self.assertEqual(kwargs['result_include_all'], False)
        
        # include_all_coins=True → result_include_all=True (要所有币种)
        load_market_data(self.test_date, include_all_coins=True)
        args, kwargs = mock_aggregator.get_daily_data.call_args
        self.assertEqual(kwargs['result_include_all'], True)
        
        print("✅ 参数映射正确性测试通过")

    def test_08_function_signature_consistency(self):
        """测试函数签名一致性"""
        print("\n--- 测试 8: 函数签名一致性 ---")
        
        import inspect
        from src.utils.notebook_utils import load_market_data
        from src.downloaders.daily_aggregator import DailyDataAggregator
        
        # 获取函数签名
        load_sig = inspect.signature(load_market_data)
        get_daily_sig = inspect.signature(DailyDataAggregator().get_daily_data)
        
        # 验证参数存在
        self.assertIn('force_refresh', load_sig.parameters)
        self.assertIn('include_all_coins', load_sig.parameters)
        self.assertIn('force_refresh', get_daily_sig.parameters)
        self.assertIn('result_include_all', get_daily_sig.parameters)
        
        # 验证默认值
        self.assertEqual(load_sig.parameters['force_refresh'].default, False)
        self.assertEqual(load_sig.parameters['include_all_coins'].default, False)
        self.assertEqual(get_daily_sig.parameters['force_refresh'].default, False)
        self.assertEqual(get_daily_sig.parameters['result_include_all'].default, False)
        
        print("✅ 函数签名一致性测试通过")


if __name__ == '__main__':
    unittest.main()
