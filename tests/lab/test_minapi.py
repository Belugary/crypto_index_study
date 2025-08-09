#!/usr/bin/env python3
"""
测试 minapi.py 模块的极简API功能

测试覆盖：
1. 基础数据获取函数 (daily, top, weights)
2. 指数计算函数 (index_value, index_series)
3. 显示函数 (show_top, prepare_index_base)
4. 边界情况和错误处理
"""

import sys
import unittest
import pandas as pd
import math
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.lab.minapi import daily, top, weights, index_value, index_series, show_top, prepare_index_base


class TestMinAPI(unittest.TestCase):
    """测试 minapi 模块"""
    
    def setUp(self):
        """测试准备"""
        # 创建测试数据
        self.test_data = pd.DataFrame({
            'symbol': ['BTC', 'ETH', 'ADA'],
            'name': ['Bitcoin', 'Ethereum', 'Cardano'], 
            'price': [50000.0, 3000.0, 1.0],
            'market_cap': [1000000000000, 400000000000, 30000000000],
            'rank': [1, 2, 3]
        })
    
    @patch('src.lab.minapi._env')
    def test_daily_function(self, mock_env):
        """测试 daily() 函数"""
        # 模拟环境
        mock_agg = MagicMock()
        mock_disp = MagicMock()
        mock_get_clean = MagicMock()
        mock_env.return_value = (mock_agg, mock_disp, mock_get_clean)
        
        # 测试清洗模式（默认）
        mock_get_clean.return_value = self.test_data
        result = daily("2024-01-01")
        mock_get_clean.assert_called_once_with("2024-01-01")
        pd.testing.assert_frame_equal(result, self.test_data)
        
        # 测试原始模式
        mock_agg.get_daily_data.return_value = self.test_data
        result = daily("2024-01-01", clean=False)
        mock_agg.get_daily_data.assert_called_once_with("2024-01-01", result_include_all=False)
    
    @patch('src.lab.minapi.daily')
    def test_top_function(self, mock_daily):
        """测试 top() 函数"""
        # 模拟数据
        mock_daily.return_value = self.test_data
        
        # 测试正常情况
        result = top("2024-01-01", n=2)
        
        # 检查结果
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]['symbol'], 'BTC')
        self.assertEqual(result.iloc[1]['symbol'], 'ETH')
        
        # 测试空数据情况
        empty_df = pd.DataFrame()
        mock_daily.return_value = empty_df
        result = top("2024-01-01", n=2)
        self.assertTrue(result.empty)
    
    @patch('src.lab.minapi.top')
    def test_weights_function(self, mock_top):
        """测试 weights() 函数"""
        # 模拟数据
        mock_top.return_value = self.test_data
        
        # 调用函数
        result = weights("2024-01-01", n=3)
        
        # 检查权重计算
        self.assertIn('weight', result.columns)
        total_weight = result['weight'].sum()
        self.assertAlmostEqual(total_weight, 1.0, places=6)
        
        # 检查权重分布（BTC应该有最高权重）
        btc_weight = result[result['symbol'] == 'BTC']['weight'].iloc[0]
        eth_weight = result[result['symbol'] == 'ETH']['weight'].iloc[0]
        self.assertGreater(btc_weight, eth_weight)
    
    @patch('src.lab.minapi.weights')
    @patch('src.lab.minapi._env')
    def test_index_value_function(self, mock_env, mock_weights):
        """测试 index_value() 函数"""
        # 模拟环境
        mock_agg = MagicMock()
        mock_disp = MagicMock()
        mock_get_clean = MagicMock()
        mock_env.return_value = (mock_agg, mock_disp, mock_get_clean)
        
        # 模拟基准日权重数据
        base_weights = self.test_data.copy()
        base_weights['weight'] = [0.7, 0.25, 0.05]
        mock_weights.return_value = base_weights
        
        # 模拟目标日数据（价格上涨）
        target_data = self.test_data.copy()
        target_data['market_cap'] = [1200000000000, 480000000000, 36000000000]  # 20%涨幅
        
        mock_agg.get_daily_data.return_value = target_data
        mock_disp.clean_data.return_value = target_data
        
        # 调用函数
        result = index_value("2023-01-01", "2024-01-01", n=3, base_value=1000.0)
        
        # 检查结果
        self.assertIsInstance(result, float)
        self.assertFalse(math.isnan(result))
        self.assertGreater(result, 1000.0)  # 价格上涨，指数应该上涨
    
    @patch('src.lab.minapi.index_value')
    def test_index_series_function(self, mock_index_value):
        """测试 index_series() 函数"""
        # 模拟指数值返回
        mock_index_value.side_effect = [1000.0, 1100.0, 1200.0, float('nan')]
        
        # 测试数据
        dates = ["2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01"]
        
        # 调用函数
        result = index_series("2023-01-01", dates, n=3, base_value=1000.0)
        
        # 检查结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # 排除NaN值
        self.assertIn('date', result.columns)
        self.assertIn('index_value', result.columns)
        
        # 检查数据
        self.assertEqual(result.iloc[0]['index_value'], 1000.0)
        self.assertEqual(result.iloc[1]['index_value'], 1100.0)
        self.assertEqual(result.iloc[2]['index_value'], 1200.0)
    
    @patch('src.lab.minapi.top')
    @patch('src.lab.minapi._env')
    def test_show_top_function(self, mock_env, mock_top):
        """测试 show_top() 函数"""
        # 模拟环境
        mock_agg = MagicMock()
        mock_disp = MagicMock()
        mock_get_clean = MagicMock()
        mock_env.return_value = (mock_agg, mock_disp, mock_get_clean)
        
        # 测试正常情况
        mock_top.return_value = self.test_data
        
        # 捕获输出
        with patch('sys.stdout', new=StringIO()):
            show_top("2024-01-01", n=3)
        
        # 验证调用
        mock_disp.show_table.assert_called_once()
        
        # 测试空数据情况
        mock_top.return_value = pd.DataFrame()
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            show_top("2024-01-01", n=3)
        
        output = fake_out.getvalue()
        self.assertIn("无数据", output)
    
    @patch('src.lab.minapi.weights')
    @patch('src.lab.minapi._env')
    def test_prepare_index_base_function(self, mock_env, mock_weights):
        """测试 prepare_index_base() 函数"""
        # 模拟环境
        mock_agg = MagicMock()
        mock_disp = MagicMock()
        mock_get_clean = MagicMock()
        mock_env.return_value = (mock_agg, mock_disp, mock_get_clean)
        
        # 模拟权重数据
        weights_data = self.test_data.copy()
        weights_data['weight'] = [0.7, 0.25, 0.05]
        mock_weights.return_value = weights_data
        
        # 模拟格式化功能
        formatted_data = weights_data.copy()
        formatted_data.columns = ['代码', '币种名称', '价格($)', '市值(1B$)', '排名', '权重(%)']
        mock_disp.format_crypto_data.return_value = formatted_data
        
        # 调用函数（不显示）
        components, index_info, formatted = prepare_index_base(
            "2023-01-01", n=3, base_value=1000.0, show=False
        )
        
        # 检查返回值
        self.assertIsInstance(components, pd.DataFrame)
        self.assertIsInstance(index_info, dict)
        self.assertIsInstance(formatted, pd.DataFrame)
        
        # 检查 index_info 内容
        self.assertEqual(index_info['name'], 'Crypto3 市值加权指数')
        self.assertEqual(index_info['base_date'], '2023-01-01')
        self.assertEqual(index_info['base_value'], 1000.0)
        self.assertEqual(index_info['constituents_count'], 3)
    
    def test_error_handling(self):
        """测试错误处理"""
        # 测试空数据情况
        with patch('src.lab.minapi._env') as mock_env:
            mock_agg = MagicMock()
            mock_disp = MagicMock()
            mock_get_clean = MagicMock()
            mock_env.return_value = (mock_agg, mock_disp, mock_get_clean)
            
            # 空数据情况
            mock_get_clean.return_value = pd.DataFrame()
            result = daily("2024-01-01")
            self.assertTrue(result.empty)


def run_tests():
    """运行所有测试"""
    print("🧪 开始测试 minapi 模块...")
    
    # 创建测试套件
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestMinAPI)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出结果统计
    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    success_rate = ((total_tests - failures - errors) / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"\n📊 测试结果统计:")
    print(f"   总测试数: {total_tests}")
    print(f"   成功: {total_tests - failures - errors}")
    print(f"   失败: {failures}")
    print(f"   错误: {errors}")
    print(f"   成功率: {success_rate:.1f}%")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
