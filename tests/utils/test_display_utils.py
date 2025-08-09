#!/usr/bin/env python3
"""
测试显示工具类 CryptoDataDisplayer

测试覆盖：
1. 类初始化
2. 数据清理功能
3. 数据格式化功能
4. 表格显示功能
5. 便捷函数测试
"""

import sys
import unittest
import pandas as pd
import numpy as np
from pathlib import Path
from io import StringIO
from unittest.mock import patch

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.display_utils import CryptoDataDisplayer


class TestCryptoDataDisplayer(unittest.TestCase):
    """CryptoDataDisplayer 类测试"""
    
    def setUp(self):
        """测试准备"""
        self.displayer = CryptoDataDisplayer()
        
        # 创建测试数据
        self.test_data = pd.DataFrame({
            'rank': [1, 2, 3, 4, 5],
            'coin_id': ['bitcoin', 'ethereum', 'ripple', 'binancecoin', 'solana'],
            'name': ['Bitcoin', 'Ethereum', 'XRP', 'BNB', 'Solana'],
            'symbol': ['btc', 'eth', 'xrp', 'bnb', 'sol'],
            'price': [65000.50, 3200.75, 1.25, 550.0, 120.30],
            'market_cap': [1280000000000, 380000000000, 68000000000, 80000000000, 55000000000],
            'volume': [25000000000, 15000000000, 3000000000, 2000000000, 1800000000],
            'change_24h': [2.5, -1.8, 5.2, 0.8, -3.1]
        })
    
    def test_initialization(self):
        """测试类初始化"""
        displayer = CryptoDataDisplayer()
        
        # 检查默认属性
        self.assertIsInstance(displayer.name_corrections, dict)
        self.assertIsInstance(displayer.column_mapping, dict)
        
        # 检查关键映射是否存在
        expected_mappings = ['rank', 'symbol', 'name', 'price', 'market_cap']
        for key in expected_mappings:
            self.assertIn(key, displayer.column_mapping)
    
    def test_clean_data_basic(self):
        """测试基本数据清理功能"""
        cleaned = self.displayer.clean_data(self.test_data)
        
        # 检查返回类型
        self.assertIsInstance(cleaned, pd.DataFrame)
        self.assertEqual(len(cleaned), len(self.test_data))
        
        # 检查数据完整性
        self.assertEqual(list(cleaned.columns), list(self.test_data.columns))
    
    def test_clean_data_empty_input(self):
        """测试空数据输入"""
        empty_data = pd.DataFrame()
        cleaned = self.displayer.clean_data(empty_data)
        
        # 应该返回空的DataFrame
        self.assertTrue(cleaned.empty)
    
    def test_clean_data_with_target_columns(self):
        """测试指定目标列的数据清理"""
        target_columns = ['rank', 'symbol', 'name', 'price']
        cleaned = self.displayer.clean_data(self.test_data, target_columns)
        
        # 检查只包含目标列
        self.assertEqual(list(cleaned.columns), target_columns)
        self.assertEqual(len(cleaned), len(self.test_data))
    
    def test_format_crypto_data_basic(self):
        """测试基本格式化功能"""
        formatted = self.displayer.format_crypto_data(self.test_data)
        expected_columns = ['排名', '代码', '币种名称', '价格($)', '市值(1B$)']
        self.assertEqual(list(formatted.columns), expected_columns)
        self.assertEqual(len(formatted), len(self.test_data))
    
    def test_format_crypto_data_price_formatting(self):
        """测试价格格式化"""
        columns = ['price']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查价格格式化 (保留4位小数，千分位分隔符)
        prices = formatted['价格($)'].tolist()
        self.assertEqual(prices[0], "65,000.5000")  # Bitcoin价格
        self.assertEqual(prices[1], "3,200.7500")   # Ethereum价格
    
    def test_format_crypto_data_market_cap_formatting(self):
        """测试市值格式化"""
        columns = ['market_cap']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        market_caps = formatted['市值(1B$)'].tolist()
        self.assertEqual(market_caps[0], "1,280")
        self.assertEqual(market_caps[1], "380")
    
    def test_format_crypto_data_name_corrections(self):
        """测试名称修正"""
        columns = ['name']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查名称修正
        names = formatted['币种名称'].tolist()
        # XRP 应该被修正为 Ripple, BNB 应该被修正为 Binance Coin
        self.assertIn('Ripple', names)
        self.assertIn('Binance Coin', names)
    
    def test_format_crypto_data_empty_dataframe(self):
        """测试空数据框的处理"""
        empty_data = pd.DataFrame()
        formatted = self.displayer.format_crypto_data(empty_data)
        
        # 检查返回空数据框
        self.assertTrue(formatted.empty)
    
    def test_show_table(self):
        """测试表格显示功能 (更新: 新版不再显示 '显示前 N 行数据' 提示)"""
        # 捕获输出
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = self.displayer.show_table(self.test_data, top_n=3, title="测试表格", show_info=True)

        # 在Jupyter环境外应该返回DataFrame，在Jupyter环境内返回None
        if result is not None:
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)

        # 检查控制台输出（新版应包含标题与行数信息）
        output = fake_out.getvalue()
        self.assertIn("📊 测试表格", output)
        self.assertIn("(rows=3)", output)
    
    def test_rank_reordering(self):
        """测试排名重排功能"""
        # 创建带有跳号的测试数据
        data_with_gaps = self.test_data.copy()
        data_with_gaps['rank'] = [1, 3, 7, 10, 15]  # 故意创建跳号
        
        # 清理数据（应该重新计算排名）
        cleaned = self.displayer.clean_data(data_with_gaps)
        
        # 检查排名是否连续
        expected_ranks = [1, 2, 3, 4, 5]
        actual_ranks = cleaned['rank'].tolist()
        self.assertEqual(actual_ranks, expected_ranks)
        
        # 检查数据按市值降序排列
        market_caps = cleaned['market_cap'].tolist()
        self.assertEqual(market_caps, sorted(market_caps, reverse=True))
    
    def test_metadata_fields_handling(self):
        """测试元数据字段处理"""
        # 创建只有coin_id的数据（模拟缺失symbol和name的情况）
        minimal_data = pd.DataFrame({
            'coin_id': ['bitcoin', 'ethereum', 'ripple'],
            'price': [65000.50, 3200.75, 1.25],
            'market_cap': [1280000000000, 380000000000, 68000000000],
            'rank': [1, 2, 3]
        })
        
        # 清理数据应该尝试添加元数据字段
        with patch('sys.stdout', new=StringIO()):
            cleaned = self.displayer.clean_data(minimal_data)
        
        # 应该添加了symbol和name列（即使可能为空）
        self.assertIn('symbol', cleaned.columns)
        self.assertIn('name', cleaned.columns)
    
    def test_symbol_uppercase_conversion(self):
        """测试符号大写转换功能"""
        formatted = self.displayer.format_crypto_data(self.test_data)
        
        # 检查所有符号都是大写
        symbols = formatted['代码'].tolist()
        for symbol in symbols:
            self.assertEqual(symbol, symbol.upper())
            
        # 检查具体的转换
        self.assertIn('BTC', symbols)
        self.assertIn('ETH', symbols)
        self.assertIn('XRP', symbols)

    def test_weight_without_percent_symbol(self):
        """测试权重列不含百分号 (表头含(%) 但单元格纯数字)"""
        data = self.test_data.copy()
        data['weight'] = [50.0, 25.0, 10.0, 8.0, 7.0]
        formatted = self.displayer.format_crypto_data(data, ['weight'])
        col_name = '权重(%)'
        self.assertIn(col_name, formatted.columns)
        # 所有单元格不应包含 '%'
        for v in formatted[col_name].tolist():
            self.assertFalse('%' in v)


def run_tests():
    """运行所有测试"""
    print("🧪 开始测试 CryptoDataDisplayer 类...")
    
    # 创建测试套件
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestCryptoDataDisplayer)
    
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
