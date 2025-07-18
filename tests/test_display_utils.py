#!/usr/bin/env python3
"""
测试显示工具类 CryptoDataDisplayer

测试覆盖：
1. 类初始化
2. 数据格式化功能
3. 表格显示功能
4. 列映射功能
5. 数据类型处理
6. 边界情况处理
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
    
    def test_format_crypto_data_basic(self):
        """测试基本数据格式化"""
        columns = ['rank', 'symbol', 'name', 'price', 'market_cap']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查返回类型
        self.assertIsInstance(formatted, pd.DataFrame)
        
        # 检查列名映射
        expected_columns = ['排名', '代码', '币种名称', '价格($)', '市值(1M$)']
        self.assertEqual(list(formatted.columns), expected_columns)
        
        # 检查数据行数
        self.assertEqual(len(formatted), len(self.test_data))
    
    def test_format_crypto_data_symbol_uppercase(self):
        """测试符号大写转换"""
        columns = ['symbol']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查符号是否转为大写
        symbols = formatted['代码'].tolist()
        expected_symbols = ['BTC', 'ETH', 'XRP', 'BNB', 'SOL']
        self.assertEqual(symbols, expected_symbols)
    
    def test_format_crypto_data_name_corrections(self):
        """测试名称修正"""
        columns = ['name']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查名称修正
        names = formatted['币种名称'].tolist()
        # XRP 应该被修正为 Ripple, BNB 应该被修正为 Binance Coin
        self.assertIn('Ripple', names)
        self.assertIn('Binance Coin', names)
    
    def test_format_crypto_data_price_formatting(self):
        """测试价格格式化"""
        columns = ['price']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查价格格式化 - 应该有千分位分隔符和2位小数
        prices = formatted['价格($)'].tolist()
        
        # 检查格式
        for price in prices:
            self.assertIsInstance(price, str)
            # 应该包含千分位分隔符和小数点
            if float(price.replace(',', '')) >= 1000:
                self.assertIn(',', price)
    
    def test_format_crypto_data_market_cap_millions(self):
        """测试市值百万单位格式化"""
        columns = ['market_cap']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查市值格式化 - 应该转换为百万单位并有千分位分隔符
        market_caps = formatted['市值(1M$)'].tolist()
        
        for mc in market_caps:
            self.assertIsInstance(mc, str)
            # 检查是否有千分位分隔符
            if ',' in mc:
                # 确保可以正确解析
                value = int(mc.replace(',', ''))
                self.assertGreater(value, 0)
    
    def test_format_crypto_data_percentage_formatting(self):
        """测试百分比格式化"""
        columns = ['change_24h']
        formatted = self.displayer.format_crypto_data(self.test_data, columns)
        
        # 检查百分比格式化
        changes = formatted['24h涨跌(%)'].tolist()
        
        for change in changes:
            self.assertIsInstance(change, str)
            self.assertTrue(change.endswith('%'))
    
    def test_format_crypto_data_missing_values(self):
        """测试缺失值处理"""
        # 创建包含缺失值的测试数据
        test_data_with_na = self.test_data.copy()
        test_data_with_na.loc[0, 'price'] = np.nan
        test_data_with_na.loc[1, 'market_cap'] = np.nan
        
        columns = ['price', 'market_cap']
        formatted = self.displayer.format_crypto_data(test_data_with_na, columns)
        
        # 检查缺失值是否正确处理
        self.assertEqual(formatted.iloc[0]['价格($)'], 'N/A')
        self.assertEqual(formatted.iloc[1]['市值(1M$)'], 'N/A')
    
    def test_show_table_basic(self):
        """测试基本表格显示"""
        columns = ['rank', 'symbol', 'name', 'price', 'market_cap']
        
        # 捕获打印输出
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = self.displayer.show_table(
                data=self.test_data,
                columns=columns,
                top_n=3,
                title="测试表格"
            )
        
        # 检查返回结果
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # top_n=3
        
        # 检查是否有标题输出
        output = fake_out.getvalue()
        self.assertIn("测试表格", output)
    
    def test_show_table_default_columns(self):
        """测试默认列显示"""
        with patch('sys.stdout', new=StringIO()):
            result = self.displayer.show_table(data=self.test_data)
        
        # 当没有指定列时，应该显示所有可用列
        self.assertGreater(len(result.columns), 0)
    
    def test_show_table_top_n_boundary(self):
        """测试 top_n 边界情况"""
        columns = ['rank', 'name']
        
        # 测试 top_n 大于数据行数
        with patch('sys.stdout', new=StringIO()):
            result = self.displayer.show_table(
                data=self.test_data,
                columns=columns,
                top_n=10  # 大于实际数据行数
            )
        
        # 应该返回所有数据
        self.assertEqual(len(result), len(self.test_data))
    
    def test_invalid_columns(self):
        """测试无效列处理"""
        # 测试不存在的列
        invalid_columns = ['rank', 'nonexistent_column']
        
        # 应该能够处理无效列而不崩溃
        try:
            with patch('sys.stdout', new=StringIO()):
                result = self.displayer.show_table(
                    data=self.test_data,
                    columns=invalid_columns
                )
            # 如果没有抛出异常，则测试通过
            self.assertIsInstance(result, pd.DataFrame)
        except KeyError:
            # 如果抛出 KeyError，也是可以接受的行为
            pass
    
    def test_empty_dataframe(self):
        """测试空数据框处理"""
        empty_df = pd.DataFrame()
        
        with patch('sys.stdout', new=StringIO()):
            result = self.displayer.show_table(data=empty_df)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)


class TestCryptoDataDisplayerIntegration(unittest.TestCase):
    """CryptoDataDisplayer 集成测试"""
    
    def test_real_data_simulation(self):
        """测试模拟真实数据场景"""
        displayer = CryptoDataDisplayer()
        
        # 模拟真实的加密货币数据
        real_data = pd.DataFrame({
            'rank': range(1, 11),
            'coin_id': [f'coin_{i}' for i in range(1, 11)],
            'name': ['Bitcoin', 'Ethereum', 'XRP', 'BNB', 'Solana', 
                    'Cardano', 'Polygon', 'Chainlink', 'Litecoin', 'Avalanche'],
            'symbol': ['BTC', 'ETH', 'XRP', 'BNB', 'SOL', 
                      'ADA', 'MATIC', 'LINK', 'LTC', 'AVAX'],
            'price': [65432.10, 3187.50, 1.23, 542.30, 118.90,
                     0.85, 1.05, 14.20, 89.50, 35.60],
            'market_cap': [1.28e12, 3.8e11, 6.8e10, 8.0e10, 5.5e10,
                          3.2e10, 9.8e9, 8.5e9, 6.6e9, 1.4e10]
        })
        
        columns = ['rank', 'symbol', 'name', 'price', 'market_cap']
        
        with patch('sys.stdout', new=StringIO()) as fake_out:
            result = displayer.show_table(
                data=real_data,
                columns=columns,
                top_n=5,
                title="前5大加密货币"
            )
        
        # 验证结果
        self.assertEqual(len(result), 5)
        self.assertIn('排名', result.columns)
        self.assertIn('代码', result.columns)
        self.assertIn('币种名称', result.columns)
        
        # 验证输出包含标题
        output = fake_out.getvalue()
        self.assertIn("前5大加密货币", output)


def run_tests():
    """运行所有测试"""
    print("🧪 开始测试 CryptoDataDisplayer 类...")
    
    # 创建测试套件
    test_suite = unittest.TestSuite()
    
    # 添加基本功能测试
    test_suite.addTest(unittest.makeSuite(TestCryptoDataDisplayer))
    test_suite.addTest(unittest.makeSuite(TestCryptoDataDisplayerIntegration))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出测试结果摘要
    if result.wasSuccessful():
        print("\n✅ 所有测试通过！")
    else:
        print(f"\n❌ 测试失败: {len(result.failures)} 个失败, {len(result.errors)} 个错误")
        
        if result.failures:
            print("\n失败的测试:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback}")
        
        if result.errors:
            print("\n错误的测试:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
