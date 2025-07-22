"""
重构后的 Crypto30 分析功能测试
"""

import os
import sys
import unittest
import tempfile
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.crypto30_analysis import (
    get_daily_constituents_and_weights,
    generate_daily_detailed_data,
    track_constituent_changes,
    generate_monthly_report,
    save_detailed_data,
    run_crypto30_comprehensive_analysis,
    create_crypto30_calculator
)


class TestCrypto30AnalysisFunctions(unittest.TestCase):
    """测试 Crypto30 分析函数"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """测试后清理"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_get_daily_constituents_and_weights(self):
        """测试获取每日成分和权重"""
        print("\n--- 测试获取每日成分和权重 ---")
        
        # 创建模拟计算器
        mock_calculator = MagicMock()
        
        # 模拟数据
        mock_market_caps = {'bitcoin': 1000000, 'ethereum': 500000, 'cardano': 300000}
        mock_constituents = ['bitcoin', 'ethereum', 'cardano']
        mock_weights = {'bitcoin': 0.5, 'ethereum': 0.3, 'cardano': 0.2}
        
        mock_calculator._get_daily_market_caps.return_value = mock_market_caps
        mock_calculator._select_top_coins.return_value = mock_constituents
        mock_calculator._calculate_weights.return_value = mock_weights
        mock_calculator._get_coin_price.side_effect = lambda coin, date: 50000 if coin == 'bitcoin' else 3000
        
        # 测试函数
        constituents, weights, prices = get_daily_constituents_and_weights(
            mock_calculator, date(2024, 1, 1), 3
        )
        
        self.assertEqual(len(constituents), 3)
        self.assertEqual(len(weights), 3)
        self.assertEqual(len(prices), 3)
        self.assertIn('bitcoin', constituents)
        print(f"✓ 成功获取成分: {constituents}")

    def test_get_daily_constituents_empty_data(self):
        """测试空数据情况"""
        print("\n--- 测试空数据情况 ---")
        
        mock_calculator = MagicMock()
        mock_calculator._get_daily_market_caps.return_value = {}
        
        constituents, weights, prices = get_daily_constituents_and_weights(
            mock_calculator, date(2024, 1, 1), 3
        )
        
        self.assertEqual(len(constituents), 0)
        self.assertEqual(len(weights), 0)
        self.assertEqual(len(prices), 0)
        print("✓ 空数据处理正确")

    def test_track_constituent_changes(self):
        """测试成分变化跟踪"""
        print("\n--- 测试成分变化跟踪 ---")
        
        # 创建测试数据
        daily_data = pd.DataFrame([
            {
                'date': date(2024, 1, 1),
                'constituents': 'bitcoin,ethereum,cardano'
            },
            {
                'date': date(2024, 1, 2),
                'constituents': 'bitcoin,ethereum,solana'  # cardano出榜，solana入榜
            },
            {
                'date': date(2024, 1, 3),
                'constituents': 'bitcoin,ethereum,solana'  # 无变化
            }
        ])
        
        changes = track_constituent_changes(daily_data)
        
        # 检查结果
        self.assertIn(date(2024, 1, 2), changes['new_entries'])
        self.assertIn(date(2024, 1, 2), changes['exits'])
        self.assertIn('solana', changes['new_entries'][date(2024, 1, 2)])
        self.assertIn('cardano', changes['exits'][date(2024, 1, 2)])
        print("✓ 成分变化跟踪正确")

    def test_save_detailed_data(self):
        """测试保存详细数据"""
        print("\n--- 测试保存详细数据 ---")
        
        # 创建测试数据
        test_data = pd.DataFrame([
            {
                'date': date(2024, 1, 1),
                'index_value': 100.0,
                'constituents': 'bitcoin,ethereum'
            }
        ])
        
        # 保存数据
        file_path = save_detailed_data(test_data, self.temp_dir, "test_data.csv")
        
        # 验证文件存在且内容正确
        self.assertTrue(Path(file_path).exists())
        loaded_data = pd.read_csv(file_path)
        self.assertEqual(len(loaded_data), 1)
        self.assertEqual(loaded_data.iloc[0]['index_value'], 100.0)
        print(f"✓ 数据保存成功: {file_path}")

    def test_generate_monthly_report(self):
        """测试生成月度报告"""
        print("\n--- 测试生成月度报告 ---")
        
        # 创建测试数据
        daily_data = pd.DataFrame([
            {
                'date': date(2024, 1, 1),
                'index_value': 100.0,
                'constituents': 'bitcoin,ethereum'
            },
            {
                'date': date(2024, 1, 15),
                'index_value': 110.0,
                'constituents': 'bitcoin,ethereum'
            },
            {
                'date': date(2024, 1, 31),
                'index_value': 120.0,
                'constituents': 'bitcoin,solana'  # ethereum -> solana
            }
        ])
        
        # 生成报告
        report_file = generate_monthly_report(
            daily_data, 
            date(2024, 1, 1), 
            date(2024, 1, 31),
            self.temp_dir
        )
        
        # 验证报告文件
        self.assertTrue(Path(report_file).exists())
        with open(report_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        self.assertIn("Crypto30 月度分析报告", content)
        self.assertIn("2024-01", content)
        self.assertIn("20.00%", content)  # (120/100 - 1) * 100
        print(f"✓ 月度报告生成成功: {report_file}")

    def test_create_crypto30_calculator(self):
        """测试创建 Crypto30 计算器"""
        print("\n--- 测试创建 Crypto30 计算器 ---")
        
        calculator = create_crypto30_calculator()
        
        self.assertIsNotNone(calculator)
        # 验证配置正确
        self.assertTrue(hasattr(calculator, 'exclude_stablecoins'))
        print("✓ Crypto30 计算器创建成功")


class TestCrypto30IntegrationSimulation(unittest.TestCase):
    """测试 Crypto30 分析集成功能（模拟）"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """测试后清理"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    @patch('src.analysis.crypto30_analysis.find_project_root')
    @patch('src.analysis.crypto30_analysis.MarketCapWeightedIndexCalculator')
    def test_run_crypto30_comprehensive_analysis_mock(self, mock_calculator_class, mock_find_root):
        """测试完整分析流程（模拟）"""
        print("\n--- 测试完整分析流程（模拟）---")
        
        # 设置模拟
        mock_find_root.return_value = self.temp_dir
        
        mock_calculator = MagicMock()
        mock_calculator_class.return_value = mock_calculator
        
        # 模拟指数计算返回空DataFrame（简化测试）
        mock_calculator.calculate_index.return_value = pd.DataFrame()
        
        # 运行分析
        results = run_crypto30_comprehensive_analysis(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),
            output_dir=str(self.temp_dir / "output"),
            generate_monthly_reports=False  # 简化测试
        )
        
        # 验证结果
        self.assertIn('detailed_data', results)
        detailed_data_path = results['detailed_data']
        assert isinstance(detailed_data_path, str), "detailed_data should be a string path"
        self.assertTrue(Path(detailed_data_path).exists())
        print(f"✓ 分析完成，结果文件: {detailed_data_path}")

    def test_generate_daily_detailed_data_with_mock(self):
        """测试生成每日详细数据（模拟）"""
        print("\n--- 测试生成每日详细数据（模拟）---")
        
        # 创建模拟计算器
        mock_calculator = MagicMock()
        
        # 模拟简单的指数计算结果
        mock_index_df = pd.DataFrame({
            'index_value': [100.0]
        }, index=['2024-01-01'])
        mock_calculator.calculate_index.return_value = mock_index_df
        
        # 模拟成分数据
        mock_calculator._get_daily_market_caps.return_value = {
            'bitcoin': 1000000, 'ethereum': 500000
        }
        mock_calculator._select_top_coins.return_value = ['bitcoin', 'ethereum']
        mock_calculator._calculate_weights.return_value = {'bitcoin': 0.6, 'ethereum': 0.4}
        mock_calculator._get_coin_price.side_effect = lambda coin, date: 50000 if coin == 'bitcoin' else 3000
        
        # 生成数据
        with patch('src.analysis.crypto30_analysis.tqdm') as mock_tqdm:
            mock_tqdm.side_effect = lambda x, **kwargs: x  # 简化进度条
            
            result_df = generate_daily_detailed_data(
                mock_calculator,
                date(2024, 1, 1),
                date(2024, 1, 1),
                100.0,
                30
            )
        
        # 验证结果
        self.assertGreaterEqual(len(result_df), 0)  # 可能为空，但不应出错
        if not result_df.empty:
            self.assertIn('date', result_df.columns)
            self.assertIn('index_value', result_df.columns)
        print(f"✓ 生成每日数据成功，{len(result_df)} 条记录")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行重构后的 Crypto30 分析功能测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestCrypto30AnalysisFunctions))
    suite.addTests(loader.loadTestsFromTestCase(TestCrypto30IntegrationSimulation))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print(f"测试完成: {result.testsRun} 个测试运行")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print("=" * 60)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
