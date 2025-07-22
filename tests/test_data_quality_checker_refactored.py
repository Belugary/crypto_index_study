"""
重构后的数据质量检查器测试
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

from src.analysis.data_quality_config import DataQualityConfig, ResolvedPaths
from src.analysis.data_quality_checker import (
    DataQualityChecker, 
    create_data_quality_checker,
    is_data_recent,
    check_timestamp_intervals,
    analyze_file_quality
)


class TestDataQualityConfig(unittest.TestCase):
    """测试数据质量配置"""

    def test_config_defaults(self):
        """测试配置默认值"""
        print("\n--- 测试配置默认值 ---")
        
        config = DataQualityConfig()
        
        self.assertEqual(config.data_dir, "data/coins")
        self.assertEqual(config.min_rows, 100)
        self.assertEqual(config.max_days_old, 2)
        self.assertEqual(config.use_database, True)
        print("✓ 默认配置正确")

    def test_config_custom_values(self):
        """测试自定义配置值"""
        print("\n--- 测试自定义配置值 ---")
        
        config = DataQualityConfig(
            data_dir="custom/path",
            min_rows=200,
            max_days_old=5,
            use_database=False
        )
        
        self.assertEqual(config.data_dir, "custom/path")
        self.assertEqual(config.min_rows, 200)
        self.assertEqual(config.max_days_old, 5)
        self.assertEqual(config.use_database, False)
        print("✓ 自定义配置正确")

    def test_resolve_paths(self):
        """测试路径解析"""
        print("\n--- 测试路径解析 ---")
        
        temp_dir = Path(tempfile.mkdtemp())
        try:
            config = DataQualityConfig(data_dir="data/coins")
            resolved = config.resolve_paths(temp_dir)
            
            self.assertIsInstance(resolved, ResolvedPaths)
            self.assertEqual(resolved.project_root, temp_dir)
            self.assertEqual(resolved.data_dir, temp_dir / "data" / "coins")
            print(f"✓ 路径解析正确: {resolved.data_dir}")
        finally:
            shutil.rmtree(temp_dir)


class TestDataQualityUtilFunctions(unittest.TestCase):
    """测试数据质量工具函数"""

    def test_is_data_recent_new_coin(self):
        """测试新币种的数据时效性判断"""
        print("\n--- 测试新币种的数据时效性判断 ---")
        
        # 新币种（数据跨度 < 30天），给予宽松标准
        result = is_data_recent(
            data_span_days=20,
            days_since_latest=5,
            min_data_span_days=30,
            max_days_old=2
        )
        self.assertTrue(result)
        print("✓ 新币种5天前的数据被认为是最新的")

    def test_is_data_recent_old_coin(self):
        """测试老币种的数据时效性判断"""
        print("\n--- 测试老币种的数据时效性判断 ---")
        
        # 老币种（数据跨度 >= 30天），使用严格标准
        result = is_data_recent(
            data_span_days=100,
            days_since_latest=3,
            min_data_span_days=30,
            max_days_old=2
        )
        self.assertFalse(result)
        print("✓ 老币种3天前的数据被认为不是最新的")

    def test_check_timestamp_intervals_normal(self):
        """测试正常的时间戳间隔"""
        print("\n--- 测试正常的时间戳间隔 ---")
        
        # 创建连续的日期数据
        dates = pd.date_range('2024-01-01', periods=5, freq='D')
        df = pd.DataFrame({'date': dates})
        
        is_ok, message = check_timestamp_intervals(df, 'date')
        self.assertTrue(is_ok)
        self.assertEqual(message, "时间间隔正常")
        print("✓ 连续日期被识别为正常间隔")

    def test_check_timestamp_intervals_gaps(self):
        """测试有间隔的时间戳"""
        print("\n--- 测试有间隔的时间戳 ---")
        
        # 创建有大间隔的日期数据
        dates = [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 15),  # 13天间隔
        ]
        df = pd.DataFrame({'date': dates})
        
        is_ok, message = check_timestamp_intervals(df, 'date')
        self.assertFalse(is_ok)
        self.assertIn("发现大时间缺失", message)
        print(f"✓ 大间隔被正确识别: {message}")

    def test_analyze_file_quality(self):
        """测试文件质量分析"""
        print("\n--- 测试文件质量分析 ---")
        
        # 创建临时CSV文件
        temp_dir = Path(tempfile.mkdtemp())
        try:
            csv_file = temp_dir / "test_coin.csv"
            
            # 创建测试数据 - 使用最近的日期确保通过时效性检查
            dates = pd.date_range(end=datetime.now().date(), periods=150, freq='D')
            df = pd.DataFrame({
                'date': dates,
                'price': range(150),
                'market_cap': range(1000, 1150)
            })
            df.to_csv(csv_file, index=False)
            
            # 分析质量
            result = analyze_file_quality(csv_file, min_rows=100, max_days_old=2, min_data_span_days=30)
            
            self.assertGreaterEqual(result["rows"], 100)
            self.assertTrue(result["has_enough_data"])
            self.assertIsNotNone(result["latest_date"])
            print(f"✓ 文件质量分析成功: {result['rows']} 行数据，最新日期: {result['latest_date']}")
            
        finally:
            shutil.rmtree(temp_dir)


class TestDataQualityChecker(unittest.TestCase):
    """测试数据质量检查器"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.data_dir = self.temp_dir / "data" / "coins"
        self.data_dir.mkdir(parents=True)

    def tearDown(self):
        """测试后清理"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_checker_initialization(self):
        """测试检查器初始化"""
        print("\n--- 测试检查器初始化 ---")
        
        config = DataQualityConfig(data_dir=str(self.data_dir))
        checker = DataQualityChecker(config)
        
        self.assertIsNotNone(checker.config)
        self.assertIsNotNone(checker.paths)
        self.assertEqual(checker.paths.data_dir, self.data_dir)
        print("✓ 检查器初始化成功")

    def test_checker_with_default_config(self):
        """测试使用默认配置的检查器"""
        print("\n--- 测试使用默认配置的检查器 ---")
        
        # 模拟项目根目录
        with patch('src.utils.path_utils.find_project_root') as mock_find_root:
            mock_find_root.return_value = self.temp_dir
            
            checker = DataQualityChecker()
            
            self.assertIsNotNone(checker.config)
            print("✓ 默认配置检查器创建成功")

    def test_scan_empty_directory(self):
        """测试扫描空目录"""
        print("\n--- 测试扫描空目录 ---")
        
        config = DataQualityConfig(data_dir=str(self.data_dir))
        checker = DataQualityChecker(config)
        
        good_files, problematic_files = checker.scan_all_files()
        
        self.assertEqual(len(good_files), 0)
        self.assertEqual(len(problematic_files), 0)
        print("✓ 空目录扫描结果正确")

    def test_scan_files_with_quality_issues(self):
        """测试扫描有质量问题的文件"""
        print("\n--- 测试扫描有质量问题的文件 ---")
        
        # 创建测试文件
        # 1. 正常文件 - 确保数据是最近的
        good_file = self.data_dir / "bitcoin.csv"
        recent_dates = pd.date_range(end=datetime.now().date(), periods=150, freq='D')
        good_df = pd.DataFrame({
            'date': recent_dates,
            'price': range(150)
        })
        good_df.to_csv(good_file, index=False)
        
        # 2. 数据不足文件
        bad_file = self.data_dir / "small_coin.csv"
        small_df = pd.DataFrame({
            'date': pd.date_range(end=datetime.now().date(), periods=50, freq='D'),
            'price': range(50)
        })
        small_df.to_csv(bad_file, index=False)
        
        # 扫描文件
        config = DataQualityConfig(data_dir=str(self.data_dir), min_rows=100)
        checker = DataQualityChecker(config)
        
        good_files, problematic_files = checker.scan_all_files()
        
        self.assertEqual(len(good_files), 1)
        self.assertEqual(len(problematic_files), 1)
        self.assertEqual(good_files[0][0], "bitcoin")
        self.assertEqual(problematic_files[0][2], "INSUFFICIENT_DATA")
        print(f"✓ 扫描结果正确: {len(good_files)} 个正常文件, {len(problematic_files)} 个问题文件")

    def test_get_quality_summary(self):
        """测试获取质量摘要"""
        print("\n--- 测试获取质量摘要 ---")
        
        # 创建测试文件 - 使用最近的日期
        test_file = self.data_dir / "test_coin.csv"
        df = pd.DataFrame({
            'date': pd.date_range(end=datetime.now().date(), periods=50, freq='D'),
            'price': range(50)
        })
        df.to_csv(test_file, index=False)
        
        config = DataQualityConfig(data_dir=str(self.data_dir), min_rows=100)
        checker = DataQualityChecker(config)
        
        summary = checker.get_quality_summary()
        
        self.assertEqual(summary["total_files"], 1)
        self.assertEqual(summary["good_files"], 0)
        self.assertEqual(summary["problematic_files"], 1)
        self.assertIn("INSUFFICIENT_DATA", summary["issue_breakdown"])
        print(f"✓ 质量摘要正确: {summary}")


class TestConvenienceFunctions(unittest.TestCase):
    """测试便捷函数"""

    def test_create_data_quality_checker(self):
        """测试便捷函数创建检查器"""
        print("\n--- 测试便捷函数创建检查器 ---")
        
        with patch('src.utils.path_utils.find_project_root') as mock_find_root:
            mock_find_root.return_value = Path("/tmp/project")
            
            checker = create_data_quality_checker(data_dir="custom/path")
            
            self.assertIsInstance(checker, DataQualityChecker)
            self.assertEqual(checker.config.data_dir, "custom/path")
            print("✓ 便捷函数创建检查器成功")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行重构后的数据质量检查器测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityUtilFunctions))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityChecker))
    suite.addTests(loader.loadTestsFromTestCase(TestConvenienceFunctions))

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
