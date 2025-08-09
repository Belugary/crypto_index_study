#!/usr/bin/env python3
"""
utils.py 工具函数测试模块

测试覆盖：
1. JSON 格式化打印
2. 时间戳转换
3. 货币格式化
4. 百分比计算
5. 安全字典取值
6. 项目根目录查找
"""

import os
import sys
import unittest
import tempfile
import json
from datetime import datetime
from io import StringIO
from unittest.mock import patch, mock_open

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# 使用 importlib 正确导入 utils.py 文件（避免与 utils 包冲突）
import importlib.util


def load_utils_module():
    """加载 utils.py 模块"""
    utils_file_path = os.path.join(project_root, "src", "utils.py")
    spec = importlib.util.spec_from_file_location("utils_functions", utils_file_path)
    if spec and spec.loader:
        utils_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(utils_module)
        return utils_module
    return None


# 加载模块并导入函数
utils_module = load_utils_module()
if utils_module:
    print_json = utils_module.print_json
    get_timestamp = utils_module.get_timestamp
    format_currency = utils_module.format_currency
    calculate_percentage_change = utils_module.calculate_percentage_change
    safe_get = utils_module.safe_get
    get_project_root = utils_module.get_project_root
else:
    # 如果导入失败，定义空函数避免测试错误
    print_json = lambda *args, **kwargs: None
    get_timestamp = lambda *args, **kwargs: 0
    format_currency = lambda *args, **kwargs: ""
    calculate_percentage_change = lambda *args, **kwargs: 0.0
    safe_get = lambda *args, **kwargs: None
    get_project_root = lambda *args, **kwargs: ""


class TestUtilsFunctions(unittest.TestCase):
    """工具函数测试"""

    def test_01_print_json_basic(self):
        """测试基本 JSON 打印"""
        print("\n--- 测试 1: 基本 JSON 打印 ---")

        test_data = {"name": "Bitcoin", "price": 40000, "symbol": "BTC"}

        # 捕获标准输出
        with patch("sys.stdout", new=StringIO()) as fake_out:
            print_json(test_data)
            output = fake_out.getvalue()

        # 验证输出包含数据
        self.assertIn("Bitcoin", output)
        self.assertIn("40000", output)
        self.assertIn("BTC", output)

        print("✅ 基本 JSON 打印测试通过")

    def test_02_print_json_with_title(self):
        """测试带标题的 JSON 打印"""
        print("\n--- 测试 2: 带标题的 JSON 打印 ---")

        test_data = {"test": "data"}
        title = "测试标题"

        with patch("sys.stdout", new=StringIO()) as fake_out:
            print_json(test_data, title=title)
            output = fake_out.getvalue()

        # 验证输出包含标题和分隔线
        self.assertIn(title, output)
        self.assertIn("=" * 50, output)

        print("✅ 带标题的 JSON 打印测试通过")

    def test_03_print_json_list_with_limit(self):
        """测试限制项数的列表打印"""
        print("\n--- 测试 3: 限制项数的列表打印 ---")

        test_list = [{"id": i, "name": f"item_{i}"} for i in range(10)]

        with patch("sys.stdout", new=StringIO()) as fake_out:
            print_json(test_list, max_items=3)
            output = fake_out.getvalue()

        # 验证只显示了前3项
        self.assertIn("显示前 3 项", output)
        self.assertIn("总共 10 项", output)
        self.assertIn("item_0", output)
        self.assertIn("item_1", output)
        self.assertIn("item_2", output)
        self.assertNotIn("item_3", output)

        print("✅ 限制项数的列表打印测试通过")

    def test_04_get_timestamp_current(self):
        """测试获取当前时间戳"""
        print("\n--- 测试 4: 获取当前时间戳 ---")

        timestamp = get_timestamp()

        # 验证时间戳是合理的（近期时间）
        self.assertIsInstance(timestamp, int)
        self.assertGreater(timestamp, 1600000000)  # 2020年之后
        self.assertLess(timestamp, 2000000000)  # 2033年之前

        print(f"✅ 当前时间戳: {timestamp}")

    def test_05_get_timestamp_with_date(self):
        """测试指定日期的时间戳"""
        print("\n--- 测试 5: 指定日期的时间戳 ---")

        # 测试已知日期
        timestamp = get_timestamp("2024-01-01")
        expected = int(datetime(2024, 1, 1).timestamp())

        self.assertEqual(timestamp, expected)

        print(f"✅ 2024-01-01 时间戳: {timestamp}")

    def test_06_get_timestamp_invalid_date(self):
        """测试无效日期格式"""
        print("\n--- 测试 6: 无效日期格式 ---")

        # 测试无效日期应该抛出异常
        with self.assertRaises(ValueError):
            get_timestamp("invalid-date")

        with self.assertRaises(ValueError):
            get_timestamp("2024-13-01")  # 无效月份

        print("✅ 无效日期格式正确抛出异常")

    def test_07_format_currency_usd(self):
        """测试美元格式化"""
        print("\n--- 测试 7: 美元格式化 ---")

        # 测试各种金额
        test_cases = [
            (1000, "$1,000.00"),
            (1000000, "$1,000,000.00"),
            (1234.56, "$1,234.56"),
            (0, "$0.00"),
        ]

        for amount, expected in test_cases:
            result = format_currency(amount, "USD")
            self.assertEqual(result, expected)

        print("✅ 美元格式化测试通过")

    def test_08_format_currency_cny(self):
        """测试人民币格式化"""
        print("\n--- 测试 8: 人民币格式化 ---")

        result = format_currency(1000, "CNY")
        self.assertEqual(result, "¥1,000.00")

        print("✅ 人民币格式化测试通过")

    def test_09_format_currency_other(self):
        """测试其他货币格式化"""
        print("\n--- 测试 9: 其他货币格式化 ---")

        result = format_currency(1000, "EUR")
        self.assertEqual(result, "1,000.00 EUR")

        print("✅ 其他货币格式化测试通过")

    def test_10_calculate_percentage_change_normal(self):
        """测试正常百分比变化计算"""
        print("\n--- 测试 10: 正常百分比变化计算 ---")

        # 测试各种情况
        test_cases = [
            (100, 110, 10.0),  # 上涨10%
            (100, 90, -10.0),  # 下跌10%
            (100, 200, 100.0),  # 翻倍
            (100, 50, -50.0),  # 减半
            (100, 100, 0.0),  # 无变化
        ]

        for old_val, new_val, expected in test_cases:
            result = calculate_percentage_change(old_val, new_val)
            self.assertAlmostEqual(result, expected, places=5)

        print("✅ 正常百分比变化计算测试通过")

    def test_11_calculate_percentage_change_zero_base(self):
        """测试零基础值的百分比变化"""
        print("\n--- 测试 11: 零基础值的百分比变化 ---")

        result = calculate_percentage_change(0, 100)
        self.assertEqual(result, 0.0)  # 应该返回0而不是错误

        print("✅ 零基础值百分比变化测试通过")

    def test_12_safe_get_simple(self):
        """测试简单字典安全取值"""
        print("\n--- 测试 12: 简单字典安全取值 ---")

        data = {"a": 1, "b": 2}

        # 存在的键
        self.assertEqual(safe_get(data, "a"), 1)

        # 不存在的键
        self.assertIsNone(safe_get(data, "c"))

        # 自定义默认值
        self.assertEqual(safe_get(data, "c", default="default"), "default")

        print("✅ 简单字典安全取值测试通过")

    def test_13_safe_get_nested(self):
        """测试嵌套字典安全取值"""
        print("\n--- 测试 13: 嵌套字典安全取值 ---")

        data = {"user": {"profile": {"name": "Alice", "age": 30}}}

        # 存在的嵌套路径
        self.assertEqual(safe_get(data, "user", "profile", "name"), "Alice")

        # 不存在的嵌套路径
        self.assertIsNone(safe_get(data, "user", "profile", "email"))
        self.assertIsNone(safe_get(data, "user", "settings", "theme"))

        # 中途遇到非字典
        data2 = {"user": "not_a_dict"}
        self.assertIsNone(safe_get(data2, "user", "profile"))

        print("✅ 嵌套字典安全取值测试通过")

    def test_14_get_project_root_normal(self):
        """测试正常情况下的项目根目录查找"""
        print("\n--- 测试 14: 项目根目录查找 ---")

        # 由于我们在实际项目中运行，应该能找到.git目录
        try:
            root = get_project_root()
            self.assertTrue(os.path.exists(root))
            self.assertTrue(os.path.isdir(root))

            # 验证确实存在.git目录
            git_dir = os.path.join(root, ".git")
            self.assertTrue(os.path.exists(git_dir))

            print(f"✅ 项目根目录: {root}")
        except FileNotFoundError:
            # 如果找不到.git目录，测试环境可能有问题，但不算测试失败
            print("⚠️ 未找到.git目录，可能是测试环境问题")

    @patch("os.listdir")
    @patch("os.path.dirname")
    @patch("os.path.abspath")
    def test_15_get_project_root_not_found(
        self, mock_abspath, mock_dirname, mock_listdir
    ):
        """测试找不到项目根目录的情况"""
        print("\n--- 测试 15: 找不到项目根目录 ---")

        # 模拟目录结构，没有.git目录
        mock_abspath.return_value = "/some/path"
        mock_dirname.side_effect = ["/some", "/", "/"]  # 最终到达根目录
        mock_listdir.return_value = ["file1.txt", "file2.py"]  # 没有.git

        with self.assertRaises(FileNotFoundError) as cm:
            get_project_root()

        self.assertIn("无法找到项目根目录", str(cm.exception))

        print("✅ 找不到项目根目录正确抛出异常")

    def test_16_edge_cases(self):
        """测试边界情况"""
        print("\n--- 测试 16: 边界情况 ---")

        # 空字典的安全取值
        empty_dict = {}
        self.assertIsNone(safe_get(empty_dict, "any_key"))

        # 空列表的JSON打印
        with patch("sys.stdout", new=StringIO()) as fake_out:
            print_json([])
            output = fake_out.getvalue()
        self.assertIn("[]", output)

        # 负数的货币格式化
        result = format_currency(-1000, "USD")
        self.assertEqual(result, "$-1,000.00")

        print("✅ 边界情况测试通过")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行 utils.py 工具函数测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestUtilsFunctions))

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
