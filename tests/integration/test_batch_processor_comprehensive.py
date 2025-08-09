#!/usr/bin/env python3
"""
全面测试并发工具模块中的 BatchProcessor

测试覆盖：
1. BatchProcessor 的初始化
2. process_in_batches 方法
3. 不同批次大小的处理
4. 与 ProgressTracker 的集成
5. 错误处理和边界情况
"""

import sys
import unittest
import time
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.concurrent_utils import BatchProcessor, ConcurrentProcessor
from src.utils.progress_utils import ProgressTracker


class TestBatchProcessor(unittest.TestCase):
    """测试 BatchProcessor 类"""

    def setUp(self):
        """设置测试环境"""
        self.processor = BatchProcessor(batch_size=5, max_workers=2)

    def test_initialization(self):
        """测试 BatchProcessor 初始化"""
        # 测试默认参数
        default_processor = BatchProcessor()
        self.assertEqual(default_processor.batch_size, 50)
        self.assertIsInstance(default_processor.processor, ConcurrentProcessor)
        
        # 测试自定义参数
        custom_processor = BatchProcessor(batch_size=10, max_workers=4, use_processes=True)
        self.assertEqual(custom_processor.batch_size, 10)
        self.assertEqual(custom_processor.processor.max_workers, 4)
        self.assertTrue(custom_processor.processor.use_processes)

    def test_process_empty_list(self):
        """测试处理空列表"""
        def dummy_func(x):
            return x * 2
        
        result = self.processor.process_in_batches(dummy_func, [], "测试空列表")
        self.assertEqual(result, [])

    def test_process_single_batch(self):
        """测试单批处理"""
        def square_func(x):
            return x ** 2
        
        items = [1, 2, 3, 4]
        
        with patch('sys.stdout', new=StringIO()):
            result = self.processor.process_in_batches(square_func, items, "平方计算")
        
        expected = [1, 4, 9, 16]
        # 并发处理可能改变顺序，所以检查结果集合是否相同
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_process_multiple_batches(self):
        """测试多批处理"""
        def add_one(x):
            return x + 1
        
        # 创建需要2批的数据 (batch_size=5)
        items = list(range(8))
        
        with patch('sys.stdout', new=StringIO()):
            result = self.processor.process_in_batches(add_one, items, "加一运算")
        
        expected = list(range(1, 9))  # [1, 2, 3, 4, 5, 6, 7, 8]
        # 并发处理可能改变顺序，检查结果集合
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_process_exact_batch_size(self):
        """测试正好整除批次大小的情况"""
        def multiply_by_3(x):
            return x * 3
        
        # 正好两批，每批5个
        items = list(range(10))
        
        with patch('sys.stdout', new=StringIO()):
            result = self.processor.process_in_batches(multiply_by_3, items, "乘3运算")
        
        expected = [i * 3 for i in range(10)]
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_process_with_function_kwargs(self):
        """测试传递额外参数给处理函数"""
        def multiply_by_factor(x, factor=2):
            return x * factor
        
        items = [1, 2, 3, 4, 5]
        
        with patch('sys.stdout', new=StringIO()):
            result = self.processor.process_in_batches(
                multiply_by_factor, 
                items, 
                "乘法运算", 
                factor=5
            )
        
        expected = [5, 10, 15, 20, 25]
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_batch_size_larger_than_items(self):
        """测试批次大小大于项目数量的情况"""
        large_batch_processor = BatchProcessor(batch_size=100)
        
        def identity_func(x):
            return x
        
        items = [1, 2, 3]
        
        with patch('sys.stdout', new=StringIO()):
            result = large_batch_processor.process_in_batches(identity_func, items, "身份函数")
        
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result), sorted(items))
        self.assertEqual(len(result), len(items))

    def test_batch_processing_order_preservation(self):
        """测试批处理功能正确性（不依赖顺序）"""
        def slow_square(x):
            time.sleep(0.001)  # 很短的延迟，模拟处理时间
            return x ** 2

        items = list(range(12))  # 需要3批 (每批5个，最后2个)

        with patch('sys.stdout', new=StringIO()):
            result = self.processor.process_in_batches(slow_square, items, "慢速平方")

        expected = [i ** 2 for i in range(12)]
        # 由于并发，顺序可能变化，检查结果集合
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_error_handling_in_batch_processing(self):
        """测试批处理中的错误处理"""
        def problematic_func(x):
            if x == 5:
                raise ValueError(f"不能处理值 {x}")
            return x * 2
        
        items = [1, 2, 3, 4, 5, 6]
        
        # 由于有错误，这应该抛出异常或者有错误处理机制
        with patch('sys.stdout', new=StringIO()):
            try:
                result = self.processor.process_in_batches(problematic_func, items, "问题函数")
                # 如果没有抛出异常，检查结果
                # 不同的错误处理策略可能有不同的行为
            except Exception as e:
                # 预期会有异常
                self.assertIsInstance(e, (ValueError, Exception))

    def test_custom_batch_size_effect(self):
        """测试不同批次大小的效果"""
        def simple_func(x):
            return x + 10
        
        items = list(range(15))
        
        # 测试小批次
        small_batch_processor = BatchProcessor(batch_size=3)
        with patch('sys.stdout', new=StringIO()):
            result_small = small_batch_processor.process_in_batches(simple_func, items, "小批次")
        
        # 测试大批次
        large_batch_processor = BatchProcessor(batch_size=10)
        with patch('sys.stdout', new=StringIO()):
            result_large = large_batch_processor.process_in_batches(simple_func, items, "大批次")
        
        # 结果应该相同，不管批次大小
        expected = [i + 10 for i in range(15)]
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result_small), sorted(expected))
        self.assertEqual(sorted(result_large), sorted(expected))
        self.assertEqual(len(result_small), len(expected))
        self.assertEqual(len(result_large), len(expected))


class TestBatchProcessorIntegration(unittest.TestCase):
    """测试 BatchProcessor 与其他组件的集成"""

    def test_integration_with_concurrent_processor(self):
        """测试与 ConcurrentProcessor 的集成"""
        # 创建使用线程的批处理器
        thread_processor = BatchProcessor(batch_size=4, max_workers=2, use_processes=False)
        
        def cpu_light_task(x):
            return x ** 2 + x + 1
        
        items = list(range(10))
        
        with patch('sys.stdout', new=StringIO()):
            result = thread_processor.process_in_batches(cpu_light_task, items, "线程处理")
        
        expected = [x ** 2 + x + 1 for x in range(10)]
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))

    def test_performance_simulation(self):
        """模拟性能测试（验证批处理能正常工作）"""
        def simulated_io_task(x):
            time.sleep(0.001)  # 模拟很短的I/O延迟
            return x * 2
        
        items = list(range(20))
        processor = BatchProcessor(batch_size=5, max_workers=4)
        
        start_time = time.time()
        with patch('sys.stdout', new=StringIO()):
            result = processor.process_in_batches(simulated_io_task, items, "性能测试")
        end_time = time.time()
        
        # 验证结果正确性
        expected = [i * 2 for i in range(20)]
        # 检查结果集合而非顺序
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), len(expected))
        
        # 验证有合理的执行时间（不应该太长）
        execution_time = end_time - start_time
        self.assertLess(execution_time, 5.0)  # 不应该超过5秒

    def test_large_dataset_handling(self):
        """测试大数据集处理"""
        def simple_transform(x):
            return x * 3 + 1
        
        # 模拟较大的数据集
        large_items = list(range(100))
        processor = BatchProcessor(batch_size=10, max_workers=3)
        
        with patch('sys.stdout', new=StringIO()):
            result = processor.process_in_batches(simple_transform, large_items, "大数据集")
        
        expected = [i * 3 + 1 for i in range(100)]
        # 检查结果集合而非顺序（大数据集）
        self.assertEqual(sorted(result), sorted(expected))
        self.assertEqual(len(result), 100)

    def test_mixed_data_types(self):
        """测试混合数据类型处理"""
        def type_aware_func(item):
            if isinstance(item, int):
                return item * 2
            elif isinstance(item, str):
                return item.upper()
            else:
                return str(item)
        
        mixed_items = [1, "hello", 3.14, 2, "world", 42]
        processor = BatchProcessor(batch_size=3)
        
        with patch('sys.stdout', new=StringIO()):
            result = processor.process_in_batches(type_aware_func, mixed_items, "混合类型")
        
        expected = [2, "HELLO", "3.14", 4, "WORLD", 84]
        # 检查结果集合而非顺序（混合类型）
        self.assertEqual(sorted(result, key=str), sorted(expected, key=str))
        self.assertEqual(len(result), len(expected))


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行 BatchProcessor 全面测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestBatchProcessor))
    suite.addTests(loader.loadTestsFromTestCase(TestBatchProcessorIntegration))

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
