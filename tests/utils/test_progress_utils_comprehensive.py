#!/usr/bin/env python3
"""
全面测试进度工具模块

测试覆盖：
1. ProgressTracker 的所有功能
2. BatchProgressTracker 的批处理功能
3. progress_wrapper 装饰器
4. with_progress 装饰器
5. estimate_time_remaining 工具函数
"""

import sys
import unittest
import time
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from io import StringIO

# 添加项目根目录到Python路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.progress_utils import (
    ProgressTracker,
    BatchProgressTracker,
    progress_wrapper,
    with_progress,
    estimate_time_remaining
)


class TestProgressTracker(unittest.TestCase):
    """测试 ProgressTracker 类"""

    def setUp(self):
        """设置测试环境"""
        self.tracker = None

    def tearDown(self):
        """清理测试环境"""
        if self.tracker and hasattr(self.tracker, 'pbar') and self.tracker.pbar:
            self.tracker.pbar.close()

    def test_basic_initialization(self):
        """测试基本初始化"""
        tracker = ProgressTracker(100, "测试进度", "项目")
        self.assertEqual(tracker.total, 100)
        self.assertEqual(tracker.desc, "测试进度")
        self.assertEqual(tracker.unit, "项目")
        self.assertIsNone(tracker.pbar)

    def test_context_manager_enter_exit(self):
        """测试上下文管理器的进入和退出"""
        with patch('sys.stdout', new=StringIO()):
            with ProgressTracker(10, "测试", "item") as tracker:
                self.assertIsNotNone(tracker.pbar)
                self.assertIsNotNone(tracker.start_time)
            # 退出后进度条应该被关闭
            # 注意：tqdm.close() 后 pbar 仍然存在，但已关闭

    def test_update_progress(self):
        """测试进度更新"""
        with patch('sys.stdout', new=StringIO()):
            with ProgressTracker(5, "测试更新", "个") as tracker:
                # 单步更新
                tracker.update(1)
                tracker.update(2)
                
                # 带postfix更新
                tracker.update(1, "当前状态")

    def test_set_description(self):
        """测试描述设置"""
        with patch('sys.stdout', new=StringIO()):
            with ProgressTracker(5, "原始描述", "个") as tracker:
                tracker.set_description("新描述")
                # 验证内部状态（通过pbar验证）
                self.assertIsNotNone(tracker.pbar)

    def test_zero_total_handling(self):
        """测试总数为0的情况"""
        with patch('sys.stdout', new=StringIO()):
            with ProgressTracker(0, "空任务", "个") as tracker:
                # 应该能正常处理
                tracker.update(0)


class TestBatchProgressTracker(unittest.TestCase):
    """测试 BatchProgressTracker 类"""

    def test_batch_initialization(self):
        """测试批处理进度跟踪器初始化"""
        tracker = BatchProgressTracker(5, 10, "批处理测试")
        self.assertEqual(tracker.total_batches, 5)
        self.assertEqual(tracker.batch_size, 10)
        self.assertEqual(tracker.desc, "批处理测试")
        self.assertEqual(tracker.current_batch, 0)

    def test_batch_context_manager(self):
        """测试批处理上下文管理器"""
        with patch('sys.stdout', new=StringIO()):
            with BatchProgressTracker(3, 5, "测试批处理") as tracker:
                self.assertIsNotNone(tracker.pbar)
                self.assertIsNotNone(tracker.start_time)
                
                # 更新批次
                tracker.update_batch(0, "第一批")
                tracker.update_batch(1, "第二批")
                tracker.update_batch(2, "第三批")

    def test_batch_update_without_info(self):
        """测试不带信息的批次更新"""
        with patch('sys.stdout', new=StringIO()):
            with BatchProgressTracker(2, 10, "简单批处理") as tracker:
                tracker.update_batch(0)  # 不传入batch_info
                tracker.update_batch(1)


class TestProgressWrapperAndDecorators(unittest.TestCase):
    """测试进度包装器和装饰器"""

    def test_progress_wrapper_with_list(self):
        """测试progress_wrapper处理列表"""
        test_list = [1, 2, 3, 4, 5]
        
        with patch('sys.stdout', new=StringIO()):
            result = list(progress_wrapper(test_list, "处理列表", "个"))
            
        self.assertEqual(result, test_list)

    def test_progress_wrapper_with_generator(self):
        """测试progress_wrapper处理生成器"""
        def test_generator():
            for i in range(3):
                yield i
        
        with patch('sys.stdout', new=StringIO()):
            result = list(progress_wrapper(test_generator(), "处理生成器", "个"))
            
        self.assertEqual(result, [0, 1, 2])

    def test_with_progress_decorator(self):
        """测试with_progress装饰器"""
        
        @with_progress("处理数据", "项")
        def process_single_item(item):
            return item * 2
        
        test_data = [1, 2, 3, 4]
        
        with patch('sys.stdout', new=StringIO()):
            result = process_single_item(test_data)
            
        # 验证结果是否正确
        expected = [2, 4, 6, 8]
        self.assertEqual(result, expected)

    def test_with_progress_decorator_with_generator(self):
        """测试装饰器处理生成器输入"""
        
        @with_progress("处理生成器", "项")
        def process_single_item(item):
            return item + 1
        
        def test_generator():
            for i in range(3):
                yield i
        
        with patch('sys.stdout', new=StringIO()):
            result = process_single_item(test_generator())
            
        self.assertEqual(result, [1, 2, 3])


class TestEstimateTimeRemaining(unittest.TestCase):
    """测试时间估算功能"""

    def test_zero_current_progress(self):
        """测试当前进度为0的情况"""
        start_time = time.time()
        result = estimate_time_remaining(0, 100, start_time)
        self.assertEqual(result, "计算中...")

    def test_zero_elapsed_time(self):
        """测试消耗时间为0的情况"""
        start_time = time.time()
        result = estimate_time_remaining(10, 100, start_time)
        # 由于时间很短，可能返回"计算中..."或很短的时间
        self.assertIsInstance(result, str)

    def test_seconds_estimate(self):
        """测试秒级时间估算"""
        start_time = time.time() - 10  # 模拟10秒前开始
        result = estimate_time_remaining(50, 100, start_time)
        # 50%完成，消耗10秒，预计还需10秒
        self.assertIn("秒", result)

    def test_minutes_estimate(self):
        """测试分钟级时间估算"""
        start_time = time.time() - 60  # 模拟60秒前开始
        result = estimate_time_remaining(25, 100, start_time)
        # 25%完成，消耗60秒，预计还需180秒=3分钟
        self.assertIn("分钟", result)

    def test_hours_estimate(self):
        """测试小时级时间估算"""
        start_time = time.time() - 3600  # 模拟1小时前开始
        result = estimate_time_remaining(20, 100, start_time)
        # 20%完成，消耗1小时，预计还需4小时
        self.assertIn("小时", result)

    def test_completion_edge_case(self):
        """测试接近完成的边界情况"""
        start_time = time.time() - 100
        result = estimate_time_remaining(99, 100, start_time)
        # 99%完成，应该显示很短的剩余时间
        self.assertIsInstance(result, str)


class TestProgressUtilsIntegration(unittest.TestCase):
    """测试进度工具的集成功能"""

    def test_real_world_batch_processing_simulation(self):
        """模拟真实世界的批处理场景"""
        total_items = 25
        batch_size = 5
        total_batches = (total_items + batch_size - 1) // batch_size
        
        processed_items = []
        
        with patch('sys.stdout', new=StringIO()):
            with BatchProgressTracker(total_batches, batch_size, "模拟批处理") as batch_tracker:
                for batch_idx in range(total_batches):
                    start_idx = batch_idx * batch_size
                    end_idx = min(start_idx + batch_size, total_items)
                    batch_items = list(range(start_idx, end_idx))
                    
                    batch_tracker.update_batch(batch_idx, f"处理 {len(batch_items)} 个项目")
                    
                    # 在批次内使用ProgressTracker
                    with ProgressTracker(len(batch_items), f"批次{batch_idx+1}", "项") as item_tracker:
                        for item in batch_items:
                            # 模拟处理
                            processed_items.append(item * 2)
                            item_tracker.update(1, f"项目{item}")
        
        # 验证处理结果
        expected = [i * 2 for i in range(total_items)]
        self.assertEqual(processed_items, expected)
        self.assertEqual(len(processed_items), total_items)

    def test_nested_progress_tracking(self):
        """测试嵌套进度跟踪"""
        outer_tasks = 3
        inner_tasks = 4
        
        results = []
        
        with patch('sys.stdout', new=StringIO()):
            with ProgressTracker(outer_tasks, "外层任务", "个") as outer:
                for i in range(outer_tasks):
                    outer.update(0, f"开始任务{i+1}")
                    
                    with ProgressTracker(inner_tasks, f"任务{i+1}内部", "步骤") as inner:
                        for j in range(inner_tasks):
                            results.append((i, j))
                            inner.update(1, f"步骤{j+1}")
                    
                    outer.update(1, f"完成任务{i+1}")
        
        # 验证结果
        expected_count = outer_tasks * inner_tasks
        self.assertEqual(len(results), expected_count)


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行进度工具全面测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestProgressTracker))
    suite.addTests(loader.loadTestsFromTestCase(TestBatchProgressTracker))
    suite.addTests(loader.loadTestsFromTestCase(TestProgressWrapperAndDecorators))
    suite.addTests(loader.loadTestsFromTestCase(TestEstimateTimeRemaining))
    suite.addTests(loader.loadTestsFromTestCase(TestProgressUtilsIntegration))

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
