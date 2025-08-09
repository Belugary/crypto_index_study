#!/usr/bin/env python3
"""并发工具扩展测试

补充覆盖以下未覆盖路径：
1. ConcurrentProcessor 单元素/空列表快速路径
2. error_handling = collect / raise 分支
3. show_progress = False 分支
4. use_processes = True (进程池路径) - 使用可 picklable 顶层函数
5. auto_concurrent_map 顺序与并发两条路径
6. smart_concurrent_execution 的参数校验与决策分流

设计原则：
 - 避免耗时：不做真实耗 CPU 任务，不使用 sleep (进程池示例最多 2 个元素)
 - Windows 兼容：进程池使用顶层函数
"""

import sys
import unittest
from pathlib import Path

# 确保项目根路径在 sys.path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.concurrent_utils import (
    ConcurrentProcessor,
    auto_concurrent_map,
    smart_concurrent_execution,
)


# 进程池可用的顶层函数
def square(x: int) -> int:
    return x * x


def error_if_three(x: int) -> int:
    if x == 3:
        raise ValueError("boom 3")
    return x + 1


def classify_decision(x: int) -> bool:
    """偶数视为 CPU 密集型，仅用于测试分流逻辑"""
    return x % 2 == 0


def cpu_task(x: int) -> int:
    return x * 10


def io_task(x: int) -> str:
    # 模拟 I/O 结果（不实际阻塞）
    return f"io_{x}"


class TestConcurrentProcessorExtended(unittest.TestCase):
    def test_empty_and_single_item(self):
        p = ConcurrentProcessor(max_workers=2)
        self.assertEqual(p.process_batch(lambda x: x * 2, []), [])
        self.assertEqual(p.process_batch(lambda x: x + 1, [5]), [6])

    def test_error_handling_collect(self):
        p = ConcurrentProcessor(max_workers=2)
        items = [1, 2, 3, 4]
        results = p.process_batch(error_if_three, items, error_handling="collect", show_progress=False)
        # 应返回与 items 数量一致，包含一个 error 结果
        self.assertEqual(len(results), len(items))
        error_entries = [r for r in results if isinstance(r, dict) and r.get("error")]
        self.assertEqual(len(error_entries), 1)
        self.assertIn("boom 3", error_entries[0]["error"])

    def test_error_handling_raise(self):
        p = ConcurrentProcessor(max_workers=2)
        with self.assertRaises(ValueError):
            p.process_batch(error_if_three, [1, 3], error_handling="raise", show_progress=False)

    def test_show_progress_false_normal(self):
        p = ConcurrentProcessor(max_workers=2)
        res = p.process_batch(square, [2, 3, 4], show_progress=False)
        self.assertCountEqual(res, [4, 9, 16])

    def test_process_pool_path(self):
        # 进程池：max_workers=1 以缩短时间; 2 元素即可触发并发路径
        p = ConcurrentProcessor(max_workers=1, use_processes=True)
        res = p.process_batch(square, [2, 5], show_progress=False)
        self.assertCountEqual(res, [4, 25])

    def test_auto_concurrent_map_threshold(self):
        # 未达阈值 -> 顺序
        seq_res = auto_concurrent_map(lambda x: x + 1, [1, 2], threshold=5)
        self.assertEqual(seq_res, [2, 3])
        # 达到阈值 -> 并发
        par_res = auto_concurrent_map(lambda x: x * 2, list(range(6)), threshold=5)
        self.assertCountEqual(par_res, [i * 2 for i in range(6)])

    def test_smart_concurrent_execution_full(self):
        items = list(range(6))  # 0..5
        result = smart_concurrent_execution(
            items,
            cpu_bound_func=cpu_task,
            io_bound_func=io_task,
            decision_func=classify_decision,
            desc="智能处理测试",
        )
        # 偶数 -> CPU, 奇数 -> I/O
        expected_cpu = [cpu_task(i) for i in items if i % 2 == 0]
        expected_io = [io_task(i) for i in items if i % 2 == 1]
        self.assertCountEqual(result["cpu_results"], expected_cpu)
        self.assertCountEqual(result["io_results"], expected_io)

    def test_smart_concurrent_execution_only_cpu(self):
        items = [1, 2, 3]
        result = smart_concurrent_execution(items, cpu_bound_func=cpu_task)
        self.assertCountEqual(result["cpu_results"], [cpu_task(i) for i in items])
        self.assertEqual(result["io_results"], [])

    def test_smart_concurrent_execution_only_io(self):
        items = [1, 2]
        result = smart_concurrent_execution(items, io_bound_func=io_task)
        self.assertEqual(result["cpu_results"], [])
        self.assertCountEqual(result["io_results"], [io_task(i) for i in items])

    def test_smart_concurrent_execution_invalid_args(self):
        with self.assertRaises(ValueError):
            smart_concurrent_execution([1])  # 无函数
        with self.assertRaises(ValueError):
            smart_concurrent_execution([1, 2], cpu_bound_func=cpu_task, io_bound_func=io_task)


if __name__ == "__main__":  # 手动运行支持
    unittest.main(verbosity=2)
