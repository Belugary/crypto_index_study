#!/usr/bin/env python3
"""
测试用户体验工具
"""
import sys
import time
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def test_progress_tracker():
    """测试进度跟踪器"""
    print("=== 测试进度跟踪器 ===")

    try:
        from src.utils.progress_utils import ProgressTracker

        with ProgressTracker(5, "测试进度跟踪", "项目") as tracker:
            for i in range(5):
                time.sleep(0.1)
                tracker.update(1, f"处理项目_{i}")

        print("✅ 进度跟踪器测试成功")
        assert True

    except Exception as e:
        print(f"❌ 进度跟踪器测试失败: {e}")
        import traceback

        traceback.print_exc()
        assert False, f"进度跟踪器测试失败: {e}"


def test_concurrent_processor():
    """测试并发处理器"""
    print("\n=== 测试并发处理器 ===")

    try:
        from src.utils.concurrent_utils import ConcurrentProcessor

        def simple_task(x):
            time.sleep(0.05)  # 模拟工作
            return x * 2

        processor = ConcurrentProcessor(max_workers=2)
        items = list(range(6))

        print(f"处理 {len(items)} 个项目...")
        results = processor.process_batch(
            simple_task, items, desc="并发测试", show_progress=True
        )

        print(f"结果: {results}")
        print("✅ 并发处理器测试成功")
        assert True

    except Exception as e:
        print(f"❌ 并发处理器测试失败: {e}")
        import traceback

        traceback.print_exc()
        assert False, f"并发处理器测试失败: {e}"


def test_auto_concurrent():
    """测试自动并发选择"""
    print("\n=== 测试自动并发选择 ===")

    try:
        from src.utils.concurrent_utils import auto_concurrent_map

        def task(x):
            time.sleep(0.02)
            return x + 10

        # 小数据集 - 应该不启用并发
        small_items = list(range(3))
        print(f"小数据集 ({len(small_items)} 项目) - 预期不启用并发:")
        results1 = auto_concurrent_map(task, small_items, threshold=5, desc="小批量")
        print(f"结果: {results1}")

        # 大数据集 - 应该启用并发
        large_items = list(range(8))
        print(f"大数据集 ({len(large_items)} 项目) - 预期启用并发:")
        results2 = auto_concurrent_map(task, large_items, threshold=5, desc="大批量")
        print(f"结果: {results2}")

        print("✅ 自动并发选择测试成功")
        assert True

    except Exception as e:
        print(f"❌ 自动并发选择测试失败: {e}")
        import traceback

        traceback.print_exc()
        assert False, f"自动并发选择测试失败: {e}"


if __name__ == "__main__":
    print("开始测试用户体验优化工具...\n")

    success_count = 0
    total_tests = 3

    if test_progress_tracker():
        success_count += 1

    if test_concurrent_processor():
        success_count += 1

    if test_auto_concurrent():
        success_count += 1

    print(f"\n=== 测试总结 ===")
    print(f"通过: {success_count}/{total_tests}")

    if success_count == total_tests:
        print("🎉 所有测试通过！用户体验工具已可用。")
    else:
        print("⚠️  部分测试失败，请检查错误信息。")
