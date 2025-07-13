#!/usr/bin/env python3
"""
并发处理工具

提供线程池和进程池的统一接口，优化处理大量数据时的性能。
遵循开发指南中的"简单胜于复杂"原则。
"""

import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Union

from .progress_utils import ProgressTracker

logger = logging.getLogger(__name__)


class ConcurrentProcessor:
    """并发处理器，自动选择最适合的并发策略"""

    def __init__(self, max_workers: Optional[int] = None, use_processes: bool = False):
        """
        初始化并发处理器

        Args:
            max_workers: 最大工作线程/进程数，None表示自动选择
            use_processes: 是否使用进程池（适合CPU密集型任务）
        """
        if max_workers is None:
            if use_processes:
                # 进程池：留一个核心给系统
                max_workers = max(1, multiprocessing.cpu_count() - 1)
            else:
                # 线程池：I/O密集型任务可以更多线程
                max_workers = min(32, (os.cpu_count() or 1) + 4)

        self.max_workers = max_workers
        self.use_processes = use_processes

    def process_batch(
        self,
        func: Callable,
        items: List[Any],
        desc: str = "批量处理",
        show_progress: bool = True,
        error_handling: str = "log",  # "log", "raise", "collect"
        **kwargs,
    ) -> List[Any]:
        """
        批量处理项目列表

        Args:
            func: 处理函数
            items: 待处理项目列表
            desc: 进度描述
            show_progress: 是否显示进度条
            error_handling: 错误处理策略
                - "log": 记录错误并继续
                - "raise": 遇到错误立即抛出
                - "collect": 收集错误并在结果中返回
            **kwargs: 传递给处理函数的额外参数

        Returns:
            处理结果列表
        """
        if not items:
            return []

        if len(items) == 1:
            # 单个项目直接处理，不使用并发
            try:
                return [func(items[0], **kwargs)]
            except Exception as e:
                if error_handling == "raise":
                    raise
                elif error_handling == "log":
                    logger.error(f"处理项目 {items[0]} 时出错: {e}")
                    return []
                else:  # collect
                    return [{"error": str(e), "item": items[0]}]

        results = []
        errors = []

        executor_class = (
            ProcessPoolExecutor if self.use_processes else ThreadPoolExecutor
        )

        with executor_class(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_item = {
                executor.submit(func, item, **kwargs): item for item in items
            }

            # 收集结果
            if show_progress:
                with ProgressTracker(len(items), desc) as tracker:
                    for future in as_completed(future_to_item):
                        item = future_to_item[future]
                        try:
                            result = future.result()
                            results.append(result)
                            tracker.update(1, f"已完成: {str(item)[:20]}")
                        except Exception as e:
                            error_msg = f"处理 {item} 时出错: {e}"

                            if error_handling == "raise":
                                tracker.set_description(f"{desc} - 发生错误")
                                raise
                            elif error_handling == "log":
                                logger.error(error_msg)
                                tracker.update(1, f"错误: {str(item)[:20]}")
                            else:  # collect
                                error_result = {"error": str(e), "item": item}
                                results.append(error_result)
                                errors.append(error_msg)
                                tracker.update(1, f"错误: {str(item)[:20]}")
            else:
                # 不显示进度条的处理
                for future in as_completed(future_to_item):
                    item = future_to_item[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        if error_handling == "raise":
                            raise
                        elif error_handling == "log":
                            logger.error(f"处理 {item} 时出错: {e}")
                        else:  # collect
                            results.append({"error": str(e), "item": item})
                            errors.append(f"处理 {item} 时出错: {e}")

        if error_handling == "collect" and errors:
            logger.warning(f"批量处理完成，发生 {len(errors)} 个错误")

        return results


def auto_concurrent_map(
    func: Callable,
    items: List[Any],
    max_workers: Optional[int] = None,
    threshold: int = 10,
    desc: str = "处理中",
    **kwargs,
) -> List[Any]:
    """
    自动选择是否使用并发的映射函数

    Args:
        func: 处理函数
        items: 待处理项目列表
        max_workers: 最大工作线程数
        threshold: 启用并发的最小项目数量
        desc: 进度描述
        **kwargs: 传递给处理函数的额外参数

    Returns:
        处理结果列表
    """
    if len(items) < threshold:
        # 项目较少，使用顺序处理
        logger.debug(f"项目数量 {len(items)} 小于阈值 {threshold}，使用顺序处理")
        results = []
        for item in items:
            results.append(func(item, **kwargs))
        return results
    else:
        # 项目较多，使用并发处理
        logger.debug(f"项目数量 {len(items)} 达到阈值，启用并发处理")
        processor = ConcurrentProcessor(max_workers=max_workers)
        return processor.process_batch(func, items, desc=desc, **kwargs)


class BatchProcessor:
    """智能批处理器，自动分批和并发处理"""

    def __init__(
        self,
        batch_size: int = 50,
        max_workers: Optional[int] = None,
        use_processes: bool = False,
    ):
        """
        初始化批处理器

        Args:
            batch_size: 每批大小
            max_workers: 最大工作数
            use_processes: 是否使用进程池
        """
        self.batch_size = batch_size
        self.processor = ConcurrentProcessor(max_workers, use_processes)

    def process_in_batches(
        self, func: Callable, items: List[Any], desc: str = "分批处理", **kwargs
    ) -> List[Any]:
        """
        分批并发处理大量项目

        Args:
            func: 处理函数
            items: 待处理项目列表
            desc: 进度描述
            **kwargs: 传递给处理函数的额外参数

        Returns:
            所有批次的处理结果
        """
        if not items:
            return []

        # 计算批次
        total_batches = (len(items) + self.batch_size - 1) // self.batch_size
        all_results = []

        logger.info(f"开始分批处理 {len(items)} 个项目，分 {total_batches} 批")

        with ProgressTracker(total_batches, f"{desc} (批次)", "批") as tracker:
            for i in range(total_batches):
                start_idx = i * self.batch_size
                end_idx = min(start_idx + self.batch_size, len(items))
                batch_items = items[start_idx:end_idx]

                batch_desc = f"第 {i+1}/{total_batches} 批"
                tracker.set_description(f"{desc} - {batch_desc}")

                # 并发处理当前批次
                batch_results = self.processor.process_batch(
                    func,
                    batch_items,
                    desc=batch_desc,
                    show_progress=False,  # 批次级别不显示内部进度
                    **kwargs,
                )

                all_results.extend(batch_results)
                tracker.update(1, f"已完成 {end_idx}/{len(items)} 个项目")

        return all_results


def smart_concurrent_execution(
    items: List[Any],
    cpu_bound_func: Optional[Callable] = None,
    io_bound_func: Optional[Callable] = None,
    decision_func: Optional[Callable[[Any], bool]] = None,
    desc: str = "智能处理",
    **kwargs,
) -> Dict[str, List[Any]]:
    """
    根据任务类型智能选择并发策略

    Args:
        items: 待处理项目列表
        cpu_bound_func: CPU密集型任务处理函数
        io_bound_func: I/O密集型任务处理函数
        decision_func: 判断函数，返回True表示CPU密集型
        desc: 进度描述
        **kwargs: 传递给处理函数的额外参数

    Returns:
        分类处理结果
    """
    if not cpu_bound_func and not io_bound_func:
        raise ValueError("至少需要提供一个处理函数")

    if not decision_func and cpu_bound_func and io_bound_func:
        raise ValueError("提供两个处理函数时需要提供判断函数")

    # 分类项目
    cpu_items = []
    io_items = []

    for item in items:
        if decision_func:
            if decision_func(item):
                cpu_items.append(item)
            else:
                io_items.append(item)
        elif cpu_bound_func:
            cpu_items.append(item)
        else:
            io_items.append(item)

    results = {"cpu_results": [], "io_results": []}

    # 处理CPU密集型任务
    if cpu_items and cpu_bound_func:
        logger.info(f"使用进程池处理 {len(cpu_items)} 个CPU密集型任务")
        processor = ConcurrentProcessor(use_processes=True)
        results["cpu_results"] = processor.process_batch(
            cpu_bound_func, cpu_items, f"{desc} (CPU)", **kwargs
        )

    # 处理I/O密集型任务
    if io_items and io_bound_func:
        logger.info(f"使用线程池处理 {len(io_items)} 个I/O密集型任务")
        processor = ConcurrentProcessor(use_processes=False)
        results["io_results"] = processor.process_batch(
            io_bound_func, io_items, f"{desc} (I/O)", **kwargs
        )

    return results


# 使用示例
if __name__ == "__main__":
    import logging
    import time

    logging.basicConfig(level=logging.INFO)

    def slow_computation(x):
        """模拟CPU密集型任务"""
        time.sleep(0.1)
        return x**2

    def slow_io(x):
        """模拟I/O密集型任务"""
        time.sleep(0.05)
        return f"processed_{x}"

    # 示例1: 基本并发处理
    print("示例1: 基本并发处理")
    items = list(range(20))
    processor = ConcurrentProcessor(max_workers=4)
    results = processor.process_batch(slow_computation, items, "计算平方")
    print(f"结果前5个: {results[:5]}")

    # 示例2: 自动并发映射
    print("\n示例2: 自动并发映射")
    small_items = list(range(5))
    large_items = list(range(50))

    # 小数据集 - 不启用并发
    results1 = auto_concurrent_map(slow_computation, small_items)
    # 大数据集 - 自动启用并发
    results2 = auto_concurrent_map(
        slow_computation, large_items[:10], desc="大批量计算"
    )

    # 示例3: 批处理
    print("\n示例3: 批处理")
    batch_processor = BatchProcessor(batch_size=5, max_workers=2)
    results = batch_processor.process_in_batches(slow_io, items[:15], "批量I/O")

    print("所有示例完成！")
