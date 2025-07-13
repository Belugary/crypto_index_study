#!/usr/bin/env python3
"""
进度显示工具

提供统一的进度条和时间估算功能，改善用户体验。
遵循开发指南中的"用户导向"原则。
"""

import time
from functools import wraps
from typing import Any, Callable, Optional

from tqdm import tqdm


class ProgressTracker:
    """进度跟踪器，提供统一的进度显示接口"""

    def __init__(self, total: int, desc: str = "处理中", unit: str = "item"):
        """
        初始化进度跟踪器

        Args:
            total: 总数量
            desc: 描述文字
            unit: 单位名称
        """
        self.total = total
        self.desc = desc
        self.unit = unit
        self.pbar: Optional[tqdm] = None
        self.start_time: Optional[float] = None

    def __enter__(self):
        """进入上下文管理器"""
        self.start_time = time.time()
        self.pbar = tqdm(
            total=self.total,
            desc=self.desc,
            unit=self.unit,
            ncols=80,  # 更合理的宽度
            leave=False,  # 完成后清除进度条，避免混乱
            dynamic_ncols=True,  # 动态调整宽度
            smoothing=0.1,  # 平滑显示
            miniters=1,  # 每次更新都显示
            mininterval=0.1,  # 最小更新间隔
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        if self.pbar:
            if exc_type is None:
                # 正常完成
                elapsed = time.time() - self.start_time if self.start_time else 0
                # 清理显示，打印完成信息
                self.pbar.close()
                print(f"✅ {self.desc} 完成 (耗时: {elapsed:.1f}s)")
            else:
                # 异常退出
                self.pbar.close()
                print(f"❌ {self.desc} 中断")

    def update(self, n: int = 1, postfix: Optional[str] = None):
        """更新进度"""
        if self.pbar:
            self.pbar.update(n)
            if postfix:
                self.pbar.set_postfix_str(postfix)

    def set_description(self, desc: str):
        """设置描述文字"""
        if self.pbar:
            self.pbar.set_description(desc)


def with_progress(desc: str = "处理中", unit: str = "item"):
    """
    装饰器：为函数添加进度显示

    适用于处理可迭代对象的函数
    """

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(items, *args, **kwargs):
            # 如果items不是list，转换为list以获取长度
            if not hasattr(items, "__len__"):
                items = list(items)

            with ProgressTracker(len(items), desc, unit) as tracker:
                results = []
                for i, item in enumerate(items):
                    result = func(item, *args, **kwargs)
                    results.append(result)
                    tracker.update(1, f"当前: {str(item)[:20]}")
                return results

        return wrapper

    return decorator


def progress_wrapper(iterable, desc: str = "处理中", unit: str = "item"):
    """
    为可迭代对象添加进度条的简单包装器

    Args:
        iterable: 可迭代对象
        desc: 描述文字
        unit: 单位

    Returns:
        带进度条的迭代器
    """
    if hasattr(iterable, "__len__"):
        total = len(iterable)
    else:
        # 对于生成器等，先转换为list
        iterable = list(iterable)
        total = len(iterable)

    return tqdm(
        iterable, 
        desc=desc, 
        unit=unit, 
        total=total, 
        ncols=80,
        leave=False,
        dynamic_ncols=True
    )


def estimate_time_remaining(current: int, total: int, start_time: float) -> str:
    """
    估算剩余时间

    Args:
        current: 当前进度
        total: 总数量
        start_time: 开始时间

    Returns:
        格式化的剩余时间字符串
    """
    if current == 0:
        return "计算中..."

    elapsed = time.time() - start_time
    if elapsed == 0:
        return "计算中..."

    items_per_second = current / elapsed
    remaining_items = total - current
    remaining_seconds = remaining_items / items_per_second

    if remaining_seconds < 60:
        return f"{remaining_seconds:.0f}秒"
    elif remaining_seconds < 3600:
        return f"{remaining_seconds/60:.1f}分钟"
    else:
        return f"{remaining_seconds/3600:.1f}小时"


class BatchProgressTracker:
    """批处理进度跟踪器"""

    def __init__(self, total_batches: int, batch_size: int, desc: str = "批处理"):
        """
        初始化批处理进度跟踪器

        Args:
            total_batches: 总批次数
            batch_size: 每批大小
            desc: 描述文字
        """
        self.total_batches = total_batches
        self.batch_size = batch_size
        self.desc = desc
        self.current_batch = 0
        self.pbar: Optional[tqdm] = None
        self.start_time: Optional[float] = None

    def __enter__(self):
        """进入上下文管理器"""
        self.start_time = time.time()
        self.pbar = tqdm(
            total=self.total_batches,
            desc=f"{self.desc} (0/{self.total_batches})",
            unit="批",
            ncols=100,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出上下文管理器"""
        if self.pbar:
            if exc_type is None:
                elapsed = time.time() - self.start_time if self.start_time else 0
                self.pbar.set_description(f"{self.desc} 已完成 (耗时: {elapsed:.1f}s)")
            else:
                self.pbar.set_description(f"{self.desc} 中断")
            self.pbar.close()

    def update_batch(self, batch_idx: int, batch_info: str = ""):
        """更新批次进度"""
        if self.pbar:
            self.current_batch = batch_idx + 1
            desc = f"{self.desc} ({self.current_batch}/{self.total_batches})"
            if batch_info:
                desc += f" - {batch_info}"
            self.pbar.set_description(desc)
            self.pbar.update(1)


# 使用示例
if __name__ == "__main__":
    import time

    # 示例1: 基本进度跟踪
    print("示例1: 基本进度跟踪")
    with ProgressTracker(100, "下载数据", "文件") as tracker:
        for i in range(100):
            time.sleep(0.01)  # 模拟工作
            tracker.update(1, f"处理文件_{i}")

    # 示例2: 批处理进度
    print("\n示例2: 批处理进度")
    total_items = 50
    batch_size = 10
    total_batches = (total_items + batch_size - 1) // batch_size

    with BatchProgressTracker(total_batches, batch_size, "批量处理") as batch_tracker:
        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, total_items)
            batch_items = list(range(start_idx, end_idx))

            batch_tracker.update_batch(batch_idx, f"处理 {len(batch_items)} 个项目")

            # 模拟批次内的处理
            for item in batch_items:
                time.sleep(0.02)

    print("\n所有示例完成！")
