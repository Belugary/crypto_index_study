#!/usr/bin/env python3
"""通用静默/抑制输出工具

提供统一的上下文管理器用于在代码块内：
1. 屏蔽 stdout / stderr
2. 临时提升日志屏蔽阈值 (默认禁止 < CRITICAL 所有日志)
3. 可选捕获内部输出，便于调试或后续分析

使用目的：
Notebook / 批处理脚本里保持输出极简，不重复冗长的 try/finally + redirect 代码。

示例：
    from src.utils.quiet_utils import silent_all
    with silent_all():
        data = aggregator.get_daily_data('2024-01-01')
        clean = displayer.clean_data(data)

    # 需要调试时：
    with silent_all(capture=True) as buf:
        risky_op()
    debug_text = buf.getvalue()

设计要点：
    - 恢复顺序可靠：先恢复 stdout/stderr，再恢复 logging.disable
    - 嵌套安全：记录进入前的 disable 阈值，退出时恢复
    - 不吞掉异常：异常正常向上抛出
"""

from __future__ import annotations

import logging
import io
import contextlib
from typing import Optional, Iterator


@contextlib.contextmanager
def silent_all(capture: bool = False, disable_level: int = logging.CRITICAL) -> Iterator[Optional[io.StringIO]]:
    """在代码块内静默所有标准输出与低于指定级别的日志。

    Args:
        capture: 是否返回捕获的输出缓冲区 (StringIO)。False 时丢弃。
        disable_level: 临时设置的全局 logging.disable 阈值，默认禁止除 CRITICAL 外所有日志。

    Yields:
        StringIO | None: 若 capture=True，返回捕获缓冲区；否则返回 None。
    """
    prev_disable = logging.root.manager.disable  # 进入前的全局屏蔽阈值
    buffer: Optional[io.StringIO] = io.StringIO() if capture else io.StringIO()
    try:
        logging.disable(disable_level)
        with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(buffer):
            yield buffer if capture else None
    finally:
        # 恢复 logging 屏蔽阈值
        logging.disable(prev_disable)


def run_silently(func, *args, capture: bool = False, **kwargs):
    """便捷函数：静默执行一个函数并返回其结果与可选捕获输出。

    Returns:
        (result, captured_text | None)
    """
    with silent_all(capture=capture) as buf:
        result = func(*args, **kwargs)
    captured_text = buf.getvalue() if (capture and buf is not None) else None  # type: ignore[attr-defined]
    return result, captured_text
