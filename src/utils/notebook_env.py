"""Notebook 环境快速初始化封装

只聚焦于原先 Notebook 中“步骤0 环境准备”那段样板代码的收敛，
不引入其它新特性，目标：一行完成初始化。

使用示例（推荐写法）：
    from src.utils.notebook_env import quick_env
    aggregator, displayer, get_clean_daily_data = quick_env()

然后：
    df = get_clean_daily_data("2024-07-01")
    displayer.show_table(df, top_n=20)

返回内容：
    (aggregator, displayer, get_clean_daily_data)

设计约束：
- 只封装现有逻辑，不做额外职责扩展
- 自动检测项目根目录，确保 sys.path 注入一次
- 若 data/market.db 存在则启用数据库模式，否则降级 CSV
- 默认静默内部初始化与数据获取，可通过 silent=False 关闭
- get_clean_daily_data 内部仍保持静默，符合原行为
"""
from __future__ import annotations

import sys
import contextlib
from pathlib import Path
from typing import Callable, Tuple

from .path_utils import find_project_root
from .quiet_utils import silent_all
from src.downloaders.daily_aggregator import DailyDataAggregator
from src.utils.display_utils import CryptoDataDisplayer

__all__ = ["quick_env"]

# 模块级缓存，避免重复初始化
_cached_result: Tuple[DailyDataAggregator, CryptoDataDisplayer, Callable[[str], object]] | None = None
_cached_root: Path | None = None


def quick_env(silent: bool = True, force: bool = False):
    """快速初始化并返回 (aggregator, displayer, get_clean_daily_data)。

    参数说明:
        silent (bool):
            True  (默认)  -> 初始化与后续 get_clean_daily_data 都在静默上下文运行，避免 Notebook 冗长日志。
            False        -> 输出底层日志（调试或想看数据库统计时使用）。
        force (bool):
            False (默认) -> 若之前已调用过 quick_env，会直接复用缓存的 (aggregator, displayer, helper)。
            True         -> 无视缓存强制重建：重新定位项目根、重新判定数据库模式、重新实例化对象。

    何时需要 force=True:
        1. 刚生成 / 替换了 data/market.db，希望从 CSV 模式切换到数据库模式或重新连接。
        2. 之前初始化时数据库缺失，现在补齐，需重新检测。
        3. 进行过调试/破坏性操作，怀疑对象内部状态被修改，想“干净重来”。

    不需要 force 的常见场景:
        - 在同一个 Notebook 里多次运行初始化单元。
        - 只是换不同日期取数据。

    返回:
        tuple: (DailyDataAggregator 实例, CryptoDataDisplayer 实例, get_clean_daily_data 可调用)

    使用示例:
        from src.utils.notebook_env import quick_env
        aggregator, displayer, get_daily = quick_env()          # 复用或创建
        df = get_daily("2024-07-01")
        displayer.show_table(df, top_n=20)

        # 若替换数据库后：
        aggregator, displayer, get_daily = quick_env(force=True)
    """
    global _cached_result, _cached_root
    if _cached_result is not None and not force:
        # 已初始化，仍输出最小提示保持一致性（不重复冗长信息）
        print("♻️ 已复用缓存环境 (use force=True 重新初始化)")
        return _cached_result

    project_root = find_project_root()
    _cached_root = project_root
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    db_path = project_root / "data" / "market.db"
    db_exists = db_path.exists()

    # 仅输出与原逻辑等价的最少信息
    print(f"✅ 项目路径: {project_root}")
    if db_exists:
        try:
            size_mb = db_path.stat().st_size / 1024 / 1024
            print(f"🔧 使用数据库: {db_path.name} ({size_mb:.1f}MB)")
        except Exception:
            print(f"🔧 使用数据库: {db_path.name}")
    else:
        print("⚠️ 未找到数据库文件，使用 CSV 模式")

    ctx = silent_all() if silent else contextlib.nullcontext()
    with ctx:
        aggregator = DailyDataAggregator(use_database=db_exists, db_path=str(db_path) if db_exists else None)
    displayer = CryptoDataDisplayer()

    def get_clean_daily_data(date: str):
        """获取指定日期清洗后的日度数据。

        流程:
            1. 调用 aggregator.get_daily_data(date)
            2. 用 displayer.clean_data 进行元数据补全 + 排名修正 + 清理
            3. 返回清洗后的 DataFrame（未做列格式化，方便后续计算）

        特性:
            - 始终使用初始化时的 silent 设定：silent=True 时内部再次静默，保持输出整洁。
            - 若目标日期无数据，返回空 DataFrame（调用方自行判断 empty）。
        """
        inner_ctx = silent_all() if silent else contextlib.nullcontext()
        with inner_ctx:
            raw = aggregator.get_daily_data(date)
            return displayer.clean_data(raw)

    print("🎉 环境就绪 (quick_env)")

    _cached_result = (aggregator, displayer, get_clean_daily_data)
    return _cached_result
