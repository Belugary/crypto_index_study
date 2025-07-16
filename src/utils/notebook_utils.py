#!/usr/bin/env python3
"""
Notebook 工具模块

为 Jupyter Notebook 提供简化的导入和使用体验，
隐藏复杂的路径处理逻辑。
"""

import sys
from pathlib import Path

# 模块加载时自动设置路径
def _auto_setup_on_import():
    """模块导入时自动设置项目路径"""
    # 从当前文件位置推断项目根目录
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent  # 从 src/utils/ 回到项目根目录
    
    # 添加到 Python 路径
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    return project_root

# 导入时自动执行
_auto_setup_on_import()


def setup_project_imports():
    """
    自动设置项目导入路径
    
    这个函数会自动查找项目根目录并添加到 Python 路径中，
    让用户可以直接导入项目模块而无需关心路径配置。
    """
    # 查找项目根目录
    current_path = Path.cwd()
    project_root = current_path
    
    while not (project_root / ".git").exists() and project_root.parent != project_root:
        project_root = project_root.parent
    
    # 添加到 Python 路径
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    
    return project_root


def get_daily_data_aggregator():
    """
    获取配置好的 DailyDataAggregator 实例
    
    Returns:
        DailyDataAggregator: 已配置的数据聚合器实例
    """
    # 确保项目路径已添加
    setup_project_imports()
    
    # 导入并返回实例
    from src.downloaders.daily_aggregator import DailyDataAggregator
    return DailyDataAggregator()


def load_market_data(date_str: str, force_refresh: bool = False, include_all_coins: bool = False):
    """
    加载指定日期的市场数据
    
    Args:
        date_str: 日期字符串，格式为 'YYYY-MM-DD'
        force_refresh: 是否强制刷新缓存，默认False
        include_all_coins: 是否包含所有币种(稳定币、包装币等)，默认False(只包含原生币种)
        
    Returns:
        pandas.DataFrame: 市场数据，根据 include_all_coins 参数决定是否包含稳定币和包装币
    """
    from src.downloaders.daily_aggregator import DailyDataAggregator
    
    # 确保使用绝对路径
    project_root = setup_project_imports()
    data_dir = str(project_root / "data" / "coins")
    output_dir = str(project_root / "data" / "daily")
    
    aggregator = DailyDataAggregator(data_dir=data_dir, output_dir=output_dir)
    # 参数转换：include_all_coins 直接对应 result_include_all
    return aggregator.get_daily_data(date_str, force_refresh=force_refresh, result_include_all=include_all_coins)
