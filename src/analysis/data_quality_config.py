"""
数据质量检查配置模块

独立管理数据质量检查的配置参数。
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DataQualityConfig:
    """数据质量检查配置"""
    
    # 路径配置
    data_dir: str = "data/coins"
    daily_dir: str = "data/daily"
    
    # 数据质量标准
    min_rows: int = 100
    max_days_old: int = 2
    min_data_span_days: int = 30
    
    # 性能配置
    use_database: bool = True
    
    def resolve_paths(self, project_root: Optional[Path] = None) -> 'ResolvedPaths':
        """解析配置中的路径"""
        if project_root is None:
            from ..utils.path_utils import find_project_root
            project_root = find_project_root()
        
        from ..utils.path_utils import resolve_data_path
        
        return ResolvedPaths(
            data_dir=resolve_data_path(self.data_dir, project_root),
            daily_dir=resolve_data_path(self.daily_dir, project_root),
            project_root=project_root
        )


@dataclass
class ResolvedPaths:
    """解析后的路径配置"""
    data_dir: Path
    daily_dir: Path
    project_root: Path
