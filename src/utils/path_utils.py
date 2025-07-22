"""
路径解析工具模块

提供项目中通用的路径解析功能，避免重复代码。
"""

from pathlib import Path
from typing import Optional


def find_project_root(start_path: Optional[Path] = None) -> Path:
    """
    查找项目根目录
    
    Args:
        start_path: 开始查找的路径，默认为当前工作目录
        
    Returns:
        项目根目录的Path对象
        
    Raises:
        FileNotFoundError: 如果找不到项目根目录
    """
    if start_path is None:
        start_path = Path.cwd()
    
    current = start_path.resolve()
    
    # 查找包含项目标志的目录
    while current.parent != current:
        # 最可靠的标志是.git目录
        if (current / ".git").exists():
            return current
        # 或者同时包含src目录和requirements.txt文件
        elif (current / "src").exists() and (current / "requirements.txt").exists():
            return current
        current = current.parent
    
    # 如果找不到，抛出异常
    raise FileNotFoundError(
        f"无法找到项目根目录。请确保从项目目录内运行，"
        f"或项目包含 .git 目录或 src/ 和 requirements.txt 文件。"
        f"搜索起始路径: {start_path}"
    )


def resolve_data_path(relative_path: str, project_root: Optional[Path] = None) -> Path:
    """
    解析数据路径（相对于项目根目录）
    
    Args:
        relative_path: 相对路径字符串（如 "data/coins"）
        project_root: 项目根目录，默认自动查找
        
    Returns:
        解析后的绝对路径
    """
    if project_root is None:
        project_root = find_project_root()
    
    path = Path(relative_path)
    if path.is_absolute():
        return path
    else:
        return project_root / path


def ensure_directory(path: Path) -> Path:
    """
    确保目录存在，如果不存在则创建
    
    Args:
        path: 目录路径
        
    Returns:
        目录路径
    """
    path.mkdir(parents=True, exist_ok=True)
    return path
