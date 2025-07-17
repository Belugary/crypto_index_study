# 项目根目录路径解析模式备忘录

## 问题模式

**症状**：类在子目录中实例化时创建意外的 `data/` 或 `logs/` 文件夹

**根因**：使用相对路径而非基于项目根目录的绝对路径

## 标准解决方案

### 项目根目录检测模式

```python
@staticmethod
def _find_project_root() -> Path:
    """查找项目根目录"""
    current = Path(__file__).parent  # 根据文件层级调整
    while current != current.parent:
        if (current / ".git").exists() or (
            (current / "src").exists() and (current / "requirements.txt").exists()
        ):
            return current
        current = current.parent
    return Path.cwd()
```

### 路径解析模式

```python
def __init__(self, data_dir: str = "data"):
    self.project_root = self._find_project_root()
    # 智能路径解析
    if Path(data_dir).is_absolute():
        self.data_dir = Path(data_dir)
    else:
        self.data_dir = self.project_root / data_dir
```

## 检查清单

新增类时检查：

- [ ] 是否使用了相对路径（如 `"data"`, `"logs"`）？
- [ ] 是否添加了 `_find_project_root()` 方法？
- [ ] 是否使用项目根目录解析路径？
- [ ] 是否添加了 `from pathlib import Path`？

## 已修复模块

核心类、分析模块、Legacy模块、脚本文件均已应用此模式。

---

**原则提醒**：简单一致胜过复杂完美
