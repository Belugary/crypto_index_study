# 自动化排序机制

## 功能说明

增量更新完成后自动重排序每日文件，确保排名与市值一致。

## 核心特性

- **自动触发**: 数据更新完成后自动执行，无需人工干预
- **智能优化**: 只处理受影响的日期范围，支持并行处理
- **灵活模式**: 支持全量/增量/手动三种运行模式

## 使用方式

**自动模式（推荐）**:

```python
from src.updaters.incremental_daily_updater import create_incremental_updater
updater = create_incremental_updater()
results = updater.update()  # 自动包含重排序
```

**手动模式**:

```bash
# 处理所有文件
python scripts/reorder_daily_files_by_market_cap.py

# 处理指定日期范围
python scripts/reorder_daily_files_by_market_cap.py --start-date 2025-01-01 --end-date 2025-01-31

# 试运行验证
python scripts/reorder_daily_files_by_market_cap.py --dry-run
```

## 技术要点

- **并行处理**: 使用 ThreadPoolExecutor，默认 8 个工作线程
- **原子操作**: 单文件处理失败不影响其他文件
- **智能范围**: 自动检测受影响日期，避免全量处理
- **数据安全**: 只修改排名字段，保持原始数据不变

## 性能参考

| 模式     | 处理量     | 时间    |
| -------- | ---------- | ------- |
| 增量模式 | 10-50 文件 | ~5 秒   |
| 全量模式 | 4400+文件  | ~3 分钟 |
