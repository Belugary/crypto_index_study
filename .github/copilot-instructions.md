# 区块链资产指数研究项目开发指南

## 核心原则

### 1. 简单至上

- 优先选择最直接的解决方案，避免过度工程化和复杂抽象
- 代码清晰胜过聪明
- **简单有效胜过复杂完美** - 当指南本身变得复杂时，说明偏离了核心原则

### 2. 用户优先体验

- 功能设计围绕用户实际需求，避免暴露技术细节给用户
- **进度可见性**：任何预期超过 5 秒或处理超过 10 个项目的操作必须显示进度
- **时间可预期**：使用 `tqdm` 显示进度条、剩余时间、处理速度
- **合理并发**：I/O 密集型任务使用线程池，CPU 密集型任务考虑进程池
- **错误友好**：异常时给出清晰的错误信息和可能的解决方案
- **用户的时间宝贵** - 焦虑的等待比慢一点但有预期的处理更糟糕

### 3. 信任权威数据源

- 优先使用 CoinGecko 官方分类
- 自定义逻辑只作为补充，不替代权威源

## 技术规范

### 代码规范

- Python + CoinGecko API
- 中文注释，清晰的类型提示
- 测试覆盖核心功能
- **强制要求：循环处理>10 项目时必须使用进度条**
- **推荐：网络请求、文件操作等 I/O 操作使用并发处理**

### 文档规范

- 朴实无华，避免营销化语言
- 聚焦用户使用方法，而非技术实现
- 保持简洁，突出核心价值

### 数据规范

- 默认 510 个原生币，可配置
- 自动过滤稳定币和包装币
- 数据更新日期格式：(最近更新: YYYY-MM-DD)

### 项目结构

- `src/` 核心代码
- `scripts/` 用户脚本（薄封装）
- `tests/` 测试用例
- `data/` 数据资产
- `logs/` 日志文件
- `docs/` 文档，包括 `user_experience_guide.md`

## 开发实践指南

### 用户体验工具与代码示例

#### 进度显示

```python
# 基本进度条（推荐用于所有循环）
from tqdm import tqdm
for item in tqdm(items, desc="处理数据", unit="个"):
    process(item)

# 高级进度跟踪
from src.utils.progress_utils import ProgressTracker
with ProgressTracker(len(items), "批量下载", "文件") as tracker:
    for item in items:
        result = download(item)
        tracker.update(1, f"当前: {item[:20]}")
```

#### 并发处理

```python
# 自动选择并发策略
from src.utils.concurrent_utils import auto_concurrent_map
results = auto_concurrent_map(
    process_function,
    items,
    threshold=10,  # 超过10个才并发
    desc="批量处理"
)

# 手动控制并发
from src.utils.concurrent_utils import ConcurrentProcessor
processor = ConcurrentProcessor(max_workers=4)
results = processor.process_batch(func, items, desc="并发处理")
```

### 代码审查必检项

#### 用户体验检查清单

- [ ] 循环处理 >10 个项目时使用了进度条
- [ ] 网络请求使用了合理的并发处理
- [ ] 长时间运行的函数有时间预期显示
- [ ] 错误信息对用户友好，提供解决建议
- [ ] 没有"黑箱"操作让用户焦虑等待

#### 性能检查清单

- [ ] I/O 密集型任务使用线程池 (`use_processes=False`)
- [ ] CPU 密集型任务考虑进程池 (`use_processes=True`)
- [ ] 批量操作有合理的批次大小 (通常 20-100)
- [ ] 避免在循环中进行单个网络请求

## 质量保证与测试

### 基本要求

- 所有测试必须通过
- 重构后立即运行测试
- 配置变更检查相关文件一致性
- Git 提交信息清晰，按功能分批提交
- **用户体验检查**：所有长时间操作都有进度反馈

### 测试管理

- 重构后必须运行 `python -m pytest tests/ -v`
- 更新测试文件中的过时 import 路径
- 保持 100%测试通过率

## 附录：常见问题与解决方案

### 大型计划管理

**临时计划跟踪最佳实践**：

- **创建跟踪文档**: 大型计划开始时在项目根目录创建 `.temp_plan_tracker.md`
- **实时更新**: 记录计划目标、任务分解、进展状态、重要决策
- **防止丢失**: 避免在对话重启或轮次过多时丢失计划上下文
- **完成清理**: 计划结束后删除临时文件，重要内容归档到正式文档
- **Git 忽略**: 确保创建的临时文件在 `.gitignore` 中排除，不会污染版本控制

### 数据源架构要点

- 指数计算使用 `data/daily/daily_files/` 每日汇总数据
- 通过 `DailyDataAggregator` 统一数据访问
- CSV 文件包含表头，使用 `pd.read_csv()` 而非 `header=None`

### 常见错误模式

**时间戳转换错误**:

**⚠️ 处理时间戳问题前必须先查阅 `docs/timestamp_handling_memo.md`**

```
ValueError: could not convert string to float: 'timestamp'
```

- 原因：CSV 包含表头，直接转换失败
- 解决：使用 `DailyDataAggregator` 或正确处理表头
- 详细方案：参考时间戳处理备忘录

**分类器字段不匹配**:

- 检查返回字段名（如 `is_wrapped_coin` vs `is_wrapped`）
- 确保分类器覆盖所有衍生品类型
- 避免过宽的异常处理

**备份系统存储爆炸**:

- 默认关闭自动备份功能 (`backup_enabled=False`)
- 实现智能备份管理，限制保留数量（如最近 3 个）
- 用户按需启用，避免无意识的存储消耗
- 大数据集操作特别注意备份策略

**用户体验问题**:

- 没有进度条的长循环让用户焦虑
- 顺序处理大量 I/O 操作效率低下
- 异常信息技术化，用户难以理解
- 解决：参考 `docs/user_experience_guide.md`

---

## 最后,再次提醒:

**当指南本身变得复杂时，说明偏离了核心原则。简单有效胜过复杂完美。**
