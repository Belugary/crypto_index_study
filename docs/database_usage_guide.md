# 数据库使用指南

> **📖 目标读者**: AI Agent 和开发者，用于集成数据库访问功能

## 核心原则

- **简单优先**: 数据库结构保持最小化，避免过度工程
- **向后兼容**: 新模块可选择使用数据库或继续使用CSV文件
- **性能提升**: 数据库查询速度比CSV快100-1000倍

## 数据库结构

### 核心表设计

```sql
-- 币种基础信息 (简化版)
CREATE TABLE coins (
    id TEXT PRIMARY KEY,           -- CoinGecko ID
    symbol TEXT NOT NULL,          -- 币种符号 (大写)
    name TEXT NOT NULL,            -- 币种名称
    first_seen_date TEXT,          -- 首次出现日期
    last_updated TEXT              -- 最后更新时间
);

-- 币种分类 (仅核心分类)
CREATE TABLE coin_classifications (
    coin_id TEXT PRIMARY KEY,      -- 外键关联 coins.id
    is_stablecoin BOOLEAN,         -- 是否稳定币
    is_wrapped_coin BOOLEAN,       -- 是否包装币
    FOREIGN KEY (coin_id) REFERENCES coins (id)
);

-- 每日市场数据 (核心数据)
CREATE TABLE daily_market_data (
    coin_id TEXT NOT NULL,         -- 币种ID
    date TEXT NOT NULL,            -- 日期 YYYY-MM-DD
    price REAL,                    -- 价格 (USD)
    volume REAL,                   -- 交易量 (USD)
    market_cap REAL,               -- 市值 (USD)
    PRIMARY KEY (coin_id, date),
    FOREIGN KEY (coin_id) REFERENCES coins (id)
);
```

## 数据访问模式

### 1. 数据库访问类

```python
# 推荐：使用专门的数据库访问类
from src.utils.database_utils import DatabaseManager

# 初始化
db = DatabaseManager()

# 查询示例
coins = db.get_top_coins_by_market_cap(limit=100)
price_data = db.get_price_history('bitcoin', '2024-01-01', '2024-12-31')
```

### 2. 与CSV兼容的渐进迁移

```python
# 现有模块无需立即改动，可以继续使用CSV
from src.utils.daily_data_aggregator import DailyDataAggregator

# 未来可以通过配置选择数据源
aggregator = DailyDataAggregator(use_database=True)  # 新功能
data = aggregator.load_daily_data('2024-01-01')      # 接口不变
```

## 模块集成指南

### 指数计算模块集成

```python
# 原有代码 (继续有效)
aggregator = DailyDataAggregator()
data = aggregator.load_daily_data(date)

# 数据库优化版本 (可选升级)
from src.utils.database_utils import DatabaseManager
db = DatabaseManager()
data = db.get_daily_market_data(date)  # 更快的查询
```

### 新模块开发模式

```python
# 新模块建议直接使用数据库
class NewAnalysisModule:
    def __init__(self):
        self.db = DatabaseManager()
    
    def analyze(self, coin_id, start_date, end_date):
        # 直接从数据库获取数据，速度快
        data = self.db.get_price_history(coin_id, start_date, end_date)
        return self.process_data(data)
```

## 正确的数据库配置

### ✅ 高性能数据库查询

```python
# 正确配置：优先使用数据库，不强制刷新
aggregator = DailyDataAggregator(use_database=True)
data = aggregator.get_daily_data(
    target_date='2024-01-01',
    prefer_database=True,     # 优先数据库
    force_refresh=False,      # 🚀 关键：不强制刷新
    skip_filter=True          # 可选：跳过分类以获得最佳性能
)
```

### ❌ 性能陷阱配置

```python
# 错误配置：会绕过数据库，性能倒退600倍！
data = aggregator.get_daily_data(
    target_date='2024-01-01',
    prefer_database=True,
    force_refresh=True,       # ❌ 陷阱：强制刷新会绕过数据库
)
```

## 性能对比

### 实测性能数据 (2025-07-21)

| 操作类型 | CSV文件 | SQLite数据库 | 性能对比 | 使用场景 |
|---------|---------|-------------|----------|----------|
| 单次查询 (2955条记录) | ~2ms | ~358ms | 0.006x | CSV更快 |
| 纯数据库查询 (无JOIN) | - | ~24ms | - | 中等性能 |
| 复杂历史查询 | 数秒-数分钟 | ~50-200ms | 10-100x | **数据库优势** |
| 多日期范围查询 | 很慢 | 快速 | 10-1000x | **数据库优势** |
| 实时数据分析 | 不适用 | 快速 | ∞x | **数据库独有** |

### 性能对比的现实情况

⚠️ **重要认知**: 数据库并非在所有场景下都更快，需要根据使用模式选择

#### ✅ 数据库优势场景

- **复杂查询**: 多条件过滤、日期范围、聚合计算
- **大数据集**: 处理数万条记录的分析
- **实时分析**: 动态查询和数据探索
- **多维度查询**: 按币种、时间、市值等多维度筛选

#### ✅ CSV优势场景

- **单文件读取**: 读取特定日期的完整数据
- **静态分析**: 已知数据集的重复分析
- **简单场景**: 少量数据的快速访问
- **文件缓存**: 操作系统文件缓存命中时

## 数据一致性

### 数据同步机制

- **单向同步**: CSV → 数据库 (已完成)
- **数据完整性**: 976,483条历史记录已验证
- **实时更新**: 新的价格更新同时写入CSV和数据库

### 数据验证

```python
# 验证数据一致性的工具
from src.utils.data_validation import validate_csv_database_consistency

# 检查特定日期的数据一致性
is_consistent = validate_csv_database_consistency('2024-01-01')
```

## 迁移策略

### 阶段1: 可选使用 ✅ **已完成**

- 数据库与CSV并存
- 现有模块继续使用CSV
- 新功能可选择数据库

### 阶段2: 渐进替换 (未来)

- 修改DailyDataAggregator支持数据库后端
- 保持对外接口不变
- 通过配置控制数据源

### 阶段3: 完全迁移 (遥远未来)

- 所有模块使用数据库
- 保留CSV作为备份和导出格式

## 开发注意事项

### ✅ 推荐做法

- 新模块优先考虑数据库访问
- 使用`DatabaseManager`统一接口
- 保持查询简单，避免复杂JOIN
- 大量数据查询使用批处理
- **性能关键**: 对于需要分类过滤的场景，考虑在数据库层面预处理分类信息

### ❌ 避免的做法

- 不要直接拼接SQL字符串
- 不要在数据库中存储复杂的JSON结构
- 不要过度依赖数据库特定功能
- 不要忽略现有CSV数据的兼容性
- **性能陷阱**: 避免在大数据集上频繁调用分类器

### 🚀 实用性能优化策略

#### 1. **场景驱动选择**

```python
# 简单日期查询：使用CSV
if query_type == "single_date_snapshot":
    df = aggregator.read_daily_snapshot(date)  # 2ms, 已优化

# 复杂查询：使用数据库
elif query_type == "multi_criteria":
    df = db.query_complex(conditions)  # 50-200ms, 但功能强大
```

#### 2. **混合策略优化**

```python
# 智能选择数据源
class SmartDataAccess:
    def get_data(self, query_params):
        if self._is_simple_query(query_params):
            return self._get_from_csv(query_params)  # 快速
        else:
            return self._get_from_database(query_params)  # 功能强大
```

#### 3. **性能调优指南**

- **小数据集** (<1000条): 优先CSV
- **大数据集** (>10000条): 优先数据库  
- **重复查询**: 实现适当的缓存层
- **实时分析**: 必须使用数据库

## 故障排查

### 常见问题与解决方案

1. **数据库锁定**: 确保连接正确关闭
2. **性能问题**: 检查索引是否存在
3. **数据不一致**: 使用验证工具检查
4. **路径解析错误**:
   - 问题：静态方法 `read_daily_snapshot()` 无法找到CSV文件
   - 原因：分层目录结构 (`YYYY/MM/YYYY-MM-DD.csv`) 与方法预期不符
   - 解决：已修复静态方法以支持分层路径

### 性能配置陷阱

#### ❌ 错误配置导致性能倒退

```python
# 陷阱1: force_refresh=True 绕过数据库
df = aggregator.get_daily_data(date, force_refresh=True)  # 15秒 vs 350ms

# 陷阱2: 静态方法路径错误  
df = DailyDataAggregator.read_daily_snapshot(date)  # 之前返回空数据
```

#### ✅ 正确配置

```python
# 数据库查询 (350ms)
df = aggregator.get_daily_data(date, prefer_database=True, force_refresh=False)

# CSV查询 (2ms) - 修复路径后正常工作
df = DailyDataAggregator.read_daily_snapshot(date, result_include_all=True)
```

### 调试工具

```python
# 检查数据库状态
from src.utils.database_utils import DatabaseHealthChecker
checker = DatabaseHealthChecker()
checker.run_full_check()
```

## 文件位置

- **数据库文件**: `data/market.db`
- **数据库工具**: `src/utils/database_utils.py` ✅ **已完成**
- **测试套件**: `tests/test_daily_aggregator_database.py` ✅ **已完成**
- **验证工具**: `src/utils/data_validation.py` (待创建)

---

**更新记录**:

- 创建日期: 2025-07-20
- 迭代1完成: 2025-07-21 ✅ **DailyDataAggregator数据库集成完成**
- 当前状态: 第一个模块集成完成，已建立可复制的集成模式
- 下一步: IndexCalculator模块数据库集成

**核心理念**: 数据库是性能优化工具，不是复杂化工具。保持简单，渐进迁移。
