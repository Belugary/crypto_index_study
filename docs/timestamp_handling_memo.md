# 时间戳与日期处理备忘录

**重要提醒：处理任何时间戳相关问题前，必须先阅读此备忘录！**

## 核心原则

1. **永远检查数据格式**：CSV 文件可能包含表头
2. **统一使用工具类**：避免直接操作时间戳
3. **明确时区处理**：UTC vs 本地时间
4. **数据类型一致性**：timestamp 字段的数据类型

## 常见错误及解决方案

### 错误 1：CSV 表头处理错误

```
ValueError: could not convert string to float: 'timestamp'
```

**原因**：CSV 文件包含表头，直接转换 'timestamp' 字符串失败

**解决方案**：

- 使用 `pd.read_csv()` 而非 `header=None`
- 通过 `DailyDataAggregator` 统一数据访问
- 检查第一行是否为表头：`df.iloc[0]['timestamp']`

### 错误 2：时间戳格式不一致

**问题**：混用 Unix 时间戳、ISO 格式、日期字符串

**解决方案**：

```python
# 统一转换函数
def normalize_timestamp(ts):
    if isinstance(ts, str):
        if ts.isdigit():
            return int(ts)  # Unix timestamp
        else:
            return pd.to_datetime(ts).timestamp()  # ISO format
    return ts
```

### 现象 3：实时数据时间戳 ℹ️ **系统正常行为**

**现象描述**：

```text
正常: 2025-07-13 00:00:00 UTC  (24小时间隔)
临时: 2025-07-13 07:08:54 UTC  (7.15小时间隔) ← 实时数据
```

**出现原因**：

- 增量更新时，当日 `00:00:00 UTC` 数据尚未生成（T+1 逻辑）
- CoinGecko API 返回当前时间的实时价格数据
- 这是系统的正常行为，不是错误

**✅ 自动校正机制**：

价格更新器使用**覆盖模式**，会自动校正：

1. `update_with_smart_strategy()` 重新下载完整历史数据
2. 完全替换整个 CSV 文件，临时记录自动被正确数据替换
3. 符合"简单胜于复杂"原则

**验证机制**：

```bash
python scripts/update_price_data.py
```

### 错误 3：时区混乱

**项目状态**: 🟡 **部分解决，仍有改进空间**

**已正确处理**：

- ✅ 所有 CoinGecko API 数据统一使用 UTC
- ✅ 批量下载器元数据使用 `datetime.now(timezone.utc)`
- ✅ API 文档明确标注 `00:00:00 UTC`

**仍存在的问题**：

- ⚠️ 统计时间记录使用本地时间 (`price_updater.py` 第 311, 440 行)
- ⚠️ 报告文件命名使用本地时间 (`price_updater.py` 第 516 行)
- ⚠️ 增量更新器操作日志使用本地时间

**影响评估**：

- 🟢 **数据质量**: 无影响，所有币种价格数据时间戳正确
- 🟡 **用户体验**: 轻微影响，日志和报告时间可能与用户期望不一致

**解决方案**：

```python
# 推荐：统一使用 UTC
self.stats["start_time"] = datetime.now(timezone.utc)
self.stats["end_time"] = datetime.now(timezone.utc)

# 文件命名也使用 UTC
report_name = f"smart_update_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.txt"
```

**修复优先级**: 低 (不影响核心功能，可在未来版本中改进)

## 项目中的时间戳使用模式

### 1. CoinGecko API 数据

- 返回 Unix 时间戳（毫秒级）
- 需要除以 1000 转换为秒级
- 时区：UTC

### 2. 每日数据文件

- 格式：`YYYY-MM-DD_crypto_data.csv`
- timestamp 列：Unix 时间戳（秒级）
- 包含表头行

### 3. 数据聚合器 (DailyDataAggregator)

- **推荐使用**：避免直接读取 CSV
- 自动处理表头问题
- 统一时间戳格式

## 最佳实践

### 1. 数据读取

```python
# 推荐：使用聚合器
from src.analysis.daily_data_aggregator import DailyDataAggregator
aggregator = DailyDataAggregator()
data = aggregator.load_daily_data(date_str)

# 避免：直接读取 CSV
# df = pd.read_csv(file_path, header=None)  # 容易出错
```

### 2. 时间戳转换

```python
# 推荐：明确转换
timestamp = pd.to_datetime(date_str).timestamp()

# 避免：假设格式
# timestamp = float(date_str)  # 可能失败
```

### 3. 日期格式化

```python
# 项目标准格式
date_str = datetime.now().strftime('%Y-%m-%d')  # YYYY-MM-DD
update_note = f"(最近更新: {date_str})"
```

## 调试检查清单

遇到时间戳相关错误时，按顺序检查：

1. [ ] CSV 文件是否包含表头？
2. [ ] 使用的是 `DailyDataAggregator` 还是直接读取？
3. [ ] 时间戳格式是否一致（秒级 vs 毫秒级）？
4. [ ] 时区是否正确处理？
5. [ ] 数据类型是否匹配（str vs int vs float）？

## 项目特定注意事项

### daily_files 目录结构

```text
data/daily/daily_files/
├── 2024-01-01_crypto_data.csv  # 包含表头
├── 2024-01-02_crypto_data.csv  # 包含表头
└── ...
```

### 表头格式

```csv
symbol,name,current_price,market_cap,total_volume,price_change_24h,price_change_percentage_24h,market_cap_rank,timestamp
```

### DailyDataAggregator 使用示例

```python
# 正确方式
aggregator = DailyDataAggregator()
data = aggregator.load_daily_data('2024-01-01')
# data 已经是处理好的 DataFrame，包含正确的时间戳

# 错误方式
df = pd.read_csv('2024-01-01_crypto_data.csv')
df['timestamp'] = df['timestamp'].astype(float)  # 可能失败
```

---

**提醒**：每次修改时间戳相关代码后，立即运行相关测试确保正确性。测试覆盖以下场景：

- 包含表头的 CSV 文件
- 不同时间戳格式的数据
- 边界日期（月初、月末、年初、年末）

**更新记录**：

- 创建日期: 2025-01-13
- 最近更新: 2025-07-14 (新增实时数据时间戳异常处理)
