# 脚本目录使用指南

## 🎯 核心脚本 (仅保留必要功能)

### 1. `update_market_data.py` - 主要数据更新工具

**用途**: 您更新量价数据的唯一入口点
**功能**:

- 更新现有币种价格数据
- 检测新币种并下载历史数据
- 重建每日汇总数据用于指数计算
- 支持交互式菜单和自动化模式

**使用方式**:

```bash
# 交互式使用（推荐）
python scripts/update_market_data.py

# 自动化使用
python scripts/update_market_data.py --auto --all
```

### 2. `data_quality_checker.py` - 数据质量检查工具

**用途**: 检查和修复数据质量问题
**功能**:

- 扫描所有币种文件检查数据质量
- 识别数据不足、过期、读取错误等问题
- 提供自动修复功能

**使用方式**:

```bash
python scripts/data_quality_checker.py
```

### 3. `calculate_index.py` - 指数计算工具

**用途**: 计算市值加权的区块链资产指数
**功能**:

- 计算指定时间范围的指数值
- 生成指数分析报告

**使用方式**:

```bash
python scripts/calculate_index.py
```

### 4. `crypto30_comprehensive_analysis.py` - 综合分析工具

**用途**: 生成Crypto30指数的详细分析报告
**功能**:

- 计算Crypto30指数历史表现
- 生成成分币种变化分析
- 输出月度报告

**使用方式**:

```bash
python scripts/crypto30_comprehensive_analysis.py
```

## 🗑️ 已删除的重复脚本

以下脚本因功能重叠或过时已被删除，其功能已整合到主脚本中：

- ~~`build_daily_summary.py`~~ → 整合到 `update_market_data.py`
- ~~`rebuild_daily_files.py`~~ → 整合到 `update_market_data.py`  
- ~~`reorder_daily_files_by_market_cap.py`~~ → 整合到 `update_market_data.py`
- ~~`update_all_existing_coins.py`~~ → 整合到 `update_market_data.py`
- ~~`update_all_metadata.py`~~ → 整合到 `update_market_data.py`
- ~~`quick_maintenance.py`~~ → 整合到 `update_market_data.py`
- ~~`daily_maintenance.py`~~ → 整合到 `update_market_data.py`
- ~~`incremental_daily_update.py`~~ → 整合到 `update_market_data.py`
- ~~`update_price_data.py`~~ → 整合到 `update_market_data.py`

## 📊 数据存储架构说明

### 当前存储方式

- **主存储**: SQLite数据库 (`data/market.db`) - 高性能查询
- **备份存储**: CSV文件 (`data/coins/*.csv`) - 兼容性和可读性

### 数据更新流程

1. **价格更新** → CSV文件 → 同步到数据库
2. **新币种检测** → CSV文件 → 同步到数据库  
3. **每日汇总** → 从数据库查询生成

### 数据访问建议

- **指数计算**: 优先使用数据库 (10-100倍性能提升)
- **单文件操作**: 可以直接读取CSV
- **批量分析**: 必须使用数据库

## 🚀 最佳实践

### 日常数据更新

1. 运行 `update_market_data.py` 选择 "完整更新"
2. 定期运行 `data_quality_checker.py` 检查数据质量

### 研究分析

1. 使用 `calculate_index.py` 计算指数
2. 使用 `crypto30_comprehensive_analysis.py` 生成详细报告

### 故障排查

1. 数据问题 → `data_quality_checker.py`
2. 更新失败 → 检查 `logs/` 目录下的日志文件
3. 性能问题 → 确保使用数据库模式而非CSV模式

---

💡 **核心理念**: 保持简单，一个功能一个脚本，避免重复和混乱。
