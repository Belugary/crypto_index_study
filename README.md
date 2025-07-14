# 区块链资产指数研究

本项目提供区块链资产历史量价数据的获取、管理和分析功能，适用于量化分析、投资研究和学术用途。

## 主要功能

- 获取加密货币日级别历史数据（价格、交易量、市值等）
- 自动过滤稳定币和包装币，专注原生资产
- 批量下载和增量更新，保持数据最新
- 市值加权指数计算，支持动态成分调整
- 每日维护自动化，集成价格更新和数据重建

## 开始使用

### 1. 安装环境

```bash
cd crypto_index_study
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

### 2. 配置 API Key（可选）

```env
# 创建 .env 文件
COINGECKO_API_KEY=你的API_KEY
```

### 3. 基础操作

```bash
# 下载元数据
python scripts/update_all_metadata.py

# 更新价格数据（默认 510 个原生币）
python scripts/update_price_data.py

# 每日维护（推荐）
python scripts/quick_maintenance.py
```

## 核心功能

### API 数据查询

```python
from src.api.coingecko import CoinGeckoAPI
api = CoinGeckoAPI()
markets = api.get_coins_markets(vs_currency='usd', per_page=10)
```

### 指数计算

```python
from src.index import MarketCapWeightedIndexCalculator

calculator = MarketCapWeightedIndexCalculator(
    exclude_stablecoins=True,
    exclude_wrapped_coins=True
)

index_df = calculator.calculate_index(
    start_date="2025-01-01",
    end_date="2025-01-31",
    top_n=30
)
```

### 币种分类

```python
from src.classification import UnifiedClassifier

classifier = UnifiedClassifier()
result = classifier.classify_coin("tether")
print(f"是稳定币: {result.is_stablecoin}")
print(f"是包装币: {result.is_wrapped_coin}")
```

## 实际应用示例

### Crypto30 指数投资分析

```bash
# 分析前30名币种的投资表现
python examples/crypto30_investment_analysis.py

# 自定义投资金额和起始日期
python examples/crypto30_investment_analysis.py --investment 50000 --start-date 2024-01-01
```

**功能特点**：

- 基于前 30 名原生币的市值加权指数
- 每日自动调仓，动态成分配置
- 自动排除稳定币和包装币
- 提供详细的投资回报分析和风险提示

详细说明请参考：[应用示例说明](examples/README.md)

### 指数计算命令行工具

```bash
# 计算指数
python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --top-n 30

# 包含稳定币和包装币
python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --include-stablecoins --include-wrapped-coins
```

## 维护工具

### 每日维护

```bash
# 标准维护
python scripts/quick_maintenance.py

# 只同步数据，不更新价格
python scripts/quick_maintenance.py --sync-only

# 完整更新
python scripts/quick_maintenance.py --full
```

### 增量更新

```bash
# 监控前1000名币种并更新历史数据
python scripts/incremental_daily_update.py

# 试运行模式
python scripts/incremental_daily_update.py --top-n 800 --dry-run
```

### 数据重建

```bash
# 重建所有历史数据
python scripts/ultra_fast_rebuild.py

# 重建指定日期范围
python scripts/ultra_fast_rebuild.py --start-date 2024-01-01 --end-date 2024-12-31
```

## 项目状态

项目运行稳定，所有核心功能可正常使用。

**(最近更新: 2025-07-13)**

## 常见问题

- 没有 API Key 也能用，项目自带部分历史数据供研究
- 下载失败或数据不全？请检查网络、API Key，并查看 `logs/` 目录日志
- 支持过滤稳定币和包装币，分类依据严格采用 CoinGecko 官方标签

## 许可证

MIT License：可自由使用、修改、分发和私有化。软件按现状提供，不承担任何担保责任。使用时请保留原始版权声明。

## 免责声明

本项目仅供学习和研究，不构成投资建议。投资有风险，盈亏自负。
