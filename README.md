# 区块链资产指数研究项目

本项目帮助你快速获取、管理和分析主流区块链资产的历史量价数据，适用于量化分析、投资研究和学术用途。

## 你能获得什么？

- 市值前 800 名加密货币的日级别历史数据（价格、交易量、市值等，最近更新: 2025-07-12）
- 支持自定义原生币目标数量（默认 510 个），系统自动扩展搜索范围，确保目标达成
- 可选自动过滤稳定币和包装币，专注原生资产
- 一键批量下载、自动增量更新，数据始终保持最新
- 完整 API 封装，支持币种列表、市场数据、历史行情等常用查询
- 支持增量每日数据更新脚本，便于新币种检测与历史数据集成
- 中文文档和示例，快速上手

## 快速开始

1. 安装依赖

```bash
cd crypto_index_study
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

2. 配置 API Key（可选）

如有 CoinGecko Pro API Key，创建 `.env` 文件：

```env
COINGECKO_API_KEY=你的API_KEY
```

3. 下载数据

```bash
# 下载并更新所有元数据
python scripts/update_all_metadata.py

# 智能更新原生币价格数据（默认 510 个）
python scripts/update_price_data.py

# 自定义原生币数量（如 700 个）
python scripts/update_price_data.py --native-coins 700
```

4. 增量每日数据更新

```bash
# 监控前1000名并更新历史数据
python scripts/incremental_daily_update.py

# 自定义监控范围（如前800名）并试运行
python scripts/incremental_daily_update.py --top-n 800 --dry-run
```

5. 使用 API 查询

```python
from src.api.coingecko import CoinGeckoAPI
api = CoinGeckoAPI()
markets = api.get_coins_markets(vs_currency='usd', per_page=10)
for i, coin in enumerate(markets, 1):
    print(f"{i}. {coin['name']}: ${coin['current_price']} ({coin['price_change_percentage_24h']:.2f}%)")
```

6. 使用核心更新模块

```python
# 价格数据智能更新
from src.updaters.price_updater import PriceDataUpdater
updater = PriceDataUpdater()
updater.update_with_smart_strategy(target_native_coins=510)

# 元数据批量管理
from src.updaters.metadata_updater import MetadataUpdater
meta_updater = MetadataUpdater()
meta_updater.batch_update_all_metadata()
meta_updater.update_all_classification_lists()
```

7. 批量下载示例

```python
from src.downloaders.batch_downloader import create_batch_downloader
downloader = create_batch_downloader()
results = downloader.download_batch(top_n=20, days="30")
print(f"已下载 {len(downloader.list_downloaded_coins())} 个币种")
```

8. 币种分类示例

```python
from src.classification import StablecoinChecker, WrappedCoinChecker
stable_checker = StablecoinChecker()
wrapped_checker = WrappedCoinChecker()

# 检查是否为稳定币
result = stable_checker.is_stablecoin("tether")
print(f"Tether 是稳定币: {result['is_stablecoin']}")

# 检查是否为包装币
result = wrapped_checker.is_wrapped_coin("wrapped-bitcoin")
print(f"WBTC 是包装币: {result['is_wrapped_coin']}")
```

9. 指数计算示例

```python
from src.index import MarketCapWeightedIndexCalculator

# 创建市值加权指数计算器
calculator = MarketCapWeightedIndexCalculator(
    exclude_stablecoins=True,  # 排除稳定币
    exclude_wrapped_coins=True  # 排除包装币
)

# 计算指数
index_df = calculator.calculate_index(
    start_date="2025-01-01",
    end_date="2025-01-31",
    base_date="2025-01-01",
    base_value=1000.0,
    top_n=30  # 前30名币种
)

# 保存结果
calculator.save_index(index_df, "data/indices/my_index.csv")
print(f"指数期间收益率: {(index_df.iloc[-1]['index_value'] / index_df.iloc[0]['index_value'] - 1) * 100:.2f}%")
```

## 指数计算功能

### 快速计算指数

```bash
# 计算前30名币种的市值加权指数
python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --top-n 30

# 包含稳定币和包装币
python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --include-stablecoins --include-wrapped-coins

# 自定义基准日期和指数值
python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --base-date 2020-01-01 --base-value 100

# 运行指数计算示例
python examples/index_calculation_example.py
```

### 指数特点

- **市值加权**: 根据每日市值动态调整权重
- **成分动态**: 每日重新选择前 N 名币种作为成分
- **分类过滤**: 支持排除稳定币和包装币，专注原生资产
- **时间灵活**: 支持任意时间范围和基准日期
- **精度保证**: 缺失数据时报错提示，确保计算准确性

## 如何运行测试

```bash
# 运行所有单元测试
python -m unittest discover tests
```

## 更新日志

查看完整的更新历史，请参考 [CHANGELOG.md](CHANGELOG.md)。

## 常见问题

- 没有 API Key 也能用，项目自带部分历史数据供研究
- 下载失败或数据不全？请检查网络、API Key，并查看 `logs/` 目录日志
- 支持过滤稳定币和包装币，分类依据严格采用 CoinGecko 官方标签

## 目录结构

```
src/                         # 核心功能代码
├── api/                     # API 接口封装
├── classification/          # 币种分类器（稳定币、包装币识别）
├── downloaders/             # 数据下载器（批量下载、日度聚合等）
├── updaters/                # 数据更新核心逻辑 🆕
│   ├── price_updater.py     # 价格数据智能更新策略
│   └── metadata_updater.py  # 元数据批量管理功能
├── analysis/                # 数据分析模块
└── utils.py                 # 工具函数
scripts/                     # 自动化脚本（薄封装层）
├── update_price_data.py     # 量价数据更新脚本
├── update_all_metadata.py   # 元数据批量更新脚本
├── build_daily_summary.py   # 日度市场摘要构建脚本
└── incremental_daily_update.py # 增量每日数据更新脚本
examples/                    # 使用示例
tests/                       # 测试代码
data/                        # 数据资产 (coins/, metadata/)
logs/                        # 日志文件
```

## 许可证

MIT License：可自由使用、修改、分发和私有化。软件按现状提供，不承担任何担保责任。使用时请保留原始版权声明。

## 免责声明

本项目仅供学习和研究，不构成投资建议。投资有风险，盈亏自负。
