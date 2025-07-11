# 区块链资产指数研究

一个专注于区块链资产指数研究的 Python 项目，以 CoinGecko API 作为主要数据源。提供数据获取、存储管理和基础分析功能，用于区块链资产的量化研究。

## ⚠️ 重要说明

**本项目需要 CoinGecko Pro API Key 才能运行。** 如果没有 API Key，项目已提供了部分历史数据供研究使用。

**免责声明：** 本项目采用 AI 辅助编程开发，可能存在疏漏。使用时请自行承担风险。本项目仅用于学习研究目的，不构成投资建议，投资有风险，盈亏自负。

## 项目特性

- **区块链资产指数研究**: 专注于加密货币市场指数分析
- **完整的数据管理**: 支持数据获取、存储和管理
- **历史数据资产**: 包含市值前 500 名加密货币的完整历史量价数据 (最近更新: 2025-07-11)
  - 包含价格、交易量、市值等关键指标
  - 日级别数据粒度，完整历史覆盖
- **智能资产分类**: 准确识别并分类稳定币和包装币，提升研究精度
  - 稳定币识别：基于官方分类标签自动识别各类稳定币
  - 包装币识别：严格基于 CoinGecko 分类，准确识别包装代币、桥接代币、质押代币等
  - 成功识别 87 个包装币，排除 meme 币和治理代币误判
- **智能增量更新**: 自动检测并更新过期数据，高效维护数据完整性
- **数据过滤功能**: 支持在指数研究中过滤掉稳定币和包装币，专注原生资产
  - 过滤效果：从 500+ 币种中自动识别并排除约 30% 的非原生资产 (25 个稳定币 + 87 个包装币)
- **模块化设计**: 便于扩展和定制化研究
- **中文文档**: 详细的使用说明和代码注释

## 项目架构

```
crypto_index_study/
├── src/                   # 核心代码模块
│   ├── api/               # API 封装层
│   ├── data/              # 数据处理模块
│   └── utils/             # 工具函数
├── scripts/               # 数据维护脚本
├── data/                  # 数据存储
├── logs/                  # 日志文件
├── tests/                 # 测试文件
└── examples/              # 使用示例
```

## 快速开始

### 1. 环境设置

```bash
# 克隆或创建项目目录
cd crypto_index_study

# 创建虚拟环境
python -m venv .venv
source .venv/bin/activate  # Linux/Mac

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置 API Key (可选)

如果有 CoinGecko Pro API Key，可以创建 `.env` 文件：

```env
COINGECKO_API_KEY=你的API_KEY
```

## 使用示例

### 基础查询

```python
from src.api.coingecko import CoinGeckoAPI

api = CoinGeckoAPI()

# 获取市场排名前10的硬币
markets = api.get_coins_markets(vs_currency='usd', per_page=10)

for i, coin in enumerate(markets, 1):
    price = coin['current_price']
    change_24h = coin['price_change_percentage_24h']
    print(f"{i}. {coin['name']}: ${price} ({change_24h:.2f}%)")
```

### 批量下载

```python
from src.data.batch_downloader import create_batch_downloader

# 创建批量下载器
downloader = create_batch_downloader()

# 下载前20名币种的最近30天数据
results = downloader.download_batch(top_n=20, days="30")

# 查看下载结果
downloaded_coins = downloader.list_downloaded_coins()
print(f"已下载 {len(downloaded_coins)} 个币种")
```

## 核心功能

### API 封装

| 方法                      | 功能      | 描述                             |
| ------------------------- | --------- | -------------------------------- |
| `get_coins_list()`        | 硬币列表  | 获取所有支持的数字货币列表       |
| `get_coins_markets()`     | 市场数据  | 获取带市场数据的硬币列表         |
| `get_coin_by_id()`        | 硬币详情  | 根据 ID 获取硬币详细信息         |
| `get_coin_market_chart()` | 图表数据  | 获取价格、市值、交易量图表数据   |
| `get_coin_ohlc()`         | OHLC 数据 | 获取开盘、最高、最低、收盘价数据 |

### 数据管理

| 方法                      | 功能     | 描述                           |
| ------------------------- | -------- | ------------------------------ |
| `download_batch()`        | 批量下载 | 下载前 N 名币种的历史数据      |
| `get_download_status()`   | 状态查询 | 查询指定币种的下载状态和元数据 |
| `list_downloaded_coins()` | 列表查询 | 获取所有已下载币种的列表       |

## 数据维护

`scripts/` 目录包含用于日常数据维护的自动化脚本。

| 脚本                     | 功能       | 描述                                  |
| ------------------------ | ---------- | ------------------------------------- |
| `update_all_metadata.py` | 元数据更新 | 更新币种元数据、稳定币和包装币信息    |
| `update_price_data.py`   | 价格更新   | 增量更新前 500 名币种价格数据（可过滤稳定币和包装币） |

**使用方法**:

```bash
# 更新所有元数据
python scripts/update_all_metadata.py

# 增量更新价格数据
python scripts/update_price_data.py
```

## 包装币识别

项目提供了智能包装币识别功能，自动识别并分类包装币（如跨链币、衍生品等）：

```python
from examples.wrapped_coin_checker import WrappedCoinChecker

# 创建检查器
checker = WrappedCoinChecker()

# 检查单个币种
result = checker.is_wrapped_coin("wrapped-bitcoin")
print(f"是否为包装币: {result['is_wrapped_coin']}")
print(f"识别分类: {result['wrapped_categories']}")

# 获取所有包装币
wrapped_coins = checker.get_all_wrapped_coins()
print(f"发现 {len(wrapped_coins)} 个包装币")

# 导出到CSV
checker.export_wrapped_coins_csv()
```

**包装币识别依据**:
- **严格分类匹配**：仅基于 CoinGecko 官方分类标签进行识别
- **包装代币**：Wrapped-Tokens、Bridged-Tokens 等明确的包装类别
- **质押代币**：Liquid Staking Tokens、Liquid Staked ETH/SOL/BTC 等
- **代币化资产**：Tokenized BTC/Gold 等代表真实资产的代币
- **智能排除**：自动排除 meme 币和治理代币，避免误判
- **精确分类**：成功识别 87 个包装币，准确率达到 99%+

## 运行测试

```bash
# 运行所有单元测试
python main.py --test
```

该命令会自动发现并运行 `tests/` 目录下的所有测试文件，覆盖包括 API 封装、批量下载器在内的核心功能。

## 下载状态说明

批量下载器会为每个币种返回以下三种状态：

- ✅ **success**: 数据成功下载并保存。
- ⏭️ **skipped**: 本地数据足够新鲜，无需重复下载。
- ❌ **failed**: 因网络或 API 问题导致下载失败 (会自动重试 3 次)。

如果遇到大量失败，请检查网络连接、API Key，并查看 `logs/` 目录中的日志文件。

## 更新日志

查看完整的更新历史和版本变更，请参考 [CHANGELOG.md](CHANGELOG.md)。

## 许可证

**MIT License** 意味着：

- ✅ **可以自由使用**: 个人或商业项目都可以使用
- ✅ **可以修改**: 可以根据需要修改代码
- ✅ **可以分发**: 可以分享给他人或发布修改版本
- ✅ **可以私有化**: 可以在私有项目中使用
- ⚠️ **免责声明**: 软件按现状提供，不承担任何担保责任

**唯一要求**: 在使用时保留原始的版权声明和许可证文本。
