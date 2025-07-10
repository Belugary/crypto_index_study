# 区块链资产指数研究

一个专注于区块链资产指数研究的 Python 项目，以 CoinGecko API 作为主要数据源。提供数据获取、存储管理和基础分析功能，用于区块链资产的量化研究。

## ⚠️ 重要说明

**本项目需要 CoinGecko Pro API Key 才能运行。** 如果没有 API Key，项目已提供了部分历史数据供研究使用。

**免责声明：** 本项目采用 AI 辅助编程开发，可能存在疏漏。使用时请自行承担风险。本项目仅用于学习研究目的，不构成投资建议，投资有风险，盈亏自负。

## 项目特性

- **区块链资产指数研究**: 专注于加密货币市场指数分析
- **完整的数据管理**: 支持数据获取、存储和管理
- **历史数据资产**: 包含市值前 300 名加密货币的完整历史量价数据 (最近更新: 2025-07-10)
- **模块化设计**: 便于扩展和定制化研究
- **中文文档**: 详细的使用说明和代码注释

## 项目结构

```
crypto_index_study/
├── main.py                   # 主入口文件
├── src/                      # 源代码目录
│   ├── api/                 # API模块
│   │   └── coingecko.py    # CoinGecko API封装
│   ├── data/               # 数据管理模块
│   │   └── batch_downloader.py # 批量下载器
│   └── utils.py            # 工具函数
├── tests/                   # 测试文件
├── examples/                # 使用示例
├── data/                    # 数据存储目录
│   ├── coins/              # CSV数据文件
│   ├── metadata/           # 元数据文件
│   └── logs/              # 日志文件
├── requirements.txt        # 项目依赖
└── README.md              # 项目说明
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

**注意：** 没有 API Key 也可以使用项目提供的历史数据进行研究。

### 3. 基本使用

```bash
# 显示基础信息
python main.py

# 运行API测试
python main.py --test
```

## API 功能

### 基础 API 功能

| 方法                      | 功能      | 描述                             |
| ------------------------- | --------- | -------------------------------- |
| `get_coins_list()`        | 硬币列表  | 获取所有支持的数字货币列表       |
| `get_coins_markets()`     | 市场数据  | 获取带市场数据的硬币列表         |
| `get_coin_by_id()`        | 硬币详情  | 根据 ID 获取硬币详细信息         |
| `get_coin_market_chart()` | 图表数据  | 获取价格、市值、交易量图表数据   |
| `get_coin_ohlc()`         | OHLC 数据 | 获取开盘、最高、最低、收盘价数据 |

### 批量数据管理

| 方法                      | 功能     | 描述                           |
| ------------------------- | -------- | ------------------------------ |
| `download_batch()`        | 批量下载 | 下载前 N 名币种的历史数据      |
| `get_download_status()`   | 状态查询 | 查询指定币种的下载状态和元数据 |
| `list_downloaded_coins()` | 列表查询 | 获取所有已下载币种的列表       |

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

## 运行测试

```bash
# 运行API测试
python main.py --test

# 或直接运行测试脚本
python test_api.py
```

## 许可证

MIT License
