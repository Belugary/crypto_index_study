# CoinGecko 数字货币数据分析项目

一个用于查询和分析数字货币数据的 Python 项目，基于 CoinGecko Pro API 构建。提供完整的数据查询、批量下载和基础分析功能。

## 项目特性

- **完整的 API 封装**: 支持 CoinGecko 主要 API 功能
- **批量数据下载**: 自动下载和管理历史数据
- **数据存储管理**: CSV 格式存储和元数据管理
- **核心数据资产**: 包含市值前 300 名加密货币的完整历史量价数据 (最近更新: 2025-07-10)
- **中文文档和注释**: 详细的使用说明和代码注释

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

### 2. 配置 API Key

创建 `.env` 文件，添加你的 CoinGecko Pro API Key：

```env
COINGECKO_API_KEY=你的API_KEY
```

### 3. 基本使用

```bash
# 显示基础信息
python main.py

# 运行API测试
python main.py --test
```

## API 功能

### 基础 API 功能

| 方法                            | 功能         | 描述                             |
| ------------------------------- | ------------ | -------------------------------- |
| `get_coins_list()`              | 硬币列表     | 获取所有支持的数字货币列表       |
| `get_coins_markets()`           | 市场数据     | 获取带市场数据的硬币列表         |
| `get_coin_by_id()`              | 硬币详情     | 根据 ID 获取硬币详细信息         |
| `get_coin_market_chart()`       | 图表数据     | 获取价格、市值、交易量图表数据   |
| `get_coin_ohlc()`               | OHLC 数据    | 获取开盘、最高、最低、收盘价数据 |

### 批量数据管理

| 方法                      | 功能       | 描述                              |
| ------------------------- | ---------- | --------------------------------- |
| `download_batch()`        | 批量下载   | 下载前 N 名币种的历史数据         |
| `get_download_status()`   | 状态查询   | 查询指定币种的下载状态和元数据    |
| `list_downloaded_coins()` | 列表查询   | 获取所有已下载币种的列表          |

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

## 依赖包

- `requests`: HTTP 请求库
- `python-dotenv`: 环境变量管理  
- `pandas`: 数据处理
- `tqdm`: 进度条显示

## 注意事项

1. 需要有效的 CoinGecko Pro API Key
2. 请遵守 CoinGecko 的 API 速率限制
3. 项目包含完整的错误处理机制

## 许可证

MIT License
