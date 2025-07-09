# CoinGecko 数字货币数据分析项目

这是一个专业的数字货币数据分析项目，使用 CoinGecko Pro API 提供完整的数据查询和分析功能。项目采用模块化设计，便于扩展和维护。

## 🎯 项目特性

- **🔧 模块化设计**: 清晰的代码结构，便于添加新的 API 和功能
- **📊 完整的 API 封装**: 支持所有 CoinGecko 基础 API 功能
- **🛠️ 工具函数**: 提供常用的数据处理和格式化工具
- **📖 丰富的示例**: 包含多种使用场景的代码示例
- **🧪 完整测试**: 全面的 API 功能测试
- **📝 中文文档**: 详细的中文注释和文档

## 📁 项目结构

```
crypto_index_study/
├── main.py                   # 主入口文件
├── src/                      # 源代码目录
│   ├── __init__.py          # 包初始化文件
│   ├── api/                 # API模块
│   │   ├── __init__.py     # API包初始化
│   │   └── coingecko.py    # CoinGecko API封装
│   └── utils.py            # 工具函数
├── tests/                   # 测试目录
│   └── test_coingecko_api.py # API测试文件
├── examples/                # 示例代码
│   └── basic_usage.py      # 基础使用示例
├── test_api.py             # API功能测试脚本
├── .env                    # 环境变量配置 (不包含在git中)
├── .gitignore              # Git忽略文件
├── requirements.txt        # 项目依赖
└── README.md              # 项目说明
```

## 🚀 快速开始

### 1. 环境设置

```bash
# 克隆项目或创建项目目录
cd crypto_index_study

# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt
```

### 2. 配置 API Key

创建 `.env` 文件，添加你的 CoinGecko Pro API Key：

```env
COINGECKO_API_KEY=你的API_KEY
```

### 3. 快速使用

**方法一：使用主入口文件**

```bash
# 显示基础信息（前5大市值硬币）
python main.py

# 运行API测试
python main.py --test

# 运行所有示例
python main.py --examples
```

**方法二：直接使用 API**

```python
from src.api.coingecko import CoinGeckoAPI

# 创建 API 客户端
api = CoinGeckoAPI()

# 获取市场数据
markets = api.get_coins_markets(vs_currency='usd', per_page=10)
print(f"前10大市值硬币: {[coin['name'] for coin in markets]}")

# 获取Bitcoin详细信息
bitcoin = api.get_coin_by_id('bitcoin')
price = bitcoin['market_data']['current_price']['usd']
print(f"Bitcoin当前价格: ${price:,.2f}")
```

## 📚 API 功能

### 🔹 基础 API 功能

| 方法                            | 功能         | 描述                             |
| ------------------------------- | ------------ | -------------------------------- |
| `get_coins_list()`              | 硬币列表     | 获取所有支持的数字货币列表       |
| `get_coins_markets()`           | 市场数据     | 获取带市场数据的硬币列表         |
| `get_coin_by_id()`              | 硬币详情     | 根据 ID 获取硬币详细信息         |
| `get_coin_tickers()`            | 交易行情     | 获取硬币的交易所行情数据         |
| `get_coin_history()`            | 历史数据     | 获取特定日期的历史数据           |
| `get_coin_market_chart()`       | 图表数据     | 获取价格、市值、交易量图表数据   |
| `get_coin_market_chart_range()` | 时间范围图表 | 获取指定时间范围的图表数据       |
| `get_coin_ohlc()`               | OHLC 数据    | 获取开盘、最高、最低、收盘价数据 |

## 🛠️ 工具函数

项目提供了丰富的工具函数，位于 `src/utils.py`：

- `print_json()`: 格式化打印 JSON 数据
- `format_currency()`: 货币格式化显示
- `calculate_percentage_change()`: 计算百分比变化
- `get_timestamp()`: 获取 Unix 时间戳
- `safe_get()`: 安全获取嵌套字典值

## 📖 使用示例

### 基础数据查询

```python
from src.api.coingecko import CoinGeckoAPI
from src.utils import format_currency

api = CoinGeckoAPI()

# 获取市场排名前10的硬币
markets = api.get_coins_markets(vs_currency='usd', per_page=10)

for i, coin in enumerate(markets, 1):
    price = coin['current_price']
    change_24h = coin['price_change_percentage_24h']
    print(f"{i}. {coin['name']}: {format_currency(price)} ({change_24h:.2f}%)")
```

### 历史数据分析

```python
import time

# 获取Bitcoin过去7天的价格数据
chart_data = api.get_coin_market_chart('bitcoin', 'usd', 7)
prices = chart_data['prices']

# 计算涨跌幅
if len(prices) >= 2:
    start_price = prices[0][1]
    end_price = prices[-1][1]
    change_pct = ((end_price - start_price) / start_price) * 100
    print(f"Bitcoin 7天涨跌幅: {change_pct:.2f}%")
```

### 时间范围数据

```python
# 获取特定时间范围的数据
current_time = int(time.time())
seven_days_ago = current_time - (7 * 24 * 60 * 60)

range_data = api.get_coin_market_chart_range(
    'bitcoin',
    seven_days_ago,
    current_time
)
```

## 🧪 运行测试

```bash
# 方法一：使用主入口文件
python main.py --test

# 方法二：直接运行测试脚本
python test_api.py

# 方法三：运行单元测试
python tests/test_coingecko_api.py

# 运行使用示例
python main.py --examples
# 或者
python examples/basic_usage.py
```

## 📦 依赖包

- `requests`: HTTP 请求库
- `python-dotenv`: 环境变量管理
- `typing`: 类型提示支持

## 🔄 扩展指南

### 添加新的 API 类

1. 在 `src/api/` 目录下创建新的 API 模块
2. 在 `src/api/__init__.py` 中导入新类
3. 在 `src/__init__.py` 中添加到 `__all__` 列表

```python
# src/api/new_api.py
class NewAPI:
    def __init__(self):
        pass

    def some_method(self):
        pass

# src/api/__init__.py
from .coingecko import CoinGeckoAPI
from .new_api import NewAPI

__all__ = ["CoinGeckoAPI", "NewAPI"]
```

### 添加新的工具函数

在 `src/utils.py` 中添加新的工具函数，或创建新的工具模块。

### 添加数据模型

可以创建 `src/models/` 目录来定义数据模型类：

```python
# src/models/coin.py
from dataclasses import dataclass
from typing import Optional

@dataclass
class Coin:
    id: str
    name: str
    symbol: str
    current_price: Optional[float] = None
    market_cap: Optional[float] = None
```

## ⚠️ 注意事项

1. **API Key**: 需要有效的 CoinGecko Pro API Key
2. **速率限制**: 请遵守 CoinGecko 的 API 速率限制
3. **错误处理**: 项目已包含完整的错误处理机制
4. **数据精度**: API 支持自定义数据精度参数

## 📄 许可证

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request 来改进项目！
