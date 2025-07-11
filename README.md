# 区块链资产指数研究项目

本项目帮助你快速获取、管理和分析主流区块链资产的历史量价数据，适用于量化分析、投资研究和学术用途。

## 你能获得什么？

- 市值前 600 名加密货币的日级别历史数据（价格、交易量、市值等，最近更新: 2025-07-11）
- 支持自定义原生币目标数量（默认 510 个），系统自动扩展搜索范围，确保目标达成
- 可选自动过滤稳定币和包装币，专注原生资产
- 一键批量下载、自动增量更新，数据始终保持最新
- 完整 API 封装，支持币种列表、市场数据、历史行情等常用查询
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

# 下载原生币价格数据（默认 510 个）
python scripts/update_price_data.py

# 自定义原生币数量（如 700 个）
python scripts/update_price_data.py --native-coins 700
```

4. 使用 API 查询

```python
from src.api.coingecko import CoinGeckoAPI
api = CoinGeckoAPI()
markets = api.get_coins_markets(vs_currency='usd', per_page=10)
for i, coin in enumerate(markets, 1):
    print(f"{i}. {coin['name']}: ${coin['current_price']} ({coin['price_change_percentage_24h']:.2f}%)")
```

5. 批量下载示例

```python
from src.data.batch_downloader import create_batch_downloader
downloader = create_batch_downloader()
results = downloader.download_batch(top_n=20, days="30")
print(f"已下载 {len(downloader.list_downloaded_coins())} 个币种")
```

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
src/         # 核心功能代码
scripts/     # 自动化脚本
examples/    # 使用示例
tests/       # 测试代码
data/        # 数据资产 (coins/, metadata/)
logs/        # 日志文件
```

## 许可证

MIT License：可自由使用、修改、分发和私有化。软件按现状提供，不承担任何担保责任。使用时请保留原始版权声明。

## 免责声明

本项目仅供学习和研究，不构成投资建议。投资有风险，盈亏自负。
