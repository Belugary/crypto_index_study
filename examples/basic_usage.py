"""
CoinGecko API 使用示例

演示如何使用 CoinGeckoAPI 类进行各种数据查询
"""

import os
import sys
import time

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.coingecko import CoinGeckoAPI
from src.utils import calculate_percentage_change, format_currency, print_json


def example_basic_usage():
    """基础使用示例"""
    print("🔹 基础使用示例")

    # 创建API客户端
    api = CoinGeckoAPI()

    # 获取市场数据
    print("\n📊 获取前10大市值硬币:")
    markets = api.get_coins_markets(vs_currency="usd", per_page=10, page=1)

    for i, coin in enumerate(markets[:5], 1):
        price = coin.get("current_price", 0)
        change_24h = coin.get("price_change_percentage_24h", 0)
        market_cap = coin.get("market_cap", 0)

        print(f"{i}. {coin['name']} ({coin['symbol'].upper()})")
        print(f"   价格: {format_currency(price)}")
        print(f"   24h变化: {change_24h:.2f}%")
        print(f"   市值: {format_currency(market_cap)}")
        print()


def example_historical_analysis():
    """历史数据分析示例"""
    print("\n📈 历史数据分析示例")

    api = CoinGeckoAPI()

    # 获取Bitcoin的7天价格数据
    print("获取Bitcoin过去7天的价格走势...")
    chart_data = api.get_coin_market_chart("bitcoin", "usd", 7)

    prices = chart_data.get("prices", [])
    if len(prices) >= 2:
        first_price = prices[0][1]
        last_price = prices[-1][1]
        price_change = calculate_percentage_change(first_price, last_price)

        print(f"7天前价格: {format_currency(first_price)}")
        print(f"当前价格: {format_currency(last_price)}")
        print(f"7天涨跌幅: {price_change:.2f}%")

        # 找出最高价和最低价
        max_price = max(prices, key=lambda x: x[1])
        min_price = min(prices, key=lambda x: x[1])

        print(f"7天最高价: {format_currency(max_price[1])}")
        print(f"7天最低价: {format_currency(min_price[1])}")


def example_coin_comparison():
    """硬币对比示例"""
    print("\n⚖️ 硬币对比示例")

    api = CoinGeckoAPI()

    # 对比Bitcoin和Ethereum
    coins = ["bitcoin", "ethereum"]
    comparison_data = []

    for coin_id in coins:
        coin_data = api.get_coin_by_id(coin_id)
        market_data = coin_data.get("market_data", {})

        comparison_data.append(
            {
                "name": coin_data.get("name"),
                "symbol": (coin_data.get("symbol") or "").upper(),
                "price": market_data.get("current_price", {}).get("usd", 0),
                "market_cap": market_data.get("market_cap", {}).get("usd", 0),
                "volume_24h": market_data.get("total_volume", {}).get("usd", 0),
                "change_24h": market_data.get("price_change_percentage_24h", 0),
            }
        )

    print("Bitcoin vs Ethereum 对比:")
    print("-" * 50)
    for coin in comparison_data:
        print(f"{coin['name']} ({coin['symbol']}):")
        print(f"  价格: {format_currency(coin['price'])}")
        print(f"  市值: {format_currency(coin['market_cap'])}")
        print(f"  24h交易量: {format_currency(coin['volume_24h'])}")
        print(f"  24h变化: {coin['change_24h']:.2f}%")
        print()


def example_time_range_analysis():
    """时间范围分析示例"""
    print("\n⏰ 时间范围分析示例")

    api = CoinGeckoAPI()

    # 使用时间戳获取特定时间范围的数据
    current_time = int(time.time())
    seven_days_ago = current_time - (7 * 24 * 60 * 60)

    print("获取Bitcoin过去7天的详细图表数据...")
    range_data = api.get_coin_market_chart_range(
        "bitcoin", seven_days_ago, current_time, vs_currency="usd"
    )

    prices = range_data.get("prices", [])
    volumes = range_data.get("total_volumes", [])

    if prices and volumes:
        avg_price = sum(price[1] for price in prices) / len(prices)
        avg_volume = sum(volume[1] for volume in volumes) / len(volumes)

        print(f"数据点数量: {len(prices)}")
        print(f"平均价格: {format_currency(avg_price)}")
        print(f"平均交易量: {format_currency(avg_volume)}")


def main():
    """主函数"""
    print("🚀 CoinGecko API 使用示例")
    print("=" * 50)

    try:
        # 基础使用示例
        example_basic_usage()

        # 历史数据分析
        example_historical_analysis()

        # 硬币对比
        example_coin_comparison()

        # 时间范围分析
        example_time_range_analysis()

        print("\n✅ 所有示例运行完成！")

    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        print("请检查网络连接和API配置。")


if __name__ == "__main__":
    main()
