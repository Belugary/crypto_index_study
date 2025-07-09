"""
CoinGecko API 测试模块

测试基础API功能、Premium API功能和Analyst API功能
"""

import sys
import os

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.coingecko import CoinGeckoAPI
from src.utils import print_json


def test_basic_api(api: CoinGeckoAPI) -> bool:
    """测试基础 API 功能"""
    print("\n🔹 测试基础 API 功能")

    try:
        # 1. 获取硬币列表
        print("\n1. 获取硬币列表 (前10个)")
        coins_list = api.get_coins_list()
        print_json(coins_list, "硬币列表", 10)

        # 2. 获取市场数据
        print("\n2. 获取前20个硬币的市场数据")
        markets = api.get_coins_markets(vs_currency="usd", per_page=20, page=1)
        print_json(markets, "市场数据", 5)

        # 3. 获取Bitcoin详细数据
        print("\n3. 获取Bitcoin详细数据")
        bitcoin = api.get_coin_by_id("bitcoin")
        # 简化显示，只显示核心信息
        bitcoin_summary = {
            "id": bitcoin.get("id"),
            "name": bitcoin.get("name"),
            "symbol": bitcoin.get("symbol"),
            "market_cap_rank": bitcoin.get("market_cap_rank"),
            "market_data": {
                "current_price": bitcoin.get("market_data", {})
                .get("current_price", {})
                .get("usd"),
                "market_cap": bitcoin.get("market_data", {})
                .get("market_cap", {})
                .get("usd"),
                "total_volume": bitcoin.get("market_data", {})
                .get("total_volume", {})
                .get("usd"),
            },
        }
        print_json(bitcoin_summary, "Bitcoin详细数据")

        # 4. 获取Bitcoin交易行情
        print("\n4. 获取Bitcoin交易行情")
        tickers = api.get_coin_tickers("bitcoin")
        # 简化显示，只显示前3个ticker
        tickers_summary = {
            "name": tickers.get("name"),
            "ticker_count": len(tickers.get("tickers", [])),
            "top_3_tickers": tickers.get("tickers", [])[:3],
        }
        print_json(tickers_summary, "Bitcoin交易行情")

        # 5. 获取历史数据
        print("\n5. 获取Bitcoin历史数据 (2024-01-01)")
        history = api.get_coin_history("bitcoin", "01-01-2024")
        # 简化显示
        history_summary = {
            "id": history.get("id"),
            "date": "01-01-2024",
            "market_data": history.get("market_data", {}),
        }
        print_json(history_summary, "Bitcoin历史数据")

        # 6. 获取图表数据
        print("\n6. 获取Bitcoin价格图表数据 (7天)")
        chart_data = api.get_coin_market_chart("bitcoin", "usd", 7)
        chart_summary = {
            "price_points": len(chart_data.get("prices", [])),
            "market_cap_points": len(chart_data.get("market_caps", [])),
            "volume_points": len(chart_data.get("total_volumes", [])),
            "first_price": chart_data.get("prices", [None])[0],
            "last_price": chart_data.get("prices", [None])[-1],
        }
        print_json(chart_summary, "Bitcoin图表数据摘要")

        # 7. 获取OHLC数据
        print("\n7. 获取Bitcoin OHLC数据 (7天)")
        ohlc_data = api.get_coin_ohlc("bitcoin", "usd", 7)
        ohlc_summary = {
            "data_type": type(ohlc_data).__name__,
            "data_length": len(ohlc_data) if isinstance(ohlc_data, list) else "N/A",
            "sample": (
                str(ohlc_data)[:200] + "..." if len(str(ohlc_data)) > 200 else ohlc_data
            ),
        }
        print_json(ohlc_summary, "Bitcoin OHLC数据")

        return True

    except Exception as e:
        print(f"❌ 基础 API 测试失败: {e}")
        return False


def main():
    """主测试函数"""
    print("🚀 开始测试 CoinGecko API")
    print("API Key:", "已配置" if CoinGeckoAPI().api_key else "未配置")

    # 创建 API 客户端
    api = CoinGeckoAPI()

    # 测试结果记录
    results = {"basic": False}

    # 测试基础 API
    results["basic"] = test_basic_api(api)

    # 输出测试总结
    print(f"\n{'='*60}")
    print("📊 测试结果总结")
    print(f"{'='*60}")
    print(f"🔹 基础 API:    {'✅ 成功' if results['basic'] else '❌ 失败'}")

    success_count = sum(results.values())
    print(f"\n总计: {success_count}/1 个 API 测试成功")

    if success_count == 1:
        print("🎉 所有 API 测试通过！CoinGecko基础API封装完成。")
    else:
        print("⚠️  API 测试失败，请检查 API Key 或网络连接。")


if __name__ == "__main__":
    main()
