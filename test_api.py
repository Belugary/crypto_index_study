"""
CoinGecko API 测试脚本
测试三种不同类型的 API 功能
"""

from src.api.coingecko import CoinGeckoAPI
import json
from typing import Any, Dict, List


def print_json(data: Any, title: str = "", max_items: int = 5):
    """格式化打印 JSON 数据"""
    print(f"\n{'='*50}")
    print(f"{title}")
    print(f"{'='*50}")

    if isinstance(data, list) and len(data) > max_items:
        print(f"显示前 {max_items} 项 (总共 {len(data)} 项):")
        print(json.dumps(data[:max_items], indent=2, ensure_ascii=False))
        print(f"... 还有 {len(data) - max_items} 项")
    else:
        print(json.dumps(data, indent=2, ensure_ascii=False))


def test_basic_api(api: CoinGeckoAPI):
    """测试基础 API 功能"""
    print("\n🔹 测试基础 API 功能")

    try:
        # 测试获取硬币列表
        print("\n1. 获取硬币列表 (前10个)")
        coins_list = api.get_coins_list()
        print_json(coins_list, "硬币列表", 10)

        # 测试获取市场数据
        print("\n2. 获取前20个硬币的市场数据")
        market_data = api.get_coins_markets(vs_currency="usd", per_page=20)
        print_json(market_data, "市场数据", 5)

        # 测试获取Bitcoin详细数据
        print("\n3. 获取Bitcoin详细数据")
        btc_data = api.get_coin_by_id("bitcoin", sparkline=True)
        # 只显示部分关键字段以节省空间
        btc_summary = {
            "id": btc_data.get("id"),
            "name": btc_data.get("name"),
            "symbol": btc_data.get("symbol"),
            "market_cap_rank": btc_data.get("market_cap_rank"),
            "market_data": {
                "current_price": btc_data.get("market_data", {})
                .get("current_price", {})
                .get("usd"),
                "market_cap": btc_data.get("market_data", {})
                .get("market_cap", {})
                .get("usd"),
                "total_volume": btc_data.get("market_data", {})
                .get("total_volume", {})
                .get("usd"),
            },
        }
        print_json(btc_summary, "Bitcoin详细数据")

        # 测试获取Bitcoin交易行情
        print("\n4. 获取Bitcoin交易行情")
        btc_tickers = api.get_coin_tickers("bitcoin", page=1)
        ticker_summary = {
            "name": btc_tickers.get("name"),
            "ticker_count": len(btc_tickers.get("tickers", [])),
            "top_3_tickers": (
                btc_tickers.get("tickers", [])[:3] if btc_tickers.get("tickers") else []
            ),
        }
        print_json(ticker_summary, "Bitcoin交易行情")

        # 测试获取历史数据
        print("\n5. 获取Bitcoin历史数据 (2024-01-01)")
        btc_history = api.get_coin_history("bitcoin", "01-01-2024")
        history_summary = {
            "id": btc_history.get("id"),
            "date": "01-01-2024",
            "market_data": btc_history.get("market_data", {}).get("current_price", {}),
        }
        print_json(history_summary, "Bitcoin历史数据")

        # 测试获取图表数据
        print("\n6. 获取Bitcoin价格图表数据 (7天)")
        btc_chart = api.get_coin_market_chart("bitcoin", days="7")
        chart_summary = {
            "price_points": len(btc_chart.get("prices", [])),
            "market_cap_points": len(btc_chart.get("market_caps", [])),
            "volume_points": len(btc_chart.get("total_volumes", [])),
            "first_price": (
                btc_chart.get("prices", [[]])[0] if btc_chart.get("prices") else None
            ),
            "last_price": (
                btc_chart.get("prices", [[]])[-1] if btc_chart.get("prices") else None
            ),
        }
        print_json(chart_summary, "Bitcoin图表数据摘要")

        # 测试获取OHLC数据
        print("\n7. 获取Bitcoin OHLC数据 (7天)")
        btc_ohlc = api.get_coin_ohlc("bitcoin", days="7")
        ohlc_summary = {
            "data_type": type(btc_ohlc).__name__,
            "data_length": (
                len(btc_ohlc) if isinstance(btc_ohlc, (list, dict)) else "N/A"
            ),
            "sample": (
                str(btc_ohlc)[:200] + "..." if len(str(btc_ohlc)) > 200 else btc_ohlc
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
