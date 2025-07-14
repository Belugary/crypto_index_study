#!/usr/bin/env python3
"""
CoinGecko API 项目主入口文件

使用方式:
    python main.py      def main(): python main.py --test       # 运行API测试

数据更新:
    python scripts/update_price_data.py                     # 智能更新510个原生币
    python scripts/update_price_data.py --native-coins 700  # 智能更新700个原生币
    python scripts/update_all_metadata.py                   # 批量更新元数据
    python scripts/update_all_metadata.py --fast            # 快速模式

指数计算:
    python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --top-n 30  # 计算30币种指数
    python scripts/calculate_index.py --start-date 2025-01-01 --end-date 2025-01-31 --include-stablecoins  # 包含稳定币

核心模块使用:
    from src.updaters import PriceDataUpdater, MetadataUpdater
    from src.index import MarketCapWeightedIndexCalculator
"""

import argparse
import sys

from src.api.coingecko import CoinGeckoAPI


def show_basic_info():
    """显示基础信息"""
    print("🚀 CoinGecko API 数字货币数据分析项目")
    print("=" * 50)

    # 创建API客户端并显示基本信息
    api = CoinGeckoAPI()

    print("📊 获取前5大市值硬币信息:")
    try:
        markets = api.get_coins_markets(vs_currency="usd", per_page=5, page=1)

        for i, coin in enumerate(markets, 1):
            price = coin.get("current_price", 0)
            change_24h = coin.get("price_change_percentage_24h", 0)
            market_cap = coin.get("market_cap", 0)

            print(f"{i}. {coin['name']} ({coin['symbol'].upper()})")
            print(f"   价格: ${price:,.2f}")
            print(f"   24h变化: {change_24h:.2f}%")
            print(f"   市值: ${market_cap:,.0f}")
            print()

    except Exception as e:
        print(f"❌ 获取数据失败: {e}")


def run_tests():
    """运行所有单元测试"""
    print("🧪 运行所有单元测试...")
    import subprocess

    try:
        # 使用 unittest discover 自动发现并运行 tests/ 目录下的所有测试
        result = subprocess.run(
            [sys.executable, "-m", "unittest", "discover", "tests"],
            capture_output=True,
            text=True,
            check=True,  # 如果测试失败，则引发异常
        )
        print(result.stdout)
        if result.stderr:
            print("--- 标准错误输出 ---\n", result.stderr)
    except subprocess.CalledProcessError as e:
        print("❌ 部分测试未通过:")
        print(e.stdout)
        print(e.stderr)
    except Exception as e:
        print(f"❌ 测试运行失败: {e}")


def main():
    """项目主入口函数

    解析命令行参数并根据选项运行对应功能。
    """
    parser = argparse.ArgumentParser(description="CoinGecko API 项目")
    parser.add_argument("--test", action="store_true", help="运行API测试")

    args = parser.parse_args()

    if args.test:
        run_tests()
    else:
        show_basic_info()


if __name__ == "__main__":
    main()
