"""
分类器使用示例

演示如何使用新的币种分类器进行稳定币和包装币识别
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.classification import StablecoinChecker, WrappedCoinChecker


def example_stablecoin_classification():
    """稳定币分类示例"""
    print("🔍 稳定币分类示例")
    print("=" * 50)

    # 创建稳定币检查器
    stablecoin_checker = StablecoinChecker()

    # 测试几个币种
    test_coins = ["bitcoin", "ethereum", "tether", "usd-coin", "dai", "frax"]

    print("📊 检查特定币种:")
    for coin_id in test_coins:
        result = stablecoin_checker.is_stablecoin(coin_id)
        if result["confidence"] != "unknown":
            name = result.get("name", coin_id)
            symbol = result.get("symbol", "").upper()
            status = "✅ 稳定币" if result["is_stablecoin"] else "❌ 非稳定币"
            print(f"  {name} ({symbol}): {status}")
            if result["stablecoin_categories"]:
                print(f"    官方分类: {', '.join(result['stablecoin_categories'])}")
        else:
            print(f"  {coin_id}: ❓ 无元数据")

    # 获取稳定币统计
    all_stablecoins = stablecoin_checker.get_all_stablecoins()
    print(f"\n📈 稳定币统计:")
    print(f"  总数: {len(all_stablecoins)} 个稳定币")

    if all_stablecoins:
        print(f"  前5个稳定币:")
        for coin in all_stablecoins[:5]:
            print(f"    • {coin['name']} ({coin['symbol'].upper()})")


def example_wrapped_coin_classification():
    """包装币分类示例"""
    print("\n🔍 包装币分类示例")
    print("=" * 50)

    # 创建包装币检查器
    wrapped_checker = WrappedCoinChecker()

    # 测试几个币种（包括明显的包装币和非包装币）
    test_coins = [
        "bitcoin",
        "ethereum",
        "wrapped-bitcoin",
        "weth",
        "staked-ether",
        "binance-coin",
        "cardano",
    ]

    print("📊 检查特定币种:")
    for coin_id in test_coins:
        result = wrapped_checker.is_wrapped_coin(coin_id)
        if result["confidence"] != "unknown":
            name = result.get("name", coin_id)
            symbol = result.get("symbol", "").upper()
            status = "✅ 包装币" if result["is_wrapped_coin"] else "❌ 非包装币"
            print(f"  {name} ({symbol}): {status}")
            if result["wrapped_categories"]:
                print(f"    官方分类: {', '.join(result['wrapped_categories'])}")
        else:
            print(f"  {coin_id}: ❓ 无元数据")

    # 获取包装币统计
    all_wrapped = wrapped_checker.get_all_wrapped_coins()
    print(f"\n📈 包装币统计:")
    print(f"  总数: {len(all_wrapped)} 个包装币")

    if all_wrapped:
        print(f"  前5个包装币:")
        for coin in all_wrapped[:5]:
            print(f"    • {coin['name']} ({coin['symbol'].upper()})")


def example_combined_classification():
    """组合分类示例 - 识别原生币"""
    print("\n🔍 原生币识别示例（排除稳定币和包装币）")
    print("=" * 50)

    # 创建两个检查器
    stablecoin_checker = StablecoinChecker()
    wrapped_checker = WrappedCoinChecker()

    # 测试一批主流币种
    test_coins = [
        "bitcoin",
        "ethereum",
        "binancecoin",
        "cardano",
        "solana",
        "tether",
        "usd-coin",
        "wrapped-bitcoin",
        "weth",
        "dai",
    ]

    native_coins = []
    stable_coins = []
    wrapped_coins = []
    unknown_coins = []

    print("📊 分类结果:")
    for coin_id in test_coins:
        # 检查稳定币
        stable_result = stablecoin_checker.is_stablecoin(coin_id)
        # 检查包装币
        wrapped_result = wrapped_checker.is_wrapped_coin(coin_id)

        if (
            stable_result["confidence"] == "unknown"
            or wrapped_result["confidence"] == "unknown"
        ):
            unknown_coins.append(coin_id)
            print(f"  {coin_id}: ❓ 无元数据")
        elif stable_result["is_stablecoin"]:
            stable_coins.append(stable_result)
            print(f"  {stable_result['name']}: 🟡 稳定币")
        elif wrapped_result["is_wrapped_coin"]:
            wrapped_coins.append(wrapped_result)
            print(f"  {wrapped_result['name']}: 🟠 包装币")
        else:
            native_coins.append(
                {
                    "coin_id": coin_id,
                    "name": stable_result["name"],
                    "symbol": stable_result["symbol"],
                }
            )
            print(f"  {stable_result['name']}: ✅ 原生币")

    print(f"\n📊 分类统计:")
    print(f"  原生币: {len(native_coins)} 个")
    print(f"  稳定币: {len(stable_coins)} 个")
    print(f"  包装币: {len(wrapped_coins)} 个")
    print(f"  未知类型: {len(unknown_coins)} 个")


def main():
    """主函数：运行所有分类示例"""
    print("🚀 币种分类器使用示例")
    print("=" * 60)

    try:
        # 稳定币分类示例
        example_stablecoin_classification()

        # 包装币分类示例
        example_wrapped_coin_classification()

        # 组合分类示例
        example_combined_classification()

        print(f"\n{'='*60}")
        print("✅ 所有分类示例运行完成！")
        print("\n💡 提示:")
        print("  - 稳定币检查器基于 CoinGecko 'Stablecoins' 官方分类")
        print("  - 包装币检查器基于 CoinGecko 'Wrapped-Tokens' 官方分类")
        print("  - 原生币 = 非稳定币且非包装币的币种")
        print("  - 分类器现在位于 src/classification/ 模块中")

    except Exception as e:
        print(f"\n❌ 示例运行出错: {e}")
        print("请检查项目依赖和数据目录是否正确配置。")


if __name__ == "__main__":
    main()
