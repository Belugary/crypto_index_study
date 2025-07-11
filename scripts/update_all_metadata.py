"""
批量更新所有币种元数据并生成稳定币列表

该脚本会：
1. 扫描 data/coins/ 目录下的所有 CSV 文件
2. 提取币种 ID
3. 批量调用 API 获取元数据
4. 存储到 data/metadata/coin_metadata/
5. 生成完整的稳定币列表
"""

import os
import sys
import time
from pathlib import Path
from typing import List, Set

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from examples.stablecoin_checker import StablecoinChecker
from src.data.batch_downloader import create_batch_downloader


def get_all_coin_ids_from_data() -> List[str]:
    """
    从 data/coins/ 目录获取所有币种 ID

    Returns:
        币种 ID 列表
    """
    coins_dir = Path("data/coins")
    coin_ids = []

    if not coins_dir.exists():
        print("❌ data/coins/ 目录不存在")
        return []

    # 扫描所有 CSV 文件
    for csv_file in coins_dir.glob("*.csv"):
        coin_id = csv_file.stem  # 去掉 .csv 后缀
        coin_ids.append(coin_id)

    # 按字母顺序排序
    coin_ids.sort()

    print(f"📊 发现 {len(coin_ids)} 个币种文件")
    return coin_ids


def get_existing_metadata_coin_ids() -> Set[str]:
    """
    获取已有元数据的币种 ID

    Returns:
        已有元数据的币种 ID 集合
    """
    metadata_dir = Path("data/metadata/coin_metadata")
    existing_ids = set()

    if metadata_dir.exists():
        for json_file in metadata_dir.glob("*.json"):
            coin_id = json_file.stem
            existing_ids.add(coin_id)

    return existing_ids


def batch_update_all_metadata(
    batch_size: int = 50, delay_seconds: float = 0.5, force_update: bool = False
) -> None:
    """
    批量更新所有币种的元数据

    Args:
        batch_size: 每批处理的币种数量
        delay_seconds: 每次API调用的延迟
        force_update: 是否强制更新
    """
    print("🚀 开始批量更新币种元数据")
    print("=" * 60)

    # 1. 获取所有币种 ID
    all_coin_ids = get_all_coin_ids_from_data()
    if not all_coin_ids:
        print("❌ 未找到任何币种数据")
        return

    # 2. 检查已有元数据
    existing_ids = get_existing_metadata_coin_ids()
    print(f"📋 当前已有元数据: {len(existing_ids)} 个币种")

    # 3. 筛选需要更新的币种
    if force_update:
        coins_to_update = all_coin_ids
        print(f"🔄 强制更新模式: 将更新所有 {len(coins_to_update)} 个币种")
    else:
        coins_to_update = [
            coin_id for coin_id in all_coin_ids if coin_id not in existing_ids
        ]
        print(f"🆕 增量更新模式: 需要更新 {len(coins_to_update)} 个新币种")

    if not coins_to_update:
        print("✅ 所有币种元数据都是最新的")
        return

    # 4. 创建下载器
    downloader = create_batch_downloader()

    # 5. 分批处理
    total_batches = (len(coins_to_update) + batch_size - 1) // batch_size
    success_count = 0

    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, len(coins_to_update))
        batch_coins = coins_to_update[start_idx:end_idx]

        print(
            f"\n📦 处理第 {batch_idx + 1}/{total_batches} 批 ({len(batch_coins)} 个币种)"
        )
        print(
            f"   币种: {', '.join(batch_coins[:5])}{'...' if len(batch_coins) > 5 else ''}"
        )

        # 批量更新这一批币种
        results = downloader.batch_update_coin_metadata(
            coin_ids=batch_coins, force=force_update, delay_seconds=delay_seconds
        )

        # 统计结果
        batch_success = sum(1 for success in results.values() if success)
        success_count += batch_success

        print(f"   结果: {batch_success}/{len(batch_coins)} 成功")

        # 批次间延迟
        if batch_idx < total_batches - 1:
            print(f"   等待 {delay_seconds * 2:.1f} 秒后继续...")
            time.sleep(delay_seconds * 2)

    print(f"\n🎉 批量更新完成!")
    print(f"   总计: {success_count}/{len(coins_to_update)} 成功")
    print(f"   失败: {len(coins_to_update) - success_count} 个")


def generate_complete_stablecoin_list() -> None:
    """
    生成完整的稳定币列表
    """
    print(f"\n💰 生成稳定币列表")
    print("=" * 40)

    checker = StablecoinChecker()

    # 获取所有稳定币
    stablecoins = checker.get_all_stablecoins()

    if not stablecoins:
        print("❌ 未找到任何稳定币")
        return

    print(f"✅ 发现 {len(stablecoins)} 个稳定币:")

    # 按市值排名或名称排序显示
    for i, coin in enumerate(stablecoins, 1):
        symbol = coin["symbol"].upper()
        name = coin["name"]
        categories = coin["stablecoin_categories"]
        print(f"  {i:2d}. {name} ({symbol})")
        print(f"      分类: {', '.join(categories)}")

    # 导出到 CSV
    success = checker.export_stablecoins_csv()
    if success:
        print(f"\n💾 稳定币列表已导出到: data/metadata/stablecoins.csv")

    # 额外分析
    print(f"\n📊 稳定币分析:")

    # 按分类统计
    category_counts = {}
    for coin in stablecoins:
        for category in coin["stablecoin_categories"]:
            category_counts[category] = category_counts.get(category, 0) + 1

    print("   主要分类:")
    for category, count in sorted(
        category_counts.items(), key=lambda x: x[1], reverse=True
    ):
        print(f"     - {category}: {count} 个")


def main():
    """主函数"""
    print("🔍 批量币种元数据更新与稳定币分析")
    print("=" * 70)

    # 检查参数
    force_update = "--force" in sys.argv
    fast_mode = "--fast" in sys.argv

    if force_update:
        print("⚠️  强制更新模式: 将重新获取所有币种的元数据")

    if fast_mode:
        print("⚡ 快速模式: 减少延迟时间")
        delay_seconds = 0.2
        batch_size = 100
    else:
        print("🐌 标准模式: 使用安全的延迟时间")
        delay_seconds = 0.5
        batch_size = 50

    try:
        # 1. 批量更新元数据
        batch_update_all_metadata(
            batch_size=batch_size,
            delay_seconds=delay_seconds,
            force_update=force_update,
        )

        # 2. 生成稳定币列表
        generate_complete_stablecoin_list()

        print(f"\n{'='*70}")
        print("✅ 所有任务完成!")
        print("\n📁 生成的文件:")
        print("   - data/metadata/coin_metadata/*.json  (单个币种元数据)")
        print("   - data/metadata/stablecoins.csv       (稳定币汇总列表)")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 执行过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    print("使用方法:")
    print("  python scripts/update_all_metadata.py          # 标准模式")
    print("  python scripts/update_all_metadata.py --fast   # 快速模式")
    print("  python scripts/update_all_metadata.py --force  # 强制更新所有")
    print("")

    main()
