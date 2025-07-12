#!/usr/bin/env python3
"""
更新所有现有币种脚本

该脚本更新 data/coins 目录中所有现有的币种数据文件。
使用智能跳过机制，只更新今天尚未更新的文件。

使用方式:
    python scripts/update_all_existing_coins.py              # 更新所有现有币种
    python scripts/update_all_existing_coins.py --batch-size 50  # 设置批处理大小
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from datetime import date
from typing import List, Tuple

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/update_all_existing_coins.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


def get_existing_coin_files(coins_dir: Path) -> List[str]:
    """获取所有现有的币种文件列表"""
    coin_files = list(coins_dir.glob("*.csv"))
    coin_ids = [f.stem for f in coin_files]
    return sorted(coin_ids)


def filter_coins_needing_update(
    coin_ids: List[str], coins_dir: Path
) -> Tuple[List[str], List[str]]:
    """筛选需要更新的币种和已经是今日更新的币种（基于数据质量检查）"""
    import pandas as pd

    today = date.today()
    needs_update = []
    already_updated = []

    for coin_id in coin_ids:
        csv_file = coins_dir / f"{coin_id}.csv"

        if not csv_file.exists():
            needs_update.append(coin_id)
            continue

        try:
            # 读取数据进行质量检查
            df = pd.read_csv(csv_file)

            # 检查1: 数据行数是否足够（至少500行表示有充足历史数据）
            if len(df) < 500:
                logger.warning(f"⚠️  {coin_id}: 数据行数不足({len(df)}行)，需要重新下载")
                needs_update.append(coin_id)
                continue

            # 检查2: 是否有timestamp列
            if "timestamp" not in df.columns:
                logger.warning(f"⚠️  {coin_id}: 缺少timestamp列，需要重新下载")
                needs_update.append(coin_id)
                continue

            # 检查3: 最新数据是否是今日的
            df["timestamp"] = pd.to_datetime(
                df["timestamp"], unit="ms", errors="coerce"
            ).dt.date
            latest_date = df["timestamp"].max()

            if pd.isna(latest_date):
                logger.warning(f"⚠️  {coin_id}: 日期数据无效，需要重新下载")
                needs_update.append(coin_id)
            elif latest_date < today:
                logger.info(f"📅 {coin_id}: 最新数据 {latest_date}，需要更新到今日")
                needs_update.append(coin_id)
            else:
                logger.debug(
                    f"✅ {coin_id}: 数据质量良好，{len(df)}行，最新:{latest_date}"
                )
                already_updated.append(coin_id)

        except Exception as e:
            # 文件损坏或无法读取，需要重新下载
            logger.warning(f"⚠️  {coin_id}: 文件读取失败，需要重新下载 - {e}")
            needs_update.append(coin_id)

    return needs_update, already_updated


def update_coin_batch(
    updater: PriceDataUpdater, coin_ids: List[str]
) -> Tuple[int, int, int]:
    """更新一批币种"""
    success_count = 0
    skip_count = 0
    fail_count = 0

    for coin_id in coin_ids:
        try:
            logger.info(f"处理币种: {coin_id}")
            success, api_called = updater.download_coin_data(coin_id)

            if success:
                if api_called:
                    success_count += 1
                    logger.info(f"✅ {coin_id} 更新成功")
                else:
                    skip_count += 1
                    logger.info(f"⏭️ {coin_id} 跳过（今日已更新）")
            else:
                fail_count += 1
                logger.error(f"❌ {coin_id} 更新失败")

        except Exception as e:
            fail_count += 1
            logger.error(f"❌ {coin_id} 处理异常: {e}")

    return success_count, skip_count, fail_count


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="更新所有现有币种数据")
    parser.add_argument(
        "--batch-size", type=int, default=100, help="批处理大小 (默认: 100)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="仅显示需要更新的币种，不执行实际更新"
    )

    args = parser.parse_args()

    print("🔄 更新所有现有币种工具")
    print("=" * 50)

    # 设置路径
    coins_dir = Path("data/coins")
    if not coins_dir.exists():
        print("❌ data/coins 目录不存在")
        sys.exit(1)

    try:
        # 获取所有现有币种
        print("📊 扫描现有币种文件...")
        all_coin_ids = get_existing_coin_files(coins_dir)
        print(f"发现 {len(all_coin_ids)} 个现有币种")

        # 筛选需要更新的币种
        needs_update, already_updated = filter_coins_needing_update(
            all_coin_ids, coins_dir
        )

        print(f"\n📈 统计信息:")
        print(f"   - 总币种数: {len(all_coin_ids)}")
        print(f"   - 今日已更新: {len(already_updated)}")
        print(f"   - 需要更新: {len(needs_update)}")

        if args.dry_run:
            print(f"\n🔍 需要更新的币种 ({len(needs_update)} 个):")
            for i, coin_id in enumerate(needs_update, 1):
                print(f"   {i:3d}. {coin_id}")
            return

        if not needs_update:
            print("\n✅ 所有币种都是今日最新数据，无需更新！")
            return

        print(f"\n🚀 开始更新 {len(needs_update)} 个币种...")
        print(f"批处理大小: {args.batch_size}")

        # 创建更新器
        updater = PriceDataUpdater()

        # 分批处理
        total_success = 0
        total_skip = 0
        total_fail = 0

        for i in range(0, len(needs_update), args.batch_size):
            batch = needs_update[i : i + args.batch_size]
            batch_num = i // args.batch_size + 1
            total_batches = (len(needs_update) + args.batch_size - 1) // args.batch_size

            print(f"\n📦 处理批次 {batch_num}/{total_batches} ({len(batch)} 个币种)")

            success, skip, fail = update_coin_batch(updater, batch)
            total_success += success
            total_skip += skip
            total_fail += fail

            print(f"批次结果: 成功={success}, 跳过={skip}, 失败={fail}")

        # 最终统计
        print(f"\n🎯 更新完成！")
        print(f"📊 最终统计:")
        print(f"   - 成功更新: {total_success}")
        print(f"   - 智能跳过: {total_skip}")
        print(f"   - 更新失败: {total_fail}")
        print(f"   - 处理总数: {total_success + total_skip + total_fail}")

        if total_fail > 0:
            print(f"\n⚠️  有 {total_fail} 个币种更新失败，请检查日志")
        else:
            print(f"\n✅ 所有币种处理完成，无失败！")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 脚本执行失败: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
