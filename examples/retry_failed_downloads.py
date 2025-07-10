#!/usr/bin/env python3
"""
重试失败下载的工具脚本

这个脚本专门用于重新下载之前失败的币种数据。
可以从日志文件中自动读取失败的币种列表，或者手动指定。

使用方法:
    # 自动重试最近失败的币种
    python examples/retry_failed_downloads.py

    # 手动指定币种重试
    python examples/retry_failed_downloads.py --coins bitcoin,ethereum,cardano

    # 强制覆盖现有数据重试
    python examples/retry_failed_downloads.py --force-overwrite

功能特性:
- 自动读取失败记录
- 手动指定币种列表
- 可调节重试参数
- 详细的重试进度显示
"""

import sys
import os
import argparse
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.batch_downloader import create_batch_downloader


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="重试失败的币种下载")
    parser.add_argument(
        "--coins",
        type=str,
        help="手动指定币种列表，用逗号分隔 (例: bitcoin,ethereum,cardano)",
    )
    parser.add_argument(
        "--days", type=str, default="max", help="历史数据天数，默认为 'max'"
    )
    parser.add_argument(
        "--force-overwrite", action="store_true", help="强制覆盖现有数据"
    )
    parser.add_argument(
        "--interval", type=int, default=3, help="请求间隔（秒），默认3秒"
    )
    parser.add_argument("--retries", type=int, default=5, help="最大重试次数，默认5次")

    return parser.parse_args()


def main():
    """主函数：重试失败的下载"""

    args = parse_args()

    print("=" * 80)
    print("🔄 失败下载重试工具")
    print("=" * 80)
    print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # 创建批量下载器
        print("🔧 初始化下载器...")
        downloader = create_batch_downloader()
        print("✅ 下载器初始化完成")
        print()

        # 确定需要重试的币种
        if args.coins:
            # 手动指定的币种
            failed_coins = [coin.strip() for coin in args.coins.split(",")]
            print(f"📝 手动指定的币种: {len(failed_coins)} 个")
            for i, coin in enumerate(failed_coins, 1):
                print(f"   {i}. {coin}")
        else:
            # 从日志文件读取失败的币种
            print("📖 从日志文件读取失败记录...")
            failed_coins = downloader.get_failed_coins_from_log()

            if not failed_coins:
                print("ℹ️  未找到失败的下载记录")
                print("💡 可以使用 --coins 参数手动指定币种")
                print(
                    "   示例: python retry_failed_downloads.py --coins bitcoin,ethereum"
                )
                return None

            print(f"📋 从日志读取到 {len(failed_coins)} 个失败的币种:")
            for i, coin in enumerate(failed_coins, 1):
                print(f"   {i}. {coin}")

        print()

        # 显示重试配置
        print("⚙️  重试配置:")
        print(f"   📊 数据范围: {args.days}")
        print(f"   🔄 最大重试: {args.retries} 次")
        print(f"   ⏱️  请求间隔: {args.interval} 秒")
        print(f"   💾 强制覆盖: {'是' if args.force_overwrite else '否'}")
        print()

        # 确认重试
        confirm = (
            input(f"确认重试这 {len(failed_coins)} 个币种吗？ (y/n): ").lower().strip()
        )
        if confirm != "y":
            print("❌ 用户取消重试")
            return None

        print()
        print("🚀 开始重试下载...")
        print("-" * 60)

        # 如果需要强制覆盖，使用 download_batch 而不是 retry_failed_downloads
        if args.force_overwrite:
            print("⚠️  使用强制覆盖模式...")
            # 构造假的 top_n 排名，实际上会下载指定的币种
            # 这需要我们临时修改获取币种列表的逻辑
            results = {}

            from tqdm import tqdm

            with tqdm(failed_coins, desc="强制重新下载", unit="币种") as pbar:
                for coin_id in pbar:
                    pbar.set_postfix({"当前": coin_id})

                    success = downloader._download_single_coin(
                        coin_id, args.days, "usd", args.retries, 5
                    )

                    results[coin_id] = "success" if success else "failed"

                    if args.interval > 0:
                        time.sleep(args.interval)
        else:
            # 使用常规重试方法
            results = downloader.retry_failed_downloads(
                failed_coins=failed_coins,
                days=args.days,
                vs_currency="usd",
                max_retries=args.retries,
                retry_delay=5,
                request_interval=args.interval,
            )

        print("-" * 60)

        # 统计结果
        success_count = sum(1 for status in results.values() if status == "success")
        failed_count = sum(1 for status in results.values() if status == "failed")

        print("📊 重试结果统计:")
        print(f"   ✅ 重试成功: {success_count} 个币种")
        print(f"   ❌ 仍然失败: {failed_count} 个币种")
        print(f"   📈 成功率: {(success_count / len(failed_coins) * 100):.1f}%")
        print()

        # 显示仍然失败的币种
        if failed_count > 0:
            still_failed = [
                coin for coin, status in results.items() if status == "failed"
            ]
            print("❌ 仍然失败的币种:")
            for i, coin in enumerate(still_failed, 1):
                print(f"   {i}. {coin}")
            print()
            print("💡 建议:")
            print("   • 检查网络连接状态")
            print("   • 确认 API Key 有效性")
            print("   • 尝试增加请求间隔 (--interval)")
            print("   • 查看日志文件了解具体错误")
        else:
            print("🎉 所有币种都重试成功！")

        print()
        print("=" * 80)
        print(f"🏁 重试任务完成！")
        print(f"📅 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        return {
            "success": success_count,
            "failed": failed_count,
            "total": len(failed_coins),
        }

    except KeyboardInterrupt:
        print("\n⚠️  用户中断重试")
        return None
    except Exception as e:
        print(f"\n❌ 重试过程中发生错误: {e}")
        return None


if __name__ == "__main__":
    import time  # 添加缺失的 import

    # 执行主函数
    result = main()

    # 根据结果设置退出码
    if result is None:
        sys.exit(1)
    elif result["failed"] > 0:
        print(f"\n⚠️  部分重试失败，请检查日志")
        sys.exit(2)
    else:
        print(f"\n✅ 所有重试都成功！")
        sys.exit(0)
