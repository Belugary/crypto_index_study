#!/usr/bin/env python3
"""
增量每日数据更新脚本

专门用于新币种检测和历史数据集成的独立脚本。
这是增量更新功能的入口点，提供灵活的配置选项。

使用方式:
    python scripts/incremental_daily_update.py                    # 默认监控前1000名
    python scripts/incremental_daily_update.py --top-n 800       # 监控前800名
    python scripts/incremental_daily_update.py --dry-run         # 试运行模式
    python scripts/incremental_daily_update.py --max-workers 5   # 设置并发数
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.incremental_daily_updater import create_incremental_updater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/incremental_daily_update.log"),
        logging.StreamHandler(),
    ],
)


def print_results_summary(results: dict):
    """打印详细的结果摘要"""
    summary = results.get("summary", {})
    new_coins = results.get("new_coins", [])
    download_results = results.get("download_results", {})
    integration_results = results.get("integration_results", {})

    print("\n" + "=" * 60)
    print("📊 增量更新详细报告")
    print("=" * 60)

    # 基本信息
    print(f"⏱️  执行时间: {summary.get('duration_seconds', 0):.1f} 秒")
    print(f"🎯 监控范围: 前 {summary.get('top_n', 0)} 名")
    print(f"🆕 发现新币: {len(new_coins)} 个")

    if new_coins:
        print(f"新币种列表: {', '.join(new_coins)}")

    # 下载结果
    if download_results:
        successful_downloads = [
            coin for coin, result in download_results.items() if result["success"]
        ]
        failed_downloads = [
            coin for coin, result in download_results.items() if not result["success"]
        ]

        print(f"\n📥 历史数据下载:")
        print(f"   - 成功: {len(successful_downloads)}/{len(download_results)}")
        if failed_downloads:
            print(f"   - 失败: {', '.join(failed_downloads)}")

    # 集成结果
    if integration_results:
        successful_integrations = [
            coin for coin, result in integration_results.items() if result["success"]
        ]
        total_insertions = sum(
            result["inserted_days"] for result in integration_results.values()
        )

        print(f"\n🔄 数据集成:")
        print(f"   - 成功: {len(successful_integrations)}/{len(integration_results)}")
        print(f"   - 总插入: {total_insertions} 天")

        # 详细集成信息
        for coin, result in integration_results.items():
            if result["success"]:
                success_rate = result["success_rate"]
                print(
                    f"   - {coin}: {result['inserted_days']}/{result['total_attempts']} 天 ({success_rate:.1f}%)"
                )
            else:
                print(f"   - {coin}: 失败 - {result.get('error', '未知错误')}")

    # 状态总结
    status = summary.get("status", "unknown")
    if status == "completed":
        print(f"\n✅ 更新成功完成")
    elif status == "dry_run_complete":
        print(f"\n🔍 试运行完成")
    elif status == "no_new_coins":
        print(f"\n😊 没有发现新币种")
    elif status == "error":
        print(f"\n❌ 更新过程中发生错误: {summary.get('error', '未知错误')}")

    print("=" * 60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="增量每日数据更新工具")

    parser.add_argument(
        "--top-n", type=int, default=1000, help="监控的市值排名范围 (默认: 1000)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=3, help="并行下载的最大工作线程数 (默认: 3)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="试运行模式：只检测新币种，不实际下载或修改数据",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        default=True,
        help="启用文件备份功能 (默认: 启用)",
    )
    parser.add_argument("--no-backup", action="store_true", help="禁用文件备份功能")
    parser.add_argument(
        "--coins-dir",
        type=str,
        default="data/coins",
        help="币种数据目录 (默认: data/coins)",
    )
    parser.add_argument(
        "--daily-dir",
        type=str,
        default="data/daily/daily_files",
        help="每日汇总数据目录 (默认: data/daily/daily_files)",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="启用详细日志输出")

    args = parser.parse_args()

    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # 处理备份选项
    backup_enabled = args.backup and not args.no_backup

    print("🔄 增量每日数据更新工具")
    print("=" * 50)
    print(f"📊 配置信息:")
    print(f"   - 监控范围: 前 {args.top_n} 名")
    print(f"   - 并行线程: {args.max_workers}")
    print(f"   - 试运行模式: {'是' if args.dry_run else '否'}")
    print(f"   - 文件备份: {'是' if backup_enabled else '否'}")
    print(f"   - 币种目录: {args.coins_dir}")
    print(f"   - 每日目录: {args.daily_dir}")
    print(f"   - 详细日志: {'是' if args.verbose else '否'}")
    print()

    try:
        # 创建增量更新器
        updater = create_incremental_updater(
            coins_dir=args.coins_dir,
            daily_dir=args.daily_dir,
            backup_enabled=backup_enabled,
        )

        # 执行增量更新
        results = updater.update_with_new_coins(
            top_n=args.top_n, max_workers=args.max_workers, dry_run=args.dry_run
        )

        # 显示结果
        print_results_summary(results)

        # 检查是否成功
        status = results.get("summary", {}).get("status", "unknown")
        if status in ["completed", "dry_run_complete", "no_new_coins"]:
            print("\n🎉 增量更新完成!")
            return 0
        else:
            print("\n⚠️ 增量更新遇到问题")
            return 1

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 脚本执行失败: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
