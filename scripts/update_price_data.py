#!/usr/bin/env python3
"""
价格数据更新脚本

该脚本调用核心模块来执行智能的价格数据更新策略。
这是一个自动化脚本，用于定期执行价格数据更新任务。

使用方式:
    python scripts/update_price_data.py                     # 智能更新510个原生币
    python scripts/update_price_data.py --native-coins 700  # 智能更新700个原生币
    python scripts/update_price_data.py --max-range 1500    # 设置最大搜索范围
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/price_data_update.log"),
        logging.StreamHandler(),
    ],
)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="智能量价数据更新工具")
    parser.add_argument(
        "--native-coins", type=int, default=510, help="目标原生币种数量 (默认: 510)"
    )
    parser.add_argument(
        "--max-range", type=int, default=1000, help="最大搜索范围 (默认: 1000)"
    )
    # 新增每日数据汇总选项
    parser.add_argument(
        "--update-daily",
        action="store_true",
        help="同时更新每日汇总数据 (用于指数计算)",
    )
    parser.add_argument(
        "--daily-days",
        type=int,
        default=7,
        help="更新最近N天的每日汇总 (默认: 7天，仅在--update-daily时生效)",
    )
    # 新增增量更新选项
    parser.add_argument(
        "--incremental-daily",
        action="store_true",
        help="使用增量模式更新每日汇总数据 (检测新币种并集成历史数据)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="试运行模式：只检测新币种，不实际下载或修改数据",
    )

    args = parser.parse_args()

    print("🔍 智能量价数据更新工具")
    print("=" * 50)
    print(f"📊 配置信息:")
    print(f"   - 目标原生币种数: {args.native_coins}")
    print(f"   - 最大搜索范围: {args.max_range}")
    print(f"   - 更新每日汇总: {'是' if args.update_daily else '否'}")
    print(f"   - 增量每日更新: {'是' if args.incremental_daily else '否'}")
    print(f"   - 试运行模式: {'是' if args.dry_run else '否'}")
    if args.update_daily:
        print(f"   - 汇总天数: 最近 {args.daily_days} 天")
    print()

    try:
        # 创建更新器并执行更新
        updater = PriceDataUpdater()
        updater.update_with_smart_strategy(args.native_coins, args.max_range)

        # 可选的每日数据汇总
        if args.update_daily or args.incremental_daily:
            if args.incremental_daily:
                print("\n🔄 开始增量每日数据更新...")

                # 导入增量更新功能
                from src.updaters.incremental_daily_updater import (
                    create_incremental_updater,
                )

                incremental_updater = create_incremental_updater()
                results = incremental_updater.update_with_new_coins(
                    top_n=args.max_range,  # 使用相同的搜索范围
                    max_workers=3,  # 控制并发数避免API限制
                    dry_run=args.dry_run,  # 支持试运行模式
                )

                # 显示结果摘要
                if args.dry_run:
                    print(f"🔍 试运行完成，发现 {len(results['new_coins'])} 个新币种")
                    if results["new_coins"]:
                        print(f"新币种: {', '.join(results['new_coins'])}")
                else:
                    summary = results["summary"]
                    print(f"✅ 增量更新完成！")
                    print(f"   - 新币种: {summary.get('new_coins_count', 0)}")
                    print(f"   - 成功下载: {summary.get('successful_downloads', 0)}")
                    print(f"   - 成功集成: {summary.get('successful_integrations', 0)}")
                    print(f"   - 总插入: {summary.get('total_insertions', 0)} 天")
                    print(f"   - 用时: {summary.get('duration_seconds', 0):.1f} 秒")

            elif args.update_daily:
                print("\n🔄 开始传统每日汇总数据更新...")

                # 导入每日汇总功能
                from scripts.rebuild_daily_files import DailyDataAggregator

                aggregator = DailyDataAggregator()
                aggregator.update_recent_days(
                    days=args.daily_days,
                    parallel=True,  # 自动使用并行处理提升效率
                    max_workers=None,  # 自动设置工作进程数
                )
                print("✅ 传统每日汇总数据更新完成!")

        print("\n✅ 价格数据更新完成!")

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
