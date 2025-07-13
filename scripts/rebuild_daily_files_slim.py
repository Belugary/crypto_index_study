#!/usr/bin/env python3
"""
每日数据重建脚本 (薄封装版本)

基于 src/downloaders/daily_aggregator.py 核心模块的薄封装实现。
提供命令行接口来重建每日汇总数据文件。

使用方式:
    python scripts/rebuild_daily_files_slim.py --full-rebuild    # 完整重建
    python scripts/rebuild_daily_files_slim.py --recent-days 7  # 最近7天
    python scripts/rebuild_daily_files_slim.py --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloaders.daily_aggregator import create_daily_aggregator

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/daily_aggregation.log"),
    ],
)
logger = logging.getLogger(__name__)


def rebuild_date_range(aggregator, start_date: str, end_date: str):
    """重建指定日期范围的数据"""
    logger.info(f"重建日期范围: {start_date} 到 {end_date}")

    # 加载币种数据到内存（启用多进程优化）
    aggregator.load_coin_data()

    # 解析日期范围
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()

    # 按日期逐个处理（使用核心模块的单日多进程处理）
    current_date = start_dt
    total_days = (end_dt - start_dt).days + 1
    processed = 0

    while current_date <= end_dt:
        logger.info(f"处理日期: {current_date} ({processed + 1}/{total_days})")
        daily_data = aggregator.get_daily_data(current_date, force_refresh=True)
        current_date += timedelta(days=1)
        processed += 1

    logger.info("重建完成")


def rebuild_recent_days(aggregator, days: int):
    """重建最近N天的数据"""
    end_date = date.today()
    start_date = end_date - timedelta(days=days - 1)

    logger.info(f"重建最近 {days} 天数据: {start_date} 到 {end_date}")

    # 加载币种数据到内存（启用多进程优化）
    aggregator.load_coin_data()

    # 按日期逐个处理（使用核心模块的单日多进程处理）
    current_date = start_date
    processed = 0

    while current_date <= end_date:
        logger.info(f"处理日期: {current_date} ({processed + 1}/{days})")
        daily_data = aggregator.get_daily_data(current_date, force_refresh=True)
        current_date += timedelta(days=1)
        processed += 1

    logger.info("重建完成")


def rebuild_all(aggregator):
    """完整重建所有数据"""
    logger.info("开始完整重建所有历史数据")

    # 加载币种数据到内存（启用多进程优化）
    aggregator.load_coin_data()

    # 使用核心模块的完整重建功能（多进程并行处理所有日期）
    aggregator.build_daily_tables(force_recalculate=True)

    logger.info("完整重建完成")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="每日数据重建脚本 (薄封装版本)")

    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="更新最近N天 (默认: 7), 设置为0则不使用此模式",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="完整重建所有历史数据，覆盖其他时间选项",
    )
    parser.add_argument("--coins-dir", default="data/coins", help="币种数据目录")
    parser.add_argument("--output-dir", default="data/daily", help="输出目录")

    args = parser.parse_args()

    print("🔄 每日数据重建工具 (薄封装版本)")
    print("=" * 50)
    print(f"📊 配置信息:")
    print(f"   - 币种目录: {args.coins_dir}")
    print(f"   - 输出目录: {args.output_dir}")
    print()

    try:
        # 创建聚合器
        aggregator = create_daily_aggregator(
            data_dir=args.coins_dir, output_dir=args.output_dir
        )

        # 根据参数选择操作模式
        if args.full_rebuild:
            print("模式: 完整重建所有历史数据")
            rebuild_all(aggregator)
        elif args.start_date and args.end_date:
            print(f"模式: 重建指定日期范围 {args.start_date} 到 {args.end_date}")
            rebuild_date_range(aggregator, args.start_date, args.end_date)
        elif args.recent_days and args.recent_days > 0:
            print(f"模式: 重建最近 {args.recent_days} 天数据")
            rebuild_recent_days(aggregator, args.recent_days)
        else:
            print("⚠️  未指定有效操作模式，使用默认模式：重建最近7天")
            rebuild_recent_days(aggregator, 7)

        print("✅ 每日数据重建完成")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        sys.exit(1)
    except Exception as e:
        logger.error(f"重建过程中发生错误: {e}", exc_info=True)
        print(f"❌ 重建失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
