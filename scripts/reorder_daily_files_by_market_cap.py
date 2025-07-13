#!/usr/bin/env python3
"""
每日汇总文件市值排序与排名重赋值工具 - 薄封装版本

基于 src/downloaders/daily_aggregator.py 核心模块的薄封装实现。

功能：
- 遍历 data/daily/daily_files/ 目录下所有每日汇总 CSV 文件
- 按市值字段降序排序
- 依次赋值 rank 字段（1,2,3...）
- 支持 dry-run 模式，仅输出排序结果不写入文件
- 支持多线程加速
- 支持按日期范围重排序文件

用法：
    python scripts/reorder_daily_files_by_market_cap_slim.py [--dry-run] [--max-workers N]
    python scripts/reorder_daily_files_by_market_cap_slim.py --start-date 2024-01-01 --end-date 2024-01-31
"""

import argparse
import logging
import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.downloaders.daily_aggregator import create_daily_aggregator

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """主函数：薄封装命令行接口"""
    parser = argparse.ArgumentParser(description="每日汇总文件市值排序与排名重赋值工具")
    parser.add_argument(
        "--dry-run", action="store_true", help="仅输出排序结果，不写入文件"
    )
    parser.add_argument("--max-workers", type=int, default=8, help="最大并发线程数")
    parser.add_argument(
        "--start-date",
        type=str,
        help="开始日期 (YYYY-MM-DD)，仅在按日期范围重排序时使用",
    )
    parser.add_argument(
        "--end-date", type=str, help="结束日期 (YYYY-MM-DD)，仅在按日期范围重排序时使用"
    )

    args = parser.parse_args()

    print("📊 每日文件市值重排序工具")
    print("=" * 50)
    
    if args.dry_run:
        print("🔍 试运行模式：仅显示排序结果，不修改文件")
    
    if args.start_date and args.end_date:
        print(f"📅 按日期范围处理：{args.start_date} 到 {args.end_date}")
    else:
        print("📁 处理所有每日文件")

    try:
        # 创建核心聚合器
        aggregator = create_daily_aggregator()

        # 执行重排序
        print(f"\n🚀 开始重排序 (并发数: {args.max_workers})...")
        successful, total = aggregator.reorder_daily_files_by_market_cap(
            dry_run=args.dry_run,
            max_workers=args.max_workers,
            start_date=args.start_date,
            end_date=args.end_date
        )

        # 显示结果
        print(f"\n📈 重排序完成！")
        print(f"📊 处理统计:")
        print(f"   - 成功处理: {successful}")
        print(f"   - 处理失败: {total - successful}")
        print(f"   - 总计文件: {total}")

        if successful == total and total > 0:
            print("🎉 所有文件处理成功!")
        elif successful > 0:
            print(f"⚠️  部分文件处理成功: {successful}/{total}")
        else:
            print("❌ 没有文件被成功处理")

    except Exception as e:
        logger.error(f"重排序时发生错误: {e}", exc_info=True)
        print(f"❌ 重排序失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
