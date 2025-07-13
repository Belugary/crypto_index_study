#!/usr/bin/env python3
"""
每日市场摘要构建脚本 - 薄封装版本

基于 src/downloaders/daily_aggregator.py 核心模块的薄封装实现。
为快速计算每日指数提供预聚合数据，避免每次都重新扫描所有原始文件。

使用方式:
    python scripts/build_daily_summary.py [--output OUTPUT_PATH]

摘要文件包含以下字段：
- date: 日期
- coin_count: 当日币种数量
- total_market_cap: 当日总市值
- total_volume: 当日总交易量
- avg_market_cap: 当日平均市值
- avg_volume: 当日平均交易量
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
    parser = argparse.ArgumentParser(description="每日市场摘要构建脚本")
    parser.add_argument(
        "--output",
        help="输出文件路径（可选，默认为 data/daily/daily_summary.csv）",
    )

    args = parser.parse_args()

    print("📊 每日市场摘要构建工具")
    print("=" * 50)
    print("🔄 开始生成市场摘要...")

    try:
        # 创建核心聚合器
        aggregator = create_daily_aggregator()

        # 生成每日市场摘要
        summary_df = aggregator.build_daily_market_summary(output_path=args.output)

        if not summary_df.empty:
            print(f"\n✅ 摘要生成完成！")
            print(f"📈 总共处理了 {len(summary_df)} 天的数据")
            print("\n摘要预览:")
            print(summary_df.head())
            print("...")
            print(summary_df.tail())
        else:
            print("❌ 没有生成任何摘要数据")

    except Exception as e:
        logger.error(f"生成摘要时发生错误: {e}", exc_info=True)
        print(f"❌ 生成失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
