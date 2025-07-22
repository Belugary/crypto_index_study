#!/usr/bin/env python3
"""
指数计算脚本

计算市值加权的区块链资产指数
"""

import argparse
import logging
import multiprocessing
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/index_calculation.log"),
        ],
    )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="计算区块链资产指数")

    parser.add_argument(
        "--start-date", required=True, help="指数计算开始日期 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", required=True, help="指数计算结束日期 (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--base-date",
        default="2020-01-01",
        help="基准日期 (YYYY-MM-DD), 默认: 2020-01-01",
    )
    parser.add_argument(
        "--base-value", type=float, default=1000.0, help="基准指数值, 默认: 1000.0"
    )
    parser.add_argument(
        "--top-n", type=int, default=30, help="每日选择的成分币种数量, 默认: 30"
    )
    parser.add_argument(
        "--include-stablecoins", action="store_true", help="包含稳定币 (默认排除)"
    )
    parser.add_argument(
        "--include-wrapped-coins", action="store_true", help="包含包装币 (默认排除)"
    )
    parser.add_argument(
        "--output",
        "-o",
        default="data/indices/market_cap_weighted_top30.csv",
        help="输出文件路径",
    )
    parser.add_argument(
        "--data-dir", default="data/coins", help="价格数据目录, 默认: data/coins"
    )
    parser.add_argument(
        "--force-rebuild",
        action="store_true",
        help="强制重建每日数据文件，确保使用最新的原始数据计算指数",
    )

    args = parser.parse_args()

    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 验证日期格式
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        base_date = datetime.strptime(args.base_date, "%Y-%m-%d").date()

        if start_date > end_date:
            raise ValueError("开始日期不能晚于结束日期")

        logger.info("=" * 50)
        logger.info("开始计算区块链资产指数")
        logger.info("=" * 50)
        logger.info(f"计算时间范围: {args.start_date} 到 {args.end_date}")
        logger.info(f"基准日期: {args.base_date}")
        logger.info(f"基准指数值: {args.base_value}")
        logger.info(f"成分币种数量: {args.top_n}")
        logger.info(f"排除稳定币: {not args.include_stablecoins}")
        logger.info(f"排除包装币: {not args.include_wrapped_coins}")
        logger.info(f"数据目录: {args.data_dir}")
        logger.info(f"输出文件: {args.output}")
        logger.info(f"强制重建每日数据: {'是' if args.force_rebuild else '否'}")

        # 创建指数计算器
        calculator = MarketCapWeightedIndexCalculator(
            data_dir=args.data_dir,
            exclude_stablecoins=not args.include_stablecoins,
            exclude_wrapped_coins=not args.include_wrapped_coins,
            force_rebuild=args.force_rebuild,
            use_database=True,  # 🚀 启用数据库模式以获得更好性能
        )

        # 计算指数
        index_df = calculator.calculate_index(
            start_date=args.start_date,
            end_date=args.end_date,
            base_date=args.base_date,
            base_value=args.base_value,
            top_n=args.top_n,
        )

        # 保存结果
        calculator.save_index(index_df, args.output)

        # 显示统计信息
        logger.info("=" * 50)
        logger.info("指数计算完成")
        logger.info("=" * 50)
        logger.info(f"数据点数量: {len(index_df)}")
        logger.info(f"指数起始值: {index_df.iloc[0]['index_value']:.6f}")
        logger.info(f"指数结束值: {index_df.iloc[-1]['index_value']:.6f}")
        logger.info(f"指数最高值: {index_df['index_value'].max():.6f}")
        logger.info(f"指数最低值: {index_df['index_value'].min():.6f}")
        logger.info(f"平均成分币种数: {index_df['constituent_count'].mean():.1f}")

        # 计算总收益率
        total_return = (
            index_df.iloc[-1]["index_value"] / index_df.iloc[0]["index_value"] - 1
        ) * 100
        logger.info(f"期间总收益率: {total_return:.2f}%")

        logger.info(f"结果已保存到: {args.output}")

    except ValueError as e:
        logger.error(f"参数错误: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"文件未找到: {e}")
        logger.error("请确保已运行价格数据更新脚本")
        sys.exit(1)
    except Exception as e:
        logger.error(f"计算过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
