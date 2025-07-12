#!/usr/bin/env python3
"""
指数计算示例

展示如何使用指数计算模块计算区块链资产指数
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator


def setup_logging():
    """设置日志"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def example_basic_usage():
    """基本使用示例"""
    print("=" * 60)
    print("基本使用示例")
    print("=" * 60)

    # 创建指数计算器
    calculator = MarketCapWeightedIndexCalculator(
        data_dir="data/coins", exclude_stablecoins=True, exclude_wrapped_coins=True
    )

    # 计算最近一周的指数（前10名币种）
    end_date = "2025-07-08"
    start_date = "2025-07-04"
    base_date = "2025-07-04"

    print(f"计算 {start_date} 到 {end_date} 的市值加权指数")
    print(f"基准日期: {base_date}")
    print(f"成分币种数: 10")

    try:
        index_df = calculator.calculate_index(
            start_date=start_date,
            end_date=end_date,
            base_date=base_date,
            base_value=1000.0,
            top_n=10,
        )

        print("\n指数数据:")
        print(index_df.to_string(index=False))

        # 计算收益率
        start_value = index_df.iloc[0]["index_value"]
        end_value = index_df.iloc[-1]["index_value"]
        total_return = (end_value / start_value - 1) * 100

        print(f"\n统计信息:")
        print(f"期间收益率: {total_return:.2f}%")
        print(f"最高指数值: {index_df['index_value'].max():.2f}")
        print(f"最低指数值: {index_df['index_value'].min():.2f}")
        print(f"平均成分币种数: {index_df['constituent_count'].mean():.1f}")

    except Exception as e:
        print(f"❌ 计算失败: {e}")


def example_with_different_settings():
    """不同设置的示例"""
    print("\n" + "=" * 60)
    print("不同设置示例")
    print("=" * 60)

    # 包含稳定币和包装币的指数
    calculator_inclusive = MarketCapWeightedIndexCalculator(
        data_dir="data/coins", exclude_stablecoins=False, exclude_wrapped_coins=False
    )

    print("计算包含稳定币和包装币的指数（前5名）")

    try:
        index_df = calculator_inclusive.calculate_index(
            start_date="2025-07-04",
            end_date="2025-07-06",
            base_date="2025-07-04",
            base_value=100.0,  # 不同的基准值
            top_n=5,
        )

        print("\n指数数据:")
        print(index_df.to_string(index=False))

    except Exception as e:
        print(f"❌ 计算失败: {e}")


def example_save_to_file():
    """保存到文件的示例"""
    print("\n" + "=" * 60)
    print("保存文件示例")
    print("=" * 60)

    calculator = MarketCapWeightedIndexCalculator(
        data_dir="data/coins", exclude_stablecoins=True, exclude_wrapped_coins=True
    )

    try:
        # 计算指数
        index_df = calculator.calculate_index(
            start_date="2025-07-04",
            end_date="2025-07-08",
            base_date="2025-07-04",
            base_value=1000.0,
            top_n=30,
        )

        # 保存到文件
        output_path = "data/indices/example_index.csv"
        calculator.save_index(index_df, output_path)

        print(f"✅ 指数已保存到: {output_path}")

        # 验证文件内容
        import pandas as pd

        loaded_df = pd.read_csv(output_path)
        print(f"✅ 文件验证成功，包含 {len(loaded_df)} 条记录")

    except Exception as e:
        print(f"❌ 保存失败: {e}")


def main():
    """主函数"""
    setup_logging()

    print("🚀 区块链资产指数计算示例")
    print("=" * 60)

    try:
        example_basic_usage()
        example_with_different_settings()
        example_save_to_file()

        print("\n" + "=" * 60)
        print("✅ 所有示例运行完成")
        print("=" * 60)

    except KeyboardInterrupt:
        print("\n❌ 用户中断")
    except Exception as e:
        print(f"\n❌ 运行示例时发生错误: {e}")


if __name__ == "__main__":
    main()
