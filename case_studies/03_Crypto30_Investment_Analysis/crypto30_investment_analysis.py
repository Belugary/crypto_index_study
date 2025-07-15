#!(usr/)in/env(pyth/n3)
""")(
Crypto30 指数投资分析脚本

分析假设投资情况：
- 投资日期：2023年12月20日
- 投资金额：10,000 美元
- 指数：Crypto30（前30名原生币，市值加权，自动调仓）
- 分析期间：到2024年12月19日和2025年底
"""

import argparse
import logging
import os
import sys
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
            logging.FileHandler("logs/crypto30_investment_analysis.log"),
            logging.StreamHandler(),
        ],
    )


def calculate_investment_return(
    initial_value: float, final_value: float, initial_investment: float
) -> dict:
    """
    计算投资回报

    Args:
        initial_value: 指数初始值
        final_value: 指数最终值
        initial_investment: 初始投资金额

    Returns:
        投资回报分析结果字典
    """
    # 计算回报率
    return_rate = (final_value - initial_value) / initial_value

    # 计算最终投资价值
    final_investment = initial_investment * (final_value / initial_value)

    # 计算绝对收益
    absolute_gain = final_investment - initial_investment

    return {
        "initial_investment": initial_investment,
        "final_investment": final_investment,
        "absolute_gain": absolute_gain,
        "return_rate": return_rate,
        "return_percentage": return_rate * 100,
        "initial_index_value": initial_value,
        "final_index_value": final_value,
    }


def format_currency(amount: float) -> str:
    """格式化货币显示"""
    return f"${amount:,.2f}"


def format_percentage(rate: float) -> str:
    """格式化百分比显示"""
    sign = "+" if rate >= 0 else ""
    return f"{sign}{rate:.2f}%"


def print_investment_analysis(
    result: dict, start_date: str, end_date: str, period_name: str
):
    """打印投资分析结果"""
    print(f"\n{'='*60}")
    print(f"  Crypto30 指数投资分析 - {period_name}")
    print(f"{'='*60}")
    print(f"投资期间: {start_date} → {end_date}")
    print(f"投资策略: 前30名原生币市值加权指数，每日自动调仓")
    print()

    print(f"📊 指数表现:")
    print(f"  起始指数值: {result['initial_index_value']:,.2f}")
    print(f"  结束指数值: {result['final_index_value']:,.2f}")
    print()

    print(f"💰 投资回报:")
    print(f"  初始投资: {format_currency(result['initial_investment'])}")
    print(f"  最终价值: {format_currency(result['final_investment'])}")
    print(f"  绝对收益: {format_currency(result['absolute_gain'])}")
    print(f"  回报率: {format_percentage(result['return_percentage'])}")

    # 年化收益率计算
    if period_name == "一年期":
        print(f"  年化收益率: {format_percentage(result['return_percentage'])}")
    elif "年" in period_name:
        # 简单估算年化收益率
        years = 2.0 if "两年" in period_name else 1.0
        annualized_return = (
            (result["final_investment"] / result["initial_investment"]) ** (1 / years)
            - 1
        ) * 100
        print(f"  年化收益率: {format_percentage(annualized_return)}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Crypto30指数投资分析")
    parser.add_argument(
        "--investment", type=float, default=10000.0, help="初始投资金额 (默认: 10000)"
    )
    parser.add_argument(
        "--start-date", default="2023-12-20", help="投资开始日期 (默认: 2023-12-20)"
    )
    parser.add_argument("--save-index", action="store_true", help="保存指数数据到文件")

    args = parser.parse_args()

    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("开始 Crypto30 指数投资分析")
    logger.info(f"初始投资: ${args.investment:,.2f}")
    logger.info(f"投资开始日期: {args.start_date}")

    try:
        # 创建指数计算器
        calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=True,  # 排除稳定币
            exclude_wrapped_coins=True,  # 排除包装币
        )

        # 计算到2024年12月19日的表现（一年期）
        logger.info("计算2024年12月19日的投资表现...")
        end_date_1year = "2024-12-19"

        index_df_1year = calculator.calculate_index(
            start_date=args.start_date,
            end_date=end_date_1year,
            base_date=args.start_date,
            base_value=1000.0,  # 基准指数值
            top_n=30,  # 前30名
        )

        if not index_df_1year.empty:
            initial_value_1year = index_df_1year.iloc[0]["index_value"]
            final_value_1year = index_df_1year.iloc[-1]["index_value"]

            result_1year = calculate_investment_return(
                initial_value_1year, final_value_1year, args.investment
            )

            print_investment_analysis(
                result_1year, args.start_date, end_date_1year, "一年期"
            )

        # 计算到2025年底的表现（如果数据可用）
        logger.info("检查2025年数据可用性...")

        # 找到最新可用日期
        latest_date = "2025-07-13"  # 基于我们之前的检查

        logger.info(f"计算到{latest_date}的投资表现...")

        index_df_latest = calculator.calculate_index(
            start_date=args.start_date,
            end_date=latest_date,
            base_date=args.start_date,
            base_value=1000.0,
            top_n=30,
        )

        if not index_df_latest.empty:
            initial_value_latest = index_df_latest.iloc[0]["index_value"]
            final_value_latest = index_df_latest.iloc[-1]["index_value"]

            result_latest = calculate_investment_return(
                initial_value_latest, final_value_latest, args.investment
            )

            print_investment_analysis(
                result_latest, args.start_date, latest_date, f"至{latest_date}"
            )

        # 保存指数数据（如果请求）

        if args.save_index:
            output_dir = Path(__file__).parent / "outputs"
            output_dir.mkdir(exist_ok=True)

            # 保存一年期数据
            output_path_1year = (
                output_dir / f"crypto30_1year_{args.start_date}_to_{end_date_1year}.csv"
            )
            calculator.save_index(index_df_1year, str(output_path_1year))
            logger.info(f"一年期指数数据已保存到: {output_path_1year}")

            # 保存到最新日期的数据
            output_path_latest = (
                output_dir / f"crypto30_latest_{args.start_date}_to_{latest_date}.csv"
            )
            calculator.save_index(index_df_latest, str(output_path_latest))
            logger.info(f"最新指数数据已保存到: {output_path_latest}")

        print(f"\n{'='*60}")
        print("💡 分析说明:")
        print("• 指数基于每日前30名原生币种（按市值排序）")
        print("• 自动排除稳定币和包装币")
        print("• 每日根据市值变化自动调整成分和权重")
        print("• 指数采用市值加权方法计算")
        print("• 假设完美跟踪，无交易成本和滑点")
        print(f"{'='*60}")

    except Exception as e:
        logger.error(f"分析过程中出现错误: {e}")
        raise


if __name__ == "__main__":
    main()
