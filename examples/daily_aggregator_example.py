#!/usr/bin/env python3
"""
每日数据聚合器使用示例

演示如何使用DailyDataAggregator进行历史数据分析
"""

from src.data.daily_aggregator import create_daily_aggregator
import pandas as pd
from datetime import datetime, timedelta


def main():
    """演示每日数据聚合器的使用"""
    print("🚀 初始化每日数据聚合器...")

    # 创建聚合器
    aggregator = create_daily_aggregator()

    # 加载数据
    print("📊 加载历史数据...")
    aggregator.load_coin_data()

    # 获取Bitcoin开始日期
    btc_start = aggregator.find_bitcoin_start_date()
    print(f"\n📅 Bitcoin数据开始日期: {btc_start}")

    # 演示：获取Bitcoin第一天的市场数据
    print(f"\n🔍 {btc_start} 当天的市场数据:")
    first_day_data = aggregator.get_daily_data(btc_start)
    print(first_day_data[["rank", "coin_id", "price", "market_cap"]].head())

    # 演示：获取一个较新日期的数据（比如2020年1月1日）
    target_date = "2020-01-01"
    print(f"\n🔍 {target_date} 当天的市场数据 (前10名):")
    day_data = aggregator.get_daily_data(target_date)
    if not day_data.empty:
        print(day_data[["rank", "coin_id", "price", "market_cap"]].head(10))
        print(f"当天共有 {len(day_data)} 个币种有数据")
    else:
        print("当天没有数据")

    # 演示：获取最近的数据
    recent_date = "2025-07-09"
    print(f"\n🔍 {recent_date} 当天的市场数据 (前10名):")
    recent_data = aggregator.get_daily_data(recent_date)
    if not recent_data.empty:
        print(recent_data[["rank", "coin_id", "price", "market_cap"]].head(10))
        print(f"当天共有 {len(recent_data)} 个币种有数据")

        # 计算总市值
        total_market_cap = recent_data["market_cap"].sum()
        print(f"当天总市值: ${total_market_cap:,.0f}")

    # 演示数据覆盖分析
    print(f"\n📈 数据覆盖情况分析:")
    coverage = aggregator.get_data_coverage_analysis()
    print(f"- 总币种数: {coverage['total_coins']}")
    print(f"- 时间跨度: {coverage['date_range']['total_days']} 天")
    print(f"- 数据最多的前5个币种:")
    for coin in coverage["coin_details"][:5]:
        print(f"  • {coin['coin_id']}: {coin['data_points']} 天数据")

    # 可选：生成少量示例每日数据文件
    print(f"\n💾 生成示例每日数据文件...")
    # 只生成最近7天的数据作为示例
    end_date = datetime.strptime("2025-07-09", "%Y-%m-%d").date()
    start_date = end_date - timedelta(days=6)  # 最近7天

    aggregator.build_daily_tables(start_date=str(start_date), end_date=str(end_date))

    print("✅ 示例完成！")
    print("📁 查看 data/daily/ 目录中生成的每日数据文件")


if __name__ == "__main__":
    main()
