#!/usr/bin/env python3
"""
加密货币指数收益率对比分析 (2018-01-01 到 2025-07-01)

计算并对比BTC、Top2、Top5、Top10、Top30、Top50指数的总收益率
"""

import os
import sys
from datetime import datetime
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.downloaders.daily_aggregator import DailyDataAggregator
from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator


class ReturnAnalyzer:
    """收益率分析器"""

    def __init__(self):
        """初始化分析器"""
        self.calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=True, exclude_wrapped_coins=True
        )
        self.daily_aggregator = DailyDataAggregator()

        # 分析参数
        self.start_date = "2018-01-01"
        self.end_date = "2025-07-01"
        self.base_value = 1000.0

        # 指数配置
        self.indices_config = {
            "Top2": 2,
            "Top5": 5,
            "Top10": 10,
            "Top30": 30,
            "Top50": 50,
        }

    def get_btc_return(self):
        """计算BTC的收益率"""
        print("📊 计算BTC收益率...")

        # 获取起始日期BTC价格
        start_date_obj = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        start_daily_data = self.daily_aggregator.get_daily_data(start_date_obj)

        if start_daily_data.empty:
            raise ValueError(f"起始日期 {self.start_date} 没有可用数据")

        btc_start_data = start_daily_data[start_daily_data["coin_id"] == "bitcoin"]
        if btc_start_data.empty:
            raise ValueError(f"起始日期 {self.start_date} 没有BTC数据")

        start_btc_price = float(btc_start_data.iloc[0]["price"])

        # 获取结束日期BTC价格
        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        end_daily_data = self.daily_aggregator.get_daily_data(end_date_obj)

        if end_daily_data.empty:
            raise ValueError(f"结束日期 {self.end_date} 没有可用数据")

        btc_end_data = end_daily_data[end_daily_data["coin_id"] == "bitcoin"]
        if btc_end_data.empty:
            raise ValueError(f"结束日期 {self.end_date} 没有BTC数据")

        end_btc_price = float(btc_end_data.iloc[0]["price"])

        # 计算收益率
        total_return = (end_btc_price / start_btc_price - 1) * 100

        return {
            "index_name": "BTC",
            "start_value": start_btc_price,
            "end_value": end_btc_price,
            "total_return": total_return,
        }

    def get_index_return(self, index_name, top_n):
        """计算指定指数的收益率"""
        print(f"📊 计算{index_name}收益率...")

        try:
            # 只计算起始和结束两个点
            start_date_obj = datetime.strptime(self.start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d").date()

            # 获取起始日期数据
            start_market_caps = self.calculator._get_daily_market_caps(start_date_obj)
            if not start_market_caps:
                raise ValueError(f"起始日期 {self.start_date} 没有可用的市值数据")

            start_constituents = self.calculator._select_top_coins(
                start_market_caps, top_n
            )
            start_total_market_cap = sum(
                start_market_caps[coin_id] for coin_id in start_constituents
            )

            # 获取结束日期数据
            end_market_caps = self.calculator._get_daily_market_caps(end_date_obj)
            if not end_market_caps:
                raise ValueError(f"结束日期 {self.end_date} 没有可用的市值数据")

            end_constituents = self.calculator._select_top_coins(end_market_caps, top_n)
            end_total_market_cap = sum(
                end_market_caps[coin_id] for coin_id in end_constituents
            )

            # 计算指数值
            start_index_value = self.base_value
            end_index_value = self.base_value * (
                end_total_market_cap / start_total_market_cap
            )

            # 计算收益率
            total_return = (end_index_value / start_index_value - 1) * 100

            return {
                "index_name": index_name,
                "start_value": start_index_value,
                "end_value": end_index_value,
                "total_return": total_return,
                "start_constituents": start_constituents[:5],  # 只显示前5名
                "end_constituents": end_constituents[:5],
                "start_market_cap": start_total_market_cap,
                "end_market_cap": end_total_market_cap,
            }

        except Exception as e:
            print(f"❌ {index_name} 计算失败: {e}")
            return None

    def analyze_returns(self):
        """分析所有指数的收益率"""
        print("🚀 开始收益率对比分析")
        print("=" * 80)
        print(f"📅 分析期间: {self.start_date} 到 {self.end_date}")
        print(f"💰 基准值: {self.base_value}")
        print(f"🔧 算法: 市值加权指数（排除稳定币和包装币）")
        print("=" * 80)

        results = []

        # 1. 计算BTC收益率
        try:
            btc_result = self.get_btc_return()
            results.append(btc_result)
            print(f"✅ BTC: {btc_result['total_return']:.2f}%")
        except Exception as e:
            print(f"❌ BTC计算失败: {e}")

        # 2. 计算各指数收益率
        for index_name, top_n in self.indices_config.items():
            result = self.get_index_return(index_name, top_n)
            if result:
                results.append(result)
                print(f"✅ {index_name}: {result['total_return']:.2f}%")

        return results

    def create_comparison_report(self, results):
        """创建对比报告"""
        print("\n" + "=" * 80)
        print("📈 收益率对比分析报告")
        print("=" * 80)

        # 排序（按收益率降序）
        results_sorted = sorted(results, key=lambda x: x["total_return"], reverse=True)

        print(f"\n🏆 收益率排名 ({self.start_date} 到 {self.end_date}):")
        print("-" * 60)

        for i, result in enumerate(results_sorted, 1):
            name = result["index_name"]
            return_pct = result["total_return"]

            if name == "BTC":
                start_val = f"${result['start_value']:.2f}"
                end_val = f"${result['end_value']:.2f}"
            else:
                start_val = f"{result['start_value']:.0f}"
                end_val = f"{result['end_value']:.0f}"

            print(f"{i:2d}. {name:6s}: {return_pct:+8.2f}%  ({start_val} → {end_val})")

        # 分析最佳和最差表现
        best = results_sorted[0]
        worst = results_sorted[-1]

        print(f"\n🎯 关键发现:")
        print("-" * 60)
        print(f"🥇 最佳表现: {best['index_name']} (+{best['total_return']:.2f}%)")
        print(f"🥉 最差表现: {worst['index_name']} (+{worst['total_return']:.2f}%)")

        # 计算超额收益
        btc_return = next(
            (r["total_return"] for r in results if r["index_name"] == "BTC"), None
        )
        if btc_return is not None:
            print(f"\n📊 相对BTC的超额收益:")
            print("-" * 60)
            for result in results_sorted:
                if result["index_name"] != "BTC":
                    excess_return = result["total_return"] - btc_return
                    print(f"{result['index_name']:6s}: {excess_return:+7.2f}% (vs BTC)")

        # 集中度分析
        print(f"\n🔍 集中度效应分析:")
        print("-" * 60)

        index_results = [r for r in results if r["index_name"] != "BTC"]
        if len(index_results) >= 2:
            top2_return = next(
                (r["total_return"] for r in index_results if "Top2" in r["index_name"]),
                None,
            )
            top50_return = next(
                (
                    r["total_return"]
                    for r in index_results
                    if "Top50" in r["index_name"]
                ),
                None,
            )

            if top2_return and top50_return:
                concentration_effect = top2_return - top50_return
                print(f"集中度溢价 (Top2 vs Top50): {concentration_effect:+.2f}%")

                if concentration_effect > 0:
                    print("💡 解读: 高集中度组合表现更佳，头部效应明显")
                else:
                    print("💡 解读: 分散化组合表现更佳，降低了集中风险")

        # 年化收益率计算
        years = (
            datetime.strptime(self.end_date, "%Y-%m-%d")
            - datetime.strptime(self.start_date, "%Y-%m-%d")
        ).days / 365.25

        print(f"\n📊 年化收益率 (期间: {years:.1f}年):")
        print("-" * 60)

        for result in results_sorted:
            total_return_decimal = result["total_return"] / 100
            if result["index_name"] == "BTC":
                annualized = (
                    pow(result["end_value"] / result["start_value"], 1 / years) - 1
                ) * 100
            else:
                annualized = (
                    pow(result["end_value"] / result["start_value"], 1 / years) - 1
                ) * 100

            print(f"{result['index_name']:6s}: {annualized:6.2f}% p.a.")

        return results_sorted

    def show_composition_analysis(self, results):
        """显示成分分析"""
        print(f"\n🔄 成分币种分析:")
        print("=" * 80)

        for result in results:
            if result["index_name"] == "BTC":
                continue

            name = result["index_name"]
            start_coins = result.get("start_constituents", [])
            end_coins = result.get("end_constituents", [])

            print(f"\n📊 {name} 成分变化 (前5名):")
            print(f"   {self.start_date}: {', '.join(start_coins)}")
            print(f"   {self.end_date}: {', '.join(end_coins)}")

            # 分析变化
            start_set = set(start_coins)
            end_set = set(end_coins)
            unchanged = start_set & end_set
            new_coins = end_set - start_set
            removed_coins = start_set - end_set

            print(f"   保持不变: {len(unchanged)}/5")
            if new_coins:
                print(f"   新进入: {', '.join(new_coins)}")
            if removed_coins:
                print(f"   被移除: {', '.join(removed_coins)}")


def main():
    """主函数"""
    try:
        print("🚀 启动收益率对比分析...")
        analyzer = ReturnAnalyzer()

        # 执行分析
        results = analyzer.analyze_returns()

        if not results:
            print("❌ 没有成功计算出任何指数")
            return

        # 生成对比报告
        analyzer.create_comparison_report(results)

        # 显示成分分析
        analyzer.show_composition_analysis(results)

        print("\n" + "=" * 80)
        print("✅ 收益率对比分析完成！")

    except Exception as e:
        print(f"❌ 分析过程中出现错误: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
