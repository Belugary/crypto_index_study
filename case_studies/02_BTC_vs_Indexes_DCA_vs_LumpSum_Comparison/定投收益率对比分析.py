#!/usr/bin/env python3
"""
定投收益率对比分析
计算从2018-01-01到2025-07-01期间，不同策略的定投收益率

对比策略：
- BTC (比特币价格指数)
- Crypto2 (前2名市值加权指数)
- Crypto5 (前5名市值加权指数)
- Crypto10 (前10名市值加权指数)
- Crypto30 (前30名市值加权指数)
- Crypto50 (前50名市值加权指数)

定投参数：
- 投资周期：2018-01-01 到 2025-07-01 (约7.5年)
- 定投频率：每月1号
- 定投金额：每月1000美元
- 总投资期数：90个月
- 总投资金额：90,000美元
"""

import os
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Union

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.downloaders.daily_aggregator import DailyDataAggregator
from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator

# 设置中文字体
plt.rcParams["font.sans-serif"] = ["Arial Unicode MS", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


class DCAAnalyzer:
    """定投收益率分析器"""

    def __init__(self):
        """初始化分析器"""
        self.calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=True, exclude_wrapped_coins=True
        )
        self.daily_aggregator = DailyDataAggregator()

        # 定投参数
        self.start_date = "2018-01-01"
        self.end_date = "2025-07-01"
        self.monthly_investment = 1000.0  # 每月定投金额（美元）
        self.base_value = 1000.0  # 指数基准值

        # 指数配置
        self.strategies = {
            "BTC": "bitcoin",
            "Crypto2": 2,
            "Crypto5": 5,
            "Crypto10": 10,
            "Crypto30": 30,
            "Crypto50": 50,
        }

    def generate_investment_dates(self) -> List[date]:
        """生成定投日期序列（每月1号）"""
        investment_dates = pd.date_range(
            start=self.start_date, end=self.end_date, freq="MS"  # Month Start
        )
        return [d.date() for d in investment_dates]

    def get_btc_index_data(self) -> pd.DataFrame:
        """获取BTC指数数据"""
        print("📊 计算BTC价格指数...")

        # 获取基准日BTC价格
        base_date_obj = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        base_daily_data = self.daily_aggregator.get_daily_data(base_date_obj)

        if base_daily_data.empty:
            raise ValueError(f"基准日期 {self.start_date} 没有可用数据")

        btc_base_data = base_daily_data[base_daily_data["coin_id"] == "bitcoin"]
        if btc_base_data.empty:
            raise ValueError(f"基准日期 {self.start_date} 没有BTC数据")

        base_btc_price = float(btc_base_data.iloc[0]["price"])
        print(f"✅ 基准日期 {self.start_date} BTC价格: ${base_btc_price:,.2f}")

        # 生成日期范围
        start_dt = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        date_range = pd.date_range(start=start_dt, end=end_dt, freq="D")

        btc_data = []

        from tqdm import tqdm

        for current_dt in tqdm(date_range, desc="计算BTC指数", unit="天"):
            current_date = current_dt.date()

            try:
                daily_data = self.daily_aggregator.get_daily_data(current_date)
                if daily_data.empty:
                    continue

                btc_data_current = daily_data[daily_data["coin_id"] == "bitcoin"]
                if btc_data_current.empty:
                    continue

                current_btc_price = float(btc_data_current.iloc[0]["price"])
                btc_index_value = self.base_value * (current_btc_price / base_btc_price)

                btc_data.append(
                    {
                        "date": current_date,
                        "index_value": btc_index_value,
                    }
                )

            except Exception as e:
                print(f"⚠️ 警告：日期 {current_date} BTC数据获取失败: {e}")
                continue

        if not btc_data:
            raise ValueError("无法获取BTC指数数据")

        btc_df = pd.DataFrame(btc_data)
        print(f"✅ BTC指数计算完成，共 {len(btc_df)} 个数据点")
        return btc_df

    def get_crypto_index_data(self, top_n: int) -> pd.DataFrame:
        """获取加密货币指数数据"""
        print(f"📈 计算 Crypto{top_n} (前{top_n}名市值加权指数)...")

        try:
            index_df = self.calculator.calculate_index(
                start_date=self.start_date,
                end_date=self.end_date,
                top_n=top_n,
                base_date=self.start_date,
                base_value=self.base_value,
            )

            print(f"✅ Crypto{top_n} 计算完成，共 {len(index_df)} 个数据点")
            return index_df

        except Exception as e:
            print(f"❌ Crypto{top_n} 计算失败: {e}")
            return pd.DataFrame()

    def get_index_value_on_date(
        self, index_df: pd.DataFrame, target_date: date
    ) -> float:
        """获取指定日期的指数值"""
        target_data = index_df[index_df["date"] == target_date]

        if target_data.empty:
            # 如果指定日期没有数据，寻找最近的前一个交易日
            available_dates = index_df[index_df["date"] <= target_date]["date"]
            if available_dates.empty:
                raise ValueError(f"找不到日期 {target_date} 或之前的数据")

            latest_date = available_dates.max()
            target_data = index_df[index_df["date"] == latest_date]

        if target_data.empty:
            raise ValueError(f"找不到日期 {target_date} 的有效数据")

        return float(target_data.iloc[0]["index_value"])

    def calculate_dca_return(
        self, index_df: pd.DataFrame, investment_dates: List[date]
    ) -> Optional[Dict]:
        """计算定投收益率"""
        total_invested = 0.0
        total_shares = 0.0
        investment_records = []

        # 逐次定投计算
        for invest_date in investment_dates:
            try:
                index_value = self.get_index_value_on_date(index_df, invest_date)
            except ValueError as e:
                print(f"⚠️ 警告：{invest_date} 无法获取指数值，跳过此次定投: {e}")
                continue

            # 计算购买份额
            shares_bought = self.monthly_investment / index_value
            total_invested += self.monthly_investment
            total_shares += shares_bought

            investment_records.append(
                {
                    "date": invest_date,
                    "investment": self.monthly_investment,
                    "index_value": index_value,
                    "shares_bought": shares_bought,
                    "total_invested": total_invested,
                    "total_shares": total_shares,
                }
            )

        if total_shares == 0:
            return None

        # 计算最终资产价值
        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d").date()
        try:
            final_index_value = self.get_index_value_on_date(index_df, end_date_obj)
        except ValueError as e:
            print(f"⚠️ 警告：无法获取 {self.end_date} 的最终指数值: {e}")
            return None

        final_value = total_shares * final_index_value

        # 计算收益率
        total_return = (final_value - total_invested) / total_invested * 100

        # 计算年化收益率
        start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        years = (end_dt - start_dt).days / 365.25
        annualized_return = (final_value / total_invested) ** (1 / years) - 1

        return {
            "total_invested": total_invested,
            "total_shares": total_shares,
            "final_value": final_value,
            "total_return_pct": total_return,
            "annualized_return_pct": annualized_return * 100,
            "investment_count": len(investment_records),
            "avg_cost": total_invested / total_shares if total_shares > 0 else 0,
            "final_index_value": final_index_value,
            "investment_records": investment_records,
        }

    def calculate_lump_sum_return(
        self, index_df: pd.DataFrame, total_investment: float
    ) -> Optional[Dict]:
        """计算一次性投资收益率"""
        start_date_obj = datetime.strptime(self.start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(self.end_date, "%Y-%m-%d").date()

        try:
            start_index_value = self.get_index_value_on_date(index_df, start_date_obj)
            final_index_value = self.get_index_value_on_date(index_df, end_date_obj)
        except ValueError as e:
            print(f"⚠️ 警告：一次性投资计算失败: {e}")
            return None

        # 计算购买份额
        shares = total_investment / start_index_value
        final_value = shares * final_index_value

        # 计算收益率
        total_return = (final_value - total_investment) / total_investment * 100

        # 计算年化收益率
        start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        years = (end_dt - start_dt).days / 365.25
        annualized_return = (final_value / total_investment) ** (1 / years) - 1

        return {
            "total_invested": total_investment,
            "shares": shares,
            "final_value": final_value,
            "total_return_pct": total_return,
            "annualized_return_pct": annualized_return * 100,
            "start_index_value": start_index_value,
            "final_index_value": final_index_value,
        }

    def analyze_all_strategies(self) -> Dict:
        """分析所有策略的定投收益率"""
        print("🚀 开始定投收益率对比分析")
        print("=" * 80)
        print(f"📅 投资期间: {self.start_date} 至 {self.end_date}")
        print(f"💰 每月定投: ${self.monthly_investment:,.0f}")
        print(f"📊 对比策略: {list(self.strategies.keys())}")
        print()

        # 生成投资日期
        investment_dates = self.generate_investment_dates()
        print(f"📆 定投次数: {len(investment_dates)} 次")
        print(f"💵 总投资金额: ${len(investment_dates) * self.monthly_investment:,.0f}")
        print()

        results = {}

        # 分析每个策略
        for strategy_name, strategy_config in self.strategies.items():
            print(f"📈 分析策略: {strategy_name}")
            print("-" * 40)

            try:
                # 获取指数数据
                if strategy_name == "BTC":
                    index_df = self.get_btc_index_data()
                else:
                    index_df = self.get_crypto_index_data(strategy_config)

                if index_df.empty:
                    print(f"❌ {strategy_name} 数据获取失败，跳过")
                    continue

                # 计算定投收益率
                dca_result = self.calculate_dca_return(index_df, investment_dates)

                if dca_result is None:
                    print(f"❌ {strategy_name} 定投计算失败，跳过")
                    continue

                # 计算一次性投资收益率
                lump_sum_result = self.calculate_lump_sum_return(
                    index_df, dca_result["total_invested"]
                )

                # 保存结果
                results[strategy_name] = {
                    "dca": dca_result,
                    "lump_sum": lump_sum_result,
                    "index_data": index_df,
                }

                # 输出结果
                print(f"✅ 定投收益率: {dca_result['total_return_pct']:+.2f}%")
                print(f"✅ 定投年化收益率: {dca_result['annualized_return_pct']:+.2f}%")
                print(f"✅ 定投最终价值: ${dca_result['final_value']:,.0f}")

                if lump_sum_result:
                    print(
                        f"📊 一次性投资收益率: {lump_sum_result['total_return_pct']:+.2f}%"
                    )
                    print(
                        f"📊 一次性投资年化收益率: {lump_sum_result['annualized_return_pct']:+.2f}%"
                    )
                    print(
                        f"📊 一次性投资最终价值: ${lump_sum_result['final_value']:,.0f}"
                    )

                print()

            except Exception as e:
                print(f"❌ {strategy_name} 分析失败: {e}")
                continue

        return results

    def create_summary_table(self, results: Dict) -> pd.DataFrame:
        """创建收益率汇总表"""
        summary_data = []

        for strategy_name, result in results.items():
            dca = result["dca"]
            lump_sum = result["lump_sum"]

            row = {
                "策略": strategy_name,
                "定投总收益率(%)": dca["total_return_pct"],
                "定投年化收益率(%)": dca["annualized_return_pct"],
                "定投最终价值($)": dca["final_value"],
                "定投次数": dca["investment_count"],
                "平均成本": dca["avg_cost"],
            }

            if lump_sum:
                row.update(
                    {
                        "一次性总收益率(%)": lump_sum["total_return_pct"],
                        "一次性年化收益率(%)": lump_sum["annualized_return_pct"],
                        "一次性最终价值($)": lump_sum["final_value"],
                        "定投vs一次性优势(%)": dca["total_return_pct"]
                        - lump_sum["total_return_pct"],
                    }
                )

            summary_data.append(row)

        return pd.DataFrame(summary_data)

    def create_visualizations(self, results: Dict, output_dir: Optional[str] = None):
        """创建可视化图表"""
        if output_dir is None:
            # 默认保存在当前案例文件夹的 outputs 子目录
            script_dir = Path(__file__).parent
            output_dir = str(script_dir / "outputs")
        Path(output_dir).mkdir(exist_ok=True)

        # 1. 定投收益率对比图
        plt.figure(figsize=(12, 8))

        strategies = []
        dca_returns = []
        lump_sum_returns = []

        for strategy_name, result in results.items():
            strategies.append(strategy_name)
            dca_returns.append(result["dca"]["total_return_pct"])
            if result["lump_sum"]:
                lump_sum_returns.append(result["lump_sum"]["total_return_pct"])
            else:
                lump_sum_returns.append(0)

        x = np.arange(len(strategies))
        width = 0.35

        bars1 = plt.bar(x - width / 2, dca_returns, width, label="定投策略", alpha=0.8)
        bars2 = plt.bar(
            x + width / 2, lump_sum_returns, width, label="一次性投资", alpha=0.8
        )

        plt.title(
            "定投 vs 一次性投资收益率对比 (2018-2025)", fontsize=16, fontweight="bold"
        )
        plt.xlabel("投资策略", fontsize=12)
        plt.ylabel("总收益率 (%)", fontsize=12)
        plt.xticks(x, strategies)
        plt.legend()
        plt.grid(True, alpha=0.3, axis="y")

        # 在柱状图上显示数值
        for bar in bars1:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(dca_returns) * 0.01,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontweight="bold",
            )

        for bar in bars2:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(lump_sum_returns) * 0.01,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontweight="bold",
            )

        plt.tight_layout()
        plt.savefig(
            f"{output_dir}/dca_vs_lump_sum_comparison.png", dpi=300, bbox_inches="tight"
        )
        print(f"📊 收益率对比图已保存: {output_dir}/dca_vs_lump_sum_comparison.png")
        plt.close()

        # 2. 年化收益率对比图
        plt.figure(figsize=(12, 8))

        dca_annual_returns = [
            result["dca"]["annualized_return_pct"] for result in results.values()
        ]
        lump_sum_annual_returns = [
            result["lump_sum"]["annualized_return_pct"] if result["lump_sum"] else 0
            for result in results.values()
        ]

        bars1 = plt.bar(
            x - width / 2, dca_annual_returns, width, label="定投策略", alpha=0.8
        )
        bars2 = plt.bar(
            x + width / 2, lump_sum_annual_returns, width, label="一次性投资", alpha=0.8
        )

        plt.title("年化收益率对比 (2018-2025)", fontsize=16, fontweight="bold")
        plt.xlabel("投资策略", fontsize=12)
        plt.ylabel("年化收益率 (%)", fontsize=12)
        plt.xticks(x, strategies)
        plt.legend()
        plt.grid(True, alpha=0.3, axis="y")

        # 在柱状图上显示数值
        for bar in bars1:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(dca_annual_returns) * 0.01,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontweight="bold",
            )

        for bar in bars2:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height + max(lump_sum_annual_returns) * 0.01,
                f"{height:.1f}%",
                ha="center",
                va="bottom",
                fontweight="bold",
            )

        plt.tight_layout()
        plt.savefig(
            f"{output_dir}/annualized_returns_comparison.png",
            dpi=300,
            bbox_inches="tight",
        )
        print(
            f"📊 年化收益率对比图已保存: {output_dir}/annualized_returns_comparison.png"
        )
        plt.close()

    def save_results(
        self, results: Dict, summary_df: pd.DataFrame, output_dir: Optional[str] = None
    ):
        """保存分析结果"""
        if output_dir is None:
            # 默认保存在当前案例文件夹的 outputs 子目录
            script_dir = Path(__file__).parent
            output_dir = str(script_dir / "outputs")
        Path(output_dir).mkdir(exist_ok=True)

        # 保存汇总表
        summary_path = f"{output_dir}/dca_analysis_summary.csv"
        summary_df.to_csv(summary_path, index=False, float_format="%.2f")
        print(f"📄 定投分析汇总已保存: {summary_path}")

        # 保存详细数据
        for strategy_name, result in results.items():
            # 保存定投记录
            if result["dca"]["investment_records"]:
                records_df = pd.DataFrame(result["dca"]["investment_records"])
                records_path = f"{output_dir}/dca_{strategy_name.lower()}_records.csv"
                records_df.to_csv(records_path, index=False, float_format="%.6f")
                print(f"📄 {strategy_name}定投记录已保存: {records_path}")

    def run_analysis(self):
        """运行完整的定投分析"""
        try:
            # 分析所有策略
            results = self.analyze_all_strategies()

            if not results:
                print("❌ 没有成功分析任何策略")
                return

            # 创建汇总表
            print("📊 生成定投分析汇总...")
            summary_df = self.create_summary_table(results)

            print("\n" + "=" * 100)
            print("📈 定投收益率分析汇总 (2018-2025)")
            print("=" * 100)
            print(summary_df.to_string(index=False, float_format="%.2f"))
            print("=" * 100)

            # 创建可视化
            print("\n📊 生成可视化图表...")
            self.create_visualizations(results)

            # 保存结果
            print("\n💾 保存分析结果...")
            self.save_results(results, summary_df)

            print("\n🎉 定投收益率对比分析完成！")
            print(f"📁 结果文件保存在 {Path(__file__).parent / 'outputs'} 目录下")

        except Exception as e:
            print(f"❌ 分析过程中出现错误: {e}")
            import traceback

            traceback.print_exc()


def main():
    """主函数"""
    print("🚀 启动定投收益率对比分析...")
    analyzer = DCAAnalyzer()
    analyzer.run_analysis()


if __name__ == "__main__":
    main()
