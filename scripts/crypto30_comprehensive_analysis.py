#!/usr/bin/env python3
"""
Crypto30 综合分析脚本

功能:
1. 计算从2020年1月1日开始的Crypto30指数(基准100)
2. 生成包含每日指数值、成分币种和占比的详细表格
3. 生成月度变化分析报告(.md格式)
4. 跟踪成分变化、排名变化、权重变化等

使用方式:
    python scripts/crypto30_comprehensive_analysis.py
    python scripts/crypto30_comprehensive_analysis.py --end-date 2024-12-31
    python scripts/crypto30_comprehensive_analysis.py --output-dir custom_output
"""

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
from tqdm import tqdm

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator
from src.downloaders.daily_aggregator import DailyDataAggregator
from src.classification.unified_classifier import UnifiedClassifier


class Crypto30ComprehensiveAnalyzer:
    """Crypto30综合分析器"""

    def __init__(self, output_dir: str = "data/crypto30_analysis"):
        """
        初始化分析器

        Args:
            output_dir: 输出目录
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 初始化组件
        self.calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=True, exclude_wrapped_coins=True
        )
        self.daily_aggregator = DailyDataAggregator()
        self.classifier = UnifiedClassifier()

        # 设置日志
        self.logger = logging.getLogger(__name__)

    def get_daily_constituents_and_weights(
        self, target_date: date, top_n: int = 30
    ) -> Tuple[List[str], Dict[str, float], Dict[str, float]]:
        """
        获取指定日期的成分币种、权重和价格

        Args:
            target_date: 目标日期
            top_n: 成分数量

        Returns:
            (成分币种列表, 权重字典, 价格字典)
        """
        # 获取市值数据
        market_caps = self.calculator._get_daily_market_caps(target_date)
        if not market_caps:
            return [], {}, {}

        # 选择前N名
        constituents = self.calculator._select_top_coins(market_caps, top_n)

        # 计算权重
        weights = self.calculator._calculate_weights(constituents, market_caps)

        # 获取价格
        prices = {}
        for coin_id in constituents:
            price = self.calculator._get_coin_price(coin_id, target_date)
            if price is not None:
                prices[coin_id] = price

        return constituents, weights, prices

    def generate_daily_detailed_data(
        self, start_date: date, end_date: date, base_value: float = 100.0
    ) -> pd.DataFrame:
        """
        生成详细的每日数据表

        Args:
            start_date: 开始日期
            end_date: 结束日期
            base_value: 基准指数值

        Returns:
            详细的每日数据DataFrame
        """
        self.logger.info(f"生成详细每日数据: {start_date} 到 {end_date}")

        # 首先计算基础指数
        index_df = self.calculator.calculate_index(
            start_date=start_date,
            end_date=end_date,
            base_date=start_date,
            base_value=base_value,
            top_n=30,
        )

        detailed_data = []

        # 为每个日期生成详细数据
        with tqdm(total=len(index_df), desc="生成详细数据", unit="天") as pbar:
            for _, row in index_df.iterrows():
                current_date = row["date"]
                index_value = row["index_value"]

                # 获取当日成分和权重
                constituents, weights, prices = self.get_daily_constituents_and_weights(
                    current_date, 30
                )

                if not constituents:
                    pbar.update(1)
                    continue

                # 构建成分权重信息 - 只保留核心数据
                import json

                constituent_weights_dict = {}
                for coin_id in constituents:
                    weight_decimal = weights.get(coin_id, 0)
                    constituent_weights_dict[coin_id] = round(
                        weight_decimal, 5
                    )  # 保留5位小数

                constituent_weights_json = json.dumps(
                    constituent_weights_dict, ensure_ascii=False
                )

                detailed_data.append(
                    {
                        "date": current_date,
                        "index_value": index_value,
                        "constituent_count": len(constituents),
                        "constituent_weights_json": constituent_weights_json,  # 只保留JSON格式的精确权重
                    }
                )

                pbar.update(1)

        return pd.DataFrame(detailed_data)

    def analyze_monthly_changes(self, detailed_df: pd.DataFrame) -> List[Dict]:
        """
        分析月度变化

        Args:
            detailed_df: 详细每日数据

        Returns:
            月度变化分析列表
        """
        self.logger.info("分析月度变化")

        monthly_analyses = []

        # 按月分组
        detailed_df["year_month"] = pd.to_datetime(detailed_df["date"]).dt.to_period(
            "M"
        )
        monthly_groups = detailed_df.groupby("year_month")

        prev_month_constituents = set()

        for period, group in monthly_groups:
            month_start = group.iloc[0]
            month_end = group.iloc[-1]

            # 解析月末成分 - 从JSON权重数据中获取
            import json

            current_constituents = set()
            if month_end["constituent_weights_json"]:
                try:
                    weights_dict = json.loads(month_end["constituent_weights_json"])
                    current_constituents = set(weights_dict.keys())
                except (json.JSONDecodeError, KeyError):
                    pass

            # 计算变化
            if prev_month_constituents:
                new_additions = current_constituents - prev_month_constituents
                removals = prev_month_constituents - current_constituents
                unchanged = current_constituents & prev_month_constituents
            else:
                new_additions = current_constituents
                removals = set()
                unchanged = set()

            # 计算指数表现
            index_change = (
                month_end["index_value"] / month_start["index_value"] - 1
            ) * 100

            # 计算最大权重
            max_weight = 0
            if month_end["constituent_weights_json"]:
                try:
                    weights_dict = json.loads(month_end["constituent_weights_json"])
                    max_weight = max(weights_dict.values()) * 100 if weights_dict else 0
                except (json.JSONDecodeError, KeyError):
                    pass

            monthly_analysis = {
                "period": str(period),
                "start_date": month_start["date"],
                "end_date": month_end["date"],
                "start_index": month_start["index_value"],
                "end_index": month_end["index_value"],
                "index_change_pct": index_change,
                "constituent_count": month_end["constituent_count"],
                "new_additions": list(new_additions),
                "removals": list(removals),
                "unchanged_count": len(unchanged),
                "turnover_rate": (len(new_additions) + len(removals)) / 30 * 100,
                "top_constituent_weight": max_weight,
            }

            monthly_analyses.append(monthly_analysis)
            prev_month_constituents = current_constituents

        return monthly_analyses

    def generate_monthly_report(
        self, monthly_analyses: List[Dict], output_path: str
    ) -> None:
        """
        生成月度变化报告

        Args:
            monthly_analyses: 月度分析数据
            output_path: 输出文件路径
        """
        self.logger.info(f"生成月度报告: {output_path}")

        with open(output_path, "w", encoding="utf-8") as f:
            f.write("# Crypto30 指数月度变化报告\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # 总体统计
            f.write("## 总体统计\n\n")
            total_months = len(monthly_analyses)
            if total_months > 0:
                start_index = monthly_analyses[0]["start_index"]
                end_index = monthly_analyses[-1]["end_index"]
                total_return = (end_index / start_index - 1) * 100

                f.write(
                    f"- **分析期间**: {monthly_analyses[0]['start_date']} 到 {monthly_analyses[-1]['end_date']}\n"
                )
                f.write(f"- **总月数**: {total_months}\n")
                f.write(f"- **期初指数**: {start_index:.2f}\n")
                f.write(f"- **期末指数**: {end_index:.2f}\n")
                f.write(f"- **总收益率**: {total_return:.2f}%\n")

                # 计算平均换手率
                avg_turnover = (
                    sum(ma["turnover_rate"] for ma in monthly_analyses) / total_months
                )
                f.write(f"- **平均月换手率**: {avg_turnover:.2f}%\n\n")

            # 月度详情
            f.write("## 月度详情\n\n")

            for analysis in monthly_analyses:
                f.write(f"### {analysis['period']}\n\n")
                f.write(
                    f"- **时间范围**: {analysis['start_date']} 到 {analysis['end_date']}\n"
                )
                f.write(
                    f"- **指数变化**: {analysis['start_index']:.2f} → {analysis['end_index']:.2f} ({analysis['index_change_pct']:+.2f}%)\n"
                )
                f.write(f"- **成分数量**: {analysis['constituent_count']}\n")
                f.write(f"- **最大权重**: {analysis['top_constituent_weight']:.2f}%\n")
                f.write(f"- **换手率**: {analysis['turnover_rate']:.2f}%\n\n")

                if analysis["new_additions"]:
                    f.write(f"**新增成分** ({len(analysis['new_additions'])}个):\n")
                    for coin in analysis["new_additions"]:
                        f.write(f"- {coin}\n")
                    f.write("\n")

                if analysis["removals"]:
                    f.write(f"**移除成分** ({len(analysis['removals'])}个):\n")
                    for coin in analysis["removals"]:
                        f.write(f"- {coin}\n")
                    f.write("\n")

                f.write(f"**不变成分**: {analysis['unchanged_count']}个\n\n")
                f.write("---\n\n")

            # 高频变化分析
            f.write("## 成分变化分析\n\n")

            # 统计各币种的出现频次
            coin_appearances = defaultdict(int)
            coin_additions = defaultdict(int)
            coin_removals = defaultdict(int)

            for analysis in monthly_analyses:
                for coin in analysis["new_additions"]:
                    coin_additions[coin] += 1
                for coin in analysis["removals"]:
                    coin_removals[coin] += 1

            # 最频繁变化的币种
            if coin_additions:
                f.write("### 最频繁新增的币种\n\n")
                sorted_additions = sorted(
                    coin_additions.items(), key=lambda x: x[1], reverse=True
                )
                for coin, count in sorted_additions[:10]:
                    f.write(f"- **{coin}**: {count}次\n")
                f.write("\n")

            if coin_removals:
                f.write("### 最频繁移除的币种\n\n")
                sorted_removals = sorted(
                    coin_removals.items(), key=lambda x: x[1], reverse=True
                )
                for coin, count in sorted_removals[:10]:
                    f.write(f"- **{coin}**: {count}次\n")
                f.write("\n")

    def run_comprehensive_analysis(
        self, start_date: date = date(2020, 1, 1), end_date: Optional[date] = None
    ) -> None:
        """
        运行完整的综合分析

        Args:
            start_date: 开始日期
            end_date: 结束日期，默认为今天
        """
        if end_date is None:
            end_date = date.today()

        self.logger.info("=" * 60)
        self.logger.info("开始 Crypto30 综合分析")
        self.logger.info("=" * 60)
        self.logger.info(f"分析期间: {start_date} 到 {end_date}")
        self.logger.info(f"输出目录: {self.output_dir}")

        try:
            # 1. 生成详细每日数据
            self.logger.info("步骤 1/3: 生成详细每日数据")
            detailed_df = self.generate_daily_detailed_data(start_date, end_date)

            # 保存详细数据
            daily_output = self.output_dir / "crypto30_daily_detailed.csv"
            detailed_df.to_csv(daily_output, index=False, float_format="%.6f")
            self.logger.info(f"详细每日数据已保存: {daily_output}")

            # 2. 分析月度变化
            self.logger.info("步骤 2/3: 分析月度变化")
            monthly_analyses = self.analyze_monthly_changes(detailed_df)

            # 保存月度分析数据
            monthly_df = pd.DataFrame(monthly_analyses)
            monthly_output = self.output_dir / "crypto30_monthly_analysis.csv"
            monthly_df.to_csv(monthly_output, index=False, float_format="%.6f")
            self.logger.info(f"月度分析数据已保存: {monthly_output}")

            # 3. 生成月度报告
            self.logger.info("步骤 3/3: 生成月度报告")
            report_output = self.output_dir / "crypto30_monthly_report.md"
            self.generate_monthly_report(monthly_analyses, str(report_output))
            self.logger.info(f"月度报告已保存: {report_output}")

            # 总结
            self.logger.info("=" * 60)
            self.logger.info("Crypto30 综合分析完成")
            self.logger.info("=" * 60)
            self.logger.info(f"📊 每日数据点: {len(detailed_df)}")
            self.logger.info(f"📅 月度分析: {len(monthly_analyses)}")
            self.logger.info(
                f"📈 期间收益率: {(detailed_df.iloc[-1]['index_value'] / detailed_df.iloc[0]['index_value'] - 1) * 100:.2f}%"
            )
            self.logger.info(f"📁 输出目录: {self.output_dir}")

            # 显示文件清单
            self.logger.info("\n生成的文件:")
            self.logger.info(f"  - {daily_output}")
            self.logger.info(f"  - {monthly_output}")
            self.logger.info(f"  - {report_output}")

        except Exception as e:
            self.logger.error(f"分析过程中发生错误: {e}")
            raise


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/crypto30_analysis.log"),
        ],
    )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="Crypto30 综合分析")

    parser.add_argument(
        "--start-date",
        default="2020-01-01",
        help="开始日期 (YYYY-MM-DD), 默认: 2020-01-01",
    )
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD), 默认: 今天")
    parser.add_argument(
        "--output-dir",
        default="data/crypto30_analysis",
        help="输出目录, 默认: data/crypto30_analysis",
    )

    args = parser.parse_args()

    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 解析日期
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date = None
        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

        # 创建分析器并运行
        analyzer = Crypto30ComprehensiveAnalyzer(args.output_dir)
        analyzer.run_comprehensive_analysis(start_date, end_date)

    except ValueError as e:
        logger.error(f"日期格式错误: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"分析失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
