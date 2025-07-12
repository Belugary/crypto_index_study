#!/usr/bin/env python3
"""
每日数据汇总脚本

基于 coins/ 目录中的价格数据重新生成 daily_files/ 汇总数据
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def setup_logging():
    """设置日志配置"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("logs/daily_aggregation.log"),
        ],
    )


class DailyDataAggregator:
    """每日数据汇总器"""

    def __init__(
        self, coins_dir: str = "data/coins", output_dir: str = "data/daily/daily_files"
    ):
        """
        初始化汇总器

        Args:
            coins_dir: 币种数据目录
            output_dir: 输出目录
        """
        self.coins_dir = Path(coins_dir)
        self.output_dir = Path(output_dir)
        self.logger = logging.getLogger(__name__)

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_coin_data(self, coin_id: str) -> Optional[pd.DataFrame]:
        """
        加载币种数据

        Args:
            coin_id: 币种ID

        Returns:
            DataFrame或None
        """
        csv_path = self.coins_dir / f"{coin_id}.csv"
        if not csv_path.exists():
            return None

        try:
            df = pd.read_csv(csv_path)
            # 转换时间戳为日期
            df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            df["coin_id"] = coin_id
            return df
        except Exception as e:
            self.logger.warning(f"读取 {coin_id} 数据失败: {e}")
            return None

    def _get_all_coin_ids(self) -> List[str]:
        """获取所有币种ID"""
        coin_ids = []
        for csv_file in self.coins_dir.glob("*.csv"):
            coin_ids.append(csv_file.stem)
        return sorted(coin_ids)

    def _get_date_range(self, coin_ids: List[str]) -> tuple[date, date]:
        """
        获取所有币种数据的日期范围

        Args:
            coin_ids: 币种ID列表

        Returns:
            (开始日期, 结束日期)
        """
        min_date = None
        max_date = None

        self.logger.info("扫描日期范围...")
        for i, coin_id in enumerate(coin_ids):
            if i % 100 == 0:
                self.logger.info(f"已扫描 {i}/{len(coin_ids)} 个币种")

            df = self._load_coin_data(coin_id)
            if df is not None and len(df) > 0:
                coin_min = df["date"].min()
                coin_max = df["date"].max()

                if min_date is None or coin_min < min_date:
                    min_date = coin_min
                if max_date is None or coin_max > max_date:
                    max_date = coin_max

        if min_date is None or max_date is None:
            raise ValueError("无法找到有效的日期范围")

        return min_date, max_date

    def _aggregate_daily_data(
        self, target_date: date, coin_ids: List[str]
    ) -> pd.DataFrame:
        """
        汇总指定日期的数据

        Args:
            target_date: 目标日期
            coin_ids: 币种ID列表

        Returns:
            当日汇总数据DataFrame
        """
        daily_data = []

        # 分别处理有数据和无数据的币种
        valid_data = []
        missing_coins = []

        for coin_id in coin_ids:
            df = self._load_coin_data(coin_id)

            try:
                if df is None:
                    missing_coins.append(coin_id)
                    continue

                # 找到目标日期的数据
                target_data = df[df["date"] == target_date]
                if target_data.empty:
                    missing_coins.append(coin_id)
                    continue

                # 取最新的记录（如果有多条）
                latest_record = target_data.iloc[-1]

                # 检查数据有效性
                if (
                    pd.isna(latest_record["price"])
                    or latest_record["price"] <= 0
                    or pd.isna(latest_record["market_cap"])
                    or latest_record["market_cap"] <= 0
                ):
                    missing_coins.append(coin_id)
                    continue

                valid_data.append(
                    {
                        "timestamp": int(latest_record["timestamp"]),
                        "price": float(latest_record["price"]),
                        "volume": (
                            float(latest_record["volume"])
                            if pd.notna(latest_record["volume"])
                            else 0.0
                        ),
                        "market_cap": float(latest_record["market_cap"]),
                        "date": target_date,
                        "coin_id": coin_id,
                    }
                )

            except Exception as e:
                self.logger.warning(f"处理 {coin_id} 在 {target_date} 的数据失败: {e}")
                missing_coins.append(coin_id)
                continue

        # 将所有数据合并到daily_data
        daily_data = valid_data

        if not daily_data:
            self.logger.warning(f"{target_date} 没有找到任何有效数据")
            return pd.DataFrame()

        df = pd.DataFrame(daily_data)

        # 按市值排序并添加排名
        df = df.sort_values("market_cap", ascending=False).reset_index(drop=True)
        df["rank"] = range(1, len(df) + 1)

        # 重新排列列顺序
        df = df[
            ["timestamp", "price", "volume", "market_cap", "date", "coin_id", "rank"]
        ]

        self.logger.info(f"{target_date}: 找到 {len(df)} 个有效币种")

        return df

    def _save_daily_file(self, df: pd.DataFrame, target_date: date) -> bool:
        """
        保存每日数据文件

        Args:
            df: 数据DataFrame
            target_date: 目标日期

        Returns:
            是否成功保存
        """
        if df.empty:
            return False

        # 创建年月目录
        year_dir = self.output_dir / str(target_date.year)
        month_dir = year_dir / f"{target_date.month:02d}"
        month_dir.mkdir(parents=True, exist_ok=True)

        # 保存文件
        filename = f"{target_date}.csv"
        filepath = month_dir / filename

        try:
            df.to_csv(filepath, index=False, float_format="%.6f")
            return True
        except Exception as e:
            self.logger.error(f"保存 {target_date} 数据失败: {e}")
            return False

    def rebuild_all_daily_files(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> None:
        """
        重建所有每日文件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)，None表示自动检测
            end_date: 结束日期 (YYYY-MM-DD)，None表示自动检测
        """
        self.logger.info("开始重建每日汇总数据")

        # 获取所有币种
        coin_ids = self._get_all_coin_ids()
        self.logger.info(f"找到 {len(coin_ids)} 个币种")

        # 确定日期范围
        if start_date is None or end_date is None:
            auto_start, auto_end = self._get_date_range(coin_ids)
            actual_start = (
                datetime.strptime(start_date, "%Y-%m-%d").date()
                if start_date
                else auto_start
            )
            actual_end = (
                datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else auto_end
            )
        else:
            actual_start = datetime.strptime(start_date, "%Y-%m-%d").date()
            actual_end = datetime.strptime(end_date, "%Y-%m-%d").date()

        self.logger.info(f"数据日期范围: {actual_start} 到 {actual_end}")

        # 逐日处理
        current_date = actual_start
        total_days = (actual_end - actual_start).days + 1
        processed_days = 0
        successful_days = 0

        while current_date <= actual_end:
            if processed_days % 100 == 0:
                self.logger.info(f"处理进度: {processed_days}/{total_days} 天")

            # 汇总当日数据
            daily_df = self._aggregate_daily_data(current_date, coin_ids)

            if not daily_df.empty:
                # 保存文件
                if self._save_daily_file(daily_df, current_date):
                    successful_days += 1
                    if processed_days % 365 == 0:  # 每年打印一次详细信息
                        self.logger.info(
                            f"{current_date}: 汇总了 {len(daily_df)} 个币种"
                        )

            current_date += timedelta(days=1)
            processed_days += 1

        self.logger.info("重建完成")
        self.logger.info(f"总处理天数: {processed_days}")
        self.logger.info(f"成功生成文件: {successful_days}")
        self.logger.info(f"成功率: {successful_days/processed_days*100:.1f}%")

    def rebuild_date_range(self, start_date: str, end_date: str) -> None:
        """
        重建指定日期范围的文件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
        """
        self.rebuild_all_daily_files(start_date, end_date)

    def update_recent_days(self, days: int = 30) -> None:
        """
        更新最近N天的数据

        Args:
            days: 天数
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

        self.logger.info(f"更新最近 {days} 天的数据")
        self.rebuild_all_daily_files(
            start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")
        )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="每日数据汇总脚本")

    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--recent-days", type=int, help="更新最近N天")
    parser.add_argument("--rebuild-all", action="store_true", help="重建所有数据")
    parser.add_argument("--coins-dir", default="data/coins", help="币种数据目录")
    parser.add_argument(
        "--output-dir", default="data/daily/daily_files", help="输出目录"
    )

    args = parser.parse_args()

    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 创建汇总器
        aggregator = DailyDataAggregator(
            coins_dir=args.coins_dir, output_dir=args.output_dir
        )

        if args.recent_days:
            aggregator.update_recent_days(args.recent_days)
        elif args.start_date and args.end_date:
            aggregator.rebuild_date_range(args.start_date, args.end_date)
        elif args.rebuild_all:
            aggregator.rebuild_all_daily_files()
        else:
            # 默认更新最近7天
            aggregator.update_recent_days(7)

        logger.info("每日数据汇总完成")

    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"汇总过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
