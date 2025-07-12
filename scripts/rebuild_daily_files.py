#!/usr/bin/env python3
"""
每日数据汇总脚本

基于 coins/ 目录中的价格数据重新生成 daily_files/ 汇总数据
"""

import argparse
import logging
import multiprocessing
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from tqdm import tqdm

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
        min_date = date.max
        max_date = date.min

        self.logger.info("扫描所有币种以确定完整日期范围...")
        all_coin_files = list(self.coins_dir.glob("*.csv"))

        # 使用tqdm显示进度条
        for csv_file in tqdm(all_coin_files, desc="扫描日期范围"):
            try:
                # 优化：只读取首尾行来快速确定日期范围
                # 这里为了简单和准确，还是读取整个文件
                df = pd.read_csv(csv_file, usecols=["timestamp"])
                if df.empty:
                    continue

                # 转换时间戳为日期
                timestamps = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
                timestamps = timestamps.dropna()
                if timestamps.empty:
                    continue

                current_min = timestamps.min().date()
                current_max = timestamps.max().date()

                if current_min < min_date:
                    min_date = current_min
                if current_max > max_date:
                    max_date = current_max
            except Exception as e:
                self.logger.warning(f"读取或处理 {csv_file.stem} 日期失败: {e}")
                continue

        if min_date == date.max or max_date == date.min:
            raise ValueError("无法从任何币种文件中找到有效的日期范围")

        return min_date, max_date

    def _aggregate_daily_data(
        self, target_date: date, all_coin_data: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        汇总指定日期的数据

        Args:
            target_date: 目标日期
            all_coin_data: 预加载的所有币种数据

        Returns:
            当日汇总数据DataFrame
        """
        daily_data = []

        for coin_id, df in all_coin_data.items():
            if df is None:
                continue

            try:
                # 找到目标日期的数据
                target_data = df[df["date"] == target_date]
                if target_data.empty:
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
                    continue

                daily_data.append(
                    {
                        "timestamp": int(latest_record["timestamp"]),
                        "price": float(latest_record["price"]),
                        "volume": (
                            float(latest_record["volume"])
                            if "volume" in latest_record
                            and pd.notna(latest_record["volume"])
                            else 0.0
                        ),
                        "market_cap": float(latest_record["market_cap"]),
                        "date": target_date.strftime("%Y-%m-%d"),
                        "coin_id": coin_id,
                    }
                )
            except Exception as e:
                self.logger.warning(f"处理 {coin_id} 在 {target_date} 的数据失败: {e}")
                continue

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
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        parallel: bool = True,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        重建所有每日文件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)，None表示自动检测
            end_date: 结束日期 (YYYY-MM-DD)，None表示自动检测
            parallel: 是否使用并行处理
            max_workers: 最大工作进程数，默认为(CPU核心数-1)
        """
        self.logger.info("开始重建每日汇总数据")
        self.logger.info(f"并行处理: {'启用' if parallel else '禁用'}")

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

        # 生成需要处理的日期列表
        date_list = [
            actual_start + timedelta(days=x)
            for x in range((actual_end - actual_start).days + 1)
        ]

        total_days = len(date_list)
        self.logger.info(f"将处理 {total_days} 天的数据")

        successful_days = 0

        # 预加载所有数据以提高并行效率
        self.logger.info("预加载所有币种数据到内存...")
        all_coin_data = {}
        for coin_id in tqdm(coin_ids, desc="预加载数据"):
            all_coin_data[coin_id] = self._load_coin_data(coin_id)

        if parallel and total_days > 1:
            # 设置工作线程数 - 对于强劲系统，可以超过CPU核心数
            if max_workers is None:
                max_workers = max(
                    1, multiprocessing.cpu_count() * 2
                )  # 使用2倍CPU核心数

            self.logger.info(f"使用 {max_workers} 个工作线程并行处理")

            with tqdm(total=total_days, desc="重建每日数据") as pbar:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(
                            self._process_single_day, process_date, all_coin_data
                        ): process_date
                        for process_date in date_list
                    }

                    for future in as_completed(futures):
                        if future.result():
                            successful_days += 1
                        pbar.update(1)
        else:
            # 使用单线程处理
            self.logger.info("使用单线程处理")
            for process_date in tqdm(date_list, desc="重建每日数据"):
                if self._process_single_day(process_date, all_coin_data):
                    successful_days += 1

        self.logger.info("重建完成")
        self.logger.info(f"总处理天数: {total_days}")
        self.logger.info(f"成功生成文件: {successful_days}")
        self.logger.info(f"成功率: {successful_days/total_days*100:.1f}%")

    def _process_single_day(
        self, target_date: date, all_coin_data: Dict[str, pd.DataFrame]
    ) -> bool:
        """
        处理单一天的数据，用于并行执行

        Args:
            target_date: 目标日期
            all_coin_data: 预加载的所有币种数据

        Returns:
            是否成功处理
        """
        try:
            # 汇总当日数据
            daily_df = self._aggregate_daily_data(target_date, all_coin_data)

            if not daily_df.empty:
                # 保存文件
                return self._save_daily_file(daily_df, target_date)
            return True  # 即使没有数据也算成功处理
        except Exception as e:
            self.logger.error(f"处理 {target_date} 失败: {e}")
            return False

    def rebuild_date_range(
        self,
        start_date: str,
        end_date: str,
        parallel: bool = True,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        重建指定日期范围的文件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            parallel: 是否使用并行处理
            max_workers: 最大工作进程数
        """
        self.rebuild_all_daily_files(start_date, end_date, parallel, max_workers)

    def update_recent_days(
        self, days: int = 30, parallel: bool = True, max_workers: Optional[int] = None
    ) -> None:
        """
        更新最近N天的数据

        Args:
            days: 天数
            parallel: 是否使用并行处理
            max_workers: 最大工作进程数
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=days - 1)

        self.logger.info(f"更新最近 {days} 天的数据")
        self.rebuild_all_daily_files(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
            parallel,
            max_workers,
        )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="每日数据汇总脚本")

    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument(
        "--recent-days",
        type=int,
        default=7,
        help="更新最近N天 (默认: 7), 设置为0则不使用此模式",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="完整重建所有历史数据，覆盖其他时间选项",
    )
    parser.add_argument("--coins-dir", default="data/coins", help="币种数据目录")
    parser.add_argument(
        "--output-dir", default="data/daily/daily_files", help="输出目录"
    )
    parser.add_argument("--no-parallel", action="store_true", help="禁用并行处理")
    parser.add_argument(
        "--max-workers", type=int, help="最大工作线程数，默认为(CPU核心数-1)"
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

        # 并行处理配置
        parallel = not args.no_parallel
        max_workers = args.max_workers

        logger.info(f"并行处理: {'禁用' if args.no_parallel else '启用'}")
        if max_workers:
            logger.info(f"最大工作线程数: {max_workers}")

        if args.full_rebuild:
            logger.info("模式: 完整重建所有历史数据")
            aggregator.rebuild_all_daily_files(
                parallel=parallel, max_workers=max_workers
            )
        elif args.start_date and args.end_date:
            logger.info(f"模式: 重建指定日期范围 {args.start_date} 到 {args.end_date}")
            aggregator.rebuild_date_range(
                args.start_date,
                args.end_date,
                parallel=parallel,
                max_workers=max_workers,
            )
        elif args.recent_days and args.recent_days > 0:
            logger.info(f"模式: 更新最近 {args.recent_days} 天数据")
            aggregator.update_recent_days(
                args.recent_days, parallel=parallel, max_workers=max_workers
            )
        else:
            logger.warning("未指定有效操作模式，脚本退出。请使用 --help 查看选项。")

        logger.info("每日数据汇总完成")

    except KeyboardInterrupt:
        logger.info("用户中断")
    except Exception as e:
        logger.error(f"汇总过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
