#!/usr/bin/env python3
"""
超高性能每日数据重建脚本

专为高性能系统优化的重建脚本，采用以下优化策略：
1. 批量处理日期以减少线程创建开销
2. 内存池化预加载数据
3. 优化的I/O操作
4. 智能线程数配置
"""

import argparse
import logging
import os
import sys
import multiprocessing
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

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
            logging.FileHandler("logs/ultra_fast_rebuild.log"),
        ],
    )


class UltraFastDailyAggregator:
    """超高性能每日数据汇总器"""

    def __init__(
        self,
        coins_dir: str = "data/coins",
        output_dir: str = "data/daily/daily_files",
        batch_size: int = 100,  # 批处理大小
    ):
        """
        初始化汇总器

        Args:
            coins_dir: 币种数据目录
            output_dir: 输出目录
            batch_size: 批处理大小，每批处理多少天
        """
        self.coins_dir = Path(coins_dir)
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        self.logger = logging.getLogger(__name__)

        # 确保输出目录存在
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_coin_data_optimized(self, coin_id: str) -> Optional[pd.DataFrame]:
        """
        优化的币种数据加载
        """
        csv_path = self.coins_dir / f"{coin_id}.csv"
        if not csv_path.exists():
            return None

        try:
            # 读取数据并在后续处理中优化数据类型
            df = pd.read_csv(csv_path)
            # 预先计算日期，避免重复计算
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

    def _get_date_range_fast(self, coin_ids: List[str]) -> tuple[date, date]:
        """
        快速获取日期范围 - 使用采样方法
        """
        min_date = date.max
        max_date = date.min

        # 只采样一部分文件来确定日期范围，假设所有文件日期范围相似
        sample_size = min(50, len(coin_ids))
        sample_files = [
            self.coins_dir / f"{coin_ids[i]}.csv"
            for i in range(0, len(coin_ids), len(coin_ids) // sample_size)
        ]

        self.logger.info(f"快速扫描 {len(sample_files)} 个样本文件以确定日期范围...")

        for csv_file in tqdm(sample_files, desc="快速扫描日期范围"):
            try:
                # 只读取时间戳列
                df = pd.read_csv(
                    csv_file, usecols=["timestamp"], dtype={"timestamp": "int64"}
                )
                if df.empty:
                    continue

                # 快速获取最小和最大时间戳
                min_ts = df["timestamp"].min()
                max_ts = df["timestamp"].max()

                current_min = pd.to_datetime(min_ts, unit="ms").date()
                current_max = pd.to_datetime(max_ts, unit="ms").date()

                if current_min < min_date:
                    min_date = current_min
                if current_max > max_date:
                    max_date = current_max

            except Exception as e:
                self.logger.warning(f"快速扫描 {csv_file.stem} 失败: {e}")
                continue

        if min_date == date.max or max_date == date.min:
            raise ValueError("无法从样本文件中找到有效的日期范围")

        return min_date, max_date

    def _process_date_batch(
        self, date_batch: List[date], all_coin_data: Dict[str, pd.DataFrame]
    ) -> int:
        """
        批量处理多个日期

        Returns:
            成功处理的日期数量
        """
        success_count = 0

        for target_date in date_batch:
            try:
                # 汇总当日数据
                daily_df = self._aggregate_daily_data(target_date, all_coin_data)

                if not daily_df.empty:
                    if self._save_daily_file(daily_df, target_date):
                        success_count += 1
                else:
                    success_count += 1  # 即使没有数据也算成功处理

            except Exception as e:
                self.logger.error(f"批处理 {target_date} 失败: {e}")

        return success_count

    def _aggregate_daily_data(
        self, target_date: date, all_coin_data: Dict[str, pd.DataFrame]
    ) -> pd.DataFrame:
        """
        汇总指定日期的数据
        """
        daily_data = []

        for coin_id, df in all_coin_data.items():
            if df is None:
                continue

            try:
                # 使用向量化操作查找目标日期的数据
                target_mask = df["date"] == target_date
                target_data = df[target_mask]

                if target_data.empty:
                    continue

                # 取最新的记录
                latest_record = target_data.iloc[-1]

                # 快速有效性检查
                if (
                    pd.notna(latest_record["price"])
                    and latest_record["price"] > 0
                    and pd.notna(latest_record["market_cap"])
                    and latest_record["market_cap"] > 0
                ):
                    daily_data.append(
                        {
                            "timestamp": int(latest_record["timestamp"]),
                            "price": float(latest_record["price"]),
                            "volume": (
                                float(latest_record["volume"])
                                if pd.notna(latest_record["volume"])
                                else 0.0
                            ),
                            "market_cap": float(latest_record["market_cap"]),
                            "date": target_date.strftime("%Y-%m-%d"),
                            "coin_id": coin_id,
                        }
                    )

            except Exception as e:
                continue  # 静默跳过错误

        if not daily_data:
            return pd.DataFrame()

        # 使用高效的DataFrame创建
        df = pd.DataFrame(daily_data)

        # 优化排序和排名操作
        df = df.sort_values("market_cap", ascending=False, ignore_index=True)
        df["rank"] = range(1, len(df) + 1)

        # 重新排列列顺序
        df = df[
            ["timestamp", "price", "volume", "market_cap", "date", "coin_id", "rank"]
        ]

        return df

    def _save_daily_file(self, df: pd.DataFrame, target_date: date) -> bool:
        """
        优化的保存每日数据文件
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
            # 使用更高效的保存选项
            df.to_csv(
                filepath, index=False, float_format="%.6f", lineterminator="\n"
            )  # 使用Unix换行符
            return True
        except Exception as e:
            self.logger.error(f"保存 {target_date} 数据失败: {e}")
            return False

    def ultra_fast_rebuild(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        超高性能重建所有每日文件
        """
        self.logger.info("🚀 启动超高性能重建模式")

        # 获取所有币种
        coin_ids = self._get_all_coin_ids()
        self.logger.info(f"找到 {len(coin_ids)} 个币种")

        # 快速确定日期范围
        if start_date is None or end_date is None:
            auto_start, auto_end = self._get_date_range_fast(coin_ids)
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

        # 智能设置线程数
        if max_workers is None:
            # 对于高性能系统，I/O密集型任务可以使用更多线程
            cpu_count = multiprocessing.cpu_count()
            max_workers = min(cpu_count * 3, 64)  # 最多64个线程

        self.logger.info(f"使用 {max_workers} 个工作线程")

        # 预加载所有数据到内存
        self.logger.info("⚡ 预加载所有币种数据到内存...")
        all_coin_data = {}

        # 使用线程池预加载数据
        with ThreadPoolExecutor(max_workers=min(max_workers, 32)) as preload_executor:
            future_to_coin = {
                preload_executor.submit(
                    self._load_coin_data_optimized, coin_id
                ): coin_id
                for coin_id in coin_ids
            }

            for future in tqdm(
                as_completed(future_to_coin), total=len(coin_ids), desc="预加载数据"
            ):
                coin_id = future_to_coin[future]
                all_coin_data[coin_id] = future.result()

        # 将日期分批处理
        date_batches = [
            date_list[i : i + self.batch_size]
            for i in range(0, len(date_list), self.batch_size)
        ]

        self.logger.info(
            f"分为 {len(date_batches)} 个批次处理，每批 {self.batch_size} 天"
        )

        successful_days = 0

        # 批量并行处理
        with tqdm(total=total_days, desc="🔥 超高速重建每日数据") as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_batch = {
                    executor.submit(
                        self._process_date_batch, batch, all_coin_data
                    ): batch
                    for batch in date_batches
                }

                for future in as_completed(future_to_batch):
                    batch_success = future.result()
                    successful_days += batch_success
                    pbar.update(batch_success)

        self.logger.info("🎉 超高性能重建完成")
        self.logger.info(f"总处理天数: {total_days}")
        self.logger.info(f"成功生成文件: {successful_days}")
        self.logger.info(f"成功率: {successful_days/total_days*100:.1f}%")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="超高性能每日数据汇总脚本")

    parser.add_argument("--start-date", help="开始日期 (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="结束日期 (YYYY-MM-DD)")
    parser.add_argument("--coins-dir", default="data/coins", help="币种数据目录")
    parser.add_argument(
        "--output-dir", default="data/daily/daily_files", help="输出目录"
    )
    parser.add_argument("--batch-size", type=int, default=100, help="批处理大小")
    parser.add_argument("--max-workers", type=int, help="最大工作线程数")

    args = parser.parse_args()

    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # 创建汇总器
        aggregator = UltraFastDailyAggregator(
            coins_dir=args.coins_dir,
            output_dir=args.output_dir,
            batch_size=args.batch_size,
        )

        # 执行超高性能重建
        aggregator.ultra_fast_rebuild(
            start_date=args.start_date,
            end_date=args.end_date,
            max_workers=args.max_workers,
        )

        logger.info("✅ 超高性能每日数据汇总完成")

    except KeyboardInterrupt:
        logger.info("❌ 用户中断")
    except Exception as e:
        logger.error(f"💥 汇总过程中发生错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
