#!/usr/bin/env python3
"""
每日数据聚合器

基于已下载的历史数据，构建按日期组织的数据集合，
用于分析每日市场构成和构建历史指数。
"""

import logging
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Union
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DailyDataAggregator:
    """每日数据聚合器

    功能：
    1. 扫描所有已下载的币种数据
    2. 按日期聚合数据，构建每日市场快照
    3. 提供查询指定日期的市场数据功能
    4. 分析数据覆盖范围和质量
    """

    def __init__(self, data_dir: str = "data/coins", output_dir: str = "data/daily"):
        """初始化聚合器

        Args:
            data_dir: 原始CSV数据目录
            output_dir: 聚合后数据输出目录
        """
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # 创建子目录用于存储不同类型的数据
        self.daily_files_dir = self.output_dir / "daily_files"
        self.daily_files_dir.mkdir(parents=True, exist_ok=True)

        # 缓存
        self.daily_cache: Dict[str, pd.DataFrame] = {}
        self.coin_data: Dict[str, pd.DataFrame] = {}
        self.loaded_coins: List[str] = []
        logger.info(
            f"每日数据聚合器初始化, 数据源: '{data_dir}', 输出到: '{output_dir}'"
        )

        # 日期范围信息
        self.min_date: Optional[datetime] = None
        self.max_date: Optional[datetime] = None

        logger.info(f"初始化每日数据聚合器")
        logger.info(f"数据目录: {self.data_dir}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"每日文件目录: {self.daily_files_dir}")

    def load_coin_data(self) -> None:
        """加载所有币种的CSV数据到内存"""
        logger.info("开始从CSV文件加载所有币种数据到内存...")
        csv_files = list(self.data_dir.glob("*.csv"))
        if not csv_files:
            logger.warning(f"数据目录 '{self.data_dir}' 中没有找到CSV文件。")
            return

        for file_path in csv_files:
            coin_id = file_path.stem
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    logger.warning(f"跳过空文件: {file_path}")
                    continue

                # 转换时间戳并创建 'date' 列
                df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
                df.dropna(subset=["timestamp"], inplace=True)
                df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime(
                    "%Y-%m-%d"
                )
                df["coin_id"] = coin_id
                self.coin_data[coin_id] = df
                self.loaded_coins.append(coin_id)
                logger.debug(f"成功加载 {coin_id} ({len(df)}条记录)")
            except Exception as e:
                logger.error(f"加载文件 {file_path} 失败: {e}")

        logger.info(f"成功加载 {len(self.loaded_coins)} 个币种的数据。")

    def _calculate_date_range(self) -> None:
        """计算所有数据的日期范围"""
        if not self.coin_data:
            return

        all_dates = []
        for df in self.coin_data.values():
            all_dates.extend(df["date"].tolist())

        if all_dates:
            self.min_date = min(all_dates)
            self.max_date = max(all_dates)

            logger.info(f"数据日期范围: {self.min_date} 到 {self.max_date}")

            # 计算总天数
            if self.min_date and self.max_date:
                total_days = (self.max_date - self.min_date).days + 1
                logger.info(f"总共 {total_days} 天的数据")

    def get_daily_data(
        self, target_date: Union[str, datetime, date], force_refresh: bool = False
    ) -> pd.DataFrame:
        """
        获取指定日期的聚合市场数据

        支持从缓存、文件或重新计算获取。

        Args:
            target_date: 目标日期，支持字符串、datetime或date类型
            force_refresh: 是否强制刷新，忽略缓存

        Returns:
            包含指定日期所有币种数据的DataFrame
        """
        if isinstance(target_date, str):
            try:
                target_date_dt = datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError:
                logger.error(f"无效的日期格式: {target_date}，应为 YYYY-MM-DD")
                return pd.DataFrame()
        elif isinstance(target_date, datetime):
            target_date_dt = target_date
        elif isinstance(target_date, date):
            # 将 date 对象转换为 datetime 对象，时间设为午夜
            target_date_dt = datetime.combine(target_date, datetime.min.time())
        else:
            logger.error(f"不支持的日期类型: {type(target_date)}")
            return pd.DataFrame()

        target_date_str = target_date_dt.strftime("%Y-%m-%d")

        # 检查内存缓存
        if not force_refresh and target_date_str in self.daily_cache:
            logger.info(f"从内存缓存加载 {target_date_str} 的数据")
            return self.daily_cache[target_date_str]

        # 检查是否有缓存文件
        daily_file_path = self._get_daily_file_path(target_date_dt.date())

        if not force_refresh and daily_file_path.exists():
            logger.info(f"从缓存文件加载 {target_date_str} 的数据: {daily_file_path}")
            try:
                df = pd.read_csv(daily_file_path)
                # 确保 'date' 列是 datetime 对象以便进行比较
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"]).dt.date
                self.daily_cache[target_date_str] = df  # 更新缓存
                return df
            except Exception as e:
                logger.warning(f"读取缓存文件 {daily_file_path} 失败，将重新计算: {e}")

        # 如果需要，加载币种数据
        if not self.coin_data:
            logger.info("内存中无币种数据，开始加载...")
            self.load_coin_data()

        logger.info(
            f"开始为 {target_date_str} 计算每日数据 (强制刷新: {force_refresh})"
        )
        daily_df = self._compute_daily_data(target_date_dt.date())

        # 保存到文件和缓存
        if not daily_df.empty:
            daily_df.to_csv(daily_file_path, index=False)
            logger.info(f"已将 {target_date_str} 的数据保存到 {daily_file_path}")
            self.daily_cache[target_date_str] = daily_df

        return daily_df

    def build_daily_tables(self, force_recalculate: bool = False) -> None:
        """构建每日数据表集合

        Args:
            force_recalculate: 是否强制重新计算所有数据，忽略缓存文件
        """
        if not self.coin_data:
            logger.error("请先调用 load_coin_data() 加载数据")
            return

        # 确定日期范围
        self._calculate_date_range()
        start_date = self.min_date
        end_date = self.max_date

        # 检查日期有效性
        if not start_date or not end_date:
            logger.error("无法确定有效的日期范围")
            return

        logger.info(f"构建每日数据表: {start_date} 到 {end_date}")

        # 生成需要处理的日期列表
        date_list = []
        current_date = (
            datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(start_date, str)
            else start_date.date()
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if isinstance(end_date, str)
            else end_date.date()
        )

        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)

        logger.info(f"将处理 {len(date_list)} 天的数据")

        # 使用并行处理
        all_daily_data = []
        with ProcessPoolExecutor(
            max_workers=max(1, multiprocessing.cpu_count() - 1)
        ) as executor:
            # 提交所有任务
            future_to_date = {
                executor.submit(self.get_daily_data, date, force_recalculate): date
                for date in date_list
            }

            # 收集结果
            completed = 0
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    daily_data = future.result()
                    if not daily_data.empty:
                        all_daily_data.append(daily_data)
                except Exception as e:
                    logger.error(f"处理日期 {date} 时出错: {e}")

                completed += 1
                if completed % 10 == 0:
                    logger.info(f"已完成 {completed}/{len(date_list)} 天的数据处理")

        logger.info(f"成功处理 {len(all_daily_data)} 天的数据")

        # 合并所有每日数据到一个DataFrame
        if all_daily_data:
            merged_daily_data = pd.concat(all_daily_data, ignore_index=True)

            # 强制按市值排序并重新分配rank
            merged_daily_data = merged_daily_data.sort_values(
                "market_cap", ascending=False
            ).reset_index(drop=True)
            merged_daily_data["rank"] = merged_daily_data.index + 1

            # 保存合并后的数据到文件
            merged_daily_file = self.output_dir / "merged_daily_data.csv"
            merged_daily_data.to_csv(merged_daily_file, index=False)
            logger.info(f"已保存合并后的每日数据到文件: {merged_daily_file}")

    def get_data_coverage_analysis(self) -> Dict:
        """分析数据覆盖情况"""
        if not self.coin_data:
            return {}

        analysis = {
            "total_coins": len(self.loaded_coins),
            "date_range": {
                "start": str(self.min_date) if self.min_date else None,
                "end": str(self.max_date) if self.max_date else None,
                "total_days": (
                    (self.max_date - self.min_date).days + 1
                    if self.min_date and self.max_date
                    else 0
                ),
            },
            "coin_details": [],
        }

        for coin_id, df in self.coin_data.items():
            coin_start = df["date"].min()
            coin_end = df["date"].max()
            coin_days = len(df)

            analysis["coin_details"].append(
                {
                    "coin_id": coin_id,
                    "start_date": str(coin_start),
                    "end_date": str(coin_end),
                    "data_points": coin_days,
                    "market_cap_avg": df["market_cap"].mean(),
                }
            )

        # 按数据点数量排序
        analysis["coin_details"].sort(key=lambda x: x["data_points"], reverse=True)

        return analysis

    def find_bitcoin_start_date(self) -> Optional[str]:
        """找到Bitcoin数据的最早日期"""
        if "bitcoin" in self.coin_data:
            btc_data = self.coin_data["bitcoin"]
            return str(btc_data["date"].min())
        return None

    def load_daily_data_from_files(self) -> None:
        """从已保存的每日数据文件中加载数据"""
        logger.info("从已保存的文件中加载每日数据...")

        csv_files = []

        # 扫描分层结构
        for year_dir in self.daily_files_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir() and month_dir.name.isdigit():
                        csv_files.extend(list(month_dir.glob("*.csv")))

        # 同时支持平铺结构
        csv_files.extend(list(self.daily_files_dir.glob("*.csv")))

        logger.info(f"发现 {len(csv_files)} 个每日数据文件")

        for csv_file in csv_files:
            date_str = csv_file.stem  # 文件名就是日期
            try:
                daily_df = pd.read_csv(csv_file)
                # 转换date列的数据类型
                daily_df["date"] = pd.to_datetime(daily_df["date"]).dt.date
                self.daily_cache[date_str] = daily_df
            except Exception as e:
                logger.error(f"加载每日数据文件失败 {date_str}: {e}")

        logger.info(f"成功加载 {len(self.daily_cache)} 天的每日数据")

    def get_available_daily_dates(self) -> List[str]:
        """获取所有已生成每日数据文件的日期"""
        dates = []
        if not self.daily_files_dir.exists():
            return dates

        for year_dir in self.daily_files_dir.iterdir():
            if year_dir.is_dir():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir():
                        for file in month_dir.glob("*.csv"):
                            dates.append(file.stem)

        return sorted(dates)

    def get_date_range_summary(self) -> Dict:
        """获取数据日期范围的摘要信息"""
        available_dates = self.get_available_daily_dates()

        if not available_dates:
            return {
                "total_days": 0,
                "start_date": None,
                "end_date": None,
                "coverage": 0.0,
            }

        start_date = available_dates[0]
        end_date = available_dates[-1]

        # 计算理论总天数
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        theoretical_days = (end_dt - start_dt).days + 1

        # 计算覆盖率
        coverage = (
            len(available_dates) / theoretical_days if theoretical_days > 0 else 0.0
        )

        return {
            "total_days": len(available_dates),
            "theoretical_days": theoretical_days,
            "start_date": start_date,
            "end_date": end_date,
            "coverage": coverage,
        }

    def _get_daily_file_path(self, target_date: date) -> Path:
        """根据日期获取每日数据文件的路径"""
        # 确保输入是 date 对象
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        date_str = target_date.strftime("%Y-%m-%d")
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")

        # 创建年月目录
        year_month_dir = self.daily_files_dir / year / month
        year_month_dir.mkdir(parents=True, exist_ok=True)

        return year_month_dir / f"{date_str}.csv"

    def _compute_daily_data(self, target_date: date) -> pd.DataFrame:
        """在内存中计算指定日期的聚合数据"""
        daily_records = []
        target_date_str = target_date.strftime("%Y-%m-%d")
        logger.info(
            f"开始计算 {target_date_str} 的数据，遍历 {len(self.coin_data)} 个已加载的币种..."
        )

        # 如果币种数量足够多，使用并行处理
        if len(self.coin_data) > 100:
            # 使用并行处理，每个进程处理一部分币种
            with ProcessPoolExecutor(
                max_workers=max(1, multiprocessing.cpu_count() - 1)
            ) as executor:
                # 准备任务，每个任务处理一批币种
                coin_batches = self._split_coins_into_batches(
                    list(self.coin_data.items()), 10
                )
                futures = []

                for batch in coin_batches:
                    futures.append(
                        executor.submit(
                            self._process_coin_batch, batch, target_date_str
                        )
                    )

                # 收集结果
                for future in as_completed(futures):
                    try:
                        batch_results = future.result()
                        daily_records.extend(batch_results)
                    except Exception as e:
                        logger.error(f"处理币种批次时出错: {e}")
        else:
            # 币种数量较少，使用单线程处理
            for coin_id, df in self.coin_data.items():
                if df.empty:
                    continue

                # 筛选特定日期的数据
                day_data = df[df["date"] == target_date_str]

                if not day_data.empty:
                    # 通常每天只有一个记录，但为防万一，取第一个
                    record = day_data.iloc[0].to_dict()
                    daily_records.append(record)
                    logger.debug(f"找到 {coin_id} 在 {target_date_str} 的数据。")
                else:
                    logger.debug(f"未找到 {coin_id} 在 {target_date_str} 的数据。")

        if not daily_records:
            logger.warning(f"在 {target_date_str} 未找到任何币种的数据。")
            return pd.DataFrame()

        # 转换为DataFrame并排序
        final_df = pd.DataFrame(daily_records)
        logger.info(f"为 {target_date_str} 聚合了 {len(final_df)} 个币种的数据。")

        # 添加排名
        if "market_cap" in final_df.columns:
            final_df = final_df.sort_values("market_cap", ascending=False)
            final_df = final_df.reset_index(drop=True)
            final_df["rank"] = final_df.index + 1

        return final_df

    @staticmethod
    def _split_coins_into_batches(coins, batch_size):
        """将币种列表分割成多个批次进行并行处理"""
        for i in range(0, len(coins), batch_size):
            yield coins[i : i + batch_size]

    @staticmethod
    def _process_coin_batch(coin_batch, target_date_str):
        """处理一批币种的数据，用于并行执行"""
        batch_results = []
        for coin_id, df in coin_batch:
            if df.empty:
                continue

            # 筛选特定日期的数据
            day_data = df[df["date"] == target_date_str]

            if not day_data.empty:
                record = day_data.iloc[0].to_dict()
                batch_results.append(record)

        return batch_results


def create_daily_aggregator(
    data_dir: str = "data/coins", output_dir: str = "data/daily"
) -> DailyDataAggregator:
    """创建每日数据聚合器实例

    Args:
        data_dir: 原始数据目录
        output_dir: 输出目录

    Returns:
        DailyDataAggregator实例
    """
    return DailyDataAggregator(data_dir, output_dir)


if __name__ == "__main__":
    # 测试代码
    aggregator = create_daily_aggregator()

    # 加载数据
    aggregator.load_coin_data()

    # 分析数据覆盖情况
    coverage = aggregator.get_data_coverage_analysis()
    print(f"数据覆盖分析:")
    print(f"- 总币种数: {coverage['total_coins']}")
    print(
        f"- 日期范围: {coverage['date_range']['start']} 到 {coverage['date_range']['end']}"
    )
    print(f"- 总天数: {coverage['date_range']['total_days']}")

    # 找到Bitcoin开始日期
    btc_start_str = aggregator.find_bitcoin_start_date()
    print(f"- Bitcoin最早日期: {btc_start_str}")

    # 测试获取某一天的数据
    if btc_start_str:
        test_data = aggregator.get_daily_data(btc_start_str)
        print(f"- {btc_start_str} 当天有 {len(test_data)} 个币种有数据")
        if not test_data.empty:
            print(f"- 前3名币种: {test_data.head(3)['coin_id'].tolist()}")
