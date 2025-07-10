#!/usr/bin/env python3
"""
每日数据聚合器

基于已下载的历史数据，构建按日期组织的数据集合，
用于分析每日市场构成和构建历史指数。
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

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

        # 存储所有币种的数据
        self.coin_data: Dict[str, pd.DataFrame] = {}
        self.loaded_coins: List[str] = []

        # 日期范围信息
        self.min_date: Optional[datetime] = None
        self.max_date: Optional[datetime] = None

        # 每日数据缓存
        self.daily_cache: Dict[str, pd.DataFrame] = {}

        logger.info(f"初始化每日数据聚合器")
        logger.info(f"数据目录: {self.data_dir}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"每日文件目录: {self.daily_files_dir}")

    def load_coin_data(self) -> None:
        """加载所有币种的历史数据"""
        logger.info("开始加载币种数据...")

        csv_files = list(self.data_dir.glob("*.csv"))
        logger.info(f"发现 {len(csv_files)} 个数据文件")

        for csv_file in csv_files:
            coin_id = csv_file.stem  # 文件名去掉.csv扩展名

            try:
                # 读取CSV数据
                df = pd.read_csv(csv_file)

                # 验证数据格式
                required_columns = ["timestamp", "price", "volume", "market_cap"]
                if not all(col in df.columns for col in required_columns):
                    logger.warning(f"跳过 {coin_id}: 缺少必要列")
                    continue

                # 转换时间戳为日期
                df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date

                # 添加币种ID列
                df["coin_id"] = coin_id

                # 存储数据
                self.coin_data[coin_id] = df
                self.loaded_coins.append(coin_id)

                logger.info(f"加载 {coin_id}: {len(df)} 条记录")

            except Exception as e:
                logger.error(f"加载 {coin_id} 失败: {e}")
                continue

        logger.info(f"成功加载 {len(self.loaded_coins)} 个币种的数据")
        self._calculate_date_range()

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

    def _get_daily_file_path(self, date_str: str) -> Path:
        """获取每日数据文件的分层路径

        Args:
            date_str: 日期字符串 "YYYY-MM-DD"

        Returns:
            分层路径: daily_files/YYYY/MM/YYYY-MM-DD.csv
        """
        try:
            from datetime import datetime

            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            year = date_obj.strftime("%Y")
            month = date_obj.strftime("%m")

            # 创建年/月子目录
            year_month_dir = self.daily_files_dir / year / month
            year_month_dir.mkdir(parents=True, exist_ok=True)

            return year_month_dir / f"{date_str}.csv"
        except ValueError:
            logger.error(f"无效日期格式: {date_str}")
            # 回退到平铺结构
            return self.daily_files_dir / f"{date_str}.csv"

    def get_daily_data(self, target_date) -> pd.DataFrame:
        """获取指定日期的所有币种数据

        Args:
            target_date: 目标日期 (datetime.date 或 str 'YYYY-MM-DD')

        Returns:
            包含当日所有有数据币种的DataFrame
        """
        # 处理日期格式
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date, "%Y-%m-%d").date()

        date_str = str(target_date)

        # 首先尝试从缓存中获取
        if date_str in self.daily_cache:
            return self.daily_cache[date_str]

        # 尝试从已保存的文件中加载
        daily_file = self._get_daily_file_path(date_str)
        if daily_file.exists():
            try:
                daily_df = pd.read_csv(daily_file)
                # 转换date列的数据类型
                daily_df["date"] = pd.to_datetime(daily_df["date"]).dt.date
                self.daily_cache[date_str] = daily_df
                return daily_df
            except Exception as e:
                logger.warning(f"加载已保存的每日数据失败 {date_str}: {e}")

        # 如果没有保存的文件，从原始数据计算
        daily_records = []

        for coin_id, df in self.coin_data.items():
            # 查找该日期的数据
            date_data = df[df["date"] == target_date]

            if not date_data.empty:
                # 取第一条记录（应该只有一条）
                record = date_data.iloc[0]
                daily_records.append(
                    {
                        "date": target_date,
                        "coin_id": coin_id,
                        "price": record["price"],
                        "volume": record["volume"],
                        "market_cap": record["market_cap"],
                        "timestamp": record["timestamp"],
                    }
                )

        # 创建DataFrame并按市值排序
        if daily_records:
            daily_df = pd.DataFrame(daily_records)
            daily_df = daily_df.sort_values("market_cap", ascending=False)
            daily_df = daily_df.reset_index(drop=True)
            daily_df["rank"] = daily_df.index + 1

            # 缓存数据
            self.daily_cache[date_str] = daily_df
            return daily_df
        else:
            return pd.DataFrame()

    def build_daily_tables(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> None:
        """构建每日数据表集合

        Args:
            start_date: 开始日期 (默认为数据最早日期)
            end_date: 结束日期 (默认为数据最晚日期)
        """
        if not self.coin_data:
            logger.error("请先调用 load_coin_data() 加载数据")
            return

        # 确定日期范围
        start = self.min_date
        end = self.max_date

        if start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d").date()
        if end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d").date()

        # 检查日期有效性
        if not start or not end:
            logger.error("无法确定有效的日期范围")
            return

        logger.info(f"构建每日数据表: {start} 到 {end}")

        # 创建每日数据
        current_date = start
        daily_summary = []

        while current_date <= end:
            daily_data = self.get_daily_data(current_date)

            if not daily_data.empty:
                # 强制按市值排序并重新分配rank
                daily_data_sorted = daily_data.sort_values(
                    "market_cap", ascending=False
                ).reset_index(drop=True)
                daily_data_sorted["rank"] = daily_data_sorted.index + 1

                # 保存每日数据文件到分层目录
                daily_file = self._get_daily_file_path(str(current_date))
                daily_data_sorted.to_csv(daily_file, index=False)

                # 记录统计信息
                summary = {
                    "date": str(current_date),
                    "coin_count": len(daily_data_sorted),
                    "total_market_cap": daily_data_sorted["market_cap"].sum(),
                    "avg_price": daily_data_sorted["price"].mean(),
                    "total_volume": daily_data_sorted["volume"].sum(),
                }
                daily_summary.append(summary)

                if len(daily_summary) % 100 == 0:
                    logger.info(f"已处理 {len(daily_summary)} 天数据...")

            current_date += timedelta(days=1)

        # 保存总结报告
        summary_df = pd.DataFrame(daily_summary)
        summary_file = self.output_dir / "daily_summary.csv"
        summary_df.to_csv(summary_file, index=False)

        logger.info(f"完成! 生成了 {len(daily_summary)} 天的数据表")
        logger.info(f"数据保存在: {self.output_dir}")

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
        """获取所有可用的每日数据日期"""
        dates = []

        # 扫描分层结构: YYYY/MM/*.csv
        for year_dir in self.daily_files_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir() and month_dir.name.isdigit():
                        csv_files = list(month_dir.glob("*.csv"))
                        dates.extend([f.stem for f in csv_files])

        # 同时支持平铺结构的文件
        csv_files = list(self.daily_files_dir.glob("*.csv"))
        dates.extend([f.stem for f in csv_files])

        # 去重并排序
        dates = list(set(dates))
        dates.sort()
        return dates

    def save_daily_data(self, target_date, daily_data: pd.DataFrame) -> None:
        """保存单日数据到文件

        Args:
            target_date: 目标日期
            daily_data: 当日数据DataFrame
        """
        if isinstance(target_date, str):
            date_str = target_date
        else:
            date_str = str(target_date)

        if not daily_data.empty:
            # 强制按市值排序并重新分配rank
            daily_data_sorted = daily_data.sort_values(
                "market_cap", ascending=False
            ).reset_index(drop=True)
            daily_data_sorted["rank"] = daily_data_sorted.index + 1

            daily_file = self._get_daily_file_path(date_str)
            daily_data_sorted.to_csv(daily_file, index=False)
            self.daily_cache[date_str] = daily_data_sorted
            logger.info(
                f"保存每日数据: {date_str} ({len(daily_data_sorted)} 个币种，已按市值排序)"
            )

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
    btc_start = aggregator.find_bitcoin_start_date()
    print(f"- Bitcoin最早日期: {btc_start}")

    # 测试获取某一天的数据
    if btc_start:
        test_data = aggregator.get_daily_data(btc_start)
        print(f"- {btc_start} 当天有 {len(test_data)} 个币种有数据")
        if not test_data.empty:
            print(f"- 前3名币种: {test_data.head(3)['coin_id'].tolist()}")
