"""
数据质量检查核心模块

提供数据质量分析、验证和修复功能的核心实现。
scripts/data_quality_checker.py 是此模块的用户接口封装。
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional

import pandas as pd

logger = logging.getLogger(__name__)


class DataQualityAnalyzer:
    """数据质量分析器核心类"""

    def __init__(self, data_dir: str = "data/coins"):
        self.data_dir = data_dir
        self.min_rows = 100
        self.max_days_old = 2
        self.min_data_span_days = 30

    def _is_data_recent(self, data_span_days: int, days_since_latest: int) -> bool:
        """判断数据是否"最新"

        新币种给予更宽松的标准，老币种使用严格标准
        """
        if data_span_days < self.min_data_span_days:
            return days_since_latest <= 7  # 新币种
        else:
            return days_since_latest <= self.max_days_old  # 老币种

    def check_timestamp_intervals(
        self, df: pd.DataFrame, time_column: str
    ) -> Tuple[bool, str]:
        """检查时间戳间隔是否合理"""
        try:
            if time_column == "timestamp":
                dates = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            else:
                dates = pd.to_datetime(df[time_column]).dt.date

            unique_dates = sorted(set(dates))
            if len(unique_dates) < 2:
                return True, "数据点太少，无法检查间隔"

            # 检查超过7天的缺失
            large_gaps = []
            for i in range(1, len(unique_dates)):
                gap_days = (unique_dates[i] - unique_dates[i - 1]).days
                if gap_days > 7:
                    large_gaps.append(
                        f"{unique_dates[i-1]} -> {unique_dates[i]} ({gap_days}天)"
                    )

            if large_gaps:
                gap_info = "; ".join(large_gaps[:3])
                if len(large_gaps) > 3:
                    gap_info += f" 等{len(large_gaps)}个缺失"
                return False, f"发现大时间缺失: {gap_info}"

            return True, "时间间隔正常"

        except Exception as e:
            return True, f"时间间隔检查失败: {str(e)}"

    def analyze_file_quality(self, filepath: str) -> Dict:
        """分析单个文件的数据质量"""
        try:
            df = pd.read_csv(filepath)
            row_count = len(df)

            # 检查时间列
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                latest_date = df["date"].max()
                earliest_date = df["date"].min()
                interval_ok, interval_msg = self.check_timestamp_intervals(df, "date")
            elif "timestamp" in df.columns:
                df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
                latest_date = df["datetime"].dt.date.max()
                earliest_date = df["datetime"].dt.date.min()
                interval_ok, interval_msg = self.check_timestamp_intervals(
                    df, "timestamp"
                )
            else:
                return {
                    "rows": row_count,
                    "latest_date": None,
                    "earliest_date": None,
                    "data_span_days": 0,
                    "days_since_latest": 999,
                    "is_recent": False,
                    "has_enough_data": row_count >= self.min_rows,
                    "interval_ok": False,
                    "interval_msg": "无时间列",
                }

            data_span_days = (latest_date - earliest_date).days
            days_since_latest = (datetime.now().date() - latest_date).days

            return {
                "rows": row_count,
                "latest_date": latest_date,
                "earliest_date": earliest_date,
                "data_span_days": data_span_days,
                "days_since_latest": days_since_latest,
                "is_recent": self._is_data_recent(data_span_days, days_since_latest),
                "has_enough_data": row_count >= self.min_rows,
                "interval_ok": interval_ok,
                "interval_msg": interval_msg,
            }

        except Exception as e:
            return {
                "error": str(e),
                "rows": 0,
                "is_recent": False,
                "has_enough_data": False,
                "interval_ok": False,
                "interval_msg": f"检查失败: {str(e)}",
            }

    def scan_all_files(self) -> Tuple[List[Tuple], List[Tuple]]:
        """扫描所有文件并分类

        Returns:
            tuple: (good_files, problematic_files)
        """
        if not os.path.exists(self.data_dir):
            raise FileNotFoundError(f"数据目录不存在: {self.data_dir}")

        files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv")]
        good_files = []
        problematic_files = []

        for filename in files:
            filepath = os.path.join(self.data_dir, filename)
            coin_name = filename[:-4]
            quality = self.analyze_file_quality(filepath)

            if "error" in quality:
                problematic_files.append((coin_name, quality, "READ_ERROR"))
            elif not quality["has_enough_data"]:
                problematic_files.append((coin_name, quality, "INSUFFICIENT_DATA"))
            elif not quality["interval_ok"]:
                problematic_files.append((coin_name, quality, "INTERVAL_ISSUE"))
            elif not quality["is_recent"]:
                problematic_files.append((coin_name, quality, "OUTDATED_DATA"))
            else:
                good_files.append((coin_name, quality))

        return good_files, problematic_files


class DataQualityRepairer:
    """数据质量修复器"""

    def __init__(self, analyzer: DataQualityAnalyzer):
        self.analyzer = analyzer
        self._updater = None

    def _get_updater(self):
        """延迟初始化updater"""
        if self._updater is None:
            from src.updaters.price_updater import PriceDataUpdater

            self._updater = PriceDataUpdater()
        return self._updater

    def repair_files(
        self, problematic_files: List[Tuple], dry_run: bool = True
    ) -> List[Dict]:
        """修复有问题的文件

        Args:
            problematic_files: 问题文件列表
            dry_run: 是否仅模拟运行

        Returns:
            修复结果列表
        """
        results = []

        for coin_name, quality, issue_type in problematic_files:
            result = {
                "coin_name": coin_name,
                "issue_type": issue_type,
                "success": False,
                "message": "",
            }

            if dry_run:
                result["message"] = "DRY RUN: 将重新下载完整历史数据"
                result["success"] = True
            else:
                try:
                    success, api_called = self._get_updater().download_coin_data(
                        coin_name
                    )
                    if success:
                        # 重新检查质量
                        filepath = os.path.join(
                            self.analyzer.data_dir, f"{coin_name}.csv"
                        )
                        new_quality = self.analyzer.analyze_file_quality(filepath)
                        result["success"] = True
                        result["message"] = (
                            f"修复成功: {new_quality['rows']}行, 最新:{new_quality['latest_date']}"
                        )
                    else:
                        result["message"] = "重新下载失败"
                except Exception as e:
                    result["message"] = f"修复错误: {e}"

            results.append(result)

        return results
