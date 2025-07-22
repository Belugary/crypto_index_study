"""
重构后的数据质量检查模块

将原有的 DataQualityAnalyzer 拆分为：
1. DataQualityChecker - 纯检查功能
2. DataQualityConfig - 配置管理
3. 工具函数 - 独立的检查逻辑
"""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd

from .data_quality_config import DataQualityConfig, ResolvedPaths

logger = logging.getLogger(__name__)


class DataQualityChecker:
    """数据质量检查器（纯检查，不做修复）"""

    def __init__(self, config: Optional[DataQualityConfig] = None):
        """
        初始化数据质量检查器
        
        Args:
            config: 数据质量配置，默认使用标准配置
        """
        self.config = config or DataQualityConfig()
        self.paths = self.config.resolve_paths()
        
        # 🚀 初始化数据库支持的数据聚合器（如果启用）
        self.daily_aggregator = None
        if self.config.use_database:
            try:
                from ..downloaders.daily_aggregator import DailyDataAggregator
                self.daily_aggregator = DailyDataAggregator(
                    data_dir=str(self.paths.data_dir),
                    output_dir=str(self.paths.daily_dir),
                    use_database=True
                )
            except ImportError:
                logger.warning("数据库模块不可用，将使用文件模式")

    def check_file_quality(self, filepath: Path) -> Dict:
        """检查单个文件的数据质量"""
        return analyze_file_quality(
            filepath, 
            self.config.min_rows,
            self.config.max_days_old,
            self.config.min_data_span_days
        )

    def scan_all_files(self) -> Tuple[List[Tuple], List[Tuple]]:
        """扫描所有文件并分类

        Returns:
            tuple: (good_files, problematic_files)
        """
        if not self.paths.data_dir.exists():
            raise FileNotFoundError(f"数据目录不存在: {self.paths.data_dir}")

        files = [f for f in self.paths.data_dir.iterdir() if f.suffix == ".csv"]
        good_files = []
        problematic_files = []

        for filepath in files:
            coin_name = filepath.stem
            quality = self.check_file_quality(filepath)

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

    def get_quality_summary(self) -> Dict:
        """获取数据质量摘要"""
        good_files, problematic_files = self.scan_all_files()
        
        # 按问题类型分组
        issue_counts = {}
        for _, _, issue_type in problematic_files:
            issue_counts[issue_type] = issue_counts.get(issue_type, 0) + 1
        
        return {
            "total_files": len(good_files) + len(problematic_files),
            "good_files": len(good_files),
            "problematic_files": len(problematic_files),
            "issue_breakdown": issue_counts,
            "good_files_list": [name for name, _ in good_files],
            "problematic_files_list": [(name, issue) for name, _, issue in problematic_files]
        }


# 工具函数 - 独立的检查逻辑

def is_data_recent(data_span_days: int, days_since_latest: int, 
                  min_data_span_days: int, max_days_old: int) -> bool:
    """判断数据是否"最新"

    新币种给予更宽松的标准，老币种使用严格标准
    """
    if data_span_days < min_data_span_days:
        return days_since_latest <= 7  # 新币种
    else:
        return days_since_latest <= max_days_old  # 老币种


def check_timestamp_intervals(df: pd.DataFrame, time_column: str) -> Tuple[bool, str]:
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


def analyze_file_quality(filepath: Path, min_rows: int, max_days_old: int, 
                        min_data_span_days: int) -> Dict:
    """分析单个文件的数据质量"""
    try:
        df = pd.read_csv(filepath)
        row_count = len(df)

        # 检查时间列
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
            latest_date = df["date"].max()
            earliest_date = df["date"].min()
            interval_ok, interval_msg = check_timestamp_intervals(df, "date")
        elif "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
            latest_date = df["datetime"].dt.date.max()
            earliest_date = df["datetime"].dt.date.min()
            interval_ok, interval_msg = check_timestamp_intervals(df, "timestamp")
        else:
            return {
                "rows": row_count,
                "latest_date": None,
                "earliest_date": None,
                "data_span_days": 0,
                "days_since_latest": 999,
                "is_recent": False,
                "has_enough_data": row_count >= min_rows,
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
            "is_recent": is_data_recent(data_span_days, days_since_latest, 
                                      min_data_span_days, max_days_old),
            "has_enough_data": row_count >= min_rows,
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


# 便捷函数

def create_data_quality_checker(data_dir: str = "data/coins", 
                               use_database: bool = True) -> DataQualityChecker:
    """创建数据质量检查器的便捷函数"""
    config = DataQualityConfig(data_dir=data_dir, use_database=use_database)
    return DataQualityChecker(config)
