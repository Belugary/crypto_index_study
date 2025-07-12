#!/usr/bin/env python3
"""
数据质量检查和修复工具
检查所有币种文件的数据完整性，识别并修复有问题的文件
"""

import logging
import os
import sys
from datetime import datetime, timedelta

import pandas as pd

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self):
        self.data_dir = "data/coins"
        # 移除PriceDataUpdater的初始化，只在需要时创建
        self.updater = None
        self.min_rows = 100  # 降低最小行数阈值，很多新币种数据确实不长
        self.max_days_old = 2  # 数据最大过期天数
        self.min_data_span_days = 30  # 最小数据跨度天数，用于区分新币种和真正过期的数据

    def _get_updater(self):
        """延迟初始化updater"""
        if self.updater is None:
            self.updater = PriceDataUpdater()
        return self.updater

    def _is_data_recent(self, data_span_days, days_since_latest):
        """
        智能判断数据是否"最新"

        逻辑：
        1. 如果数据跨度很短（<30天），可能是新币种，只要不超过7天就算正常
        2. 如果数据跨度长（>=30天），说明是老币种，按标准的2天判断
        3. 这样可以避免把"数据本身就不长的新币种"误判为"数据过期"

        Args:
            data_span_days: 数据跨度天数（最新日期 - 最早日期）
            days_since_latest: 距离最新数据的天数

        Returns:
            bool: 数据是否算"最新"
        """
        if data_span_days < self.min_data_span_days:
            # 新币种或数据跨度短的币种，给更宽松的标准
            return days_since_latest <= 7
        else:
            # 老币种，使用标准的判断
            return days_since_latest <= self.max_days_old

    def check_timestamp_intervals(self, df, time_column):
        """检查时间戳间隔是否合理（主要检测是否有1天间隔的缺失）"""
        try:
            if time_column == "timestamp":
                # 转换毫秒时间戳为日期
                dates = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            else:
                # date列直接使用
                dates = pd.to_datetime(df[time_column]).dt.date

            # 去重并排序
            unique_dates = sorted(set(dates))

            if len(unique_dates) < 2:
                return True, "数据点太少，无法检查间隔"

            # 检查是否有异常的间隔（超过7天的缺失）
            large_gaps = []
            for i in range(1, len(unique_dates)):
                gap_days = (unique_dates[i] - unique_dates[i - 1]).days
                if gap_days > 7:  # 超过7天的缺失被认为是问题
                    large_gaps.append(
                        f"{unique_dates[i-1]} -> {unique_dates[i]} ({gap_days}天)"
                    )

            if large_gaps:
                gap_info = "; ".join(large_gaps[:3])  # 只显示前3个
                if len(large_gaps) > 3:
                    gap_info += f" 等{len(large_gaps)}个缺失"
                return False, f"发现大时间缺失: {gap_info}"

            return True, "时间间隔正常"

        except Exception as e:
            return True, f"时间间隔检查失败: {str(e)}"

    def check_file_quality(self, filepath):
        """检查单个文件的数据质量"""
        try:
            df = pd.read_csv(filepath)
            row_count = len(df)

            # 检查数据时间范围 - 支持date列和timestamp列
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                latest_date = df["date"].max()
                earliest_date = df["date"].min()

                # 计算数据天数和最新程度
                data_span_days = (latest_date - earliest_date).days
                days_since_latest = (datetime.now().date() - latest_date).days

                # 检查时间戳间隔
                interval_ok, interval_msg = self.check_timestamp_intervals(df, "date")

            elif "timestamp" in df.columns:
                # 处理timestamp列（毫秒时间戳）
                df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")
                latest_date = df["datetime"].dt.date.max()
                earliest_date = df["datetime"].dt.date.min()

                # 计算数据天数和最新程度
                data_span_days = (latest_date - earliest_date).days
                days_since_latest = (datetime.now().date() - latest_date).days

                # 检查时间戳间隔
                interval_ok, interval_msg = self.check_timestamp_intervals(
                    df, "timestamp"
                )

            else:
                # 没有时间列的情况
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

    def scan_all_files(self):
        """扫描所有文件并分类"""
        if not os.path.exists(self.data_dir):
            print(f"❌ 数据目录不存在: {self.data_dir}")
            return

        files = [f for f in os.listdir(self.data_dir) if f.endswith(".csv")]

        print(f"🔍 扫描 {len(files)} 个币种文件...")
        print("=" * 80)

        good_files = []
        problematic_files = []

        for i, filename in enumerate(files):
            # 添加进度显示
            if i % 50 == 0:
                print(f"进度: {i}/{len(files)} ({i/len(files)*100:.1f}%)")

            filepath = os.path.join(self.data_dir, filename)
            coin_name = filename[:-4]

            try:
                # 添加当前处理文件的调试信息
                if i % 100 == 0:  # 每100个文件显示一次
                    print(f"正在处理: {coin_name}")

                quality = self.check_file_quality(filepath)

                try:
                    if "error" in quality:
                        print(f"❌ {coin_name}: 读取错误 - {quality['error']}")
                        problematic_files.append((coin_name, quality, "READ_ERROR"))
                    elif not quality["has_enough_data"]:
                        print(f"⚠️  {coin_name}: 数据不足 - {quality['rows']}行")
                        problematic_files.append(
                            (coin_name, quality, "INSUFFICIENT_DATA")
                        )
                    elif not quality["interval_ok"]:
                        print(
                            f"⚠️  {coin_name}: 时间间隔异常 - {quality['interval_msg']}"
                        )
                        problematic_files.append((coin_name, quality, "INTERVAL_ISSUE"))
                    elif not quality["is_recent"]:
                        print(
                            f"⚠️  {coin_name}: 数据过期 - 最新:{quality['latest_date']} ({quality['days_since_latest']}天前)"
                        )
                        problematic_files.append((coin_name, quality, "OUTDATED_DATA"))
                    else:
                        print(
                            f"✅ {coin_name}: 正常 - {quality['rows']}行, 最新:{quality['latest_date']}"
                        )
                        good_files.append((coin_name, quality))
                except BrokenPipeError:
                    # 管道中断时静默退出
                    break
                except KeyboardInterrupt:
                    print("\n用户中断操作")
                    break
            except Exception as e:
                print(f"❌ {coin_name}: 处理异常 - {str(e)}")
                problematic_files.append(
                    (coin_name, {"error": str(e)}, "PROCESSING_ERROR")
                )
                continue

        try:
            print("\n" + "=" * 80)
            print(f"📊 扫描结果:")
            print(f"   ✅ 正常文件: {len(good_files)}")
            print(f"   ⚠️  问题文件: {len(problematic_files)}")
        except BrokenPipeError:
            pass  # 静默处理管道中断

        return good_files, problematic_files

    def fix_problematic_files(self, problematic_files, dry_run=True):
        """修复有问题的文件"""
        if not problematic_files:
            print("🎉 没有发现问题文件需要修复！")
            return

        if dry_run:
            print(f"\n🔍 DRY RUN: 将修复以下 {len(problematic_files)} 个问题文件:")
        else:
            print(f"\n🔧 开始修复 {len(problematic_files)} 个问题文件:")

        for i, (coin_name, quality, issue_type) in enumerate(problematic_files, 1):
            print(f"\n[{i}/{len(problematic_files)}] 处理 {coin_name} ({issue_type})")

            if dry_run:
                print(f"   📋 将执行: 重新下载完整历史数据")
            else:
                try:
                    print(f"   📥 重新下载完整历史数据...")
                    success, api_called = self._get_updater().download_coin_data(
                        coin_name
                    )

                    if success:
                        # 重新检查质量
                        filepath = os.path.join(self.data_dir, f"{coin_name}.csv")
                        new_quality = self.check_file_quality(filepath)
                        print(
                            f"   ✅ 修复成功: {new_quality['rows']}行, 最新:{new_quality['latest_date']}"
                        )
                    else:
                        print(f"   ❌ 修复失败")

                except Exception as e:
                    print(f"   ❌ 修复错误: {e}")


def main():
    """主函数"""
    checker = DataQualityChecker()

    print("🔍 数据质量检查工具")
    print("=" * 50)  # 扫描所有文件
    result = checker.scan_all_files()
    if result is None:
        return

    good_files, problematic_files = result

    if problematic_files:
        print(f"\n⚠️  发现 {len(problematic_files)} 个问题文件:")
        for coin_name, quality, issue_type in problematic_files:
            if issue_type == "INSUFFICIENT_DATA":
                print(f"   📉 {coin_name}: 仅{quality['rows']}行数据")
            elif issue_type == "OUTDATED_DATA":
                print(f"   📅 {coin_name}: {quality['days_since_latest']}天未更新")
            elif issue_type == "READ_ERROR":
                print(f"   💥 {coin_name}: 文件读取错误")

        # 询问是否修复
        response = input(f"\n🔧 是否修复这些问题文件? (y/N): ").strip().lower()

        if response == "y":
            checker.fix_problematic_files(problematic_files, dry_run=False)
        else:
            print("📋 跳过修复，您可以稍后运行此工具进行修复")
    else:
        print("🎉 所有文件数据质量良好！")


if __name__ == "__main__":
    main()
