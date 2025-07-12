#!/usr/bin/env python3
"""
数据质量检查和修复工具
检查所有币种文件的数据完整性，识别并修复有问题的文件
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


class DataQualityChecker:
    """数据质量检查器"""

    def __init__(self):
        self.data_dir = "data/coins"
        self.updater = PriceDataUpdater()
        self.min_rows = 500  # 最小行数阈值
        self.max_days_old = 2  # 数据最大过期天数

    def check_file_quality(self, filepath):
        """检查单个文件的数据质量"""
        try:
            df = pd.read_csv(filepath)
            row_count = len(df)

            # 检查数据时间范围
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
                latest_date = df["date"].max()
                earliest_date = df["date"].min()

                # 计算数据天数和最新程度
                data_span_days = (latest_date - earliest_date).days
                days_since_latest = (datetime.now().date() - latest_date).days

                return {
                    "rows": row_count,
                    "latest_date": latest_date,
                    "earliest_date": earliest_date,
                    "data_span_days": data_span_days,
                    "days_since_latest": days_since_latest,
                    "is_recent": days_since_latest <= self.max_days_old,
                    "has_enough_data": row_count >= self.min_rows,
                }
            else:
                return {
                    "rows": row_count,
                    "latest_date": None,
                    "earliest_date": None,
                    "data_span_days": 0,
                    "days_since_latest": 999,
                    "is_recent": False,
                    "has_enough_data": row_count >= self.min_rows,
                }
        except Exception as e:
            return {
                "error": str(e),
                "rows": 0,
                "is_recent": False,
                "has_enough_data": False,
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

        for filename in files:
            filepath = os.path.join(self.data_dir, filename)
            coin_name = filename[:-4]

            quality = self.check_file_quality(filepath)

            if "error" in quality:
                print(f"❌ {coin_name}: 读取错误 - {quality['error']}")
                problematic_files.append((coin_name, quality, "READ_ERROR"))
            elif not quality["has_enough_data"]:
                print(f"⚠️  {coin_name}: 数据不足 - {quality['rows']}行")
                problematic_files.append((coin_name, quality, "INSUFFICIENT_DATA"))
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

        print("\n" + "=" * 80)
        print(f"📊 扫描结果:")
        print(f"   ✅ 正常文件: {len(good_files)}")
        print(f"   ⚠️  问题文件: {len(problematic_files)}")

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
                    success, api_called = self.updater.download_coin_data(coin_name)

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
