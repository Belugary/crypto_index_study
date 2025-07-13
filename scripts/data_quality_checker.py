#!/usr/bin/env python3
"""
数据质量检查和修复工具

用户友好的数据质量检查脚本，提供命令行接口。
核心功能由 src.analysis.data_quality 模块实现。
"""

import logging
import os
import sys

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.analysis.data_quality import DataQualityAnalyzer, DataQualityRepairer

# 设置日志
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")


def print_scan_results(good_files, problematic_files):
    """打印扫描结果"""
    print(f"🔍 扫描 {len(good_files) + len(problematic_files)} 个币种文件...")
    print("=" * 80)

    # 显示问题文件
    for coin_name, quality, issue_type in problematic_files:
        if issue_type == "READ_ERROR":
            print(f"❌ {coin_name}: 读取错误 - {quality.get('error', '未知')}")
        elif issue_type == "INSUFFICIENT_DATA":
            print(f"⚠️  {coin_name}: 数据不足 - {quality['rows']}行")
        elif issue_type == "INTERVAL_ISSUE":
            print(f"⚠️  {coin_name}: 时间间隔异常 - {quality['interval_msg']}")
        elif issue_type == "OUTDATED_DATA":
            print(f"⚠️  {coin_name}: 数据过期 - 最新:{quality['latest_date']} ({quality['days_since_latest']}天前)")

    # 显示正常文件（采样显示）
    if good_files:
        sample_size = min(5, len(good_files))
        print(f"\n✅ 正常文件示例 (显示{sample_size}/{len(good_files)}个):")
        for coin_name, quality in good_files[:sample_size]:
            print(f"   {coin_name}: {quality['rows']}行, 最新:{quality['latest_date']}")

    print("\n" + "=" * 80)
    print(f"📊 扫描结果:")
    print(f"   ✅ 正常文件: {len(good_files)}")
    print(f"   ⚠️  问题文件: {len(problematic_files)}")


def print_repair_results(results):
    """打印修复结果"""
    successful = [r for r in results if r["success"]]
    failed = [r for r in results if not r["success"]]

    print(f"\n� 修复结果:")
    print(f"   ✅ 成功: {len(successful)}")
    print(f"   ❌ 失败: {len(failed)}")

    if failed:
        print(f"\n失败的修复:")
        for result in failed[:5]:  # 只显示前5个
            print(f"   {result['coin_name']}: {result['message']}")


def main():
    """主函数"""
    print("🔍 数据质量检查工具")
    print("=" * 50)

    try:
        # 创建分析器
        analyzer = DataQualityAnalyzer()

        # 扫描所有文件
        good_files, problematic_files = analyzer.scan_all_files()

        # 显示结果
        print_scan_results(good_files, problematic_files)

        if problematic_files:
            print(f"\n⚠️  发现 {len(problematic_files)} 个问题文件:")
            for coin_name, quality, issue_type in problematic_files[:10]:  # 只显示前10个
                if issue_type == "INSUFFICIENT_DATA":
                    print(f"   📉 {coin_name}: 仅{quality['rows']}行数据")
                elif issue_type == "OUTDATED_DATA":
                    print(f"   📅 {coin_name}: {quality['days_since_latest']}天未更新")
                elif issue_type == "READ_ERROR":
                    print(f"   💥 {coin_name}: 文件读取错误")

            # 询问是否修复
            response = input(f"\n🔧 是否修复这些问题文件? (y/N): ").strip().lower()

            if response == "y":
                print(f"\n🔧 开始修复 {len(problematic_files)} 个问题文件...")
                repairer = DataQualityRepairer(analyzer)
                results = repairer.repair_files(problematic_files, dry_run=False)
                print_repair_results(results)
            else:
                print("📋 跳过修复，您可以稍后运行此工具进行修复")
        else:
            print("🎉 所有文件数据质量良好！")

    except FileNotFoundError as e:
        print(f"❌ {e}")
    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
    except Exception as e:
        print(f"❌ 执行失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
