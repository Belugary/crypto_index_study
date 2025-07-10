#!/usr/bin/env python3
"""
下载前100名币种完整历史数据的用例

这个脚本专门用于下载市值前100名数字货币的完整历史数据（从上市开始到现在）。
适用于需要进行长期历史分析、回测等场景。

使用方法:
    python examples/download_top100_max_history.py

功能特性:
- 下载前100名币种的完整历史数据
- 智能缓存，避免重复下载
- 详细的进度显示和状态报告
- 自动错误处理和重试
- 下载完成后的数据统计
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.batch_downloader import create_batch_downloader


def main():
    """主函数：下载前100名币种的完整历史数据"""

    print("=" * 80)
    print("🚀 市值前100名数字货币完整历史数据下载器")
    print("=" * 80)
    print(f"📅 开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        # 创建批量下载器
        print("🔧 初始化批量下载器...")
        downloader = create_batch_downloader()
        print("✅ 下载器初始化完成")
        print()

        # 配置下载参数
        top_n = 100  # 前100名
        days = "max"  # 完整历史数据
        force_update = False  # 使用智能缓存
        request_interval = 2  # 请求间隔（秒）

        print("📋 下载配置:")
        print(f"   • 目标币种数量: 前 {top_n} 名")
        print(f"   • 数据时间范围: {days} (完整历史)")
        print(f"   • 智能缓存: {'启用' if not force_update else '禁用'}")
        print(f"   • 请求间隔: {request_interval} 秒")
        print()

        print("🎯 开始批量下载...")
        print("-" * 60)

        # 执行批量下载
        raw_results = downloader.download_batch(
            top_n=top_n,
            days=days,
            force_update=force_update,
            force_overwrite=False,  # 新增参数：是否强制覆盖
            request_interval=request_interval,
        )

        # 统计结果
        success_count = sum(1 for status in raw_results.values() if status == "success")
        failed_count = sum(1 for status in raw_results.values() if status == "failed")
        skipped_count = sum(1 for status in raw_results.values() if status == "skipped")

        # 获取失败的币种列表
        failed_coins = [
            coin for coin, status in raw_results.items() if status == "failed"
        ]

        results = {
            "success": success_count,
            "failed": failed_count,
            "skipped": skipped_count,
            "failed_coins": failed_coins,
        }

        print("-" * 60)
        print("📊 下载结果统计:")
        print(f"   ✅ 成功下载: {results['success']} 个币种")
        print(f"   ❌ 下载失败: {results['failed']} 个币种")
        print(f"   ⏭️  智能跳过: {results['skipped']} 个币种")
        print(
            f"   📈 总计处理: {results['success'] + results['failed'] + results['skipped']} 个币种"
        )
        print()

        # 显示失败的币种详情
        if failed_coins:
            print("❌ 下载失败的币种列表:")
            for i, coin in enumerate(failed_coins, 1):
                print(f"   {i:2d}. {coin}")
            print()

            # 询问是否重试失败的下载
            print("🔄 检测到失败的下载，可以尝试重新下载这些币种")
            retry_choice = input("是否重试失败的币种？ (y/n): ").lower().strip()

            if retry_choice == "y":
                print(f"\n🔄 开始重试下载 {len(failed_coins)} 个失败的币种...")
                retry_results = downloader.retry_failed_downloads(
                    failed_coins=failed_coins,
                    days=days,
                    vs_currency="usd",
                    request_interval=request_interval + 1,  # 增加间隔避免限制
                )

                retry_success = sum(
                    1 for status in retry_results.values() if status == "success"
                )
                retry_failed = sum(
                    1 for status in retry_results.values() if status == "failed"
                )

                print(f"\n🔄 重试结果:")
                print(f"   ✅ 重试成功: {retry_success} 个币种")
                print(f"   ❌ 仍然失败: {retry_failed} 个币种")

                # 更新总体结果
                results["success"] += retry_success
                results["failed"] = retry_failed

                if retry_failed > 0:
                    still_failed = [
                        coin
                        for coin, status in retry_results.items()
                        if status == "failed"
                    ]
                    print("   仍然失败的币种:", ", ".join(still_failed))
                print()

        # 获取已下载币种列表
        downloaded_coins = downloader.list_downloaded_coins()
        total_coins = len(downloaded_coins)

        print("💾 数据库状态:")
        print(f"   📁 总计币种: {total_coins} 个")
        print(f"   📂 数据目录: data/coins/")
        print(f"   📋 元数据文件: data/metadata/download_metadata.json")
        print(f"   📋 失败记录: data/logs/failed_downloads.log")
        print()

        # 显示部分下载的币种（前20个）
        if downloaded_coins:
            print("🏆 已下载币种列表 (显示前20个):")
            display_coins = downloaded_coins[:20]
            for i, coin_id in enumerate(display_coins, 1):
                # 尝试获取币种状态
                try:
                    status = downloader.get_download_status(coin_id)
                    if status:
                        last_update = status.get("last_updated", "N/A")
                        record_count = status.get("record_count", "N/A")
                        print(
                            f"   {i:2d}. {coin_id:<30} | 记录数: {record_count:<6} | 更新: {last_update}"
                        )
                    else:
                        print(f"   {i:2d}. {coin_id:<30} | 状态: 未知")
                except:
                    print(f"   {i:2d}. {coin_id}")

            if total_coins > 20:
                print(f"   ... 还有 {total_coins - 20} 个币种")

        print()
        print("=" * 80)
        print(f"🎉 下载任务完成！")
        print(f"📅 完成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # 成功率统计
        if results["success"] + results["failed"] > 0:
            success_rate = (
                results["success"] / (results["success"] + results["failed"])
            ) * 100
            print(f"📈 成功率: {success_rate:.1f}%")

        print("=" * 80)

        return results

    except KeyboardInterrupt:
        print("\n⚠️  用户中断下载")
        return None
    except Exception as e:
        print(f"\n❌ 下载过程中发生错误: {e}")
        return None


if __name__ == "__main__":
    # 执行主函数
    result = main()

    # 根据结果设置退出码
    if result is None:
        sys.exit(1)
    elif result["failed"] > 0:
        print(f"\n⚠️  部分下载失败，建议检查网络连接或API配置")
        sys.exit(2)
    else:
        print(f"\n✅ 所有数据下载成功！")
        sys.exit(0)
