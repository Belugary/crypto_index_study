#!/usr/bin/env python3
"""
测试批量下载器的新功能

测试项目:
1. 强制覆盖参数
2. 失败记录功能
3. 重试下载功能
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.batch_downloader import create_batch_downloader


def test_force_overwrite():
    """测试强制覆盖功能"""
    print("🧪 测试强制覆盖功能...")

    downloader = create_batch_downloader()

    # 下载少量币种进行测试
    print("📥 第一次下载...")
    results1 = downloader.download_batch(top_n=3, days="30", force_overwrite=False)

    print("📥 第二次下载（应该被跳过）...")
    results2 = downloader.download_batch(top_n=3, days="30", force_overwrite=False)

    print("📥 第三次下载（强制覆盖）...")
    results3 = downloader.download_batch(top_n=3, days="30", force_overwrite=True)

    print(f"第一次结果: {results1}")
    print(f"第二次结果: {results2}")
    print(f"第三次结果: {results3}")

    # 验证结果
    skipped_count2 = sum(1 for status in results2.values() if status == "skipped")
    success_count3 = sum(1 for status in results3.values() if status == "success")

    if skipped_count2 > 0:
        print("✅ 智能跳过功能正常")
    else:
        print("❌ 智能跳过功能异常")

    if success_count3 > 0:
        print("✅ 强制覆盖功能正常")
    else:
        print("❌ 强制覆盖功能异常")


def test_failed_logging():
    """测试失败记录功能"""
    print("\n🧪 测试失败记录功能...")

    downloader = create_batch_downloader()

    # 尝试下载一个不存在的币种（应该失败）
    print("📥 尝试下载不存在的币种...")

    # 手动调用下载单个币种的方法来模拟失败
    fake_coin = "definitely-not-a-real-coin-12345"
    success = downloader._download_single_coin(fake_coin, "30", "usd", 1, 1)

    if not success:
        print(f"✅ 模拟失败成功: {fake_coin}")

        # 手动保存失败记录
        downloader._save_failed_coins_log([fake_coin], "30")

        # 测试从日志读取失败记录
        failed_from_log = downloader.get_failed_coins_from_log()
        print(f"从日志读取的失败币种: {failed_from_log}")

        if fake_coin in failed_from_log:
            print("✅ 失败记录功能正常")
        else:
            print("❌ 失败记录功能异常")
    else:
        print(f"❌ 模拟失败不成功: {fake_coin}")


def main():
    """主测试函数"""
    print("=" * 60)
    print("🧪 批量下载器新功能测试")
    print("=" * 60)
    print(f"📅 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    try:
        test_force_overwrite()
        test_failed_logging()

        print("\n" + "=" * 60)
        print("🎉 测试完成！")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}")
        return False

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
