"""
批量下载器使用示例

演示如何使用批量下载器获取数字货币市场数据。
"""

import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.batch_downloader import create_batch_downloader


def main():
    """主函数：演示批量下载功能"""

    print("=== 批量数据下载器示例 ===\n")

    # 创建批量下载器
    downloader = create_batch_downloader(data_dir="data")

    # 示例1：下载前10名币种的最近30天数据
    print("示例1：下载前10名币种的最近30天数据")
    results1 = downloader.download_batch(
        top_n=10, days="30", force_update=False  # 使用缓存
    )

    print(f"下载结果: {results1}\n")

    # 示例2：下载前5名币种的全部历史数据
    print("示例2：下载前5名币种的全部历史数据")
    results2 = downloader.download_batch(
        top_n=5, days="max", force_update=False, request_interval=2  # 增加请求间隔
    )

    print(f"下载结果: {results2}\n")

    # 查看下载状态
    print("=== 下载状态查询 ===")
    downloaded_coins = downloader.list_downloaded_coins()
    print(f"已下载的币种: {downloaded_coins[:10]}...")  # 只显示前10个

    # 查看特定币种状态
    if downloaded_coins:
        coin_id = downloaded_coins[0]
        status = downloader.get_download_status(coin_id)
        print(f"{coin_id} 的下载状态: {status}")


if __name__ == "__main__":
    main()
