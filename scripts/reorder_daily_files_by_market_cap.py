"""
每日汇总文件市值排序与排名重赋值工具

功能：
- 遍历 data/daily/daily_files/ 目录下所有每日汇总 CSV 文件
- 按市值字段降序排序
- 依次赋值 rank 字段（1,2,3...）
- 支持 dry-run 模式，仅输出排序结果不写入文件
- 支持多线程加速

用法：
python scripts/reorder_daily_files_by_market_cap.py [--dry-run] [--max-workers N]

"""

import os
import glob
import pandas as pd
import argparse
from concurrent.futures import ThreadPoolExecutor


def process_file(file_path, dry_run=False):
    df = pd.read_csv(file_path)
    # 市值字段降序排序
    df_sorted = df.sort_values(by="market_cap", ascending=False)
    # 重新赋值排名
    df_sorted["rank"] = range(1, len(df_sorted) + 1)
    if dry_run:
        print(f"[DRY-RUN] {os.path.basename(file_path)} 排序后前5:")
        print(df_sorted.head())
    else:
        df_sorted.to_csv(file_path, index=False)
        print(f"✅ 已重排序并赋值排名: {os.path.basename(file_path)}")


def main():
    parser = argparse.ArgumentParser(description="每日汇总文件市值排序与排名重赋值工具")
    parser.add_argument(
        "--dry-run", action="store_true", help="仅输出排序结果，不写入文件"
    )
    parser.add_argument("--max-workers", type=int, default=4, help="最大并发线程数")
    args = parser.parse_args()

    daily_dir = "data/daily/daily_files"
    # 递归搜索所有子目录下的CSV文件
    files = []
    for year_dir in os.listdir(daily_dir):
        year_path = os.path.join(daily_dir, year_dir)
        if os.path.isdir(year_path) and year_dir not in ['.backup']:
            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if os.path.isdir(month_path) and month_dir not in ['.backup']:
                    csv_files = glob.glob(os.path.join(month_path, "*.csv"))
                    files.extend(csv_files)
    
    print(f"🔄 待处理每日文件数: {len(files)}")
    print(f"示例文件: {files[:3] if files else '无'}")
    
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        for file_path in files:
            executor.submit(process_file, file_path, args.dry_run)


if __name__ == "__main__":
    main()
