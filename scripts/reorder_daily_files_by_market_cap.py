"""
每日汇总文件市值排序与排名重赋值工具

功能：
- 遍历 data/daily/daily_files/ 目录下所有每日汇总 CSV 文件
- 按市值字段降序排序
- 依次赋值 rank 字段（1,2,3...）
- 支持 dry-run 模式，仅输出排序结果不写入文件
- 支持多线程加速
- 支持按日期范围重排序文件

用法：
python scripts/reorder_daily_files_by_market_cap.py [--dry-run] [--max-workers N]

"""

import os
import glob
import pandas as pd
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Optional
from datetime import datetime, timedelta


def process_file(file_path: str, dry_run: bool = False) -> Tuple[str, bool]:
    """
    处理单个文件的排序和排名重分配

    Args:
        file_path: 文件路径
        dry_run: 是否为试运行模式

    Returns:
        Tuple[str, bool]: (文件名, 是否成功)
    """
    try:
        df = pd.read_csv(file_path)

        # 检查必要字段是否存在
        if "market_cap" not in df.columns or "rank" not in df.columns:
            return os.path.basename(file_path), False

        # 市值字段降序排序
        df_sorted = df.sort_values(by="market_cap", ascending=False)
        # 重新赋值排名
        df_sorted["rank"] = range(1, len(df_sorted) + 1)

        if dry_run:
            print(f"[DRY-RUN] {os.path.basename(file_path)} 排序后前5:")
            print(df_sorted.head())
        else:
            df_sorted.to_csv(file_path, index=False)

        return os.path.basename(file_path), True
    except Exception as e:
        print(f"处理失败 {os.path.basename(file_path)}: {e}")
        return os.path.basename(file_path), False


def find_daily_files(daily_dir: str = "data/daily/daily_files") -> List[str]:
    """
    查找所有每日汇总文件

    Args:
        daily_dir: 每日文件目录

    Returns:
        List[str]: 文件路径列表
    """
    files = []
    if not os.path.exists(daily_dir):
        print(f"目录不存在: {daily_dir}")
        return files

    for year_dir in os.listdir(daily_dir):
        year_path = os.path.join(daily_dir, year_dir)
        if os.path.isdir(year_path) and year_dir not in [".backup", ".git"]:
            for month_dir in os.listdir(year_path):
                month_path = os.path.join(year_path, month_dir)
                if os.path.isdir(month_path) and month_dir not in [".backup", ".git"]:
                    csv_files = glob.glob(os.path.join(month_path, "*.csv"))
                    files.extend(csv_files)
    return files


def reorder_all_daily_files(
    dry_run: bool = False,
    max_workers: int = 8,
    target_files: Optional[List[str]] = None,
) -> Tuple[int, int]:
    """
    重排序所有每日汇总文件

    Args:
        dry_run: 是否为试运行模式
        max_workers: 最大并发线程数
        target_files: 指定的目标文件列表，如果为None则处理所有文件

    Returns:
        Tuple[int, int]: (成功数量, 总数量)
    """
    if target_files is None:
        files = find_daily_files()
    else:
        files = target_files

    print(f"待处理每日文件数: {len(files)}")
    if files:
        print(f"示例文件: {files[:3]}")

    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_file = {
            executor.submit(process_file, file_path, dry_run): file_path
            for file_path in files
        }

        # 等待完成并收集结果
        for future in as_completed(future_to_file):
            file_name, success = future.result()
            if success:
                successful += 1
                if not dry_run:
                    print(f"已重排序: {file_name}")
            else:
                failed += 1

    print(f"\n处理完成: 成功 {successful}, 失败 {failed}, 总计 {len(files)}")
    return successful, len(files)


def reorder_files_by_date_range(
    start_date: str, end_date: str, dry_run: bool = False, max_workers: int = 8
) -> Tuple[int, int]:
    """
    按日期范围重排序文件（优化版本，只处理指定日期范围的文件）

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        dry_run: 是否为试运行模式
        max_workers: 最大并发线程数

    Returns:
        Tuple[int, int]: (成功数量, 总数量)
    """

    # 解析日期
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"日期格式错误: {e}")
        return 0, 0

    # 查找指定日期范围内的文件
    daily_dir = "data/daily/daily_files"
    target_files = []

    current_date = start_dt
    while current_date <= end_dt:
        year_month_dir = f"{daily_dir}/{current_date.year:04d}/{current_date.month:02d}"
        file_name = f"{current_date.strftime('%Y-%m-%d')}.csv"
        file_path = f"{year_month_dir}/{file_name}"

        if os.path.exists(file_path):
            target_files.append(file_path)

        # 下一天
        current_date += timedelta(days=1)

    if not target_files:
        print(f"📝 指定日期范围内未找到文件: {start_date} 到 {end_date}")
        return 0, 0

    print(f"🎯 针对性重排序: {start_date} 到 {end_date} ({len(target_files)} 个文件)")

    return reorder_all_daily_files(
        dry_run=dry_run, max_workers=max_workers, target_files=target_files
    )


def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description="每日汇总文件市值排序与排名重赋值工具")
    parser.add_argument(
        "--dry-run", action="store_true", help="仅输出排序结果，不写入文件"
    )
    parser.add_argument("--max-workers", type=int, default=8, help="最大并发线程数")
    parser.add_argument(
        "--start-date",
        type=str,
        help="开始日期 (YYYY-MM-DD)，仅在按日期范围重排序时使用",
    )
    parser.add_argument(
        "--end-date", type=str, help="结束日期 (YYYY-MM-DD)，仅在按日期范围重排序时使用"
    )
    args = parser.parse_args()

    if args.start_date and args.end_date:
        successful, total = reorder_files_by_date_range(
            start_date=args.start_date,
            end_date=args.end_date,
            dry_run=args.dry_run,
            max_workers=args.max_workers,
        )
    else:
        successful, total = reorder_all_daily_files(
            dry_run=args.dry_run, max_workers=args.max_workers
        )

    if successful == total and total > 0:
        print("🎉 所有文件处理成功!")
    elif successful > 0:
        print(f"⚠️  部分文件处理成功: {successful}/{total}")
    else:
        print("❌ 没有文件被成功处理")


if __name__ == "__main__":
    main()
