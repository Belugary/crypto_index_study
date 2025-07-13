"""
该脚本用于根据 data/daily/daily_files/ 目录下的所有日度数据文件，
重新生成一个完整的每日市场摘要文件 (daily_summary.csv)。

**重要用途**: 为快速计算每日指数提供预聚合数据，避免每次都重新扫描所有原始文件。

摘要文件包含以下字段：
- date: 日期
- coin_count: 当日币种数量
- total_market_cap: 当日总市值
- total_volume: 当日总交易量
- avg_market_cap: 当日平均市值
- avg_volume: 当日平均交易量

使用场景:
- 指数计算的快速数据源
- 市场概览和分析
- 历史趋势统计
"""

from pathlib import Path

import pandas as pd
from tqdm import tqdm


def get_project_root() -> Path:
    """获取项目根目录"""
    return Path(__file__).parent.parent


def build_daily_summary():
    """
    遍历所有每日数据文件，计算市场摘要，并保存到 daily_summary.csv。
    """
    project_root = get_project_root()
    daily_files_dir = project_root / "data" / "daily" / "daily_files"
    output_path = project_root / "data" / "daily" / "daily_summary.csv"

    if not daily_files_dir.exists():
        print(f"错误：每日数据目录不存在: {daily_files_dir}")
        return

    # 使用 glob 查找所有日度 csv 文件
    daily_files = sorted(list(daily_files_dir.glob("*/*/*.csv")))

    if not daily_files:
        print(f"错误：在 {daily_files_dir} 中没有找到任何日度数据文件。")
        return

    print(f"找到了 {len(daily_files)} 个日度数据文件。开始生成摘要...")

    summary_data = []

    for file_path in tqdm(daily_files, desc="处理每日数据"):
        try:
            # 从文件名中提取日期
            date_str = file_path.stem

            df = pd.read_csv(file_path)

            # 跳过空文件
            if df.empty:
                continue

            coin_count = len(df)
            total_market_cap = df["market_cap"].sum()
            # 修正: 将 'total_volume' 改为 'volume'
            total_volume = df["volume"].sum()

            # 计算平均值，避免除以零
            avg_market_cap = total_market_cap / coin_count if coin_count > 0 else 0
            avg_volume = total_volume / coin_count if coin_count > 0 else 0

            summary_data.append(
                {
                    "date": date_str,
                    "coin_count": coin_count,
                    "total_market_cap": total_market_cap,
                    "total_volume": total_volume,
                    "avg_market_cap": avg_market_cap,
                    "avg_volume": avg_volume,
                }
            )
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {e}")

    if not summary_data:
        print("没有生成任何摘要数据。")
        return

    # 创建 DataFrame 并按日期排序
    summary_df = pd.DataFrame(summary_data)
    summary_df["date"] = pd.to_datetime(summary_df["date"])
    summary_df = summary_df.sort_values(by="date").reset_index(drop=True)

    # 将 date 列格式化为 YYYY-MM-DD
    summary_df["date"] = summary_df["date"].dt.strftime("%Y-%m-%d")

    # 保存到 CSV 文件
    summary_df.to_csv(output_path, index=False)

    print(f"\n每日市场摘要已成功生成并保存到: {output_path}")
    print(f"总共处理了 {len(summary_df)} 天的数据。")
    print("摘要文件预览:")
    print(summary_df.head())
    print("...")
    print(summary_df.tail())


if __name__ == "__main__":
    build_daily_summary()
