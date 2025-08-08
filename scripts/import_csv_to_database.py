#!/usr/bin/env python3
"""
CSV 数据导入数据库脚本

从 data/daily/daily_files/ 中的 CSV 文件批量导入数据到数据库中
"""

import sqlite3
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from tqdm import tqdm
import sys
import os

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database_utils import DatabaseManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def import_csv_to_database():
    """从CSV文件导入数据到数据库"""
    
    # 初始化路径
    daily_files_path = project_root / 'data' / 'daily' / 'daily_files'
    db_path = project_root / 'data' / 'market.db'
    
    print(f"📁 CSV文件路径: {daily_files_path}")
    print(f"🗃️  数据库路径: {db_path}")
    
    if not daily_files_path.exists():
        print("❌ CSV文件目录不存在")
        return
        
    if not db_path.exists():
        print("❌ 数据库文件不存在")
        return
    
    # 收集所有CSV文件
    csv_files = []
    for year_dir in sorted(daily_files_path.glob('20*')):
        if year_dir.is_dir():
            for month_dir in sorted(year_dir.glob('*')):
                if month_dir.is_dir():
                    for date_file in sorted(month_dir.glob('*.csv')):
                        date_str = date_file.stem
                        if len(date_str) == 10 and date_str.count('-') == 2:
                            csv_files.append((date_str, date_file))
    
    print(f"📊 找到 {len(csv_files)} 个CSV文件")
    
    if len(csv_files) == 0:
        print("❌ 没有找到任何CSV文件")
        return
    
    # 显示文件范围
    csv_files.sort()
    print(f"📅 日期范围: {csv_files[0][0]} 到 {csv_files[-1][0]}")
    
    # 连接数据库并开始导入
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            
            # 统计信息
            total_inserted = 0
            failed_files = []
            
            # 导入数据
            for date_str, csv_file in tqdm(csv_files, desc="导入CSV文件"):
                try:
                    # 读取CSV文件
                    df = pd.read_csv(csv_file)
                    
                    if df.empty:
                        logger.warning(f"跳过空文件: {date_str}")
                        continue
                    
                    # 准备数据库记录
                    records = []
                    for _, row in df.iterrows():
                        # 首先确保币种存在于 coins 表中
                        coin_id = row.get('coin_id', '')
                        if not coin_id:
                            continue
                            
                        # 插入币种信息（如果不存在）
                        cursor.execute("""
                            INSERT OR IGNORE INTO coins (id, symbol, name, last_updated)
                            VALUES (?, ?, ?, ?)
                        """, (
                            coin_id,
                            row.get('symbol', '').upper(),
                            row.get('name', ''),
                            datetime.now().isoformat()
                        ))
                        
                        # 准备日市场数据 - 注意使用 rank 而不是 market_cap_rank
                        timestamp = row.get('timestamp', 0)
                        if pd.isna(timestamp):
                            timestamp = int(datetime.strptime(date_str, '%Y-%m-%d').timestamp() * 1000)
                        
                        record = (
                            coin_id,                        # coin_id
                            date_str,                       # date
                            int(timestamp),                 # timestamp
                            float(row.get('price', 0)) if pd.notna(row.get('price', 0)) else 0,
                            float(row.get('volume', 0)) if pd.notna(row.get('volume', 0)) else 0,
                            float(row.get('market_cap', 0)) if pd.notna(row.get('market_cap', 0)) else 0,
                            int(row.get('rank', 0)) if pd.notna(row.get('rank', 0)) else 0,
                            datetime.now().isoformat()      # created_at
                        )
                        records.append(record)
                    
                    # 批量插入日市场数据
                    if records:
                        cursor.executemany("""
                            REPLACE INTO daily_market_data 
                            (coin_id, date, timestamp, price, volume, market_cap, rank, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, records)
                        
                        total_inserted += len(records)
                    
                except Exception as e:
                    logger.error(f"导入文件 {date_str} 失败: {e}")
                    failed_files.append((date_str, str(e)))
                    continue
            
            # 提交事务
            conn.commit()
            
            print(f"\n✅ 导入完成!")
            print(f"📊 总计插入记录: {total_inserted:,}")
            
            if failed_files:
                print(f"❌ 失败的文件: {len(failed_files)}")
                for date_str, error in failed_files[:5]:  # 只显示前5个错误
                    print(f"   {date_str}: {error}")
                    
            # 验证导入结果
            cursor.execute("SELECT COUNT(*) FROM daily_market_data")
            db_records = cursor.fetchone()[0]
            print(f"🗃️  数据库中总记录数: {db_records:,}")
            
            cursor.execute("SELECT COUNT(DISTINCT date) FROM daily_market_data")
            db_dates = cursor.fetchone()[0]
            print(f"📅 数据库中日期数: {db_dates}")
            
            cursor.execute("SELECT MIN(date), MAX(date) FROM daily_market_data")
            date_range = cursor.fetchone()
            print(f"📅 数据库日期范围: {date_range[0]} 到 {date_range[1]}")
            
    except Exception as e:
        print(f"❌ 数据库操作失败: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🚀 开始从CSV文件导入数据到数据库...")
    import_csv_to_database()
    print("🎉 导入完成!")
