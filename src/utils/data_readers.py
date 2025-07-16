#!/usr/bin/env python3
"""
数据读取工具函数

提供轻量级的数据读取功能，用于 notebook 和脚本快速调用。
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

# 导入统一分类器
from src.classification.unified_classifier import UnifiedClassifier


def read_daily_snapshot(date_str: str, daily_dir: str = "data/daily/daily_files") -> pd.DataFrame:
    """
    读取已聚合的每日市场快照 CSV（支持年/月分组目录结构）

    Args:
        date_str: 日期字符串，格式为 'YYYY-MM-DD'
        daily_dir: 每日快照文件夹路径，默认 'data/daily/daily_files'

    Returns:
        指定日期的市场快照 DataFrame，若文件不存在则返回空 DataFrame
    """
    # 解析日期
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    year = date_obj.year
    month = f"{date_obj.month:02d}"
    
    # 构建文件路径：daily_dir/YYYY/MM/YYYY-MM-DD.csv
    file_path = Path(daily_dir) / str(year) / month / f"{date_str}.csv"
    
    if not file_path.exists():
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def get_coin_info(coin_id: str, metadata_dir: str = "data/metadata/coin_metadata") -> Tuple[str, str]:
    """
    获取币种的symbol和name信息

    Args:
        coin_id: 币种ID
        metadata_dir: 元数据目录路径

    Returns:
        tuple: (symbol, name)
    """
    try:
        metadata_path = Path(metadata_dir) / f"{coin_id}.json"
        if metadata_path.exists():
            with open(metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('symbol', coin_id).upper(), data.get('name', coin_id)
        return coin_id.upper(), coin_id
    except:
        return coin_id.upper(), coin_id


def display_coins_table(top_coins_df: pd.DataFrame, title: str = "🏆 前10大币种", 
                       metadata_dir: str = "data/metadata/coin_metadata") -> None:
    """
    以表格形式显示币种信息

    Args:
        top_coins_df: 币种数据DataFrame
        title: 表格标题
        metadata_dir: 元数据目录路径
    """
    print(f"\n{title}")
    print("=" * 70)
    
    # 表头
    print(f"{'排名':<4} {'代码':<8} {'名称':<18} {'市值 (USD)':<20}")
    print("-" * 70)
    
    # 数据行
    total_market_cap = 0
    for rank, (_, row) in enumerate(top_coins_df.iterrows(), 1):
        symbol, name = get_coin_info(row['coin_id'], metadata_dir)
        
        # 排名补位显示 (01, 02, ...)
        rank_display = f"{rank:02d}"
        
        # 市值格式化（去掉美元符号）
        market_cap = row['market_cap']
        market_cap_display = f"{market_cap:,.0f}"
        total_market_cap += market_cap
        
        print(f"{rank_display:<4} {symbol:<8} {name:<18} {market_cap_display:>20}")
    
    # 汇总行
    print("-" * 70)
    print(f"{'合计':<4} {'':<8} {'':<18} {total_market_cap:>20,.0f}")


def filter_native_coins(df: pd.DataFrame) -> pd.DataFrame:
    """
    过滤掉稳定币和衍生品，只保留原生币种
    
    使用项目的统一分类器进行专业的币种分类过滤

    Args:
        df: 市场数据DataFrame

    Returns:
        过滤后的DataFrame
    """
    if df.empty:
        return df
    
    # 使用统一分类器进行过滤
    classifier = UnifiedClassifier()
    coin_ids = df['coin_id'].tolist()
    
    # 过滤掉稳定币和包装币
    filtered_coin_ids = classifier.filter_coins(
        coin_ids, 
        exclude_stablecoins=True, 
        exclude_wrapped_coins=True
    )
    
    # 返回过滤后的DataFrame
    filtered_df = df[df['coin_id'].isin(filtered_coin_ids)].copy()
    
    print(f"🔍 过滤前: {len(df)} 个币种")
    print(f"✅ 过滤后: {len(filtered_df)} 个币种 (使用统一分类器排除稳定币和衍生品)")
    
    return filtered_df


def analyze_top_coins(df: pd.DataFrame, top_n: int = 10, filter_derivatives: bool = True) -> Tuple[pd.DataFrame, float]:
    """
    分析市值前N的币种
    
    Args:
        df: 市场数据 DataFrame
        top_n: 返回前N个币种，默认10
        filter_derivatives: 是否过滤稳定币和衍生品，默认True
    
    Returns:
        tuple: (前N个币种的DataFrame, 总市值)
    """
    if df.empty:
        return pd.DataFrame(), 0.0
    
    # 过滤有效数据（市值>0）
    valid_df = df[df['market_cap'] > 0].copy()
    
    # 如果需要，过滤掉稳定币和衍生品
    if filter_derivatives:
        valid_df = filter_native_coins(valid_df)
    
    # 按市值排序
    top_coins = valid_df.nlargest(top_n, 'market_cap')
    
    # 计算总市值
    total_market_cap = valid_df['market_cap'].sum()
    
    return top_coins, total_market_cap
