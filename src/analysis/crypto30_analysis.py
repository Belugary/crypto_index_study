"""
Crypto30 综合分析工具

重构后的函数式设计，替代原有的面向对象版本。
提供简洁、易测试的分析功能。
"""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Union
from collections import defaultdict

import pandas as pd
from tqdm import tqdm

from ..index.market_cap_weighted import MarketCapWeightedIndexCalculator
from ..downloaders.daily_aggregator import DailyDataAggregator
from ..classification.unified_classifier import UnifiedClassifier
from ..utils.path_utils import find_project_root, resolve_data_path, ensure_directory

logger = logging.getLogger(__name__)


def get_daily_constituents_and_weights(
    calculator: MarketCapWeightedIndexCalculator,
    target_date: date, 
    top_n: int = 30
) -> Tuple[List[str], Dict[str, float], Dict[str, float]]:
    """
    获取指定日期的成分币种、权重和价格

    Args:
        calculator: 指数计算器实例
        target_date: 目标日期
        top_n: 成分数量

    Returns:
        (成分币种列表, 权重字典, 价格字典)
    """
    # 获取市值数据
    market_caps = calculator._get_daily_market_caps(target_date)
    if not market_caps:
        return [], {}, {}

    # 选择前N名
    constituents = calculator._select_top_coins(market_caps, top_n)

    # 计算权重
    weights = calculator._calculate_weights(constituents, market_caps)

    # 获取价格
    prices = {}
    for coin_id in constituents:
        price = calculator._get_coin_price(coin_id, target_date)
        if price is not None:
            prices[coin_id] = price

    return constituents, weights, prices


def generate_daily_detailed_data(
    calculator: MarketCapWeightedIndexCalculator,
    start_date: date, 
    end_date: date, 
    base_value: float = 100.0,
    top_n: int = 30
) -> pd.DataFrame:
    """
    生成详细的每日数据表

    Args:
        calculator: 指数计算器实例
        start_date: 开始日期
        end_date: 结束日期
        base_value: 基准值
        top_n: 成分数量

    Returns:
        包含每日指数值和成分信息的DataFrame
    """
    date_range = pd.date_range(start_date, end_date, freq='D')
    results = []

    for current_date in tqdm(date_range, desc="生成每日详细数据"):
        current_date = current_date.date()
        
        # 获取指数值 - 使用完整的指数计算，然后提取当日值
        # 计算从基准日到当前日期的指数
        base_date = start_date  # 使用开始日期作为基准日
        try:
            index_df = calculator.calculate_index(
                start_date=base_date,
                end_date=current_date, 
                top_n=top_n,
                base_date=base_date,
                base_value=base_value
            )
            
            if index_df.empty:
                continue
                
            # 查找当前日期的指数值
            date_str = current_date.strftime('%Y-%m-%d')
            if date_str in index_df.index:
                index_value = index_df.loc[date_str, 'index_value']
            else:
                # 如果直接匹配不到，尝试最后一个值
                index_value = index_df.iloc[-1]['index_value']
        except Exception as e:
            logger.warning(f"计算 {current_date} 指数值失败: {e}")
            continue

        # 获取成分信息
        constituents, weights, prices = get_daily_constituents_and_weights(
            calculator, current_date, top_n
        )

        # 构建当日记录
        record = {
            'date': current_date,
            'index_value': index_value,
            'constituents_count': len(constituents),
            'constituents': ','.join(constituents),
            'top_5_constituents': ','.join(constituents[:5]),
            'weights': str(weights),
            'prices': str(prices)
        }

        # 添加前5名的详细信息
        for i, coin_id in enumerate(constituents[:5]):
            record[f'top_{i+1}_coin'] = coin_id
            record[f'top_{i+1}_weight'] = weights.get(coin_id, 0.0)
            record[f'top_{i+1}_price'] = prices.get(coin_id, 0.0)

        results.append(record)

    return pd.DataFrame(results)


def track_constituent_changes(daily_data: pd.DataFrame) -> Dict:
    """
    跟踪成分变化

    Args:
        daily_data: 每日详细数据

    Returns:
        成分变化统计信息
    """
    changes = {
        'new_entries': defaultdict(list),
        'exits': defaultdict(list),
        'rank_changes': defaultdict(list)
    }

    prev_constituents = set()
    for _, row in daily_data.iterrows():
        current_constituents = set(row['constituents'].split(',')) if row['constituents'] else set()
        
        # 新入榜
        new_entries = current_constituents - prev_constituents
        if new_entries:
            changes['new_entries'][row['date']].extend(new_entries)

        # 出榜
        exits = prev_constituents - current_constituents
        if exits:
            changes['exits'][row['date']].extend(exits)

        prev_constituents = current_constituents

    return changes


def generate_monthly_report(
    daily_data: pd.DataFrame, 
    month_start: date,
    month_end: date,
    output_dir: Path
) -> str:
    """
    生成月度分析报告

    Args:
        daily_data: 每日详细数据
        month_start: 月初日期
        month_end: 月末日期
        output_dir: 输出目录

    Returns:
        生成的报告文件路径
    """
    month_data = daily_data[
        (daily_data['date'] >= month_start) & 
        (daily_data['date'] <= month_end)
    ]

    if month_data.empty:
        return ""

    # 生成报告内容
    month_str = month_start.strftime("%Y-%m")
    report_content = f"""# Crypto30 月度分析报告 - {month_str}

## 概览

- 报告期间: {month_start} 至 {month_end}
- 数据点数: {len(month_data)}
- 起始指数值: {month_data.iloc[0]['index_value']:.2f}
- 结束指数值: {month_data.iloc[-1]['index_value']:.2f}
- 月度涨跌幅: {((month_data.iloc[-1]['index_value'] / month_data.iloc[0]['index_value']) - 1) * 100:.2f}%

## 成分变化

"""

    # 跟踪成分变化
    changes = track_constituent_changes(month_data)
    
    if changes['new_entries']:
        report_content += "### 新入榜币种\n\n"
        for date, coins in changes['new_entries'].items():
            report_content += f"- {date}: {', '.join(coins)}\n"
        report_content += "\n"

    if changes['exits']:
        report_content += "### 出榜币种\n\n"
        for date, coins in changes['exits'].items():
            report_content += f"- {date}: {', '.join(coins)}\n"
        report_content += "\n"

    # 保存报告
    report_file = output_dir / f"crypto30_monthly_report_{month_str}.md"
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)

    return str(report_file)


def save_detailed_data(
    daily_data: pd.DataFrame, 
    output_dir: Path,
    filename: str = "crypto30_detailed_data.csv"
) -> str:
    """
    保存详细数据到CSV文件

    Args:
        daily_data: 每日详细数据
        output_dir: 输出目录
        filename: 文件名

    Returns:
        保存的文件路径
    """
    ensure_directory(output_dir)
    file_path = output_dir / filename
    daily_data.to_csv(file_path, index=False, encoding='utf-8')
    return str(file_path)


def run_crypto30_comprehensive_analysis(
    start_date: date,
    end_date: Optional[date] = None,
    output_dir: str = "data/crypto30_analysis",
    base_value: float = 100.0,
    top_n: int = 30,
    generate_monthly_reports: bool = True
) -> Dict[str, Union[str, List[str]]]:
    """
    运行 Crypto30 综合分析

    Args:
        start_date: 开始日期
        end_date: 结束日期，默认为今天
        output_dir: 输出目录
        base_value: 基准值
        top_n: 成分数量
        generate_monthly_reports: 是否生成月度报告

    Returns:
        生成的文件路径字典
    """
    if end_date is None:
        end_date = datetime.now().date()

    # 解析输出路径
    project_root = find_project_root()
    output_path = resolve_data_path(output_dir, project_root)
    ensure_directory(output_path)

    logger.info(f"开始 Crypto30 综合分析: {start_date} 至 {end_date}")

    # 初始化组件 - 启用数据库模式以获得更好性能
    calculator = MarketCapWeightedIndexCalculator(
        exclude_stablecoins=True, 
        exclude_wrapped_coins=True,
        use_database=True
    )

    # 生成每日详细数据
    logger.info("生成每日详细数据...")
    daily_data = generate_daily_detailed_data(
        calculator, start_date, end_date, base_value, top_n
    )

    # 保存详细数据
    detailed_data_file = save_detailed_data(daily_data, output_path)
    logger.info(f"详细数据已保存到: {detailed_data_file}")

    results: Dict[str, Union[str, List[str]]] = {
        'detailed_data': detailed_data_file
    }

    # 生成月度报告
    if generate_monthly_reports and not daily_data.empty:
        logger.info("生成月度报告...")
        
        # 按月分组生成报告
        daily_data['year_month'] = pd.to_datetime(daily_data['date']).dt.to_period('M')
        monthly_reports = []
        
        for period in daily_data['year_month'].unique():
            month_start = period.start_time.date()
            month_end = period.end_time.date()
            
            report_file = generate_monthly_report(
                daily_data, month_start, month_end, output_path
            )
            if report_file:
                monthly_reports.append(report_file)

        results['monthly_reports'] = monthly_reports
        logger.info(f"已生成 {len(monthly_reports)} 个月度报告")

    logger.info("Crypto30 综合分析完成")
    return results


# 便捷函数

def create_crypto30_calculator(use_database: bool = True) -> MarketCapWeightedIndexCalculator:
    """创建 Crypto30 指数计算器的便捷函数"""
    return MarketCapWeightedIndexCalculator(
        exclude_stablecoins=True, 
        exclude_wrapped_coins=True,
        use_database=use_database
    )
