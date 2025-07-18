#!/usr/bin/env python3
"""
数据展示工具模块

提供统一的加密货币数据格式化和展示方法，确保表格显示的一致性和美观性。
用于notebook演示和其他数据可视化场景。
"""

import pandas as pd
from typing import List, Optional, Dict, Any


class CryptoDataDisplayer:
    """加密货币数据展示工具类
    
    提供统一的数据格式化和展示方法，确保表格显示的一致性和美观性
    """
    
    def __init__(self):
        # 币种名称修正映射
        self.name_corrections = {
            'XRP': 'Ripple',
            'BNB': 'Binance Coin'
        }
        
        # 列名映射（英文 -> 中文）
        self.column_mapping = {
            'rank': '排名',
            'symbol': '代码',
            'name': '币种名称', 
            'price': '价格($)',
            'market_cap': '市值(1M$)',
            'volume': '成交量($)',
            'weight': '权重(%)',
            'change_24h': '24h涨跌(%)',
            'change_7d': '7d涨跌(%)'
        }
    
    def format_crypto_data(self, data: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """格式化加密货币数据用于展示
        
        Args:
            data: 包含加密货币数据的DataFrame
            columns: 要显示的列名列表，默认显示 ['rank', 'name', 'symbol', 'price', 'market_cap']
        
        Returns:
            格式化后的DataFrame
        """
        if columns is None:
            columns = ['rank', 'name', 'symbol', 'price', 'market_cap']
        
        # 处理空数据框的情况
        if data.empty:
            return pd.DataFrame()
        
        # 选择显示列
        available_columns = [col for col in columns if col in data.columns]
        if not available_columns:
            raise ValueError(f"没有找到指定的列: {columns}")
            
        display_data = data[available_columns].copy()
        
        # 修正币种名称
        if 'name' in display_data.columns:
            display_data['name'] = display_data['name'].replace(self.name_corrections)
        
        # 符号大写
        if 'symbol' in display_data.columns:
            display_data['symbol'] = display_data['symbol'].str.upper()
        
        # 重命名列为中文
        new_columns = []
        for col in display_data.columns:
            new_columns.append(self.column_mapping.get(col, col))
        display_data.columns = new_columns
        
        # 格式化数值列
        for col in display_data.columns:
            if '价格($)' in col:
                display_data[col] = display_data[col].apply(lambda x: f"{x:,.2f}" if pd.notna(x) else "N/A")
            elif '市值(1M$)' in col:
                # 将市值转换为百万美元单位，四舍五入到整数位，添加千分位分隔符
                display_data[col] = display_data[col].apply(lambda x: f"{round(x/1e6):,}" if pd.notna(x) else "N/A")
            elif '成交量($)' in col:
                display_data[col] = display_data[col].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
            elif '权重(%)' in col or '涨跌(%)' in col:
                display_data[col] = display_data[col].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A")
        
        return display_data
    
    def show_table(self, data: pd.DataFrame, columns: Optional[List[str]] = None, 
                   top_n: int = 5, title: str = "数据展示") -> pd.DataFrame:
        """展示格式化的表格
        
        Args:
            data: 原始数据DataFrame
            columns: 要显示的列
            top_n: 显示前N行数据
            title: 表格标题
            
        Returns:
            格式化后用于显示的DataFrame
        """
        formatted_data = self.format_crypto_data(data, columns)
        
        print(f"📊 {title}:")
        
        # 直接显示前N行，不设置索引，保持所有列名对齐
        display_table = formatted_data.head(top_n)
        
        return display_table
    
    def show_simple_list(self, data: pd.DataFrame, name_col: str = 'name', 
                        symbol_col: str = 'symbol', top_n: int = 10) -> None:
        """显示简单的币种列表
        
        Args:
            data: 数据DataFrame
            name_col: 名称列名
            symbol_col: 符号列名
            top_n: 显示数量
        """
        print(f"前{top_n}大币种:")
        for idx, (i, row) in enumerate(data.head(top_n).iterrows()):
            name = self.name_corrections.get(row[name_col], row[name_col])
            symbol = row[symbol_col].upper()
            print(f"{idx+1:2d}. {name} ({symbol})")


def create_displayer() -> CryptoDataDisplayer:
    """创建数据展示器实例的便捷函数"""
    return CryptoDataDisplayer()
