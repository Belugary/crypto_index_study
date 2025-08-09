#!/usr/bin/env python3
"""
数据展示工具模块

提供统一的加密货币数据格式化和展示方法，确保表格显示的一致性和美观性。
专注于数据展示，数据获取由外部完成。
"""

import pandas as pd
from typing import List, Optional, Dict, Any
from pathlib import Path

try:
    from IPython.display import display
    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False


class CryptoDataDisplayer:
    """加密货币数据展示工具类
    
    专门负责加密货币数据的清理、格式化和展示。
    遵循单一职责原则：只处理数据展示，数据获取由外部完成。
    
    设计原则：
    - 数据获取与展示分离：本类专注于接收数据并进行格式化展示
    - 统一格式化：提供一致的数值格式化和列名映射
    - 灵活配置：支持自定义列选择、排序和显示参数
    - Jupyter优化：在notebook环境中提供最佳的表格显示效果
    
    主要功能：
    - 数据清理（去除无效数据、处理缺失值）
    - 格式化显示（价格、市值等数值的格式化）
    - 列名本地化（英文列名转换为中文显示）
    - 表格展示（适配Jupyter环境的表格显示）
    
    使用模式：
        # 1. 外部获取数据
        raw_data = aggregator.get_daily_data('2024-01-15')
        
        # 2. 使用展示工具处理和显示
        displayer = CryptoDataDisplayer()
        clean_data = displayer.clean_data(raw_data)
        displayer.show_table(clean_data, top_n=10)
    """
    
    def __init__(self):
        """
        初始化数据展示器
        
        移除了数据获取相关的参数，专注于数据展示功能。
        数据获取应该在外部完成，本类只负责接收和展示数据。
        """
        # 币种名称修正映射 - 修正一些常见的显示问题
        self.name_corrections = {
            'XRP': 'Ripple',
            'BNB': 'Binance Coin'
        }
        
        # 列名映射（英文 -> 中文）- 统一的本地化显示
        self.column_mapping = {
            'rank': '排名',
            'symbol': '代码',
            'name': '币种名称', 
            'price': '价格($)',
            # 市值改为按十亿美元(1B$)为单位显示
            'market_cap': '市值(1B$)',
            'volume': '成交量($)',
            # 权重列表头已包含(%)，单元格内部不再附加百分号
            'weight': '权重(%)',
            'change_24h': '24h涨跌(%)',
            'change_7d': '7d涨跌(%)'
        }
    
    def _add_metadata_fields(self, data: pd.DataFrame) -> pd.DataFrame:
        """智能加载币种元数据（symbol 和 name），支持动态补充
        
        优先从元数据文件加载，如果缺失则从币种CSV文件补充基础信息。
        
        Args:
            data: 包含 coin_id 列的数据DataFrame
            
        Returns:
            pd.DataFrame: 合并了 symbol 和 name 字段的数据
        """
        try:
            # 尝试找到项目根目录
            current_path = Path.cwd()
            project_root = None
            
            # 向上寻找包含 data 目录的父目录
            for parent in [current_path] + list(current_path.parents):
                if (parent / 'data' / 'metadata').exists():
                    project_root = parent
                    break
            
            if project_root is None:
                print("⚠️  警告：未找到项目根目录，无法加载元数据")
                return self._add_basic_metadata_from_files(data, None)
                
            metadata_path = project_root / 'data' / 'metadata' / 'native_coins.csv'
            coins_path = project_root / 'data' / 'coins'
            
            # 加载现有元数据
            if metadata_path.exists():
                metadata = pd.read_csv(metadata_path)[['coin_id', 'name', 'symbol']]
                total_metadata_count = len(metadata)
                print(f"📚 加载元数据库: {total_metadata_count} 个币种（包含所有分类）")
            else:
                metadata = pd.DataFrame(columns=['coin_id', 'name', 'symbol'])
                total_metadata_count = 0
                print("📚 元数据文件不存在，将从币种文件补充信息")
            
            # 合并现有数据
            merged_data = data.merge(metadata, on='coin_id', how='left')
            
            # 统计匹配情况
            matched_count = merged_data['symbol'].notna().sum()
            missing_count = len(merged_data) - matched_count
            print(f"🎯 在 {len(data)} 个原生代币中匹配到 {matched_count} 个币种的名称和符号")
            
            if missing_count > 0:
                print(f"⚠️  {missing_count} 个原生代币在元数据库中缺失信息")
                merged_data = self._add_basic_metadata_from_files(merged_data, coins_path)
                
                # 重新统计
                new_matched_count = merged_data['symbol'].notna().sum()
                newly_added = new_matched_count - matched_count
                if newly_added > 0:
                    print(f"📈 从币种文件补充了 {newly_added} 个币种的基础信息")
            
            return merged_data
            
        except Exception as e:
            print(f"⚠️  加载元数据时出错: {e}")
            return self._add_basic_metadata_from_files(data, None)
    
    def _add_basic_metadata_from_files(self, data: pd.DataFrame, coins_path: Optional[Path]) -> pd.DataFrame:
        """从币种CSV文件提取基础元数据信息
        
        Args:
            data: 包含 coin_id 列的数据DataFrame
            coins_path: 币种文件目录路径，如果为None则尝试自动找到
            
        Returns:
            pd.DataFrame: 补充了基础元数据的数据
        """
        if coins_path is None:
            # 尝试自动找到coins目录
            current_path = Path.cwd()
            for parent in [current_path] + list(current_path.parents):
                if (parent / 'data' / 'coins').exists():
                    coins_path = parent / 'data' / 'coins'
                    break
        
        if coins_path is None or not coins_path.exists():
            print("⚠️  未找到币种文件目录，无法补充元数据")
            return data
        
        # 为缺失元数据的币种补充基础信息
        # 如果数据中还没有symbol和name列，先创建空列
        if 'symbol' not in data.columns:
            data['symbol'] = pd.NA
        if 'name' not in data.columns:
            data['name'] = pd.NA
            
        missing_mask = data['symbol'].isna() | data['name'].isna()
        missing_coins = data[missing_mask]['coin_id'].unique()
        
        print(f"🔍 检查 {len(missing_coins)} 个缺失元数据的币种...")
        
        补充信息 = []
        成功计数 = 0
        
        for coin_id in missing_coins:
            coin_file = coins_path / f"{coin_id}.csv"
            if coin_file.exists():
                try:
                    # 读取CSV文件的第一行数据（通常包含最新信息）
                    coin_data = pd.read_csv(coin_file, nrows=1)
                    if not coin_data.empty and 'symbol' in coin_data.columns and 'name' in coin_data.columns:
                        symbol = coin_data['symbol'].iloc[0]
                        name = coin_data['name'].iloc[0]
                        if pd.notna(symbol) and pd.notna(name):
                            补充信息.append({
                                'coin_id': coin_id,
                                'symbol': symbol,
                                'name': name
                            })
                            成功计数 += 1
                except Exception as e:
                    # 静默忽略单个文件读取错误
                    continue
        
        print(f"📁 从币种文件中找到 {成功计数} 个币种的完整信息")
        
        # 如果找到补充信息，更新数据
        if 补充信息:
            补充df = pd.DataFrame(补充信息)
            
            # 更新缺失的字段
            for _, row in 补充df.iterrows():
                coin_id = row['coin_id']
                mask = (data['coin_id'] == coin_id)
                
                # 更新symbol字段（如果缺失）
                symbol_na_mask = mask & data['symbol'].isna()
                if symbol_na_mask.any():
                    data.loc[symbol_na_mask, 'symbol'] = row['symbol']
                
                # 更新name字段（如果缺失）
                name_na_mask = mask & data['name'].isna()
                if name_na_mask.any():
                    data.loc[name_na_mask, 'name'] = row['name']
        
        return data
    
    def clean_data(self, raw_data: pd.DataFrame, 
                   target_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """清理和预处理原始数据
        
        对传入的原始数据进行清理和预处理，包括：
        1. 去除无效数据（缺失关键字段的记录）
        2. 数据类型转换和格式标准化
        3. 币种名称修正
        4. 列筛选和重排序
        
        Args:
            raw_data: 原始数据DataFrame
            target_columns: 目标列名列表，如果为None则保留所有有效列
                          常用列名：rank, symbol, name, price, market_cap, volume, change_24h等
        
        Returns:
            pd.DataFrame: 清理后的数据
            
        Examples:
            >>> displayer = CryptoDataDisplayer()
            >>> clean_data = displayer.clean_data(raw_data, ['rank', 'symbol', 'name', 'price'])
        """
        if raw_data.empty:
            print("⚠️  警告：输入数据为空")
            return pd.DataFrame()
        
        print(f"📊 开始清理数据: {len(raw_data)} 行")
        
        # 创建数据副本避免修改原始数据
        data = raw_data.copy()
        
        # 如果数据只有 coin_id 但没有 symbol 和 name，从元数据加载
        if 'coin_id' in data.columns and ('symbol' not in data.columns or 'name' not in data.columns):
            data = self._add_metadata_fields(data)
        
        # 再次检查并尝试从coin_id直接补充缺失的元数据
        if 'coin_id' in data.columns and ('symbol' in data.columns and 'name' in data.columns):
            missing_symbol = data['symbol'].isna().sum()
            missing_name = data['name'].isna().sum()
            if missing_symbol > 0 or missing_name > 0:
                print(f"🔄 仍有 {missing_symbol} 个symbol和 {missing_name} 个name缺失，尝试最终补充...")
                data = self._add_metadata_fields(data)
        
        # 去除关键字段缺失的记录
        essential_columns = ['symbol', 'name']
        available_essential = [col for col in essential_columns if col in data.columns]
        if available_essential:
            before_count = len(data)
            data = data.dropna(subset=available_essential)
            after_count = len(data)
            if before_count != after_count:
                print(f"📝 移除了 {before_count - after_count} 个缺失关键字段的记录")
        
        # 应用币种名称修正
        if 'name' in data.columns:
            for symbol, corrected_name in self.name_corrections.items():
                if 'symbol' in data.columns:
                    data.loc[data['symbol'] == symbol, 'name'] = corrected_name
        
        # 筛选目标列
        if target_columns:
            available_columns = [col for col in target_columns if col in data.columns]
            if available_columns != target_columns:
                missing_columns = set(target_columns) - set(available_columns)
                print(f"⚠️  警告：以下列不存在于数据中: {missing_columns}")
            data = data[available_columns]
        
        # 重新计算排名（避免跳号）
        if 'rank' in data.columns and 'market_cap' in data.columns:
            # 按市值降序重新排名
            data = data.sort_values('market_cap', ascending=False).reset_index(drop=True)
            data['rank'] = range(1, len(data) + 1)
            print(f"📊 重新计算排名: 1-{len(data)}")
        
        print(f"✅ 数据清理完成: {len(data)} 行，{len(data.columns)} 列")
        return data
    
    def format_crypto_data(self, data: pd.DataFrame, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """格式化加密货币数据用于展示
        
        对清理后的数据进行格式化，包括：
        1. 列选择和重排序
        2. 数值格式化（价格、市值等）
        3. 列名本地化（英文转中文）
        4. 百分比和货币格式化
        
        Args:
            data: 清理后的数据DataFrame
            columns: 要显示的列名列表，默认显示 ['rank', 'symbol', 'name', 'price', 'market_cap']
        
        Returns:
            pd.DataFrame: 格式化后的数据，列名已本地化，数值已格式化
            
        Examples:
            >>> displayer = CryptoDataDisplayer()
            >>> formatted_data = displayer.format_crypto_data(clean_data, ['rank', 'symbol', 'name', 'price'])
        """
        if columns is None:
            columns = ['rank', 'symbol', 'name', 'price', 'market_cap']
        
        # 处理空数据框的情况
        if data.empty:
            print("⚠️  警告：数据为空，无法格式化")
            return pd.DataFrame()
        
        # 选择显示列
        available_columns = [col for col in columns if col in data.columns]
        if not available_columns:
            raise ValueError(f"没有找到指定的列: {columns}")
            
        display_data = data[available_columns].copy()
        
        # 修正币种名称
        if 'name' in display_data.columns:
            display_data['name'] = display_data['name'].replace(self.name_corrections)
        
        # 符号大写转换
        if 'symbol' in display_data.columns:
            display_data['symbol'] = display_data['symbol'].str.upper()
        
        # 格式化数值列
        if 'price' in display_data.columns:
            display_data['price'] = display_data['price'].apply(lambda x: f"{x:,.4f}" if pd.notna(x) else "N/A")
        
        if 'market_cap' in display_data.columns:
            # 市值以十亿美元为单位显示 (1B$)，保持整数与千分位
            display_data['market_cap'] = display_data['market_cap'].apply(
                lambda x: f"{x/1_000_000_000:,.0f}" if pd.notna(x) and x > 0 else "N/A"
            )
        
        if 'volume' in display_data.columns:
            display_data['volume'] = display_data['volume'].apply(
                lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
            )
        
        # 格式化百分比列
        # 百分比列格式化：权重不加百分号，其它涨跌幅保留百分号
        if 'weight' in display_data.columns:
            display_data['weight'] = display_data['weight'].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "N/A"
            )
        for pct_col in [c for c in ['change_24h', 'change_7d'] if c in display_data.columns]:
            display_data[pct_col] = display_data[pct_col].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"
            )
        
        # 应用列名映射（英文转中文）
        display_data = display_data.rename(columns=self.column_mapping)
        
        return display_data
    
    def show_table(self, data: pd.DataFrame,
                   columns: Optional[List[str]] = None,
                   top_n: Optional[int] = None,
                   title: Optional[str] = None,
                   page_size: int = 50,
                   show_info: bool = False) -> Optional[pd.DataFrame]:
        """展示格式化的数据表格
        
        整合数据格式化和表格显示功能，提供完整的展示流程：
        1. 数据格式化
        2. 行数筛选（前N行）
        3. 表格显示（Jupyter环境优化）
        4. 汇总信息输出
        
        Args:
            data: 要展示的数据DataFrame
            columns: 要显示的列名列表
            top_n: 显示前N行（在分页之前裁剪）；None 表示不裁剪
            title: 表格标题
            page_size: 分页大小（仅 Jupyter 显示时生效）；若数据行数 > page_size，则分页
            show_info: 是否显示行数提示（默认关闭，保持输出极简）
        
        Returns:
            pd.DataFrame: 格式化后的展示数据
            
        Examples:
            >>> displayer = CryptoDataDisplayer()
            >>> displayer.show_table(data, columns=['rank', 'symbol', 'name', 'price'], top_n=10)
        """
        if data.empty:
            print("⚠️  没有数据可显示")
            return pd.DataFrame()
        
        # 格式化数据
        formatted_data = self.format_crypto_data(data, columns)
        
        # 筛选前N行（如果指定）
        if top_n is not None:
            display_subset = formatted_data.head(top_n)
        else:
            display_subset = formatted_data

        total_rows = len(display_subset)

        # 标题
        if title:
            print(f"\n📊 {title}")
        if show_info:
            print(f"(rows={total_rows})")
        
        # 在Jupyter环境中优化显示
        if IPYTHON_AVAILABLE:
            # 分页显示：若行数超过 page_size，分块展示
            if total_rows > page_size and page_size > 0:
                for start in range(0, total_rows, page_size):
                    end = min(start + page_size, total_rows)
                    if show_info:
                        print(f"第 {start+1}-{end} 行 / 共 {total_rows} 行")
                    display(display_subset.iloc[start:end])
            else:
                display(display_subset)
            # 返回None避免Jupyter自动显示返回值
            return None
        else:
            print(display_subset.to_string(index=False))
            return display_subset
