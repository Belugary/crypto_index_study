"""
市值加权指数计算器

基于市值加权方式计算区块链资产指数
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import logging

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.classification.stablecoin_checker import StablecoinChecker
from src.classification.wrapped_coin_checker import WrappedCoinChecker


class MarketCapWeightedIndexCalculator:
    """市值加权指数计算器"""

    def __init__(
        self,
        data_dir: str = "data/coins",
        exclude_stablecoins: bool = True,
        exclude_wrapped_coins: bool = True,
    ):
        """
        初始化指数计算器

        Args:
            data_dir: 价格数据目录路径
            exclude_stablecoins: 是否排除稳定币
            exclude_wrapped_coins: 是否排除包装币
        """
        self.data_dir = Path(data_dir)
        self.exclude_stablecoins = exclude_stablecoins
        self.exclude_wrapped_coins = exclude_wrapped_coins

        # 初始化分类器
        if exclude_stablecoins:
            self.stablecoin_checker = StablecoinChecker()
        if exclude_wrapped_coins:
            self.wrapped_coin_checker = WrappedCoinChecker()

        # 设置日志
        self.logger = logging.getLogger(__name__)

    def _load_coin_data(self, coin_id: str) -> Optional[pd.DataFrame]:
        """
        加载单个币种的价格数据

        Args:
            coin_id: 币种ID（文件名不含.csv后缀）

        Returns:
            包含价格数据的DataFrame，如果文件不存在返回None
        """
        csv_path = self.data_dir / f"{coin_id}.csv"
        if not csv_path.exists():
            return None

        try:
            df = pd.read_csv(csv_path)
            # 转换时间戳为日期
            df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            df = df.sort_values("date")
            return df
        except Exception as e:
            self.logger.warning(f"读取 {coin_id} 数据失败: {e}")
            return None

    def _get_available_coins(self) -> List[str]:
        """
        获取所有可用的币种ID列表

        Returns:
            币种ID列表
        """
        coins = []
        for csv_file in self.data_dir.glob("*.csv"):
            coin_id = csv_file.stem

            # 检查是否需要排除稳定币
            if self.exclude_stablecoins:
                try:
                    stablecoin_result = self.stablecoin_checker.is_stablecoin(coin_id)
                    if stablecoin_result.get("is_stablecoin", False):
                        continue
                except Exception:
                    # 如果检查失败，保守处理，不排除
                    pass

            # 检查是否需要排除包装币
            if self.exclude_wrapped_coins:
                try:
                    wrapped_result = self.wrapped_coin_checker.is_wrapped_coin(coin_id)
                    if wrapped_result.get("is_wrapped_coin", False):
                        continue
                except Exception:
                    # 如果检查失败，保守处理，不排除
                    pass

            coins.append(coin_id)

        return coins

    def _get_daily_market_caps(self, target_date: date) -> Dict[str, float]:
        """
        获取指定日期所有币种的市值

        Args:
            target_date: 目标日期

        Returns:
            币种ID到市值的映射字典
        """
        market_caps = {}
        available_coins = self._get_available_coins()

        for coin_id in available_coins:
            df = self._load_coin_data(coin_id)
            if df is None:
                continue

            # 找到目标日期的数据
            target_data = df[df["date"] == target_date]
            if target_data.empty:
                continue

            # 取最新的记录（如果有多条）
            latest_record = target_data.iloc[-1]
            market_cap = latest_record["market_cap"]

            # 确保市值数据有效
            if pd.notna(market_cap) and market_cap > 0:
                market_caps[coin_id] = float(market_cap)

        return market_caps

    def _select_top_coins(self, market_caps: Dict[str, float], top_n: int) -> List[str]:
        """
        根据市值选择前N名币种

        Args:
            market_caps: 币种市值字典
            top_n: 选择数量

        Returns:
            按市值排序的前N名币种ID列表
        """
        sorted_coins = sorted(market_caps.items(), key=lambda x: x[1], reverse=True)
        return [coin_id for coin_id, _ in sorted_coins[:top_n]]

    def _calculate_weights(
        self, coin_ids: List[str], market_caps: Dict[str, float]
    ) -> Dict[str, float]:
        """
        计算各币种的权重

        Args:
            coin_ids: 成分币种ID列表
            market_caps: 币种市值字典

        Returns:
            币种ID到权重的映射字典
        """
        total_market_cap = sum(market_caps[coin_id] for coin_id in coin_ids)

        weights = {}
        for coin_id in coin_ids:
            weights[coin_id] = market_caps[coin_id] / total_market_cap

        return weights

    def _get_coin_price(self, coin_id: str, target_date: date) -> Optional[float]:
        """
        获取指定币种在指定日期的价格

        Args:
            coin_id: 币种ID
            target_date: 目标日期

        Returns:
            价格，如果无数据返回None
        """
        df = self._load_coin_data(coin_id)
        if df is None:
            return None

        target_data = df[df["date"] == target_date]
        if target_data.empty:
            return None

        latest_record = target_data.iloc[-1]
        price = latest_record["price"]

        return float(price) if pd.notna(price) and price > 0 else None

    def calculate_index(
        self,
        start_date: str,
        end_date: str,
        base_date: str = "2020-01-01",
        base_value: float = 1000.0,
        top_n: int = 30,
    ) -> pd.DataFrame:
        """
        计算市值加权指数

        Args:
            start_date: 指数计算开始日期 (YYYY-MM-DD)
            end_date: 指数计算结束日期 (YYYY-MM-DD)
            base_date: 基准日期 (YYYY-MM-DD)
            base_value: 基准指数值
            top_n: 每日选择的成分币种数量

        Returns:
            包含指数数据的DataFrame，列为：date, index_value, constituent_count
        """
        self.logger.info(f"开始计算市值加权指数: {start_date} 到 {end_date}")
        self.logger.info(f"基准日期: {base_date}, 基准值: {base_value}")
        self.logger.info(f"每日成分币种数: {top_n}")
        self.logger.info(
            f"排除稳定币: {self.exclude_stablecoins}, 排除包装币: {self.exclude_wrapped_coins}"
        )

        # 转换日期字符串为date对象
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        base_dt = datetime.strptime(base_date, "%Y-%m-%d").date()

        # 获取基准日期的市值数据和成分币种
        base_market_caps = self._get_daily_market_caps(base_dt)
        if not base_market_caps:
            raise ValueError(f"基准日期 {base_date} 没有可用的市值数据")

        base_constituents = self._select_top_coins(base_market_caps, top_n)
        actual_base_count = len(base_constituents)

        if actual_base_count < top_n:
            self.logger.warning(
                f"基准日期只找到 {actual_base_count} 个币种，少于目标 {top_n} 个"
            )
            self.logger.info(f"将使用所有可用的 {actual_base_count} 个币种作为基准成分")
            # 更新top_n为实际可用数量，确保后续计算的一致性
            effective_top_n = actual_base_count
        else:
            effective_top_n = top_n

        base_weights = self._calculate_weights(base_constituents, base_market_caps)

        # 获取基准日期各币种价格
        base_prices = {}
        for coin_id in base_constituents:
            price = self._get_coin_price(coin_id, base_dt)
            if price is None:
                raise ValueError(f"基准日期 {base_date} 缺少币种 {coin_id} 的价格数据")
            base_prices[coin_id] = price

        # 计算基准日期的加权价格
        base_weighted_price = sum(
            base_weights[coin_id] * base_prices[coin_id]
            for coin_id in base_constituents
        )

        # 生成日期范围
        date_range = pd.date_range(start=start_dt, end=end_dt, freq="D")

        index_data = []

        for current_dt in date_range:
            current_date = current_dt.date()

            # 获取当日市值数据和成分币种
            current_market_caps = self._get_daily_market_caps(current_date)
            if not current_market_caps:
                self.logger.warning(f"日期 {current_date} 没有可用的市值数据，跳过")
                continue

            current_constituents = self._select_top_coins(
                current_market_caps, effective_top_n
            )
            if not current_constituents:
                self.logger.warning(f"日期 {current_date} 没有找到成分币种，跳过")
                continue

            current_weights = self._calculate_weights(
                current_constituents, current_market_caps
            )

            # 获取当日各币种价格
            current_prices = {}
            missing_prices = []

            for coin_id in current_constituents:
                price = self._get_coin_price(coin_id, current_date)
                if price is None:
                    missing_prices.append(coin_id)
                else:
                    current_prices[coin_id] = price

            # 如果有缺失价格，报错并显示详情
            if missing_prices:
                error_msg = (
                    f"日期 {current_date} 缺少以下币种的价格数据: "
                    f"{', '.join(missing_prices)}"
                )
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            # 计算当日加权价格
            current_weighted_price = sum(
                current_weights[coin_id] * current_prices[coin_id]
                for coin_id in current_constituents
            )

            # 计算指数值
            index_value = base_value * (current_weighted_price / base_weighted_price)

            index_data.append(
                {
                    "date": current_date,
                    "index_value": index_value,
                    "constituent_count": len(current_constituents),
                }
            )

            if len(index_data) % 100 == 0:
                self.logger.info(f"已处理 {len(index_data)} 个交易日")

        if not index_data:
            raise ValueError(
                f"无法生成指数数据，时间范围 {start_date} 到 {end_date} 内没有可用数据"
            )

        result_df = pd.DataFrame(index_data)

        self.logger.info(f"指数计算完成，共生成 {len(result_df)} 个数据点")
        self.logger.info(
            f"指数范围: {result_df['index_value'].min():.2f} - {result_df['index_value'].max():.2f}"
        )

        return result_df

    def save_index(self, index_df: pd.DataFrame, output_path: str) -> None:
        """
        保存指数数据到CSV文件

        Args:
            index_df: 指数数据DataFrame
            output_path: 输出文件路径
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        index_df.to_csv(output_file, index=False, float_format="%.6f")
        self.logger.info(f"指数数据已保存到: {output_file}")
