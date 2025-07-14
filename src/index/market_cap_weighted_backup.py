"""
市值加权指数计算器

基于市值加权方式计算区块链资产指数
"""

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.classification.stablecoin_checker import StablecoinChecker
from src.classification.wrapped_coin_checker import WrappedCoinChecker
from src.downloaders.daily_aggregator import DailyDataAggregator


class MarketCapWeightedIndexCalculator:
    """市值加权指数计算器

    重要说明：
    - 使用 data/daily/daily_files/ 目录下的每日汇总数据
    - 每日数据文件格式：timestamp,price,volume,market_cap,date,coin_id,rank
    - 数据已按市值排序并包含排名信息
    - 支持自动排除稳定币和包装币
    """

    def __init__(
        self,
        daily_data_dir: str = "data/daily",
        exclude_stablecoins: bool = True,
        exclude_wrapped_coins: bool = True,
    ):
        """
        初始化指数计算器

        Args:
            daily_data_dir: 每日数据目录路径 (data/daily)
            exclude_stablecoins: 是否排除稳定币
            exclude_wrapped_coins: 是否排除包装币
        """
        self.daily_data_dir = Path(daily_data_dir)
        self.exclude_stablecoins = exclude_stablecoins
        self.exclude_wrapped_coins = exclude_wrapped_coins

        # 初始化每日数据聚合器
        self.daily_aggregator = DailyDataAggregator(
            data_dir="data/coins",  # 原始数据源（备用）
            output_dir=str(self.daily_data_dir),
        )

        # 初始化分类器
        if exclude_stablecoins:
            self.stablecoin_checker = StablecoinChecker()
        if exclude_wrapped_coins:
            self.wrapped_coin_checker = WrappedCoinChecker()

        # 设置日志
        self.logger = logging.getLogger(__name__)

    def _get_daily_market_caps(self, target_date: date) -> Dict[str, float]:
        """
        获取指定日期的所有币种市值数据

        Args:
            target_date: 目标日期

        Returns:
            币种ID到市值的映射字典
        """
        try:
            # 从每日汇总数据获取
            daily_df = self.daily_aggregator.get_daily_data(target_date)

            if daily_df.empty:
                return {}

            # 确保数据格式正确
            if (
                "coin_id" not in daily_df.columns
                or "market_cap" not in daily_df.columns
            ):
                self.logger.error(f"日期 {target_date} 的数据格式不正确，缺少必要列")
                return {}

            # 过滤稳定币和包装币
            filtered_df = self._filter_coins(daily_df)

            # 转换为字典
            market_caps = filtered_df.set_index("coin_id")["market_cap"].to_dict()

            self.logger.debug(
                f"日期 {target_date}: 获取到 {len(market_caps)} 个币种的市值数据"
            )
            return market_caps

        except Exception as e:
            self.logger.error(f"获取日期 {target_date} 的市值数据失败: {e}")
            return {}

    def _filter_coins(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        过滤稳定币和包装币

        Args:
            df: 包含币种数据的DataFrame

        Returns:
            过滤后的DataFrame
        """
        if df.empty or "coin_id" not in df.columns:
            return df

        filtered_df = df.copy()

        # 过滤稳定币
        if self.exclude_stablecoins:
            stable_mask = []
            for coin_id in df["coin_id"]:
                try:
                    stablecoin_result = self.stablecoin_checker.is_stablecoin(coin_id)
                    is_stable = stablecoin_result.get("is_stablecoin", False)
                    stable_mask.append(not is_stable)  # 保留非稳定币
                except Exception:
                    stable_mask.append(True)  # 检查失败时保留

            filtered_df = filtered_df[stable_mask]
            self.logger.debug(f"稳定币过滤: {len(df)} -> {len(filtered_df)}")

        # 过滤包装币
        if self.exclude_wrapped_coins:
            wrapped_mask = []
            for coin_id in filtered_df["coin_id"]:
                try:
                    wrapped_result = self.wrapped_coin_checker.is_wrapped_coin(coin_id)
                    is_wrapped = wrapped_result.get("is_wrapped_coin", False)
                    wrapped_mask.append(not is_wrapped)  # 保留非包装币
                except Exception:
                    wrapped_mask.append(True)  # 检查失败时保留

            final_df = filtered_df[wrapped_mask]
            self.logger.debug(f"包装币过滤: {len(filtered_df)} -> {len(final_df)}")
            filtered_df = final_df

        return filtered_df

    def _select_top_coins(self, market_caps: Dict[str, float], top_n: int) -> List[str]:
        """
        根据市值选择前N名币种

        Args:
            market_caps: 币种ID到市值的映射
            top_n: 选择的币种数量

        Returns:
            选中的币种ID列表（按市值降序）
        """
        if not market_caps:
            return []

        # 按市值排序并选择前N名
        sorted_coins = sorted(market_caps.items(), key=lambda x: x[1], reverse=True)
        top_coins = [coin_id for coin_id, _ in sorted_coins[:top_n]]

        return top_coins

    def _calculate_weights(
        self, coin_ids: List[str], market_caps: Dict[str, float]
    ) -> Dict[str, float]:
        """
        计算市值权重

        Args:
            coin_ids: 币种ID列表
            market_caps: 币种市值映射

        Returns:
            币种权重映射
        """
        total_market_cap = sum(market_caps[coin_id] for coin_id in coin_ids)

        if total_market_cap == 0:
            return {}

        weights = {
            coin_id: market_caps[coin_id] / total_market_cap for coin_id in coin_ids
        }

        return weights

    def _get_coin_price(self, coin_id: str, target_date: date) -> Optional[float]:
        """
        获取指定币种在指定日期的价格

        Args:
            coin_id: 币种ID
            target_date: 目标日期

        Returns:
            价格，如果不存在返回None
        """
        try:
            daily_df = self.daily_aggregator.get_daily_data(target_date)

            if daily_df.empty:
                return None

            coin_data = daily_df[daily_df["coin_id"] == coin_id]

            if coin_data.empty:
                return None

            return float(coin_data.iloc[0]["price"])

        except Exception as e:
            self.logger.warning(f"获取 {coin_id} 在 {target_date} 的价格失败: {e}")
            return None

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

    # ... 保留原有的其他方法以维持兼容性 ...
