"""
市值加权指数计算器

基于市值加权方式计算区块链资产指数

⚠️ 重要数据说明:
- 使用的市值为流通市值 (Circulating Market Cap)，非完全稀释市值
- 流通市值 = 当前价格 × 流通供应量
- 这是金融市场和指数编制的标准做法，更能反映真实可交易价值
- 数据来源: CoinGecko API 的历史图表数据
"""

import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.classification.unified_classifier import UnifiedClassifier
from src.downloaders.daily_aggregator import DailyDataAggregator


class MarketCapWeightedIndexCalculator:
    """市值加权指数计算器"""

    def __init__(
        self,
        data_dir: str = "data/coins",
        daily_output_dir: str = "data/daily",
        exclude_stablecoins: bool = True,
        exclude_wrapped_coins: bool = True,
        force_rebuild: bool = False,
    ):
        """
        初始化指数计算器

        Args:
            data_dir: 原始价格数据目录路径
            daily_output_dir: 每日汇总数据的输出目录
            exclude_stablecoins: 是否排除稳定币
            exclude_wrapped_coins: 是否排除包装币
            force_rebuild: 是否强制重建每日数据文件

        注意：
        - 核心数据来源：{daily_output_dir}/daily_files/
        """
        self.data_dir = Path(data_dir)
        self.daily_output_dir = Path(daily_output_dir)
        self.exclude_stablecoins = exclude_stablecoins
        self.exclude_wrapped_coins = exclude_wrapped_coins
        self.force_rebuild = force_rebuild

        # 初始化每日数据聚合器 - 核心数据源
        self.daily_aggregator = DailyDataAggregator(
            data_dir=str(self.data_dir), output_dir=str(self.daily_output_dir)
        )

        # 初始化统一分类器 (替代原有的两个分离分类器)
        self.classifier = UnifiedClassifier()

        # 保持向后兼容：记录过滤设置
        self.exclude_stablecoins = exclude_stablecoins
        self.exclude_wrapped_coins = exclude_wrapped_coins

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

            # 使用统一分类器进行过滤 (性能优化：一次调用获取所有分类信息)
            if self.exclude_stablecoins or self.exclude_wrapped_coins:
                try:
                    classification_result = self.classifier.classify_coin(coin_id)

                    # 检查是否需要排除稳定币
                    if self.exclude_stablecoins and classification_result.is_stablecoin:
                        continue

                    # 检查是否需要排除包装币
                    if (
                        self.exclude_wrapped_coins
                        and classification_result.is_wrapped_coin
                    ):
                        continue
                except Exception:
                    # 如果检查失败，保守处理，不排除
                    pass

            coins.append(coin_id)

        return coins

    def _get_daily_data_cached(self, target_date: date) -> pd.DataFrame:
        """
        获取指定日期的每日数据（带缓存）

        Args:
            target_date: 目标日期

        Returns:
            当日所有币种数据的DataFrame
        """
        # 检查是否已经在缓存中
        cache_key = target_date.isoformat()
        if hasattr(self, "_daily_cache") and cache_key in self._daily_cache:
            return self._daily_cache[cache_key]

        # 初始化缓存
        if not hasattr(self, "_daily_cache"):
            self._daily_cache = {}

        # 从数据源获取（只有第一次会强制刷新）
        force_refresh = self.force_rebuild and cache_key not in self._daily_cache
        daily_df = self.daily_aggregator.get_daily_data(
            target_date, force_refresh=force_refresh
        )

        # 缓存结果
        self._daily_cache[cache_key] = daily_df
        return daily_df

    def _get_daily_market_caps(self, target_date: date) -> Dict[str, float]:
        """
        获取指定日期所有币种的市值

        使用每日汇总数据源而非单独的币种文件

        Args:
            target_date: 目标日期

        Returns:
            币种ID到市值的映射字典

        ⚠️ 重要: 返回的是流通市值 (Circulating Market Cap)
        - 流通市值 = 当前价格 × 流通供应量
        - 用于指数权重计算和排名筛选
        - 符合传统金融指数编制标准
        """
        try:
            # 使用缓存的数据获取方法
            daily_df = self._get_daily_data_cached(target_date)

            if daily_df.empty:
                self.logger.warning(f"日期 {target_date} 没有可用的每日汇总数据")
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

            # 转换为字典，只保留有效的市值数据
            market_caps = {}
            for _, row in filtered_df.iterrows():
                market_cap = row["market_cap"]
                if pd.notna(market_cap) and market_cap > 0:
                    market_caps[row["coin_id"]] = float(market_cap)

            self.logger.debug(
                f"日期 {target_date}: 获取到 {len(market_caps)} 个币种的市值数据"
            )
            return market_caps

        except Exception as e:
            self.logger.error(f"获取日期 {target_date} 的市值数据失败: {e}")
            return {}

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

        使用每日汇总数据源而非单独的币种文件

        Args:
            coin_id: 币种ID
            target_date: 目标日期

        Returns:
            价格，如果无数据返回None
        """
        try:
            # 使用缓存的数据获取方法
            daily_df = self._get_daily_data_cached(target_date)

            if daily_df.empty:
                return None

            coin_data = daily_df[daily_df["coin_id"] == coin_id]

            if coin_data.empty:
                return None

            price = coin_data.iloc[0]["price"]
            return float(price) if pd.notna(price) and price > 0 else None

        except Exception as e:
            self.logger.warning(f"获取 {coin_id} 在 {target_date} 的价格失败: {e}")
            return None

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

        # 使用统一分类器进行批量过滤 (性能优化)
        if self.exclude_stablecoins or self.exclude_wrapped_coins:
            coin_ids = df["coin_id"].tolist()

            # 批量过滤，一次调用处理所有币种
            filtered_coin_ids = self.classifier.filter_coins(
                coin_ids=coin_ids,
                exclude_stablecoins=self.exclude_stablecoins,
                exclude_wrapped_coins=self.exclude_wrapped_coins,
                use_cache=True,
            )

            # 只保留过滤后的币种
            filtered_df = filtered_df[filtered_df["coin_id"].isin(filtered_coin_ids)]

            self.logger.debug(
                f"分类过滤: {len(df)} -> {len(filtered_df)} "
                f"(排除稳定币: {self.exclude_stablecoins}, 排除包装币: {self.exclude_wrapped_coins})"
            )

        return filtered_df

    def calculate_index(
        self,
        start_date: Union[str, date],
        end_date: Union[str, date],
        top_n: int = 510,
        base_date: Optional[Union[str, date]] = None,
        base_value: float = 1000.0,
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
        if self.force_rebuild:
            self.logger.info(
                "已启用强制重建每日数据功能，将使用最新的原始数据生成每日汇总"
            )

        # 转换日期字符串为date对象
        if isinstance(start_date, str):
            start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        else:
            start_dt = start_date

        if isinstance(end_date, str):
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        else:
            end_dt = end_date

        if base_date is None:
            base_dt = start_dt
        elif isinstance(base_date, str):
            base_dt = datetime.strptime(base_date, "%Y-%m-%d").date()
        else:
            base_dt = base_date

        # 获取基准日期的市值数据和成分币种
        base_market_caps = self._get_daily_market_caps(base_dt)
        if not base_market_caps:
            raise ValueError(f"基准日期 {base_date} 没有可用的市值数据")

        base_constituents = self._select_top_coins(base_market_caps, top_n)
        actual_base_count = len(base_constituents)

        if actual_base_count < top_n:
            raise ValueError(
                f"基准日期 {base_date} 只有 {actual_base_count} 个可用币种，"
                f"不足以满足 top_n={top_n} 的要求。"
            )

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

        # 使用进度条显示计算进度
        with tqdm(
            total=len(date_range),
            desc=f"计算市值加权指数 (前{top_n}名)",
            unit="天",
            ncols=100,
        ) as pbar:
            for i, current_dt in enumerate(date_range):
                current_date = current_dt.date()

                # 更新进度条显示当前日期
                pbar.set_description(
                    f"计算市值加权指数 - {current_date} ({i+1}/{len(date_range)})"
                )

                # 获取当日市值数据和成分币种
                current_market_caps = self._get_daily_market_caps(current_date)
                if not current_market_caps:
                    self.logger.warning(f"日期 {current_date} 没有可用的市值数据，跳过")
                    pbar.set_postfix_str(f"跳过: {current_date}")
                    pbar.update(1)
                    continue

                current_constituents = self._select_top_coins(
                    current_market_caps, top_n
                )
                if not current_constituents:
                    self.logger.warning(f"日期 {current_date} 没有找到成分币种，跳过")
                    pbar.set_postfix_str(f"跳过: {current_date}")
                    pbar.update(1)
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
                    pbar.set_description("计算中断 - 缺少价格数据")
                    raise ValueError(error_msg)

                # 计算当日加权价格
                current_weighted_price = sum(
                    current_weights[coin_id] * current_prices[coin_id]
                    for coin_id in current_constituents
                )

                # 计算指数值
                index_value = base_value * (
                    current_weighted_price / base_weighted_price
                )

                index_data.append(
                    {
                        "date": current_date,
                        "index_value": index_value,
                        "constituent_count": len(current_constituents),
                    }
                )

                # 更新进度条
                pbar.set_postfix_str(f"指数值: {index_value:.2f}")
                pbar.update(1)

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
