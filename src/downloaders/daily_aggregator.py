#!/usr/bin/env python3
"""
每日数据聚合器

基于已下载的历史数据，构建按日期组织的数据集合，
用于分析每日市场构成和构建历史指数。

⚠️ 数据字段说明:
- price: 当日价格 (USD)
- volume: 当日24小时交易量 (USD)
- market_cap: 当日流通市值 (USD)
  重要: 这是流通市值 (Circulating Market Cap)，计算公式为价格×流通供应量
  这是金融指数编制的标准做法，更准确反映可交易价值
"""

import glob
import logging
import multiprocessing
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DailyDataAggregator:
    """每日数据聚合器

    功能：
    1. 扫描所有已下载的币种数据
    2. 按日期聚合数据，构建每日市场快照
    3. 提供查询指定日期的市场数据功能
    4. 分析数据覆盖范围和质量
    """

    @staticmethod
    def _find_project_root() -> Path:
        """查找项目根目录（包含.git目录或同时包含src和requirements.txt的目录）"""
        cur = Path.cwd()
        project_root = cur
        
        # 查找包含项目标志的目录
        while project_root.parent != project_root:
            # 最可靠的标志是.git目录
            if (project_root / ".git").exists():
                break
            # 或者同时包含src目录和requirements.txt文件（更严格的项目根目录判断）
            elif (project_root / "src").exists() and (project_root / "requirements.txt").exists():
                break
            project_root = project_root.parent
        
        return project_root

    @staticmethod
    def read_daily_snapshot(date_str: str, daily_dir: str = "data/daily/daily_files", result_include_all: bool = False) -> pd.DataFrame:
        """
        📁 静态方法：读取已聚合的每日市场快照 CSV
        
        🎯 优势: 不需要实例化 DailyDataAggregator，直接读取文件
        📌 用途: 适合快速获取历史数据，避免重复加载所有币种数据
        
        Args:
            date_str: 日期字符串，格式为 'YYYY-MM-DD'
            daily_dir: 每日快照文件夹路径，默认 'data/daily/daily_files'
            result_include_all: 是否包含所有币种
                              - True: 返回全部币种 (包括稳定币、包装币)
                              - False: 只返回原生币种 (排除稳定币、包装币)

        Returns:
            指定日期的市场快照 DataFrame，根据result_include_all参数过滤
            若文件不存在则返回空 DataFrame
        """
        file_path = Path(daily_dir) / f"{date_str}.csv"
        if not file_path.exists():
            return pd.DataFrame()
        
        df = pd.read_csv(file_path)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        
        # 如果需要，过滤出原生币种
        if not result_include_all and not df.empty:
            from src.classification.unified_classifier import UnifiedClassifier
            
            # 推断数据根目录
            data_dir = Path(daily_dir).parent.parent
            classifier = UnifiedClassifier(data_dir=str(data_dir))
            coin_ids = df['coin_id'].unique().tolist()
            
            native_coin_ids = classifier.filter_coins(
                coin_ids=coin_ids,
                exclude_stablecoins=True,
                exclude_wrapped_coins=True,
                use_cache=True
            )
            
            df = df[df['coin_id'].isin(native_coin_ids)].copy()
            
        return df

    def __init__(self, data_dir: Optional[str] = None, output_dir: Optional[str] = None, result_include_all: bool = False):
        """
        初始化每日数据聚合器
        
        📁 路径管理: 自动定位项目根目录，避免在子目录创建错误的data文件夹
        ⚠️ 废弃参数: result_include_all 在构造函数中已废弃，请在 get_daily_data() 方法中使用

        Args:
            data_dir: 原始CSV数据目录（可选，默认自动定位项目根目录下的data/coins）
            output_dir: 聚合后数据输出目录（可选，默认自动定位项目根目录下的data/daily）
            result_include_all: [已废弃] 此参数在构造函数中无效，请在调用 get_daily_data() 时指定
                              保留此参数仅为向后兼容，建议使用方法级别的参数控制
        """
        # 自动查找项目根目录（包含.git、README.md或src目录的目录）
        self.project_root = self._find_project_root()
        
        # 默认路径：始终基于项目根目录，避免在子目录创建文件夹
        if data_dir is None:
            data_dir = str(self.project_root / "data" / "coins")
        if output_dir is None:
            output_dir = str(self.project_root / "data" / "daily")
        
        # 路径解析：如果是相对路径，基于项目根目录解析
        self.data_dir = Path(data_dir) if Path(data_dir).is_absolute() else self.project_root / data_dir
        self.output_dir = Path(output_dir) if Path(output_dir).is_absolute() else self.project_root / output_dir
        
        # 只在项目根目录创建文件夹（除非显式指定绝对路径）
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 保存 result_include_all 设置
        self.result_include_all = result_include_all
        
        # 设置日志目录：始终在项目根目录
        self.log_folder = self.project_root / "logs"
        self.log_folder.mkdir(parents=True, exist_ok=True)

        # 初始化logger
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

        # 创建子目录用于存储不同类型的数据
        self.daily_files_dir = self.output_dir / "daily_files"
        self.daily_files_dir.mkdir(parents=True, exist_ok=True)

        # 缓存
        self.daily_cache: Dict[str, pd.DataFrame] = {}
        self.coin_data: Dict[str, pd.DataFrame] = {}
        self.loaded_coins: List[str] = []
        logger.info(
            f"每日数据聚合器初始化, 数据源: '{self.data_dir}', 输出到: '{self.output_dir}'"
        )

        # 日期范围信息
        self.min_date: Optional[datetime] = None
        self.max_date: Optional[datetime] = None

        logger.info(f"初始化每日数据聚合器")
        logger.info(f"数据目录: {self.data_dir}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"每日文件目录: {self.daily_files_dir}")

    def load_coin_data(self) -> None:
        """加载所有币种的CSV数据到内存"""
        logger.info("开始从CSV文件加载所有币种数据到内存...")
        csv_files = list(self.data_dir.glob("*.csv"))
        if not csv_files:
            logger.warning(f"数据目录 '{self.data_dir}' 中没有找到CSV文件。")
            return

        for file_path in csv_files:
            coin_id = file_path.stem
            try:
                df = pd.read_csv(file_path)
                if df.empty:
                    logger.warning(f"跳过空文件: {file_path}")
                    continue

                # 转换时间戳并创建 'date' 列
                df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce")
                df.dropna(subset=["timestamp"], inplace=True)
                df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime(
                    "%Y-%m-%d"
                )
                df["coin_id"] = coin_id
                self.coin_data[coin_id] = df
                self.loaded_coins.append(coin_id)
                logger.debug(f"成功加载 {coin_id} ({len(df)}条记录)")
            except Exception as e:
                logger.error(f"加载文件 {file_path} 失败: {e}")

        logger.info(f"成功加载 {len(self.loaded_coins)} 个币种的数据。")

    def _calculate_date_range(self) -> None:
        """计算所有数据的日期范围"""
        if not self.coin_data:
            return

        all_dates = []
        for df in self.coin_data.values():
            all_dates.extend(df["date"].tolist())

        if all_dates:
            # 将字符串日期转换为 datetime 对象以支持日期运算
            from datetime import datetime

            date_objects = [datetime.strptime(d, "%Y-%m-%d") for d in all_dates]
            self.min_date = min(date_objects)
            self.max_date = max(date_objects)

            logger.info(f"数据日期范围: {self.min_date} 到 {self.max_date}")

            # 计算总天数
            if self.min_date and self.max_date:
                total_days = (self.max_date - self.min_date).days + 1
                logger.info(f"总共 {total_days} 天的数据")

    def get_daily_data(
        self, target_date: Union[str, datetime, date], force_refresh: bool = False, result_include_all: bool = False
    ) -> pd.DataFrame:
        """
        获取指定日期的聚合市场数据

        ⭐ 核心参数说明（最近修复重点）:
        - force_refresh: 控制数据来源 (缓存 vs 重新计算)
        - result_include_all: 控制过滤逻辑 (原生币 vs 全部币种)
        
        📊 数据获取优先级:
        1. 内存缓存 (force_refresh=False 且存在)
        2. 文件缓存 (force_refresh=False 且文件存在) 
        3. 重新计算 (force_refresh=True 或无缓存)
        
        🔍 过滤逻辑应用:
        - result_include_all=True: 包含稳定币、包装币等所有币种
        - result_include_all=False: 只包含原生币种，排除稳定币和包装币
        - ⚠️ 过滤在所有数据获取路径后统一应用

        Args:
            target_date: 目标日期，支持字符串、datetime或date类型
            force_refresh: 是否强制刷新，忽略所有缓存 (内存+文件)
            result_include_all: 结果是否包含所有币种，False时只返回原生币种

        Returns:
            包含指定日期市场数据的DataFrame，根据result_include_all参数过滤
        """
        if isinstance(target_date, str):
            try:
                target_date_dt = datetime.strptime(target_date, "%Y-%m-%d")
            except ValueError:
                logger.error(f"无效的日期格式: {target_date}，应为 YYYY-MM-DD")
                return pd.DataFrame()
        elif isinstance(target_date, datetime):
            target_date_dt = target_date
        elif isinstance(target_date, date):
            # 将 date 对象转换为 datetime 对象，时间设为午夜
            target_date_dt = datetime.combine(target_date, datetime.min.time())
        else:
            logger.error(f"不支持的日期类型: {type(target_date)}")
            return pd.DataFrame()

        target_date_str = target_date_dt.strftime("%Y-%m-%d")

        # 🧠 步骤1: 检查内存缓存 (最快)
        # ✅ 修复后: 缓存数据也会应用 result_include_all 过滤
        if not force_refresh and target_date_str in self.daily_cache:
            logger.info(f"从内存缓存加载 {target_date_str} 的数据")
            cached_df = self.daily_cache[target_date_str]
            return self._apply_result_filter(cached_df, result_include_all)

        # 📁 步骤2: 检查文件缓存 (中等速度)
        # ✅ 修复后: 文件缓存数据也会应用 result_include_all 过滤
        daily_file_path = self._get_daily_file_path(target_date_dt.date())

        if not force_refresh and daily_file_path.exists():
            logger.info(f"从缓存文件加载 {target_date_str} 的数据: {daily_file_path}")
            try:
                df = pd.read_csv(daily_file_path)
                # 确保 'date' 列是 datetime 对象以便进行比较
                if "date" in df.columns:
                    df["date"] = pd.to_datetime(df["date"]).dt.date
                self.daily_cache[target_date_str] = df  # 更新缓存
                return self._apply_result_filter(df, result_include_all)
            except Exception as e:
                logger.warning(f"读取缓存文件 {daily_file_path} 失败，将重新计算: {e}")

        # 💾 步骤3: 重新计算数据 (最慢，但数据最新)
        # 当 force_refresh=True 或无缓存时执行
        if not self.coin_data:
            logger.info("内存中无币种数据，开始加载...")
            self.load_coin_data()

        logger.info(
            f"开始为 {target_date_str} 计算每日数据 (强制刷新: {force_refresh})"
        )
        daily_df = self._compute_daily_data(target_date_dt.date())

        # 保存到文件和缓存
        if not daily_df.empty:
            daily_df.to_csv(daily_file_path, index=False)
            logger.info(f"已将 {target_date_str} 的数据保存到 {daily_file_path}")
            self.daily_cache[target_date_str] = daily_df

        # 🔍 步骤4: 应用过滤逻辑 (统一在此处处理)
        # ✅ 修复重点: 确保所有数据获取路径都会应用此过滤
        return self._apply_result_filter(daily_df, result_include_all)

    def _apply_result_filter(self, df: pd.DataFrame, result_include_all: bool) -> pd.DataFrame:
        """
        🔍 核心过滤方法：根据 result_include_all 参数统一应用过滤逻辑
        
        📌 修复说明: 此方法确保所有数据获取路径(内存缓存/文件缓存/重新计算)
                   都会统一应用 result_include_all 过滤，解决之前缓存忽略过滤的Bug
        
        Args:
            df: 待过滤的 DataFrame (包含所有币种数据)
            result_include_all: 是否包含所有币种
                              - True: 返回全部币种 (包括稳定币、包装币)
                              - False: 只返回原生币种 (排除稳定币、包装币)
            
        Returns:
            根据参数过滤后的 DataFrame
        """
        if result_include_all or df.empty:
            return df
            
        from src.classification.unified_classifier import UnifiedClassifier
        
        classifier = UnifiedClassifier(data_dir=str(self.data_dir.parent))
        coin_ids = df['coin_id'].unique().tolist()
        
        native_coin_ids = classifier.filter_coins(
            coin_ids=coin_ids,
            exclude_stablecoins=True,
            exclude_wrapped_coins=True,
            use_cache=True
        )
        
        filtered_df = df[df['coin_id'].isin(native_coin_ids)].copy()
        logger.info(f"过滤后保留 {len(native_coin_ids)} 个原生币种")
        
        return filtered_df

    def build_daily_tables(self, force_recalculate: bool = False) -> None:
        """构建每日数据表集合

        Args:
            force_recalculate: 是否强制重新计算所有数据，忽略缓存文件
        """
        if not self.coin_data:
            logger.error("请先调用 load_coin_data() 加载数据")
            return

        # 确定日期范围
        self._calculate_date_range()
        start_date = self.min_date
        end_date = self.max_date

        # 检查日期有效性
        if not start_date or not end_date:
            logger.error("无法确定有效的日期范围")
            return

        logger.info(f"构建每日数据表: {start_date} 到 {end_date}")

        # 生成需要处理的日期列表
        date_list = []
        current_date = (
            datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(start_date, str)
            else start_date.date()
        )
        end_date = (
            datetime.strptime(end_date, "%Y-%m-%d").date()
            if isinstance(end_date, str)
            else end_date.date()
        )

        while current_date <= end_date:
            date_list.append(current_date)
            current_date += timedelta(days=1)

        logger.info(f"将处理 {len(date_list)} 天的数据")

        # 使用并行处理
        all_daily_data = []
        with ProcessPoolExecutor(
            max_workers=max(1, multiprocessing.cpu_count() - 1)
        ) as executor:
            # 提交所有任务
            future_to_date = {
                executor.submit(self.get_daily_data, date, force_recalculate): date
                for date in date_list
            }

            # 收集结果
            completed = 0
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                try:
                    daily_data = future.result()
                    if not daily_data.empty:
                        all_daily_data.append(daily_data)
                except Exception as e:
                    logger.error(f"处理日期 {date} 时出错: {e}")

                completed += 1
                if completed % 10 == 0:
                    logger.info(f"已完成 {completed}/{len(date_list)} 天的数据处理")

        logger.info(f"成功处理 {len(all_daily_data)} 天的数据")

        # 合并所有每日数据到一个DataFrame
        if all_daily_data:
            merged_daily_data = pd.concat(all_daily_data, ignore_index=True)

            # 强制按市值排序并重新分配rank
            merged_daily_data = merged_daily_data.sort_values(
                "market_cap", ascending=False
            ).reset_index(drop=True)
            merged_daily_data["rank"] = merged_daily_data.index + 1

            # 保存合并后的数据到文件
            merged_daily_file = self.output_dir / "merged_daily_data.csv"
            merged_daily_data.to_csv(merged_daily_file, index=False)
            logger.info(f"已保存合并后的每日数据到文件: {merged_daily_file}")

    def get_data_coverage_analysis(self) -> Dict:
        """分析数据覆盖情况"""
        if not self.coin_data:
            return {}

        analysis = {
            "total_coins": len(self.loaded_coins),
            "date_range": {
                "start": str(self.min_date) if self.min_date else None,
                "end": str(self.max_date) if self.max_date else None,
                "total_days": (
                    (self.max_date - self.min_date).days + 1
                    if self.min_date and self.max_date
                    else 0
                ),
            },
            "coin_details": [],
        }

        for coin_id, df in self.coin_data.items():
            coin_start = df["date"].min()
            coin_end = df["date"].max()
            coin_days = len(df)

            analysis["coin_details"].append(
                {
                    "coin_id": coin_id,
                    "start_date": str(coin_start),
                    "end_date": str(coin_end),
                    "data_points": coin_days,
                    "market_cap_avg": df["market_cap"].mean(),
                }
            )

        # 按数据点数量排序
        analysis["coin_details"].sort(key=lambda x: x["data_points"], reverse=True)

        return analysis

    def find_bitcoin_start_date(self) -> Optional[str]:
        """找到Bitcoin数据的最早日期"""
        if "bitcoin" in self.coin_data:
            btc_data = self.coin_data["bitcoin"]
            return str(btc_data["date"].min())
        return None

    def load_daily_data_from_files(self) -> None:
        """从已保存的每日数据文件中加载数据"""
        logger.info("从已保存的文件中加载每日数据...")

        csv_files = []

        # 扫描分层结构
        for year_dir in self.daily_files_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir() and month_dir.name.isdigit():
                        csv_files.extend(list(month_dir.glob("*.csv")))

        # 同时支持平铺结构
        csv_files.extend(list(self.daily_files_dir.glob("*.csv")))

        logger.info(f"发现 {len(csv_files)} 个每日数据文件")

        for csv_file in csv_files:
            date_str = csv_file.stem  # 文件名就是日期
            try:
                daily_df = pd.read_csv(csv_file)
                # 转换date列的数据类型
                daily_df["date"] = pd.to_datetime(daily_df["date"]).dt.date
                self.daily_cache[date_str] = daily_df
            except Exception as e:
                logger.error(f"加载每日数据文件失败 {date_str}: {e}")

        logger.info(f"成功加载 {len(self.daily_cache)} 天的每日数据")

    def get_available_daily_dates(self) -> List[str]:
        """获取所有已生成每日数据文件的日期"""
        dates = []
        if not self.daily_files_dir.exists():
            return dates

        for year_dir in self.daily_files_dir.iterdir():
            if year_dir.is_dir():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir():
                        for file in month_dir.glob("*.csv"):
                            dates.append(file.stem)

        return sorted(dates)

    def get_date_range_summary(self) -> Dict:
        """获取数据日期范围的摘要信息"""
        available_dates = self.get_available_daily_dates()

        if not available_dates:
            return {
                "total_days": 0,
                "start_date": None,
                "end_date": None,
                "coverage": 0.0,
            }

        start_date = available_dates[0]
        end_date = available_dates[-1]

        # 计算理论总天数
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        theoretical_days = (end_dt - start_dt).days + 1

        # 计算覆盖率
        coverage = (
            len(available_dates) / theoretical_days if theoretical_days > 0 else 0.0
        )

        return {
            "total_days": len(available_dates),
            "theoretical_days": theoretical_days,
            "start_date": start_date,
            "end_date": end_date,
            "coverage": coverage,
        }

    def _get_daily_file_path(self, target_date: date) -> Path:
        """根据日期获取每日数据文件的路径"""
        # 确保输入是 date 对象
        if isinstance(target_date, datetime):
            target_date = target_date.date()

        date_str = target_date.strftime("%Y-%m-%d")
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")

        # 创建年月目录
        year_month_dir = self.daily_files_dir / year / month
        year_month_dir.mkdir(parents=True, exist_ok=True)

        return year_month_dir / f"{date_str}.csv"

    def _compute_daily_data(self, target_date: date) -> pd.DataFrame:
        """在内存中计算指定日期的聚合数据"""
        daily_records = []
        target_date_str = target_date.strftime("%Y-%m-%d")
        logger.info(
            f"开始计算 {target_date_str} 的数据，遍历 {len(self.coin_data)} 个已加载的币种..."
        )

        # 如果币种数量足够多，使用并行处理
        if len(self.coin_data) > 100:
            # 使用并行处理，每个进程处理一部分币种
            with ProcessPoolExecutor(
                max_workers=max(1, multiprocessing.cpu_count() - 1)
            ) as executor:
                # 准备任务，每个任务处理一批币种
                coin_batches = self._split_coins_into_batches(
                    list(self.coin_data.items()), 10
                )
                futures = []

                for batch in coin_batches:
                    futures.append(
                        executor.submit(
                            self._process_coin_batch, batch, target_date_str
                        )
                    )

                # 收集结果
                for future in as_completed(futures):
                    try:
                        batch_results = future.result()
                        daily_records.extend(batch_results)
                    except Exception as e:
                        logger.error(f"处理币种批次时出错: {e}")
        else:
            # 币种数量较少，使用单线程处理
            for coin_id, df in self.coin_data.items():
                if df.empty:
                    continue

                # 筛选特定日期的数据
                day_data = df[df["date"] == target_date_str]

                if not day_data.empty:
                    # 通常每天只有一个记录，但为防万一，取第一个
                    record = day_data.iloc[0].to_dict()
                    daily_records.append(record)
                    logger.debug(f"找到 {coin_id} 在 {target_date_str} 的数据。")
                else:
                    logger.debug(f"未找到 {coin_id} 在 {target_date_str} 的数据。")

        if not daily_records:
            logger.warning(f"在 {target_date_str} 未找到任何币种的数据。")
            return pd.DataFrame()

        # 转换为DataFrame并排序
        final_df = pd.DataFrame(daily_records)
        logger.info(f"为 {target_date_str} 聚合了 {len(final_df)} 个币种的数据。")

        # 添加排名
        if "market_cap" in final_df.columns:
            final_df = final_df.sort_values("market_cap", ascending=False)
            final_df = final_df.reset_index(drop=True)
            final_df["rank"] = final_df.index + 1

        return final_df

    @staticmethod
    def _split_coins_into_batches(coins, batch_size):
        """将币种列表分割成多个批次进行并行处理"""
        for i in range(0, len(coins), batch_size):
            yield coins[i : i + batch_size]

    @staticmethod
    def _process_coin_batch(coin_batch, target_date_str):
        """处理一批币种的数据，用于并行执行"""
        batch_results = []
        for coin_id, df in coin_batch:
            if df.empty:
                continue

            # 筛选特定日期的数据
            day_data = df[df["date"] == target_date_str]

            if not day_data.empty:
                record = day_data.iloc[0].to_dict()
                batch_results.append(record)

        return batch_results

    def build_daily_market_summary(
        self, output_path: Optional[str] = None
    ) -> pd.DataFrame:
        """
        生成每日市场摘要数据

        遍历所有每日数据文件，计算市场摘要统计数据

        Args:
            output_path: 可选的输出文件路径，如果提供则保存CSV文件

        Returns:
            包含每日市场摘要的DataFrame
        """
        if output_path is None:
            output_file = self.output_dir / "daily_summary.csv"
        else:
            output_file = Path(output_path)

        daily_files_dir = self.output_dir / "daily_files"

        if not daily_files_dir.exists():
            logger.error(f"每日数据目录不存在: {daily_files_dir}")
            return pd.DataFrame()

        # 查找所有日度数据文件
        daily_files = sorted(list(daily_files_dir.glob("*/*/*.csv")))

        if not daily_files:
            logger.warning(f"在 {daily_files_dir} 中没有找到任何日度数据文件")
            return pd.DataFrame()

        logger.info(f"找到 {len(daily_files)} 个日度数据文件，开始生成摘要...")

        summary_data = []

        for file_path in daily_files:
            try:
                # 从文件名中提取日期
                date_str = file_path.stem

                df = pd.read_csv(file_path)

                # 跳过空文件
                if df.empty:
                    continue

                coin_count = len(df)
                total_market_cap = df["market_cap"].sum()
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
                logger.warning(f"处理文件 {file_path} 时出错: {e}")
                continue

        if not summary_data:
            logger.warning("没有生成任何摘要数据")
            return pd.DataFrame()

        # 创建DataFrame并按日期排序
        summary_df = pd.DataFrame(summary_data)
        summary_df["date"] = pd.to_datetime(summary_df["date"])
        summary_df = summary_df.sort_values(by="date").reset_index(drop=True)

        # 格式化日期列
        summary_df["date"] = summary_df["date"].dt.strftime("%Y-%m-%d")

        # 保存到CSV文件
        summary_df.to_csv(output_file, index=False)
        logger.info(f"每日市场摘要已保存到: {output_file}")
        logger.info(f"总共处理了 {len(summary_df)} 天的数据")

        return summary_df

    def reorder_daily_files_by_market_cap(
        self,
        dry_run: bool = False,
        max_workers: int = 8,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Tuple[int, int]:
        """
        按市值重排序每日文件并重新分配rank字段

        Args:
            dry_run: 是否为试运行模式
            max_workers: 最大并发线程数
            start_date: 开始日期 (YYYY-MM-DD，可选)
            end_date: 结束日期 (YYYY-MM-DD，可选)

        Returns:
            Tuple[int, int]: (成功处理数量, 总文件数量)
        """
        import glob
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from datetime import datetime

        self.logger.info(f"开始重排序每日文件，dry_run={dry_run}")

        # 获取目标文件列表
        if start_date and end_date:
            target_files = self._find_files_by_date_range(start_date, end_date)
            self.logger.info(
                f"按日期范围筛选：{start_date} 到 {end_date}，找到 {len(target_files)} 个文件"
            )
        else:
            target_files = self._find_all_daily_files()
            self.logger.info(f"处理所有每日文件，找到 {len(target_files)} 个文件")

        if not target_files:
            self.logger.warning("没有找到需要处理的文件")
            return 0, 0

        successful = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_file = {
                executor.submit(
                    self._process_single_file_reorder, file_path, dry_run
                ): file_path
                for file_path in target_files
            }

            # 等待完成并收集结果
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        successful += 1
                        if not dry_run:
                            self.logger.debug(
                                f"已重排序: {os.path.basename(file_path)}"
                            )
                    else:
                        failed += 1
                        self.logger.warning(
                            f"重排序失败: {os.path.basename(file_path)}"
                        )
                except Exception as e:
                    failed += 1
                    self.logger.error(f"处理文件 {file_path} 时发生异常: {e}")

        self.logger.info(
            f"重排序完成: 成功 {successful}, 失败 {failed}, 总计 {len(target_files)}"
        )
        return successful, len(target_files)

    def _process_single_file_reorder(
        self, file_path: str, dry_run: bool = False
    ) -> bool:
        """
        处理单个文件的重排序

        Args:
            file_path: 文件路径
            dry_run: 是否为试运行模式

        Returns:
            bool: 是否成功
        """
        try:
            df = pd.read_csv(file_path)

            # 检查必要字段是否存在
            if "market_cap" not in df.columns or "rank" not in df.columns:
                self.logger.warning(
                    f"文件 {file_path} 缺少必要字段 (market_cap 或 rank)"
                )
                return False

            # 市值字段降序排序
            df_sorted = df.sort_values(by="market_cap", ascending=False)
            # 重新赋值排名
            df_sorted["rank"] = range(1, len(df_sorted) + 1)

            if dry_run:
                self.logger.info(
                    f"[DRY-RUN] {os.path.basename(file_path)} 重排序预览 (前3行):"
                )
                self.logger.info(f"\n{df_sorted.head(3).to_string()}")
            else:
                df_sorted.to_csv(file_path, index=False)

            return True

        except Exception as e:
            self.logger.error(f"处理文件 {file_path} 时出错: {e}")
            return False

    def _find_all_daily_files(self) -> List[str]:
        """查找所有每日汇总文件"""
        files = []
        daily_files_dir = self.daily_files_dir

        if not os.path.exists(daily_files_dir):
            self.logger.warning(f"每日文件目录不存在: {daily_files_dir}")
            return files

        # 使用glob模式匹配所有CSV文件
        pattern = os.path.join(daily_files_dir, "*", "*", "*.csv")
        files = glob.glob(pattern)

        return sorted(files)

    def _find_files_by_date_range(self, start_date: str, end_date: str) -> List[str]:
        """
        根据日期范围查找文件

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            List[str]: 符合日期范围的文件路径列表
        """
        from datetime import datetime, timedelta

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            self.logger.error(f"日期格式错误: {e}")
            return []

        files = []
        current_dt = start_dt

        while current_dt <= end_dt:
            date_str = current_dt.strftime("%Y-%m-%d")
            year = current_dt.strftime("%Y")
            month = current_dt.strftime("%m")

            file_path = os.path.join(
                self.daily_files_dir, year, month, f"{date_str}.csv"
            )

            if os.path.exists(file_path):
                files.append(file_path)

            current_dt += timedelta(days=1)

        return files


def create_daily_aggregator(
    data_dir: str = "data/coins", output_dir: str = "data/daily"
) -> DailyDataAggregator:
    """创建每日数据聚合器实例"""
    logger.info(f"每日数据聚合器初始化, 数据源: '{data_dir}', 输出到: '{output_dir}'")
    return DailyDataAggregator(data_dir, output_dir)
