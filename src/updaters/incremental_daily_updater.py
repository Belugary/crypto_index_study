#!/usr/bin/env python3
"""
增量每日数据更新器

功能：检测新币种、下载历史数据、集成到每日文件
特性：并行处理、错误恢复、自动排序
"""

import json
import logging
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from tqdm import tqdm

from ..api.coingecko import CoinGeckoAPI
from ..downloaders.batch_downloader import create_batch_downloader
from ..updaters.price_updater import MarketDataFetcher
from ..utils.path_utils import find_project_root, resolve_data_path, ensure_directory

logger = logging.getLogger(__name__)


class IncrementalDailyUpdater:
    """增量每日数据更新器"""

    def __init__(
        self,
        coins_dir: str = "data/coins",
        daily_dir: str = "data/daily/daily_files",
        backup_enabled: bool = False,  # 默认禁用备份，避免产生大量文件
        use_database: bool = True,  # 🚀 新增：启用数据库模式以获得更好性能
    ):
        """
        初始化增量更新器
        
        Args:
            coins_dir: 币种数据目录
            daily_dir: 每日数据目录
            backup_enabled: 是否启用备份功能
            use_database: 是否启用数据库模式（推荐开启以获得更好性能）
        """
        # 使用新的路径工具
        self.project_root = find_project_root()
        
        # 解析路径：使用新的路径工具
        self.coins_dir = resolve_data_path(coins_dir, self.project_root)
        self.daily_dir = resolve_data_path(daily_dir, self.project_root)
        self.backup_enabled = backup_enabled
        self.use_database = use_database

        # 初始化依赖组件
        self.downloader = create_batch_downloader()
        api = CoinGeckoAPI()
        self.market_fetcher = MarketDataFetcher(api)

        # 🚀 初始化数据库支持的数据聚合器
        if use_database:
            from ..downloaders.daily_aggregator import DailyDataAggregator
            self.daily_aggregator = DailyDataAggregator(
                data_dir=str(self.coins_dir.parent / "coins"),
                output_dir=str(self.daily_dir.parent),
                use_database=True
            )

        # 确保目录存在
        ensure_directory(self.daily_dir)

        # 操作日志文件
        operation_log_path = "logs/incremental_daily_operations.jsonl"
        self.operation_log = resolve_data_path(operation_log_path, self.project_root)
        ensure_directory(self.operation_log.parent)

        logger.info("增量更新器初始化完成")

    def get_existing_coins(self) -> Set[str]:
        """获取已有的币种列表"""
        existing = set()
        for csv_file in self.coins_dir.glob("*.csv"):
            existing.add(csv_file.stem)
        logger.debug(f"发现 {len(existing)} 个已有币种")
        return existing

    def get_current_market_coins(self, top_n: int = 1000) -> Set[str]:
        """获取当前市值前N名币种"""
        logger.info(f"获取当前市值前 {top_n} 名币种...")
        try:
            market_data = self.market_fetcher.get_top_coins(top_n)
            coin_ids = {coin["id"] for coin in market_data}
            logger.info(f"成功获取 {len(coin_ids)} 个市值排名币种")
            return coin_ids
        except Exception as e:
            logger.error(f"获取市值排名失败: {e}")
            return set()

    def detect_new_coins(self, top_n: int = 1000) -> List[str]:
        """检测新币种

        Returns:
            新币种ID列表
        """
        logger.info("开始检测新币种...")

        existing = self.get_existing_coins()
        current = self.get_current_market_coins(top_n)

        if not current:
            logger.warning("无法获取当前市值排名，跳过新币种检测")
            return []

        new_coins = current - existing

        if new_coins:
            logger.info(f"发现 {len(new_coins)} 个新币种")
            for coin in sorted(new_coins):
                logger.info(f"  - {coin}")
        else:
            logger.info("没有发现新币种")

        return list(new_coins)

    def download_new_coin_history(self, coin_id: str) -> bool:
        """下载新币种的完整历史数据

        Args:
            coin_id: 币种ID

        Returns:
            是否下载成功
        """
        logger.info(f"开始下载 {coin_id} 的历史数据")

        try:
            # 使用 max days 获取完整历史
            success = self.downloader.download_coin_data(coin_id, days="max")

            if success:
                logger.info(f"{coin_id} 历史数据下载成功")
                # 记录操作日志
                self._log_operation("download", coin_id, success=True)
            else:
                logger.error(f"{coin_id} 历史数据下载失败")
                self._log_operation(
                    "download", coin_id, success=False, error="下载失败"
                )

            return success

        except Exception as e:
            error_msg = f"下载 {coin_id} 历史数据时出错: {e}"
            logger.error(error_msg)
            self._log_operation("download", coin_id, success=False, error=str(e))
            return False

    def load_coin_data(self, coin_id: str) -> Optional[pd.DataFrame]:
        """加载币种数据

        Args:
            coin_id: 币种ID

        Returns:
            币种数据DataFrame，失败则返回None
        """
        csv_path = self.coins_dir / f"{coin_id}.csv"
        if not csv_path.exists():
            logger.debug(f"币种数据文件不存在: {csv_path}")
            return None

        try:
            df = pd.read_csv(csv_path)

            # 数据验证
            required_columns = ["timestamp", "price", "market_cap"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"{coin_id} 数据缺少必要列: {missing_columns}")
                return None

            # 转换日期
            df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
            df["coin_id"] = coin_id

            # 数据清洗：移除无效记录
            original_len = len(df)
            df = df.dropna(subset=["price", "market_cap"])
            df = df[(df["price"] > 0) & (df["market_cap"] > 0)]

            if len(df) < original_len:
                logger.warning(f"{coin_id} 清理了 {original_len - len(df)} 条无效记录")

            logger.debug(f"成功加载 {coin_id} 数据: {len(df)} 条记录")
            return df

        except Exception as e:
            logger.error(f"加载 {coin_id} 数据失败: {e}")
            return None

    def get_existing_daily_dates(self) -> Set[date]:
        """获取已有的每日数据文件日期"""
        dates = set()

        # 扫描分层结构: YYYY/MM/YYYY-MM-DD.csv
        for year_dir in self.daily_dir.iterdir():
            if year_dir.is_dir() and year_dir.name.isdigit():
                for month_dir in year_dir.iterdir():
                    if month_dir.is_dir() and month_dir.name.isdigit():
                        for csv_file in month_dir.glob("*.csv"):
                            try:
                                file_date = datetime.strptime(
                                    csv_file.stem, "%Y-%m-%d"
                                ).date()
                                dates.add(file_date)
                            except ValueError:
                                continue

        logger.debug(f"发现 {len(dates)} 个已有每日数据文件")
        return dates

    def _backup_daily_file(self, filepath: Path) -> Optional[Path]:
        """备份每日数据文件（智能备份）

        Args:
            filepath: 要备份的文件路径

        Returns:
            备份文件路径，失败则返回None
        """
        if not self.backup_enabled or not filepath.exists():
            return None

        try:
            # 创建备份目录
            backup_dir = filepath.parent / ".backup"
            backup_dir.mkdir(exist_ok=True)

            # 智能备份：只保留最新的3个备份
            existing_backups = sorted(
                backup_dir.glob(f"{filepath.stem}_*.csv"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )

            # 删除超过3个的旧备份
            for old_backup in existing_backups[2:]:  # 保留最新2个，第3个开始删除
                try:
                    old_backup.unlink()
                    logger.debug(f"删除旧备份: {old_backup}")
                except Exception as e:
                    logger.warning(f"删除旧备份失败: {e}")

            # 生成备份文件名（包含时间戳）
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"{filepath.stem}_{timestamp}.csv"
            backup_path = backup_dir / backup_filename

            # 复制文件
            shutil.copy2(filepath, backup_path)
            logger.debug(f"已备份文件: {backup_path}")
            return backup_path

        except Exception as e:
            logger.warning(f"备份文件失败 {filepath}: {e}")
            return None

    def insert_coin_into_daily_file(
        self, target_date: date, coin_data: Dict, pbar: Optional[tqdm] = None
    ) -> bool:
        """将币种数据插入到指定日期的每日文件中

        Args:
            target_date: 目标日期
            coin_data: 币种数据字典
            pbar: tqdm 进度条实例 (可选)

        Returns:
            是否插入成功
        """
        try:
            # 构造文件路径
            year_dir = self.daily_dir / str(target_date.year)
            month_dir = year_dir / f"{target_date.month:02d}"
            filepath = month_dir / f"{target_date}.csv"

            # 确保目录存在
            month_dir.mkdir(parents=True, exist_ok=True)

            # 备份现有文件
            backup_path = self._backup_daily_file(filepath)

            try:
                if filepath.exists():
                    # 读取现有文件
                    df = pd.read_csv(filepath)

                    # 检查币种是否已存在
                    if coin_data["coin_id"] in df["coin_id"].values:
                        logger.debug(
                            f"{coin_data['coin_id']} 在 {target_date} 已存在，跳过"
                        )
                        return True

                    # 添加新币种数据
                    new_row = pd.DataFrame([coin_data])
                    df = pd.concat([df, new_row], ignore_index=True)

                else:
                    # 创建新文件
                    df = pd.DataFrame([coin_data])

                # 重新排序并更新排名
                df = df.sort_values("market_cap", ascending=False).reset_index(
                    drop=True
                )
                df["rank"] = range(1, len(df) + 1)

                # 保存文件
                df.to_csv(filepath, index=False, float_format="%.6f")

                # 更新日志，但不使用 f-string 以提高性能
                if pbar:
                    pbar.set_description(
                        f"已集成 {coin_data['coin_id']} 到 {target_date}"
                    )
                else:
                    logger.info(
                        f"已将 {coin_data['coin_id']} 插入到 {target_date} (排名: {df[df['coin_id'] == coin_data['coin_id']]['rank'].iloc[0]})"
                    )

                # 记录操作日志
                self._log_operation(
                    "insert",
                    coin_data["coin_id"],
                    success=True,
                    target_date=str(target_date),
                    rank=int(df[df["coin_id"] == coin_data["coin_id"]]["rank"].iloc[0]),
                )

                return True

            except Exception as e:
                # 如果有备份，尝试恢复
                if backup_path and backup_path.exists():
                    try:
                        shutil.copy2(backup_path, filepath)
                        logger.warning(f"操作失败，已从备份恢复: {filepath}")
                    except Exception as restore_error:
                        logger.error(f"恢复备份失败: {restore_error}")

                raise e

        except Exception as e:
            error_msg = f"插入 {coin_data['coin_id']} 到 {target_date} 失败: {e}"
            logger.error(error_msg)
            self._log_operation(
                "insert",
                coin_data["coin_id"],
                success=False,
                error=str(e),
                target_date=str(target_date),
            )
            return False

    def integrate_new_coin_into_daily_files(self, coin_id: str) -> Tuple[int, int]:
        """将新币种数据集成到所有相关的每日文件中

        Args:
            coin_id: 币种ID

        Returns:
            (成功插入天数, 总尝试天数)
        """
        # logger.info(f"开始集成 {coin_id} 到每日文件") # 在并行模式下过于嘈杂

        # 加载币种数据
        coin_df = self.load_coin_data(coin_id)
        if coin_df is None:
            logger.error(f"无法加载 {coin_id} 数据")
            return 0, 0

        # 获取已有的每日文件日期
        existing_dates = self.get_existing_daily_dates()

        # 找到币种数据与已有日期的交集
        coin_dates = set(coin_df["date"].unique())
        relevant_dates = existing_dates.intersection(coin_dates)

        total_attempts = len(relevant_dates)
        if total_attempts == 0:
            # logger.warning(f"{coin_id} 数据与现有每日文件无交集") # 在并行模式下过于嘈杂
            return 0, 0

        # logger.info(
        #     f"{coin_id} 有 {len(coin_dates)} 天数据，其中 {total_attempts} 天与已有文件重叠"
        # )

        # 逐日插入
        successful_insertions = 0

        for target_date in sorted(relevant_dates):
            # 获取该日期的币种数据
            day_data = coin_df[coin_df["date"] == target_date]
            if day_data.empty:
                continue

            # 取最新记录（防止同日多条记录）
            latest_record = day_data.iloc[-1]

            # 检查数据有效性
            if (
                pd.isna(latest_record["price"])
                or latest_record["price"] <= 0
                or pd.isna(latest_record["market_cap"])
                or latest_record["market_cap"] <= 0
            ):
                logger.debug(f"{coin_id} 在 {target_date} 的数据无效，跳过")
                continue

            # 构造数据记录
            coin_data = {
                "timestamp": int(latest_record["timestamp"]),
                "price": float(latest_record["price"]),
                "volume": (
                    float(latest_record["volume"])
                    if pd.notna(latest_record["volume"])
                    else 0.0
                ),
                "market_cap": float(latest_record["market_cap"]),
                "date": target_date,
                "coin_id": coin_id,
            }

            # 插入到每日文件
            if self.insert_coin_into_daily_file(target_date, coin_data):
                successful_insertions += 1

        success_rate = (
            (successful_insertions / total_attempts * 100) if total_attempts > 0 else 0
        )
        logger.info(
            f"{coin_id} 集成完成: {successful_insertions}/{total_attempts} 天成功 ({success_rate:.1f}%)"
        )

        return successful_insertions, total_attempts

    def _log_operation(self, operation: str, coin_id: str, success: bool, **kwargs):
        """记录操作日志

        Args:
            operation: 操作类型 (download, insert, etc.)
            coin_id: 币种ID
            success: 是否成功
            **kwargs: 其他信息
        """
        try:
            log_entry = {
                "timestamp": datetime.now().isoformat(),
                "operation": operation,
                "coin_id": coin_id,
                "success": success,
                **kwargs,
            }

            with open(self.operation_log, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry) + "\n")

        except Exception as e:
            logger.warning(f"记录操作日志失败: {e}")

    def update_with_new_coins(
        self, top_n: int = 1000, max_workers: int = 3, dry_run: bool = False
    ) -> Dict:
        """检测并集成新币种的完整流程

        Args:
            top_n: 监控的市值排名范围
            max_workers: 并行下载的工作线程数
            dry_run: 是否为试运行模式（不实际修改文件）

        Returns:
            操作结果字典
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("开始增量每日数据更新")
        logger.info(f"监控范围: 前 {top_n} 名")
        logger.info(f"并行线程: {max_workers}")
        logger.info(f"试运行模式: {'是' if dry_run else '否'}")
        logger.info("=" * 60)

        results = {
            "summary": {
                "start_time": start_time.isoformat(),
                "top_n": top_n,
                "dry_run": dry_run,
            },
            "new_coins": [],
            "download_results": {},
            "integration_results": {},
        }

        try:
            # 1. 检测新币种
            new_coins = self.detect_new_coins(top_n)
            results["new_coins"] = new_coins

            if not new_coins:
                logger.info("没有发现新币种，无需更新")
                results["summary"]["status"] = "no_new_coins"
                return results

            if dry_run:
                logger.info(
                    f"试运行模式：发现 {len(new_coins)} 个新币种，实际运行时将会下载并集成"
                )
                results["summary"]["status"] = "dry_run_complete"
                return results

            # 2. 下载新币种历史数据
            logger.info(f"开始下载 {len(new_coins)} 个新币种的历史数据")

            with tqdm(total=len(new_coins), desc="下载新币种数据") as pbar:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_coin = {
                        executor.submit(self.download_new_coin_history, coin): coin
                        for coin in new_coins
                    }

                    for future in as_completed(future_to_coin):
                        coin = future_to_coin[future]
                        pbar.set_description(f"下载 {coin}")
                        try:
                            success = future.result()
                            results["download_results"][coin] = {
                                "success": success,
                                "error": None,
                            }
                        except Exception as e:
                            logger.error(f"下载 {coin} 时出错: {e}")
                            results["download_results"][coin] = {
                                "success": False,
                                "error": str(e),
                            }
                        pbar.update(1)

            # 3. 集成到每日文件 (并行化)
            logger.info("开始并行集成新币种数据到每日文件")
            successful_coins = [
                coin
                for coin, res in results["download_results"].items()
                if res["success"]
            ]

            with tqdm(total=len(successful_coins), desc="集成新币种") as pbar:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_to_coin = {
                        executor.submit(
                            self.integrate_new_coin_into_daily_files, coin
                        ): coin
                        for coin in successful_coins
                    }

                    for future in as_completed(future_to_coin):
                        coin = future_to_coin[future]
                        pbar.set_description(f"集成 {coin}")
                        try:
                            inserted_count, total_attempts = future.result()
                            results["integration_results"][coin] = {
                                "success": inserted_count > 0,
                                "inserted_days": inserted_count,
                                "total_attempts": total_attempts,
                                "success_rate": (
                                    (inserted_count / total_attempts * 100)
                                    if total_attempts > 0
                                    else 0
                                ),
                                "error": None,
                            }
                        except Exception as e:
                            logger.error(f"集成 {coin} 时出错: {e}")
                            results["integration_results"][coin] = {
                                "success": False,
                                "inserted_days": 0,
                                "total_attempts": 0,
                                "success_rate": 0,
                                "error": str(e),
                            }
                        pbar.update(1)

            # 标记下载失败的币种
            for coin in new_coins:
                if coin not in successful_coins:
                    results["integration_results"][coin] = {
                        "success": False,
                        "inserted_days": 0,
                        "total_attempts": 0,
                        "success_rate": 0,
                        "error": "下载失败，跳过集成",
                    }

            # 4. 生成总结报告
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            successful_downloads = sum(
                1 for r in results["download_results"].values() if r["success"]
            )
            successful_integrations = sum(
                1 for r in results["integration_results"].values() if r["success"]
            )
            total_insertions = sum(
                r.get("inserted_days", 0)
                for r in results["integration_results"].values()
            )

            results["summary"].update(
                {
                    "end_time": end_time.isoformat(),
                    "duration_seconds": duration,
                    "successful_downloads": successful_downloads,
                    "successful_integrations": successful_integrations,
                    "total_insertions": total_insertions,
                    "status": "completed",
                }
            )

            logger.info(
                f"增量更新完成：{successful_downloads} 个新币种历史数据下载成功，{successful_integrations} 个币种数据成功集成"
            )
        except KeyboardInterrupt:
            logger.warning("用户中断了增量更新操作")
            results["summary"]["status"] = "interrupted"
            results["summary"]["error"] = "User interrupted the process"
            return results
        except Exception as e:
            logger.error(f"增量更新过程中发生错误: {e}")
            results["summary"]["status"] = "error"
            results["summary"]["error"] = str(e)
            return results

        # 自动重排序
        try:
            logger.info("尝试对每日文件进行自动重排序...")
            # 获取所有每日文件
            daily_files = [
                f for f in self.daily_dir.glob("*/*/*.csv") if f.stem != f.parent.name
            ]

            if not daily_files:
                logger.info("没有找到需要重排序的每日文件。")
            else:
                logger.info(f"找到 {len(daily_files)} 个文件需要检查和重排序。")

            for target_file in tqdm(daily_files, desc="重排序每日文件"):
                try:
                    # 备份现有文件
                    self._backup_daily_file(target_file)

                    # 执行排序
                    df = pd.read_csv(target_file)
                    df = df.sort_values("market_cap", ascending=False).reset_index(
                        drop=True
                    )
                    df["rank"] = range(1, len(df) + 1)
                    df.to_csv(target_file, index=False, float_format="%.6f")

                    logger.debug(f"已对 {target_file} 进行重排序")
                except Exception as sort_e:
                    logger.error(f"重排序文件 {target_file} 失败: {sort_e}")
                    # 记录到结果中，但不中断整个流程
                    if "resorting_errors" not in results["summary"]:
                        results["summary"]["resorting_errors"] = []
                    results["summary"]["resorting_errors"].append(
                        f"File: {target_file}, Error: {str(sort_e)}"
                    )

            logger.info("自动重排序完成。")

        except Exception as e:
            logger.error(f"自动重排序过程中发生意外错误: {e}")
            results["summary"]["status"] = "completed_with_resorting_error"
            results["summary"]["resorting_error"] = str(e)

        return results


def create_incremental_updater(
    coins_dir: str = "data/coins",
    daily_dir: str = "data/daily/daily_files",
    backup_enabled: bool = False,  # 默认禁用备份
    use_database: bool = True,  # 🚀 新增：默认启用数据库模式
) -> IncrementalDailyUpdater:
    """创建增量每日数据更新器实例

    Args:
        coins_dir: 币种数据目录
        daily_dir: 每日数据目录
        backup_enabled: 是否启用备份
        use_database: 是否启用数据库模式（推荐开启以获得更好性能）

    Returns:
        IncrementalDailyUpdater 实例
    """
    return IncrementalDailyUpdater(coins_dir, daily_dir, backup_enabled, use_database)
