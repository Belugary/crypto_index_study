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

from ..api.coingecko import CoinGeckoAPI
from ..downloaders.batch_downloader import create_batch_downloader
from ..updaters.price_updater import MarketDataFetcher

logger = logging.getLogger(__name__)


class IncrementalDailyUpdater:
    """增量每日数据更新器"""

    def __init__(
        self,
        coins_dir: str = "data/coins",
        daily_dir: str = "data/daily/daily_files",
        backup_enabled: bool = True,
    ):
        """
        初始化增量更新器
        """
        self.coins_dir = Path(coins_dir)
        self.daily_dir = Path(daily_dir)
        self.backup_enabled = backup_enabled

        # 初始化依赖组件
        self.downloader = create_batch_downloader()
        api = CoinGeckoAPI()
        self.market_fetcher = MarketDataFetcher(api)

        # 确保目录存在
        self.daily_dir.mkdir(parents=True, exist_ok=True)

        # 操作日志文件
        self.operation_log = Path("logs/incremental_daily_operations.jsonl")
        self.operation_log.parent.mkdir(exist_ok=True)

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
        """备份每日数据文件

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

    def insert_coin_into_daily_file(self, target_date: date, coin_data: Dict) -> bool:
        """将币种数据插入到指定日期的每日文件中

        Args:
            target_date: 目标日期
            coin_data: 币种数据字典

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
        logger.info(f"开始集成 {coin_id} 到每日文件")

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
            logger.warning(f"{coin_id} 数据与现有每日文件无交集")
            return 0, 0

        logger.info(
            f"{coin_id} 有 {len(coin_dates)} 天数据，其中 {total_attempts} 天与已有文件重叠"
        )

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
    ) -> Dict[str, Dict]:
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

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_coin = {
                    executor.submit(self.download_new_coin_history, coin): coin
                    for coin in new_coins
                }

                for future in as_completed(future_to_coin):
                    coin = future_to_coin[future]
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

            # 3. 集成到每日文件
            logger.info("开始集成新币种数据到每日文件")

            for coin in new_coins:
                download_result = results["download_results"][coin]
                if download_result["success"]:
                    try:
                        inserted_count, total_attempts = (
                            self.integrate_new_coin_into_daily_files(coin)
                        )
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
                else:
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
                r["inserted_days"] for r in results["integration_results"].values()
            )

            results["summary"].update(
                {
                    "end_time": end_time.isoformat(),
                    "duration_seconds": duration,
                    "new_coins_count": len(new_coins),
                    "successful_downloads": successful_downloads,
                    "successful_integrations": successful_integrations,
                    "total_insertions": total_insertions,
                    "status": "completed",
                }
            )

            logger.info("=" * 60)
            logger.info("增量更新完成")
            logger.info(f"执行时间: {duration:.1f} 秒")
            logger.info(f"新币种数量: {len(new_coins)}")
            logger.info(f"成功下载: {successful_downloads}/{len(new_coins)}")
            logger.info(f"成功集成: {successful_integrations}/{len(new_coins)}")
            logger.info(f"总插入次数: {total_insertions}")
            logger.info("=" * 60)

            # 收集所有受影响的日期
            affected_dates = set()
            for result in results["integration_results"].values():
                if result.get("success", False) and "inserted_days" in result:
                    affected_dates.update(result["inserted_days"])

            # 自动重排序（如果有数据更新）
            if successful_integrations > 0:
                logger.info("开始自动重排序每日文件")
                reorder_success = self.auto_reorder_after_update(
                    dry_run=False, affected_dates=affected_dates
                )
                if reorder_success:
                    logger.info("每日文件重排序完成")
                else:
                    logger.warning("重排序过程出现问题，请检查日志")

            return results

        except Exception as e:
            logger.error(f"增量更新过程中发生错误: {e}")
            results["summary"]["status"] = "error"
            results["summary"]["error"] = str(e)
            return results

    def auto_reorder_after_update(
        self, dry_run: bool = False, affected_dates: Optional[Set[str]] = None
    ) -> bool:
        """
        更新完成后自动重排序每日文件

        Args:
            dry_run: 是否为试运行模式
            affected_dates: 受影响的日期集合（YYYY-MM-DD格式），如果为None则处理所有文件

        Returns:
            bool: 是否成功
        """
        try:
            # 动态导入以避免循环依赖
            import importlib.util
            import sys
            from pathlib import Path

            # 获取脚本文件路径
            scripts_path = Path(__file__).parent.parent.parent / "scripts"
            reorder_script = scripts_path / "reorder_daily_files_by_market_cap.py"

            # 动态加载模块
            spec = importlib.util.spec_from_file_location(
                "reorder_daily_files_by_market_cap", reorder_script
            )
            if spec is None or spec.loader is None:
                raise ImportError(f"无法加载脚本: {reorder_script}")

            reorder_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(reorder_module)

            logger.info("开始自动重排序每日文件")

            # 如果指定了受影响的日期，使用优化的范围重排序
            if affected_dates:
                sorted_dates = sorted(affected_dates)
                start_date = sorted_dates[0]
                end_date = sorted_dates[-1]

                logger.info(f"针对性重排序: {start_date} 到 {end_date}")
                successful, total = reorder_module.reorder_files_by_date_range(
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=dry_run,
                    max_workers=8,
                )
            else:
                # 处理所有文件
                logger.info("全量重排序所有每日文件")
                successful, total = reorder_module.reorder_all_daily_files(
                    dry_run=dry_run, max_workers=8
                )

            if successful == total and total > 0:
                logger.info("每日文件重排序完成")
                return True
            else:
                logger.warning(f"重排序部分完成: {successful}/{total}")
                return False

        except Exception as e:
            logger.error(f"自动重排序失败: {e}")
            return False
        finally:
            # 清理导入路径（如果使用了sys.path方式）
            pass


def create_incremental_updater(
    coins_dir: str = "data/coins",
    daily_dir: str = "data/daily/daily_files",
    backup_enabled: bool = True,
) -> IncrementalDailyUpdater:
    """便捷函数：创建增量更新器实例"""
    return IncrementalDailyUpdater(coins_dir, daily_dir, backup_enabled)
