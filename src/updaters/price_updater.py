"""
价格数据更新器

核心功能模块，提供智能的价格数据更新策略。

设计哲学：
1. 简单胜于复杂 - 每个方法职责单一
2. 信任权威数据源 - 基于CoinGecko官方分类
3. 用户导向设计 - 确保用户需求得到满足

核心策略：
1. 按市值顺序获取币种
2. 基于CoinGecko category简单分类：非包装币且非稳定币 = 原生币
3. 按顺序更新，确保原生币达到目标数量
4. 同时更新遇到的非原生币（为未来研究准备）

⚠️ 重要提醒：
- 本更新器使用覆盖模式，会完全重新生成CSV文件
- 自动解决实时数据时间戳异常问题（如07:08:54格式的非标准时间戳）
- 详见：docs/timestamp_handling_memo.md
"""

import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from tqdm import tqdm

from ..api.coingecko import CoinGeckoAPI
from ..classification.unified_classifier import UnifiedClassifier
from ..downloaders.batch_downloader import create_batch_downloader

# API限流配置
RATE_LIMIT_CONFIG = {
    "delay_seconds": 0.13,
    "calls_per_minute": 461.5,
}

logger = logging.getLogger(__name__)


class MarketDataFetcher:
    """市场数据获取器 - 职责单一：获取市值排名数据"""

    def __init__(self, api: CoinGeckoAPI):
        self.api = api

    def get_top_coins(self, n: int) -> List[Dict]:
        """
        获取市值前N名币种

        Args:
            n: 目标币种数量

        Returns:
            币种列表，按市值排序
        """
        logger.info(f"🔍 获取市值前 {n} 名加密货币")

        coins = []
        pages = math.ceil(n / 250)  # 每页最多250个

        with tqdm(
            total=pages,
            desc="获取市值排名",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
            leave=False,
        ) as pbar:
            for page in range(1, pages + 1):
                try:
                    # 计算这一页应该获取多少个币种
                    per_page = min(250, n - len(coins))
                    market_data = self.api.get_coins_markets(
                        vs_currency="usd",
                        order="market_cap_desc",
                        per_page=per_page,
                        page=page,
                        sparkline=False,
                    )

                    if not market_data:
                        logger.warning(f"第 {page} 页未获取到数据，停止获取")
                        break

                    for coin in market_data:
                        if len(coins) >= n:
                            break
                        coins.append(
                            {
                                "id": coin["id"],
                                "symbol": coin["symbol"],
                                "name": coin["name"],
                                "market_cap_rank": coin.get("market_cap_rank", 0),
                            }
                        )

                    pbar.set_postfix({"已获取": len(coins), "目标": n, "当前页": page})
                    pbar.update(1)

                    if len(coins) >= n:
                        break

                except Exception as e:
                    logger.error(f"获取第 {page} 页数据时出错: {e}")
                    break

                # API限流延迟
                time.sleep(RATE_LIMIT_CONFIG["delay_seconds"])

        logger.info(f"✅ 成功获取 {len(coins)} 个币种的市值排名")
        return coins[:n]


class PriceDataUpdater:
    """价格数据更新器 - 主要逻辑协调者"""

    def __init__(self):
        self.api = CoinGeckoAPI()
        self.downloader = create_batch_downloader()
        self.classifier = UnifiedClassifier()  # 直接使用统一分类器
        self.market_fetcher = MarketDataFetcher(self.api)

        # 查找项目根目录并设置路径
        self.project_root = self._find_project_root()
        self.coins_dir = self.project_root / "data" / "coins"
        self.metadata_dir = self.project_root / "data" / "metadata"

        # 统计信息
        self.stats = {
            "start_time": None,
            "end_time": None,
            "total_processed": 0,
            "native_updated": 0,
            "stable_updated": 0,
            "wrapped_updated": 0,
            "failed_updates": 0,
            "new_coins": 0,
            "api_calls": 0,
        }

        self.errors = []

    @staticmethod
    def _find_project_root() -> Path:
        """查找项目根目录
        
        Returns:
            Path: 项目根目录路径
        """
        current_path = Path(__file__).parent
        
        # 向上查找，直到找到项目根目录标志
        while current_path != current_path.parent:
            # 检查是否存在 .git 目录
            if (current_path / ".git").exists():
                return current_path
            
            # 检查是否同时存在 src 目录和 requirements.txt
            if (current_path / "src").exists() and (current_path / "requirements.txt").exists():
                return current_path
            
            current_path = current_path.parent
        
        # 如果找不到，返回当前文件所在目录的上两级（假设是项目根目录）
        return Path(__file__).parent.parent.parent

    def download_coin_data(self, coin_id: str) -> Tuple[bool, bool]:
        """
        下载币种数据

        重要设计原则：
        对于 CoinGecko API，全量更新和增量更新的 API 权重消耗一致，
        因此始终使用全量更新 (days="max") 来确保数据完整性，
        避免增量更新可能导致的历史数据丢失问题。

        智能跳过策略：
        1. 检查文件是否存在且修改时间是今天
        2. 检查数据行数是否充足（>500行）
        3. 检查是否有今日的数据

        Returns:
            Tuple[bool, bool]: (success, api_called)
            - success: 是否成功（包括跳过的情况）
            - api_called: 是否实际调用了API
        """
        try:
            # 检查文件是否需要更新（使用改进的数据质量检查）
            csv_file = self.coins_dir / f"{coin_id}.csv"
            if csv_file.exists():
                if self._check_data_quality(csv_file):
                    logger.info(f"⏭️ {coin_id} 数据质量良好，跳过下载")
                    return True, False  # 成功但没有API调用
                else:
                    logger.info(f"⚠️ {coin_id} 数据质量需要改善，重新下载")

            # 统一使用全量更新策略
            logger.info(f"📥 下载 {coin_id} 完整历史数据 (全量更新)...")
            success = self.downloader.download_coin_data(coin_id, days="max")

            if success:
                logger.info(f"✅ {coin_id} 数据下载成功")
                return True, True  # 成功且有API调用
            else:
                logger.error(f"❌ {coin_id} 数据下载失败")
                return False, True  # 失败但有API调用

        except Exception as e:
            logger.error(f"下载 {coin_id} 数据时出错: {e}")
            return False, True  # 失败但有API调用

    def _check_data_quality(self, csv_file: Path) -> bool:
        """
        检查数据质量

        Args:
            csv_file: CSV文件路径

        Returns:
            bool: 数据质量是否良好
        """
        try:
            import os
            from datetime import date

            import pandas as pd

            # 1. 检查文件修改时间
            mtime = os.path.getmtime(csv_file)
            file_date = date.fromtimestamp(mtime)
            today = date.today()

            # 如果不是今天修改的，需要更新
            if file_date != today:
                return False

            # 2. 检查数据内容
            try:
                df = pd.read_csv(csv_file)
            except Exception:
                return False  # 读取失败，需要重新下载

            # 3. 检查数据行数（至少500行）
            if len(df) < 500:
                return False

            # 4. 检查是否有必要的列
            if "timestamp" not in df.columns:
                return False

            # 5. 检查最新数据日期
            try:
                # 转换timestamp（毫秒时间戳）为日期
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                latest_date = df["timestamp"].dt.date.max()

                # 最新数据应该是今天或昨天（考虑时区差异）
                days_diff = (today - latest_date).days
                if days_diff > 1:
                    return False
            except Exception:
                return False  # 日期解析失败

            return True  # 所有检查都通过

        except Exception:
            return False  # 任何异常都认为需要重新下载

    def get_existing_coin_ids(self) -> Set[str]:
        """获取已存在的币种ID"""
        existing_ids = set()
        if self.coins_dir.exists():
            for csv_file in self.coins_dir.glob("*.csv"):
                coin_id = csv_file.stem
                existing_ids.add(coin_id)
        return existing_ids

    def update_with_smart_strategy(
        self, target_native_coins: int = 510, max_search_range: int = 1000
    ):
        """
        智能更新策略

        Args:
            target_native_coins: 目标原生币种数量
            max_search_range: 最大搜索范围
        """
        logger.info(f"🚀 开始智能量价数据更新")
        logger.info(f"📋 目标: 确保至少 {target_native_coins} 个原生币种数据最新")
        logger.info(f"🔍 最大搜索范围: {max_search_range} 个币种")
        logger.info("=" * 60)

        self.stats["start_time"] = datetime.now()

        try:
            # 1. 获取现有币种ID
            existing_ids = self.get_existing_coin_ids()
            logger.info(f"📋 现有币种数量: {len(existing_ids)}")

            # 2. 按市值顺序获取币种并逐个处理
            native_coins_updated = 0
            search_range = min(
                max_search_range, target_native_coins * 2
            )  # 开始搜索范围

            while (
                native_coins_updated < target_native_coins
                and search_range <= max_search_range
            ):
                logger.info(f"🔍 搜索市值前 {search_range} 名币种...")

                # 获取市值排名数据
                all_coins = self.market_fetcher.get_top_coins(search_range)

                # 按顺序处理每个币种
                with tqdm(
                    total=len(all_coins),
                    desc="处理币种数据",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                    ncols=120,
                    leave=False,
                ) as pbar:
                    for coin_info in all_coins:
                        coin_id = coin_info["id"]
                        coin_symbol = coin_info["symbol"].upper()

                        try:
                            # 使用统一分类器进行分类
                            classification_result = self.classifier.classify_coin(
                                coin_id
                            )

                            # 确定币种类型
                            if classification_result.is_stablecoin:
                                coin_type = "stable"
                            elif classification_result.is_wrapped_coin:
                                coin_type = "wrapped"
                            else:
                                coin_type = "native"

                            # 检查是否需要更新
                            # 每个币种都直接下载全量数据，简单直接
                            success, api_called = self.download_coin_data(coin_id)

                            if success:
                                # 更新统计
                                if coin_type == "native":
                                    native_coins_updated += 1
                                    self.stats["native_updated"] += 1
                                elif coin_type == "stable":
                                    self.stats["stable_updated"] += 1
                                elif coin_type == "wrapped":
                                    self.stats["wrapped_updated"] += 1

                                # 检查是否是新币种
                                if coin_id not in existing_ids:
                                    self.stats["new_coins"] += 1
                                    existing_ids.add(coin_id)

                                # 只在实际调用API时计数
                                if api_called:
                                    self.stats["api_calls"] += 1

                                # API限流延迟（只在实际调用API时延迟）
                                if api_called:
                                    time.sleep(RATE_LIMIT_CONFIG["delay_seconds"])
                            else:
                                # 失败的情况也要统计API调用
                                if api_called:
                                    self.stats["api_calls"] += 1
                                self.stats["failed_updates"] += 1
                                self.errors.append(f"{coin_id}: 下载失败")

                            self.stats["total_processed"] += 1

                            # 更新进度条
                            pbar.set_postfix(
                                {
                                    "原生币": native_coins_updated,
                                    "目标": target_native_coins,
                                    "类型": coin_type,
                                    "当前": coin_symbol[:10],
                                }
                            )
                            pbar.update(1)

                            # 如果已达到目标，提前结束
                            if native_coins_updated >= target_native_coins:
                                logger.info(
                                    f"🎯 已达到目标！成功处理 {native_coins_updated} 个原生币种"
                                )
                                break

                        except Exception as e:
                            logger.error(f"处理 {coin_id} 时出错: {e}")
                            self.errors.append(f"{coin_id}: {str(e)}")
                            self.stats["failed_updates"] += 1
                            pbar.update(1)

                # 检查是否需要扩大搜索范围
                if native_coins_updated < target_native_coins:
                    if search_range >= max_search_range:
                        logger.warning(
                            f"⚠️ 已达到最大搜索范围 {max_search_range}，但只找到 {native_coins_updated} 个原生币种"
                        )
                        break
                    else:
                        search_range = min(search_range + 200, max_search_range)
                        logger.info(f"🔄 扩大搜索范围到 {search_range}...")
                else:
                    break

            # 更新元数据
            logger.info("💰 更新稳定币和包装币元数据...")
            self.update_metadata()

            # 验证结果
            if native_coins_updated >= target_native_coins:
                logger.info(
                    f"✅ 成功达成目标！处理了 {native_coins_updated} 个原生币种"
                )
            else:
                logger.warning(
                    f"⚠️ 未完全达成目标，只处理了 {native_coins_updated}/{target_native_coins} 个原生币种"
                )

        except Exception as e:
            logger.error(f"更新过程中发生异常: {e}")
            self.errors.append(f"更新异常: {str(e)}")

        finally:
            self.stats["end_time"] = datetime.now()
            duration = self.stats["end_time"] - self.stats["start_time"]

            # 生成报告
            self.generate_final_report(duration)

    def update_metadata(self):
        """更新稳定币和包装币元数据"""
        try:
            # 使用统一分类器更新元数据
            classifier = UnifiedClassifier()

            # 获取所有币种数据
            from ..downloaders.batch_downloader import create_batch_downloader

            downloader = create_batch_downloader()
            metadata_dir = Path(downloader.data_dir) / "metadata" / "coin_metadata"

            if not metadata_dir.exists():
                logger.warning("❌ 元数据目录不存在")
                return

            coin_ids = [f.stem for f in metadata_dir.glob("*.json")]
            if not coin_ids:
                logger.warning("❌ 未找到任何币种元数据")
                return

            logger.info(f"🔍 正在分析 {len(coin_ids)} 个币种...")

            # 批量分类
            classification_results = classifier.classify_coins_batch(coin_ids)

            # 导出稳定币列表
            stablecoins = [
                result
                for result in classification_results.values()
                if result.is_stablecoin
            ]
            stable_file = self.metadata_dir / "stablecoins.csv"

            with open(stable_file, "w", encoding="utf-8") as f:
                f.write("coin_id,symbol,name\n")
                for result in stablecoins:
                    f.write(
                        f"{result.coin_id},{result.symbol or ''},{result.name or ''}\n"
                    )

            logger.info(f"✅ 稳定币列表已导出到: {stable_file}")
            logger.info(f"   共导出 {len(stablecoins)} 个稳定币")

            # 导出包装币列表
            wrapped_coins = [
                result
                for result in classification_results.values()
                if result.is_wrapped_coin
            ]
            wrapped_file = self.metadata_dir / "wrapped_coins.csv"

            with open(wrapped_file, "w", encoding="utf-8") as f:
                f.write("coin_id,symbol,name\n")
                for result in wrapped_coins:
                    f.write(
                        f"{result.coin_id},{result.symbol or ''},{result.name or ''}\n"
                    )

            logger.info(f"✅ 包装币列表已导出到: {wrapped_file}")
            logger.info(f"   共导出 {len(wrapped_coins)} 个包装币")

            logger.info("✅ 元数据更新完成")
        except Exception as e:
            logger.error(f"更新元数据时出错: {e}")
            self.errors.append(f"元数据更新错误: {str(e)}")

    def generate_final_report(self, duration):
        """生成最终报告"""
        report = f"""
🔍 智能量价数据更新报告
============================================================
📊 处理统计:
   - 总处理币种数: {self.stats['total_processed']}
   - 原生币更新数: {self.stats['native_updated']}
   - 稳定币更新数: {self.stats['stable_updated']}
   - 包装币更新数: {self.stats['wrapped_updated']}
   - 新币种数: {self.stats['new_coins']}
   - 失败更新数: {self.stats['failed_updates']}

⚡ 性能统计:
   - API调用次数: {self.stats['api_calls']}
   - 总耗时: {duration}

🕐 时间信息:
   - 开始时间: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
   - 结束时间: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}

{'✅ 无错误' if not self.errors else f'❌ 错误列表:'}
{chr(10).join(f'   - {error}' for error in self.errors[:10]) if self.errors else ''}
"""

        logger.info(report)

        # 保存报告到文件
        report_file = (
            Path("logs")
            / f"smart_update_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        report_file.write_text(report, encoding="utf-8")

        logger.info(f"📋 更新报告已保存至: {report_file}")
        logger.info("🎉 智能量价数据更新流程完成！")
