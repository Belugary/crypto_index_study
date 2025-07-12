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
"""

import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from tqdm import tqdm

from ..api.coingecko import CoinGeckoAPI
from ..classification.stablecoin_checker import StablecoinChecker
from ..classification.wrapped_coin_checker import WrappedCoinChecker
from ..downloaders.batch_downloader import create_batch_downloader

# API限流配置
RATE_LIMIT_CONFIG = {
    "delay_seconds": 0.13,
    "calls_per_minute": 461.5,
}

logger = logging.getLogger(__name__)


class CoinClassifier:
    """币种分类器 - 职责单一：基于CoinGecko分类币种"""

    def __init__(self):
        self.stablecoin_checker = StablecoinChecker()
        self.wrapped_checker = WrappedCoinChecker()

    def classify_coin(self, coin_id: str) -> str:
        """
        分类单个币种

        Args:
            coin_id: 币种ID

        Returns:
            'stable' | 'wrapped' | 'native'
        """
        # 检查是否为稳定币
        stable_result = self.stablecoin_checker.is_stablecoin(coin_id)
        if isinstance(stable_result, dict):
            is_stable = stable_result.get("is_stablecoin", False)
        else:
            is_stable = stable_result

        if is_stable:
            return "stable"

        # 检查是否为包装币
        wrapped_result = self.wrapped_checker.is_wrapped_coin(coin_id)
        if isinstance(wrapped_result, dict):
            is_wrapped = wrapped_result.get("is_wrapped_coin", False)
        else:
            is_wrapped = wrapped_result

        if is_wrapped:
            return "wrapped"

        # 既不是稳定币也不是包装币，就是原生币
        return "native"


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
        self.classifier = CoinClassifier()
        self.market_fetcher = MarketDataFetcher(self.api)

        # 目录设置
        self.coins_dir = Path("data/coins")
        self.metadata_dir = Path("data/metadata")

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

    def needs_update(self, coin_id: str) -> Tuple[bool, Optional[str]]:
        """检查币种是否需要更新"""
        csv_file = self.coins_dir / f"{coin_id}.csv"

        if not csv_file.exists():
            return True, None

        try:
            # 读取最后一行获取最新时间戳
            with open(csv_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                if len(lines) <= 1:  # 只有表头
                    return True, None

                last_line = lines[-1].strip()
                if not last_line:
                    return True, None

                # 获取时间戳并转换为日期
                timestamp_str = last_line.split(",")[0]
                try:
                    timestamp = int(float(timestamp_str))
                    # 转换为UTC日期进行比较
                    last_date = datetime.fromtimestamp(
                        timestamp / 1000, tz=timezone.utc
                    )
                    today_utc = datetime.now(tz=timezone.utc)

                    # 比较日期部分
                    last_date_str = last_date.strftime("%Y-%m-%d")
                    today_str = today_utc.strftime("%Y-%m-%d")

                    if last_date_str < today_str:
                        logger.debug(
                            f"{coin_id}: 数据过期 (最新: {last_date_str}, 今天: {today_str})"
                        )
                        return True, last_date_str
                    else:
                        logger.debug(
                            f"{coin_id}: 数据最新 (最新: {last_date_str}, 今天: {today_str})"
                        )
                        return False, last_date_str
                except (ValueError, TypeError) as e:
                    logger.warning(f"{coin_id}: 时间戳格式错误 {timestamp_str}: {e}")
                    return True, None

        except Exception as e:
            logger.error(f"检查 {coin_id} 更新状态时出错: {e}")
            return True, None

    def download_coin_data(
        self, coin_id: str, is_new_coin: bool, from_date: Optional[str]
    ) -> bool:
        """下载币种数据"""
        try:
            if is_new_coin:
                logger.info(f"📥 新币种 {coin_id}，下载完整历史数据...")
                success = self.downloader.download_coin_data(coin_id, days="max")
            else:
                days_to_update = (
                    self._calculate_days_since(from_date) if from_date else 1
                )
                logger.info(
                    f"📥 增量更新 {coin_id}，从 {from_date} 开始，共 {days_to_update} 天..."
                )
                success = self.downloader.download_coin_data(
                    coin_id, days=str(days_to_update)
                )

            if success:
                logger.info(f"✅ {coin_id} 数据下载成功")
                return True
            else:
                logger.error(f"❌ {coin_id} 数据下载失败")
                return False

        except Exception as e:
            logger.error(f"下载 {coin_id} 数据时出错: {e}")
            return False

    def _calculate_days_since(self, date_str: str) -> int:
        """计算从指定日期到今天的天数"""
        try:
            from datetime import date

            target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            today = datetime.now(tz=timezone.utc).date()
            days_diff = (today - target_date).days + 1  # +1 确保包含今天
            return max(1, days_diff)  # 至少返回1天
        except Exception as e:
            logger.warning(f"计算日期差异失败: {e}, 默认返回1天")
            return 1

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
                            # 分类币种
                            coin_type = self.classifier.classify_coin(coin_id)

                            # 检查是否需要更新
                            is_new_coin = coin_id not in existing_ids
                            needs_update, last_date = self.needs_update(coin_id)

                            if needs_update:
                                # 下载数据
                                success = self.download_coin_data(
                                    coin_id, is_new_coin, last_date
                                )

                                if success:
                                    # 更新统计
                                    if coin_type == "native":
                                        native_coins_updated += 1
                                        self.stats["native_updated"] += 1
                                    elif coin_type == "stable":
                                        self.stats["stable_updated"] += 1
                                    elif coin_type == "wrapped":
                                        self.stats["wrapped_updated"] += 1

                                    if is_new_coin:
                                        self.stats["new_coins"] += 1
                                        existing_ids.add(coin_id)

                                    self.stats["api_calls"] += 1

                                    # API限流延迟
                                    time.sleep(RATE_LIMIT_CONFIG["delay_seconds"])
                                else:
                                    self.stats["failed_updates"] += 1
                                    self.errors.append(f"{coin_id}: 下载失败")
                            else:
                                # 数据已是最新，但如果是原生币仍计入统计
                                if coin_type == "native":
                                    native_coins_updated += 1
                                    self.stats["native_updated"] += 1

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
            # 更新稳定币元数据
            stablecoin_checker = StablecoinChecker()
            # 导出稳定币列表 (基于现有方法)
            stablecoins = stablecoin_checker.get_all_stablecoins()
            stable_file = self.metadata_dir / "stablecoins.csv"

            with open(stable_file, "w", encoding="utf-8") as f:
                f.write("coin_id,symbol,name\n")
                for coin_info in stablecoins:
                    f.write(
                        f"{coin_info.get('id', '')},{coin_info.get('symbol', '')},{coin_info.get('name', '')}\n"
                    )

            print(f"✅ 稳定币列表已导出到: {stable_file}")
            print(f"   共导出 {len(stablecoins)} 个稳定币")

            # 更新包装币元数据
            wrapped_checker = WrappedCoinChecker()
            # 导出包装币列表 (基于现有方法)
            wrapped_coins = wrapped_checker.get_all_wrapped_coins()
            wrapped_file = self.metadata_dir / "wrapped_coins.csv"

            with open(wrapped_file, "w", encoding="utf-8") as f:
                f.write("coin_id,symbol,name\n")
                for coin_info in wrapped_coins:
                    f.write(
                        f"{coin_info.get('id', '')},{coin_info.get('symbol', '')},{coin_info.get('name', '')}\n"
                    )

            print(f"✅ 包装币列表已导出到: {wrapped_file}")
            print(f"   共导出 {len(wrapped_coins)} 个包装币")

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
