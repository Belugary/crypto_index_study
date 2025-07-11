"""
批量更新加密货币量价数据

该脚本会：
1. 获取市值前N名的加密货币
2. 与现有coins目录对比，发现新币种
3. 检测每个币种的最新数据日期
4. 增量下载缺失的量价数据
5. 更新稳定币和包装币元数据
6. 生成更新报告并更新README
"""

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
from pandas.errors import OutOfBoundsDatetime
from tqdm import tqdm

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from examples.stablecoin_checker import StablecoinChecker
from examples.wrapped_coin_checker import WrappedCoinChecker
from src.api.coingecko import CoinGeckoAPI
from src.data.batch_downloader import create_batch_downloader

# API限流配置 (CoinGecko Analyst计划)
RATE_LIMIT_CONFIG = {
    "calls_per_minute": 500,
    "delay_seconds": 0.13,  # 500/min = 8.33/sec ≈ 0.12s间隔，保险起见用0.13s
    "batch_size": 50,  # 每批处理币种数
    "max_retries": 3,  # 最大重试次数
}

# 确保日志目录存在
Path("logs").mkdir(exist_ok=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/price_data_update.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class PriceDataUpdater:
    """量价数据更新器"""

    def __init__(self, api=None, downloader=None, checker=None, wrapped_checker=None):
        """
        初始化量价数据更新器

        Args:
            api: CoinGeckoAPI 实例
            downloader: BatchDownloader 实例
            checker: StablecoinChecker 实例
            wrapped_checker: WrappedCoinChecker 实例
        """
        self.api = api or CoinGeckoAPI()
        self.downloader = downloader or create_batch_downloader()
        self.checker = checker or StablecoinChecker()
        self.wrapped_checker = wrapped_checker or WrappedCoinChecker()
        self.coins_dir = Path("data/coins")
        self.metadata_dir = Path("data/metadata")

        self.errors = []
        self.updated_coins = []
        self.new_coins = []

        self.stats = {
            "total_coins": 0,
            "native_coins": 0,
            "stablecoins": 0,
            "wrapped_coins": 0,
            "searched_coins": 0,
            "target_native_coins": 0,
            "new_coins": 0,
            "updated_coins": 0,
            "failed_coins": 0,
            "total_api_calls": 0,
            "start_time": None,
            "end_time": None,
        }

        # 确保日志目录存在
        Path("logs").mkdir(exist_ok=True)

    def get_top_n_coins_by_market_cap(self, n: int = 500) -> List[Dict]:
        """
        获取市值前N名的加密货币

        Args:
            n: 获取前N名

        Returns:
            币种列表
        """
        logger.info(f"🔍 获取市值前 {n} 名加密货币")

        all_coins = []
        page = 1
        per_page = 250  # CoinGecko单页最大值

        with tqdm(desc="获取市值排名", unit="页", leave=True) as pbar:
            while len(all_coins) < n:
                try:
                    logger.info(f"正在获取第 {page} 页市场数据...")

                    # 计算本页需要获取的数量
                    remaining = n - len(all_coins)
                    current_per_page = min(per_page, remaining)

                    coins = self.api.get_coins_markets(
                        vs_currency="usd",
                        order="market_cap_desc",
                        per_page=current_per_page,
                        page=page,
                    )

                    if not coins:
                        logger.warning(f"第 {page} 页未获取到数据，停止获取")
                        break

                    all_coins.extend(coins)
                    self.stats["total_api_calls"] += 1

                    pbar.update(1)
                    pbar.set_postfix(
                        {"已获取": len(all_coins), "目标": n, "当前页": page}
                    )

                    page += 1

                    # API限流延迟
                    time.sleep(RATE_LIMIT_CONFIG["delay_seconds"])

                except Exception as e:
                    error_msg = f"获取第 {page} 页市场数据失败: {e}"
                    logger.error(error_msg)
                    self.errors.append(error_msg)
                    break

        # 确保只返回前N名
        result = all_coins[:n]
        logger.info(f"✅ 成功获取 {len(result)} 个币种的市值排名")

        return result

    def get_top_n_native_coins_by_market_cap(
        self, target_native_coins: int = 510, max_search_range: int = 3000
    ) -> List[Dict]:
        """
        获取市值前N名的原生币种，自动扩大搜索范围直到找到足够的原生币

        Args:
            target_native_coins: 目标原生币种数量
            max_search_range: 最大搜索范围（默认3000，确保能覆盖足够的币种）

        Returns:
            原生币种列表
        """
        logger.info(f"🔍 获取市值前 {target_native_coins} 名原生币种")

        native_coins = []
        # 智能设定初始搜索范围：目标数量 + 预估的稳定币/包装币数量
        estimated_non_native = int(target_native_coins * 0.3)  # 预估30%为非原生币
        search_range = min(target_native_coins + estimated_non_native, max_search_range)

        while (
            len(native_coins) < target_native_coins and search_range <= max_search_range
        ):
            logger.info(
                f"搜索市值前 {search_range} 名币种以找到 {target_native_coins} 个原生币..."
            )

            # 获取市值排名
            all_coins = self.get_top_n_coins_by_market_cap(search_range)
            if not all_coins:
                break

            # 过滤出原生币种
            native_coins = []
            stable_count = 0
            wrapped_count = 0

            for coin in all_coins:
                try:
                    # 检查是否为稳定币或包装币
                    stable_result = self.checker.is_stablecoin(coin["id"])
                    wrapped_result = self.wrapped_checker.is_wrapped_coin(coin["id"])

                    # 提取实际的布尔值
                    is_stable = (
                        stable_result.get("is_stablecoin", False)
                        if isinstance(stable_result, dict)
                        else stable_result
                    )
                    is_wrapped = (
                        wrapped_result.get("is_wrapped_coin", False)
                        if isinstance(wrapped_result, dict)
                        else wrapped_result
                    )

                    if is_stable:
                        stable_count += 1
                    elif is_wrapped:
                        wrapped_count += 1
                    else:
                        native_coins.append(coin)

                    if len(native_coins) >= target_native_coins:
                        break
                except Exception as e:
                    logger.warning(f"检查币种 {coin['id']} 时出错: {e}")
                    # 如果检查失败，暂时认为是原生币种
                    native_coins.append(coin)
                    if len(native_coins) >= target_native_coins:
                        break

            # 更新统计信息
            self.stats["searched_coins"] = len(all_coins)
            self.stats["stablecoins"] = stable_count
            self.stats["wrapped_coins"] = wrapped_count
            self.stats["native_coins"] = len(native_coins)

            if len(native_coins) >= target_native_coins:
                break
            else:
                # 如果原生币种不足，智能扩大搜索范围
                old_range = search_range
                # 根据缺口大小决定扩大幅度
                shortage = target_native_coins - len(native_coins)
                if shortage > 200:
                    increment = 500  # 缺口大时大幅扩大
                elif shortage > 100:
                    increment = 300  # 中等缺口中等扩大
                else:
                    increment = 200  # 小缺口小幅扩大

                search_range = min(search_range + increment, max_search_range)

                if search_range == old_range:
                    # 达到最大搜索范围，停止
                    logger.warning(
                        f"达到最大搜索范围 {max_search_range}，但只找到 {len(native_coins)} 个原生币种"
                    )
                    break
                logger.info(
                    f"原生币种不足 ({len(native_coins)}/{target_native_coins})，扩大搜索范围到 {search_range}"
                )

        result = native_coins[:target_native_coins]
        self.stats["target_native_coins"] = len(result)
        logger.info(f"✅ 成功获取 {len(result)} 个原生币种")

        return result

    def get_existing_coin_ids(self) -> Set[str]:
        """
        获取现有coins目录中的币种ID

        Returns:
            币种ID集合
        """
        existing_ids = set()

        if self.coins_dir.exists():
            for csv_file in self.coins_dir.glob("*.csv"):
                coin_id = csv_file.stem
                existing_ids.add(coin_id)

        logger.info(f"📋 现有币种数量: {len(existing_ids)}")
        return existing_ids

    def find_new_coins(
        self, top_coins: List[Dict], existing_ids: Set[str]
    ) -> List[Dict]:
        """
        找出新的币种

        Args:
            top_coins: 市值前N名币种
            existing_ids: 现有币种ID集合

        Returns:
            新币种列表
        """
        new_coins = []

        for coin in top_coins:
            coin_id = coin["id"]
            if coin_id not in existing_ids:
                new_coins.append(coin)

        logger.info(f"🆕 发现新币种: {len(new_coins)} 个")
        for coin in new_coins:
            logger.info(
                f"   - {coin['name']} ({coin['symbol'].upper()}) - 市值排名: {coin['market_cap_rank']}"
            )

        return new_coins

    def get_coin_last_date(self, coin_id: str) -> Optional[str]:
        """
        获取币种的最新数据日期

        Args:
            coin_id: 币种ID

        Returns:
            最新日期字符串 (YYYY-MM-DD) 或 None
        """
        csv_file = self.coins_dir / f"{coin_id}.csv"

        if not csv_file.exists():
            logger.debug(f"📄 {coin_id}: CSV文件不存在")
            return None

        try:
            df = pd.read_csv(csv_file)

            # 检查是否有时间列（可能是timestamp或date）
            time_column = None
            if "date" in df.columns:
                time_column = "date"
            elif "timestamp" in df.columns:
                time_column = "timestamp"
            else:
                logger.warning(f"📄 {coin_id}: CSV文件缺少时间列（date或timestamp）")
                return None

            if len(df) == 0:
                logger.warning(f"📄 {coin_id}: CSV文件为空")
                return None

            # 获取最新日期
            if time_column == "timestamp":
                # 如果是timestamp，需要正确处理单位
                try:
                    # 尝试毫秒单位
                    df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.strftime(
                        "%Y-%m-%d"
                    )
                    last_date = df["date"].max()
                except (ValueError, OutOfBoundsDatetime):
                    try:
                        # 尝试秒单位
                        df["date"] = pd.to_datetime(
                            df["timestamp"], unit="s"
                        ).dt.strftime("%Y-%m-%d")
                        last_date = df["date"].max()
                    except (ValueError, OutOfBoundsDatetime):
                        logger.warning(f"📄 {coin_id}: 无法解析timestamp格式")
                        return None
            else:
                last_date = df[time_column].max()

            logger.debug(f"📄 {coin_id}: 最新数据日期 {last_date}")
            return last_date

        except Exception as e:
            logger.warning(f"📄 {coin_id}: 读取CSV文件失败 - {e}")
            return None

    def needs_update(self, coin_id: str) -> Tuple[bool, Optional[str]]:
        """
        检查币种是否需要更新

        Args:
            coin_id: 币种ID

        Returns:
            (是否需要更新, 最新日期)
        """
        last_date = self.get_coin_last_date(coin_id)

        if last_date is None:
            return True, None  # 新币种，需要下载全部数据

        try:
            last_datetime = datetime.strptime(last_date, "%Y-%m-%d")
            today = datetime.now()

            # 如果最新数据不是今天，则需要更新
            if last_datetime.date() < today.date():
                return True, last_date

        except ValueError:
            logger.warning(f"解析日期失败: {last_date}")
            return True, last_date

        return False, last_date

    def download_coin_data(
        self, coin_id: str, is_new_coin: bool = False, from_date: Optional[str] = None
    ) -> bool:
        """
        下载币种的量价数据

        Args:
            coin_id: 币种ID
            is_new_coin: 是否为新币种
            from_date: 起始日期 (增量更新时使用)

        Returns:
            是否下载成功
        """
        try:
            if is_new_coin:
                # 新币种，下载最大天数的历史数据
                logger.info(f"📥 下载新币种 {coin_id} 的完整历史数据...")
                success = self.downloader.download_coin_data(
                    coin_id=coin_id, days="max", vs_currency="usd"
                )
            else:
                # 现有币种，增量更新
                if from_date:
                    # 计算需要更新的天数
                    from_datetime = datetime.strptime(from_date, "%Y-%m-%d")
                    today = datetime.now()
                    days_to_update = (today - from_datetime).days + 1

                    logger.info(
                        f"📥 增量更新 {coin_id}，从 {from_date} 开始，共 {days_to_update} 天..."
                    )
                    success = self.downloader.download_coin_data(
                        coin_id=coin_id, days=str(days_to_update), vs_currency="usd"
                    )
                else:
                    # 无法确定起始日期，重新下载全部数据
                    logger.info(
                        f"📥 重新下载 {coin_id} 的完整历史数据（无法确定增量起始点）..."
                    )
                    success = self.downloader.download_coin_data(
                        coin_id=coin_id, days="max", vs_currency="usd"
                    )

            if success:
                logger.info(f"✅ {coin_id} 数据下载成功")
                return True
            else:
                logger.error(f"❌ {coin_id} 数据下载失败")
                return False

        except Exception as e:
            error_msg = f"下载 {coin_id} 数据时发生异常: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return False

    def update_stablecoin_metadata(self):
        """
        更新稳定币和包装币元数据 (复用已有数据)
        """
        logger.info("💰 更新稳定币和包装币元数据...")

        try:
            # 获取所有需要元数据的币种
            coin_ids = [f.stem for f in self.coins_dir.glob("*.csv")]

            # 检查哪些币种缺少元数据
            missing_metadata = []
            for coin_id in coin_ids:
                metadata_file = self.metadata_dir / "coin_metadata" / f"{coin_id}.json"
                if not metadata_file.exists():
                    missing_metadata.append(coin_id)

            if missing_metadata:
                logger.info(
                    f"🔄 发现 {len(missing_metadata)} 个币种缺少元数据，开始更新..."
                )

                # 批量更新缺失的元数据
                results = self.downloader.batch_update_coin_metadata(
                    coin_ids=missing_metadata,
                    force=False,
                    delay_seconds=RATE_LIMIT_CONFIG["delay_seconds"],
                )

                success_count = sum(1 for success in results.values() if success)
                logger.info(
                    f"✅ 元数据更新完成: {success_count}/{len(missing_metadata)} 成功"
                )

                self.stats["total_api_calls"] += len(missing_metadata)
            else:
                logger.info("✅ 所有币种元数据都是最新的")

            # 重新生成稳定币列表
            checker = StablecoinChecker()
            success = checker.export_stablecoins_csv()

            if success:
                logger.info("✅ 稳定币列表更新成功")
            else:
                logger.error("❌ 稳定币列表更新失败")

            # 重新生成包装币列表
            wrapped_checker = WrappedCoinChecker()
            success = wrapped_checker.export_wrapped_coins_csv()

            if success:
                logger.info("✅ 包装币列表更新成功")
            else:
                logger.error("❌ 包装币列表更新失败")

        except Exception as e:
            error_msg = f"更新稳定币和包装币元数据时发生异常: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)

    def generate_update_report(self) -> str:
        """
        生成更新报告

        Returns:
            报告内容
        """
        duration = self.stats["end_time"] - self.stats["start_time"]

        report = f"""
🔍 量价数据更新报告
{'='*60}
📊 币种分类统计:
   - 搜索范围: 前{self.stats['searched_coins']}名
   - 目标原生币种数: {self.stats['target_native_coins']}
   - 发现原生币种: {self.stats['native_coins']}个
   - 发现稳定币: {self.stats['stablecoins']}个
   - 发现包装币: {self.stats['wrapped_coins']}个

📈 处理统计:
   - 实际处理币种数: {self.stats['total_coins']}
   - 新币种数: {self.stats['new_coins']}
   - 更新币种数: {self.stats['updated_coins']}
   - 失败币种数: {self.stats['failed_coins']}

⚡ 性能统计:
   - API调用次数: {self.stats['total_api_calls']}
   - 总耗时: {duration}

🕐 时间信息:
   - 开始时间: {self.stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
   - 结束时间: {self.stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}

{'❌ 错误信息:' if self.errors else '✅ 无错误'}
"""

        if self.errors:
            for i, error in enumerate(self.errors, 1):
                report += f"   {i}. {error}\n"

        return report

    def update_readme_timestamp(self):
        """
        更新README.md中的最近更新时间
        """
        readme_path = Path("README.md")

        if not readme_path.exists():
            logger.warning("README.md 文件不存在，跳过时间戳更新")
            return

        try:
            content = readme_path.read_text(encoding="utf-8")
            today = datetime.now().strftime("%Y-%m-%d")

            # 查找并替换更新时间
            import re

            pattern = r"\(最近更新: \d{4}-\d{2}-\d{2}\)"
            replacement = f"(最近更新: {today})"

            if re.search(pattern, content):
                new_content = re.sub(pattern, replacement, content)
                readme_path.write_text(new_content, encoding="utf-8")
                logger.info(f"✅ README.md 更新时间已更新为: {today}")
            else:
                logger.warning("README.md 中未找到更新时间格式，跳过更新")

        except Exception as e:
            error_msg = f"更新README.md时发生异常: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)

    def run(self, top_n: int = 600, native_coins: int = 510) -> None:
        """
        执行完整的量价数据更新流程

        Args:
            top_n: 初始搜索范围建议值 (已弃用，现在会自动计算)
            native_coins: 目标原生币种数量
        """
        logger.info(f"🚀 开始量价数据更新流程 (前{native_coins}名原生币)")
        logger.info("=" * 60)

        self.stats["start_time"] = datetime.now()

        try:
            # 1. 获取市值前N名原生币种 (自动计算合适的搜索范围)
            max_search_range = max(native_coins * 2, 2000)  # 确保搜索范围足够大
            native_coins_list = self.get_top_n_native_coins_by_market_cap(
                native_coins, max_search_range
            )
            if not native_coins_list:
                logger.error("❌ 无法获取原生币种数据")
                return

            # 2. 获取现有币种ID
            existing_ids = self.get_existing_coin_ids()

            # 3. 找出新币种
            new_coins = self.find_new_coins(native_coins_list, existing_ids)
            self.stats["new_coins"] = len(new_coins)

            # 4. 所有需要处理的币种就是这些原生币种
            all_target_coins = {coin["id"]: coin for coin in native_coins_list}

            self.stats["total_coins"] = len(all_target_coins)

            # 5. 批量更新量价数据
            logger.info(f"📥 开始批量更新 {len(all_target_coins)} 个币种的量价数据")

            updated_count = 0
            failed_count = 0

            with tqdm(
                total=len(all_target_coins),
                desc="更新量价数据",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}{postfix}]",
                ncols=100,
                leave=False,
                position=0,
            ) as pbar:
                for coin_id, coin_info in all_target_coins.items():
                    try:
                        # 检查是否是稳定币
                        stable_result = self.checker.is_stablecoin(
                            coin_info.get("symbol", "")
                        )
                        is_stable = (
                            stable_result.get("is_stablecoin", False)
                            if isinstance(stable_result, dict)
                            else stable_result
                        )

                        if is_stable:
                            pbar.update(1)
                            pbar.set_postfix({"状态": "跳过稳定币"})
                            continue

                        # 检查是否是包装币
                        if self.wrapped_checker.is_wrapped_coin(coin_id)[
                            "is_wrapped_coin"
                        ]:
                            pbar.update(1)
                            pbar.set_postfix({"状态": "跳过包装币"})
                            continue

                        # 检查是否需要更新
                        is_new_coin = coin_id in [c["id"] for c in new_coins]
                        needs_update, last_date = self.needs_update(coin_id)

                        if needs_update:
                            success = self.download_coin_data(
                                coin_id=coin_id,
                                is_new_coin=is_new_coin,
                                from_date=last_date,
                            )

                            if success:
                                updated_count += 1
                                if is_new_coin:
                                    self.new_coins.append(coin_id)
                                else:
                                    self.updated_coins.append(coin_id)
                            else:
                                failed_count += 1

                            self.stats["total_api_calls"] += 1

                            # API限流延迟
                            time.sleep(RATE_LIMIT_CONFIG["delay_seconds"])

                        pbar.update(1)
                        pbar.set_postfix(
                            {
                                "更新": updated_count,
                                "失败": failed_count,
                                "当前": coin_id[:20],
                            }
                        )

                    except Exception as e:
                        error_msg = f"处理币种 {coin_id} 时发生异常: {e}"
                        logger.error(error_msg)
                        self.errors.append(error_msg)
                        failed_count += 1

                        pbar.update(1)

            self.stats["updated_coins"] = updated_count
            self.stats["failed_coins"] = failed_count

            # 6. 更新稳定币和包装币元数据
            self.update_stablecoin_metadata()

            # 7. 更新README时间戳
            self.update_readme_timestamp()

        except Exception as e:
            error_msg = f"更新流程中发生异常: {e}"
            logger.error(error_msg)
            self.errors.append(error_msg)

        finally:
            self.stats["end_time"] = datetime.now()

            # 8. 生成更新报告
            report = self.generate_update_report()
            logger.info(report)

            # 保存报告到文件
            report_file = (
                Path("logs")
                / f"price_update_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )
            report_file.write_text(report, encoding="utf-8")

            logger.info(f"📋 更新报告已保存至: {report_file}")
            logger.info("🎉 量价数据更新流程完成！")


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(description="更新加密货币量价数据")
    parser.add_argument(
        "--top-n", type=int, default=600, help="市值前N名搜索范围 (默认: 600)"
    )
    parser.add_argument(
        "--native-coins", type=int, default=510, help="目标原生币种数量 (默认: 510)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=50, help="批处理大小 (默认: 50)"
    )
    parser.add_argument(
        "--delay", type=float, default=0.13, help="API调用延迟秒数 (默认: 0.13)"
    )

    args = parser.parse_args()

    # 更新配置
    RATE_LIMIT_CONFIG["batch_size"] = args.batch_size
    RATE_LIMIT_CONFIG["delay_seconds"] = args.delay

    print(f"🔍 量价数据更新工具")
    print(f"📊 配置信息:")
    print(f"   - 目标原生币种数: {args.native_coins}")
    print(f"   - 搜索范围: 动态扩展（自动调整直到找到足够的原生币）")
    print(f"   - 批处理大小: {args.batch_size}")
    print(f"   - API调用延迟: {args.delay}秒")
    print(f"   - 预估API调用频率: {60/args.delay:.1f}次/分钟")
    print("")

    # 创建更新器并运行
    updater = PriceDataUpdater()
    updater.run(top_n=args.top_n, native_coins=args.native_coins)


if __name__ == "__main__":
    main()
