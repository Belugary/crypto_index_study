"""
批量数据下载器

实现数字货币市场数据的批量下载、存储和管理功能。
遵循 "简单胜于复杂" 的设计理念，提供高效可靠的数据获取服务。
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from tqdm import tqdm

from ..api.coingecko import CoinGeckoAPI


class BatchDownloader:
    """
    数字货币市场数据批量下载器

    提供智能缓存、错误重试、进度跟踪等功能，
    遵循 "优雅胜于丑陋" 的设计原则。
    """

    def __init__(self, api: CoinGeckoAPI, data_dir: str = "data"):
        """
        初始化批量下载器

        Args:
            api: CoinGecko API 客户端实例
            data_dir: 数据存储根目录
        """
        self.api = api
        self.data_dir = Path(data_dir)
        self.coins_dir = self.data_dir / "coins"
        self.metadata_dir = self.data_dir / "metadata"
        self.logs_dir = self.data_dir / "logs"

        # 创建必要的目录结构
        self._ensure_directories()

        # 设置日志
        self.logger = self._setup_logger()

        self.logger.info("批量下载器初始化完成")

    def download_batch(
        self,
        top_n: int,
        days: str,
        vs_currency: str = "usd",
        force_update: bool = False,
        force_overwrite: bool = False,
        buffer_size: int = 500,
        max_retries: int = 3,
        retry_delay: int = 5,
        request_interval: int = 1,
    ) -> Dict[str, str]:
        """
        批量下载币种市场数据

        "现在胜于永不" - 立即开始下载，而不是等待完美的时机

        Args:
            top_n: 需要下载的前 N 名币种数量
            days: 历史数据天数，可以是数字字符串或 "max"
            vs_currency: 对比货币，默认为 "usd"
            force_update: 是否强制更新（忽略缓存）
            force_overwrite: 是否强制覆盖（忽略新鲜度检查），默认 False
            buffer_size: 获取币种列表的缓冲区大小
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）
            request_interval: 请求间隔（秒）

        Returns:
            Dict[str, str]: 每个币种的处理状态
            - "success": 成功下载/更新
            - "skipped": 数据已是最新，跳过
            - "failed": 下载失败
        """
        self.logger.info(f"开始批量下载任务：前{top_n}名币种，{days}天数据")

        results = {}
        failed_coins = []  # 记录失败的币种

        try:
            # 获取前 N 名币种列表
            coin_list = self._get_top_coins(top_n, buffer_size)
            self.logger.info(f"获取到 {len(coin_list)} 个目标币种")

            # 使用进度条显示下载进度
            with tqdm(coin_list, desc="下载币种数据", unit="币种") as pbar:
                for coin_id in pbar:
                    pbar.set_postfix({"当前": coin_id})

                    # 检查是否需要更新 (force_overwrite 会跳过新鲜度检查)
                    if (
                        not force_update
                        and not force_overwrite
                        and self._check_data_freshness(coin_id, days)
                    ):
                        results[coin_id] = "skipped"
                        self.logger.debug(f"{coin_id}: 数据已是最新，跳过")
                        continue

                    # 下载数据
                    success = self._download_single_coin(
                        coin_id, days, vs_currency, max_retries, retry_delay
                    )

                    if success:
                        results[coin_id] = "success"
                    else:
                        results[coin_id] = "failed"
                        failed_coins.append(coin_id)  # 记录失败的币种

                    # API 请求间隔
                    if request_interval > 0:
                        time.sleep(request_interval)

            # 记录下载统计
            success_count = sum(1 for status in results.values() if status == "success")
            skipped_count = sum(1 for status in results.values() if status == "skipped")
            failed_count = sum(1 for status in results.values() if status == "failed")

            self.logger.info(
                f"批量下载完成 - 成功: {success_count}, 跳过: {skipped_count}, 失败: {failed_count}"
            )

            # 记录失败的币种详情
            if failed_coins:
                self.logger.warning(f"下载失败的币种: {', '.join(failed_coins)}")
                self._save_failed_coins_log(failed_coins, days)

        except Exception as e:
            self.logger.error(f"批量下载过程中发生错误: {e}")
            raise

        return results

    def _get_top_coins(self, top_n: int, buffer_size: int) -> List[str]:
        """
        获取按市值排名的前 N 名币种

        "实用胜于纯粹" - 使用更大的缓冲区来应对排名波动
        """
        try:
            # 获取市场数据，按市值排序
            market_data = self.api.get_coins_markets(
                vs_currency="usd",
                order="market_cap_desc",
                per_page=min(buffer_size, 250),  # API限制每页最多250个
                page=1,
                sparkline=False,
            )

            # 提取币种ID列表
            coin_ids = [coin["id"] for coin in market_data[:top_n]]

            self.logger.info(f"按市值排序，从 {len(market_data)} 个币种中选择前 {len(coin_ids)} 个")
            return coin_ids

        except Exception as e:
            self.logger.error(f"获取币种列表失败: {e}")
            raise

    def _check_data_freshness(self, coin_id: str, days: str) -> bool:
        """
        检查本地数据是否足够新鲜

        "简单胜于复杂" - 使用直观的时间判断逻辑
        """
        try:
            metadata_file = self.metadata_dir / "download_metadata.json"
            if not metadata_file.exists():
                return False

            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            coin_metadata = metadata.get(coin_id, {})
            if not coin_metadata:
                return False

            last_update = datetime.fromisoformat(coin_metadata.get("last_update", ""))
            last_days = coin_metadata.get("days", "")

            # 检查参数是否匹配
            if last_days != days:
                return False

            # 检查时间新鲜度
            now = datetime.now(timezone.utc)

            if days == "max":
                # max 数据：24小时内的更新认为是新鲜的
                return (now - last_update).total_seconds() < 24 * 3600
            else:
                # 具体天数：检查是否包含最新的完整交易日
                # 简化实现：12小时内的更新认为是新鲜的
                return (now - last_update).total_seconds() < 12 * 3600

        except Exception as e:
            self.logger.debug(f"检查数据新鲜度时出错 ({coin_id}): {e}")
            return False

    def _download_single_coin(
        self,
        coin_id: str,
        days: str,
        vs_currency: str,
        max_retries: int,
        retry_delay: int,
    ) -> bool:
        """
        下载单个币种的数据

        "错误不应该悄悄通过" - 明确处理每一个可能的错误
        """
        for attempt in range(max_retries):
            try:
                # 调用 API 获取数据
                data = self.api.get_coin_market_chart(
                    coin_id=coin_id, vs_currency=vs_currency, days=days
                )

                # 保存数据到 CSV
                if self._save_to_csv(coin_id, data):
                    # 更新元数据
                    self._update_metadata(coin_id, days)
                    self.logger.info(f"{coin_id}: 下载成功")
                    return True
                else:
                    self.logger.warning(f"{coin_id}: 数据保存失败")
                    return False

            except Exception as e:
                self.logger.warning(f"{coin_id}: 第 {attempt + 1} 次尝试失败: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                else:
                    self.logger.error(f"{coin_id}: 所有重试都失败，放弃下载")

        return False

    def _save_to_csv(self, coin_id: str, data: Dict[str, Any]) -> bool:
        """
        将市场数据保存为 CSV 文件

        "应该有一种-- 最好只有一种 --明显的方法来做一件事"
        """
        try:
            # 提取数据
            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])
            total_volumes = data.get("total_volumes", [])

            if not prices:
                self.logger.warning(f"{coin_id}: 没有价格数据")
                return False

            # 创建 DataFrame
            df_data = []
            for i, (timestamp, price) in enumerate(prices):
                row = {
                    "timestamp": int(timestamp),
                    "price": float(price),
                    "volume": (
                        float(total_volumes[i][1]) if i < len(total_volumes) else None
                    ),
                    "market_cap": (
                        float(market_caps[i][1]) if i < len(market_caps) else None
                    ),
                }
                df_data.append(row)

            df = pd.DataFrame(df_data)

            # 保存到 CSV
            csv_file = self.coins_dir / f"{coin_id}.csv"
            df.to_csv(csv_file, index=False)

            self.logger.debug(f"{coin_id}: 保存 {len(df)} 条记录到 {csv_file}")
            return True

        except Exception as e:
            self.logger.error(f"{coin_id}: 保存 CSV 文件失败: {e}")
            return False

    def _update_metadata(self, coin_id: str, days: str) -> None:
        """
        更新下载元数据

        "命名空间是一个绝妙的理念" - 将元数据组织得清晰明了
        """
        try:
            metadata_file = self.metadata_dir / "download_metadata.json"

            # 读取现有元数据
            metadata = {}
            if metadata_file.exists():
                with open(metadata_file, "r", encoding="utf-8") as f:
                    metadata = json.load(f)

            # 更新币种元数据
            metadata[coin_id] = {
                "last_update": datetime.now(timezone.utc).isoformat(),
                "days": days,
                "version": "1.0",
            }

            # 保存元数据
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

        except Exception as e:
            self.logger.error(f"更新元数据失败 ({coin_id}): {e}")

    def _ensure_directories(self) -> None:
        """
        确保所有必要的目录存在

        "显式胜于隐式" - 明确创建所需的目录结构
        """
        for directory in [
            self.data_dir,
            self.coins_dir,
            self.metadata_dir,
            self.logs_dir,
        ]:
            directory.mkdir(parents=True, exist_ok=True)

    def _setup_logger(self) -> logging.Logger:
        """
        设置日志系统

        "可读性很重要" - 提供清晰的日志输出
        """
        logger = logging.getLogger(f"BatchDownloader.{id(self)}")
        logger.setLevel(logging.INFO)

        # 避免重复添加处理器
        if logger.handlers:
            return logger

        # 创建格式器
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 文件处理器
        file_handler = logging.FileHandler(
            self.logs_dir / "batch_download.log", encoding="utf-8"
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)

        # 添加处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

        return logger

    def get_download_status(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """
        获取指定币种的下载状态

        "如果实现很难解释，那就是个坏想法" - 提供简单的状态查询接口
        """
        try:
            metadata_file = self.metadata_dir / "download_metadata.json"
            if not metadata_file.exists():
                return None

            with open(metadata_file, "r", encoding="utf-8") as f:
                metadata = json.load(f)

            return metadata.get(coin_id)

        except Exception as e:
            self.logger.error(f"获取下载状态失败 ({coin_id}): {e}")
            return None

    def list_downloaded_coins(self) -> List[str]:
        """
        列出所有已下载的币种

        "应该有一种-- 最好只有一种 --明显的方法来做一件事"
        """
        try:
            csv_files = list(self.coins_dir.glob("*.csv"))
            return [f.stem for f in csv_files]
        except Exception as e:
            self.logger.error(f"列出已下载币种失败: {e}")
            return []

    def _save_failed_coins_log(self, failed_coins: List[str], days: str) -> None:
        """
        保存失败币种记录到日志文件

        "显式胜于隐式" - 明确记录失败信息供后续处理
        """
        try:
            log_file = self.logs_dir / "failed_downloads.log"
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"\n=== 下载失败记录 ({timestamp}) ===\n")
                f.write(f"参数: days={days}\n")
                f.write(f"失败币种数量: {len(failed_coins)}\n")
                f.write("失败币种列表:\n")
                for coin in failed_coins:
                    f.write(f"  - {coin}\n")
                f.write("\n")

            self.logger.info(f"失败记录已保存到: {log_file}")

        except Exception as e:
            self.logger.error(f"保存失败记录时出错: {e}")

    def retry_failed_downloads(
        self,
        failed_coins: List[str],
        days: str,
        vs_currency: str = "usd",
        max_retries: int = 3,
        retry_delay: int = 5,
        request_interval: int = 2,
    ) -> Dict[str, str]:
        """
        重新下载失败的币种

        "面对错误，永远不要默默忽视" - 提供重试机制修复失败

        Args:
            failed_coins: 失败的币种列表
            days: 历史数据天数
            vs_currency: 对比货币
            max_retries: 最大重试次数
            retry_delay: 重试延迟
            request_interval: 请求间隔

        Returns:
            Dict[str, str]: 重试结果
        """
        self.logger.info(f"开始重试下载 {len(failed_coins)} 个失败的币种")

        results = {}

        try:
            with tqdm(failed_coins, desc="重试下载", unit="币种") as pbar:
                for coin_id in pbar:
                    pbar.set_postfix({"当前": coin_id})

                    success = self._download_single_coin(
                        coin_id, days, vs_currency, max_retries, retry_delay
                    )

                    results[coin_id] = "success" if success else "failed"

                    if request_interval > 0:
                        time.sleep(request_interval)

            # 统计重试结果
            retry_success = sum(1 for status in results.values() if status == "success")
            retry_failed = sum(1 for status in results.values() if status == "failed")

            self.logger.info(
                f"重试完成 - 成功: {retry_success}, 仍失败: {retry_failed}"
            )

            # 如果还有失败的，再次记录
            still_failed = [
                coin for coin, status in results.items() if status == "failed"
            ]
            if still_failed:
                self._save_failed_coins_log(still_failed, days)

        except Exception as e:
            self.logger.error(f"重试下载过程中发生错误: {e}")

        return results

    def get_failed_coins_from_log(self) -> List[str]:
        """
        从日志文件中获取最近失败的币种列表

        Returns:
            List[str]: 最近失败的币种列表
        """
        try:
            log_file = self.logs_dir / "failed_downloads.log"
            if not log_file.exists():
                return []

            with open(log_file, "r", encoding="utf-8") as f:
                content = f.read()

            # 找到最后一次失败记录
            records = content.split("=== 下载失败记录")
            if len(records) < 2:
                return []

            # 获取最后一个记录
            last_record = records[-1]

            failed_coins = []
            lines = last_record.split("\n")

            # 寻找币种列表
            in_coins_section = False
            for line in lines:
                line = line.strip()
                if line == "失败币种列表:":
                    in_coins_section = True
                    continue
                elif in_coins_section and line.startswith("- "):
                    coin_name = line[2:]  # 去掉 "- " 前缀
                    failed_coins.append(coin_name)
                elif in_coins_section and line == "" and failed_coins:
                    # 遇到空行且已经找到币种，结束币种列表
                    break

            return failed_coins

        except Exception as e:
            self.logger.error(f"读取失败记录时出错: {e}")
            return []


def create_batch_downloader(
    api_key: Optional[str] = None, data_dir: str = "data"
) -> BatchDownloader:
    """
    创建批量下载器的便捷函数

    "美丽胜于丑陋" - 提供优雅的创建接口

    Args:
        api_key: CoinGecko Pro API 密钥
        data_dir: 数据存储目录

    Returns:
        BatchDownloader: 配置好的批量下载器实例

    Example:
        >>> # 创建下载器
        >>> downloader = create_batch_downloader()
        >>>
        >>> # 下载前30名币种的最近30天数据
        >>> results = downloader.download_batch(top_n=30, days="30")
        >>>
        >>> # 下载前10名币种的全部历史数据
        >>> results = downloader.download_batch(top_n=10, days="max")
    """
    from ..api.coingecko import create_api_client

    api = create_api_client(api_key)
    return BatchDownloader(api, data_dir)
