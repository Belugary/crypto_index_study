"""
批量数据下载器

实现数字货币市场数据的批量下载、存储和管理功能。
遵循 "简单胜于复杂" 的设计理念，提供高效可靠的数据获取服务。

⚠️ 数据时间戳注意事项：
- API可能返回实时数据时间戳（如07:08:54格式）
- 覆盖模式保存确保数据一致性和自动校正
- 详见：docs/timestamp_handling_memo.md
"""

import json
import logging
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
        # 项目根目录，用于存放日志文件
        self.base_dir = (
            self.data_dir.parent if self.data_dir.name == "data" else Path(".")
        )
        self.coins_dir = self.data_dir / "coins"
        self.metadata_dir = self.data_dir / "metadata"
        # 日志文件统一放在项目根目录的 logs/ 下
        self.logs_dir = self.base_dir / "logs"

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

        "实用胜于纯粹" - 使用分页获取来确保能获取足够的币种数据
        """
        try:
            market_data = []
            needed_coins = max(top_n, buffer_size)  # 确保获取足够的币种

            # 计算需要多少页
            per_page = 250  # API每页最大250个
            total_pages = (needed_coins + per_page - 1) // per_page  # 向上取整

            self.logger.info(
                f"需要获取前 {needed_coins} 个币种，将分 {total_pages} 页获取"
            )

            # 分页获取市场数据
            for page in range(1, total_pages + 1):
                page_size = min(per_page, needed_coins - len(market_data))

                self.logger.info(
                    f"正在获取第 {page}/{total_pages} 页数据 (每页 {page_size} 个)"
                )

                page_data = self.api.get_coins_markets(
                    vs_currency="usd",
                    order="market_cap_desc",
                    per_page=page_size,
                    page=page,
                    sparkline=False,
                )

                market_data.extend(page_data)

                # 如果获取的数据已经够了，就停止
                if len(market_data) >= needed_coins:
                    break

                # 避免API限制，稍微延迟一下
                if page < total_pages:
                    time.sleep(0.5)

            # 提取币种ID列表，取前top_n个
            coin_ids = [coin["id"] for coin in market_data[:top_n]]

            self.logger.info(
                f"成功获取 {len(market_data)} 个币种数据，选择前 {len(coin_ids)} 个"
            )
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

        保存的字段说明:
        - timestamp: Unix时间戳 (毫秒)
        - price: 价格 (USD)
        - volume: 24小时交易量 (USD)
        - market_cap: 流通市值 (USD)
          ⚠️ 重要: 这是流通市值 (Circulating Market Cap)，不是完全稀释市值
          计算公式: market_cap = 当前价格 × 流通供应量
          数据来源: CoinGecko API get_coin_market_chart 端点的 market_caps 字段

        "应该有一种-- 最好只有一种 --明显的方法来做一件事"
        """
        try:
            # 提取数据
            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])  # 流通市值，非完全稀释市值
            total_volumes = data.get("total_volumes", [])

            if not prices:
                self.logger.warning(f"{coin_id}: 没有价格数据")
                return False

            # 创建 DataFrame
            df_data = []
            for i, (timestamp, price) in enumerate(prices):
                # 安全处理 None 值
                volume = None
                if i < len(total_volumes) and total_volumes[i][1] is not None:
                    volume = float(total_volumes[i][1])

                market_cap = None
                if i < len(market_caps) and market_caps[i][1] is not None:
                    market_cap = float(
                        market_caps[i][1]
                    )  # 流通市值 (Circulating Market Cap)

                row = {
                    "timestamp": int(timestamp),
                    "price": float(price) if price is not None else None,
                    "volume": volume,
                    "market_cap": market_cap,  # 流通市值，用于指数计算和排名
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
            self.metadata_dir / "coin_metadata",  # 新增：币种元数据目录
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

    def _load_coin_metadata(self, coin_id: str) -> Optional[Dict[str, Any]]:
        """
        加载单个币种的元数据

        Args:
            coin_id: 币种ID

        Returns:
            币种元数据字典，如果文件不存在则返回None
        """
        try:
            metadata_file = self.metadata_dir / "coin_metadata" / f"{coin_id}.json"
            if metadata_file.exists():
                with open(metadata_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            return None
        except Exception as e:
            self.logger.error(f"加载币种元数据失败 ({coin_id}): {e}")
            return None

    def _save_coin_metadata(self, coin_id: str, metadata: Dict[str, Any]) -> bool:
        """
        保存单个币种的元数据

        Args:
            coin_id: 币种ID
            metadata: 元数据字典

        Returns:
            bool: 保存是否成功
        """
        try:
            metadata_file = self.metadata_dir / "coin_metadata" / f"{coin_id}.json"
            with open(metadata_file, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            self.logger.error(f"保存币种元数据失败 ({coin_id}): {e}")
            return False

    def _need_coin_metadata_update(self, coin_id: str, max_age_days: int = 7) -> bool:
        """
        检查币种元数据是否需要更新

        Args:
            coin_id: 币种ID
            max_age_days: 最大缓存天数

        Returns:
            bool: 是否需要更新
        """
        try:
            metadata = self._load_coin_metadata(coin_id)
            if not metadata:
                return True  # 没有元数据，需要更新

            last_updated = metadata.get("last_updated")
            if not last_updated:
                return True  # 没有更新时间，需要更新

            # 解析时间并检查是否过期
            from datetime import datetime, timedelta, timezone

            try:
                last_update_time = datetime.fromisoformat(
                    last_updated.replace("Z", "+00:00")
                )
                now = datetime.now(timezone.utc)
                age = now - last_update_time

                return age.days >= max_age_days
            except (ValueError, AttributeError):
                return True  # 时间格式错误，需要更新

        except Exception as e:
            self.logger.error(f"检查币种元数据更新状态失败 ({coin_id}): {e}")
            return True  # 出错时默认需要更新

    def download_coin_data(
        self,
        coin_id: str,
        days: str,
        vs_currency: str = "usd",
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> bool:
        """
        下载单个币种的量价数据

        Args:
            coin_id: 币种ID
            days: 历史数据天数，可以是数字字符串或 "max"
            vs_currency: 对比货币，默认为 "usd"
            max_retries: 最大重试次数
            retry_delay: 重试延迟（秒）

        Returns:
            bool: 下载是否成功
        """
        return self._download_single_coin(
            coin_id=coin_id,
            days=days,
            vs_currency=vs_currency,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )

    def update_coin_metadata(self, coin_id: str, force: bool = False) -> bool:
        """
        更新单个币种的元数据

        Args:
            coin_id: 币种ID
            force: 是否强制更新

        Returns:
            bool: 更新是否成功
        """
        try:
            # 检查是否需要更新
            if not force and not self._need_coin_metadata_update(coin_id):
                self.logger.info(f"币种元数据无需更新 ({coin_id})")
                return True

            self.logger.info(f"开始更新币种元数据 ({coin_id})")

            # 调用API获取完整的币种信息
            coin_data = self.api.get_coin_by_id(
                coin_id=coin_id,
                localization=False,  # 不需要本地化
                tickers=False,  # 不需要交易行情
                market_data=False,  # 不需要市场数据
                community_data=True,  # 需要社区数据
                developer_data=True,  # 需要开发者数据
                sparkline=False,  # 不需要走势图
            )

            # 提取需要保存的字段
            metadata = {
                "id": coin_data.get("id"),
                "symbol": coin_data.get("symbol"),
                "name": coin_data.get("name"),
                "categories": coin_data.get("categories", []),
                "asset_platform_id": coin_data.get("asset_platform_id"),
                "platforms": coin_data.get("platforms", {}),
                "block_time_in_minutes": coin_data.get("block_time_in_minutes"),
                "hashing_algorithm": coin_data.get("hashing_algorithm"),
                "genesis_date": coin_data.get("genesis_date"),
                "country_origin": coin_data.get("country_origin"),
                "description": coin_data.get("description", {}),
                "links": coin_data.get("links", {}),
                "image": coin_data.get("image", {}),
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            # 保存元数据
            if self._save_coin_metadata(coin_id, metadata):
                self.logger.info(f"币种元数据更新成功 ({coin_id})")
                return True
            else:
                self.logger.error(f"币种元数据保存失败 ({coin_id})")
                return False

        except Exception as e:
            self.logger.error(f"更新币种元数据失败 ({coin_id}): {e}")
            return False

    def batch_update_coin_metadata(
        self, coin_ids: List[str], force: bool = False, delay_seconds: float = 0.2
    ) -> Dict[str, bool]:
        """
        批量更新币种元数据

        Args:
            coin_ids: 币种ID列表
            force: 是否强制更新
            delay_seconds: 每次API调用之间的延迟(秒)

        Returns:
            Dict[str, bool]: 每个币种的更新结果
        """
        results = {}

        self.logger.info(f"开始批量更新币种元数据，共 {len(coin_ids)} 个币种")

        for i, coin_id in enumerate(coin_ids):
            self.logger.info(f"正在更新 ({i+1}/{len(coin_ids)}): {coin_id}")

            # 更新单个币种
            results[coin_id] = self.update_coin_metadata(coin_id, force)

            # 添加延迟避免API限制
            if i < len(coin_ids) - 1:  # 最后一个不需要延迟
                time.sleep(delay_seconds)

        # 统计结果
        success_count = sum(1 for success in results.values() if success)
        self.logger.info(f"批量更新完成: {success_count}/{len(coin_ids)} 成功")

        return results


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
