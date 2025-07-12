"""
元数据更新器

核心功能模块，提供批量元数据更新和分析功能。

该模块提供：
1. 批量更新币种元数据
2. 生成稳定币、包装币、原生币分类列表
3. 数据完整性检查和分析
4. 增量更新和强制更新模式
"""

import logging
import time
from pathlib import Path
from typing import List, Set, Dict, Optional

from ..classification.stablecoin_checker import StablecoinChecker
from ..classification.wrapped_coin_checker import WrappedCoinChecker
from ..downloaders.batch_downloader import create_batch_downloader

logger = logging.getLogger(__name__)


class MetadataUpdater:
    """元数据更新器 - 提供完整的元数据管理功能"""

    def __init__(self, project_root: Optional[Path] = None):
        """
        初始化元数据更新器

        Args:
            project_root: 项目根目录，如果为None则自动推导
        """
        if project_root is None:
            # 从模块位置推导项目根目录 (src/updaters -> project_root)
            self.project_root = Path(__file__).parent.parent.parent
        else:
            self.project_root = project_root

        self.coins_dir = self.project_root / "data" / "coins"
        self.metadata_dir = self.project_root / "data" / "metadata"

        # 初始化检查器和下载器
        self.stablecoin_checker = StablecoinChecker()
        self.wrapped_checker = WrappedCoinChecker()
        self.downloader = create_batch_downloader()

    def get_all_coin_ids_from_data(self) -> List[str]:
        """
        从 data/coins/ 目录获取所有币种 ID

        Returns:
            币种 ID 列表
        """
        coin_ids = []

        if not self.coins_dir.exists():
            logger.error(f"data/coins/ 目录不存在: {self.coins_dir}")
            return []

        # 扫描所有 CSV 文件
        for csv_file in self.coins_dir.glob("*.csv"):
            coin_id = csv_file.stem  # 去掉 .csv 后缀
            coin_ids.append(coin_id)

        # 按字母顺序排序
        coin_ids.sort()

        logger.info(f"📊 发现 {len(coin_ids)} 个币种文件")
        return coin_ids

    def get_existing_metadata_coin_ids(self) -> Set[str]:
        """
        获取已有元数据的币种 ID

        Returns:
            已有元数据的币种 ID 集合
        """
        metadata_coin_dir = self.metadata_dir / "coin_metadata"
        existing_ids = set()

        if metadata_coin_dir.exists():
            for json_file in metadata_coin_dir.glob("*.json"):
                coin_id = json_file.stem
                existing_ids.add(coin_id)

        return existing_ids

    def batch_update_all_metadata(
        self,
        batch_size: int = 50,
        delay_seconds: float = 0.5,
        force_update: bool = False,
    ) -> Dict[str, bool]:
        """
        批量更新所有币种的元数据

        Args:
            batch_size: 每批处理的币种数量
            delay_seconds: 每次API调用的延迟
            force_update: 是否强制更新

        Returns:
            更新结果字典 {coin_id: success}
        """
        logger.info("🚀 开始批量更新币种元数据")
        logger.info("=" * 60)

        # 1. 获取所有币种 ID
        all_coin_ids = self.get_all_coin_ids_from_data()
        if not all_coin_ids:
            logger.error("❌ 未找到任何币种数据")
            return {}

        # 2. 检查已有元数据
        existing_ids = self.get_existing_metadata_coin_ids()
        logger.info(f"📋 当前已有元数据: {len(existing_ids)} 个币种")

        # 3. 筛选需要更新的币种
        if force_update:
            coins_to_update = all_coin_ids
            logger.info(f"🔄 强制更新模式: 将更新所有 {len(coins_to_update)} 个币种")
        else:
            coins_to_update = [
                coin_id for coin_id in all_coin_ids if coin_id not in existing_ids
            ]
            logger.info(f"🆕 增量更新模式: 需要更新 {len(coins_to_update)} 个新币种")

        if not coins_to_update:
            logger.info("✅ 所有币种元数据都是最新的")
            return {}

        # 4. 分批处理
        total_batches = (len(coins_to_update) + batch_size - 1) // batch_size
        success_count = 0
        all_results = {}

        for batch_idx in range(total_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, len(coins_to_update))
            batch_coins = coins_to_update[start_idx:end_idx]

            logger.info(
                f"\n📦 处理第 {batch_idx + 1}/{total_batches} 批 ({len(batch_coins)} 个币种)"
            )
            logger.info(
                f"   币种: {', '.join(batch_coins[:5])}{'...' if len(batch_coins) > 5 else ''}"
            )

            # 批量更新这一批币种
            results = self.downloader.batch_update_coin_metadata(
                coin_ids=batch_coins, force=force_update, delay_seconds=delay_seconds
            )

            # 合并结果
            all_results.update(results)

            # 统计结果
            batch_success = sum(1 for success in results.values() if success)
            success_count += batch_success

            logger.info(f"   结果: {batch_success}/{len(batch_coins)} 成功")

            # 批次间延迟
            if batch_idx < total_batches - 1:
                logger.info(f"   等待 {delay_seconds * 2:.1f} 秒后继续...")
                time.sleep(delay_seconds * 2)

        logger.info(f"\n🎉 批量更新完成!")
        logger.info(f"   总计: {success_count}/{len(coins_to_update)} 成功")
        logger.info(f"   失败: {len(coins_to_update) - success_count} 个")

        return all_results

    def generate_complete_stablecoin_list(self) -> bool:
        """
        生成完整的稳定币列表

        Returns:
            是否成功生成
        """
        logger.info(f"\n💰 生成稳定币列表")
        logger.info("=" * 40)

        try:
            # 获取所有稳定币
            stablecoins = self.stablecoin_checker.get_all_stablecoins()

            if not stablecoins:
                logger.warning("❌ 未找到任何稳定币")
                return False

            logger.info(f"✅ 发现 {len(stablecoins)} 个稳定币:")

            # 按市值排名或名称排序显示
            for i, coin in enumerate(stablecoins, 1):
                symbol = coin["symbol"].upper()
                name = coin["name"]
                categories = coin["stablecoin_categories"]
                logger.info(f"  {i:2d}. {name} ({symbol})")
                logger.info(f"      分类: {', '.join(categories)}")

            # 导出到 CSV
            success = self.stablecoin_checker.export_stablecoins_csv()
            if success:
                logger.info(f"\n💾 稳定币列表已导出到: data/metadata/stablecoins.csv")

            # 额外分析
            logger.info(f"\n📊 稳定币分析:")

            # 按分类统计
            category_counts = {}
            for coin in stablecoins:
                for category in coin["stablecoin_categories"]:
                    category_counts[category] = category_counts.get(category, 0) + 1

            logger.info("   主要分类:")
            for category, count in sorted(
                category_counts.items(), key=lambda x: x[1], reverse=True
            ):
                logger.info(f"     - {category}: {count} 个")

            return success

        except Exception as e:
            logger.error(f"生成稳定币列表失败: {e}")
            return False

    def generate_complete_wrapped_coin_list(self) -> bool:
        """
        生成完整的包装币列表

        Returns:
            是否成功生成
        """
        logger.info(f"\n📦 生成包装币列表")
        logger.info("=" * 40)

        try:
            # 获取所有包装币
            wrapped_coins = self.wrapped_checker.get_all_wrapped_coins()

            if not wrapped_coins:
                logger.warning("❌ 未找到任何包装币")
                return False

            logger.info(f"✅ 发现 {len(wrapped_coins)} 个包装币:")

            # 按市值排名或名称排序显示
            for i, coin in enumerate(wrapped_coins, 1):
                symbol = coin["symbol"].upper()
                name = coin["name"]
                confidence = coin["confidence"]
                indicators = []
                if coin["wrapped_categories"]:
                    indicators.extend(coin["wrapped_categories"])
                if coin["name_indicators"]:
                    indicators.extend(
                        [f"名称:{ind}" for ind in coin["name_indicators"]]
                    )
                if coin["symbol_patterns"]:
                    indicators.extend(
                        [f"符号:{ind}" for ind in coin["symbol_patterns"]]
                    )

                logger.info(f"  {i:2d}. {name} ({symbol}) - 置信度: {confidence}")
                if indicators:
                    logger.info(f"      识别依据: {', '.join(indicators[:3])}")

            # 导出到 CSV
            success = self.wrapped_checker.export_wrapped_coins_csv()
            if success:
                logger.info(f"\n💾 包装币列表已导出到: data/metadata/wrapped_coins.csv")

            # 额外分析
            logger.info(f"\n📊 包装币分析:")

            # 按置信度统计
            confidence_counts = {}
            for coin in wrapped_coins:
                conf = coin["confidence"]
                confidence_counts[conf] = confidence_counts.get(conf, 0) + 1

            logger.info("   置信度分布:")
            for conf, count in sorted(
                confidence_counts.items(), key=lambda x: x[1], reverse=True
            ):
                logger.info(f"     - {conf}: {count} 个")

            # 按分类统计
            category_counts = {}
            for coin in wrapped_coins:
                for category in coin["wrapped_categories"]:
                    category_counts[category] = category_counts.get(category, 0) + 1

            if category_counts:
                logger.info("   主要分类:")
                for category, count in sorted(
                    category_counts.items(), key=lambda x: x[1], reverse=True
                ):
                    logger.info(f"     - {category}: {count} 个")

            return success

        except Exception as e:
            logger.error(f"生成包装币列表失败: {e}")
            return False

    def generate_complete_native_coin_list(self) -> bool:
        """
        生成完整的原生币列表（排除稳定币和包装币）

        该函数会：
        1. 获取所有币种列表
        2. 使用稳定币检查器识别稳定币
        3. 使用包装币检查器识别包装币
        4. 生成原生币列表并导出到CSV

        Returns:
            是否成功生成
        """
        logger.info(f"\n🔍 生成完整的原生币列表...")

        try:
            # 获取所有币种ID
            coin_ids = self.get_all_coin_ids_from_data()

            if not coin_ids:
                logger.error("❌ 没有找到任何币种数据")
                return False

            # 获取稳定币列表
            stablecoin_results = []
            for coin_id in coin_ids:
                result = self.stablecoin_checker.is_stablecoin(coin_id)
                if result["is_stablecoin"]:
                    stablecoin_results.append(coin_id)

            # 获取包装币列表
            wrapped_results = []
            for coin_id in coin_ids:
                result = self.wrapped_checker.is_wrapped_coin(coin_id)
                if result["is_wrapped_coin"]:
                    wrapped_results.append(coin_id)

            # 生成原生币列表（排除稳定币和包装币）
            excluded_coins = set(stablecoin_results + wrapped_results)
            native_coins = [
                coin_id for coin_id in coin_ids if coin_id not in excluded_coins
            ]

            logger.info(f"📊 原生币统计:")
            logger.info(f"   总币种数: {len(coin_ids)}")
            logger.info(f"   稳定币数: {len(stablecoin_results)}")
            logger.info(f"   包装币数: {len(wrapped_results)}")
            logger.info(f"   原生币数: {len(native_coins)}")

            # 导出到CSV
            import pandas as pd

            # 准备数据
            csv_data = []
            for coin_id in native_coins:
                metadata = self.downloader._load_coin_metadata(coin_id)
                if metadata:
                    csv_data.append(
                        {
                            "coin_id": coin_id,
                            "name": metadata.get("name", ""),
                            "symbol": metadata.get("symbol", ""),
                            "categories": ";".join(metadata.get("categories", [])),
                            "last_updated": metadata.get("last_updated", ""),
                        }
                    )

            # 创建DataFrame并保存
            df = pd.DataFrame(csv_data)
            df = df.sort_values("coin_id")

            output_path = self.metadata_dir / "native_coins.csv"
            df.to_csv(output_path, index=False, encoding="utf-8-sig")

            logger.info(f"\n💾 原生币列表已导出到: {output_path}")
            logger.info(f"   共导出 {len(csv_data)} 个原生币")

            return True

        except Exception as e:
            logger.error(f"❌ 导出原生币列表失败: {e}")
            return False

    def update_all_classification_lists(self) -> Dict[str, bool]:
        """
        更新所有分类列表

        Returns:
            各个列表的生成结果
        """
        results = {}

        logger.info("🔄 开始更新所有分类列表...")

        # 1. 生成稳定币列表
        results["stablecoins"] = self.generate_complete_stablecoin_list()

        # 2. 生成包装币列表
        results["wrapped_coins"] = self.generate_complete_wrapped_coin_list()

        # 3. 生成原生币列表
        results["native_coins"] = self.generate_complete_native_coin_list()

        # 汇总结果
        success_count = sum(1 for success in results.values() if success)
        total_count = len(results)

        logger.info(f"\n🎉 分类列表更新完成！")
        logger.info(f"   成功: {success_count}/{total_count}")
        logger.info(f"   失败: {total_count - success_count}")

        if success_count == total_count:
            logger.info("\n📁 生成的文件:")
            logger.info("   - data/metadata/coin_metadata/*.json  (单个币种元数据)")
            logger.info("   - data/metadata/stablecoins.csv       (稳定币汇总列表)")
            logger.info("   - data/metadata/wrapped_coins.csv     (包装币汇总列表)")
            logger.info("   - data/metadata/native_coins.csv      (原生币汇总列表)")

        return results


# 便捷函数，用于向后兼容
def batch_update_all_metadata(
    batch_size: int = 50, delay_seconds: float = 0.5, force_update: bool = False
) -> Dict[str, bool]:
    """
    便捷函数：批量更新所有元数据

    Args:
        batch_size: 每批处理的币种数量
        delay_seconds: 每次API调用的延迟
        force_update: 是否强制更新

    Returns:
        更新结果字典
    """
    updater = MetadataUpdater()
    return updater.batch_update_all_metadata(batch_size, delay_seconds, force_update)


def update_all_classification_lists() -> Dict[str, bool]:
    """
    便捷函数：更新所有分类列表

    Returns:
        各个列表的生成结果
    """
    updater = MetadataUpdater()
    return updater.update_all_classification_lists()
