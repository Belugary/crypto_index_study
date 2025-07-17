"""
统一分类器

将稳定币和包装币检查整合到一个高效的分类器中，
减少重复的元数据加载，提高性能。
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.downloaders.batch_downloader import create_batch_downloader


@dataclass
class ClassificationResult:
    """分类结果数据类"""

    coin_id: str
    name: Optional[str] = None
    symbol: Optional[str] = None
    is_stablecoin: bool = False
    is_wrapped_coin: bool = False
    confidence: str = "unknown"
    stablecoin_categories: Optional[List[str]] = None
    wrapped_categories: Optional[List[str]] = None
    all_categories: Optional[List[str]] = None
    last_updated: Optional[str] = None

    def __post_init__(self):
        if self.stablecoin_categories is None:
            self.stablecoin_categories = []
        if self.wrapped_categories is None:
            self.wrapped_categories = []
        if self.all_categories is None:
            self.all_categories = []


class UnifiedClassifier:
    """统一币种分类器

    整合稳定币和包装币检查功能，提供高效的批量分类能力
    """

    # 分类关键词定义
    STABLECOIN_KEYWORDS = {"Stablecoins"}

    WRAPPED_COIN_KEYWORDS = {
        "Wrapped-Tokens",
        "Liquid Staking Tokens",
        "Liquid Staked ETH",
        "Liquid Staking",
        "Tokenized BTC",
        "Crypto-Backed Tokens",
    }

    def __init__(self, data_dir: str = "data"):
        """初始化分类器

        Args:
            data_dir: 数据目录路径
        """
        # 查找项目根目录
        self.project_root = self._find_project_root()
        
        # 如果是相对路径，基于项目根目录解析
        if Path(data_dir).is_absolute():
            self.data_dir = Path(data_dir)
        else:
            self.data_dir = self.project_root / data_dir
            
        self.metadata_dir = self.data_dir / "metadata" / "coin_metadata"
        self.downloader = create_batch_downloader(data_dir=str(self.data_dir))
        self._cache: Dict[str, ClassificationResult] = {}

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

    def classify_coin(
        self, coin_id: str, use_cache: bool = True
    ) -> ClassificationResult:
        """分类单个币种

        Args:
            coin_id: 币种ID
            use_cache: 是否使用缓存

        Returns:
            分类结果
        """
        # 检查缓存
        if use_cache and coin_id in self._cache:
            return self._cache[coin_id]

        # 加载元数据
        metadata = self.downloader._load_coin_metadata(coin_id)

        if not metadata:
            result = ClassificationResult(coin_id=coin_id, confidence="unknown")
            if use_cache:
                self._cache[coin_id] = result
            return result

        # 分析分类
        categories = metadata.get("categories", [])
        categories_set = set(categories)

        # 检查稳定币
        stablecoin_categories = []
        for category in categories:
            if category in self.STABLECOIN_KEYWORDS:
                stablecoin_categories.append(category)

        # 检查包装币
        wrapped_categories = []
        for category in categories:
            if category in self.WRAPPED_COIN_KEYWORDS:
                wrapped_categories.append(category)

        # 创建结果
        result = ClassificationResult(
            coin_id=coin_id,
            name=metadata.get("name"),
            symbol=metadata.get("symbol"),
            is_stablecoin=len(stablecoin_categories) > 0,
            is_wrapped_coin=len(wrapped_categories) > 0,
            confidence="high",
            stablecoin_categories=stablecoin_categories,
            wrapped_categories=wrapped_categories,
            all_categories=categories,
            last_updated=metadata.get("last_updated"),
        )

        # 缓存结果
        if use_cache:
            self._cache[coin_id] = result

        return result

    def is_native_coin(self, coin_id: str, use_cache: bool = True) -> bool:
        """判断一个币种是否为原生币（非稳定币且非包装币）

        Args:
            coin_id: 币种ID
            use_cache: 是否使用缓存

        Returns:
            如果是原生币则返回True，否则返回False
        """
        result = self.classify_coin(coin_id, use_cache)
        return not result.is_stablecoin and not result.is_wrapped_coin

    def classify_coins_batch(
        self, coin_ids: List[str], use_cache: bool = True
    ) -> Dict[str, ClassificationResult]:
        """批量分类币种

        Args:
            coin_ids: 币种ID列表
            use_cache: 是否使用缓存

        Returns:
            分类结果字典
        """
        from tqdm import tqdm

        results = {}

        # 显示进度条当处理超过10个币种时
        if len(coin_ids) > 10:
            iterator = tqdm(coin_ids, desc="分类币种", unit="个")
        else:
            iterator = coin_ids

        for coin_id in iterator:
            results[coin_id] = self.classify_coin(coin_id, use_cache)

        return results

    def filter_coins(
        self,
        coin_ids: List[str],
        exclude_stablecoins: bool = False,
        exclude_wrapped_coins: bool = False,
        use_cache: bool = True,
    ) -> List[str]:
        """过滤币种列表

        Args:
            coin_ids: 币种ID列表
            exclude_stablecoins: 是否排除稳定币
            exclude_wrapped_coins: 是否排除包装币
            use_cache: 是否使用缓存

        Returns:
            过滤后的币种ID列表
        """
        if not exclude_stablecoins and not exclude_wrapped_coins:
            return coin_ids.copy()

        filtered_coins = []
        results = self.classify_coins_batch(coin_ids, use_cache)

        for coin_id in coin_ids:
            result = results[coin_id]

            # 跳过无元数据的币种（保守处理）
            if result.confidence == "unknown":
                filtered_coins.append(coin_id)
                continue

            # 应用过滤规则
            if exclude_stablecoins and result.is_stablecoin:
                continue
            if exclude_wrapped_coins and result.is_wrapped_coin:
                continue

            filtered_coins.append(coin_id)

        return filtered_coins

    def get_classification_summary(self, coin_ids: List[str]) -> Dict[str, int]:
        """获取分类汇总统计

        Args:
            coin_ids: 币种ID列表

        Returns:
            分类统计字典
        """
        results = self.classify_coins_batch(coin_ids)

        summary = {
            "total": len(coin_ids),
            "stablecoins": 0,
            "wrapped_coins": 0,
            "native_coins": 0,
            "unknown": 0,
            "both_stable_and_wrapped": 0,
        }

        for result in results.values():
            if result.confidence == "unknown":
                summary["unknown"] += 1
            elif result.is_stablecoin and result.is_wrapped_coin:
                summary["both_stable_and_wrapped"] += 1
            elif result.is_stablecoin:
                summary["stablecoins"] += 1
            elif result.is_wrapped_coin:
                summary["wrapped_coins"] += 1
            else:
                summary["native_coins"] += 1

        return summary

    def clear_cache(self):
        """清空缓存"""
        self._cache.clear()

    def export_classification_csv(
        self,
        coin_ids: List[str],
        output_path: str = "data/metadata/classification_results.csv",
    ) -> bool:
        """导出分类结果到CSV

        Args:
            coin_ids: 币种ID列表
            output_path: 输出文件路径

        Returns:
            是否成功
        """
        try:
            import pandas as pd

            results = self.classify_coins_batch(coin_ids)

            # 转换为DataFrame格式
            df_data = []
            for coin_id, result in results.items():
                df_data.append(
                    {
                        "coin_id": result.coin_id,
                        "name": result.name,
                        "symbol": result.symbol,
                        "is_stablecoin": result.is_stablecoin,
                        "is_wrapped_coin": result.is_wrapped_coin,
                        "confidence": result.confidence,
                        "stablecoin_categories": ";".join(
                            result.stablecoin_categories or []
                        ),
                        "wrapped_categories": ";".join(result.wrapped_categories or []),
                        "all_categories": ";".join(result.all_categories or []),
                        "last_updated": result.last_updated,
                    }
                )

            df = pd.DataFrame(df_data)

            # 确保输出目录存在
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # 保存CSV
            df.to_csv(output_file, index=False, encoding="utf-8-sig")

            print(f"✅ 分类结果已导出到: {output_path}")
            print(f"   共导出 {len(df_data)} 个币种的分类结果")

            return True

        except Exception as e:
            print(f"❌ 导出分类结果失败: {e}")
            return False


def main():
    """演示统一分类器功能"""
    print("🔍 统一币种分类器")
    print("=" * 50)

    classifier = UnifiedClassifier()

    # 测试币种
    test_coins = [
        "bitcoin",
        "ethereum",
        "tether",
        "usd-coin",
        "dai",
        "wrapped-bitcoin",
        "weth",
        "staked-ether",
        "solana",
        "binancecoin",  # 修复: bnb -> binancecoin
    ]

    print("📊 单个币种分类测试:")
    for coin_id in test_coins:
        result = classifier.classify_coin(coin_id)
        print(f"  {result.name or coin_id}:")
        print(f"    稳定币: {'✅' if result.is_stablecoin else '❌'}")
        print(f"    包装币: {'✅' if result.is_wrapped_coin else '❌'}")
        if result.stablecoin_categories:
            print(f"    稳定币分类: {', '.join(result.stablecoin_categories)}")
        if result.wrapped_categories:
            print(f"    包装币分类: {', '.join(result.wrapped_categories)}")
        print()

    print("📈 批量分类统计:")
    summary = classifier.get_classification_summary(test_coins)
    for key, value in summary.items():
        print(f"  {key}: {value}")

    print(f"\n🔧 过滤测试:")
    original_count = len(test_coins)
    filtered_no_stable = classifier.filter_coins(test_coins, exclude_stablecoins=True)
    filtered_no_wrapped = classifier.filter_coins(
        test_coins, exclude_wrapped_coins=True
    )
    filtered_native_only = classifier.filter_coins(
        test_coins, exclude_stablecoins=True, exclude_wrapped_coins=True
    )

    print(f"  原始币种: {original_count}")
    print(f"  排除稳定币: {len(filtered_no_stable)}")
    print(f"  排除包装币: {len(filtered_no_wrapped)}")
    print(f"  仅原生币种: {len(filtered_native_only)}")

    print(f"\n💾 导出测试:")
    classifier.export_classification_csv(test_coins)

    print(f"\n{'='*50}")
    print("✅ 统一分类器测试完成！")


if __name__ == "__main__":
    main()
