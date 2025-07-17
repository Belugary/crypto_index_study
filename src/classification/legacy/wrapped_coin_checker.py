"""
严格的包装币查询工具

基于 CoinGecko 官方分类标签进行精确识别，避免误判
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.downloaders.batch_downloader import create_batch_downloader


class WrappedCoinChecker:
    """包装币检查器 - 基于 CoinGecko 官方分类标签进行精确识别"""

    def __init__(self, data_dir: str = "data"):
        self.project_root = self._find_project_root()
        # 解析数据目录路径
        if Path(data_dir).is_absolute():
            base_data_dir = Path(data_dir)
        else:
            base_data_dir = self.project_root / data_dir
        
        self.metadata_dir = base_data_dir / "metadata" / "coin_metadata"
        self.downloader = create_batch_downloader(data_dir=str(base_data_dir))

    @staticmethod
    def _find_project_root() -> Path:
        """查找项目根目录"""
        current = Path(__file__).parent.parent.parent.parent
        while current != current.parent:
            if (current / ".git").exists() or (
                (current / "src").exists() and (current / "requirements.txt").exists()
            ):
                return current
            current = current.parent
        return Path.cwd()

    def is_wrapped_coin(self, coin_id: str) -> Dict[str, Any]:
        """
        检查指定币种是否为包装币

        Args:
            coin_id: 币种ID

        Returns:
            检查结果字典
        """
        metadata = self.downloader._load_coin_metadata(coin_id)

        if not metadata:
            return {
                "coin_id": coin_id,
                "is_wrapped_coin": False,
                "confidence": "unknown",
                "reason": "no_metadata",
                "categories": [],
            }

        categories = metadata.get("categories", [])
        wrapped_categories = []

        # 严格按照 CoinGecko 的官方分类判断
        # 包装币和衍生品分类（包括液体质押代币）
        wrapped_category_keywords = [
            "Wrapped-Tokens",  # 包装代币
            "Liquid Staking Tokens",  # 液体质押代币
            "Liquid Staked ETH",  # 液体质押ETH
            "Liquid Staking",  # 液体质押
            "Tokenized BTC",  # 代币化BTC
            "Crypto-Backed Tokens",  # 加密货币支持的代币
        ]

        # 检查分类中是否包含包装币关键词
        for category in categories:
            if category in wrapped_category_keywords:
                wrapped_categories.append(category)

        is_wrapped = len(wrapped_categories) > 0

        return {
            "coin_id": coin_id,
            "name": metadata.get("name"),
            "symbol": metadata.get("symbol"),
            "is_wrapped_coin": is_wrapped,
            "confidence": "high" if is_wrapped else "high",
            "wrapped_categories": wrapped_categories,
            "all_categories": categories,
            "last_updated": metadata.get("last_updated"),
            "note": "包括包装代币、液体质押代币等衍生品" if is_wrapped else "原生资产",
        }

    def get_all_wrapped_coins(self) -> List[Dict[str, Any]]:
        """
        获取所有包装币列表

        Returns:
            包装币列表
        """
        wrapped_coins = []

        if not self.metadata_dir.exists():
            return wrapped_coins

        # 遍历所有元数据文件
        for metadata_file in self.metadata_dir.glob("*.json"):
            coin_id = metadata_file.stem
            result = self.is_wrapped_coin(coin_id)

            if result["is_wrapped_coin"]:
                wrapped_coins.append(result)

        # 按名称排序
        wrapped_coins.sort(key=lambda x: x.get("name", ""))
        return wrapped_coins

    def export_wrapped_coins_csv(
        self, output_path: str = "data/metadata/wrapped_coins.csv"
    ) -> bool:
        """
        导出包装币列表到CSV文件

        Args:
            output_path: 输出文件路径

        Returns:
            是否成功
        """
        try:
            import pandas as pd

            wrapped_coins = self.get_all_wrapped_coins()

            if not wrapped_coins:
                print("❌ 未找到任何包装币")
                return False

            # 转换为DataFrame
            df_data = []
            for coin in wrapped_coins:
                df_data.append(
                    {
                        "coin_id": coin["coin_id"],
                        "name": coin["name"],
                        "symbol": coin["symbol"],
                        "is_wrapped_coin": coin["is_wrapped_coin"],
                        "confidence": coin["confidence"],
                        "wrapped_categories": ";".join(coin["wrapped_categories"]),
                        "all_categories": ";".join(coin["all_categories"]),
                        "last_updated": coin["last_updated"],
                    }
                )

            df = pd.DataFrame(df_data)

            # 确保输出目录存在
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)

            # 保存CSV
            df.to_csv(output_file, index=False, encoding="utf-8-sig")

            print(f"✅ 包装币列表已导出到: {output_path}")
            print(f"   共导出 {len(wrapped_coins)} 个包装币")

            return True

        except Exception as e:
            print(f"❌ 导出包装币列表失败: {e}")
            return False


def main():
    """主函数"""
    print("🔍 严格包装币查询工具")
    print("=" * 40)

    checker = WrappedCoinChecker()

    # 1. 检查几个特定币种
    test_coins = [
        "bitcoin",
        "ethereum",
        "wrapped-bitcoin",
        "weth",
        "staked-ether",
        "binance-wrapped-btc",
        "avalanche-bridged-usdc-avalanche",
        "tether",
        "solana",
        "bnb",
        "cardano",
        "arbitrum",
        "avalanche-2",
    ]

    print("📊 检查特定币种:")
    for coin_id in test_coins:
        result = checker.is_wrapped_coin(coin_id)
        if result["confidence"] != "unknown":
            status = "✅ 包装币" if result["is_wrapped_coin"] else "❌ 非包装币"
            print(f"  {result['name']} ({coin_id}): {status}")
            if result["wrapped_categories"]:
                print(f"    包装币分类: {', '.join(result['wrapped_categories'])}")
        else:
            print(f"  {coin_id}: ❓ 无元数据")

    # 2. 获取所有包装币
    print(f"\n📋 所有包装币列表:")
    wrapped_coins = checker.get_all_wrapped_coins()

    if wrapped_coins:
        print(f"✅ 发现 {len(wrapped_coins)} 个包装币:")
        for coin in wrapped_coins[:20]:  # 只显示前20个
            print(f"  ✅ {coin['name']} ({coin['symbol'].upper()})")
            if coin["wrapped_categories"]:
                print(f"     分类: {', '.join(coin['wrapped_categories'])}")

        if len(wrapped_coins) > 20:
            print(f"  ... 还有 {len(wrapped_coins) - 20} 个包装币")
    else:
        print("  暂无包装币数据")

    # 3. 导出CSV
    print(f"\n💾 导出包装币列表:")
    checker.export_wrapped_coins_csv()

    print(f"\n{'='*40}")
    print("✅ 严格包装币查询完成！")


if __name__ == "__main__":
    main()
