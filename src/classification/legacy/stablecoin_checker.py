"""
稳定币查询工具

基于存储的币种元数据快速查询稳定币信息
"""

import os
import sys
from pathlib import Path
from typing import Any, Dict, List

# 添加项目根目录到Python路径
sys.path.insert(
    0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
)

from src.downloaders.batch_downloader import create_batch_downloader


class StablecoinChecker:
    """稳定币检查器 - 基于本地元数据"""

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

    def is_stablecoin(self, coin_id: str) -> Dict[str, Any]:
        """
        检查指定币种是否为稳定币

        Args:
            coin_id: 币种ID

        Returns:
            检查结果字典
        """
        metadata = self.downloader._load_coin_metadata(coin_id)

        if not metadata:
            return {
                "coin_id": coin_id,
                "is_stablecoin": False,
                "confidence": "unknown",
                "reason": "no_metadata",
                "categories": [],
            }

        categories = metadata.get("categories", [])
        stablecoin_categories = []

        # 严格按照 CoinGecko 的官方分类判断
        # 只有明确包含 "Stablecoins" 分类的才算稳定币
        stablecoin_keywords = ["Stablecoins"]

        for category in categories:
            if category in stablecoin_keywords:
                stablecoin_categories.append(category)

        is_stablecoin = len(stablecoin_categories) > 0

        return {
            "coin_id": coin_id,
            "name": metadata.get("name"),
            "symbol": metadata.get("symbol"),
            "is_stablecoin": is_stablecoin,
            "confidence": "high" if is_stablecoin else "high",
            "stablecoin_categories": stablecoin_categories,
            "all_categories": categories,
            "last_updated": metadata.get("last_updated"),
        }

    def get_all_stablecoins(self) -> List[Dict[str, Any]]:
        """
        获取所有稳定币列表

        Returns:
            稳定币列表
        """
        stablecoins = []

        if not self.metadata_dir.exists():
            return stablecoins

        # 遍历所有元数据文件
        for metadata_file in self.metadata_dir.glob("*.json"):
            coin_id = metadata_file.stem
            result = self.is_stablecoin(coin_id)

            if result["is_stablecoin"]:
                stablecoins.append(result)

        # 按名称排序
        stablecoins.sort(key=lambda x: x.get("name", ""))
        return stablecoins

    def export_stablecoins_csv(
        self, output_path: str = "data/metadata/stablecoins.csv"
    ) -> bool:
        """
        导出稳定币列表到CSV文件

        Args:
            output_path: 输出文件路径

        Returns:
            是否成功
        """
        try:
            import pandas as pd

            stablecoins = self.get_all_stablecoins()

            if not stablecoins:
                print("❌ 未找到任何稳定币")
                return False

            # 转换为DataFrame
            df_data = []
            for coin in stablecoins:
                df_data.append(
                    {
                        "coin_id": coin["coin_id"],
                        "name": coin["name"],
                        "symbol": coin["symbol"],
                        "is_stablecoin": coin["is_stablecoin"],
                        "stablecoin_categories": ";".join(
                            coin["stablecoin_categories"]
                        ),
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

            print(f"✅ 稳定币列表已导出到: {output_path}")
            print(f"   共导出 {len(stablecoins)} 个稳定币")

            return True

        except Exception as e:
            print(f"❌ 导出稳定币列表失败: {e}")
            return False


def main():
    """主函数"""
    print("🔍 稳定币查询工具")
    print("=" * 40)

    checker = StablecoinChecker()

    # 1. 检查几个特定币种
    test_coins = ["bitcoin", "ethereum", "tether", "usd-coin", "dai"]

    print("📊 检查特定币种:")
    for coin_id in test_coins:
        result = checker.is_stablecoin(coin_id)
        if result["confidence"] != "unknown":
            status = "✅ 稳定币" if result["is_stablecoin"] else "❌ 非稳定币"
            print(f"  {result['name']} ({coin_id}): {status}")
            if result["stablecoin_categories"]:
                print(f"    稳定币分类: {', '.join(result['stablecoin_categories'])}")
        else:
            print(f"  {coin_id}: ❓ 无元数据")

    # 2. 获取所有稳定币
    print(f"\n📋 所有稳定币列表:")
    stablecoins = checker.get_all_stablecoins()

    if stablecoins:
        for coin in stablecoins:
            print(f"  ✅ {coin['name']} ({coin['symbol'].upper()})")
            print(f"     分类: {', '.join(coin['stablecoin_categories'])}")
    else:
        print("  暂无稳定币数据")

    # 3. 导出CSV
    print(f"\n💾 导出稳定币列表:")
    checker.export_stablecoins_csv()

    print(f"\n{'='*40}")
    print("✅ 稳定币查询完成！")


if __name__ == "__main__":
    main()
