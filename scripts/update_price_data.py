#!/usr/bin/env python3
"""
价格数据更新脚本

该脚本调用核心模块来执行智能的价格数据更新策略。
这是一个自动化脚本，用于定期执行价格数据更新任务。

使用方式:
    python scripts/update_price_data.py                     # 智能更新510个原生币
    python scripts/update_price_data.py --native-coins 700  # 智能更新700个原生币
    python scripts/update_price_data.py --max-range 1500    # 设置最大搜索范围
"""

import argparse
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/price_data_update.log"),
        logging.StreamHandler(),
    ],
)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="智能量价数据更新工具")
    parser.add_argument(
        "--native-coins", type=int, default=510, help="目标原生币种数量 (默认: 510)"
    )
    parser.add_argument(
        "--max-range", type=int, default=1000, help="最大搜索范围 (默认: 1000)"
    )

    args = parser.parse_args()

    print("🔍 智能量价数据更新工具")
    print("=" * 50)
    print(f"📊 配置信息:")
    print(f"   - 目标原生币种数: {args.native_coins}")
    print(f"   - 最大搜索范围: {args.max_range}")
    print()

    try:
        # 创建更新器并执行更新
        updater = PriceDataUpdater()
        updater.update_with_smart_strategy(args.native_coins, args.max_range)
        
        print("\n✅ 价格数据更新完成!")
        
    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 脚本执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
