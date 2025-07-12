#!/usr/bin/env python3
"""
元数据更新脚本

该脚本调用核心模块来执行批量元数据更新和分类列表生成。
这是一个自动化脚本，用于定期执行元数据更新任务。

使用方式:
    python scripts/update_all_metadata.py          # 标准模式
    python scripts/update_all_metadata.py --fast   # 快速模式
    python scripts/update_all_metadata.py --force  # 强制更新所有
"""

import argparse
import logging
import os
import sys

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.metadata_updater import MetadataUpdater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="批量元数据更新工具")
    parser.add_argument(
        "--force", action="store_true", help="强制更新所有币种元数据"
    )
    parser.add_argument(
        "--fast", action="store_true", help="快速模式（减少延迟时间）"
    )

    args = parser.parse_args()

    print("🔍 批量币种元数据更新与分类分析")
    print("=" * 70)

    if args.force:
        print("⚠️  强制更新模式: 将重新获取所有币种的元数据")

    if args.fast:
        print("⚡ 快速模式: 减少延迟时间")
        delay_seconds = 0.2
        batch_size = 100
    else:
        print("🐌 标准模式: 使用安全的延迟时间")
        delay_seconds = 0.5
        batch_size = 50

    try:
        # 创建更新器
        updater = MetadataUpdater()

        # 1. 批量更新元数据
        print("\n🚀 开始批量更新元数据...")
        metadata_results = updater.batch_update_all_metadata(
            batch_size=batch_size,
            delay_seconds=delay_seconds,
            force_update=args.force,
        )

        # 2. 更新所有分类列表
        print("\n🔄 开始更新分类列表...")
        classification_results = updater.update_all_classification_lists()

        # 汇总结果
        metadata_success = len([r for r in metadata_results.values() if r])
        classification_success = len([r for r in classification_results.values() if r])
        
        print(f"\n{'='*70}")
        print("✅ 所有任务完成!")
        print(f"\n📊 执行结果:")
        print(f"   - 元数据更新: {metadata_success}/{len(metadata_results)} 成功")
        print(f"   - 分类列表: {classification_success}/{len(classification_results)} 成功")
        
        if classification_success == len(classification_results):
            print("\n📁 生成的文件:")
            print("   - data/metadata/coin_metadata/*.json  (单个币种元数据)")
            print("   - data/metadata/stablecoins.csv       (稳定币汇总列表)")
            print("   - data/metadata/wrapped_coins.csv     (包装币汇总列表)")
            print("   - data/metadata/native_coins.csv      (原生币汇总列表)")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 执行过程中出现错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    print("使用方法:")
    print("  python scripts/update_all_metadata.py          # 标准模式")
    print("  python scripts/update_all_metadata.py --fast   # 快速模式")
    print("  python scripts/update_all_metadata.py --force  # 强制更新所有")
    print("")

    main()
