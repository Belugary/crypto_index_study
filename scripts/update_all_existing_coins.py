#!/usr/bin/env python3
"""
智能更新所有现有币种数据

该脚本使用更新日志来高效地更新 `data/coins` 目录中的所有币种数据文件。
它会跳过今天已经更新过的币种，并为整个过程提供进度条。

使用方式:
    python scripts/update_all_existing_coins.py
"""

import argparse
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
from tqdm import tqdm

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater


def _find_project_root() -> Path:
    """查找项目根目录"""
    current = Path(__file__).parent.parent
    while current != current.parent:
        if (current / ".git").exists() or (
            (current / "src").exists() and (current / "requirements.txt").exists()
        ):
            return current
        current = current.parent
    return Path.cwd()


# --- 配置 ---
PROJECT_ROOT = _find_project_root()
LOG_FILE = PROJECT_ROOT / "logs/update_all_existing_coins.log"
COINS_DIR = PROJECT_ROOT / "data/coins"
METADATA_DIR = PROJECT_ROOT / "data/metadata"
UPDATE_LOG_PATH = METADATA_DIR / "update_log.csv"

# --- 日志配置 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class UpdateLogger:
    """
    管理更新日志 (update_log.csv)
    """

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_df = self._load_or_create_log()

    def _load_or_create_log(self) -> pd.DataFrame:
        """加载或创建更新日志"""
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.log_path.exists():
            logger.info(f"从 {self.log_path} 加载更新日志")
            return pd.read_csv(self.log_path)
        else:
            logger.info(f"创建新的更新日志: {self.log_path}")
            df = pd.DataFrame(columns=["coin_id", "last_updated"])
            df.to_csv(self.log_path, index=False)
            return df

    def get_last_update_date(self, coin_id: str) -> Optional[date]:
        """获取币种的最后更新日期"""
        record = self.log_df[self.log_df["coin_id"] == coin_id]
        if not record.empty:
            try:
                return datetime.strptime(
                    record.iloc[0]["last_updated"], "%Y-%m-%d"
                ).date()
            except (ValueError, TypeError):
                return None
        return None

    def log_update(self, coin_id: str):
        """记录币种的更新时间"""
        today_str = date.today().strftime("%Y-%m-%d")
        if coin_id in self.log_df["coin_id"].values:
            self.log_df.loc[self.log_df["coin_id"] == coin_id, "last_updated"] = (
                today_str
            )
        else:
            new_record = pd.DataFrame([{"coin_id": coin_id, "last_updated": today_str}])
            self.log_df = pd.concat([self.log_df, new_record], ignore_index=True)

    def save_log(self):
        """保存更新日志"""
        self.log_df.to_csv(self.log_path, index=False)
        logger.info(f"更新日志已保存到 {self.log_path}")


def get_coins_to_update(
    all_coins: List[str], update_logger: UpdateLogger
) -> Tuple[List[str], List[str]]:
    """
    根据更新日志筛选需要更新的币种
    """
    today = date.today()
    needs_update = []
    already_updated = []

    for coin_id in tqdm(all_coins, desc="检查更新状态"):
        last_update = update_logger.get_last_update_date(coin_id)
        if last_update != today:
            needs_update.append(coin_id)
        else:
            already_updated.append(coin_id)

    return needs_update, already_updated


def run_update(
    updater: PriceDataUpdater,
    coins_to_update: List[str],
    update_logger: UpdateLogger,
    max_workers: int,
) -> Tuple[int, int]:
    """
    使用并行处理更新所有需要更新的币种
    """
    success_count = 0
    fail_count = 0

    with tqdm(total=len(coins_to_update), desc="更新币种数据") as pbar:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_coin = {
                executor.submit(updater.download_coin_data, coin_id): coin_id
                for coin_id in coins_to_update
            }

            for future in as_completed(future_to_coin):
                coin_id = future_to_coin[future]
                try:
                    success, _ = future.result()
                    if success:
                        success_count += 1
                        update_logger.log_update(coin_id)
                    else:
                        fail_count += 1
                except Exception as e:
                    logger.error(f"更新 {coin_id} 时发生异常: {e}")
                    fail_count += 1
                pbar.update(1)

    return success_count, fail_count


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="智能更新所有现有币种数据")
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="并行下载的最大工作线程数 (默认: 10)",
    )
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="强制更新所有币种，忽略更新日志",
    )
    args = parser.parse_args()

    print("🔄 智能更新所有现有币种工具")
    print("=" * 50)

    if not COINS_DIR.exists():
        logger.error(f"❌ 币种数据目录不存在: {COINS_DIR}")
        sys.exit(1)

    try:
        # 1. 初始化
        all_coin_ids = sorted([f.stem for f in COINS_DIR.glob("*.csv")])
        update_logger = UpdateLogger(UPDATE_LOG_PATH)
        updater = PriceDataUpdater()

        print(f"发现 {len(all_coin_ids)} 个现有币种")

        # 2. 筛选需要更新的币种
        if args.force_all:
            print("⚡️ 强制模式：将更新所有币种")
            needs_update = all_coin_ids
            already_updated = []
        else:
            needs_update, already_updated = get_coins_to_update(
                all_coin_ids, update_logger
            )

        print(f"\n📈 统计信息:")
        print(f"   - 总币种数: {len(all_coin_ids)}")
        print(f"   - 今日已更新 (跳过): {len(already_updated)}")
        print(f"   - 需要更新: {len(needs_update)}")

        if not needs_update:
            print("\n✅ 所有币种都是今日最新数据，无需更新！")
            return

        # 3. 执行更新
        print(
            f"\n🚀 开始更新 {len(needs_update)} 个币种 (并行数: {args.max_workers})..."
        )
        success_count, fail_count = run_update(
            updater, needs_update, update_logger, args.max_workers
        )

        # 4. 保存日志并报告结果
        update_logger.save_log()

        print(f"\n🎯 更新完成！")
        print(f"📊 最终统计:")
        print(f"   - 成功更新: {success_count}")
        print(f"   - 更新失败: {fail_count}")

        if fail_count > 0:
            print(f"\n⚠️  有 {fail_count} 个币种更新失败，请检查日志: {LOG_FILE}")
        else:
            print(f"\n✅ 所有需要更新的币种都已成功处理！")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断操作")
        # 确保在中断时也能保存已完成的日志
        if "update_logger" in locals():
            update_logger.save_log()
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ 脚本执行失败: {e}", exc_info=True)
        if "update_logger" in locals():
            update_logger.save_log()
        sys.exit(1)


if __name__ == "__main__":
    main()
