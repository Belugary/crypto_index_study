#!/usr/bin/env python3
"""
每日维护一键脚本 - 自动化数据更新和维护流程

这个脚本整合了完整的每日数据维护工作流：
1. 询问用户配置参数
2. 检测并更新指定数量的原生币种价格数据
3. 检测每日汇总数据的缺失天数
4. 自动重建缺失的每日数据
5. 生成完整的维护报告

设计哲学：
- 一键执行，减少重复劳动
- 智能检测，只做必要的更新
- 友好交互，清晰的进度反馈
- 容错处理，异常情况优雅降级

使用方式:
    python scripts/daily_maintenance.py                    # 交互式运行
    python scripts/daily_maintenance.py --auto --coins 500 # 自动化运行
    python scripts/daily_maintenance.py --help             # 查看所有选项
"""

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Tuple, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.updaters.price_updater import PriceDataUpdater
from src.updaters.incremental_daily_updater import create_incremental_updater

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/daily_maintenance.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class DailyMaintenanceManager:
    """每日维护管理器 - 统一管理所有维护任务"""

    def __init__(self):
        self.daily_files_dir = Path("data/daily/daily_files")
        self.coins_dir = Path("data/coins")
        self.today = date.today()

    def print_banner(self):
        """显示脚本横幅"""
        print("🔧 每日维护一键脚本")
        print("=" * 60)
        print("📅 今日维护日期:", self.today.strftime("%Y年%m月%d日"))
        print("🎯 维护内容: 价格数据更新 + 每日汇总重建")
        print("⚡ 设计理念: 一键执行，智能检测，友好反馈")
        print("=" * 60)
        print()

    def get_user_config(
        self, auto_mode: bool = False, default_coins: int = 500
    ) -> dict:
        """获取用户配置参数"""
        if auto_mode:
            return {
                "target_coins": default_coins,
                "max_range": default_coins + 200,
                "skip_price_update": False,
                "max_workers": 6,
            }

        print("📋 请配置维护参数:")
        print()

        # 获取目标原生币种数量
        while True:
            try:
                target_str = input(
                    f"🎯 需要确保多少个原生币种数据最新? [默认: 500]: "
                ).strip()
                target_coins = int(target_str) if target_str else 500
                if target_coins > 0:
                    break
                print("❌ 请输入大于0的数字")
            except ValueError:
                print("❌ 请输入有效的数字")

        # 计算搜索范围（自动增加缓冲）
        max_range = target_coins + 200

        # 询问是否跳过价格更新
        skip_price = (
            input("⏭️  是否跳过价格数据更新? (如果最近已更新) [y/N]: ").strip().lower()
            == "y"
        )

        # 设置并发数
        workers_str = input("🚀 并发下载线程数 [默认: 6]: ").strip()
        max_workers = int(workers_str) if workers_str else 6

        config = {
            "target_coins": target_coins,
            "max_range": max_range,
            "skip_price_update": skip_price,
            "max_workers": max_workers,
        }

        print()
        print("✅ 配置确认:")
        for key, value in config.items():
            print(f"   - {key}: {value}")
        print()

        return config

    def update_price_data(self, config: dict) -> bool:
        """更新价格数据"""
        if config["skip_price_update"]:
            print("⏭️  跳过价格数据更新 (用户选择)")
            return True

        print("📈 开始价格数据更新...")
        print(f"🎯 目标: 确保 {config['target_coins']} 个原生币种数据最新")
        print(f"🔍 搜索范围: 市值前 {config['max_range']} 名")
        print(f"🚀 并发线程: {config['max_workers']}")
        print()

        try:
            # 方式1: 使用智能更新现有币种（推荐，速度快）
            print("🔧 策略: 智能更新现有币种数据")
            from scripts.update_all_existing_coins import main as update_existing_main

            # 临时修改 sys.argv 来传递参数
            original_argv = sys.argv[:]
            sys.argv = [
                "update_all_existing_coins.py",
                "--max-workers",
                str(config["max_workers"]),
            ]

            update_existing_main()

            # 恢复原始 argv
            sys.argv = original_argv

            print("✅ 价格数据更新完成")
            return True

        except Exception as e:
            logger.error(f"价格数据更新失败: {e}")
            print(f"❌ 价格数据更新失败: {e}")

            # 询问是否继续
            if input("是否继续执行每日数据重建? [Y/n]: ").strip().lower() != "n":
                return True
            return False

    def detect_missing_daily_data(self, lookback_days: int = 7) -> List[date]:
        """检测缺失的每日数据文件"""
        print("🔍 检测每日汇总数据完整性...")

        missing_dates = []

        # 检查最近N天的数据
        for i in range(lookback_days):
            check_date = self.today - timedelta(days=i)
            file_path = self._get_daily_file_path(check_date)

            if not file_path.exists():
                missing_dates.append(check_date)
                print(f"❌ 缺失: {check_date.strftime('%Y-%m-%d')}")
            else:
                # 检查文件大小（小于10KB可能不完整）
                file_size = file_path.stat().st_size
                if file_size < 10 * 1024:  # 10KB
                    missing_dates.append(check_date)
                    print(
                        f"⚠️  不完整: {check_date.strftime('%Y-%m-%d')} ({file_size} bytes)"
                    )
                else:
                    print(
                        f"✅ 完整: {check_date.strftime('%Y-%m-%d')} ({file_size // 1024}KB)"
                    )

        if missing_dates:
            print(f"📋 发现 {len(missing_dates)} 天数据需要重建")
        else:
            print("🎉 最近数据完整，无需重建")

        return missing_dates

    def rebuild_daily_data(self, missing_dates: List[date]) -> bool:
        """重建缺失的每日数据"""
        if not missing_dates:
            return True

        print("🔨 开始重建每日汇总数据...")

        # 计算日期范围
        start_date = min(missing_dates).strftime("%Y-%m-%d")
        end_date = max(missing_dates).strftime("%Y-%m-%d")

        print(f"📅 重建范围: {start_date} 到 {end_date}")
        print(f"📊 涉及天数: {len(missing_dates)}")
        print()

        try:
            # 使用 rebuild_daily_files 脚本
            from scripts.rebuild_daily_files import (
                rebuild_date_range,
                create_daily_aggregator,
            )

            aggregator = create_daily_aggregator()
            rebuild_date_range(aggregator, start_date, end_date)

            print("✅ 每日数据重建完成")
            return True

        except Exception as e:
            logger.error(f"每日数据重建失败: {e}")
            print(f"❌ 每日数据重建失败: {e}")
            return False

    def _get_daily_file_path(self, target_date: date) -> Path:
        """获取指定日期的每日数据文件路径"""
        return (
            self.daily_files_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.strftime('%Y-%m-%d')}.csv"
        )

    def generate_maintenance_report(
        self, config: dict, missing_dates: List[date], success: bool
    ):
        """生成维护报告"""
        print()
        print("📊 维护报告")
        print("=" * 40)
        print(f"🕐 维护时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🎯 目标币种: {config['target_coins']} 个原生币种")
        print(f"🔍 搜索范围: 前 {config['max_range']} 名")
        print(f"📈 价格更新: {'跳过' if config['skip_price_update'] else '已执行'}")
        print(f"📊 数据重建: {len(missing_dates)} 天")
        print(f"✅ 执行状态: {'成功' if success else '部分失败'}")

        if missing_dates:
            print(
                f"📅 重建日期: {', '.join(d.strftime('%m-%d') for d in missing_dates)}"
            )

        # 检查当前数据状况
        today_file = self._get_daily_file_path(self.today)
        if today_file.exists():
            size_kb = today_file.stat().st_size // 1024
            print(f"📁 今日数据: {size_kb}KB")

        print("=" * 40)

        if success:
            print("🎉 每日维护完成！数据已是最新状态。")
        else:
            print("⚠️  维护过程中遇到问题，请检查日志。")

    def run_maintenance(self, config: dict) -> bool:
        """执行完整的维护流程"""
        try:
            # 1. 更新价格数据
            price_success = self.update_price_data(config)
            if not price_success:
                return False

            # 2. 检测缺失的每日数据
            missing_dates = self.detect_missing_daily_data()

            # 3. 重建每日数据
            daily_success = self.rebuild_daily_data(missing_dates)

            # 4. 生成报告
            success = price_success and daily_success
            self.generate_maintenance_report(config, missing_dates, success)

            return success

        except KeyboardInterrupt:
            print("\n⚠️  用户中断维护流程")
            return False
        except Exception as e:
            logger.error(f"维护流程异常: {e}")
            print(f"❌ 维护流程异常: {e}")
            return False


def main():
    """主函数 - 命令行接口"""
    parser = argparse.ArgumentParser(
        description="每日维护一键脚本 - 自动化数据更新工作流",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  python scripts/daily_maintenance.py                     # 交互式配置
  python scripts/daily_maintenance.py --auto --coins 500  # 自动模式
  python scripts/daily_maintenance.py --coins 300 --workers 4  # 指定参数
        """,
    )

    parser.add_argument(
        "--auto", action="store_true", help="自动模式，使用默认配置无需交互"
    )
    parser.add_argument(
        "--coins", type=int, default=500, help="目标原生币种数量 (默认: 500)"
    )
    parser.add_argument(
        "--workers", type=int, default=6, help="并发下载线程数 (默认: 6)"
    )
    parser.add_argument("--skip-price", action="store_true", help="跳过价格数据更新")

    args = parser.parse_args()

    # 创建维护管理器
    manager = DailyMaintenanceManager()
    manager.print_banner()

    # 获取配置
    if args.auto:
        config = {
            "target_coins": args.coins,
            "max_range": args.coins + 200,
            "skip_price_update": args.skip_price,
            "max_workers": args.workers,
        }
        print("🤖 自动模式启动")
        print(
            f"📊 配置: {args.coins}个币种, {args.workers}线程, 跳过价格更新: {args.skip_price}"
        )
        print()
    else:
        config = manager.get_user_config()

    # 执行维护
    success = manager.run_maintenance(config)

    # 退出状态
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
