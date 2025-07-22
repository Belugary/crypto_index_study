#!/usr/bin/env python3
"""
市场数据统一更新脚本

🎯 您更新量价数据的唯一工具！
支持交互式菜单选择和命令行参数两种使用方式。

交互式使用（推荐）：
    python scripts/update_market_data.py                          # 显示菜单，选择操作

命令行使用（高级用户）：
    python scripts/update_market_data.py --auto --all             # 自动更新所有数据
    python scripts/update_market_data.py --auto --price           # 只更新价格
         if not dry_run:
            # 3. 生成每日汇总
            print("\n📊 步骤 3/3: 生成每日汇总...")
            daily_success = rebuild_daily_data(config['verbose'])ython scripts/update_market_data.py --auto --new-coins       # 只检测新币种
    python scripts/update_market_data.py --auto --daily           # 只重建每日汇总
"""

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 配置日志 - 修复Windows控制台Unicode问题
import io
import sys

class SafeStreamHandler(logging.StreamHandler):
    """安全的流处理器，避免Unicode编码错误"""
    def emit(self, record):
        try:
            msg = self.format(record)
            # 移除emoji字符，避免Windows控制台编码问题
            safe_msg = msg.encode('ascii', 'ignore').decode('ascii')
            stream = self.stream
            stream.write(safe_msg + self.terminator)
            self.flush()
        except Exception:
            self.handleError(record)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/market_data_update.log", encoding='utf-8'),
        SafeStreamHandler(),
    ],
)


def update_price_data(coins: int = 510, max_range: int = 1000, verbose: bool = False) -> bool:
    """更新现有币种的价格数据"""
    try:
        from src.updaters.price_updater import PriceDataUpdater
        
        print(f"\n[价格更新] 开始更新价格数据...")
        print(f"   - 目标币种数: {coins}")
        print(f"   - 搜索范围: {max_range}")
        
        # 临时调整日志级别以减少输出干扰进度条
        original_level = logging.getLogger().level
        if not verbose:
            logging.getLogger().setLevel(logging.WARNING)
        
        try:
            updater = PriceDataUpdater()
            updater.update_with_smart_strategy(coins, max_range)
        finally:
            # 恢复原始日志级别
            logging.getLogger().setLevel(original_level)
        
        print("[价格更新] 价格数据更新完成")
        return True
        
    except Exception as e:
        print(f"[错误] 价格数据更新失败: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return False


def detect_new_coins(top_n: int = 1000, max_workers: int = 3, dry_run: bool = False, verbose: bool = False) -> Dict[str, Any]:
    """检测新币种并下载历史数据"""
    try:
        from src.updaters.incremental_daily_updater import create_incremental_updater
        
        print(f"\n[新币检测] 开始检测新币种...")
        print(f"   - 监控范围: 前 {top_n} 名")
        print(f"   - 并发数: {max_workers}")
        print(f"   - 试运行: {'是' if dry_run else '否'}")
        
        updater = create_incremental_updater(use_database=True)
        results = updater.update_with_new_coins(
            top_n=top_n, 
            max_workers=max_workers, 
            dry_run=dry_run
        )
        
        new_coins_count = len(results.get("new_coins", []))
        if dry_run:
            print(f"[新币检测] 试运行完成，发现 {new_coins_count} 个新币种")
        else:
            print(f"[新币检测] 新币种检测完成，处理了 {new_coins_count} 个新币种")
        
        return results
        
    except Exception as e:
        print(f"[错误] 新币种检测失败: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return {"summary": {"status": "error", "error": str(e)}}


def rebuild_daily_data(verbose: bool = False) -> bool:
    """重建每日汇总数据"""
    try:
        from src.downloaders.daily_aggregator import DailyDataAggregator
        
        print(f"\n[每日汇总] 开始生成最新的每日汇总数据...")
        
        aggregator = DailyDataAggregator(use_database=True)
        # 使用新方法生成最近3天的每日汇总数据
        results = aggregator.generate_latest_daily_summary(target_days=3)
        
        success_count = results.get("success_count", 0)
        total_count = results.get("total_count", 0)
        
        if success_count > 0:
            print(f"[每日汇总] 每日汇总数据生成完成: {success_count}/{total_count} 天成功")
            processed_dates = results.get("processed_dates", [])
            if processed_dates:
                print(f"[每日汇总] 已生成日期: {', '.join(processed_dates)}")
            return True
        else:
            print(f"[每日汇总] 每日汇总数据生成失败或没有可用数据")
            return False
        
    except Exception as e:
        print(f"[错误] 每日汇总数据生成失败: {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        return False


def show_menu():
    """显示交互式菜单"""
    print("[市场数据更新工具]")
    print("=" * 50)
    print("请选择您要执行的操作：")
    print()
    print("1.  更新最新价格数据 - 获取现有币种的最新价格和交易量")
    print("2.  检测新币种 - 发现排名中的新币种并下载历史数据")
    print("3.  生成每日汇总 - 从最新价格数据生成用于指数计算的每日汇总")
    print("4.  完整更新 - 执行上述所有操作（推荐）")
    print("5.  预览模式 - 查看将要执行的操作，不实际运行")
    print("0.  退出")
    print()


def get_user_choice():
    """获取用户选择"""
    while True:
        try:
            choice = input("请输入选项编号 (0-5): ").strip()
            if choice in ['0', '1', '2', '3', '4', '5']:
                return int(choice)
            else:
                print("❌ 请输入有效的选项编号 (0-5)")
        except KeyboardInterrupt:
            print("\n\n[退出] 再见！")
            sys.exit(0)
        except Exception:
            print("[错误] 请输入有效的选项编号 (0-5)")


def get_advanced_config():
    """获取高级配置参数"""
    print("\n[高级配置] （直接回车使用默认值）:")
    
    # 币种数量配置
    while True:
        try:
            coins_input = input(f"监控币种数量 (默认: 510): ").strip()
            coins = 510 if not coins_input else int(coins_input)
            if coins > 0:
                break
            else:
                print("[错误] 币种数量必须大于0")
        except ValueError:
            print("[错误] 请输入有效的数字")
    
    # 搜索范围配置
    while True:
        try:
            range_input = input(f"搜索范围 (默认: 1000): ").strip()
            search_range = 1000 if not range_input else int(range_input)
            if search_range >= coins:
                break
            else:
                print(f"[错误] 搜索范围必须不小于币种数量 ({coins})")
        except ValueError:
            print("[错误] 请输入有效的数字")
    
    # 并发数配置
    while True:
        try:
            workers_input = input(f"并发线程数 (默认: 3): ").strip()
            workers = 3 if not workers_input else int(workers_input)
            if 1 <= workers <= 10:
                break
            else:
                print("[错误] 并发线程数应在 1-10 之间")
        except ValueError:
            print("[错误] 请输入有效的数字")
    
    # 详细日志选择
    verbose_input = input("显示详细日志? (y/N): ").strip().lower()
    verbose = verbose_input in ['y', 'yes', '是']
    
    return {
        'coins': coins,
        'max_range': search_range,
        'max_workers': workers,
        'verbose': verbose
    }


def interactive_mode():
    """交互式模式主流程"""
    while True:
        show_menu()
        choice = get_user_choice()
        
        if choice == 0:
            print("👋 再见！")
            return 0
        
        # 获取高级配置
        print(f"\n🎯 您选择了：{['', '更新价格数据', '检测新币种', '生成每日汇总', '完整更新', '预览模式'][choice]}")
        
        config_input = input("\n是否需要自定义配置? (y/N): ").strip().lower()
        if config_input in ['y', 'yes', '是']:
            config = get_advanced_config()
        else:
            config = {
                'coins': 510,
                'max_range': 1000,
                'max_workers': 3,
                'verbose': False
            }
        
        # 预览模式特殊处理
        dry_run = (choice == 5)
        if dry_run:
            print(f"\n🔍 预览模式：将检查 前{config['max_range']}名 中的新币种")
            confirm = input("确认执行预览? (Y/n): ").strip().lower()
            if confirm in ['n', 'no', '否']:
                continue
        else:
            # 显示配置摘要
            print(f"\n� 配置摘要:")
            print(f"   - 监控币种数: {config['coins']}")
            print(f"   - 搜索范围: 前 {config['max_range']} 名")
            print(f"   - 并发线程: {config['max_workers']}")
            print(f"   - 详细日志: {'是' if config['verbose'] else '否'}")
            
            confirm = input("\n确认执行? (Y/n): ").strip().lower()
            if confirm in ['n', 'no', '否']:
                continue
        
        # 执行相应操作
        try:
            if choice == 1:
                # 更新价格数据
                success = update_price_data(config['coins'], config['max_range'], config['verbose'])
                result_message = "🎉 价格数据更新完成!" if success else "⚠️ 价格数据更新遇到问题"
                
            elif choice == 2:
                # 检测新币种
                results = detect_new_coins(config['max_range'], config['max_workers'], dry_run, config['verbose'])
                new_coins_count = len(results.get("new_coins", []))
                status = results.get("summary", {}).get("status", "unknown")
                if status in ["completed", "dry_run_complete", "no_new_coins"]:
                    result_message = f"� 新币种检测完成! (发现 {new_coins_count} 个新币种)" if new_coins_count > 0 else "😊 没有发现新币种"
                else:
                    result_message = "⚠️ 新币种检测遇到问题"
                    
            elif choice == 3:
                # 生成每日汇总
                success = rebuild_daily_data(config['verbose'])
                result_message = "🎉 每日汇总生成完成!" if success else "⚠️ 每日汇总生成遇到问题"
                
            elif choice == 4:
                # 完整更新
                return execute_full_update(config, dry_run)
                
            elif choice == 5:
                # 预览模式（只检测新币种）
                results = detect_new_coins(config['max_range'], config['max_workers'], True, config['verbose'])
                new_coins = results.get("new_coins", [])
                if new_coins:
                    result_message = f"🔍 预览完成! 发现 {len(new_coins)} 个新币种: {', '.join(new_coins[:3])}{'...' if len(new_coins) > 3 else ''}"
                else:
                    result_message = "🔍 预览完成! 没有发现新币种"
            
            print(f"\n{result_message}")
            
        except KeyboardInterrupt:
            print("\n⚠️ 操作被中断")
        except Exception as e:
            print(f"\n❌ 操作失败: {e}")
            if config['verbose']:
                import traceback
                traceback.print_exc()
        
        # 询问是否继续
        print()
        continue_input = input("是否继续使用? (Y/n): ").strip().lower()
        if continue_input in ['n', 'no', '否']:
            print("👋 再见！")
            return 0


def execute_full_update(config: dict, dry_run: bool = False) -> int:
    """执行完整更新流程"""
    print(f"\n🚀 开始完整的市场数据更新流程...")
    
    price_success = True
    new_coins_results = {"summary": {"status": "skipped"}}
    daily_success = True
    
    try:
        if not dry_run:
            # 1. 更新价格数据
            print("\n📍 步骤 1/3: 更新价格数据...")
            price_success = update_price_data(config['coins'], config['max_range'], config['verbose'])
            
        # 2. 检测新币种
        print(f"\n📍 步骤 {'2/2' if dry_run else '2/3'}: 检测新币种...")
        new_coins_results = detect_new_coins(config['max_range'], config['max_workers'], dry_run, config['verbose'])
        
        if not dry_run:
            # 3. 重建每日汇总
            print("\n� 步骤 3/3: 重建每日汇总...")
            daily_success = rebuild_daily_data(config['verbose'])
        
        # 显示完整结果摘要
        print_summary(price_success, new_coins_results, daily_success, dry_run)
        
        # 检查整体成功状态
        overall_success = True
        if not dry_run:
            overall_success &= price_success and daily_success
        
        new_coins_status = new_coins_results.get("summary", {}).get("status", "unknown")
        overall_success &= new_coins_status in ["completed", "dry_run_complete", "no_new_coins"]
        
        return 0 if overall_success else 1
        
    except KeyboardInterrupt:
        print("\n⚠️ 操作被中断")
        return 1
    except Exception as e:
        print(f"\n❌ 完整更新失败: {e}")
        if config['verbose']:
            import traceback
            traceback.print_exc()
        return 1


def print_summary(price_success: bool, new_coins_results: Dict[str, Any], daily_success: bool, dry_run: bool = False):
    """打印更新摘要"""
    print("\n" + "=" * 60)
    print(f"📊 {'预览' if dry_run else '市场数据更新'}完成报告")
    print("=" * 60)
    
    if not dry_run:
        # 价格数据更新结果
        print(f"💰 价格数据更新: {'✅ 成功' if price_success else '❌ 失败'}")
    
    # 新币种检测结果
    new_coins = new_coins_results.get("new_coins", [])
    status = new_coins_results.get("summary", {}).get("status", "unknown")
    
    if status == "no_new_coins":
        print(f"🆕 新币种检测: ✅ 完成（无新币种）")
    elif status in ["completed", "dry_run_complete"]:
        action = "预览发现" if dry_run else "处理了"
        print(f"🆕 新币种检测: ✅ 完成（{action} {len(new_coins)} 个新币种）")
        if new_coins:
            print(f"    新币种: {', '.join(new_coins[:5])}{'...' if len(new_coins) > 5 else ''}")
    else:
        print(f"🆕 新币种检测: ❌ 失败")
    
    if not dry_run:
        # 每日汇总生成结果
        print(f"📊 每日汇总生成: {'✅ 成功' if daily_success else '❌ 失败'}")
    
    print("=" * 60)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description="市场数据统一更新工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚀 交互式使用（推荐）:
  %(prog)s                          # 显示菜单，选择操作

🤖 自动化使用:
  %(prog)s --auto --all             # 自动执行完整更新
  %(prog)s --auto --price           # 只更新价格
  %(prog)s --auto --new-coins       # 只检测新币种
  %(prog)s --auto --daily           # 只重建每日汇总
        """
    )
    
    # 自动化模式
    parser.add_argument("--auto", action="store_true", help="自动化模式，跳过交互")
    
    # 操作选择（仅自动化模式有效）
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--all", action="store_true", help="执行完整更新")
    mode_group.add_argument("--price", action="store_true", help="只更新价格数据")
    mode_group.add_argument("--new-coins", action="store_true", help="只检测新币种")
    mode_group.add_argument("--daily", action="store_true", help="只生成每日汇总")
    
    # 配置参数
    parser.add_argument("--coins", type=int, default=510, help="目标币种数量 (默认: 510)")
    parser.add_argument("--max-range", type=int, default=1000, help="最大搜索范围 (默认: 1000)")
    parser.add_argument("--max-workers", type=int, default=3, help="并发工作线程数 (默认: 3)")
    parser.add_argument("--dry-run", action="store_true", help="试运行模式：预览操作，不实际执行")
    parser.add_argument("--verbose", "-v", action="store_true", help="显示详细日志")
    
    args = parser.parse_args()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 检查是否为交互式模式
    if not args.auto:
        return interactive_mode()
    
    # 自动化模式
    if not any([args.all, args.price, args.new_coins, args.daily]):
        # 默认执行完整更新
        args.all = True
    
    config = {
        'coins': args.coins,
        'max_range': args.max_range,
        'max_workers': args.max_workers,
        'verbose': args.verbose
    }
    
    print("🤖 自动化模式")
    print("=" * 30)
    
    try:
        if args.price:
            print("� 执行价格数据更新...")
            success = update_price_data(config['coins'], config['max_range'], config['verbose'])
            return 0 if success else 1
            
        elif args.new_coins:
            print("🆕 执行新币种检测...")
            results = detect_new_coins(config['max_range'], config['max_workers'], args.dry_run, config['verbose'])
            status = results.get("summary", {}).get("status", "unknown")
            return 0 if status in ["completed", "dry_run_complete", "no_new_coins"] else 1
            
        elif args.daily:
            print("📊 执行每日汇总生成...")
            success = rebuild_daily_data(config['verbose'])
            return 0 if success else 1
            
        elif args.all:
            print("🚀 执行完整更新...")
            return execute_full_update(config, args.dry_run)
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断操作")
        return 1
    except Exception as e:
        print(f"\n❌ 脚本执行失败: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
