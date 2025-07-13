#!/usr/bin/env python3
"""
快速维护脚本 - 常用场景的一键执行

这是 daily_maintenance.py 的简化版本，针对最常见的使用场景：
- 每日例行维护（500个币种，智能检测）
- 快速数据同步（跳过价格更新，只重建daily数据）
- 完整更新（包含价格和daily数据）

使用方式:
    python scripts/quick_maintenance.py                # 每日例行维护
    python scripts/quick_maintenance.py --sync-only    # 只同步daily数据
    python scripts/quick_maintenance.py --full         # 完整更新
"""

import sys
import subprocess
from pathlib import Path


def run_daily_maintenance(args):
    """调用主维护脚本"""
    script_path = Path(__file__).parent / "daily_maintenance.py"
    cmd = [sys.executable, str(script_path)] + args
    return subprocess.run(cmd).returncode


def main():
    if len(sys.argv) > 1:
        arg = sys.argv[1]

        if arg == "--sync-only":
            print("🔄 快速同步模式 - 只重建daily数据")
            exit_code = run_daily_maintenance(
                ["--auto", "--coins", "500", "--skip-price"]
            )

        elif arg == "--full":
            print("🚀 完整更新模式 - 价格数据 + daily数据")
            exit_code = run_daily_maintenance(["--auto", "--coins", "500"])

        elif arg == "--help" or arg == "-h":
            print("快速维护脚本使用方法:")
            print(
                "  python scripts/quick_maintenance.py           # 每日例行维护(500币种)"
            )
            print(
                "  python scripts/quick_maintenance.py --sync-only    # 只同步daily数据"
            )
            print("  python scripts/quick_maintenance.py --full         # 完整更新")
            print("  python scripts/quick_maintenance.py --help         # 显示帮助")
            print()
            print("💡 提示: 如需自定义配置，请使用 daily_maintenance.py")
            exit_code = 0
        else:
            print(f"❌ 未知参数: {arg}")
            print("使用 --help 查看可用选项")
            exit_code = 1
    else:
        print("📅 每日例行维护 - 自动检测500个币种")
        exit_code = run_daily_maintenance(["--auto", "--coins", "500"])

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
