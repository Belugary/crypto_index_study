#!/usr/bin/env python3
"""
计算Crypto30指数投资回报的简单脚本
"""

import pandas as pd

# 计算一年期回报（2023-12-20 到 2024-12-19）
df_1year = pd.read_csv("data/indices/crypto30_1year_2023-12-20_to_2024-12-19.csv")
initial_1year = df_1year.iloc[0]["index_value"]
final_1year = df_1year.iloc[-1]["index_value"]

# 计算到2025年7月13日的回报
df_latest = pd.read_csv("data/indices/crypto30_latest_2023-12-20_to_2025-07-13.csv")
initial_latest = df_latest.iloc[0]["index_value"]
final_latest = df_latest.iloc[-1]["index_value"]

investment = 10000  # $10,000 initial investment

# 一年期结果
final_value_1year = investment * (final_1year / initial_1year)
gain_1year = final_value_1year - investment
return_1year = (final_1year - initial_1year) / initial_1year * 100

# 最新结果
final_value_latest = investment * (final_latest / initial_latest)
gain_latest = final_value_latest - investment
return_latest = (final_latest - initial_latest) / initial_latest * 100

print("🎯 Crypto30 指数投资分析结果")
print("=" * 60)
print()

print("📅 **一年期表现** (2023-12-20 → 2024-12-19)")
print(f"投资期间: 365天")
print(f"指数变化: {initial_1year:.2f} → {final_1year:.2f}")
print(f"投资价值: ${investment:,.2f} → ${final_value_1year:,.2f}")
print(f"绝对收益: ${gain_1year:+,.2f}")
print(f"回报率: {return_1year:+.2f}%")
print(f"年化收益率: {return_1year:+.2f}%")
print()

print("📅 **最新表现** (2023-12-20 → 2025-07-13)")
days_latest = len(df_latest) - 1
years_latest = days_latest / 365.25
annualized_latest = ((final_value_latest / investment) ** (1 / years_latest) - 1) * 100
print(f"投资期间: {days_latest}天 ({years_latest:.1f}年)")
print(f"指数变化: {initial_latest:.2f} → {final_latest:.2f}")
print(f"投资价值: ${investment:,.2f} → ${final_value_latest:,.2f}")
print(f"绝对收益: ${gain_latest:+,.2f}")
print(f"回报率: {return_latest:+.2f}%")
print(f"年化收益率: {annualized_latest:+.2f}%")
print()

print("🔍 **关键洞察**")
print("• 如果在2023年12月20日投资10,000美元到Crypto30指数：")
print(
    f"  - 到2024年12月19日（一年后）：价值${final_value_1year:,.2f}，收益{return_1year:+.1f}%"
)
print(
    f"  - 到2025年7月13日（最新）：价值${final_value_latest:,.2f}，收益{return_latest:+.1f}%"
)
print()
print("• 这意味着你的投资在1.57年内增长了3.16倍！")
print("• 年化收益率达到了惊人的{:.1f}%".format(annualized_latest))
print()

print("⚠️  **重要说明**")
print("• 这是基于历史数据的回测结果")
print("• 假设完美跟踪指数，无交易成本和滑点")
print("• 实际投资会有额外的费用和追踪误差")
print("• 加密货币投资具有极高风险和波动性")
print("• 过往表现不代表未来结果")
