"""极简 Notebook API (实验版)

目标：把常见 Notebook 操作压缩成**单行调用**，避免显式初始化样板。

设计原则：
1. 懒加载：首次调用时才初始化底层 quick_env()
2. 无状态界面：函数式 API，隐藏内部实例
3. 安全默认：返回清洗后的原生币数据（过滤稳定币/包装币）
4. 可控扩展：必要时允许 include_all / force 透传
5. 专注 80% 场景：按日期取数据、看前 N、市值权重、指数值/序列

核心函数：
    daily(date, clean=True, include_all=False)
    top(date, n=30, include_all=False, columns=None)
    weights(base_date, n=30, include_all=False)
    index_value(base_date, target_date, n=30, base_value=1000, include_all=False)
    index_series(base_date, dates, n=30, base_value=1000, include_all=False)
    show_top(date, n=30, include_all=False, columns=None, title=None)

最小使用示例：
    from src.lab.minapi import top, index_value
    top("2024-07-01", 20)                      # 查看前20市值
    iv = index_value("2023-12-01", "2024-07-01", n=15)  # 指数点位

依赖：内部复用 quick_env -> DailyDataAggregator + CryptoDataDisplayer

注意：这是 **实验接口**，命名和签名未来可能根据使用反馈微调；
      若需稳定依赖，请仍使用 quick_env 原接口。
"""
from __future__ import annotations

from typing import Iterable, List, Optional, Callable, Tuple, Dict, Any, TYPE_CHECKING
import math
import pandas as pd

from src.utils.notebook_env import quick_env
from src.utils.quiet_utils import silent_all

if TYPE_CHECKING:  # 仅类型检查时导入，避免运行时不必要依赖
    from pandas import DataFrame
    # 真实类路径：如果将来路径变动，这里更新即可
    from src.analysis.daily_aggregator import DailyDataAggregator  # type: ignore
    from src.utils.display_utils import CryptoDataDisplayer  # type: ignore

__all__ = [
    "daily", "top", "weights",
    "index_value", "index_series", "show_top", "prepare_index_base"
]

# 运行期缓存 (env -> (aggregator, displayer, get_clean_func))
_CACHE: Dict[str, Any] = {"env": None}


def _env() -> Tuple[Any, Any, Callable[[str], "DataFrame"]]:
    """惰性获取底层环境三元组。

    返回: (aggregator, displayer, get_clean_daily_data)
    """
    if _CACHE["env"] is None:
        _CACHE["env"] = quick_env()
    return _CACHE["env"]  # type: ignore[return-value]


def daily(date: str, clean: bool = True, include_all: bool = False) -> "DataFrame":
    """获取某日市场数据。

    Args:
        date: YYYY-MM-DD
        clean: True 返回清洗后；False 返回原始 (含未过滤列)
        include_all: True 不过滤稳定币/包装币
    Returns:
        DataFrame (可能为空)
    """
    aggregator, displayer, get_clean = _env()
    if clean:
        if not include_all:
            return get_clean(date)
        raw_all = aggregator.get_daily_data(date, result_include_all=True)
        return displayer.clean_data(raw_all)
    # raw 模式 (可能不是 DataFrame? 假设为 DataFrame)
    return aggregator.get_daily_data(date, result_include_all=include_all)  # type: ignore[return-value]


def top(date: str, n: int = 30, include_all: bool = False, columns: Optional[List[str]] = None) -> "DataFrame":
    """获取某日市值前 n 行（清洗后）。"""
    df = daily(date, clean=True, include_all=include_all)
    if getattr(df, "empty", True):
        return df
    if "market_cap" in getattr(df, "columns", []):
        return df.sort_values("market_cap", ascending=False).head(n)
    return df.head(n)


def weights(base_date: str, n: int = 30, include_all: bool = False) -> "DataFrame":
    """基准日选前 n 市值并计算权重 (返回含 weight 列, 0-1)。"""
    df = top(base_date, n=n, include_all=include_all)
    if getattr(df, "empty", True) or "market_cap" not in getattr(df, "columns", []):
        return df
    work = df.copy()
    total = work["market_cap"].sum()
    if total <= 0 or math.isclose(total, 0.0):
        return work
    work["weight"] = work["market_cap"] / total
    return work


def index_value(base_date: str, target_date: str, n: int = 30, base_value: float = 1000.0, include_all: bool = False) -> float:
    """计算单个目标日指数值 (基准日选前 n 市值成分，固定成分方式)。

    Returns:
        float 指数值 (若目标日无任何成分数据则返回 float('nan'))
    """
    base_w = weights(base_date, n=n, include_all=include_all)
    if getattr(base_w, "empty", True):
        return float("nan")
    # 成分集合
    symbols = set(base_w["symbol"].tolist()) if "symbol" in getattr(base_w, "columns", []) else set()
    if not symbols:
        return float("nan")
    agg, disp, get_clean = _env()
    with silent_all():
        tgt = agg.get_daily_data(target_date, result_include_all=include_all)
    if tgt.empty:
        return float("nan")
    tgt = disp.clean_data(tgt)
    # 过滤为基准成分
    if "symbol" in tgt.columns:
        tgt_const = tgt[tgt["symbol"].isin(symbols)].copy()
    else:
        return float("nan")
    if getattr(tgt_const, "empty", True) or "market_cap" not in getattr(tgt_const, "columns", []):
        return float("nan")
    base_total = base_w["market_cap"].sum()
    current_total = tgt_const["market_cap"].sum()
    if base_total <= 0:
        return float("nan")
    return base_value * (current_total / base_total)


def index_series(base_date: str, dates: Iterable[str], n: int = 30, base_value: float = 1000.0, include_all: bool = False) -> "DataFrame":
    """批量计算一组日期的指数值（固定基准成分）。

    Returns: DataFrame(columns=["date", "index_value"]) 排除无数据日期
    """
    rows = []
    for d in dates:
        val = index_value(base_date, d, n=n, base_value=base_value, include_all=include_all)
        if not (math.isnan(val) or val is None):
            rows.append({"date": d, "index_value": val})
    return pd.DataFrame(rows)


def show_top(date: str, n: int = 30, include_all: bool = False, columns: Optional[List[str]] = None, title: Optional[str] = None) -> None:
    """显示某日市值前 n 行（格式化输出）。"""
    agg, disp, _ = _env()
    df = top(date, n=n, include_all=include_all, columns=columns)
    if getattr(df, "empty", True):
        print(f"⚠️ {date} 无数据")
        return
    disp.show_table(df, columns=columns, top_n=len(df), title=title or f"{date} 前{n}市值", page_size=50)


def prepare_index_base(
    base_date: str,
    n: int = 30,
    base_value: float = 1000.0,
    market_cap_precision: int = 3,
    show: bool = True
):
    """准备指数基准：成分(前 n)、权重、基准信息、格式化展示.

    返回 (components_df, index_info_dict, formatted_df)
    components_df: 含 0-1 权重 (weight)
    formatted_df: 已格式化（市值按十亿美元、weight 转百分数数值字符串）
    index_info_dict: {name, base_date, base_value, base_market_cap, constituents_count}
    """
    import pandas as pd  # 局部导入防止加载时副作用
    comps = weights(base_date, n=n)
    if getattr(comps, "empty", True):
        raise ValueError("基准日无成分数据")
    base_total = comps["market_cap"].sum() if "market_cap" in comps.columns else 0.0
    info = {
        "name": f"Crypto{n} 市值加权指数",
        "base_date": base_date,
        "base_value": base_value,
        "base_market_cap": base_total,
        "constituents_count": len(comps)
    }
    # 获取 displayer
    agg, disp, _ = _env()
    # 展示副本：权重 -> 百分值
    disp_df = comps.copy()
    if "weight" in disp_df.columns:
        disp_df["weight"] = disp_df["weight"] * 100
    cols = [c for c in ["rank","symbol","name","price","market_cap","weight"] if c in disp_df.columns]
    formatted = disp.format_crypto_data(disp_df, columns=cols)
    # 覆写市值精度
    mc_col = "市值(1B$)"
    if mc_col in formatted.columns and "market_cap" in comps.columns:
        formatted[mc_col] = (comps["market_cap"] / 1_000_000_000).round(market_cap_precision).map(
            lambda x: f"{x:,.{market_cap_precision}f}"
        )
    # 权重两位小数
    w_col = "权重(%)"
    if w_col in formatted.columns:
        formatted[w_col] = formatted[w_col].apply(lambda v: v if v == "N/A" else f"{float(v):.2f}")
    if show:
        # formatted 已经过 format_crypto_data + 自定义覆写, 不能再交给 show_table (否则列名不匹配)
        try:
            from IPython.display import display as _ipd  # type: ignore
            print(f"\n📊 指数构成 (基准 {base_date})")
            _ipd(formatted)
        except Exception:
            print(formatted.to_string(index=False))
    return comps, info, formatted
