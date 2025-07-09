"""
CoinGecko API 封装模块

提供对 CoinGecko Pro API 的完整封装，支持数字货币的各种数据查询功能。
"""

import requests
import os
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()


class CoinGeckoAPI:
    """CoinGecko API 封装类，支持 Pro API Key - 基础功能"""

    def __init__(self, api_key: Optional[str] = None):
        """
        初始化 CoinGecko API 客户端

        Args:
            api_key: CoinGecko Pro API Key，如果不提供则从环境变量获取
        """
        self.api_key = api_key or os.getenv("COINGECKO_API_KEY")
        self.base_url = "https://pro-api.coingecko.com/api/v3"
        self.session = requests.Session()

        if self.api_key:
            self.session.headers.update(
                {"x-cg-pro-api-key": self.api_key, "accept": "application/json"}
            )
        else:
            print("警告: 未找到 API Key，将使用免费接口（有限制）")
            self.base_url = "https://api.coingecko.com/api/v3"

    def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Any:
        """
        发送 API 请求的通用方法

        Args:
            endpoint: API 端点
            params: 请求参数

        Returns:
            API 响应数据
        """
        url = f"{self.base_url}/{endpoint}"

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API 请求失败: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(f"状态码: {e.response.status_code}")
                print(f"响应内容: {e.response.text}")
            raise

    # ===== 🔹 基础 API =====
    def get_coins_list(self, include_platform: bool = False) -> List[Dict[str, Any]]:
        """
        获取所有支持的硬币列表（ID 映射）

        Args:
            include_platform: 是否包含平台信息

        Returns:
            硬币列表，包含 id, symbol, name 等信息
        """
        endpoint = "coins/list"
        params = {"include_platform": str(include_platform).lower()}

        print("正在获取硬币列表...")
        return self._make_request(endpoint, params)

    def get_coins_markets(
        self,
        vs_currency: str = "usd",
        ids: Optional[str] = None,
        per_page: int = 100,
        page: int = 1,
        order: str = "market_cap_desc",
        sparkline: bool = False,
        price_change_percentage: Optional[str] = None,
        locale: str = "en",
        precision: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取带市场数据的硬币列表

        Args:
            vs_currency: 对比货币 (usd, eur, cny 等)
            ids: 硬币ID列表，用逗号分隔
            order: 排序方式
            per_page: 每页数量 (1-250)
            page: 页码
            sparkline: 是否包含sparkline
            price_change_percentage: 价格变化百分比时间范围
            locale: 语言
            precision: 精度

        Returns:
            带市场数据的硬币列表
        """
        endpoint = "coins/markets"
        params = {
            "vs_currency": vs_currency,
            "order": order,
            "per_page": per_page,
            "page": page,
            "sparkline": str(sparkline).lower(),
            "locale": locale,
        }

        if ids:
            params["ids"] = ids
        if price_change_percentage:
            params["price_change_percentage"] = price_change_percentage
        if precision:
            params["precision"] = precision

        print(f"正在获取市场数据 (第{page}页)...")
        return self._make_request(endpoint, params)

    def get_coin_by_id(
        self,
        coin_id: str,
        localization: bool = True,
        tickers: bool = True,
        market_data: bool = True,
        community_data: bool = True,
        developer_data: bool = True,
        sparkline: bool = False,
    ) -> Dict[str, Any]:
        """
        根据ID获取硬币详细数据

        Args:
            coin_id: 硬币ID (如 'bitcoin', 'ethereum')
            localization: 是否包含本地化数据
            tickers: 是否包含交易行情数据
            market_data: 是否包含市场数据
            community_data: 是否包含社区数据
            developer_data: 是否包含开发者数据
            sparkline: 是否包含价格走势图数据

        Returns:
            硬币详细数据
        """
        endpoint = f"coins/{coin_id}"
        params = {
            "localization": str(localization).lower(),
            "tickers": str(tickers).lower(),
            "market_data": str(market_data).lower(),
            "community_data": str(community_data).lower(),
            "developer_data": str(developer_data).lower(),
            "sparkline": str(sparkline).lower(),
        }

        print(f"正在获取 {coin_id} 的详细数据...")
        return self._make_request(endpoint, params)

    def get_coin_tickers(
        self,
        coin_id: str,
        exchange_ids: Optional[str] = None,
        include_exchange_logo: bool = False,
        page: int = 1,
        order: str = "trust_score_desc",
        depth: bool = False,
    ) -> Dict[str, Any]:
        """
        根据ID获取硬币的交易行情数据

        Args:
            coin_id: 硬币ID
            exchange_ids: 交易所ID列表，用逗号分隔
            include_exchange_logo: 是否包含交易所logo
            page: 页码
            order: 排序方式 (trust_score_desc, trust_score_asc, volume_desc)
            depth: 是否包含2%深度数据

        Returns:
            交易行情数据
        """
        endpoint = f"coins/{coin_id}/tickers"
        params = {
            "include_exchange_logo": str(include_exchange_logo).lower(),
            "page": page,
            "order": order,
            "depth": str(depth).lower(),
        }

        if exchange_ids:
            params["exchange_ids"] = exchange_ids

        print(f"正在获取 {coin_id} 的交易行情数据...")
        return self._make_request(endpoint, params)

    def get_coin_history(
        self, coin_id: str, date: str, localization: bool = True
    ) -> Dict[str, Any]:
        """
        获取硬币在特定日期的历史数据

        Args:
            coin_id: 硬币ID
            date: 日期，格式为 dd-mm-yyyy (如 '30-12-2017')
            localization: 是否包含本地化数据

        Returns:
            历史数据
        """
        endpoint = f"coins/{coin_id}/history"
        params = {"date": date, "localization": str(localization).lower()}

        print(f"正在获取 {coin_id} 在 {date} 的历史数据...")
        return self._make_request(endpoint, params)

    def get_coin_market_chart(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        days: int = 1,
        interval: Optional[str] = None,
        precision: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取硬币的历史图表数据

        Args:
            coin_id: 硬币ID
            vs_currency: 对比货币
            days: 天数 (1, 7, 14, 30, 90, 180, 365, max)
            interval: 数据间隔 (5m, 1h, 1d) - 自动根据天数选择
            precision: 小数位精度 (0-18)

        Returns:
            包含价格、市值、交易量的历史图表数据
        """
        endpoint = f"coins/{coin_id}/market_chart"
        params = {"vs_currency": vs_currency, "days": str(days)}

        if interval:
            params["interval"] = interval
        if precision:
            params["precision"] = precision

        print(f"正在获取 {coin_id} 的历史图表数据 ({days}天)...")
        return self._make_request(endpoint, params)

    def get_coin_market_chart_range(
        self,
        coin_id: str,
        from_timestamp: int,
        to_timestamp: int,
        vs_currency: str = "usd",
        precision: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取硬币在指定时间范围内的历史图表数据

        Args:
            coin_id: 硬币ID
            vs_currency: 对比货币
            from_timestamp: 开始时间戳 (Unix时间戳)
            to_timestamp: 结束时间戳 (Unix时间戳)
            precision: 小数位精度

        Returns:
            指定时间范围的历史图表数据
        """
        endpoint = f"coins/{coin_id}/market_chart/range"
        params = {
            "vs_currency": vs_currency,
            "from": from_timestamp,
            "to": to_timestamp,
        }

        if precision:
            params["precision"] = precision

        print(f"正在获取 {coin_id} 在指定时间范围的历史图表数据...")
        return self._make_request(endpoint, params)

    def get_coin_ohlc(
        self,
        coin_id: str,
        vs_currency: str = "usd",
        days: int = 1,
        precision: Optional[str] = None,
    ) -> List[List[float]]:
        """
        获取硬币的OHLC图表数据

        Args:
            coin_id: 硬币ID
            vs_currency: 对比货币
            days: 天数 (1, 7, 14, 30, 90, 180, 365)
            precision: 小数位精度

        Returns:
            OHLC数据 [[timestamp, open, high, low, close], ...]
        """
        endpoint = f"coins/{coin_id}/ohlc"
        params = {"vs_currency": vs_currency, "days": str(days)}

        if precision:
            params["precision"] = precision

        print(f"正在获取 {coin_id} 的OHLC数据 ({days}天)...")
        return self._make_request(endpoint, params)


def create_api_client(api_key: Optional[str] = None) -> CoinGeckoAPI:
    """
    创建 CoinGecko API 客户端的便捷函数

    Args:
        api_key: 可选的API密钥

    Returns:
        CoinGeckoAPI实例
    """
    return CoinGeckoAPI(api_key)
