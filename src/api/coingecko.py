"""
CoinGecko API 封装模块

提供对 CoinGecko Pro API 的完整封装，支持数字货币的各种数据查询功能。
"""

import os
from typing import Any, Dict, List, Optional

import requests
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

        官方文档: https://docs.coingecko.com/reference/coins-list

        Args:
            include_platform (bool, optional): 是否包含平台信息，默认为 False。
                如果为 True，将包含每个币种在不同区块链平台上的地址信息。

        Returns:
            List[Dict[str, Any]]: 硬币列表，每个硬币包含以下字段：

            **基础字段（总是包含）：**
                - id (str): 硬币的唯一标识符（如 'bitcoin', 'ethereum'）
                - symbol (str): 硬币符号（如 'btc', 'eth'）
                - name (str): 硬币全名（如 'Bitcoin', 'Ethereum'）

            **平台信息（仅当 include_platform=True 时）：**
                - platforms (Dict[str, str]): 各区块链平台上的地址信息
                    - ethereum (str): 在以太坊网络上的合约地址
                    - binance-smart-chain (str): 在BSC网络上的合约地址
                    - polygon-pos (str): 在Polygon网络上的合约地址
                    - （其他支持的平台名称和对应地址）

            **示例数据：**
                [
                    {
                        "id": "bitcoin",
                        "symbol": "btc",
                        "name": "Bitcoin"
                    },
                    {
                        "id": "ethereum",
                        "symbol": "eth",
                        "name": "Ethereum"
                    }
                ]

        Note:
            - 此端点不需要分页
            - 缓存/更新频率：Pro API（Analyst、Lite、Pro、Enterprise）每 5 分钟一次
            - 可以使用此端点查询包含 coin ID 的硬币列表，供其他包含 `id` 或 `ids` 参数的端点使用
            - 默认返回当前在 CoinGecko.com 上列出的活跃硬币的完整列表

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-markets

        Args:
            vs_currency (str, optional): 对比货币代码，默认为 'usd'。
                支持的货币包括：usd, eur, jpy, btc, eth, ltc, bch, bnb, eos, xrp, xlm 等。
            ids (str, optional): 指定硬币ID列表，用逗号分隔。
                如果不指定，则返回按市值排序的硬币列表。
            per_page (int, optional): 每页返回的硬币数量，范围 1-250，默认为 100。
            page (int, optional): 页码，从 1 开始，默认为 1。
            order (str, optional): 排序方式，默认为 'market_cap_desc'。
                可选值：'market_cap_desc', 'gecko_desc', 'gecko_asc', 'market_cap_asc',
                'market_cap_desc', 'volume_asc', 'volume_desc', 'id_asc', 'id_desc'。
            sparkline (bool, optional): 是否包含价格走势图数据，默认为 False。
            price_change_percentage (str, optional): 价格变化百分比的时间范围。
                可选值：'1h', '24h', '7d', '14d', '30d', '200d', '1y'，用逗号分隔多个值。
            locale (str, optional): 语言代码，默认为 'en'。
            precision (str, optional): 价格精度，可选值：'full' 或具体的小数位数。

        Returns:
            List[Dict[str, Any]]: 硬币市场数据列表，每个硬币包含以下字段：

            **基础信息：**
                - id (str): 硬币ID
                - symbol (str): 硬币符号（如 'btc', 'eth'）
                - name (str): 硬币名称（如 'Bitcoin', 'Ethereum'）
                - image (str): 硬币图标URL

            **价格相关：**
                - current_price (float): 当前价格
                - high_24h (float): 24小时最高价
                - low_24h (float): 24小时最低价
                - price_change_24h (float): 24小时价格变化（绝对值）
                - price_change_percentage_24h (float): 24小时价格变化百分比
                - price_change_percentage_*h (float, optional): 指定时间范围的价格变化百分比

            **市值和排名：**
                - market_cap (int): 市值
                - market_cap_rank (int): 市值排名
                - market_cap_change_24h (int): 24小时市值变化（绝对值）
                - market_cap_change_percentage_24h (float): 24小时市值变化百分比
                - fully_diluted_valuation (int): 完全稀释估值

            **交易量和供应量：**
                - total_volume (int): 24小时交易量
                - circulating_supply (float): 流通供应量
                - total_supply (float): 总供应量
                - max_supply (float): 最大供应量

            **历史价格记录：**
                - ath (float): 历史最高价
                - ath_change_percentage (float): 距离历史最高价的变化百分比
                - ath_date (str): 历史最高价日期（ISO 8601格式）
                - atl (float): 历史最低价
                - atl_change_percentage (float): 距离历史最低价的变化百分比
                - atl_date (str): 历史最低价日期（ISO 8601格式）

            **其他字段：**
                - last_updated (str): 最后更新时间（ISO 8601格式）
                - sparkline_in_7d (Dict, optional): 7天价格走势数据（仅当 sparkline=True 时）
                    - price (List[float]): 价格数据点列表

        Note:
            - 当提供多个查找参数时，优先级顺序为：`category`（最高）> `ids` > `names` > `symbols`（最低）
            - 按 `name` 搜索时，需要对空格进行 URL 编码（如 "Binance Coin" 变为 "Binance%20Coin"）
            - `include_tokens=all` 参数仅适用于 `symbols` 查找，每次请求最多限制 50 个符号
            - 查找参数（`ids`、`names`、`symbols`）不支持通配符搜索
            - 缓存/更新频率：Pro API（Analyst、Lite、Pro、Enterprise）每 45 秒一次
            - 可以使用 `per_page` 和 `page` 参数管理结果数量并浏览数据

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-id

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum', 'cardano'。
            localization (bool, optional): 是否包含本地化名称和描述，默认为 True。
            tickers (bool, optional): 是否包含交易行情数据，默认为 True。
            market_data (bool, optional): 是否包含详细的市场数据，默认为 True。
            community_data (bool, optional): 是否包含社区数据（Twitter、Reddit等），默认为 True。
            developer_data (bool, optional): 是否包含开发者数据（GitHub等），默认为 True。
            sparkline (bool, optional): 是否包含7天价格走势图数据，默认为 False。

        Returns:
            Dict[str, Any]: 硬币详细数据，包含以下字段：

            **基础信息：**
                - id (str): 硬币ID
                - symbol (str): 硬币符号
                - name (str): 硬币名称
                - asset_platform_id (str): 资产平台ID
                - block_time_in_minutes (int): 区块时间（分钟）
                - hashing_algorithm (str): 哈希算法
                - categories (List[str]): 分类标签
                - country_origin (str): 原产国
                - genesis_date (str): 创世日期

            **平台和地址信息：**
                - platforms (Dict[str, str]): 平台地址信息
                - detail_platforms (Dict): 详细平台信息

            **描述和链接：**
                - description (Dict[str, str], optional): 多语言描述（仅当 localization=True 时）
                - links (Dict): 相关链接
                    - homepage (List[str]): 官方网站
                    - blockchain_site (List[str]): 区块浏览器
                    - official_forum_url (List[str]): 官方论坛
                    - chat_url (List[str]): 聊天群组
                    - announcement_url (List[str]): 公告页面
                    - twitter_screen_name (str): Twitter用户名
                    - facebook_username (str): Facebook用户名
                    - bitcointalk_thread_identifier (int): BitcoinTalk论坛ID
                    - telegram_channel_identifier (str): Telegram频道
                    - subreddit_url (str): Reddit社区
                    - repos_url (Dict): 代码仓库链接

                - image (Dict[str, str]): 不同尺寸的图标URL
                    - thumb (str): 缩略图
                    - small (str): 小图标
                    - large (str): 大图标

            **公告和通知：**
                - public_notice (str): 公告信息
                - additional_notices (List): 额外通知
                - status_updates (List): 状态更新

            **评分和排名：**
                - market_cap_rank (int): 市值排名
                - coingecko_rank (int): CoinGecko排名
                - coingecko_score (float): CoinGecko评分
                - developer_score (float): 开发者评分
                - community_score (float): 社区评分
                - liquidity_score (float): 流动性评分
                - public_interest_score (float): 公众兴趣评分

            **用户互动数据：**
                - sentiment_votes_up_percentage (float): 正面情绪投票百分比
                - sentiment_votes_down_percentage (float): 负面情绪投票百分比
                - watchlist_portfolio_users (int): 关注用户数
                - public_interest_stats (Dict): 公众兴趣统计

            **可选数据（根据参数决定）：**
                - market_data (Dict, optional): 详细市场数据（仅当 market_data=True 时）
                - community_data (Dict, optional): 社区数据（仅当 community_data=True 时）
                - developer_data (Dict, optional): 开发者数据（仅当 developer_data=True 时）
                - tickers (List[Dict], optional): 交易行情列表（仅当 tickers=True 时）

            **时间戳：**
                - last_updated (str): 最后更新时间（ISO 8601格式）

        Note:
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit
            - 返回的是硬币的完整详细信息，包括描述、链接、社区数据等

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-id-tickers

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum'。
            exchange_ids (str, optional): 指定交易所ID列表，用逗号分隔。
                如果不指定，则返回所有交易所的数据。
            include_exchange_logo (bool, optional): 是否包含交易所logo URL，默认为 False。
            page (int, optional): 页码，从 1 开始，默认为 1。
            order (str, optional): 排序方式，默认为 'trust_score_desc'。
                可选值：'trust_score_desc', 'trust_score_asc', 'volume_desc'。
            depth (bool, optional): 是否包含2%深度的买卖盘数据，默认为 False。

        Returns:
            Dict[str, Any]: 交易行情数据，包含以下字段：

            **主要字段：**
                - name (str): 硬币名称
                - tickers (List[Dict]): 交易行情列表

            **tickers 中每个元素的结构：**
                - base (str): 基础货币符号
                - target (str): 目标货币符号
                - last (float): 最新价格
                - volume (float): 24小时交易量
                - trust_score (str): 信任评分 ('green', 'yellow', 'red')
                - bid_ask_spread_percentage (float): 买卖价差百分比
                - timestamp (str): 时间戳
                - last_traded_at (str): 最后交易时间
                - last_fetch_at (str): 最后获取时间
                - is_anomaly (bool): 是否为异常数据
                - is_stale (bool): 是否为过期数据
                - trade_url (str): 交易页面URL
                - token_info_url (str): 代币信息URL
                - coin_id (str): 硬币ID
                - target_coin_id (str): 目标硬币ID

                **market（交易所信息）：**
                    - name (str): 交易所名称
                    - identifier (str): 交易所标识符
                    - has_trading_incentive (bool): 是否有交易激励
                    - logo (str, optional): 交易所logo URL（仅当 include_exchange_logo=True 时）

                **converted_last（转换后的最新价格）：**
                    - btc (float): BTC计价的价格
                    - eth (float): ETH计价的价格
                    - usd (float): USD计价的价格

                **converted_volume（转换后的交易量）：**
                    - btc (float): BTC计价的交易量
                    - eth (float): ETH计价的交易量
                    - usd (float): USD计价的交易量

        Note:
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit
            - 返回指定硬币在各个交易所的实时交易行情数据
            - 可以通过 exchange_ids 过滤特定交易所的数据

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-id-history

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum'。
            date (str): 查询日期，格式为 'dd-mm-yyyy'，如 '30-12-2017'。
            localization (bool, optional): 是否包含本地化名称和描述，默认为 True。

        Returns:
            Dict[str, Any]: 指定日期的历史数据，包含以下字段：

            **基础信息：**
                - id (str): 硬币ID
                - symbol (str): 硬币符号
                - name (str): 硬币名称
                - localization (Dict[str, str], optional): 多语言名称（仅当 localization=True 时）
                - image (Dict[str, str]): 不同尺寸的图标URL
                    - thumb (str): 缩略图
                    - small (str): 小图标
                    - large (str): 大图标

            **市场数据：**
                - market_data (Dict): 历史市场数据
                    - current_price (Dict[str, float]): 多种货币的当时价格
                        - usd (float): 美元价格
                        - eur (float): 欧元价格
                        - btc (float): 比特币价格
                        - eth (float): 以太坊价格
                        - （其他支持的货币）
                    - market_cap (Dict[str, float]): 多种货币的市值
                    - total_volume (Dict[str, float]): 多种货币的交易量

            **社区数据：**
                - community_data (Dict): 社区统计数据
                    - facebook_likes (int): Facebook点赞数
                    - twitter_followers (int): Twitter关注者数
                    - reddit_average_posts_48h (float): Reddit 48小时平均帖子数
                    - reddit_average_comments_48h (float): Reddit 48小时平均评论数
                    - reddit_subscribers (int): Reddit订阅者数
                    - reddit_accounts_active_48h (str): Reddit 48小时活跃账户数

            **开发者数据：**
                - developer_data (Dict): GitHub开发统计数据
                    - forks (int): GitHub分叉数
                    - stars (int): GitHub星标数
                    - subscribers (int): GitHub订阅者数
                    - total_issues (int): GitHub总问题数
                    - closed_issues (int): GitHub已关闭问题数
                    - pull_requests_merged (int): 已合并的拉取请求数
                    - pull_request_contributors (int): 拉取请求贡献者数
                    - code_additions_deletions_4_weeks (Dict): 4周内代码增删统计
                        - additions (int): 新增代码行数
                        - deletions (int): 删除代码行数
                    - commit_count_4_weeks (int): 4周内提交次数

            **公众兴趣统计：**
                - public_interest_stats (Dict): 网络关注度数据
                    - alexa_rank (int): Alexa排名
                    - bing_matches (int): Bing搜索匹配数

        Note:
            - 返回的数据时间为 00:00:00 UTC
            - 最后一个完整的 UTC 日（00:00）在下一个 UTC 日的午夜后 35 分钟（00:35）可用
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit
            - ⚠️ 注意：`twitter_followers` 数据字段将从 2025年5月15日起不再受我们的 API 支持

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-id-market-chart

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum'。
            vs_currency (str, optional): 对比货币代码，默认为 'usd'。
                支持的货币包括：usd, eur, jpy, btc, eth, ltc, bch, bnb, eos, xrp, xlm 等。
            days (int, optional): 查询的天数，默认为 1。
                可选值：1, 7, 14, 30, 90, 180, 365, 'max'。
                当 days=1 时，数据间隔为 5 分钟；days=1-90 时为 1 小时；days>90 时为 1 天。
            interval (str, optional): 数据间隔，默认根据天数自动选择。
                可选值：'5m'（5分钟）, '1h'（1小时）, '1d'（1天）。
            precision (str, optional): 价格精度，范围 0-18 位小数，或 'full' 显示完整精度。

        Returns:
            Dict[str, Any]: 历史图表数据，包含三个主要数据数组：

            **价格数据：**
                - prices (List[List[float]]): 价格历史数据
                    格式：[[timestamp, price], [timestamp, price], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - price (float): 对应时间点的价格

            **市值数据：**
                - market_caps (List[List[float]]): 市值历史数据
                    格式：[[timestamp, market_cap], [timestamp, market_cap], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - market_cap (float): 对应时间点的市值

            **交易量数据：**
                - total_volumes (List[List[float]]): 交易量历史数据
                    格式：[[timestamp, volume], [timestamp, volume], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - volume (float): 对应时间点的24小时交易量

            **数据间隔说明：**
                - days=1: 每5分钟一个数据点
                - days=1-90: 每1小时一个数据点
                - days>90: 每1天一个数据点

        Note:
            - 所有时间戳都是 Unix 时间戳（毫秒）
            - 数据点的数量取决于查询的天数和时间间隔
            - 对于较长的时间范围，数据会被聚合以减少数据点数量
            - 数据间隔会根据天数自动选择：
                • days=1 时：5分钟间隔
                • days=1-90 时：1小时间隔
                • days>90 时：1天间隔
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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

        官方文档: https://docs.coingecko.com/reference/coins-id-market-chart-range

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum'。
            from_timestamp (int): 开始时间的 Unix 时间戳（秒）。
            to_timestamp (int): 结束时间的 Unix 时间戳（秒）。
            vs_currency (str, optional): 对比货币代码，默认为 'usd'。
                支持的货币包括：usd, eur, jpy, btc, eth, ltc, bch, bnb, eos, xrp, xlm 等。
            precision (str, optional): 价格精度，范围 0-18 位小数，或 'full' 显示完整精度。

        Returns:
            Dict[str, Any]: 指定时间范围的历史图表数据，包含三个主要数据数组：

            **价格数据：**
                - prices (List[List[float]]): 价格历史数据
                    格式：[[timestamp, price], [timestamp, price], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - price (float): 对应时间点的价格

            **市值数据：**
                - market_caps (List[List[float]]): 市值历史数据
                    格式：[[timestamp, market_cap], [timestamp, market_cap], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - market_cap (float): 对应时间点的市值

            **交易量数据：**
                - total_volumes (List[List[float]]): 交易量历史数据
                    格式：[[timestamp, volume], [timestamp, volume], ...]
                    - timestamp (float): Unix时间戳（毫秒）
                    - volume (float): 对应时间点的24小时交易量

            **注意事项：**
                - 输入时间戳使用秒级，但返回数据中的时间戳是毫秒级
                - 数据间隔根据查询的时间范围自动调整

        Note:
            - 时间戳参数使用秒级 Unix 时间戳，但返回数据中的时间戳是毫秒级
            - 数据间隔会根据查询的时间范围自动调整
            - 最大查询范围取决于硬币的历史数据可用性
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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
        获取硬币的OHLC（开盘价、最高价、最低价、收盘价）图表数据

        官方文档: https://docs.coingecko.com/reference/coins-id-ohlc

        Args:
            coin_id (str): 硬币的唯一标识符，如 'bitcoin', 'ethereum'。
            vs_currency (str, optional): 对比货币代码，默认为 'usd'。
                支持的货币包括：usd, eur, jpy, btc, eth, ltc, bch, bnb, eos, xrp, xlm 等。
            days (int, optional): 查询的天数，默认为 1。
                可选值：1, 7, 14, 30, 90, 180, 365。注意：不支持 'max' 选项。
            precision (str, optional): 价格精度，范围 0-18 位小数，或 'full' 显示完整精度。

        Returns:
            List[List[float]]: OHLC（开高低收）数据列表，每个元素格式为：

            **数据结构：**
                [timestamp, open, high, low, close]

                - timestamp (float): Unix时间戳（毫秒）- 表示该时间段的结束时间
                - open (float): 开盘价 - 该时间段的第一个价格
                - high (float): 最高价 - 该时间段内的最高价格
                - low (float): 最低价 - 该时间段内的最低价格
                - close (float): 收盘价 - 该时间段的最后一个价格

            **时间间隔（自动调整）：**
                - 1-2天: 30分钟K线
                - 3-30天: 4小时K线
                - 31天及以上: 4天K线

            **示例数据：**
                [
                    [1640995200000, 47686.0, 48200.5, 47500.0, 48000.0],
                    [1641081600000, 48000.0, 48500.0, 47800.0, 48300.0],
                    ...
                ]

        Note:
            - 数据间隔会根据查询的天数自动调整
            - 每个时间段（如1天、1小时等）对应一个OHLC数据点
            - 适用于创建K线图（蜡烛图）
            - 响应负载中显示的时间戳表示 OHLC 数据的结束（或收盘）时间
            - 数据粒度（蜡烛主体）是自动的：
                • 1-2天：30分钟
                • 3-30天：4小时
                • 31天及以上：4天
            - 缓存/更新频率：所有 API 计划每 15 分钟一次
            - 最后一个完整的 UTC 日（00:00）在下一个 UTC 日的午夜后 35 分钟（00:35）可用
            - 付费计划订阅者可使用专属的每日和每小时蜡烛间隔参数：
                • 'daily' 间隔仅适用于 1/7/14/30/90/180 天
                • 'hourly' 间隔仅适用于 1/7/14/30/90 天
            - 可通过多种方式获取硬币 ID（API ID）：
                • 访问相应硬币页面并查找 'API ID'
                • 使用 /coins/list 端点
                • 参考 Google Sheets：https://docs.google.com/spreadsheets/d/1wTTuxXt8n9q7C4NDXqQpI3wpKu1_5bGVmP9Xz0XGSyU/edit
            - 如需更好粒度的历史图表数据，可考虑使用 /coins/{id}/market_chart 端点

        Raises:
            requests.exceptions.RequestException: 当 API 请求失败时抛出异常
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
        api_key (str, optional): CoinGecko Pro API 密钥。
            如果不提供，将尝试从环境变量 COINGECKO_API_KEY 中获取。
            如果环境变量也不存在，将使用免费API（有限制）。

    Returns:
        CoinGeckoAPI: 已配置的 CoinGecko API 客户端实例。

    Example:
        >>> # 使用环境变量中的API密钥
        >>> api = create_api_client()
        >>>
        >>> # 或者直接提供API密钥
        >>> api = create_api_client("your-api-key-here")
        >>>
        >>> # 获取Bitcoin市场数据
        >>> btc_data = api.get_coins_markets(ids="bitcoin")
    """
    return CoinGeckoAPI(api_key)
