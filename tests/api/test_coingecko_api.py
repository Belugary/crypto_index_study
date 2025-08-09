"""
CoinGecko API 测试模块

使用 unittest 框架测试 API 功能
"""

import json
import os
import sys
import unittest

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.coingecko import CoinGeckoAPI


class TestCoinGeckoAPI(unittest.TestCase):
    """测试 CoinGeckoAPI 类的功能"""

    @classmethod
    def setUpClass(cls):
        """在所有测试开始前运行一次，初始化API客户端"""
        print("🚀 开始测试 CoinGecko API")
        cls.api = CoinGeckoAPI()
        if not cls.api.api_key:
            raise unittest.SkipTest("API Key 未配置，跳过所有测试")

    def test_ping(self):
        """测试与 CoinGecko API 的连接"""
        print("\n--- 测试 API 连接 (ping) ---")
        data = self.api.ping()
        self.assertIsNotNone(data, "Ping 失败，返回 None")
        # 检查返回的是否是字典类型，并且包含 'gecko_says' 键
        self.assertIsInstance(data, dict, "Ping 返回的不是一个字典")
        self.assertIn("gecko_says", data, "Ping 响应中缺少 'gecko_says' 键")
        print(f"✅ Ping 成功: {data['gecko_says']}")

    def test_get_coins_list(self):
        """测试获取所有币种列表"""
        print("\n--- 测试获取所有币种列表 ---")
        data = self.api.get_coins_list()
        self.assertIsNotNone(data, "未能获取币种列表")
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        print(f"成功获取币种列表，总数: {len(data)}")

    def test_get_coins_markets(self):
        """测试获取市场数据"""
        print("\n--- 测试获取市场数据 ---")
        data = self.api.get_coins_markets(vs_currency="usd", per_page=5)
        self.assertIsNotNone(data, "未能获取市场数据")
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 5)
        print(f"成功获取前5个币种的市场数据: {json.dumps(data, indent=2)}")

    def test_get_coin_categories_list(self):
        """测试获取所有币种分类列表"""
        print("\n--- 测试获取所有币种分类列表 ---")
        data = self.api.get_coin_categories_list()
        self.assertIsNotNone(data, "未能获取币种分类列表")
        assert data is not None  # 帮助类型检查器确认 data 不为 None
        self.assertIsInstance(data, list, "返回的数据类型不是列表")
        self.assertGreater(len(data), 0, "返回的分类列表为空")

        # 验证列表中的元素结构
        for category in data[:5]:  # 只检查前5个样本
            self.assertIn("category_id", category)
            self.assertIn("name", category)
            self.assertIsInstance(category["category_id"], str)
            self.assertIsInstance(category["name"], str)

        print(f"成功获取币种分类列表，总数: {len(data)}")
        print(f"分类列表样本: {json.dumps(data[:5], indent=2)}")

    def test_get_coin_by_id(self):
        """测试通过 ID 获取单个币种信息"""
        print("\n--- 测试通过 ID 获取单个币种信息 ---")
        data = self.api.get_coin_by_id("bitcoin")
        self.assertIsNotNone(data)
        self.assertEqual(data["id"], "bitcoin")
        print("成功获取 Bitcoin 的详细数据")

    def test_get_coin_tickers(self):
        """测试获取币种的交易行情"""
        print("\n--- 测试获取币种的交易行情 ---")
        data = self.api.get_coin_tickers("bitcoin")
        self.assertIsNotNone(data)
        self.assertEqual(data["name"], "Bitcoin")
        self.assertGreater(len(data["tickers"]), 0)
        print("成功获取 Bitcoin 的交易行情数据")

    def test_get_coin_history(self):
        """测试获取币种的历史数据"""
        print("\n--- 测试获取币种的历史数据 ---")
        data = self.api.get_coin_history("bitcoin", "01-01-2024")
        self.assertIsNotNone(data)
        self.assertEqual(data["id"], "bitcoin")
        self.assertIn("market_data", data)
        print("成功获取 Bitcoin 在 2024-01-01 的历史数据")

    def test_get_coin_market_chart(self):
        """测试获取市场图表数据"""
        print("\n--- 测试获取市场图表数据 ---")
        data = self.api.get_coin_market_chart("bitcoin", "usd", "7")
        self.assertIsNotNone(data)
        self.assertIn("prices", data)
        self.assertIn("market_caps", data)
        self.assertIn("total_volumes", data)
        self.assertGreater(len(data["prices"]), 0)
        print("成功获取 Bitcoin 7天内的市场图表数据")

    def test_get_coin_ohlc(self):
        """测试获取OHLC数据"""
        print("\n--- 测试获取OHLC数据 ---")
        data = self.api.get_coin_ohlc("bitcoin", "usd", 7)
        self.assertIsNotNone(data)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 0)
        # 验证OHLC数据格式 [time, open, high, low, close]
        self.assertEqual(len(data[0]), 5)
        print("成功获取 Bitcoin 7天内的OHLC数据")


if __name__ == "__main__":
    unittest.main()
