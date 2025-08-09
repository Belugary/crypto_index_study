"""数据库查询核心功能测试（第一阶段新增）

覆盖 DatabaseManager 主要读取接口（不修改真实库）：
1. get_available_dates / get_daily_market_data 基本正确性
2. get_top_coins_by_market_cap limit 行为
3. get_price_history 基本路径
4. get_coin_info 存在 / 不存在
5. get_database_stats 字段完整

使用现有 data/market.db 只读；若不存在则跳过。避免写操作。
"""
import os
import sys
import unittest
from pathlib import Path

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.utils.database_utils import DatabaseManager  # noqa: E402


class TestDatabaseReadOnlyQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_path = Path(project_root) / "data" / "market.db"
        if not cls.db_path.exists():
            raise unittest.SkipTest("market.db 不存在，跳过数据库读取测试")
        cls.db = DatabaseManager(str(cls.db_path))

    def test_available_dates_non_empty_sorted(self):
        dates = self.db.get_available_dates()
        self.assertIsInstance(dates, list)
        self.assertGreater(len(dates), 10, "日期数量异常过少，可能数据未准备")
        self.assertEqual(dates, sorted(dates))

    def test_get_daily_market_data_sample(self):
        dates = self.db.get_available_dates()
        sample_date = dates[0]
        df = self.db.get_daily_market_data(sample_date)
        # 允许为空（边缘情况），但若不为空则应包含关键列
        if not df.empty:
            self.assertTrue({'coin_id', 'market_cap'}.issubset(set(df.columns)))

    def test_get_top_coins_limit(self):
        dates = self.db.get_available_dates()
        sample_date = dates[-1]
        df10 = self.db.get_top_coins_by_market_cap(sample_date, limit=10)
        self.assertLessEqual(len(df10), 10)
        if len(df10) > 1:
            # 市值非增序
            self.assertTrue(all(df10.market_cap.iloc[i] >= df10.market_cap.iloc[i+1] for i in range(len(df10)-1)))

    def test_price_history_range(self):
        dates = self.db.get_available_dates()
        if len(dates) < 3:
            self.skipTest("可用日期不足，跳过")
        mid = dates[len(dates)//2]
        df = self.db.get_price_history('bitcoin', dates[0], mid)
        if not df.empty:
            self.assertTrue({'date', 'price'}.issubset(set(df.columns)))
            self.assertGreaterEqual(df.date.min(), dates[0])
            self.assertLessEqual(df.date.max(), mid)

    def test_coin_info_exist_and_missing(self):
        info = self.db.get_coin_info('bitcoin')
        if info is None:
            self.skipTest('bitcoin 未在数据库中，跳过')
        else:
            self.assertEqual(info['id'], 'bitcoin')
        missing = self.db.get_coin_info('non-existent-coin-id-xyz')
        self.assertIsNone(missing)

    def test_database_stats_fields(self):
        stats = self.db.get_database_stats()
        for k in ['total_coins','total_records','earliest_date','latest_date']:
            self.assertIn(k, stats)


if __name__ == '__main__':
    unittest.main(verbosity=2)
