import unittest
import tempfile
import pandas as pd
from pathlib import Path
from datetime import date
from unittest import mock

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator


class TestMarketCapWeightedIndexCalculatorAdditional(unittest.TestCase):
    """补充测试：覆盖之前未命中的关键分支/方法

    覆盖目标:
    1. save_index 输出文件与小数格式
    2. _get_daily_market_caps 缺少列时返回 {}
    3. _get_coin_price 在空数据时返回 None
    4. _calculate_weights (已废弃) 仍保持向后兼容
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.calc = MarketCapWeightedIndexCalculator(
            data_dir=self.tmpdir.name,
            daily_output_dir=self.tmpdir.name,
            exclude_stablecoins=False,
            exclude_wrapped_coins=False,
            use_database=False,
        )

    def test_save_index_creates_file_with_six_decimals(self):
        df = pd.DataFrame([
            {"date": date(2024, 1, 1), "index_value": 1000.0, "constituent_count": 2},
            {"date": date(2024, 1, 2), "index_value": 1010.1234567, "constituent_count": 2},
        ])
        out_path = Path(self.tmpdir.name) / "idx.csv"
        self.calc.save_index(df, str(out_path))
        self.assertTrue(out_path.exists(), "输出文件未创建")
        content = out_path.read_text(encoding="utf-8").strip().splitlines()
        header = content[0].split(',')
        self.assertEqual(header, ["date", "index_value", "constituent_count"])
        first_data = content[1].split(',')
        self.assertRegex(first_data[1], r"^1000\.000000$", "未按 6 位小数格式化")

    def test_get_daily_market_caps_missing_columns_returns_empty(self):
        df_missing = pd.DataFrame({
            "coin_id": ["a", "b"],
            "price": [1.0, 2.0],
        })
        with mock.patch.object(
            self.calc.daily_aggregator,
            "get_daily_data",
            return_value=df_missing,
        ):
            result = self.calc._get_daily_market_caps(date(2024, 1, 1))
        self.assertEqual(result, {}, "应在缺少列时返回空字典")

    def test_get_coin_price_empty_dataframe_returns_none(self):
        empty_df = pd.DataFrame(columns=["coin_id", "price", "market_cap"])
        with mock.patch.object(
            self.calc.daily_aggregator, "get_daily_data", return_value=empty_df
        ):
            price = self.calc._get_coin_price("bitcoin", date(2024, 1, 1))
        self.assertIsNone(price, "空数据应返回 None")

    def test_calculate_weights_deprecated_method(self):
        market_caps = {"a": 100.0, "b": 300.0, "c": 600.0}
        weights = self.calc._calculate_weights(["a", "b", "c"], market_caps)
        self.assertAlmostEqual(sum(weights.values()), 1.0, places=6)
        self.assertAlmostEqual(weights["a"], 0.1)
        self.assertAlmostEqual(weights["b"], 0.3)
        self.assertAlmostEqual(weights["c"], 0.6)


if __name__ == "__main__":
    unittest.main(verbosity=2)
