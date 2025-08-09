#!/usr/bin/env python3
"""calculate_index 脚本覆盖增强

目标：提高 scripts.calculate_index 覆盖率，覆盖
 - 成功路径（含 DataFrame 指标访问、保存调用）
 - 开始日期晚于结束日期的参数错误
 - FileNotFoundError 分支
 - 其它异常分支
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

# 项目根路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestCalculateIndexCLI(unittest.TestCase):
    def _build_df(self):
        return pd.DataFrame(
            [
                {"date": "2024-01-01", "index_value": 1000.0, "constituent_count": 30},
                {"date": "2024-01-02", "index_value": 1010.0, "constituent_count": 30},
            ]
        )

    @patch("scripts.calculate_index.MarketCapWeightedIndexCalculator")
    def test_success_path(self, MockCalc):
        mock_calc = MockCalc.return_value
        mock_calc.calculate_index.return_value = self._build_df()
        mock_calc.save_index.return_value = None

        argv = [
            "calculate_index.py",
            "--start-date","2024-01-01",
            "--end-date","2024-01-02",
            "--base-date","2024-01-01",
            "--base-value","1000",
            "--top-n","30",
        ]

        with patch.object(sys, "argv", argv):
            from scripts import calculate_index  # noqa: F401
            from scripts.calculate_index import main
            main()  # 不应异常
        mock_calc.calculate_index.assert_called_once()
        mock_calc.save_index.assert_called_once()

    @patch("scripts.calculate_index.MarketCapWeightedIndexCalculator")
    def test_invalid_date_range(self, MockCalc):  # noqa: ARG002
        argv = [
            "calculate_index.py",
            "--start-date","2024-01-05",
            "--end-date","2024-01-02",
        ]
        with patch.object(sys, "argv", argv), self.assertRaises(SystemExit):
            from scripts.calculate_index import main
            main()

    @patch("scripts.calculate_index.MarketCapWeightedIndexCalculator")
    def test_file_not_found(self, MockCalc):
        mock_calc = MockCalc.return_value
        mock_calc.calculate_index.side_effect = FileNotFoundError("missing data")

        argv = [
            "calculate_index.py",
            "--start-date","2024-01-01",
            "--end-date","2024-01-02",
        ]
        with patch.object(sys, "argv", argv), self.assertRaises(SystemExit):
            from scripts.calculate_index import main
            main()

    @patch("scripts.calculate_index.MarketCapWeightedIndexCalculator")
    def test_generic_exception(self, MockCalc):
        mock_calc = MockCalc.return_value
        mock_calc.calculate_index.side_effect = RuntimeError("boom")

        argv = [
            "calculate_index.py",
            "--start-date","2024-01-01",
            "--end-date","2024-01-02",
        ]
        with patch.object(sys, "argv", argv), self.assertRaises(SystemExit):
            from scripts.calculate_index import main
            main()


if __name__ == "__main__":  # 手动执行
    unittest.main(verbosity=2)
