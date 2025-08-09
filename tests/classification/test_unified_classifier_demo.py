#!/usr/bin/env python3
"""覆盖 unified_classifier.main 演示代码

目的：提升 unified_classifier.py 覆盖率（演示部分 329+ 行段落）。
策略：打补丁替换 classify_coin / get_classification_summary / filter_coins / export_classification_csv，避免真实网络或文件访问。
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class DummyResult:
    def __init__(self, coin_id):
        self.coin_id = coin_id
        self.name = coin_id.title()
        self.symbol = coin_id[:3]
        self.is_stablecoin = coin_id.startswith("usd") or coin_id in {"tether", "dai"}
        self.is_wrapped_coin = coin_id.startswith("wrapped")
        self.stablecoin_categories = ["Stablecoins"] if self.is_stablecoin else []
        self.wrapped_categories = ["Wrapped-Tokens"] if self.is_wrapped_coin else []
        self.all_categories = self.stablecoin_categories + self.wrapped_categories
        self.last_updated = "2024-01-01"


class TestUnifiedClassifierDemo(unittest.TestCase):
    def test_main_demo(self):
        with patch("src.classification.unified_classifier.UnifiedClassifier.classify_coin", side_effect=lambda cid: DummyResult(cid)), \
             patch("src.classification.unified_classifier.UnifiedClassifier.get_classification_summary", return_value={"total": 3}), \
             patch("src.classification.unified_classifier.UnifiedClassifier.filter_coins", side_effect=lambda ids, **k: ids), \
             patch("src.classification.unified_classifier.UnifiedClassifier.export_classification_csv", return_value=True):
            from src.classification import unified_classifier
            unified_classifier.main()  # 应正常运行


if __name__ == "__main__":
    unittest.main(verbosity=2)
