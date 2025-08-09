"""UnifiedClassifier 逻辑测试（补充分类真实分支）

通过模拟 downloader._load_coin_metadata 返回不同 categories 组合，覆盖：
1. 稳定币 only
2. 包装币 only
3. 同时稳定+包装
4. 普通原生
5. 缺失元数据 (unknown)
6. filter_coins / get_classification_summary / export_classification_csv
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.classification.unified_classifier import UnifiedClassifier  # noqa: E402


class TestUnifiedClassifierLogic(unittest.TestCase):
    def setUp(self):
        self.meta_map = {
            'usdt': {'name':'Tether','symbol':'usdt','categories':['Stablecoins']},
            'weth': {'name':'WETH','symbol':'weth','categories':['Wrapped-Tokens']},
            'steth': {'name':'stETH','symbol':'steth','categories':['Stablecoins','Wrapped-Tokens']},
            'bitcoin': {'name':'Bitcoin','symbol':'btc','categories':['Layer 1']},
        }
        fake_downloader = MagicMock()
        fake_downloader._load_coin_metadata.side_effect = lambda cid: self.meta_map.get(cid, None)
        p = patch('src.classification.unified_classifier.create_batch_downloader', return_value=fake_downloader)
        self.addCleanup(p.stop)
        p.start()
        self.classifier = UnifiedClassifier()

    def test_classify_various_types(self):
        self.assertTrue(self.classifier.classify_coin('usdt', use_cache=False).is_stablecoin)
        self.assertTrue(self.classifier.classify_coin('weth', use_cache=False).is_wrapped_coin)
        both = self.classifier.classify_coin('steth', use_cache=False)
        self.assertTrue(both.is_wrapped_coin and both.is_stablecoin)
        btc = self.classifier.classify_coin('bitcoin', use_cache=False)
        self.assertFalse(btc.is_stablecoin or btc.is_wrapped_coin)
        unknown = self.classifier.classify_coin('unknown-x', use_cache=False)
        self.assertEqual(unknown.confidence, 'unknown')

    def test_filter_summary_export(self):
        ids = ['usdt','weth','steth','bitcoin','unknown-x']
        filtered = self.classifier.filter_coins(ids, exclude_stablecoins=True, exclude_wrapped_coins=False, use_cache=False)
        self.assertNotIn('usdt', filtered)
        summary = self.classifier.get_classification_summary(ids)
        self.assertEqual(summary['stablecoins'], 1)
        self.assertEqual(summary['wrapped_coins'], 1)
        self.assertEqual(summary['both_stable_and_wrapped'], 1)
        with tempfile.TemporaryDirectory() as td:
            out = Path(td)/'classification.csv'
            ok = self.classifier.export_classification_csv(ids, output_path=str(out))
            self.assertTrue(ok and out.exists())


if __name__ == '__main__':
    unittest.main(verbosity=2)
