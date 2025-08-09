import unittest
from unittest.mock import patch, MagicMock

from src.classification.unified_classifier import UnifiedClassifier


class TestUnifiedClassifierFuzzyStablecoin(unittest.TestCase):
    """测试稳定币模糊匹配与误判防护

    覆盖点：
    1. 多种官方细分类别 (USD Stablecoin / Fiat-backed Stablecoin / Algorithmic Stablecoin)
    2. 小写 / 大写变体 (stablecoin, StablecoinS)
    3. 负例：包含 'unstablecoin' 不应被标记
    4. 包装币不应互相干扰
    """

    def setUp(self):
        meta_map = {
            'usd-coin': {'name': 'USDC', 'symbol': 'usdc', 'categories': ['USD Stablecoin']},
            'frax': {'name': 'Frax', 'symbol': 'frax', 'categories': ['Algorithmic Stablecoin']},
            'tether': {'name': 'Tether', 'symbol': 'usdt', 'categories': ['Fiat-backed Stablecoin']},
            'my-stable': {'name': 'MyStable', 'symbol': 'ms', 'categories': ['stablecoin utility']},  # 含 stablecoin 单词
            'plural-stable': {'name': 'PluralStable', 'symbol': 'ps', 'categories': ['Stablecoins']},
            'wrapped-bitcoin': {'name': 'WBTC', 'symbol': 'wbtc', 'categories': ['Tokenized BTC', 'Crypto-Backed Tokens']},
            'fake-unstable': {'name': 'Unstable Token', 'symbol': 'unst', 'categories': ['Unstablecoin Experiment']},  # 不应匹配
        }
        fake_downloader = MagicMock()
        fake_downloader._load_coin_metadata.side_effect = lambda cid: meta_map.get(cid, None)
        p = patch('src.classification.unified_classifier.create_batch_downloader', return_value=fake_downloader)
        self.addCleanup(p.stop)
        p.start()
        self.classifier = UnifiedClassifier()

    def test_fuzzy_and_negative_cases(self):
        self.assertTrue(self.classifier.classify_coin('usd-coin', use_cache=False).is_stablecoin)
        self.assertTrue(self.classifier.classify_coin('frax', use_cache=False).is_stablecoin)
        self.assertTrue(self.classifier.classify_coin('tether', use_cache=False).is_stablecoin)
        self.assertTrue(self.classifier.classify_coin('my-stable', use_cache=False).is_stablecoin)
        self.assertTrue(self.classifier.classify_coin('plural-stable', use_cache=False).is_stablecoin)
        self.assertFalse(self.classifier.classify_coin('fake-unstable', use_cache=False).is_stablecoin)
        wrapped = self.classifier.classify_coin('wrapped-bitcoin', use_cache=False)
        self.assertTrue(wrapped.is_wrapped_coin)
        self.assertFalse(wrapped.is_stablecoin)  # 不应被错误归类为稳定币

    def test_filter_exclude_stablecoins(self):
        ids = ['usd-coin','frax','tether','my-stable','fake-unstable','wrapped-bitcoin']
        filtered = self.classifier.filter_coins(ids, exclude_stablecoins=True, use_cache=False)
        # 所有真正稳定币被排除，fake-unstable 不排除
        for cid in ['usd-coin','frax','tether','my-stable']:
            self.assertNotIn(cid, filtered)
        self.assertIn('fake-unstable', filtered)
        self.assertIn('wrapped-bitcoin', filtered)


if __name__ == '__main__':
    unittest.main(verbosity=2)
