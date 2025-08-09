"""PriceDataUpdater 智能策略核心路径测试

聚焦 update_with_smart_strategy:
1. 分类过滤：稳定币 / 包装币不计入 native 目标
2. 目标达到提前退出
3. 统计字段更新
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.updaters.price_updater import PriceDataUpdater  # noqa: E402
from src.classification.unified_classifier import ClassificationResult  # noqa: E402


class TestPriceUpdaterStrategy(unittest.TestCase):
    @patch('src.updaters.price_updater.find_project_root')
    @patch('src.updaters.price_updater.create_batch_downloader')
    @patch('src.updaters.price_updater.MarketDataFetcher')
    @patch('src.updaters.price_updater.UnifiedClassifier')
    @patch('src.updaters.price_updater.CoinGeckoAPI')
    def test_native_target_achieved(self, m_api, m_classifier, m_fetcher, m_downloader, m_root):
        m_root.return_value = project_root
        dl_inst = MagicMock()
        dl_inst.download_coin_data.return_value = True
        m_downloader.return_value = dl_inst
        def classify(cid):
            if cid.startswith('n'):
                return ClassificationResult(coin_id=cid,is_stablecoin=False,is_wrapped_coin=False,confidence='high')
            if cid.startswith('s'):
                return ClassificationResult(coin_id=cid,is_stablecoin=True,confidence='high')
            if cid.startswith('w'):
                return ClassificationResult(coin_id=cid,is_wrapped_coin=True,confidence='high')
            return ClassificationResult(coin_id=cid,confidence='unknown')
        cls_inst = MagicMock()
        cls_inst.classify_coin.side_effect = classify
        # 补充 batch 接口，使 update_metadata() 阶段能够得到稳定币/包装币统计，避免日志显示 0
        def classify_batch(ids, use_cache=True):
            return {cid: classify(cid) for cid in ids}
        cls_inst.classify_coins_batch.side_effect = classify_batch
        m_classifier.return_value = cls_inst
        fetch_inst = MagicMock()
        fetch_inst.get_top_coins.return_value = [
            {'id':'native1','symbol':'n1','name':'N1'},
            {'id':'stable1','symbol':'s1','name':'S1'},
            {'id':'wrapped1','symbol':'w1','name':'W1'},
            {'id':'native2','symbol':'n2','name':'N2'},
            {'id':'native3','symbol':'n3','name':'N3'},
        ]
        m_fetcher.return_value = fetch_inst
        updater = PriceDataUpdater()
        updater.update_with_smart_strategy(target_native_coins=2, max_search_range=500)
        self.assertGreaterEqual(updater.stats['native_updated'], 2)
        self.assertEqual(updater.stats['failed_updates'], 0)
        fetch_inst.get_top_coins.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
