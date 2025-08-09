"""BatchDownloader 高级行为测试

覆盖关键分支：
1. 新鲜度跳过
2. force_update 强制重新下载
3. 重试成功路径

使用临时目录隔离环境。
"""
import os
import sys
import json
import tempfile
import unittest
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone, timedelta

# 添加项目根目录到路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from src.downloaders.batch_downloader import BatchDownloader  # noqa: E402


class TestBatchDownloaderAdvanced(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        (self.root / 'data' / 'coins').mkdir(parents=True)
        (self.root / 'data' / 'metadata').mkdir(parents=True)
        (self.root / 'logs').mkdir(parents=True)
        self.mock_api = MagicMock()
        self.mock_api.get_coins_markets.return_value = [{'id':'coinA'},{'id':'coinB'}]
        base_chart = {
            'prices': [[1000,1.0],[1001,1.1]],
            'market_caps': [[1000,10.0],[1001,11.0]],
            'total_volumes': [[1000,5.0],[1001,6.0]]
        }
        state = {'coinA':0}
        def side_effect_chart(coin_id, vs_currency, days):
            if coin_id=='coinA':
                state['coinA'] +=1
                if state['coinA'] <2:
                    raise RuntimeError('temp error')
            return base_chart
        self.mock_api.get_coin_market_chart.side_effect = side_effect_chart
        p = patch('src.downloaders.batch_downloader.find_project_root', return_value=self.root)
        self.addCleanup(p.stop)
        p.start()
        self.downloader = BatchDownloader(self.mock_api, data_dir=str(self.root/'data'), enable_database=False)

    def tearDown(self):
        """确保日志文件句柄及时关闭，避免 Windows 上临时目录清理失败 (WinError 32)."""
        temp_root_str = str(self.root) if hasattr(self, 'root') else None
        try:
            # 1. 关闭当前 downloader 的 logger handlers
            if hasattr(self, 'downloader') and getattr(self.downloader, 'logger', None):
                for h in list(self.downloader.logger.handlers):
                    try:
                        if isinstance(h, logging.FileHandler):
                            h.flush()
                        h.close()
                        self.downloader.logger.removeHandler(h)
                    except Exception:
                        pass

            # 2. 扫描全局 logging registry，关闭所有指向临时目录下的 FileHandler
            if temp_root_str:
                manager = logging.root.manager
                for logger_name, logger_obj in list(manager.loggerDict.items()):
                    if not isinstance(logger_obj, logging.Logger):
                        continue
                    # 仅处理 BatchDownloader 相关的，减少不必要操作
                    if not logger_name.startswith('BatchDownloader.'):
                        continue
                    for h in list(logger_obj.handlers):
                        if isinstance(h, logging.FileHandler):
                            base = getattr(h, 'baseFilename', '')
                            if temp_root_str in base:
                                try:
                                    h.flush()
                                    h.close()
                                    logger_obj.removeHandler(h)
                                except Exception:
                                    pass
        except Exception:
            # 避免清理失败影响测试结果
            pass

    def _write_fresh_metadata(self, coin_id, days='7', age_hours=1):
        meta_file = self.root/'data'/'metadata'/'download_metadata.json'
        now = datetime.now(timezone.utc) - timedelta(hours=age_hours)
        with open(meta_file,'w',encoding='utf-8') as f:
            json.dump({coin_id:{'last_update':now.isoformat(),'days':days}}, f)

    def test_skip_due_to_freshness(self):
        self._write_fresh_metadata('coinA')
        res = self.downloader.download_batch(1,'7',force_update=False,force_overwrite=False,max_retries=2,retry_delay=0)
        self.assertEqual(res.get('coinA'),'skipped')

    def test_force_update_ignores_fresh(self):
        self._write_fresh_metadata('coinB')
        res = self.downloader.download_batch(2,'7',force_update=True,force_overwrite=False,max_retries=2,retry_delay=0)
        self.assertIn(res.get('coinB'), ['success','failed'])

    def test_retry_then_success(self):
        res = self.downloader.download_batch(1,'7',force_update=True,max_retries=3,retry_delay=0)
        self.assertEqual(res.get('coinA'),'success')


if __name__ == '__main__':
    unittest.main(verbosity=2)
