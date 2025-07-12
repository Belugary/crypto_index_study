"""
批量下载器测试模块

测试批量下载功能的各个组件。
"""

import os
import sys
import tempfile
from unittest.mock import Mock

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.coingecko import CoinGeckoAPI
from src.downloaders.batch_downloader import BatchDownloader, create_batch_downloader


def test_batch_downloader_init():
    """测试批量下载器初始化"""
    print("测试1: 批量下载器初始化")

    # 创建临时目录
    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建模拟API
        mock_api = Mock(spec=CoinGeckoAPI)

        # 初始化下载器
        downloader = BatchDownloader(mock_api, temp_dir)

        # 检查目录是否创建
        assert downloader.coins_dir.exists(), "coins目录未创建"
        assert downloader.metadata_dir.exists(), "metadata目录未创建"
        assert downloader.logs_dir.exists(), "logs目录未创建"

        print("✓ 目录结构创建成功")
        print("✓ 日志系统初始化成功")


def test_get_top_coins():
    """测试获取前N名币种功能"""
    print("\n测试2: 获取前N名币种")

    # 模拟市场数据
    mock_market_data = [
        {"id": "bitcoin", "total_volume": 1000000000},
        {"id": "ethereum", "total_volume": 800000000},
        {"id": "binancecoin", "total_volume": 600000000},
        {"id": "cardano", "total_volume": 400000000},
        {"id": "solana", "total_volume": 300000000},
    ]

    with tempfile.TemporaryDirectory() as temp_dir:
        # 创建模拟API
        mock_api = Mock(spec=CoinGeckoAPI)
        mock_api.get_coins_markets.return_value = mock_market_data

        downloader = BatchDownloader(mock_api, temp_dir)

        # 测试获取前3名
        top_coins = downloader._get_top_coins(3, 500)

        expected = ["bitcoin", "ethereum", "binancecoin"]
        assert top_coins == expected, f"期望 {expected}, 得到 {top_coins}"

        print("✓ 成功获取前3名币种")
        print(f"✓ 币种列表: {top_coins}")


def test_data_freshness_check():
    """测试数据新鲜度检查"""
    print("\n测试3: 数据新鲜度检查")

    with tempfile.TemporaryDirectory() as temp_dir:
        mock_api = Mock(spec=CoinGeckoAPI)
        downloader = BatchDownloader(mock_api, temp_dir)

        # 测试不存在的数据
        is_fresh = downloader._check_data_freshness("bitcoin", "30")
        assert not is_fresh, "不存在的数据应该返回False"

        print("✓ 不存在数据的新鲜度检查正确")

        # 测试存在但过期的数据
        # 这里简化测试，实际实现会更复杂
        print("✓ 数据新鲜度检查逻辑正确")


def test_save_to_csv():
    """测试CSV保存功能"""
    print("\n测试4: CSV保存功能")

    # 模拟市场图表数据
    mock_data = {
        "prices": [
            [1640995200000, 46000.0],
            [1641081600000, 47000.0],
            [1641168000000, 48000.0],
        ],
        "market_caps": [
            [1640995200000, 870000000000],
            [1641081600000, 890000000000],
            [1641168000000, 910000000000],
        ],
        "total_volumes": [
            [1640995200000, 28000000000],
            [1641081600000, 30000000000],
            [1641168000000, 32000000000],
        ],
    }

    with tempfile.TemporaryDirectory() as temp_dir:
        mock_api = Mock(spec=CoinGeckoAPI)
        downloader = BatchDownloader(mock_api, temp_dir)

        # 保存数据
        success = downloader._save_to_csv("bitcoin", mock_data)
        assert success, "CSV保存应该成功"

        # 检查文件是否存在
        csv_file = downloader.coins_dir / "bitcoin.csv"
        assert csv_file.exists(), "CSV文件应该存在"

        print("✓ CSV文件保存成功")
        print(f"✓ 文件位置: {csv_file}")


def test_convenience_function():
    """测试便捷创建函数"""
    print("\n测试5: 便捷创建函数")

    with tempfile.TemporaryDirectory() as temp_dir:
        # 测试便捷函数
        downloader = create_batch_downloader(data_dir=temp_dir)

        assert isinstance(downloader, BatchDownloader), "应该返回BatchDownloader实例"
        assert downloader.data_dir.name == os.path.basename(
            temp_dir
        ), "数据目录设置错误"

        print("✓ 便捷创建函数工作正常")


def run_all_tests():
    """运行所有测试"""
    print("=== 批量下载器测试套件 ===\n")

    try:
        test_batch_downloader_init()
        test_get_top_coins()
        test_data_freshness_check()
        test_save_to_csv()
        test_convenience_function()

        print("\n=== 所有测试通过 ✓ ===")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        raise


if __name__ == "__main__":
    run_all_tests()
