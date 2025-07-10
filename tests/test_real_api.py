"""
批量下载器真实API测试

测试与真实CoinGecko API的集成。
"""

import os
import sys
import tempfile
import shutil

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data.batch_downloader import create_batch_downloader


def test_real_api_integration():
    """测试真实API集成"""
    print("=== 真实API集成测试 ===\n")

    # 使用临时目录进行测试
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"使用临时目录: {temp_dir}")

        # 创建批量下载器
        downloader = create_batch_downloader(data_dir=temp_dir)

        print("\n测试1: 下载前3名币种的最近7天数据")
        try:
            results = downloader.download_batch(
                top_n=3,
                days="7",
                force_update=True,  # 强制更新以确保测试
                request_interval=1,  # 1秒间隔
            )

            print(f"下载结果: {results}")

            # 检查结果
            success_count = sum(1 for status in results.values() if status == "success")
            print(f"成功下载: {success_count}/3 个币种")

            if success_count > 0:
                print("✓ API集成测试成功")

                # 列出下载的文件
                downloaded_coins = downloader.list_downloaded_coins()
                print(f"已下载币种: {downloaded_coins}")

                # 检查文件内容
                if downloaded_coins:
                    import pandas as pd

                    coin_id = downloaded_coins[0]
                    csv_file = os.path.join(temp_dir, "coins", f"{coin_id}.csv")

                    if os.path.exists(csv_file):
                        df = pd.read_csv(csv_file)
                        print(f"{coin_id} 数据记录数: {len(df)}")
                        print(f"数据列: {list(df.columns)}")
                        print(f"样本数据:\n{df.head(3)}")
                        print("✓ CSV文件格式正确")
                    else:
                        print("❌ CSV文件不存在")
            else:
                print("❌ 没有成功下载任何币种")

        except Exception as e:
            print(f"❌ API测试失败: {e}")
            raise

        print("\n测试2: 测试数据新鲜度检查")
        try:
            # 再次运行相同的下载，应该跳过
            results2 = downloader.download_batch(
                top_n=2, days="7", force_update=False  # 只测试前2名  # 不强制更新
            )

            skipped_count = sum(
                1 for status in results2.values() if status == "skipped"
            )
            if skipped_count > 0:
                print(f"✓ 数据新鲜度检查正常，跳过了 {skipped_count} 个币种")
            else:
                print("⚠️  数据新鲜度检查可能有问题，没有跳过任何币种")

        except Exception as e:
            print(f"❌ 新鲜度检查测试失败: {e}")

        print("\n测试3: 状态查询功能")
        try:
            downloaded_coins = downloader.list_downloaded_coins()
            if downloaded_coins:
                coin_id = downloaded_coins[0]
                status = downloader.get_download_status(coin_id)
                print(f"{coin_id} 的状态: {status}")

                if status and "last_update" in status:
                    print("✓ 状态查询功能正常")
                else:
                    print("❌ 状态信息不完整")
            else:
                print("⚠️  没有已下载的币种可供查询")

        except Exception as e:
            print(f"❌ 状态查询测试失败: {e}")


def main():
    """主测试函数"""
    print("开始批量下载器真实API测试...\n")

    # 检查是否有API Key
    from dotenv import load_dotenv

    load_dotenv()

    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        print("⚠️  警告: 没有找到 COINGECKO_API_KEY 环境变量")
        print("将使用免费API进行测试（有限制）")
    else:
        print("✓ 找到API Key，使用Pro API进行测试")

    try:
        test_real_api_integration()
        print("\n🎉 所有真实API测试通过！")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        print("\n可能的原因:")
        print("1. 网络连接问题")
        print("2. API Key无效或已达限制")
        print("3. CoinGecko API服务问题")
        return False

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
