#!/usr/bin/env python3
"""
测试数据库插入功能
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.database_utils import DatabaseManager
import time

def test_database_insert():
    """测试数据库插入功能"""
    print("🧪 测试数据库插入功能...")
    
    try:
        # 创建数据库管理器
        db_manager = DatabaseManager()
        
        # 准备测试数据
        test_coin_id = "test-coin-" + str(int(time.time()))
        test_data = [
            {
                'timestamp': int(time.time() * 1000),  # 当前时间戳（毫秒）
                'price': 100.5,
                'volume': 1000000,
                'market_cap': 50000000,
                'market_cap_rank': 999
            }
        ]
        
        print(f"测试币种: {test_coin_id}")
        print(f"测试数据: {test_data}")
        
        # 尝试插入
        result = db_manager.insert_coin_price_data(test_coin_id, test_data)
        
        if result:
            print("✅ 数据库插入测试成功！")
        else:
            print("❌ 数据库插入测试失败！")
            
        # 使用断言而不是返回值
        assert result, "数据库插入操作失败"
        
    except Exception as e:
        print(f"❌ 测试过程中出错: {e}")
        # 使用断言失败而不是返回 False
        assert False, f"测试过程中出错: {e}"

if __name__ == "__main__":
    success = test_database_insert()
    print(f"\n🎯 测试结果: {'通过' if success else '失败'}")
