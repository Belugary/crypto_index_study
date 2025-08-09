#!/usr/bin/env python3
"""
测试 notebook_env.py 模块的快速环境初始化功能

测试覆盖：
1. quick_env() 基本功能
2. 缓存机制
3. 强制重新初始化
4. 数据获取辅助函数
"""

import sys
import unittest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.notebook_env import quick_env


class TestNotebookEnv(unittest.TestCase):
    """测试 notebook_env 模块"""
    
    def test_quick_env_basic_functionality(self):
        """测试 quick_env 基本功能"""
        # 调用 quick_env
        agg, disp, get_clean = quick_env()
        
        # 检查返回类型
        self.assertIsNotNone(agg)
        self.assertIsNotNone(disp)
        self.assertTrue(callable(get_clean))
        
        # 检查 aggregator 有必要的方法
        self.assertTrue(hasattr(agg, 'get_daily_data'))
        
        # 检查 displayer 有必要的方法
        self.assertTrue(hasattr(disp, 'clean_data'))
        self.assertTrue(hasattr(disp, 'show_table'))
    
    def test_quick_env_caching(self):
        """测试缓存机制"""
        # 第一次调用
        agg1, disp1, get_clean1 = quick_env()
        
        # 第二次调用应该返回相同实例
        agg2, disp2, get_clean2 = quick_env()
        
        # 检查是否是同一个实例
        self.assertIs(agg1, agg2)
        self.assertIs(disp1, disp2)
        self.assertIs(get_clean1, get_clean2)
    
    def test_quick_env_force_refresh(self):
        """测试强制重新初始化"""
        # 第一次调用
        agg1, disp1, get_clean1 = quick_env()
        
        # 强制重新初始化
        agg2, disp2, get_clean2 = quick_env(force=True)
        
        # 应该是新的实例
        self.assertIsNot(agg1, agg2)
        self.assertIsNot(disp1, disp2)
        self.assertIsNot(get_clean1, get_clean2)
    
    def test_get_clean_daily_data_function(self):
        """测试数据获取辅助函数"""
        agg, disp, get_clean = quick_env()
        
        # 模拟调用（避免实际数据库依赖）
        with patch.object(agg, 'get_daily_data') as mock_get_data, \
             patch.object(disp, 'clean_data') as mock_clean:
            
            # 设置模拟返回值
            mock_raw_data = pd.DataFrame({'test': [1, 2, 3]})
            mock_clean_data = pd.DataFrame({'cleaned': [1, 2, 3]})
            mock_get_data.return_value = mock_raw_data
            mock_clean.return_value = mock_clean_data
            
            # 调用函数
            result = get_clean("2024-01-01")
            
            # 验证调用
            mock_get_data.assert_called_once_with("2024-01-01")
            mock_clean.assert_called_once_with(mock_raw_data)
            
            # 检查返回值
            self.assertIsInstance(result, pd.DataFrame)
            if isinstance(result, pd.DataFrame):
                pd.testing.assert_frame_equal(result, mock_clean_data)
    
    def test_silent_mode(self):
        """测试静默模式"""
        # 测试静默模式（默认）
        with patch('sys.stdout') as mock_stdout:
            agg, disp, get_clean = quick_env(silent=True, force=True)
            # 应该有一些输出（项目路径等）
            # 但内部初始化应该是静默的
        
        # 测试非静默模式
        with patch('sys.stdout') as mock_stdout:
            agg, disp, get_clean = quick_env(silent=False, force=True)


def run_tests():
    """运行所有测试"""
    print("🧪 开始测试 notebook_env 模块...")
    
    # 创建测试套件
    test_suite = unittest.TestLoader().loadTestsFromTestCase(TestNotebookEnv)
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # 输出结果统计
    total_tests = result.testsRun
    failures = len(result.failures)
    errors = len(result.errors)
    success_rate = ((total_tests - failures - errors) / total_tests) * 100 if total_tests > 0 else 0
    
    print(f"\n📊 测试结果统计:")
    print(f"   总测试数: {total_tests}")
    print(f"   成功: {total_tests - failures - errors}")
    print(f"   失败: {failures}")
    print(f"   错误: {errors}")
    print(f"   成功率: {success_rate:.1f}%")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
