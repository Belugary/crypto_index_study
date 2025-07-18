#!/usr/bin/env python3
"""
MarketCapWeightedIndexCalculator 缓存修复测试

验证修复后的缓存一致性问题
"""

import sys
import os
import unittest
from datetime import date

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.market_cap_weighted import MarketCapWeightedIndexCalculator

class TestMarketCapCalculatorCacheFix(unittest.TestCase):
    """测试市值加权指数计算器的缓存修复"""
    
    def setUp(self):
        self.test_date = date(2023, 10, 1)
        
    def test_cache_consistency_with_all_coins(self):
        """测试包含所有币种时的缓存一致性"""
        print('\n🧪 测试包含所有币种的缓存一致性')
        
        calculator = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=False, 
            exclude_wrapped_coins=False
        )
        
        # 清空缓存
        if hasattr(calculator, '_daily_cache'):
            calculator._daily_cache.clear()
        
        # 第一次调用
        data1 = calculator._get_daily_data_cached(self.test_date)
        print(f'   第一次调用: {len(data1)} 个币种')
        
        # 第二次调用（应该从缓存获取）
        data2 = calculator._get_daily_data_cached(self.test_date)
        print(f'   第二次调用: {len(data2)} 个币种')
        
        # 验证一致性
        self.assertEqual(len(data1), len(data2))
        print(f'   ✅ 缓存一致性通过')
        
        # 验证现在包含稳定币（修复前不包含）
        stablecoins = ['tether', 'usd-coin', 'binance-usd', 'dai']
        if not data1.empty:
            has_stables = any(s in data1['coin_id'].values for s in stablecoins)
            print(f'   缓存数据包含稳定币: {has_stables}')
            self.assertTrue(has_stables, "配置为包含稳定币时，缓存数据应该包含稳定币")
    
    def test_filter_logic_works_correctly(self):
        """测试过滤逻辑工作正确"""
        print('\n🧪 测试过滤逻辑的正确性')
        
        # 测试排除稳定币的配置
        calculator_exclude = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=True, 
            exclude_wrapped_coins=True
        )
        
        # 测试包含所有币种的配置
        calculator_include = MarketCapWeightedIndexCalculator(
            exclude_stablecoins=False, 
            exclude_wrapped_coins=False
        )
        
        # 清空缓存
        for calc in [calculator_exclude, calculator_include]:
            if hasattr(calc, '_daily_cache'):
                calc._daily_cache.clear()
        
        # 获取原始数据（现在都应该是全量数据）
        raw_data_exclude = calculator_exclude._get_daily_data_cached(self.test_date)
        raw_data_include = calculator_include._get_daily_data_cached(self.test_date)
        
        print(f'   排除配置缓存数据: {len(raw_data_exclude)} 个币种')
        print(f'   包含配置缓存数据: {len(raw_data_include)} 个币种')
        
        # 原始数据应该相同（都是全量）
        self.assertEqual(len(raw_data_exclude), len(raw_data_include))
        print(f'   ✅ 原始缓存数据一致性通过')
        
        # 过滤后的数据应该不同
        filtered_exclude = calculator_exclude._filter_coins(raw_data_exclude)
        filtered_include = calculator_include._filter_coins(raw_data_include)
        
        print(f'   排除配置过滤后: {len(filtered_exclude)} 个币种')
        print(f'   包含配置过滤后: {len(filtered_include)} 个币种')
        
        # 排除配置的结果应该更少或相等
        self.assertLessEqual(len(filtered_exclude), len(filtered_include))
        print(f'   ✅ 过滤逻辑正确性通过')
        
    def test_comprehensive_cache_behavior(self):
        """全面测试缓存行为"""
        print('\n🧪 全面测试缓存行为')
        
        test_configs = [
            {'exclude_stablecoins': True, 'exclude_wrapped_coins': True, 'desc': '排除稳定币和包装币'},
            {'exclude_stablecoins': False, 'exclude_wrapped_coins': False, 'desc': '包含所有币种'},
        ]
        
        for config in test_configs:
            print(f'\n   测试配置: {config["desc"]}')
            
            calculator = MarketCapWeightedIndexCalculator(
                exclude_stablecoins=config['exclude_stablecoins'],
                exclude_wrapped_coins=config['exclude_wrapped_coins']
            )
            
            # 清空缓存
            if hasattr(calculator, '_daily_cache'):
                calculator._daily_cache.clear()
            
            # 缓存层数据（应该总是全量）
            raw_data = calculator._get_daily_data_cached(self.test_date)
            
            # 业务层过滤
            filtered_data = calculator._filter_coins(raw_data)
            
            # 检查稳定币
            stablecoins = ['tether', 'usd-coin', 'binance-usd', 'dai']
            has_stables_raw = any(s in raw_data['coin_id'].values for s in stablecoins if not raw_data.empty)
            has_stables_filtered = any(s in filtered_data['coin_id'].values for s in stablecoins if not filtered_data.empty)
            
            print(f'     缓存数据包含稳定币: {has_stables_raw}')
            print(f'     过滤后包含稳定币: {has_stables_filtered}')
            
            # 验证过滤逻辑
            if config['exclude_stablecoins']:
                self.assertFalse(has_stables_filtered, f"排除稳定币配置下，过滤后不应包含稳定币")
            else:
                if has_stables_raw:  # 只有当原始数据有稳定币时才验证
                    self.assertTrue(has_stables_filtered, f"包含稳定币配置下，过滤后应该包含稳定币")
            
            print(f'     ✅ 配置 "{config["desc"]}" 验证通过')

if __name__ == "__main__":
    print('🔧 MarketCapWeightedIndexCalculator 缓存修复验证')
    print('=' * 60)
    unittest.main(verbosity=2)
