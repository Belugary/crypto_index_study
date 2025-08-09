"""
路径工具模块测试
"""

import os
import sys
import unittest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.path_utils import find_project_root, resolve_data_path, ensure_directory


class TestPathUtils(unittest.TestCase):
    """测试路径工具模块"""

    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def tearDown(self):
        """测试后清理"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)

    def test_find_project_root_with_git(self):
        """测试通过.git目录找到项目根目录"""
        print("\n--- 测试通过.git目录找到项目根目录 ---")
        
        # 创建测试目录结构
        project_root = self.temp_dir / "project"
        project_root.mkdir()
        (project_root / ".git").mkdir()
        
        # 在子目录中查找
        sub_dir = project_root / "src" / "utils"
        sub_dir.mkdir(parents=True)
        
        # 从子目录查找项目根目录
        found_root = find_project_root(sub_dir)
        self.assertEqual(found_root, project_root)
        print(f"✓ 成功从 {sub_dir} 找到项目根目录 {found_root}")

    def test_find_project_root_with_src_requirements(self):
        """测试通过src目录和requirements.txt找到项目根目录"""
        print("\n--- 测试通过src目录和requirements.txt找到项目根目录 ---")
        
        # 创建测试目录结构
        project_root = self.temp_dir / "project"
        project_root.mkdir()
        (project_root / "src").mkdir()
        (project_root / "requirements.txt").touch()
        
        # 在子目录中查找
        sub_dir = project_root / "data" / "coins"
        sub_dir.mkdir(parents=True)
        
        # 从子目录查找项目根目录
        found_root = find_project_root(sub_dir)
        self.assertEqual(found_root, project_root)
        print(f"✓ 成功从 {sub_dir} 找到项目根目录 {found_root}")

    def test_find_project_root_not_found(self):
        """测试找不到项目根目录的情况"""
        print("\n--- 测试找不到项目根目录的情况 ---")
        
        # 创建没有项目标志的目录
        empty_dir = self.temp_dir / "empty"
        empty_dir.mkdir()
        
        # 应该抛出异常
        with self.assertRaises(FileNotFoundError) as context:
            find_project_root(empty_dir)
        
        self.assertIn("无法找到项目根目录", str(context.exception))
        print(f"✓ 正确抛出异常: {context.exception}")

    def test_find_project_root_default_path(self):
        """测试默认路径参数"""
        print("\n--- 测试默认路径参数 ---")
        
        # 使用当前工作目录（应该能找到真实的项目根目录）
        with patch('pathlib.Path.cwd') as mock_cwd:
            # 找到真实的项目根目录
            real_project_root = Path(__file__).parent.parent
            # 模拟当前目录为项目内的某个子目录
            mock_cwd.return_value = real_project_root / "src"
            
            found_root = find_project_root()
            
            # 验证找到的是真实的项目根目录
            self.assertTrue((found_root / "src").exists())
            self.assertTrue((found_root / "requirements.txt").exists() or (found_root / ".git").exists())
            print(f"✓ 默认查找成功找到项目根目录: {found_root}")

    def test_resolve_data_path_relative(self):
        """测试解析相对数据路径"""
        print("\n--- 测试解析相对数据路径 ---")
        
        # 创建测试项目根目录
        project_root = self.temp_dir / "project"
        project_root.mkdir()
        
        # 解析相对路径
        resolved_path = resolve_data_path("data/coins", project_root)
        expected_path = project_root / "data" / "coins"
        
        self.assertEqual(resolved_path, expected_path)
        print(f"✓ 相对路径 'data/coins' 解析为: {resolved_path}")

    def test_resolve_data_path_absolute(self):
        """测试解析绝对数据路径"""
        print("\n--- 测试解析绝对数据路径 ---")
        
        # 创建测试项目根目录
        project_root = self.temp_dir / "project"
        project_root.mkdir()
        
        # 绝对路径应该原样返回
        absolute_path = self.temp_dir / "external_data"
        resolved_path = resolve_data_path(str(absolute_path), project_root)
        
        self.assertEqual(resolved_path, absolute_path)
        print(f"✓ 绝对路径保持不变: {resolved_path}")

    def test_resolve_data_path_auto_project_root(self):
        """测试自动查找项目根目录"""
        print("\n--- 测试自动查找项目根目录 ---")
        
        with patch('src.utils.path_utils.find_project_root') as mock_find_root:
            mock_project_root = self.temp_dir / "mock_project"
            mock_find_root.return_value = mock_project_root
            
            resolved_path = resolve_data_path("data/coins")
            expected_path = mock_project_root / "data" / "coins"
            
            self.assertEqual(resolved_path, expected_path)
            mock_find_root.assert_called_once()
            print(f"✓ 自动查找项目根目录成功: {resolved_path}")

    def test_ensure_directory_creates_directory(self):
        """测试创建目录功能"""
        print("\n--- 测试创建目录功能 ---")
        
        # 测试创建不存在的目录
        new_dir = self.temp_dir / "new" / "nested" / "directory"
        self.assertFalse(new_dir.exists())
        
        result = ensure_directory(new_dir)
        
        self.assertTrue(new_dir.exists())
        self.assertTrue(new_dir.is_dir())
        self.assertEqual(result, new_dir)
        print(f"✓ 成功创建目录: {new_dir}")

    def test_ensure_directory_existing_directory(self):
        """测试已存在目录的情况"""
        print("\n--- 测试已存在目录的情况 ---")
        
        # 测试已存在的目录
        existing_dir = self.temp_dir / "existing"
        existing_dir.mkdir()
        
        result = ensure_directory(existing_dir)
        
        self.assertTrue(existing_dir.exists())
        self.assertEqual(result, existing_dir)
        print(f"✓ 已存在目录处理正确: {existing_dir}")


def run_tests():
    """运行所有测试"""
    print("=" * 60)
    print("开始运行路径工具模块测试")
    print("=" * 60)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 加载测试
    suite.addTests(loader.loadTestsFromTestCase(TestPathUtils))

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("\n" + "=" * 60)
    print(f"测试完成: {result.testsRun} 个测试运行")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print("=" * 60)

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
