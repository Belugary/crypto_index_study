# Legacy Classification Components

这个目录包含已被弃用的分类器组件，它们已被 `UnifiedClassifier` 替代。

## 迁移历史

**日期**: 2025-07-14
**原因**: 重构优化和代码简化

## 已移动的组件

### StablecoinChecker (stablecoin_checker.py)

- **功能**: 基于元数据检查稳定币
- **替代**: `UnifiedClassifier.classify_coin()` 中的 `is_stablecoin` 字段
- **迁移方法**:

  ```python
  # 旧方式
  from src.classification.stablecoin_checker import StablecoinChecker
  checker = StablecoinChecker()
  result = checker.is_stablecoin("tether")

  # 新方式
  from src.classification import UnifiedClassifier
  classifier = UnifiedClassifier()
  result = classifier.classify_coin("tether")
  is_stable = result.is_stablecoin
  ```

### WrappedCoinChecker (wrapped_coin_checker.py)

- **功能**: 基于元数据检查包装币
- **替代**: `UnifiedClassifier.classify_coin()` 中的 `is_wrapped_coin` 字段
- **迁移方法**:

  ```python
  # 旧方式
  from src.classification.wrapped_coin_checker import WrappedCoinChecker
  checker = WrappedCoinChecker()
  result = checker.is_wrapped_coin("wrapped-bitcoin")

  # 新方式
  from src.classification import UnifiedClassifier
  classifier = UnifiedClassifier()
  result = classifier.classify_coin("wrapped-bitcoin")
  is_wrapped = result.is_wrapped_coin
  ```

## UnifiedClassifier 的优势

1. **性能提升**: 批量处理和缓存机制
2. **代码简化**: 单一接口处理所有分类需求
3. **一致性**: 统一的返回格式和错误处理
4. **扩展性**: 更容易添加新的分类类型

## 向后兼容性

这些 legacy 组件仍然可以使用，但不再推荐。它们可能在未来版本中完全移除。

如需使用 legacy 组件，请直接导入：

```python
# 不推荐，但仍可用
from src.classification.legacy.stablecoin_checker import StablecoinChecker
from src.classification.legacy.wrapped_coin_checker import WrappedCoinChecker
```
