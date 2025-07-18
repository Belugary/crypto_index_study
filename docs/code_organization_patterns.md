# 代码组织与工具类设计最佳实践

## 工具类设计模式

### 何时创建工具类

当发现以下情况时，应立即考虑创建专门的工具类：

1. **重复代码出现** - 同样的格式化、处理逻辑在多处重复
2. **代码块过长** - 单个代码块超过 30 行且包含多个不同职责
3. **显示逻辑复杂** - 数据格式化、表格展示等需要多步处理

### 工具类设计原则

```python
# ✅ 好的设计 - CryptoDataDisplayer 示例
class CryptoDataDisplayer:
    """专门负责加密货币数据的格式化和显示
    
    职责清晰：
    1. 数据清理与预处理（clean_data）
    2. 智能元数据加载（_add_metadata_fields）
    3. 数据格式化（format_crypto_data）
    4. 表格显示接口（show_table）
    5. 排名连续性修复
    6. 符号大写格式化
    """
    
    def __init__(self):
        # 配置集中管理
        self.column_mapping = {...}
        self.name_corrections = {...}
    
    def clean_data(self, raw_data, target_columns=None):
        """集成数据清理：元数据加载 + 排名修复"""
        pass
    
    def format_crypto_data(self, data, columns=None):
        """单一职责：格式化数据（包含符号大写）"""
        pass
    
    def show_table(self, data, **kwargs):
        """单一职责：显示表格（防重复显示）"""
        pass

# ❌ 避免的设计
def format_and_display_everything(data, format_type, display_type, ...):
    """一个函数做太多事情，难以维护和测试"""
    pass
```

### 工具类文件组织

```text
src/utils/
├── display_utils.py      # 显示相关工具类
├── progress_utils.py     # 进度跟踪工具
├── concurrent_utils.py   # 并发处理工具
└── data_utils.py         # 数据处理工具
```

**命名规范：**

- 文件名：`{功能}_utils.py`
- 类名：`{功能}Helper` 或 `{功能}Displayer`
- 方法名：动词开头，清晰表达动作

### 立即测试原则

创建工具类后立即编写测试：

```python
# tests/test_display_utils.py
class TestCryptoDataDisplayer(unittest.TestCase):
    def setUp(self):
        self.displayer = CryptoDataDisplayer()
        
    def test_format_crypto_data_basic(self):
        """测试基本格式化功能"""
        pass
        
    def test_empty_dataframe(self):
        """测试边界情况"""
        pass
```

**测试覆盖重点：**

- 正常功能测试
- 边界情况（空数据、缺失值）
- 错误输入处理
- 格式化正确性

## Notebook 代码组织

### 代码块合并原则

**应该合并的情况：**

- 逻辑紧密相关的连续操作
- 数据的获取→清理→展示流程
- 没有中间结果需要单独验证的步骤

**应该分离的情况：**

- 需要单独调试的复杂逻辑
- 耗时很长的操作（方便单独重跑）
- 不同算法步骤之间的界限

```python
# ✅ 好的合并示例
# 获取、清理并展示代币数据
def get_and_process_data():
    # 1. 获取数据
    data = get_raw_data()
    
    # 2. 清理数据
    clean_data = clean_and_prepare(data)
    
    # 3. 展示结果
    display_results(clean_data)

# ❌ 过度分割的反例
# Cell 1: 仅获取数据
# Cell 2: 仅清理数据  
# Cell 3: 仅展示数据
# (当这些步骤紧密相关且都很简单时)
```

### 代码注释标准

```python
# 功能性注释 - 说明这段代码的目的
# 获取、清理并展示代币数据

# 步骤注释 - 说明每个步骤
# 1. 获取最新市场数据
data = aggregator.get_daily_data(latest_date)

# 2. 数据清理和准备
clean_data = process_data(data)

# 3. 展示前5大市值币种
displayer.show_table(clean_data, top_n=5)
```

## 重构时机与策略

### 立即重构的信号

1. **发现重复代码** - 第二次写类似代码时立即重构
2. **代码块过长** - 超过 20-30 行且职责不单一
3. **格式化逻辑复杂** - 数据显示需要多步转换

### 重构步骤

1. **提取工具类** - 将通用逻辑抽取为可复用的类
2. **编写测试** - 确保重构不破坏功能
3. **简化调用方** - 用简洁的API替换复杂代码
4. **验证效果** - 运行测试确保一切正常

### 重构后的好处验证

```python
# 重构前：每次都要写复杂的格式化代码
display_data['价格($)'] = display_data['价格($)'].apply(lambda x: f"{x:,.2f}")
display_data['市值($)'] = display_data['市值($)'].apply(lambda x: f"{x:,.0f}")
display_data['symbol'] = display_data['symbol'].str.upper()
# 手动处理排名跳号问题
display_data = display_data.reset_index(drop=True)
display_data['rank'] = range(1, len(display_data) + 1)
# 手动加载元数据
# ... 更多重复代码

# 重构后：一行搞定，集成所有智能功能
displayer = CryptoDataDisplayer()
clean_data = displayer.clean_data(raw_data)  # 自动元数据加载 + 排名修复
displayer.show_table(clean_data, columns=['rank', 'symbol', 'name', 'price', 'market_cap'])
```

**衡量标准：**

- 代码行数减少 50%+
- 重复逻辑消除
- 更易于测试和维护
- API 更加直观

## 工具类进化路径

### 第一版：解决眼前问题

```python
def simple_display(data):
    """简单解决当前显示需求"""
    pass
```

### 第二版：增加配置

```python
class DataDisplayer:
    """支持基本配置的显示类"""
    def __init__(self, column_mapping=None):
        pass
```

### 第三版：完善功能

```python
class CryptoDataDisplayer:
    """功能完善，测试覆盖的专业显示类"""
    # 支持多种格式、错误处理、边界情况等
```

**进化原则：**

- 先解决问题，再优化设计
- 每次迭代都要有测试覆盖
- 保持 API 的向后兼容性
