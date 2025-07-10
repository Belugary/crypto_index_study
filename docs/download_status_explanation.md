# 批量下载状态说明

## 📊 下载状态类型

批量下载器会为每个币种返回以下三种状态之一：

### 1. ✅ **success** (下载成功)

**含义**: 币种数据成功从 CoinGecko API 获取并保存到本地

**触发条件**:

- API 请求成功返回数据
- 数据格式验证通过
- CSV 文件成功写入 `data/coins/` 目录
- 元数据文件成功更新

**示例场景**:

```
✅ bitcoin: 下载成功 (获取了 2847 条历史记录)
✅ ethereum: 下载成功 (获取了 2847 条历史记录)
```

### 2. ⏭️ **skipped** (智能跳过)

**含义**: 本地已有足够新鲜的数据，无需重新下载

**触发条件**:

- 本地存在该币种的数据文件
- 数据参数匹配（同样的 days 参数）
- 数据足够新鲜（在时间阈值内）

**时间阈值规则**:

- `days="max"` (完整历史): 24 小时内的数据认为新鲜
- `days="30"` 等具体天数: 12 小时内的数据认为新鲜

**示例场景**:

```
⏭️ bitcoin: 数据已是最新，跳过 (上次更新: 2小时前)
⏭️ ethereum: 数据已是最新，跳过 (上次更新: 6小时前)
```

### 3. ❌ **failed** (下载失败)

**含义**: 尝试下载但遇到错误，经过重试后仍然失败

**可能原因**:

1. **网络问题**: 网络连接不稳定或超时
2. **API 限制**: 达到 CoinGecko API 的速率限制
3. **币种不存在**: 币种 ID 无效或已下架
4. **API Key 问题**: API Key 无效或权限不足
5. **数据格式问题**: API 返回的数据格式异常
6. **文件系统问题**: 磁盘空间不足或权限问题

**重试机制**:

- 默认重试 3 次
- 每次重试间隔 5 秒
- 重试期间会记录详细错误信息

**示例场景**:

```
❌ some-coin: 第 1 次尝试失败: HTTPError 429 (Too Many Requests)
❌ some-coin: 第 2 次尝试失败: HTTPError 429 (Too Many Requests)
❌ some-coin: 第 3 次尝试失败: HTTPError 429 (Too Many Requests)
❌ some-coin: 所有重试都失败，放弃下载
```

## 🔍 状态查看方法

### 1. 运行时实时状态

下载过程中会显示实时进度：

```
下载进度: 45%|████▌     | 45/100 [02:15<02:45, 0.33it/s, 当前=cardano]
```

### 2. 完成后的统计报告

```
📊 下载结果统计:
   ✅ 成功下载: 78 个币种
   ❌ 下载失败: 12 个币种
   ⏭️ 智能跳过: 10 个币种
   📈 总计处理: 100 个币种
```

### 3. 查看具体币种状态

```python
# 查看特定币种的详细状态
status = downloader.get_download_status('bitcoin')
print(status)
# 输出: {'last_update': '2025-01-10T08:30:00Z', 'days': 'max', 'record_count': 2847}
```

## 🛠️ 问题处理建议

### 如果遇到大量 "failed" 状态:

1. **检查网络连接**:

   ```bash
   ping api.coingecko.com
   ```

2. **检查 API Key**:

   ```bash
   # 确认 .env 文件中的 API Key 正确
   cat .env | grep COINGECKO_API_KEY
   ```

3. **调整下载参数**:

   ```python
   # 增加请求间隔，减少速率限制问题
   results = downloader.download_batch(
       top_n=50,  # 减少数量
       days="30", # 减少数据量
       request_interval=3  # 增加间隔到3秒
   )
   ```

4. **分批下载**:
   ```python
   # 分批下载，避免一次性下载过多
   for i in range(0, 100, 20):
       results = downloader.download_batch(
           top_n=20,
           days="max",
           skip_first=i  # 跳过前 i 个
       )
   ```

### 如果希望强制重新下载:

```python
# 忽略本地缓存，强制重新下载
results = downloader.download_batch(
    top_n=100,
    days="max",
    force_update=True  # 强制更新
)
```

## 📈 最佳实践

1. **首次大量下载**: 使用较长的请求间隔 (2-3 秒)
2. **增量更新**: 使用智能缓存 (`force_update=False`)
3. **错误处理**: 检查失败的币种，单独重试
4. **监控进度**: 关注进度条和日志输出
5. **资源管理**: 避免在高峰时段进行大量下载

## 📁 相关文件

- **数据文件**: `data/coins/*.csv`
- **元数据**: `data/metadata/download_metadata.json`
- **日志文件**: `data/logs/batch_downloader.log`
- **配置文件**: `.env` (API Key 配置)
