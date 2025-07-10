# 每日数据文件结构说明

## 文件组织方式
```
data/daily/daily_files/
├── YYYY/                    # 年份目录
│   ├── MM/                  # 月份目录
│   │   ├── YYYY-MM-DD.csv   # 每日数据文件
│   │   └── ...
│   └── ...
└── daily_files_backup/     # 原始文件备份
```

## 文件内容
每个CSV文件包含当日所有币种的数据，按市值降序排列：
- date: 日期
- coin_id: 币种ID
- price: 价格
- volume: 24小时交易量
- market_cap: 市值
- timestamp: 时间戳
- rank: 市值排名

## 访问方式
使用更新后的 DailyDataAggregator 类自动处理分层结构的文件访问。
