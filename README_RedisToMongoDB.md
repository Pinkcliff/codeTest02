# Redis到MongoDB温度数据迁移与同步系统

## 系统概述

本系统实现了将Redis中的温度数据迁移和实时同步到MongoDB的功能。支持历史数据的批量迁移和新增数据的实时同步，并提供了完整的数据查询和可视化工具。

## 文件说明

### 核心模块

1. **mongodb_client.py** - MongoDB连接管理类
   - 提供MongoDB的连接、断开、增删改查等基本操作
   - 支持批量操作、索引创建、聚合查询等高级功能
   - 包含错误处理和连接管理

2. **redis_to_mongodb_migrator.py** - 数据迁移工具
   - 批量迁移Redis中的历史温度数据到MongoDB
   - 支持全量迁移和指定会话迁移
   - 保留所有数据结构：实时数据、历史数据、时间序列数据、统计数据
   - 提供迁移统计和进度报告

3. **realtime_redis_to_mongodb_sync.py** - 实时同步工具
   - 实时监控Redis中的新数据并同步到MongoDB
   - 支持增量同步，避免重复数据
   - 多线程架构，不影响Redis的正常操作
   - 记录同步状态，支持断点续传

4. **mongodb_data_viewer.py** - 数据查询和验证工具
   - 提供友好的命令行界面查询MongoDB数据
   - 支持按时间范围、会话、通道等多维度查询
   - 数据可视化功能（温度趋势图）
   - 支持Redis与MongoDB数据对比
   - 数据导出为JSON格式

## MongoDB数据结构

### 1. realtime_temperature 实时数据集合
```json
{
  "session_prefix": "20231208_120000",
  "timestamp": "2023-12-08T12:00:00",
  "channel_count": 12,
  "channels": {
    "channel_01": {"value": 25.5, "raw": 255},
    "channel_02": {"value": 26.1, "raw": 261},
    ...
  },
  "synced_at": "2023-12-08T12:00:01"
}
```

### 2. historical_temperature 历史数据集合
```json
{
  "session_prefix": "20231208_120000",
  "timestamp": "2023-12-08T12:00:00",
  "temperatures": [25.5, 26.1, 24.8, ...],
  "channel_count": 12,
  "synced_at": "2023-12-08T12:00:01"
}
```

### 3. timeseries_temperature 时间序列数据集合
```json
{
  "session_prefix": "20231208_120000",
  "channel": "channel_01",
  "timestamp": "2023-12-08T12:00:00",
  "temperature": 25.5,
  "timestamp_unix": 1702032000.0,
  "synced_at": "2023-12-08T12:00:01"
}
```

### 4. statistics_temperature 统计数据集合
```json
{
  "session_prefix": "20231208_120000",
  "last_update": "2023-12-08T12:00:00",
  "channel_count": 12,
  "statistics": {
    "min_temperature": 24.8,
    "max_temperature": 26.5,
    "avg_temperature": 25.6,
    "channel_min": "channel_03",
    "channel_max": "channel_07"
  },
  "channels": {
    "channel_01": 25.5,
    "channel_02": 26.1,
    ...
  },
  "synced_at": "2023-12-08T12:00:01"
}
```

### 5. sync_status 同步状态集合（内部使用）
```json
{
  "data_type": "realtime",
  "key": "20231208_120000:temperature:realtime",
  "timestamp": "2023-12-08T12:00:00",
  "synced_at": "2023-12-08T12:00:01"
}
```

### 6. sync_progress 同步进度集合（内部使用）
```json
{
  "data_type": "historical",
  "key": "20231208_120000:temperature:history",
  "count": 1000,
  "last_score": 1702032000.0,
  "updated_at": "2023-12-08T12:00:01"
}
```

## 使用指南

### 1. 环境准备

安装Python依赖：
```bash
pip install pymongo pandas matplotlib redis
```

启动MongoDB服务（默认端口27017）。

### 2. 数据迁移

执行历史数据迁移：
```bash
python redis_to_mongodb_migrator.py
```

选择迁移选项：
- 1：迁移所有会话数据
- 2：迁移指定会话数据

### 3. 实时同步

启动实时同步服务：
```bash
python realtime_redis_to_mongodb_sync.py
```

同步服务会：
- 监控Redis中的新数据
- 自动同步到MongoDB
- 显示同步进度和统计信息

### 4. 数据查询

启动数据查询工具：
```bash
python mongodb_data_viewer.py
```

支持的功能：
- 查看集合信息
- 查询各类温度数据
- 数据可视化
- 数据导出
- Redis vs MongoDB数据对比

## 特性

### 1. 数据完整性
- 保留所有原始数据，包括原始ADC值和转换后的温度值
- 时间戳精确到毫秒
- 支持批量操作，保证数据一致性

### 2. 高性能
- 使用批量插入提高写入性能
- 创建合适的索引优化查询
- 支持并发操作

### 3. 可靠性
- 完善的错误处理机制
- 断点续传功能
- 数据对比验证

### 4. 易用性
- 友好的命令行界面
- 详细的操作提示
- 清晰的统计报告

## 性能优化建议

1. **MongoDB配置优化**
   - 调整缓存大小
   - 使用SSD存储
   - 配置副本集

2. **索引优化**
   - 根据查询模式创建复合索引
   - 定期分析慢查询

3. **批量操作**
   - 调整批量大小
   - 使用异步写入

## 故障排除

### 1. 连接失败
- 检查MongoDB服务是否启动
- 验证连接参数
- 检查防火墙设置

### 2. 同步延迟
- 调整同步间隔
- 增加批量大小
- 优化网络连接

### 3. 数据不一致
- 运行数据对比工具
- 检查同步状态记录
- 重新同步指定时间段

## 扩展开发

系统采用模块化设计，易于扩展：

1. **添加新的数据类型**
   - 扩展迁移器中的对应方法
   - 更新数据结构定义

2. **支持其他数据库**
   - 实现新的数据库连接管理器
   - 保持接口一致性

3. **添加实时分析**
   - 使用MongoDB的聚合管道
   - 实现告警功能

## 许可证

本项目仅供学习和研究使用。