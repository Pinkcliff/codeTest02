# 增强版传感器管理系统

## 系统概述

这是一个增强版的中控机传感器管理系统，支持多I/O模块并发管理、多种传感器类型（温度、风速、压力、湿度），并提供Redis和MongoDB双重数据存储方案。

## 主要改进

### 1. 架构改进
- ✅ 支持多I/O模块并发管理（最多16个模块）
- ✅ 支持多线程并发数据采集
- ✅ 自动故障检测和恢复机制
- ✅ 模块化设计，易于扩展

### 2. 传感器支持
- ✅ 温度传感器（支持RTC模块和普通模块）
- ✅ 风速传感器（100个）
- ✅ 压力传感器（1个，带温度）
- ✅ 湿度传感器（4个，每个带温度）
- ✅ 总计：205个传感器

### 3. 数据存储
- ✅ Redis高速缓存（实时数据、历史数据、时间序列）
- ✅ MongoDB持久化存储（支持复杂查询）
- ✅ 实时数据同步
- ✅ 数据统计分析

## 核心组件

### 1. enhanced_sensor_system.py
增强版传感器数据采集系统，包含：
- `MultiIOModuleManager`: 多I/O模块管理器
- `IOModuleReader`: 独立的I/O模块读取线程
- `EnhancedSensorData`: 增强的传感器数据结构
- `ModbusRTUClient`: Modbus RTU通信客户端

### 2. integrated_sensor_system.py
集成系统管理器，提供：
- Redis和MongoDB集成存储
- 完整的205个传感器配置
- 系统统计和监控
- 数据导出功能

## 安装依赖

```bash
pip install -r requirements.txt
```

依赖列表：
- redis==5.0.1
- pymongo==4.6.0
- matplotlib==3.8.2
- numpy==1.26.2
- pandas==2.1.4

## 使用方法

### 1. 基础传感器管理
```python
from enhanced_sensor_system import MultiIOModuleManager, create_default_config

# 创建管理器
manager = MultiIOModuleManager()

# 添加配置
configs = create_default_config()
for config in configs:
    manager.add_module(config)

# 启动系统
manager.start_all()
```

### 2. 集成系统（Redis+MongoDB）
```python
from integrated_sensor_system import IntegratedSensorSystem

# Redis配置
redis_config = {
    "host": "localhost",
    "port": 6379,
    "db": 0
}

# MongoDB配置
mongo_config = {
    "host": "localhost",
    "port": 27017,
    "db_name": "sensor_database"
}

# 创建并启动系统
system = IntegratedSensorSystem(redis_config, mongo_config)
configs = create_full_system_config()  # 创建完整的205个传感器配置
for config in configs:
    system.add_module_config(config)

system.start()
```

## 系统架构

### 网络配置
- **温度模块**: 192.168.0.101-109（9个模块，100个温度传感器）
- **风速模块**: 192.168.0.110-116（7个模块，100个风速传感器）
- **压力模块**: 192.168.0.117（1个模块，压力+温度）
- **湿度模块**: 192.168.0.118-121（4个模块，湿度+温度）

### 数据流
```
I/O模块 → 传感器数据采集线程 → Redis缓存 → MongoDB持久化
                    ↓
                实时监控界面
                    ↓
                数据分析工具
```

## 数据结构

### Redis数据结构
- `sensor:{type}:{id}:realtime` - 实时数据（Hash，1小时TTL）
- `sensor:{type}:{id}:history` - 历史数据（List，1000条）
- `sensor:{type}:{id}:timeseries` - 时间序列（Sorted Set，10000条）

### MongoDB集合
- `realtime_data` - 实时数据快照
- `historical_data` - 历史数据记录
- `statistics` - 统计信息（最大值、最小值、平均值等）

## 性能指标

- 支持16个I/O模块并发连接
- 205个传感器实时数据采集
- 1秒数据采集间隔（可配置）
- Redis响应时间 < 1ms
- MongoDB批量写入优化

## 监控和日志

系统提供完整的监控和日志功能：
- 模块连接状态监控
- 数据采集成功率统计
- 错误日志记录
- 性能指标追踪

## 扩展性

系统设计支持轻松扩展：
- 新增I/O模块：只需添加配置
- 新增传感器类型：扩展SensorType枚举
- 自定义数据转换：支持JSON格式的转换公式
- 新增存储后端：实现相应的存储接口

## 故障处理

- 自动重连机制：连接断开后自动尝试重连
- 数据完整性校验：CRC校验确保数据准确性
- 优雅降级：部分模块故障不影响其他模块运行
- 错误隔离：单个传感器错误不影响整体系统

## 运行示例

```bash
# 运行基础传感器系统
python enhanced_sensor_system.py

# 运行完整集成系统
python integrated_sensor_system.py
```

系统运行后将显示：
- 模块连接状态
- 实时数据变化
- 系统统计信息
- 错误和警告信息