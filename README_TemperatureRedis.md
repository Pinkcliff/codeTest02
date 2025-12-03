# 温度数据采集与Redis存储系统

## 📋 系统概述

本系统将12路RTC温度模块的数据采集到Redis数据库中，提供实时监控、历史数据查询和数据统计分析功能。

## 🚀 功能特性

- **实时数据采集**: 每秒采集12路温度传感器数据
- **多种存储方式**:
  - 实时数据 (Hash结构)
  - 历史数据 (List结构)
  - 时间序列数据 (Sorted Set结构)
  - 统计信息 (Hash结构)
- **自动重连**: 设备或Redis连接断开时自动重连
- **数据可视化**: 提供完整的数据查询和导出功能

## 📁 文件结构

```
codeTest01/
├── tem.py                    # 原始温度采集程序
├── redis_client.py           # Redis连接管理类
├── temperature_redis.py      # 集成采集与存储的主程序
├── redis_data_viewer.py      # Redis数据查看和查询工具
└── README_TemperatureRedis.md # 使用说明文档
```

## 🔧 使用前准备

### 1. 环境要求

- Python 3.7+
- Redis服务器
- 12路RTC温度模块 (Modbus RTU)

### 2. 安装依赖

```bash
pip install redis
```

### 3. Redis服务器配置

确保Redis服务器运行在默认配置：
- 地址: localhost
- 端口: 6379
- 无密码认证
- 数据库: 0

## 🚀 快速开始

### 1. 启动数据采集程序

```bash
python temperature_redis.py
```

程序将自动：
- 连接Redis数据库
- 连接温度采集设备 (IP: 192.168.1.101:8234)
- 开始采集并存储温度数据
- 按Ctrl+C停止程序

### 2. 查看实时数据

在另一个终端运行：

```bash
python redis_data_viewer.py
```

选择菜单选项查看不同类型的数据。

## 📊 数据存储结构

### 1. 实时数据 (Hash)

**键名**: `temperature:realtime`

```redis
HGET temperature:realtime timestamp
HGET temperature:realtime channel_01
HGET temperature:realtime channel_01_raw
```

**字段说明**:
- `timestamp`: 数据采集时间戳
- `channel_XX`: 通道XX的温度值 (℃)
- `channel_XX_raw`: 通道XX的原始值
- `channel_count`: 通道总数

### 2. 历史数据 (List)

**键名**: `temperature:history`

```redis
LRANGE temperature:history 0 9  # 查看最新10条记录
```

**数据格式**: JSON字符串，包含时间戳和温度数组

### 3. 时间序列数据 (Sorted Set)

**键名**: `temperature:timeseries:channel_XX`

```redis
ZREVRANGE temperature:timeseries:channel_01 0 9  # 查看通道01最新10个数据点
```

**格式**: 分数为时间戳，成员为温度值

### 4. 统计信息 (Hash)

**键名**: `temperature:statistics`

```redis
HGETALL temperature:statistics
```

**字段说明**:
- `last_update`: 最后更新时间
- `min_temperature`: 最低温度
- `max_temperature`: 最高温度
- `avg_temperature`: 平均温度
- `channel_min`: 最低温度通道
- `channel_max`: 最高温度通道

## 🛠️ 配置说明

### 设备参数配置 (temperature_redis.py)

```python
DEVICE_IP = "192.168.1.101"    # RTC模块IP地址
DEVICE_PORT = 8234           # 设备Modbus端口
SLAVE_ADDR = 1               # 设备从站地址
FUNC_CODE = 0x04             # 功能码
START_REG = 0                # 起始寄存器地址
REG_COUNT = 12               # 读取寄存器数量
READ_INTERVAL = 1            # 读取间隔(秒)
```

### Redis连接配置

```python
redis_manager = RedisManager(
    host='localhost',      # Redis服务器地址
    port=6379,            # Redis端口
    password=None,        # 密码
    db=0,                 # 数据库编号
    decode_responses=True # 自动解码响应
)
```

## 📈 数据查询示例

### 1. 使用Redis CLI查询

```bash
# 查看实时数据
redis-cli HGETALL temperature:realtime

# 查看历史数据
redis-cli LRANGE temperature:history 0 4

# 查看统计信息
redis-cli HGETALL temperature:statistics

# 查看特定通道时间序列
redis-cli ZREVRANGE temperature:timeseries:channel_01 0 9
```

### 2. 使用数据查看器

运行 `python redis_data_viewer.py` 提供以下功能：
- 查看实时温度数据
- 查看历史记录
- 查看统计信息
- 查看时间序列数据
- 查看Redis数据库信息
- 导出数据到JSON文件

### 3. Python代码查询示例

```python
from redis_client import RedisManager
import json

# 连接Redis
redis_manager = RedisManager()
redis_manager.connect()

# 获取实时数据
realtime_data = redis_manager.get_hash("temperature:realtime")
print(f"当前温度: {realtime_data.get('channel_01', 'N/A')}℃")

# 获取最新历史记录
history_records = redis_manager.redis_client.lrange("temperature:history", 0, 0)
if history_records:
    latest_record = json.loads(history_records[0])
    print(f"最新记录时间: {latest_record['timestamp']}")

redis_manager.disconnect()
```

## 📤 数据导出

使用数据查看器导出完整数据：

```bash
python redis_data_viewer.py
# 选择选项6，按提示导出数据
```

导出的JSON文件包含：
- 实时数据
- 历史数据
- 统计信息
- 各通道时间序列数据

## 🔍 故障排除

### 1. Redis连接失败

- 检查Redis服务是否启动
- 确认连接参数是否正确
- 检查网络连接

### 2. 设备连接失败

- 检查设备IP地址和端口
- 确认设备在线
- 检查网络连通性

### 3. 数据读取异常

- 检查Modbus参数配置
- 确认设备功能码和寄存器地址
- 查看设备手册确认参数

## 📊 性能优化

### 1. 数据保留策略

- 实时数据: 1小时过期
- 历史数据: 最多1000条记录
- 时间序列数据: 每通道最多10000个数据点

### 2. 内存使用优化

- 定期清理过期数据
- 监控Redis内存使用情况
- 根据需要调整数据保留策略

## 🔄 扩展功能

### 1. 添加告警功能

```python
def check_temperature_alerts(temperatures):
    """检查温度告警"""
    for i, temp in enumerate(temperatures):
        if temp > 50.0:  # 高温告警
            print(f"⚠️ 通道{i+1:02d}温度过高: {temp}℃")
        elif temp < -10.0:  # 低温告警
            print(f"⚠️ 通道{i+1:02d}温度过低: {temp}℃")
```

### 2. 添加数据备份

```python
def backup_to_file(data, filename):
    """备份到文件"""
    with open(filename, 'w') as f:
        json.dump(data, f)
```

### 3. 添加Web界面

可以使用Flask或FastAPI构建Web界面，实时显示温度数据。

## 📞 技术支持

如遇到问题，请检查：
1. Redis服务状态
2. 设备网络连接
3. 程序日志输出
4. 数据库存储情况

---

**注意**: 本系统默认配置适用于12路RTC温度模块，如需连接其他设备，请相应调整Modbus参数。