"""
集成传感器系统 - 支持Redis和MongoDB存储
结合了多I/O模块管理和数据存储功能
"""

import time
import threading
from typing import Dict, List
from datetime import datetime
import json
import logging
from enhanced_sensor_system import (
    MultiIOModuleManager, IOModuleConfig, SensorConfig, SensorType,
    EnhancedSensorData, create_default_config
)
from redis_client import RedisClient
from mongodb_client import MongoDBClient
from temperature_redis import TemperatureRedisManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegratedSensorSystem:
    """集成传感器系统管理器"""

    def __init__(self, redis_config: dict = None, mongo_config: dict = None):
        """
        初始化集成系统

        Args:
            redis_config: Redis连接配置
            mongo_config: MongoDB连接配置
        """
        self.sensor_manager = MultiIOModuleManager()
        self.redis_client = None
        self.mongo_client = None
        self.temp_redis_manager = None

        # 初始化Redis连接
        if redis_config:
            try:
                self.redis_client = RedisClient(**redis_config)
                self.temp_redis_manager = TemperatureRedisManager(self.redis_client)
                logger.info("Redis连接成功")
            except Exception as e:
                logger.error(f"Redis连接失败: {e}")

        # 初始化MongoDB连接
        if mongo_config:
            try:
                self.mongo_client = MongoDBClient(**mongo_config)
                logger.info("MongoDB连接成功")
            except Exception as e:
                logger.error(f"MongoDB连接失败: {e}")

        # 数据统计
        self.stats = {
            "total_read": 0,
            "redis_saved": 0,
            "mongo_saved": 0,
            "errors": 0
        }
        self.stats_lock = threading.Lock()

    def add_module_config(self, config: IOModuleConfig):
        """添加模块配置"""
        self.sensor_manager.add_module(config)

        # 注册数据回调
        self.sensor_manager.add_data_callback(self.on_sensor_data)

    def on_sensor_data(self, sensor_data: EnhancedSensorData):
        """传感器数据回调处理"""
        with self.stats_lock:
            self.stats["total_read"] += 1

        # 存储到Redis
        if self.redis_client:
            try:
                self.save_to_redis(sensor_data)
                with self.stats_lock:
                    self.stats["redis_saved"] += 1
            except Exception as e:
                logger.error(f"Redis存储失败: {e}")
                with self.stats_lock:
                    self.stats["errors"] += 1

        # 存储到MongoDB
        if self.mongo_client:
            try:
                self.save_to_mongodb(sensor_data)
                with self.stats_lock:
                    self.stats["mongo_saved"] += 1
            except Exception as e:
                logger.error(f"MongoDB存储失败: {e}")
                with self.stats_lock:
                    self.stats["errors"] += 1

    def save_to_redis(self, sensor_data: EnhancedSensorData):
        """保存数据到Redis"""
        data_dict = sensor_data.to_dict()
        session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 使用TemperatureRedisManager保存温度数据
        if sensor_data.sensor_type == SensorType.TEMPERATURE and self.temp_redis_manager:
            # 提取通道号（假设sensor_id格式为 tem_ch01）
            if "ch" in sensor_data.sensor_id:
                channel = sensor_data.sensor_id.split("ch")[-1]
                self.temp_redis_manager.store_temperature(session_id, int(channel), sensor_data.value)

        # 保存所有类型的数据
        key_prefix = f"sensor:{sensor_data.sensor_type.value}:{sensor_data.sensor_id}"

        # 实时数据
        self.redis_client.set_hash(f"{key_prefix}:realtime", {
            "value": str(sensor_data.value),
            "raw_value": str(sensor_data.raw_value),
            "timestamp": sensor_data.timestamp.isoformat(),
            "quality": sensor_data.quality
        })
        self.redis_client.set_expire(f"{key_prefix}:realtime", 3600)  # 1小时过期

        # 历史数据（列表）
        self.redis_client.list_push(f"{key_prefix}:history", json.dumps(data_dict))
        self.redis_client.list_trim(f"{key_prefix}:history", 0, 999)  # 保留最近1000条

        # 时间序列数据（有序集合）
        score = sensor_data.timestamp.timestamp()
        self.redis_client.z_add(f"{key_prefix}:timeseries", {json.dumps(data_dict): score})
        self.redis_client.z_rem_range_by_rank(f"{key_prefix}:timeseries", 0, -10001)  # 保留最近10000条

    def save_to_mongodb(self, sensor_data: EnhancedSensorData):
        """保存数据到MongoDB"""
        data_dict = sensor_data.to_dict()
        collection_name = f"{sensor_data.sensor_type.value}_data"

        # 插入实时数据
        self.mongo_client.insert_one("realtime_data", {
            "sensor_id": sensor_data.sensor_id,
            "sensor_type": sensor_data.sensor_type.value,
            "data": data_dict,
            "updated_at": datetime.now()
        }, upsert=True)

        # 插入历史数据
        self.mongo_client.insert_one("historical_data", data_dict)

        # 更新统计信息
        self.mongo_client.update_one(
            "statistics",
            {"sensor_id": sensor_data.sensor_id},
            {
                "$set": {
                    "sensor_id": sensor_data.sensor_id,
                    "sensor_type": sensor_data.sensor_type.value,
                    "last_value": sensor_data.value,
                    "last_updated": sensor_data.timestamp,
                    "unit": sensor_data._get_unit()
                },
                "$inc": {"read_count": 1},
                "$max": {"max_value": sensor_data.value},
                "$min": {"min_value": sensor_data.value},
                "$setOnInsert": {"first_read": sensor_data.timestamp}
            },
            upsert=True
        )

    def start(self):
        """启动系统"""
        logger.info("启动集成传感器系统...")
        self.sensor_manager.start_all()

    def stop(self):
        """停止系统"""
        logger.info("停止集成传感器系统...")
        self.sensor_manager.stop_all()

        # 关闭数据库连接
        if self.redis_client:
            self.redis_client.close()
        if self.mongo_client:
            self.mongo_client.close()

    def get_system_stats(self) -> dict:
        """获取系统统计信息"""
        sensor_stats = self.sensor_manager.get_stats()

        # 获取Redis统计
        redis_stats = {}
        if self.redis_client:
            try:
                redis_info = self.redis_client.get_info()
                redis_stats = {
                    "connected": True,
                    "used_memory": redis_info.get("used_memory_human", "N/A"),
                    "connected_clients": redis_info.get("connected_clients", 0)
                }
            except:
                redis_stats = {"connected": False}

        # 获取MongoDB统计
        mongo_stats = {}
        if self.mongo_client:
            try:
                mongo_stats["connected"] = True
                # 可以添加更多统计信息
            except:
                mongo_stats = {"connected": False}

        return {
            "sensor_stats": sensor_stats,
            "data_stats": self.stats.copy(),
            "redis_stats": redis_stats,
            "mongo_stats": mongo_stats
        }

    def export_data(self, start_time: datetime = None, end_time: datetime = None,
                   sensor_type: SensorType = None, format: str = "json") -> str:
        """导出数据"""
        # 实现数据导出功能
        # 这里可以扩展为支持CSV、Excel等格式
        pass


def create_full_system_config() -> List[IOModuleConfig]:
    """创建完整的系统配置"""
    configs = []

    # 根据中控机系统分析文档创建配置
    # 温度传感器模块 - 9个模块（100个温度传感器）
    base_temp_ip = "192.168.0.101"
    temp_sensor_count = 0

    for module_idx in range(9):
        sensors = []
        sensors_in_module = min(12, 100 - temp_sensor_count)  # 每个模块最多12路

        for i in range(sensors_in_module):
            temp_sensor_count += 1
            sensors.append(SensorConfig(
                sensor_id=f"temp_{temp_sensor_count:03d}",
                sensor_type=SensorType.TEMPERATURE,
                slave_addr=1,
                start_reg=i,
                reg_count=1,
                unit="°C"
            ))

        configs.append(IOModuleConfig(
            module_id=f"temp_module_{module_idx+1:02d}",
            ip=f"{base_temp_ip[:-1]}{module_idx+1}",  # 192.168.0.101-109
            port=8234,
            sensors=sensors,
            read_interval=1.0
        ))

    # 风速传感器模块 - 7个模块（100个风速传感器）
    base_wind_ip = "192.168.0.110"
    wind_sensor_count = 0

    for module_idx in range(7):
        sensors = []
        sensors_in_module = min(16, 100 - wind_sensor_count)  # 每个模块最多16路

        for i in range(sensors_in_module):
            wind_sensor_count += 1
            sensors.append(SensorConfig(
                sensor_id=f"wind_{wind_sensor_count:03d}",
                sensor_type=SensorType.WIND_SPEED,
                slave_addr=1,
                start_reg=i,
                reg_count=1,
                unit="m/s"
            ))

        configs.append(IOModuleConfig(
            module_id=f"wind_module_{module_idx+1:02d}",
            ip=f"{base_wind_ip[:-1]}{module_idx}",  # 192.168.0.110-116
            port=8234,
            sensors=sensors,
            read_interval=0.5
        ))

    # 压力传感器模块 - 1个模块
    pressure_sensors = [
        SensorConfig(
            sensor_id="pressure_001",
            sensor_type=SensorType.PRESSURE,
            slave_addr=1,
            start_reg=0,
            reg_count=1,
            unit="kPa"
        ),
        SensorConfig(
            sensor_id="pressure_temp_001",  # 压力传感器中的温度
            sensor_type=SensorType.TEMPERATURE,
            slave_addr=1,
            start_reg=1,
            reg_count=1,
            unit="°C"
        )
    ]

    configs.append(IOModuleConfig(
        module_id="pressure_module_01",
        ip="192.168.0.117",
        port=8234,
        sensors=pressure_sensors,
        read_interval=1.0
    ))

    # 湿度传感器模块 - 4个模块（4个湿度传感器，每个带温度）
    for i in range(4):
        humidity_sensors = [
            SensorConfig(
                sensor_id=f"humidity_{i+1:03d}",
                sensor_type=SensorType.HUMIDITY,
                slave_addr=1,
                start_reg=i*2,
                reg_count=1,
                unit="%RH"
            ),
            SensorConfig(
                sensor_id=f"humidity_temp_{i+1:03d}",
                sensor_type=SensorType.TEMPERATURE,
                slave_addr=1,
                start_reg=i*2+1,
                reg_count=1,
                unit="°C"
            )
        ]

        configs.append(IOModuleConfig(
            module_id=f"humidity_module_{i+1:02d}",
            ip=f"192.168.0.{118+i}",
            port=8234,
            sensors=humidity_sensors,
            read_interval=2.0
        ))

    return configs


def main():
    """主程序"""
    print("="*80)
    print("🚀 启动集成传感器管理系统（支持Redis+MongoDB）")
    print("="*80)

    # Redis配置
    redis_config = {
        "host": "localhost",
        "port": 6379,
        "db": 0,
        "decode_responses": True
    }

    # MongoDB配置
    mongo_config = {
        "host": "localhost",
        "port": 27017,
        "db_name": "sensor_database",
        "collection_name": "sensor_data"
    }

    # 创建集成系统
    system = IntegratedSensorSystem(redis_config, mongo_config)

    # 添加所有模块配置
    configs = create_full_system_config()
    for config in configs:
        system.add_module_config(config)
        print(f"✅ 添加模块 {config.module_id} ({config.ip}) "
              f"- {len(config.sensors)} 个传感器")

    print("\n📊 系统配置摘要:")
    print(f"  总模块数: {len(configs)}")
    print(f"  温度传感器: 100个")
    print(f"  风速传感器: 100个")
    print(f"  压力传感器: 1个")
    print(f"  湿度传感器: 4个")
    print(f"  总计: 205个传感器")

    # 启动系统
    system.start()

    print("\n" + "="*80)
    print("✅ 系统已启动，按 Ctrl+C 停止")
    print("="*80)

    # 主循环
    last_stats_time = time.time()
    stats_interval = 30  # 30秒打印一次统计

    try:
        while True:
            time.sleep(1)

            # 定期打印统计信息
            if time.time() - last_stats_time >= stats_interval:
                stats = system.get_system_stats()
                print("\n" + "-"*80)
                print("📊 系统统计信息")
                print("-"*80)

                # 数据统计
                data_stats = stats["data_stats"]
                print(f"总读取次数: {data_stats['total_read']}")
                print(f"Redis保存: {data_stats['redis_saved']}")
                print(f"MongoDB保存: {data_stats['mongo_saved']}")
                print(f"错误次数: {data_stats['errors']}")

                # 模块统计
                sensor_stats = stats["sensor_stats"]
                print(f"\n运行中的模块: {sensor_stats['total_modules']}")
                for module_id, module_stats in sensor_stats["module_stats"].items():
                    if module_stats["thread_alive"]:
                        success_rate = (module_stats["success_count"] /
                                      max(module_stats["read_count"], 1)) * 100
                        print(f"  {module_id}: 成功率 {success_rate:.1f}%")

                # 数据库状态
                if stats["redis_stats"].get("connected"):
                    print(f"\nRedis: 已连接 | 内存: {stats['redis_stats'].get('used_memory', 'N/A')}")
                if stats["mongo_stats"].get("connected"):
                    print(f"MongoDB: 已连接")

                print("-"*80)
                last_stats_time = time.time()

    except KeyboardInterrupt:
        print("\n⚠️  用户中断，正在停止系统...")
    finally:
        # 停止系统
        system.stop()
        print("\n✅ 系统已停止")

        # 最终统计
        final_stats = system.get_system_stats()
        data_stats = final_stats["data_stats"]
        print("\n" + "="*80)
        print("📋 最终统计报告")
        print("="*80)
        print(f"总读取次数: {data_stats['total_read']}")
        print(f"Redis保存: {data_stats['redis_saved']}")
        print(f"MongoDB保存: {data_stats['mongo_saved']}")
        print(f"错误次数: {data_stats['errors']}")

        if data_stats["total_read"] > 0:
            print(f"\n成功率: {((data_stats['redis_saved'] + data_stats['mongo_saved']) / (2 * data_stats['total_read'])) * 100:.1f}%")

        print("="*80)


if __name__ == "__main__":
    main()