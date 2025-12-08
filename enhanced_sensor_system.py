import socket
import time
import threading
from typing import List, Dict, Optional, Tuple, Callable
from dataclasses import dataclass
from enum import Enum
import json
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SensorType(Enum):
    """传感器类型枚举"""
    TEMPERATURE = "temperature"
    WIND_SPEED = "wind_speed"
    PRESSURE = "pressure"
    HUMIDITY = "humidity"


@dataclass
class SensorConfig:
    """传感器配置"""
    sensor_id: str
    sensor_type: SensorType
    slave_addr: int
    start_reg: int
    reg_count: int
    func_code: int = 0x04
    conversion_formula: Optional[str] = None  # 转换公式（JSON格式）
    unit: str = ""


@dataclass
class IOModuleConfig:
    """I/O模块配置"""
    module_id: str
    ip: str
    port: int
    sensors: List[SensorConfig]
    read_interval: float = 1.0
    timeout: float = 5.0


class EnhancedSensorData:
    """增强的传感器数据结构"""
    def __init__(self, sensor_id: str, sensor_type: SensorType, value: float,
                 raw_value: int, timestamp: datetime, quality: str = "good"):
        self.sensor_id = sensor_id
        self.sensor_type = sensor_type
        self.value = value
        self.raw_value = raw_value
        self.timestamp = timestamp
        self.quality = quality  # good, bad, uncertain

    def to_dict(self) -> dict:
        """转换为字典格式"""
        return {
            "sensor_id": self.sensor_id,
            "sensor_type": self.sensor_type.value,
            "value": self.value,
            "raw_value": self.raw_value,
            "timestamp": self.timestamp.isoformat(),
            "quality": self.quality,
            "unit": self._get_unit()
        }

    def _get_unit(self) -> str:
        """获取单位"""
        units = {
            SensorType.TEMPERATURE: "°C",
            SensorType.WIND_SPEED: "m/s",
            SensorType.PRESSURE: "kPa",
            SensorType.HUMIDITY: "%RH"
        }
        return units.get(self.sensor_type, "")


class ModbusRTUClient:
    """增强的Modbus RTU客户端"""

    @staticmethod
    def modbus_crc(data: List[int]) -> List[int]:
        """计算Modbus CRC校验"""
        crc = 0xFFFF
        for byte in data:
            crc ^= byte
            for _ in range(8):
                if crc & 0x0001:
                    crc >>= 1
                    crc ^= 0xA001
                else:
                    crc >>= 1
        return [crc & 0xFF, (crc >> 8) & 0xFF]

    @staticmethod
    def build_rtu_request(slave_addr: int, start_reg: int, reg_count: int, func_code: int = 0x04) -> bytes:
        """构建Modbus RTU请求帧"""
        frame = [
            slave_addr,
            func_code,
            (start_reg >> 8) & 0xFF,
            start_reg & 0xFF,
            (reg_count >> 8) & 0xFF,
            reg_count & 0xFF
        ]
        crc = ModbusRTUClient.modbus_crc(frame)
        frame.extend(crc)
        return bytearray(frame)

    @staticmethod
    def parse_rtu_response(response_bytes: bytes) -> dict:
        """解析Modbus RTU响应帧"""
        response = list(response_bytes)
        if len(response) < 4:
            return {"error": "响应帧过短"}

        slave_addr = response[0]
        func_code = response[1]
        data = response[2:-2]
        received_crc = response[-2:]

        calculated_crc = ModbusRTUClient.modbus_crc(response[:-2])
        if received_crc != calculated_crc:
            return {"error": f"CRC校验失败"}

        if func_code in [0x03, 0x04]:
            if len(data) < 1:
                return {"error": f"功能码{func_code:02X}响应数据为空"}
            byte_count = data[0]
            registers = []
            for i in range(1, len(data), 2):
                if i + 1 > len(data):
                    break
                reg_value = (data[i] << 8) | data[i + 1]
                registers.append(reg_value)
            return {
                "slave_addr": slave_addr,
                "func_code": func_code,
                "registers": registers,
                "valid": True
            }
        else:
            return {"error": f"不支持的功能码：0x{func_code:02X}"}


class IOModuleReader(threading.Thread):
    """I/O模块读取线程"""

    def __init__(self, config: IOModuleConfig, data_callback: Callable):
        super().__init__(name=f"IOModule-{config.module_id}")
        self.config = config
        self.data_callback = data_callback
        self.running = False
        self.sock: Optional[socket.socket] = None
        self.last_values = {}  # 记录上次的值，用于检测变化
        self.stats = {
            "read_count": 0,
            "success_count": 0,
            "fail_count": 0,
            "last_read_time": None
        }

    def connect(self) -> bool:
        """连接到I/O模块"""
        try:
            if self.sock:
                self.sock.close()

            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.config.timeout)
            self.sock.connect((self.config.ip, self.config.port))
            logger.info(f"成功连接到模块 {self.config.module_id} ({self.config.ip}:{self.config.port})")
            return True
        except Exception as e:
            logger.error(f"连接模块 {self.config.module_id} 失败: {e}")
            return False

    def disconnect(self):
        """断开连接"""
        if self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None

    def read_sensor_data(self, sensor_config: SensorConfig) -> Optional[EnhancedSensorData]:
        """读取单个传感器数据"""
        try:
            # 构建请求
            request = ModbusRTUClient.build_rtu_request(
                slave_addr=sensor_config.slave_addr,
                start_reg=sensor_config.start_reg,
                reg_count=sensor_config.reg_count,
                func_code=sensor_config.func_code
            )

            # 发送请求
            self.sock.sendall(request)

            # 接收响应
            response_bytes = b""
            start_time = time.time()

            while True:
                chunk = self.sock.recv(1024)
                if chunk:
                    response_bytes += chunk
                    # 检查完整帧
                    if len(response_bytes) >= 5:
                        data_len = response_bytes[2]
                        full_frame_len = 1 + 1 + 1 + data_len + 2
                        if len(response_bytes) >= full_frame_len:
                            break

                # 超时判断
                if time.time() - start_time > self.config.timeout:
                    raise socket.timeout("接收超时")
                time.sleep(0.01)

            # 解析响应
            parsed_data = ModbusRTUClient.parse_rtu_response(response_bytes)
            if "error" in parsed_data:
                logger.error(f"解析响应失败: {parsed_data['error']}")
                return None

            # 提取原始值
            registers = parsed_data["registers"]
            if len(registers) < sensor_config.reg_count:
                logger.error(f"数据不足: 实际{len(registers)}个，期望{sensor_config.reg_count}个")
                return None

            raw_value = registers[0]

            # 应用转换公式
            converted_value = self.apply_conversion_formula(raw_value, sensor_config)

            # 创建传感器数据对象
            sensor_data = EnhancedSensorData(
                sensor_id=sensor_config.sensor_id,
                sensor_type=sensor_config.sensor_type,
                value=converted_value,
                raw_value=raw_value,
                timestamp=datetime.now()
            )

            return sensor_data

        except Exception as e:
            logger.error(f"读取传感器 {sensor_config.sensor_id} 数据失败: {e}")
            return None

    def apply_conversion_formula(self, raw_value: int, sensor_config: SensorConfig) -> float:
        """应用转换公式"""
        # 默认转换公式
        if sensor_config.conversion_formula:
            try:
                # 使用JSON格式的公式（可扩展）
                formula = json.loads(sensor_config.conversion_formula)
                if formula.get("type") == "linear":
                    a = formula.get("a", 1.0)
                    b = formula.get("b", 0.0)
                    return a * raw_value + b
            except:
                pass

        # 内置默认公式
        if sensor_config.sensor_type == SensorType.TEMPERATURE:
            if sensor_config.sensor_id.startswith("tem_"):  # RTC温度模块
                return raw_value / 10.0
            else:  # 普通温度传感器
                return ((raw_value / 249) - 4) * 7.5 - 40
        elif sensor_config.sensor_type == SensorType.PRESSURE:
            return ((raw_value / 249) - 4) * 7.5
        elif sensor_config.sensor_type == SensorType.WIND_SPEED:
            return raw_value * 0.1  # 假设转换公式
        elif sensor_config.sensor_type == SensorType.HUMIDITY:
            return raw_value * 0.1  # 假设转换公式
        else:
            return float(raw_value)

    def run(self):
        """线程主循环"""
        self.running = True

        # 首次连接
        if not self.connect():
            logger.error(f"模块 {self.config.module_id} 初始连接失败，线程退出")
            return

        logger.info(f"模块 {self.config.module_id} 开始数据读取")

        while self.running:
            self.stats["read_count"] += 1
            read_success = True

            try:
                # 检查连接
                if not self.sock:
                    if not self.connect():
                        read_success = False

                if read_success:
                    # 读取所有传感器
                    for sensor_config in self.config.sensors:
                        sensor_data = self.read_sensor_data(sensor_config)
                        if sensor_data:
                            # 检测数据变化
                            last_value = self.last_values.get(sensor_config.sensor_id)
                            if last_value is None or abs(sensor_data.value - last_value) > 0.1:
                                # 数据发生变化，调用回调
                                self.data_callback(sensor_data)
                                self.last_values[sensor_config.sensor_id] = sensor_data.value
                        else:
                            read_success = False
                            break

                if read_success:
                    self.stats["success_count"] += 1
                else:
                    self.stats["fail_count"] += 1

            except Exception as e:
                logger.error(f"模块 {self.config.module_id} 读取异常: {e}")
                self.stats["fail_count"] += 1
                self.disconnect()
                read_success = False

            self.stats["last_read_time"] = datetime.now()

            # 等待下次读取
            time.sleep(self.config.read_interval)

        # 清理
        self.disconnect()
        logger.info(f"模块 {self.config.module_id} 线程结束")

    def stop(self):
        """停止线程"""
        self.running = False
        if self.is_alive():
            self.join(timeout=5)

    def get_stats(self) -> dict:
        """获取统计信息"""
        return {
            "module_id": self.config.module_id,
            "thread_alive": self.is_alive(),
            "connected": self.sock is not None,
            **self.stats
        }


class MultiIOModuleManager:
    """多I/O模块管理器"""

    def __init__(self):
        self.modules = {}  # module_id -> IOModuleReader
        self.all_data = []  # 存储所有传感器数据
        self.data_callbacks = []  # 数据回调列表
        self.lock = threading.Lock()

    def add_module(self, config: IOModuleConfig):
        """添加I/O模块"""
        if config.module_id in self.modules:
            logger.warning(f"模块 {config.module_id} 已存在，将被替换")
            self.remove_module(config.module_id)

        # 创建模块读取器
        reader = IOModuleReader(config, self.on_data_received)
        self.modules[config.module_id] = reader

        logger.info(f"添加模块 {config.module_id} ({config.ip}:{config.config.port})，"
                   f"包含 {len(config.sensors)} 个传感器")

    def remove_module(self, module_id: str):
        """移除I/O模块"""
        if module_id in self.modules:
            reader = self.modules[module_id]
            reader.stop()
            del self.modules[module_id]
            logger.info(f"移除模块 {module_id}")

    def start_all(self):
        """启动所有模块"""
        for module_id, reader in self.modules.items():
            if not reader.is_alive():
                reader.start()
                logger.info(f"启动模块 {module_id}")

    def stop_all(self):
        """停止所有模块"""
        for module_id, reader in self.modules.items():
            reader.stop()
            logger.info(f"停止模块 {module_id}")

    def on_data_received(self, sensor_data: EnhancedSensorData):
        """数据接收回调"""
        with self.lock:
            self.all_data.append(sensor_data)
            # 保持最近1000条记录
            if len(self.all_data) > 1000:
                self.all_data = self.all_data[-1000:]

        # 调用所有注册的回调
        for callback in self.data_callbacks:
            try:
                callback(sensor_data)
            except Exception as e:
                logger.error(f"数据回调执行失败: {e}")

    def add_data_callback(self, callback: Callable[[EnhancedSensorData], None]):
        """添加数据回调函数"""
        self.data_callbacks.append(callback)

    def get_latest_data(self, sensor_id: str = None,
                       sensor_type: SensorType = None) -> List[EnhancedSensorData]:
        """获取最新数据"""
        with self.lock:
            data = self.all_data.copy()

        # 过滤
        if sensor_id:
            data = [d for d in data if d.sensor_id == sensor_id]
        if sensor_type:
            data = [d for d in data if d.sensor_type == sensor_type]

        # 按时间排序，返回最新的
        data.sort(key=lambda x: x.timestamp, reverse=True)
        return data

    def get_stats(self) -> dict:
        """获取所有模块统计信息"""
        return {
            "total_modules": len(self.modules),
            "module_stats": {mid: reader.get_stats()
                           for mid, reader in self.modules.items()},
            "total_data_points": len(self.all_data)
        }


# 示例配置创建函数
def create_default_config() -> List[IOModuleConfig]:
    """创建默认配置（示例）"""
    configs = []

    # 温度传感器模块配置（12路RTC）
    temp_sensors = []
    for i in range(12):
        temp_sensors.append(SensorConfig(
            sensor_id=f"tem_ch{i+1:02d}",
            sensor_type=SensorType.TEMPERATURE,
            slave_addr=1,
            start_reg=i,
            reg_count=1,
            unit="°C"
        ))

    configs.append(IOModuleConfig(
        module_id="temp_module_01",
        ip="192.168.0.101",
        port=8234,
        sensors=temp_sensors,
        read_interval=1.0
    ))

    # 压力传感器配置
    pressure_sensors = [
        SensorConfig(
            sensor_id="pressure_01",
            sensor_type=SensorType.PRESSURE,
            slave_addr=1,
            start_reg=0,
            reg_count=1,
            unit="kPa"
        )
    ]

    configs.append(IOModuleConfig(
        module_id="pressure_module_01",
        ip="192.168.0.102",
        port=8234,
        sensors=pressure_sensors,
        read_interval=1.0
    ))

    # 风速传感器配置（示例）
    wind_sensors = []
    for i in range(16):  # 假设一个模块支持16个风速传感器
        wind_sensors.append(SensorConfig(
            sensor_id=f"wind_{i+1:03d}",
            sensor_type=SensorType.WIND_SPEED,
            slave_addr=1,
            start_reg=i,
            reg_count=1,
            unit="m/s"
        ))

    configs.append(IOModuleConfig(
        module_id="wind_module_01",
        ip="192.168.0.103",
        port=8234,
        sensors=wind_sensors,
        read_interval=0.5  # 风速变化可能更快
    ))

    # 湿度传感器配置
    humidity_sensors = []
    for i in range(4):
        humidity_sensors.append(SensorConfig(
            sensor_id=f"humidity_{i+1:02d}",
            sensor_type=SensorType.HUMIDITY,
            slave_addr=1,
            start_reg=i*2,  # 假设温度和湿度交替存储
            reg_count=1,
            unit="%RH"
        ))

    configs.append(IOModuleConfig(
        module_id="humidity_module_01",
        ip="192.168.0.104",
        port=8234,
        sensors=humidity_sensors,
        read_interval=2.0
    ))

    return configs


# 主程序示例
def main():
    """主程序示例"""
    print("="*80)
    print("🚀 启动增强版多I/O模块传感器管理系统")
    print("="*80)

    # 创建管理器
    manager = MultiIOModuleManager()

    # 添加数据变化打印回调
    def print_data_change(sensor_data: EnhancedSensorData):
        """打印数据变化"""
        print(f"[{sensor_data.timestamp.strftime('%H:%M:%S')}] "
              f"📡 {sensor_data.sensor_id} "
              f"({sensor_data.sensor_type.value}): "
              f"{sensor_data.value:.2f}{sensor_data._get_unit()}")

    manager.add_data_callback(print_data_change)

    # 创建并添加配置
    configs = create_default_config()
    for config in configs:
        manager.add_module(config)

    # 启动所有模块
    manager.start_all()

    try:
        # 主循环
        while True:
            time.sleep(10)

            # 打印统计信息
            stats = manager.get_stats()
            print("\n" + "-"*60)
            print("📊 系统统计信息")
            print("-"*60)
            print(f"总模块数: {stats['total_modules']}")
            print(f"总数据点: {stats['total_data_points']}")

            for module_id, module_stats in stats['module_stats'].items():
                status = "🟢运行中" if module_stats['thread_alive'] else "🔴已停止"
                success_rate = (module_stats['success_count'] /
                              max(module_stats['read_count'], 1)) * 100
                print(f"\n{module_id}: {status}")
                print(f"  读取次数: {module_stats['read_count']} "
                      f"| 成功: {module_stats['success_count']} "
                      f"| 失败: {module_stats['fail_count']} "
                      f"| 成功率: {success_rate:.1f}%")

    except KeyboardInterrupt:
        print("\n⚠️  用户中断，正在停止程序...")
    finally:
        # 停止所有模块
        manager.stop_all()
        print("✅ 所有模块已停止")

    print("="*80)


if __name__ == "__main__":
    main()