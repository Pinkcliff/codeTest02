import socket
import time
import json
from typing import List, Dict, Optional
from datetime import datetime
from redis_client import RedisManager


# --------------------------
# 核心工具函数：Modbus RTU帧处理
# --------------------------
def modbus_crc(data: List[int]) -> List[int]:
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


def build_rtu_request(slave_addr: int, start_reg: int, reg_count: int, func_code: int = 0x04) -> bytes:
    frame = [
        slave_addr,
        func_code,
        (start_reg >> 8) & 0xFF,
        start_reg & 0xFF,
        (reg_count >> 8) & 0xFF,
        reg_count & 0xFF
    ]
    crc = modbus_crc(frame)
    frame.extend(crc)
    return bytearray(frame)


def parse_rtu_response(response_bytes: bytes) -> Dict:
    response = list(response_bytes)
    if len(response) < 4:
        return {"error": "响应帧过短"}

    slave_addr = response[0]
    func_code = response[1]
    data = response[2:-2]
    received_crc = response[-2:]

    calculated_crc = modbus_crc(response[:-2])
    if received_crc != calculated_crc:
        return {"error": f"CRC校验失败（接收: {received_crc}，计算: {calculated_crc}）"}

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


# --------------------------
# Redis数据存储类
# --------------------------
class TemperatureDataStorage:
    """温度数据Redis存储管理类"""

    def __init__(self, redis_manager: RedisManager, session_prefix: str = None):
        self.redis = redis_manager
        self.session_prefix = session_prefix or datetime.now().strftime("%Y%m%d_%H%M%S")

    def save_realtime_data(self, temperatures: List[float], timestamp: str = None) -> bool:
        """保存实时温度数据到Hash结构"""
        if not timestamp:
            timestamp = datetime.now().isoformat()

        try:
            # 存储实时数据到Hash
            realtime_key = f"{self.session_prefix}:temperature:realtime"
            mapping = {
                "timestamp": timestamp,
                "channel_count": str(len(temperatures))
            }

            # 添加各通道数据
            for i, temp in enumerate(temperatures):
                mapping[f"channel_{i+1:02d}"] = f"{temp:.1f}"
                mapping[f"channel_{i+1:02d}_raw"] = f"{int(temp * 10)}"

            result = self.redis.set_hash(realtime_key, mapping)

            # 设置实时数据的过期时间（1小时）
            self.redis.redis_client.expire(realtime_key, 3600)

            return result

        except Exception as e:
            print(f"❌ 保存实时数据失败: {e}")
            return False

    def save_historical_data(self, temperatures: List[float], timestamp: str = None) -> bool:
        """保存历史温度数据到List结构"""
        if not timestamp:
            timestamp = datetime.now().isoformat()

        try:
            # 构建历史记录
            record = {
                "timestamp": timestamp,
                "temperatures": [round(temp, 1) for temp in temperatures]
            }
            record_json = json.dumps(record)

            # 保存到历史数据列表（最新数据在前）
            history_key = f"{self.session_prefix}:temperature:history"
            self.redis.redis_client.lpush(history_key, record_json)

            # 保持历史数据列表长度（最多保存1000条记录）
            self.redis.redis_client.ltrim(history_key, 0, 999)

            return True

        except Exception as e:
            print(f"❌ 保存历史数据失败: {e}")
            return False

    def save_time_series_data(self, temperatures: List[float], timestamp: str = None) -> bool:
        """保存时间序列数据到Sorted Set（按时间排序）"""
        if not timestamp:
            timestamp = datetime.now().isoformat()

        try:
            # 将时间戳转换为时间戳分数
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            score = dt.timestamp()

            # 为每个通道创建单独的时间序列
            for i, temp in enumerate(temperatures):
                channel_key = f"{self.session_prefix}:temperature:timeseries:channel_{i+1:02d}"
                # 使用温度值作为成员，时间戳作为分数
                self.redis.redis_client.zadd(channel_key, {f"{temp:.1f}": score})

                # 保持时间序列数据长度（最多保存10000个数据点）
                self.redis.redis_client.zremrangebyrank(channel_key, 0, -10001)

            return True

        except Exception as e:
            print(f"❌ 保存时间序列数据失败: {e}")
            return False

    def update_statistics(self, temperatures: List[float]) -> bool:
        """更新统计信息"""
        try:
            # 计算统计数据
            min_temp = min(temperatures)
            max_temp = max(temperatures)
            avg_temp = sum(temperatures) / len(temperatures)

            stats_key = f"{self.session_prefix}:temperature:statistics"
            stats_mapping = {
                "last_update": datetime.now().isoformat(),
                "channel_count": str(len(temperatures)),
                "min_temperature": f"{min_temp:.1f}",
                "max_temperature": f"{max_temp:.1f}",
                "avg_temperature": f"{avg_temp:.1f}",
                "channel_min": f"{temperatures.index(min_temp) + 1:02d}",
                "channel_max": f"{temperatures.index(max_temp) + 1:02d}"
            }

            # 更新各通道统计
            for i, temp in enumerate(temperatures):
                stats_mapping[f"channel_{i+1:02d}"] = f"{temp:.1f}"

            return self.redis.set_hash(stats_key, stats_mapping)

        except Exception as e:
            print(f"❌ 更新统计信息失败: {e}")
            return False

    def save_all_data(self, temperatures: List[float], timestamp: str = None) -> bool:
        """保存所有类型的数据"""
        success_count = 0
        total_operations = 4

        # 保存实时数据
        if self.save_realtime_data(temperatures, timestamp):
            success_count += 1

        # 保存历史数据
        if self.save_historical_data(temperatures, timestamp):
            success_count += 1

        # 保存时间序列数据
        if self.save_time_series_data(temperatures, timestamp):
            success_count += 1

        # 更新统计信息
        if self.update_statistics(temperatures):
            success_count += 1

        print(f"📊 数据保存完成: {success_count}/{total_operations} 项成功")
        return success_count == total_operations


# --------------------------
# 主程序：集成数据采集和Redis存储
# --------------------------
def temperature_data_collector_with_redis():
    # 设备参数
    DEVICE_IP = "192.168.1.101"
    DEVICE_PORT = 8234
    SLAVE_ADDR = 1
    FUNC_CODE = 0x04
    START_REG = 0
    REG_COUNT = 12
    READ_INTERVAL = 1
    TIMEOUT = 5
    BUFFER_SIZE = 1024
    RECONNECT_ATTEMPT = 1

    # 全局变量
    last_temperatures: List[Optional[float]] = [None] * 12
    read_count = 0
    success_count = 0
    fail_count = 0
    sock: Optional[socket.socket] = None
    start_time = time.time()

    # 颜色编码
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

    print("="*80)
    print("🚀 启动 [12路RTC温度模块] 数据采集与Redis存储系统")
    print("="*80)
    print(f"📡 设备地址: {DEVICE_IP}:{DEVICE_PORT}")
    print(f"🔌 从站地址: {SLAVE_ADDR} | 功能码: 0x{FUNC_CODE:02X}")
    print(f"📝 读取范围: 寄存器{START_REG}~{START_REG+REG_COUNT-1}（共{REG_COUNT}路温度传感器）")
    print(f"⏱️  读取间隔: {READ_INTERVAL}秒 | 超时时间: {TIMEOUT}秒")
    print(f"🔍 温度变化将以 {RED}红色{RESET} 高亮显示")
    print(f"💾 数据将自动保存到Redis数据库")
    print("⛔ 按 Ctrl+C 停止程序")
    print("="*80)

    # 初始化Redis连接
    print(f"\n📡 正在连接Redis服务器...")
    redis_manager = RedisManager(
        host='localhost',
        port=6379,
        password=None,
        db=0,
        decode_responses=True
    )

    if not redis_manager.connect():
        print(f"{RED}❌ Redis连接失败，程序退出{RESET}")
        return

    # 生成会话前缀
    session_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")

    # 初始化数据存储管理器
    storage = TemperatureDataStorage(redis_manager, session_prefix)
    print(f"{GREEN}✅ Redis连接成功，开始数据采集...{RESET}")
    print(f"{BLUE}📁 本次采集会话ID: {session_prefix}{RESET}")
    print("-"*80)

    # 连接函数
    def connect_device() -> bool:
        nonlocal sock
        try:
            if sock:
                try:
                    sock.close()
                except:
                    pass

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(TIMEOUT)
            sock.connect((DEVICE_IP, DEVICE_PORT))
            print(f"{GREEN}✅ 设备连接成功{RESET}")
            return True
        except Exception as e:
            print(f"{RED}❌ 设备连接失败: {str(e)}{RESET}")
            return False

    # 首次连接
    print(f"📞 正在连接设备 {DEVICE_IP}:{DEVICE_PORT}...")
    if not connect_device():
        print(f"{YELLOW}⚠️  设备连接失败，程序退出{RESET}")
        redis_manager.disconnect()
        return

    print("-"*80)

    # 主采集循环
    try:
        while True:
            read_count += 1
            current_time = time.strftime("%H:%M:%S", time.localtime())
            timestamp_iso = datetime.now().isoformat()
            read_start_time = time.time()
            read_success = False

            try:
                if not sock:
                    print(f"[{current_time}] {YELLOW}⚠️  无有效设备连接，尝试重连...{RESET}")
                    if not connect_device():
                        fail_count += 1
                        time.sleep(READ_INTERVAL)
                        continue

                # 发送Modbus请求
                request = build_rtu_request(
                    slave_addr=SLAVE_ADDR,
                    start_reg=START_REG,
                    reg_count=REG_COUNT,
                    func_code=FUNC_CODE
                )
                sock.sendall(request)

                # 接收响应
                response_bytes = b""
                request_start_time = time.time()

                while True:
                    chunk = sock.recv(BUFFER_SIZE)
                    if chunk:
                        response_bytes += chunk
                        if len(response_bytes) >= 5:
                            data_len = response_bytes[2]
                            full_frame_len = 1 + 1 + 1 + data_len + 2
                            if len(response_bytes) >= full_frame_len:
                                break

                    if time.time() - request_start_time > TIMEOUT:
                        raise socket.timeout(f"接收超时（{TIMEOUT}秒）")
                    time.sleep(0.01)

                # 解析响应
                parsed_data = parse_rtu_response(response_bytes)
                if "error" in parsed_data:
                    print(f"[{current_time}] ❌ 第{read_count:03d}次: 解析失败 - {parsed_data['error']}")
                    fail_count += 1
                    time.sleep(READ_INTERVAL)
                    continue

                # 提取温度数据
                registers = parsed_data["registers"]
                if len(registers) < REG_COUNT:
                    print(f"[{current_time}] ❌ 第{read_count:03d}次: 数据不足（实际{len(registers)}个，期望{REG_COUNT}个）")
                    fail_count += 1
                    time.sleep(READ_INTERVAL)
                    continue

                # 数据转换
                temperatures = []
                temp_display_strings = []

                for i in range(12):
                    temp_raw = registers[i]
                    temperature = temp_raw / 10  # RTC温度转换公式
                    temperatures.append(temperature)

                    # 高亮变化数据
                    temp_str = f"{temperature:5.1f}℃"
                    temp_raw_str = f"{temp_raw:4d}"

                    if last_temperatures[i] is not None and abs(temperature - last_temperatures[i]) > 0.1:
                        temp_str = f"{RED}{temp_str}{RESET}"
                        temp_raw_str = f"{RED}{temp_raw_str}{RESET}"

                    temp_display_strings.append(f"CH{i+1:02d}:{temp_raw_str}→{temp_str}")

                read_duration = (time.time() - read_start_time) * 1000

                # 显示采集结果
                header = f"[{current_time}] ✅ 第{read_count:03d}次 | 耗时:{read_duration:4.0f}ms | 12路温度数据:"
                print(header)

                for i in range(12):
                    print(f"    {temp_display_strings[i]}")

                # 保存数据到Redis
                storage_start_time = time.time()
                print(f"    {BLUE}💾 正在保存数据到Redis...{RESET}", end="")

                if storage.save_all_data(temperatures, timestamp_iso):
                    storage_duration = (time.time() - storage_start_time) * 1000
                    print(f" {GREEN}✅ 成功 | 耗时:{storage_duration:4.0f}ms{RESET}")
                else:
                    print(f" {RED}❌ 失败{RESET}")

                print()

                # 更新记录
                last_temperatures = temperatures.copy()
                success_count += 1
                read_success = True

            except socket.timeout as e:
                print(f"[{current_time}] ⏰ 第{read_count:03d}次: 读取超时 - {str(e)}")
                fail_count += 1
            except ConnectionResetError:
                print(f"[{current_time}] {RED}🚫 第{read_count:03d}次: 连接被设备重置{RESET}")
                reconnect_success = False
                for attempt in range(RECONNECT_ATTEMPT):
                    print(f"[{current_time}] 🔄 正在重连（{attempt+1}/{RECONNECT_ATTEMPT}）...")
                    if connect_device():
                        reconnect_success = True
                        break
                    time.sleep(2)
                if not reconnect_success:
                    print(f"[{current_time}] {RED}❌ 重连失败，程序将退出{RESET}")
                    break
                fail_count += 1
            except Exception as e:
                print(f"[{current_time}] {RED}❌ 第{read_count:03d}次: 异常 - {str(e)}（{type(e).__name__}）{RESET}")
                fail_count += 1

            time.sleep(READ_INTERVAL)

    except KeyboardInterrupt:
        print(f"\n{YELLOW}⚠️  用户中断，正在停止程序...{RESET}")
    finally:
        # 关闭设备连接
        if sock:
            try:
                sock.close()
                print(f"{GREEN}🔌 设备连接已关闭{RESET}")
            except:
                pass

        # 关闭Redis连接
        redis_manager.disconnect()

    # 最终统计报告
    total_runtime = time.time() - start_time
    success_rate = (success_count / read_count * 100) if read_count > 0 else 0.0

    print("\n" + "="*80)
    print("📋 温度采集与Redis存储结束 - 统计报告")
    print("="*80)
    print(f"🕐 总运行时间: {total_runtime:.1f} 秒")
    print(f"🔢 总读取次数: {read_count}")
    print(f"✅ 成功次数: {success_count}")
    print(f"❌ 失败次数: {fail_count}")
    print(f"📈 成功率: {success_rate:.1f}%")
    print(f"💾 数据已保存到Redis数据库")
    print("="*80)


if __name__ == "__main__":
    temperature_data_collector_with_redis()