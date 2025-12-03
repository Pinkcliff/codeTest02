import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from redis_client import RedisManager


class RedisDataViewer:
    """Redis数据查看和查询工具"""

    def __init__(self, redis_manager: RedisManager):
        self.redis = redis_manager

    def show_realtime_data(self) -> bool:
        """显示实时温度数据"""
        try:
            realtime_key = "temperature:realtime"
            data = self.redis.get_hash(realtime_key)

            if not data:
                print("📊 没有实时温度数据")
                return False

            print("🌡️  实时温度数据:")
            print("-" * 50)
            print(f"🕐 时间戳: {data.get('timestamp', 'N/A')}")
            print(f"📡 通道数量: {data.get('channel_count', 'N/A')}")
            print()

            # 显示各通道温度
            print("📋 各通道温度:")
            channel_count = int(data.get('channel_count', 0))
            for i in range(1, channel_count + 1):
                temp_key = f"channel_{i:02d}"
                raw_key = f"channel_{i:02d}_raw"
                temp = data.get(temp_key, 'N/A')
                raw = data.get(raw_key, 'N/A')
                print(f"   通道 {i:02d}: {temp}℃ (原始值: {raw})")

            return True

        except Exception as e:
            print(f"❌ 获取实时数据失败: {e}")
            return False

    def show_latest_history(self, count: int = 10) -> bool:
        """显示最新的历史记录"""
        try:
            history_key = "temperature:history"
            records_json = self.redis.redis_client.lrange(history_key, 0, count - 1)

            if not records_json:
                print("📊 没有历史温度数据")
                return False

            print(f"📜 最新 {len(records_json)} 条历史记录:")
            print("-" * 50)

            for i, record_json in enumerate(records_json, 1):
                try:
                    record = json.loads(record_json)
                    timestamp = record.get('timestamp', 'N/A')
                    temperatures = record.get('temperatures', [])

                    print(f"🔢 记录 {i}: {timestamp}")
                    if temperatures:
                        for j, temp in enumerate(temperatures):
                            print(f"     通道 {j+1:02d}: {temp}℃")
                    print()

                except json.JSONDecodeError:
                    print(f"🔢 记录 {i}: 数据格式错误")
                    print()

            return True

        except Exception as e:
            print(f"❌ 获取历史数据失败: {e}")
            return False

    def show_statistics(self) -> bool:
        """显示统计信息"""
        try:
            stats_key = "temperature:statistics"
            stats = self.redis.get_hash(stats_key)

            if not stats:
                print("📊 没有统计数据")
                return False

            print("📈 温度统计信息:")
            print("-" * 50)
            print(f"🕐 最后更新: {stats.get('last_update', 'N/A')}")
            print(f"📡 通道数量: {stats.get('channel_count', 'N/A')}")
            print(f"🌡️  最低温度: {stats.get('min_temperature', 'N/A')}℃ (通道 {stats.get('channel_min', 'N/A')})")
            print(f"🌡️  最高温度: {stats.get('max_temperature', 'N/A')}℃ (通道 {stats.get('channel_max', 'N/A')})")
            print(f"🌡️  平均温度: {stats.get('avg_temperature', 'N/A')}℃")
            print()

            # 显示各通道当前温度
            channel_count = int(stats.get('channel_count', 0))
            print("📋 各通道当前温度:")
            for i in range(1, channel_count + 1):
                temp_key = f"channel_{i:02d}"
                temp = stats.get(temp_key, 'N/A')
                print(f"   通道 {i:02d}: {temp}℃")

            return True

        except Exception as e:
            print(f"❌ 获取统计信息失败: {e}")
            return False

    def show_channel_timeseries(self, channel: int, count: int = 20) -> bool:
        """显示指定通道的时间序列数据"""
        try:
            channel_key = f"temperature:timeseries:channel_{channel:02d}"
            # 获取最新的数据点（分数从高到低排序）
            data_points = self.redis.redis_client.zrevrange(
                channel_key, 0, count - 1, withscores=True
            )

            if not data_points:
                print(f"📊 通道 {channel:02d} 没有时间序列数据")
                return False

            print(f"📈 通道 {channel:02d} 时间序列数据 (最新 {len(data_points)} 个数据点):")
            print("-" * 50)

            for i, (temp_value, timestamp_score) in enumerate(data_points, 1):
                # 将时间戳分数转换为可读时间
                timestamp = datetime.fromtimestamp(timestamp_score).strftime("%H:%M:%S")
                print(f"   {i:2d}. {timestamp}: {temp_value}℃")

            return True

        except Exception as e:
            print(f"❌ 获取时间序列数据失败: {e}")
            return False

    def show_redis_info(self) -> bool:
        """显示Redis数据库信息"""
        try:
            # 获取数据库基本信息
            info = self.redis.redis_client.info()

            print("💾 Redis数据库信息:")
            print("-" * 50)
            print(f"🔧 Redis版本: {info.get('redis_version', 'Unknown')}")
            print(f"⏱️  运行时间: {info.get('uptime_in_seconds', 0)} 秒")
            print(f"🔌 连接客户端: {info.get('connected_clients', 0)}")
            print(f"💾 内存使用: {info.get('used_memory_human', 'Unknown')}")
            print(f"🗄️  数据库大小: {info.get('db0', {}).get('keys', 0)} 个键")

            # 获取温度数据相关的键
            temperature_keys = [
                "temperature:realtime",
                "temperature:history",
                "temperature:statistics"
            ]

            # 添加通道时间序列键
            for i in range(1, 13):  # 12个通道
                temperature_keys.append(f"temperature:timeseries:channel_{i:02d}")

            print("\n🔑 温度数据键信息:")
            for key in temperature_keys:
                if self.redis.exists_key(key):
                    key_type = self.redis.redis_client.type(key)
                    if hasattr(key_type, 'decode'):
                        key_type = key_type.decode()
                    ttl = self.redis.redis_client.ttl(key)
                    ttl_str = f"TTL: {ttl}秒" if ttl > 0 else "永不过期"

                    # 获取数据大小信息
                    if key_type == 'hash':
                        size = self.redis.redis_client.hlen(key)
                        size_str = f"{size} 个字段"
                    elif key_type == 'list':
                        size = self.redis.redis_client.llen(key)
                        size_str = f"{size} 条记录"
                    elif key_type == 'zset':
                        size = self.redis.redis_client.zcard(key)
                        size_str = f"{size} 个数据点"
                    else:
                        size_str = "N/A"

                    print(f"   {key} (类型: {key_type}, {ttl_str}, 大小: {size_str})")

            return True

        except Exception as e:
            print(f"❌ 获取Redis信息失败: {e}")
            return False

    def export_data_to_json(self, output_file: str = "temperature_data_export.json") -> bool:
        """导出温度数据到JSON文件"""
        try:
            export_data = {
                "export_time": datetime.now().isoformat(),
                "realtime_data": {},
                "history_data": [],
                "statistics": {},
                "timeseries_data": {}
            }

            # 导出实时数据
            realtime_key = "temperature:realtime"
            export_data["realtime_data"] = self.redis.get_hash(realtime_key)

            # 导出历史数据
            history_key = "temperature:history"
            history_records = self.redis.redis_client.lrange(history_key, 0, -1)
            for record_json in history_records:
                try:
                    record = json.loads(record_json)
                    export_data["history_data"].append(record)
                except json.JSONDecodeError:
                    continue

            # 导出统计信息
            stats_key = "temperature:statistics"
            export_data["statistics"] = self.redis.get_hash(stats_key)

            # 导出时间序列数据（每个通道最新50个数据点）
            for i in range(1, 13):
                channel_key = f"temperature:timeseries:channel_{i:02d}"
                data_points = self.redis.redis_client.zrevrange(
                    channel_key, 0, 49, withscores=True
                )
                if data_points:
                    export_data["timeseries_data"][f"channel_{i:02d}"] = [
                        {
                            "temperature": float(temp_value),
                            "timestamp": datetime.fromtimestamp(timestamp_score).isoformat()
                        }
                        for temp_value, timestamp_score in data_points
                    ]

            # 写入文件
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, ensure_ascii=False, indent=2)

            print(f"✅ 数据已导出到文件: {output_file}")
            print(f"   📊 实时数据: {len(export_data['realtime_data'])} 个字段")
            print(f"   📜 历史数据: {len(export_data['history_data'])} 条记录")
            print(f"   📈 统计信息: {len(export_data['statistics'])} 个字段")
            print(f"   ⏱️  时间序列数据: {len(export_data['timeseries_data'])} 个通道")

            return True

        except Exception as e:
            print(f"❌ 导出数据失败: {e}")
            return False


def main():
    """主函数 - 数据查看演示"""
    print("=" * 60)
    print("🔍 Redis温度数据查看器")
    print("=" * 60)

    # 连接Redis
    redis_manager = RedisManager(
        host='localhost',
        port=6379,
        password=None,
        db=0,
        decode_responses=True
    )

    if not redis_manager.connect():
        print("❌ 无法连接到Redis，程序退出")
        return

    viewer = RedisDataViewer(redis_manager)

    try:
        while True:
            print("\n" + "="*60)
            print("📋 请选择要查看的数据:")
            print("1. 🌡️  实时温度数据")
            print("2. 📜 最新历史记录")
            print("3. 📈 统计信息")
            print("4. ⏱️  时间序列数据")
            print("5. 💾 Redis数据库信息")
            print("6. 📤 导出数据到JSON文件")
            print("0. 🚪 退出程序")
            print("="*60)

            choice = input("请输入选项 (0-6): ").strip()

            if choice == '0':
                print("👋 再见！")
                break
            elif choice == '1':
                print("\n" + "="*60)
                viewer.show_realtime_data()
            elif choice == '2':
                count = input("显示多少条记录 (默认10): ").strip()
                count = int(count) if count.isdigit() else 10
                print("\n" + "="*60)
                viewer.show_latest_history(count)
            elif choice == '3':
                print("\n" + "="*60)
                viewer.show_statistics()
            elif choice == '4':
                channel = input("查看哪个通道 (1-12): ").strip()
                if channel.isdigit() and 1 <= int(channel) <= 12:
                    count = input("显示多少个数据点 (默认20): ").strip()
                    count = int(count) if count.isdigit() else 20
                    print("\n" + "="*60)
                    viewer.show_channel_timeseries(int(channel), count)
                else:
                    print("❌ 请输入1-12之间的数字")
            elif choice == '5':
                print("\n" + "="*60)
                viewer.show_redis_info()
            elif choice == '6':
                filename = input("导出文件名 (默认 temperature_data_export.json): ").strip()
                filename = filename if filename else "temperature_data_export.json"
                print("\n" + "="*60)
                viewer.export_data_to_json(filename)
            else:
                print("❌ 无效选项，请重新输入")

            print("\n按Enter键继续...")
            input()

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
    finally:
        redis_manager.disconnect()
        print("👋 程序结束")


if __name__ == "__main__":
    main()