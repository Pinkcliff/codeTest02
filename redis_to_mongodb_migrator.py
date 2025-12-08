import json
import time
from datetime import datetime
from typing import List, Dict, Optional, Any
from redis_client import RedisManager
from mongodb_client import MongoDBManager


class RedisToMongoDBMigrator:
    """Redis到MongoDB的数据迁移器"""

    def __init__(self, redis_manager: RedisManager, mongo_manager: MongoDBManager):
        self.redis = redis_manager
        self.mongo = mongo_manager
        self.migration_stats = {
            "sessions_migrated": 0,
            "realtime_records": 0,
            "historical_records": 0,
            "timeseries_points": 0,
            "statistics_records": 0,
            "errors": 0,
            "start_time": None,
            "end_time": None
        }

    def discover_sessions(self) -> List[str]:
        """发现Redis中的所有会话"""
        sessions = []
        try:
            # 查找所有包含temperature的键
            keys = self.redis.redis_client.keys("*temperature*")
            session_prefixes = set()

            for key in keys:
                # 从键名中提取会话前缀
                parts = key.split(":")
                if len(parts) >= 3 and parts[0].isdigit():
                    session_prefixes.add(parts[0])

            sessions = sorted(list(session_prefixes))
            print(f"🔍 发现 {len(sessions)} 个会话: {sessions[:5]}{'...' if len(sessions) > 5 else ''}")
            return sessions

        except Exception as e:
            print(f"❌ 发现会话失败: {e}")
            return []

    def migrate_session(self, session_prefix: str) -> bool:
        """迁移单个会话的所有数据"""
        print(f"\n📦 开始迁移会话: {session_prefix}")
        success = True

        # 迁移实时数据
        if not self.migrate_realtime_data(session_prefix):
            success = False

        # 迁移历史数据
        if not self.migrate_historical_data(session_prefix):
            success = False

        # 迁移时间序列数据
        if not self.migrate_timeseries_data(session_prefix):
            success = False

        # 迁移统计数据
        if not self.migrate_statistics_data(session_prefix):
            success = False

        if success:
            self.migration_stats["sessions_migrated"] += 1
            print(f"✅ 会话 {session_prefix} 迁移完成")
        else:
            self.migration_stats["errors"] += 1
            print(f"❌ 会话 {session_prefix} 迁移失败")

        return success

    def migrate_realtime_data(self, session_prefix: str) -> bool:
        """迁移实时数据"""
        try:
            realtime_key = f"{session_prefix}:temperature:realtime"
            realtime_data = self.redis.get_hash(realtime_key)

            if not realtime_data:
                print(f"⚠️ 未找到实时数据: {realtime_key}")
                return True

            # 转换数据格式
            document = {
                "session_prefix": session_prefix,
                "timestamp": realtime_data.get("timestamp"),
                "channel_count": int(realtime_data.get("channel_count", 0)),
                "channels": {},
                "created_at": datetime.now()
            }

            # 添加通道数据
            for key, value in realtime_data.items():
                if key.startswith("channel_"):
                    if key.endswith("_raw"):
                        channel_num = key.split("_")[1]
                        document["channels"][f"channel_{channel_num}"] = {
                            "value": float(realtime_data.get(f"channel_{channel_num}", 0)),
                            "raw": int(value)
                        }

            # 插入到MongoDB
            result_id = self.mongo.insert_one("realtime_temperature", document)
            if result_id:
                self.migration_stats["realtime_records"] += 1
                print(f"  ✅ 实时数据迁移成功")
                return True
            else:
                print(f"  ❌ 实时数据迁移失败")
                return False

        except Exception as e:
            print(f"  ❌ 迁移实时数据出错: {e}")
            self.migration_stats["errors"] += 1
            return False

    def migrate_historical_data(self, session_prefix: str) -> bool:
        """迁移历史数据"""
        try:
            history_key = f"{session_prefix}:temperature:history"
            history_list = self.redis.get_list(history_key)

            if not history_list:
                print(f"⚠️ 未找到历史数据: {history_key}")
                return True

            # 批量转换并插入
            documents = []
            for record_json in history_list:
                try:
                    record = json.loads(record_json)
                    document = {
                        "session_prefix": session_prefix,
                        "timestamp": record.get("timestamp"),
                        "temperatures": record.get("temperatures", []),
                        "channel_count": len(record.get("temperatures", [])),
                        "created_at": datetime.now()
                    }
                    documents.append(document)
                except json.JSONDecodeError:
                    print(f"  ⚠️ 跳过无效的JSON记录: {record_json[:50]}...")
                    continue

            # 批量插入
            if documents:
                result_ids = self.mongo.insert_many("historical_temperature", documents)
                if result_ids:
                    self.migration_stats["historical_records"] += len(documents)
                    print(f"  ✅ 历史数据迁移成功: {len(documents)} 条记录")
                    return True
                else:
                    print(f"  ❌ 历史数据迁移失败")
                    return False
            else:
                print(f"  ⚠️ 没有有效的历史数据记录")
                return True

        except Exception as e:
            print(f"  ❌ 迁移历史数据出错: {e}")
            self.migration_stats["errors"] += 1
            return False

    def migrate_timeseries_data(self, session_prefix: str) -> bool:
        """迁移时间序列数据"""
        try:
            # 查找所有时间序列键
            ts_pattern = f"{session_prefix}:temperature:timeseries:*"
            ts_keys = self.redis.redis_client.keys(ts_pattern)

            if not ts_keys:
                print(f"⚠️ 未找到时间序列数据")
                return True

            total_points = 0
            for ts_key in ts_keys:
                channel_num = ts_key.split(":")[-1]

                # 获取时间序列数据（有序集合）
                try:
                    # 使用ZRANGE获取所有数据（包含分数）
                    ts_data = self.redis.redis_client.zrange(ts_key, 0, -1, withscores=True)

                    documents = []
                    for value, score in ts_data:
                        document = {
                            "session_prefix": session_prefix,
                            "channel": channel_num,
                            "timestamp": datetime.fromtimestamp(score).isoformat(),
                            "temperature": float(value),
                            "timestamp_unix": score,
                            "created_at": datetime.now()
                        }
                        documents.append(document)

                    # 批量插入
                    if documents:
                        collection_name = "timeseries_temperature"
                        result_ids = self.mongo.insert_many(collection_name, documents)
                        if result_ids:
                            total_points += len(documents)
                        else:
                            print(f"  ❌ 通道 {channel_num} 时间序列数据插入失败")
                            return False

                except Exception as e:
                    print(f"  ❌ 获取通道 {channel_num} 时间序列数据失败: {e}")
                    return False

            if total_points > 0:
                self.migration_stats["timeseries_points"] += total_points
                print(f"  ✅ 时间序列数据迁移成功: {total_points} 个数据点")
            else:
                print(f"  ⚠️ 没有有效的时间序列数据")

            return True

        except Exception as e:
            print(f"  ❌ 迁移时间序列数据出错: {e}")
            self.migration_stats["errors"] += 1
            return False

    def migrate_statistics_data(self, session_prefix: str) -> bool:
        """迁移统计数据"""
        try:
            stats_key = f"{session_prefix}:temperature:statistics"
            stats_data = self.redis.get_hash(stats_key)

            if not stats_data:
                print(f"⚠️ 未找到统计数据: {stats_key}")
                return True

            # 转换数据格式
            document = {
                "session_prefix": session_prefix,
                "last_update": stats_data.get("last_update"),
                "channel_count": int(stats_data.get("channel_count", 0)),
                "statistics": {
                    "min_temperature": float(stats_data.get("min_temperature", 0)),
                    "max_temperature": float(stats_data.get("max_temperature", 0)),
                    "avg_temperature": float(stats_data.get("avg_temperature", 0)),
                    "channel_min": stats_data.get("channel_min"),
                    "channel_max": stats_data.get("channel_max")
                },
                "channels": {},
                "created_at": datetime.now()
            }

            # 添加各通道数据
            for key, value in stats_data.items():
                if key.startswith("channel_") and not key.endswith(("_min", "_max")):
                    document["channels"][key] = float(value)

            # 插入到MongoDB
            result_id = self.mongo.insert_one("statistics_temperature", document)
            if result_id:
                self.migration_stats["statistics_records"] += 1
                print(f"  ✅ 统计数据迁移成功")
                return True
            else:
                print(f"  ❌ 统计数据迁移失败")
                return False

        except Exception as e:
            print(f"  ❌ 迁移统计数据出错: {e}")
            self.migration_stats["errors"] += 1
            return False

    def migrate_all_data(self, session_prefixes: List[str] = None) -> bool:
        """迁移所有数据"""
        self.migration_stats["start_time"] = datetime.now()

        print("="*80)
        print("🚀 开始Redis到MongoDB数据迁移")
        print("="*80)

        # 如果未指定会话，则发现所有会话
        if not session_prefixes:
            session_prefixes = self.discover_sessions()
            if not session_prefixes:
                print("⚠️ 未发现任何会话数据")
                return False

        print(f"\n📋 准备迁移 {len(session_prefixes)} 个会话")
        print("-"*80)

        # 创建必要的索引
        self.create_indexes()

        # 逐个迁移会话
        success_count = 0
        for session_prefix in session_prefixes:
            if self.migrate_session(session_prefix):
                success_count += 1

        self.migration_stats["end_time"] = datetime.now()

        # 打印迁移统计
        self.print_migration_summary()

        return success_count == len(session_prefixes)

    def create_indexes(self):
        """创建MongoDB索引"""
        print("\n🔧 创建MongoDB索引...")

        # 实时数据索引
        self.mongo.create_index("realtime_temperature", [
            ("session_prefix", 1),
            ("timestamp", -1)
        ])

        # 历史数据索引
        self.mongo.create_index("historical_temperature", [
            ("session_prefix", 1),
            ("timestamp", -1)
        ])

        # 时间序列数据索引
        self.mongo.create_index("timeseries_temperature", [
            ("session_prefix", 1),
            ("channel", 1),
            ("timestamp", -1)
        ])
        self.mongo.create_index("timeseries_temperature", [
            ("channel", 1),
            ("timestamp", -1)
        ])

        # 统计数据索引
        self.mongo.create_index("statistics_temperature", [
            ("session_prefix", -1)
        ])

    def print_migration_summary(self):
        """打印迁移统计摘要"""
        duration = (self.migration_stats["end_time"] - self.migration_stats["start_time"]).total_seconds()

        print("\n" + "="*80)
        print("📊 数据迁移完成 - 统计摘要")
        print("="*80)
        print(f"📦 迁移的会话数: {self.migration_stats['sessions_migrated']}")
        print(f"📊 实时数据记录: {self.migration_stats['realtime_records']}")
        print(f"📈 历史数据记录: {self.migration_stats['historical_records']}")
        print(f"📉 时间序列数据点: {self.migration_stats['timeseries_points']}")
        print(f"📋 统计数据记录: {self.migration_stats['statistics_records']}")
        print(f"❌ 错误数量: {self.migration_stats['errors']}")
        print(f"⏱️ 总耗时: {duration:.1f} 秒")
        print("="*80)


def main():
    """主函数 - 执行数据迁移"""
    print("="*80)
    print("🔄 Redis到MongoDB数据迁移工具")
    print("="*80)

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

    # 连接MongoDB
    mongo_manager = MongoDBManager(
        host='localhost',
        port=27017,
        username=None,
        password=None,
        database='temperature_data'
    )

    if not mongo_manager.connect():
        print("❌ 无法连接到MongoDB，程序退出")
        redis_manager.disconnect()
        return

    try:
        # 创建迁移器
        migrator = RedisToMongoDBMigrator(redis_manager, mongo_manager)

        # 询问用户是否要迁移所有数据
        print("\n选择迁移选项:")
        print("1. 迁移所有会话数据")
        print("2. 迁移指定会话数据")

        choice = input("\n请输入选择 (1 或 2): ").strip()

        if choice == "1":
            # 迁移所有数据
            migrator.migrate_all_data()
        elif choice == "2":
            # 迁移指定会话
            sessions = migrator.discover_sessions()
            if sessions:
                print(f"\n发现的会话: {sessions}")
                selected = input("请输入要迁移的会话前缀（用逗号分隔，留空表示全部）: ").strip()

                if selected:
                    selected_sessions = [s.strip() for s in selected.split(",")]
                    # 验证会话是否存在
                    valid_sessions = [s for s in selected_sessions if s in sessions]
                    if valid_sessions:
                        migrator.migrate_all_data(valid_sessions)
                    else:
                        print("❌ 没有有效的会话前缀")
                else:
                    migrator.migrate_all_data()
            else:
                print("⚠️ 未发现任何会话数据")
        else:
            print("❌ 无效的选择")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断迁移")
    except Exception as e:
        print(f"❌ 迁移过程出错: {e}")
    finally:
        # 断开连接
        redis_manager.disconnect()
        mongo_manager.disconnect()
        print("\n👋 迁移程序结束")


if __name__ == "__main__":
    main()