import json
import time
import threading
from datetime import datetime
from typing import List, Dict, Optional, Any
from redis_client import RedisManager
from mongodb_client import MongoDBManager


class RealTimeSyncManager:
    """Redis到MongoDB实时数据同步管理器"""

    def __init__(self, redis_manager: RedisManager, mongo_manager: MongoDBManager,
                 sync_interval: int = 1, batch_size: int = 100):
        """
        初始化实时同步管理器

        Args:
            redis_manager: Redis管理器实例
            mongo_manager: MongoDB管理器实例
            sync_interval: 同步间隔（秒）
            batch_size: 批量处理大小
        """
        self.redis = redis_manager
        self.mongo = mongo_manager
        self.sync_interval = sync_interval
        self.batch_size = batch_size
        self.is_running = False
        self.sync_thread: Optional[threading.Thread] = None
        self.sync_stats = {
            "total_synced": 0,
            "realtime_synced": 0,
            "historical_synced": 0,
            "timeseries_synced": 0,
            "statistics_synced": 0,
            "errors": 0,
            "last_sync_time": None,
            "start_time": None
        }
        self.session_prefix = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.last_known_keys = set()

    def sync_realtime_data(self, key: str) -> bool:
        """同步单个实时数据"""
        try:
            realtime_data = self.redis.get_hash(key)

            if not realtime_data:
                return True

            # 检查是否已同步（使用时间戳判断）
            timestamp = realtime_data.get("timestamp")
            if self.is_already_synced("realtime", key, timestamp):
                return True

            # 转换数据格式
            document = {
                "session_prefix": self.session_prefix,
                "timestamp": timestamp,
                "channel_count": int(realtime_data.get("channel_count", 0)),
                "channels": {},
                "synced_at": datetime.now()
            }

            # 添加通道数据
            for key_name, value in realtime_data.items():
                if key_name.startswith("channel_"):
                    if key_name.endswith("_raw"):
                        channel_num = key_name.split("_")[1]
                        document["channels"][f"channel_{channel_num}"] = {
                            "value": float(realtime_data.get(f"channel_{channel_num}", 0)),
                            "raw": int(value)
                        }

            # 插入到MongoDB
            result_id = self.mongo.insert_one("realtime_temperature", document)
            if result_id:
                self.sync_stats["realtime_synced"] += 1
                self.record_sync("realtime", key, timestamp)
                return True
            else:
                return False

        except Exception as e:
            print(f"❌ 同步实时数据失败 {key}: {e}")
            self.sync_stats["errors"] += 1
            return False

    def sync_historical_data(self, key: str) -> bool:
        """同步历史数据（支持增量同步）"""
        try:
            history_list = self.redis.get_list(key)

            if not history_list:
                return True

            # 获取已同步的最新记录数
            synced_count = self.get_synced_count("historical", key)
            new_records = history_list[synced_count:]  # 只处理新记录

            if not new_records:
                return True

            # 批量转换
            documents = []
            for record_json in new_records:
                try:
                    record = json.loads(record_json)
                    timestamp = record.get("timestamp")

                    # 跳过已同步的记录
                    if self.is_already_synced("historical", key, timestamp):
                        continue

                    document = {
                        "session_prefix": self.session_prefix,
                        "timestamp": timestamp,
                        "temperatures": record.get("temperatures", []),
                        "channel_count": len(record.get("temperatures", [])),
                        "synced_at": datetime.now()
                    }
                    documents.append(document)
                except json.JSONDecodeError:
                    continue

            # 批量插入
            if documents:
                result_ids = self.mongo.insert_many("historical_temperature", documents)
                if result_ids:
                    self.sync_stats["historical_synced"] += len(documents)
                    # 更新同步计数
                    self.update_sync_count("historical", key, synced_count + len(documents))
                    return True
                else:
                    return False

            return True

        except Exception as e:
            print(f"❌ 同步历史数据失败 {key}: {e}")
            self.sync_stats["errors"] += 1
            return False

    def sync_timeseries_data(self, key: str) -> bool:
        """同步时间序列数据"""
        try:
            channel_num = key.split(":")[-1]

            # 获取新的时间序列数据
            last_synced_score = self.get_last_synced_score("timeseries", key)
            ts_data = self.redis.redis_client.zrangebyscore(
                key,
                f"({last_synced_score}",  # 使用开放式区间获取新数据
                "+inf",
                withscores=True
            )

            if not ts_data:
                return True

            # 批量转换
            documents = []
            max_score = last_synced_score

            for value, score in ts_data:
                document = {
                    "session_prefix": self.session_prefix,
                    "channel": channel_num,
                    "timestamp": datetime.fromtimestamp(score).isoformat(),
                    "temperature": float(value),
                    "timestamp_unix": score,
                    "synced_at": datetime.now()
                }
                documents.append(document)
                max_score = max(max_score, score)

            # 批量插入
            if documents:
                collection_name = "timeseries_temperature"
                result_ids = self.mongo.insert_many(collection_name, documents)
                if result_ids:
                    self.sync_stats["timeseries_synced"] += len(documents)
                    # 更新最后同步的分值
                    self.update_last_synced_score("timeseries", key, max_score)
                    return True
                else:
                    return False

            return True

        except Exception as e:
            print(f"❌ 同步时间序列数据失败 {key}: {e}")
            self.sync_stats["errors"] += 1
            return False

    def sync_statistics_data(self, key: str) -> bool:
        """同步统计数据"""
        try:
            stats_data = self.redis.get_hash(key)

            if not stats_data:
                return True

            # 检查是否已同步
            last_update = stats_data.get("last_update")
            if self.is_already_synced("statistics", key, last_update):
                return True

            # 转换数据格式
            document = {
                "session_prefix": self.session_prefix,
                "last_update": last_update,
                "channel_count": int(stats_data.get("channel_count", 0)),
                "statistics": {
                    "min_temperature": float(stats_data.get("min_temperature", 0)),
                    "max_temperature": float(stats_data.get("max_temperature", 0)),
                    "avg_temperature": float(stats_data.get("avg_temperature", 0)),
                    "channel_min": stats_data.get("channel_min"),
                    "channel_max": stats_data.get("channel_max")
                },
                "channels": {},
                "synced_at": datetime.now()
            }

            # 添加各通道数据
            for key_name, value in stats_data.items():
                if key_name.startswith("channel_") and not key_name.endswith(("_min", "_max")):
                    document["channels"][key_name] = float(value)

            # 插入到MongoDB
            result_id = self.mongo.insert_one("statistics_temperature", document)
            if result_id:
                self.sync_stats["statistics_synced"] += 1
                self.record_sync("statistics", key, last_update)
                return True
            else:
                return False

        except Exception as e:
            print(f"❌ 同步统计数据失败 {key}: {e}")
            self.sync_stats["errors"] += 1
            return False

    def monitor_and_sync(self):
        """监控并同步数据变化"""
        print(f"🚀 开始实时监控和同步 (间隔: {self.sync_interval}秒)")
        print(f"📁 当前会话: {self.session_prefix}")

        while self.is_running:
            try:
                sync_start_time = time.time()
                synced_this_round = 0

                # 获取所有温度相关的键
                all_keys = self.redis.redis_client.keys("*temperature*")
                current_keys = set(all_keys)

                # 检查新增的键
                new_keys = current_keys - self.last_known_keys
                if new_keys:
                    print(f"🔍 发现新键: {new_keys}")

                # 同步各类数据
                for key in all_keys:
                    key_str = key.decode() if isinstance(key, bytes) else key

                    if "realtime" in key_str:
                        if self.sync_realtime_data(key_str):
                            synced_this_round += 1
                    elif "history" in key_str:
                        if self.sync_historical_data(key_str):
                            synced_this_round += 1
                    elif "timeseries" in key_str:
                        if self.sync_timeseries_data(key_str):
                            synced_this_round += 1
                    elif "statistics" in key_str:
                        if self.sync_statistics_data(key_str):
                            synced_this_round += 1

                # 更新键集合
                self.last_known_keys = current_keys

                # 更新统计
                self.sync_stats["total_synced"] += synced_this_round
                self.sync_stats["last_sync_time"] = datetime.now()

                # 打印进度
                if synced_this_round > 0:
                    sync_duration = (time.time() - sync_start_time) * 1000
                    print(f"✅ 同步完成: {synced_this_round}项 | 耗时: {sync_duration:.1f}ms")

                # 等待下一次同步
                time.sleep(self.sync_interval)

            except Exception as e:
                print(f"❌ 监控同步出错: {e}")
                self.sync_stats["errors"] += 1
                time.sleep(self.sync_interval)

    def start_sync(self):
        """启动实时同步"""
        if self.is_running:
            print("⚠️ 同步已在运行中")
            return

        self.is_running = True
        self.sync_stats["start_time"] = datetime.now()

        # 创建并启动同步线程
        self.sync_thread = threading.Thread(target=self.monitor_and_sync, daemon=True)
        self.sync_thread.start()

        print("✅ 实时同步已启动")

    def stop_sync(self):
        """停止实时同步"""
        if not self.is_running:
            print("⚠️ 同步未在运行")
            return

        print("🛑 正在停止实时同步...")
        self.is_running = False

        # 等待线程结束
        if self.sync_thread and self.sync_thread.is_alive():
            self.sync_thread.join(timeout=5)

        print("✅ 实时同步已停止")

    def print_sync_stats(self):
        """打印同步统计"""
        print("\n" + "="*60)
        print("📊 实时同步统计")
        print("="*60)
        print(f"✅ 实时数据: {self.sync_stats['realtime_synced']}")
        print(f"📈 历史数据: {self.sync_stats['historical_synced']}")
        print(f"📉 时间序列: {self.sync_stats['timeseries_synced']}")
        print(f"📋 统计数据: {self.sync_stats['statistics_synced']}")
        print(f"🔄 总同步数: {self.sync_stats['total_synced']}")
        print(f"❌ 错误数: {self.sync_stats['errors']}")

        if self.sync_stats["start_time"]:
            duration = (datetime.now() - self.sync_stats["start_time"]).total_seconds()
            print(f"⏱️ 运行时长: {duration:.1f}秒")

        if self.sync_stats["last_sync_time"]:
            print(f"🕐 最后同步: {self.sync_stats['last_sync_time'].strftime('%H:%M:%S')}")

        print("="*60)

    # 辅助方法：使用MongoDB存储同步状态
    def is_already_synced(self, data_type: str, key: str, timestamp: str) -> bool:
        """检查数据是否已同步"""
        try:
            sync_record = self.mongo.find_one("sync_status", {
                "data_type": data_type,
                "key": key,
                "timestamp": timestamp
            })
            return sync_record is not None
        except:
            return False

    def record_sync(self, data_type: str, key: str, timestamp: str):
        """记录同步状态"""
        try:
            self.mongo.insert_one("sync_status", {
                "data_type": data_type,
                "key": key,
                "timestamp": timestamp,
                "synced_at": datetime.now()
            })
        except:
            pass

    def get_synced_count(self, data_type: str, key: str) -> int:
        """获取已同步的记录数"""
        try:
            sync_record = self.mongo.find_one("sync_progress", {
                "data_type": data_type,
                "key": key
            })
            return sync_record.get("count", 0) if sync_record else 0
        except:
            return 0

    def update_sync_count(self, data_type: str, key: str, count: int):
        """更新同步计数"""
        try:
            self.mongo.update_one(
                "sync_progress",
                {"data_type": data_type, "key": key},
                {"$set": {"count": count, "updated_at": datetime.now()}},
                upsert=True
            )
        except:
            pass

    def get_last_synced_score(self, data_type: str, key: str) -> float:
        """获取最后同步的时间戳分数"""
        try:
            sync_record = self.mongo.find_one("sync_progress", {
                "data_type": data_type,
                "key": key
            })
            return sync_record.get("last_score", 0) if sync_record else 0
        except:
            return 0

    def update_last_synced_score(self, data_type: str, key: str, score: float):
        """更新最后同步的时间戳分数"""
        try:
            self.mongo.update_one(
                "sync_progress",
                {"data_type": data_type, "key": key},
                {"$set": {"last_score": score, "updated_at": datetime.now()}},
                upsert=True
            )
        except:
            pass


def main():
    """主函数 - 启动实时同步"""
    print("="*80)
    print("🔄 Redis到MongoDB实时同步工具")
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
        # 创建实时同步管理器
        sync_manager = RealTimeSyncManager(
            redis_manager,
            mongo_manager,
            sync_interval=1,  # 1秒同步间隔
            batch_size=100
        )

        # 创建索引（如果还没有）
        sync_manager.mongo.create_index("sync_status", [
            ("data_type", 1),
            ("key", 1),
            ("timestamp", -1)
        ])

        print("\n🚀 启动实时同步...")
        sync_manager.start_sync()

        # 定期打印统计信息
        try:
            while True:
                time.sleep(30)  # 每30秒打印一次统计
                sync_manager.print_sync_stats()
        except KeyboardInterrupt:
            print("\n⚠️ 用户中断同步")

        # 停止同步
        sync_manager.stop_sync()

        # 打印最终统计
        sync_manager.print_sync_stats()

    except Exception as e:
        print(f"❌ 同步过程出错: {e}")
    finally:
        # 断开连接
        redis_manager.disconnect()
        mongo_manager.disconnect()
        print("\n👋 实时同步程序结束")


if __name__ == "__main__":
    main()