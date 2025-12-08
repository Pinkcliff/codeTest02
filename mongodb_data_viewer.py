import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from mongodb_client import MongoDBManager
import pandas as pd
import matplotlib.pyplot as plt


class MongoDBDataViewer:
    """MongoDB数据查询和可视化工具"""

    def __init__(self, mongo_manager: MongoDBManager):
        self.mongo = mongo_manager

    def show_collections_info(self):
        """显示所有集合的信息"""
        print("="*60)
        print("📋 MongoDB集合信息")
        print("="*60)

        collections = self.mongo.get_collections()
        for collection in collections:
            count = self.mongo.count_documents(collection)
            print(f"\n📁 集合: {collection}")
            print(f"   📊 文档数量: {count:,}")

            # 显示示例文档结构
            sample = self.mongo.find_one(collection)
            if sample:
                print(f"   📝 示例文档结构:")
                for key, value in sample.items():
                    if key == "_id":
                        continue
                    value_type = type(value).__name__
                    if isinstance(value, (dict, list)):
                        value_preview = f"({value_type}, 长度: {len(value)})"
                    else:
                        value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    print(f"      - {key}: {value_type} = {value_preview}")

    def query_realtime_data(self, session_prefix: str = None, limit: int = 10):
        """查询实时温度数据"""
        print("\n" + "="*60)
        print("🌡️ 实时温度数据")
        print("="*60)

        query = {}
        if session_prefix:
            query["session_prefix"] = session_prefix

        # 按时间戳降序查询最新数据
        documents = self.mongo.find_many(
            "realtime_temperature",
            query=query,
            sort=[("timestamp", -1)],
            limit=limit
        )

        if not documents:
            print("⚠️ 未找到实时数据")
            return

        print(f"📊 查询到 {len(documents)} 条记录\n")

        for doc in documents:
            print(f"🕐 时间: {doc.get('timestamp')}")
            print(f"📦 会话: {doc.get('session_prefix')}")
            print(f"📡 通道数: {doc.get('channel_count')}")

            channels = doc.get("channels", {})
            if channels:
                print("🌡️ 温度数据:")
                for channel_name, data in channels.items():
                    print(f"   {channel_name}: {data.get('value', 0):.1f}℃ (原始值: {data.get('raw', 0)})")

            print("-"*40)

    def query_historical_data(self, session_prefix: str = None,
                            start_time: str = None, end_time: str = None,
                            limit: int = 100):
        """查询历史温度数据"""
        print("\n" + "="*60)
        print("📈 历史温度数据")
        print("="*60)

        query = {}
        if session_prefix:
            query["session_prefix"] = session_prefix

        # 时间范围查询
        if start_time or end_time:
            time_query = {}
            if start_time:
                time_query["$gte"] = start_time
            if end_time:
                time_query["$lte"] = end_time
            query["timestamp"] = time_query

        # 按时间戳降序查询
        documents = self.mongo.find_many(
            "historical_temperature",
            query=query,
            sort=[("timestamp", -1)],
            limit=limit
        )

        if not documents:
            print("⚠️ 未找到历史数据")
            return

        print(f"📊 查询到 {len(documents)} 条记录\n")

        # 显示统计信息
        temps = []
        for doc in documents:
            temperatures = doc.get("temperatures", [])
            if temperatures:
                temps.extend(temperatures)

        if temps:
            print(f"📈 温度统计:")
            print(f"   最高温度: {max(temps):.1f}℃")
            print(f"   最低温度: {min(temps):.1f}℃")
            print(f"   平均温度: {sum(temps)/len(temps):.1f}℃")
            print()

        # 显示前几条记录
        for i, doc in enumerate(documents[:5]):
            print(f"🕐 记录 {i+1}: {doc.get('timestamp')}")
            temperatures = doc.get("temperatures", [])
            if temperatures:
                print(f"   各通道温度: {[f'{t:.1f}℃' for t in temperatures[:6]]}{'...' if len(temperatures) > 6 else ''}")
            print("-"*40)

    def query_timeseries_data(self, channel: str = None,
                            start_time: str = None, end_time: str = None,
                            limit: int = 1000):
        """查询时间序列温度数据"""
        print("\n" + "="*60)
        print("📉 时间序列温度数据")
        print("="*60)

        query = {}
        if channel:
            query["channel"] = channel

        # 时间范围查询
        if start_time or end_time:
            time_query = {}
            if start_time:
                time_query["$gte"] = start_time
            if end_time:
                time_query["$lte"] = end_time
            query["timestamp"] = time_query

        # 按时间戳升序查询（便于绘图）
        documents = self.mongo.find_many(
            "timeseries_temperature",
            query=query,
            sort=[("timestamp", 1)],
            limit=limit
        )

        if not documents:
            print("⚠️ 未找到时间序列数据")
            return

        print(f"📊 查询到 {len(documents)} 条记录\n")

        # 按通道分组统计
        channel_stats = {}
        for doc in documents:
            ch = doc.get("channel", "unknown")
            temp = doc.get("temperature", 0)
            if ch not in channel_stats:
                channel_stats[ch] = []
            channel_stats[ch].append(temp)

        print("📈 各通道温度统计:")
        for ch, temps in channel_stats.items():
            print(f"   {ch}: 平均 {sum(temps)/len(temps):.1f}℃, "
                  f"最高 {max(temps):.1f}℃, 最低 {min(temps):.1f}℃, "
                  f"采样点 {len(temps)}")

        # 询问是否绘制图表
        if channel_stats and input("\n是否绘制温度趋势图? (y/n): ").lower() == 'y':
            self.plot_temperature_trend(documents, channel)

    def query_statistics_data(self, session_prefix: str = None):
        """查询统计数据"""
        print("\n" + "="*60)
        print("📋 温度统计数据")
        print("="*60)

        query = {}
        if session_prefix:
            query["session_prefix"] = session_prefix

        documents = self.mongo.find_many(
            "statistics_temperature",
            query=query,
            sort=[("last_update", -1)]
        )

        if not documents:
            print("⚠️ 未找到统计数据")
            return

        print(f"📊 查询到 {len(documents)} 条统计记录\n")

        for doc in documents:
            print(f"📦 会话: {doc.get('session_prefix')}")
            print(f"🕐 最后更新: {doc.get('last_update')}")

            stats = doc.get("statistics", {})
            if stats:
                print(f"📈 统计信息:")
                print(f"   平均温度: {stats.get('avg_temperature', 0):.1f}℃")
                print(f"   最高温度: {stats.get('max_temperature', 0):.1f}℃ (通道: {stats.get('channel_max')})")
                print(f"   最低温度: {stats.get('min_temperature', 0):.1f}℃ (通道: {stats.get('channel_min')})")
                print(f"   通道数: {doc.get('channel_count', 0)}")

            channels = doc.get("channels", {})
            if channels:
                print("   各通道温度:")
                for ch, temp in channels.items():
                    print(f"      {ch}: {temp:.1f}℃")

            print("-"*40)

    def plot_temperature_trend(self, documents: List[Dict], channel: str = None):
        """绘制温度趋势图"""
        try:
            # 准备数据
            timestamps = [doc.get("timestamp") for doc in documents]
            temperatures = [doc.get("temperature", 0) for doc in documents]

            # 转换时间戳为datetime对象
            times = [datetime.fromisoformat(ts.replace('Z', '+00:00')) for ts in timestamps]

            # 创建图表
            plt.figure(figsize=(12, 6))
            plt.plot(times, temperatures, label=f'通道 {channel}' if channel else '温度')
            plt.title(f"温度趋势图 - {channel if channel else '所有通道'}")
            plt.xlabel("时间")
            plt.ylabel("温度 (℃)")
            plt.grid(True, alpha=0.3)
            plt.legend()

            # 格式化x轴
            plt.xticks(rotation=45)
            plt.tight_layout()

            # 显示图表
            plt.show()

        except Exception as e:
            print(f"❌ 绘制图表失败: {e}")

    def compare_redis_mongodb_data(self, redis_manager, session_prefix: str = None):
        """比较Redis和MongoDB中的数据"""
        print("\n" + "="*60)
        print("🔍 Redis vs MongoDB 数据对比")
        print("="*60)

        if not session_prefix:
            print("⚠️ 请提供会话前缀进行对比")
            return

        # 比较实时数据
        print("\n📊 实时数据对比:")
        redis_key = f"{session_prefix}:temperature:realtime"
        redis_data = redis_manager.get_hash(redis_key)

        mongo_doc = self.mongo.find_one("realtime_temperature",
                                      {"session_prefix": session_prefix})

        if redis_data and mongo_doc:
            print("✅ 两者都有数据")
            # 比较通道数据
            redis_channels = {k: v for k, v in redis_data.items() if k.startswith("channel_") and not k.endswith("_raw")}
            mongo_channels = mongo_doc.get("channels", {})

            print(f"Redis通道数: {len(redis_channels)}")
            print(f"MongoDB通道数: {len(mongo_channels)}")

            # 检查数据一致性
            mismatch_count = 0
            for ch_num in range(1, 13):
                redis_key = f"channel_{ch_num:02d}"
                mongo_key = f"channel_{ch_num:02d}"

                if redis_key in redis_data and mongo_key in mongo_channels:
                    redis_temp = float(redis_data[redis_key])
                    mongo_temp = mongo_channels[mongo_key].get("value", 0)
                    if abs(redis_temp - mongo_temp) > 0.1:
                        print(f"⚠️ 通道{ch_num:02d}不匹配: Redis={redis_temp:.1f}℃, MongoDB={mongo_temp:.1f}℃")
                        mismatch_count += 1

            if mismatch_count == 0:
                print("✅ 所有通道数据一致")
            else:
                print(f"❌ {mismatch_count} 个通道数据不一致")
        else:
            print("⚠️ 缺少对比数据")

    def export_to_json(self, collection_name: str, output_file: str,
                      session_prefix: str = None, query_filter: Dict = None):
        """导出数据到JSON文件"""
        print(f"\n📤 导出 {collection_name} 数据到 {output_file}")

        query = query_filter or {}
        if session_prefix:
            query["session_prefix"] = session_prefix

        documents = self.mongo.find_many(collection_name, query=query)

        if not documents:
            print("⚠️ 没有数据可导出")
            return

        # 转换ObjectId为字符串
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        # 写入文件
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(documents, f, ensure_ascii=False, indent=2)

        print(f"✅ 成功导出 {len(documents)} 条记录")

    def show_data_summary(self):
        """显示数据汇总"""
        print("\n" + "="*60)
        print("📊 MongoDB数据汇总")
        print("="*60)

        # 各集合的数据量
        collections_info = {
            "realtime_temperature": "实时数据",
            "historical_temperature": "历史数据",
            "timeseries_temperature": "时间序列数据",
            "statistics_temperature": "统计数据",
            "sync_status": "同步状态",
            "sync_progress": "同步进度"
        }

        total_documents = 0
        for collection, desc in collections_info.items():
            count = self.mongo.count_documents(collection)
            total_documents += count
            print(f"{desc:15}: {count:>10,} 条")

        print("-"*60)
        print(f"{'总计':15}: {total_documents:>10,} 条")

        # 最新数据时间
        latest_doc = self.mongo.find_one(
            "realtime_temperature",
            sort=[("timestamp", -1)]
        )
        if latest_doc:
            print(f"\n🕐 最新数据时间: {latest_doc.get('timestamp')}")

        # 会话数量
        sessions = self.mongo.aggregate("realtime_temperature", [
            {"$group": {"_id": "$session_prefix"}},
            {"$count": "total"}
        ])
        session_count = sessions[0]["total"] if sessions else 0
        print(f"📦 会话数量: {session_count}")


def main():
    """主函数 - 数据查询工具"""
    print("="*80)
    print("🔍 MongoDB数据查询和验证工具")
    print("="*80)

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
        return

    # 可选连接Redis进行数据对比
    redis_manager = None
    if input("\n是否连接Redis进行数据对比? (y/n): ").lower() == 'y':
        try:
            from redis_client import RedisManager
            redis_manager = RedisManager(
                host='localhost',
                port=6379,
                password=None,
                db=0,
                decode_responses=True
            )
            if not redis_manager.connect():
                print("⚠️ Redis连接失败，将跳过数据对比功能")
                redis_manager = None
        except:
            print("⚠️ Redis模块导入失败，将跳过数据对比功能")
            redis_manager = None

    try:
        # 创建查看器
        viewer = MongoDBDataViewer(mongo_manager)

        while True:
            print("\n" + "="*60)
            print("📋 请选择功能:")
            print("="*60)
            print("1. 显示集合信息")
            print("2. 查看数据汇总")
            print("3. 查询实时数据")
            print("4. 查询历史数据")
            print("5. 查询时间序列数据")
            print("6. 查询统计数据")
            print("7. 数据导出")
            if redis_manager:
                print("8. Redis vs MongoDB 数据对比")
            print("0. 退出")

            choice = input("\n请输入选择: ").strip()

            if choice == "1":
                viewer.show_collections_info()
            elif choice == "2":
                viewer.show_data_summary()
            elif choice == "3":
                session = input("输入会话前缀 (留空查询所有): ").strip() or None
                limit = int(input("输入查询数量 (默认10): ").strip() or "10")
                viewer.query_realtime_data(session, limit)
            elif choice == "4":
                session = input("输入会话前缀 (留空查询所有): ").strip() or None
                start = input("输入开始时间 (YYYY-MM-DD HH:MM:SS, 留空忽略): ").strip() or None
                end = input("输入结束时间 (YYYY-MM-DD HH:MM:SS, 留空忽略): ").strip() or None
                limit = int(input("输入查询数量 (默认100): ").strip() or "100")
                viewer.query_historical_data(session, start, end, limit)
            elif choice == "5":
                channel = input("输入通道号 (如 channel_01, 留空查询所有): ").strip() or None
                start = input("输入开始时间 (YYYY-MM-DD HH:MM:SS, 留空忽略): ").strip() or None
                end = input("输入结束时间 (YYYY-MM-DD HH:MM:SS, 留空忽略): ").strip() or None
                limit = int(input("输入查询数量 (默认1000): ").strip() or "1000")
                viewer.query_timeseries_data(channel, start, end, limit)
            elif choice == "6":
                session = input("输入会话前缀 (留空查询所有): ").strip() or None
                viewer.query_statistics_data(session)
            elif choice == "7":
                print("\n选择导出的集合:")
                print("1. realtime_temperature")
                print("2. historical_temperature")
                print("3. timeseries_temperature")
                print("4. statistics_temperature")
                coll_choice = input("请选择 (1-4): ").strip()
                collections = {
                    "1": "realtime_temperature",
                    "2": "historical_temperature",
                    "3": "timeseries_temperature",
                    "4": "statistics_temperature"
                }
                if coll_choice in collections:
                    collection = collections[coll_choice]
                    filename = f"{collection}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                    session = input("输入会话前缀 (留空导出所有): ").strip() or None
                    viewer.export_to_json(collection, filename, session)
            elif choice == "8" and redis_manager:
                session = input("输入要对比的会话前缀: ").strip()
                viewer.compare_redis_mongodb_data(redis_manager, session)
            elif choice == "0":
                print("👋 退出程序")
                break
            else:
                print("❌ 无效的选择")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
    except Exception as e:
        print(f"❌ 程序运行出错: {e}")
    finally:
        # 断开连接
        mongo_manager.disconnect()
        if redis_manager:
            redis_manager.disconnect()
        print("\n👋 查询程序结束")


if __name__ == "__main__":
    main()