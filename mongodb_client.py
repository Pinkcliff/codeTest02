import pymongo
import time
from typing import Any, Optional, Dict, List
from datetime import datetime


class MongoDBManager:
    """MongoDB连接和管理类"""

    def __init__(self, host: str = 'localhost', port: int = 27017,
                 username: Optional[str] = None, password: Optional[str] = None,
                 database: str = 'temperature_data', auth_source: str = 'admin'):
        """
        初始化MongoDB连接

        Args:
            host: MongoDB服务器地址
            port: MongoDB端口
            username: 用户名
            password: 密码
            database: 数据库名称
            auth_source: 认证源数据库
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database
        self.auth_source = auth_source
        self.client: Optional[pymongo.MongoClient] = None
        self.db: Optional[pymongo.database.Database] = None
        self.is_connected = False

    def connect(self) -> bool:
        """连接到MongoDB服务器"""
        try:
            # 构建连接URI
            if self.username and self.password:
                uri = f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database_name}?authSource={self.auth_source}"
            else:
                uri = f"mongodb://{self.host}:{self.port}/{self.database_name}"

            # 创建客户端连接
            self.client = pymongo.MongoClient(
                uri,
                serverSelectionTimeoutMS=5000,  # 5秒连接超时
                socketTimeoutMS=5000,          # 5秒socket超时
                connectTimeoutMS=5000,         # 5秒连接超时
                retryWrites=True,              # 启用重试写入
                w="majority"                   # 多数确认写入
            )

            # 测试连接
            self.client.admin.command('ping')

            # 获取数据库
            self.db = self.client[self.database_name]
            self.is_connected = True

            print(f"✅ 成功连接到MongoDB服务器: {self.host}:{self.port}")
            print(f"📁 使用数据库: {self.database_name}")
            return True

        except pymongo.errors.ServerSelectionTimeoutError as e:
            print(f"❌ MongoDB连接超时: {e}")
            self.is_connected = False
            return False
        except pymongo.errors.ConnectionFailure as e:
            print(f"❌ MongoDB连接失败: {e}")
            self.is_connected = False
            return False
        except Exception as e:
            print(f"❌ 连接错误: {e}")
            self.is_connected = False
            return False

    def disconnect(self):
        """断开MongoDB连接"""
        if self.client:
            try:
                self.client.close()
                self.is_connected = False
                print("🔌 MongoDB连接已关闭")
            except Exception as e:
                print(f"⚠️ 关闭连接时出错: {e}")

    def insert_one(self, collection_name: str, document: Dict[str, Any]) -> Optional[str]:
        """插入单个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return None

        try:
            collection = self.db[collection_name]
            result = collection.insert_one(document)
            print(f"✅ 插入文档到 {collection_name}: {result.inserted_id}")
            return str(result.inserted_id)
        except Exception as e:
            print(f"❌ 插入文档失败: {e}")
            return None

    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> Optional[List[str]]:
        """插入多个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return None

        try:
            collection = self.db[collection_name]
            result = collection.insert_many(documents)
            print(f"✅ 插入 {len(result.inserted_ids)} 个文档到 {collection_name}")
            return [str(id) for id in result.inserted_ids]
        except Exception as e:
            print(f"❌ 批量插入文档失败: {e}")
            return None

    def find_one(self, collection_name: str, query: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """查询单个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return None

        try:
            collection = self.db[collection_name]
            document = collection.find_one(query)
            print(f"📖 从 {collection_name} 查询到文档")
            return document
        except Exception as e:
            print(f"❌ 查询文档失败: {e}")
            return None

    def find_many(self, collection_name: str, query: Dict[str, Any] = None,
                  limit: int = 0, sort: List[tuple] = None) -> List[Dict[str, Any]]:
        """查询多个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return []

        try:
            collection = self.db[collection_name]
            cursor = collection.find(query)

            if sort:
                cursor = cursor.sort(sort)
            if limit > 0:
                cursor = cursor.limit(limit)

            documents = list(cursor)
            print(f"📖 从 {collection_name} 查询到 {len(documents)} 个文档")
            return documents
        except Exception as e:
            print(f"❌ 查询文档失败: {e}")
            return []

    def update_one(self, collection_name: str, query: Dict[str, Any],
                   update: Dict[str, Any], upsert: bool = False) -> Optional[int]:
        """更新单个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return None

        try:
            collection = self.db[collection_name]
            result = collection.update_one(query, update, upsert=upsert)
            print(f"✅ 更新 {collection_name} 中 {result.modified_count} 个文档")
            return result.modified_count
        except Exception as e:
            print(f"❌ 更新文档失败: {e}")
            return None

    def delete_one(self, collection_name: str, query: Dict[str, Any]) -> Optional[int]:
        """删除单个文档"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return None

        try:
            collection = self.db[collection_name]
            result = collection.delete_one(query)
            if result.deleted_count > 0:
                print(f"🗑️ 从 {collection_name} 删除 {result.deleted_count} 个文档")
            else:
                print(f"⚠️ 未找到匹配的文档进行删除")
            return result.deleted_count
        except Exception as e:
            print(f"❌ 删除文档失败: {e}")
            return None

    def count_documents(self, collection_name: str, query: Dict[str, Any] = None) -> int:
        """统计文档数量"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return 0

        try:
            collection = self.db[collection_name]
            count = collection.count_documents(query or {})
            print(f"📊 {collection_name} 中有 {count} 个文档")
            return count
        except Exception as e:
            print(f"❌ 统计文档失败: {e}")
            return 0

    def create_index(self, collection_name: str, keys: List[tuple], unique: bool = False) -> bool:
        """创建索引"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return False

        try:
            collection = self.db[collection_name]
            collection.create_index(keys, unique=unique)
            index_name = "_".join([f"{k}_{d}" for k, d in keys])
            print(f"✅ 在 {collection_name} 上创建索引: {index_name}")
            return True
        except Exception as e:
            print(f"❌ 创建索引失败: {e}")
            return False

    def get_collections(self) -> List[str]:
        """获取所有集合名称"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return []

        try:
            collections = self.db.list_collection_names()
            print(f"📋 数据库 {self.database_name} 中的集合: {collections}")
            return collections
        except Exception as e:
            print(f"❌ 获取集合列表失败: {e}")
            return []

    def get_server_info(self) -> Dict[str, Any]:
        """获取MongoDB服务器信息"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return {}

        try:
            server_info = self.client.server_info()
            print("📊 MongoDB服务器信息:")
            print(f"   版本: {server_info.get('version', 'Unknown')}")
            print(f"   Git版本: {server_info.get('gitVersion', 'Unknown')}")
            print(f"   操作系统: {server_info.get('sysInfo', 'Unknown')}")
            return server_info
        except Exception as e:
            print(f"❌ 获取服务器信息失败: {e}")
            return {}

    def aggregate(self, collection_name: str, pipeline: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """执行聚合查询"""
        if not self.is_connected:
            print("⚠️ MongoDB未连接")
            return []

        try:
            collection = self.db[collection_name]
            results = list(collection.aggregate(pipeline))
            print(f"📊 聚合查询返回 {len(results)} 个结果")
            return results
        except Exception as e:
            print(f"❌ 聚合查询失败: {e}")
            return []


def main():
    """主函数 - 演示MongoDB基本操作"""

    print("=" * 60)
    print("🚀 MongoDB连接演示程序")
    print("=" * 60)

    # 创建MongoDB管理器实例
    mongo_manager = MongoDBManager(
        host='localhost',      # MongoDB服务器地址
        port=27017,            # MongoDB端口
        username=None,         # 用户名（如果有）
        password=None,         # 密码（如果有）
        database='temperature_data'  # 数据库名称
    )

    try:
        # 连接MongoDB
        if not mongo_manager.connect():
            print("❌ 无法连接到MongoDB，程序退出")
            return

        # 获取服务器信息
        mongo_manager.get_server_info()
        print()

        # 创建集合和索引
        print("🔧 创建集合和索引:")
        # 创建时间戳索引，用于时间序列查询
        mongo_manager.create_index("realtime_data", [("timestamp", pymongo.DESCENDING)])
        # 创建通道编号索引，用于按通道查询
        mongo_manager.create_index("realtime_data", [("session_prefix", pymongo.ASCENDING)])
        # 创建复合索引用于时间范围查询
        mongo_manager.create_index("historical_data", [
            ("session_prefix", pymongo.ASCENDING),
            ("timestamp", pymongo.DESCENDING)
        ])
        print()

        # 演示插入文档
        print("📝 插入文档演示:")
        test_data = {
            "session_prefix": "20231208_120000",
            "timestamp": datetime.now().isoformat(),
            "channel_count": 12,
            "channel_01": 25.5,
            "channel_02": 26.1,
            "channel_03": 24.8,
            "avg_temperature": 25.5,
            "min_temperature": 24.8,
            "max_temperature": 26.1
        }
        mongo_manager.insert_one("realtime_data", test_data)
        print()

        # 演示查询文档
        print("🔍 查询文档演示:")
        results = mongo_manager.find_many("realtime_data", limit=5)
        for doc in results:
            print(f"   时间: {doc.get('timestamp')}, 平均温度: {doc.get('avg_temperature')}℃")
        print()

        # 演示聚合查询
        print("📊 聚合查询演示:")
        pipeline = [
            {"$group": {
                "_id": "$session_prefix",
                "avg_temp": {"$avg": "$avg_temperature"},
                "max_temp": {"$max": "$max_temperature"},
                "min_temp": {"$min": "$min_temperature"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 5}
        ]
        agg_results = mongo_manager.aggregate("realtime_data", pipeline)
        for result in agg_results:
            print(f"   会话: {result['_id']}, 平均温度: {result['avg_temp']:.1f}℃, 记录数: {result['count']}")
        print()

        # 获取集合列表
        print("📋 获取集合列表:")
        mongo_manager.get_collections()
        print()

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
    except Exception as e:
        print(f"❌ 程序运行出错: {e}")
    finally:
        # 断开连接
        mongo_manager.disconnect()
        print("👋 程序结束")


if __name__ == "__main__":
    main()