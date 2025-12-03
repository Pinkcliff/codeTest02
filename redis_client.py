import redis
import time
import json
from typing import Any, Optional, Dict, List


class RedisManager:
    """Redis连接和管理类"""

    def __init__(self, host: str = 'localhost', port: int = 6379,
                 password: Optional[str] = None, db: int = 0,
                 decode_responses: bool = True):
        """
        初始化Redis连接

        Args:
            host: Redis服务器地址
            port: Redis端口
            password: Redis密码
            db: 数据库编号
            decode_responses: 是否自动解码响应
        """
        self.host = host
        self.port = port
        self.password = password
        self.db = db
        self.decode_responses = decode_responses
        self.redis_client: Optional[redis.Redis] = None
        self.is_connected = False

    def connect(self) -> bool:
        """连接到Redis服务器"""
        try:
            self.redis_client = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=self.decode_responses,
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True
            )

            # 测试连接
            self.redis_client.ping()
            self.is_connected = True
            print(f"✅ 成功连接到Redis服务器: {self.host}:{self.port}")
            return True

        except redis.ConnectionError as e:
            print(f"❌ Redis连接失败: {e}")
            self.is_connected = False
            return False
        except Exception as e:
            print(f"❌ 连接错误: {e}")
            self.is_connected = False
            return False

    def disconnect(self):
        """断开Redis连接"""
        if self.redis_client:
            try:
                self.redis_client.close()
                self.is_connected = False
                print("🔌 Redis连接已关闭")
            except Exception as e:
                print(f"⚠️ 关闭连接时出错: {e}")

    def set_string(self, key: str, value: str, ex: Optional[int] = None) -> bool:
        """设置字符串值"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return False

        try:
            result = self.redis_client.set(key, value, ex=ex)
            print(f"✅ 设置字符串: {key} = {value}")
            return bool(result)
        except Exception as e:
            print(f"❌ 设置字符串失败: {e}")
            return False

    def get_string(self, key: str) -> Optional[str]:
        """获取字符串值"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return None

        try:
            value = self.redis_client.get(key)
            print(f"📖 获取字符串: {key} = {value}")
            return value
        except Exception as e:
            print(f"❌ 获取字符串失败: {e}")
            return None

    def set_hash(self, name: str, mapping: Dict[str, Any]) -> bool:
        """设置哈希表"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return False

        try:
            # 使用兼容的方式设置哈希表
            for key, value in mapping.items():
                self.redis_client.hset(name, key, value)
            print(f"✅ 设置哈希表: {name} = {mapping}")
            return True
        except Exception as e:
            print(f"❌ 设置哈希表失败: {e}")
            return False

    def get_hash(self, name: str) -> Dict[str, str]:
        """获取哈希表"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return {}

        try:
            data = self.redis_client.hgetall(name)
            print(f"📖 获取哈希表: {name} = {data}")
            return data
        except Exception as e:
            print(f"❌ 获取哈希表失败: {e}")
            return {}

    def set_list(self, name: str, values: List[str]) -> bool:
        """设置列表（会覆盖原有列表）"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return False

        try:
            # 先删除原有列表
            self.redis_client.delete(name)
            # 添加新元素
            for value in values:
                self.redis_client.rpush(name, value)
            print(f"✅ 设置列表: {name} = {values}")
            return True
        except Exception as e:
            print(f"❌ 设置列表失败: {e}")
            return False

    def get_list(self, name: str) -> List[str]:
        """获取列表"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return []

        try:
            data = self.redis_client.lrange(name, 0, -1)
            print(f"📖 获取列表: {name} = {data}")
            return data
        except Exception as e:
            print(f"❌ 获取列表失败: {e}")
            return []

    def delete_key(self, key: str) -> bool:
        """删除键"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return False

        try:
            result = self.redis_client.delete(key)
            if result:
                print(f"🗑️ 删除键: {key}")
            else:
                print(f"⚠️ 键不存在: {key}")
            return bool(result)
        except Exception as e:
            print(f"❌ 删除键失败: {e}")
            return False

    def exists_key(self, key: str) -> bool:
        """检查键是否存在"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return False

        try:
            exists = self.redis_client.exists(key)
            print(f"🔍 键存在性检查: {key} = {'存在' if exists else '不存在'}")
            return bool(exists)
        except Exception as e:
            print(f"❌ 检查键存在性失败: {e}")
            return False

    def get_info(self) -> Dict[str, Any]:
        """获取Redis服务器信息"""
        if not self.is_connected:
            print("⚠️ Redis未连接")
            return {}

        try:
            info = self.redis_client.info()
            print("📊 Redis服务器信息:")
            print(f"   Redis版本: {info.get('redis_version', 'Unknown')}")
            print(f"   运行时间: {info.get('uptime_in_seconds', 0)} 秒")
            print(f"   连接客户端数: {info.get('connected_clients', 0)}")
            print(f"   内存使用: {info.get('used_memory_human', 'Unknown')}")
            return info
        except Exception as e:
            print(f"❌ 获取服务器信息失败: {e}")
            return {}


def main():
    """主函数 - 演示Redis基本操作"""

    print("=" * 60)
    print("🚀 Redis连接演示程序")
    print("=" * 60)

    # 创建Redis管理器实例（可根据实际情况修改连接参数）
    redis_manager = RedisManager(
        host='localhost',      # Redis服务器地址
        port=6379,            # Redis端口
        password=None,        # 密码（如果有）
        db=0,                 # 数据库编号
        decode_responses=True # 自动解码响应
    )

    try:
        # 连接Redis
        if not redis_manager.connect():
            print("❌ 无法连接到Redis，程序退出")
            return

        # 获取服务器信息
        redis_manager.get_info()
        print()

        # 演示字符串操作
        print("🔤 字符串操作演示:")
        redis_manager.set_string("test:name", "张三")
        redis_manager.set_string("test:age", "25")
        redis_manager.get_string("test:name")
        redis_manager.get_string("test:age")
        print()

        # 演示哈希表操作
        print("🗂️ 哈希表操作演示:")
        user_data = {
            "name": "李四",
            "age": "30",
            "city": "北京",
            "email": "lisi@example.com"
        }
        redis_manager.set_hash("user:1001", user_data)
        redis_manager.get_hash("user:1001")
        print()

        # 演示列表操作
        print("📋 列表操作演示:")
        fruits = ["苹果", "香蕉", "橙子", "葡萄", "草莓"]
        redis_manager.set_list("fruits", fruits)
        redis_manager.get_list("fruits")
        print()

        # 设置带过期时间的键
        print("⏰ 过期键演示:")
        redis_manager.set_string("temp:token", "abc123", ex=10)  # 10秒后过期
        print("设置了一个10秒后过期的临时令牌")
        print()

        # 键存在性检查
        print("🔍 键存在性检查:")
        redis_manager.exists_key("test:name")
        redis_manager.exists_key("nonexistent:key")
        print()

        # 显示所有键（谨慎在生产环境中使用）
        print("🔑 当前数据库中的键:")
        if redis_manager.is_connected:
            try:
                keys = redis_manager.redis_client.keys("*")
                if keys:
                    for key in keys:
                        key_type = redis_manager.redis_client.type(key).decode() if isinstance(redis_manager.redis_client.type(key), bytes) else redis_manager.redis_client.type(key)
                        ttl = redis_manager.redis_client.ttl(key)
                        ttl_str = f"TTL: {ttl}秒" if ttl > 0 else "永不过期"
                        print(f"   {key} (类型: {key_type}, {ttl_str})")
                else:
                    print("   数据库为空")
            except Exception as e:
                print(f"   获取键列表失败: {e}")
        print()

        # 等待一段时间，然后检查过期键
        print("⏳ 等待12秒检查过期键...")
        time.sleep(12)
        redis_manager.exists_key("temp:token")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断程序")
    except Exception as e:
        print(f"❌ 程序运行出错: {e}")
    finally:
        # 断开连接
        redis_manager.disconnect()
        print("👋 程序结束")


if __name__ == "__main__":
    main()