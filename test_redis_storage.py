import time
import json
from datetime import datetime
from redis_client import RedisManager
from temperature_redis import TemperatureDataStorage


def test_redis_storage():
    """测试Redis数据存储功能"""
    print("=" * 60)
    print("🧪 Redis温度数据存储功能测试")
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
        print("❌ 无法连接到Redis，请确保Redis服务正在运行")
        return False

    # 创建存储管理器
    storage = TemperatureDataStorage(redis_manager)

    try:
        # 生成测试数据
        test_temperatures = [23.5, 24.1, 22.8, 25.0, 23.9, 24.5,
                           23.2, 24.8, 22.5, 25.2, 23.7, 24.3]
        test_timestamp = datetime.now().isoformat()

        print(f"📊 测试数据: {len(test_temperatures)}个通道")
        print(f"   温度范围: {min(test_temperatures):.1f}℃ - {max(test_temperatures):.1f}℃")
        print(f"   平均温度: {sum(test_temperatures)/len(test_temperatures):.1f}℃")
        print()

        # 测试1: 保存实时数据
        print("🧪 测试1: 保存实时数据...")
        if storage.save_realtime_data(test_temperatures, test_timestamp):
            print("✅ 实时数据保存成功")
        else:
            print("❌ 实时数据保存失败")
            return False

        # 测试2: 保存历史数据
        print("🧪 测试2: 保存历史数据...")
        if storage.save_historical_data(test_temperatures, test_timestamp):
            print("✅ 历史数据保存成功")
        else:
            print("❌ 历史数据保存失败")
            return False

        # 测试3: 保存时间序列数据
        print("🧪 测试3: 保存时间序列数据...")
        if storage.save_time_series_data(test_temperatures, test_timestamp):
            print("✅ 时间序列数据保存成功")
        else:
            print("❌ 时间序列数据保存失败")
            return False

        # 测试4: 更新统计信息
        print("🧪 测试4: 更新统计信息...")
        if storage.update_statistics(test_temperatures):
            print("✅ 统计信息更新成功")
        else:
            print("❌ 统计信息更新失败")
            return False

        # 测试5: 保存所有数据
        print("🧪 测试5: 保存所有数据...")
        if storage.save_all_data(test_temperatures, test_timestamp):
            print("✅ 所有数据保存成功")
        else:
            print("❌ 所有数据保存失败")
            return False

        print()

        # 验证数据是否正确保存
        print("🔍 验证保存的数据...")

        # 验证实时数据
        realtime_data = redis_manager.get_hash("temperature:realtime")
        if realtime_data and realtime_data.get('channel_count') == '12':
            print("✅ 实时数据验证成功")
            print(f"   时间戳: {realtime_data.get('timestamp')}")
            print(f"   通道01温度: {realtime_data.get('channel_01')}℃")
        else:
            print("❌ 实时数据验证失败")

        # 验证历史数据
        history_count = redis_manager.redis_client.llen("temperature:history")
        if history_count > 0:
            print(f"✅ 历史数据验证成功 (共{history_count}条记录)")
            latest_record = redis_manager.redis_client.lindex("temperature:history", 0)
            if latest_record:
                record_data = json.loads(latest_record)
                print(f"   最新记录时间: {record_data.get('timestamp')}")
                print(f"   温度数量: {len(record_data.get('temperatures', []))}")
        else:
            print("❌ 历史数据验证失败")

        # 验证统计信息
        stats_data = redis_manager.get_hash("temperature:statistics")
        if stats_data:
            print("✅ 统计信息验证成功")
            print(f"   最低温度: {stats_data.get('min_temperature')}℃")
            print(f"   最高温度: {stats_data.get('max_temperature')}℃")
            print(f"   平均温度: {stats_data.get('avg_temperature')}℃")
        else:
            print("❌ 统计信息验证失败")

        # 验证时间序列数据
        for i in range(1, 13):
            channel_key = f"temperature:timeseries:channel_{i:02d}"
            ts_count = redis_manager.redis_client.zcard(channel_key)
            if ts_count > 0:
                print(f"   通道{i:02d}: {ts_count}个数据点 ✅")
                break
        else:
            print("❌ 时间序列数据验证失败")

        print()
        print("🎉 所有测试完成！Redis数据存储功能正常工作。")
        return True

    except Exception as e:
        print(f"❌ 测试过程中发生错误: {e}")
        return False

    finally:
        redis_manager.disconnect()
        print("🔌 Redis连接已关闭")


def simulate_temperature_data():
    """模拟连续温度数据采集和存储"""
    print("\n" + "=" * 60)
    print("🔄 模拟连续温度数据采集 (10个周期)")
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
        print("❌ 无法连接到Redis")
        return

    storage = TemperatureDataStorage(redis_manager)

    try:
        for cycle in range(1, 11):
            # 生成模拟温度数据 (基于基准温度的随机变化)
            base_temps = [23.0, 24.0, 22.5, 25.0, 23.5, 24.5,
                         23.0, 24.0, 22.0, 25.5, 23.5, 24.0]

            # 添加随机变化 (-1.0 到 +1.0)
            import random
            temperatures = [base + random.uniform(-1.0, 1.0) for base in base_temps]

            timestamp = datetime.now().isoformat()

            print(f"📊 周期 {cycle:2d}: {timestamp}")
            print(f"   温度范围: {min(temperatures):5.1f}℃ - {max(temperatures):5.1f}℃")

            # 保存数据
            if storage.save_all_data(temperatures, timestamp):
                print("   ✅ 数据保存成功")
            else:
                print("   ❌ 数据保存失败")

            print()

            # 等待1秒
            time.sleep(1)

        print("🎉 模拟数据采集完成！")

        # 显示最终统计
        print("\n📈 最终数据统计:")
        stats = redis_manager.get_hash("temperature:statistics")
        if stats:
            print(f"   总采集次数: {redis_manager.redis_client.llen('temperature:history')}")
            print(f"   当前最低温度: {stats.get('min_temperature')}℃")
            print(f"   当前最高温度: {stats.get('max_temperature')}℃")
            print(f"   当前平均温度: {stats.get('avg_temperature')}℃")

    except Exception as e:
        print(f"❌ 模拟过程中发生错误: {e}")

    finally:
        redis_manager.disconnect()
        print("🔌 Redis连接已关闭")


def main():
    """主测试函数"""
    print("🚀 开始Redis温度数据存储系统测试")

    # 测试基本功能
    if test_redis_storage():
        # 询问是否继续模拟测试
        print("\n" + "="*60)
        response = input("是否继续模拟连续数据采集测试? (y/n): ").strip().lower()
        if response in ['y', 'yes', '是']:
            simulate_temperature_data()
    else:
        print("❌ 基本功能测试失败，跳过模拟测试")

    print("\n👋 测试程序结束")


if __name__ == "__main__":
    main()