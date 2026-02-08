import os
import json
import threading
import time
import paho.mqtt.client as mqtt
from typing import List, Dict, Any
from datetime import datetime
from supabase import create_client, Client

# Supabase配置
SUPABASE_URL = "https://xiljofsijsanvvhmxcsl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhpbGpvZnNpanNhbnZ2aG14Y3NsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkxNzAxNzQsImV4cCI6MjA4NDc0NjE3NH0.8ZqHtwzz2c-2vLOM0qAW4_NdPhTx8bM-Rlp5_CgOxXA"

# MQTT配置
MQTT_CONFIG = {
    "host": "k5f33d11.ala.cn-hangzhou.emqxsl.cn",
    "port": 8883,
    "username": "admin",
    "password": "public",
    "use_tls": True,
    "tls_version": mqtt.ssl.PROTOCOL_TLSv1_2,
    "insecure": False,
    "base_topic": "bms/telemetry/",
    "control_topic": "bms/control"
}

# 初始化Supabase客户端
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

class MQTTListener:
    def __init__(self):
        self.client = None
        self.connected = False
        self.subscriptions = set()
        # 添加连接延迟配置
        self.connection_delay = 0.1  # 100毫秒连接延迟
        
    def on_connect(self, client, userdata, flags, rc, properties=None):
        """MQTT连接回调 - 使用最新API版本"""
        if rc == 0:
            print("✅ MQTT连接成功")
            self.connected = True
            # 重新订阅所有主题
            for topic in self.subscriptions:
                time.sleep(self.connection_delay)  # 订阅之间添加延迟
                client.subscribe(topic)
                print(f"✅ 重新订阅主题: {topic}")
        else:
            print(f"❌ MQTT连接失败，返回码: {rc}")
            self.connected = False
            
    def on_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            
            print(f"📨 收到MQTT消息 - 主题: {topic}")
            print(f"   消息内容: {payload[:200]}...")  # 只打印前200字符
            
            # 解析消息
            data = json.loads(payload)
            
            # 从主题中提取packsn (bms/telemetry/PKG001 -> PKG001)
            packsn = topic.split('/')[-1] if '/' in topic else topic
            # 去除首尾空格
            packsn = packsn.strip()
            
            # 处理电池数据
            self.process_battery_data(packsn, data)
            
        except Exception as e:
            print(f"❌ 处理MQTT消息时出错: {e}")
            
    def on_disconnect(self, client, userdata, rc, properties=None):
        """MQTT断开连接回调 - 使用最新API版本"""
        print("⚠️ MQTT连接断开")
        self.connected = False
        
    def process_battery_data(self, packsn: str, data: Dict):
        """处理电池数据并插入数据库"""
        try:
            # 验证必需的字段
            required_fields = ['cell_voltages', 'cell_socs', 'cell_temperatures']
            for field in required_fields:
                if field not in data:
                    print(f"❌ 消息缺少必需字段: {field}")
                    return
            
            # 准备插入数据
            insert_data = {
                "packsn": packsn,
                "cell_voltages": data['cell_voltages'],
                "cell_socs": data['cell_socs'],
                "cell_temperatures": data['cell_temperatures'],
                "created_at": datetime.now().isoformat()
            }
            
            # 插入到Supabase
            response = supabase.table("battery_cell_data").insert(insert_data).execute()
            
            if response.data:
                print(f"✅ 成功插入电池包 {packsn} 的数据")
                print(f"   电压数组长度: {len(data['cell_voltages'])}")
                print(f"   SOC数组长度: {len(data['cell_socs'])}")
                print(f"   温度数组长度: {len(data['cell_temperatures'])}")
            else:
                print(f"❌ 插入电池包 {packsn} 数据失败")
                
        except Exception as e:
            print(f"❌ 处理电池数据时出错: {e}")
            
    def connect(self):
        """连接MQTT服务器"""
        try:
            # 使用最新的Callback API版本
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
            self.client.on_connect = self.on_connect
            self.client.on_message = self.on_message
            self.client.on_disconnect = self.on_disconnect
            
            # 设置认证
            self.client.username_pw_set(MQTT_CONFIG['username'], MQTT_CONFIG['password'])
            
            # 配置TLS
            if MQTT_CONFIG['use_tls']:
                self.client.tls_set(
                    cert_reqs=mqtt.ssl.CERT_NONE if MQTT_CONFIG['insecure'] else mqtt.ssl.CERT_REQUIRED,
                    tls_version=MQTT_CONFIG['tls_version']
                )
            
            # 连接
            time.sleep(self.connection_delay)  # 连接前添加延迟
            self.client.connect(MQTT_CONFIG['host'], MQTT_CONFIG['port'], 60)
            
            # 启动网络循环（在后台线程中）
            self.client.loop_start()
            
            # 等待连接建立
            for i in range(10):
                if self.connected:
                    return True
                time.sleep(0.5)
                
            return False
            
        except Exception as e:
            print(f"❌ MQTT连接失败: {e}")
            return False
            
    def subscribe_to_pack(self, packsn: str):
        """订阅指定电池包的主题"""
        try:
            # 去除首尾空格
            packsn = packsn.strip()
            if not packsn:  # 如果为空字符串，跳过
                print("⚠️ 电池包序列号为空，跳过订阅")
                return False
                
            topic = f"{MQTT_CONFIG['base_topic']}{packsn}"
            
            # 检查是否已经订阅了这个主题
            if topic in self.subscriptions:
                print(f"ℹ️ 已经订阅了主题 {topic}，跳过重复订阅")
                return True
                
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 订阅前添加延迟
                result, mid = self.client.subscribe(topic)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self.subscriptions.add(topic)
                    print(f"✅ 订阅主题: {topic}")
                    return True
                else:
                    print(f"❌ 订阅主题失败: {topic}")
                    return False
            else:
                print("⚠️ MQTT客户端未连接")
                return False
                
        except Exception as e:
            print(f"❌ 订阅主题时出错: {e}")
            return False
            
    def unsubscribe_from_pack(self, packsn: str):
        """取消订阅指定电池包的主题"""
        try:
            # 去除首尾空格
            packsn = packsn.strip()
            topic = f"{MQTT_CONFIG['base_topic']}{packsn}"
            
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 取消订阅前添加延迟
                self.client.unsubscribe(topic)
                self.subscriptions.discard(topic)
                print(f"✅ 取消订阅主题: {topic}")
                
        except Exception as e:
            print(f"❌ 取消订阅主题时出错: {e}")
            
    def publish(self, topic: str, payload: Dict, qos: int = 0, retain: bool = False):
        """发布消息到MQTT服务器"""
        try:
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 发布前添加延迟
                result = self.client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"✅ 发布成功 - 主题: {topic}")
                    return True
                else:
                    print(f"❌ 发布失败 - 主题: {topic}, 错误码: {result.rc}")
                    return False
            else:
                print("⚠️ MQTT客户端未连接，无法发布")
                return False
        except Exception as e:
            print(f"❌ 发布消息时出错: {e}")
            return False
            
    def disconnect(self):
        """断开MQTT连接"""
        try:
            if self.client:
                time.sleep(self.connection_delay)  # 断开连接前添加延迟
                self.client.loop_stop()
                self.client.disconnect()
                self.connected = False
                print("✅ MQTT连接已断开")
                
        except Exception as e:
            print(f"❌ 断开MQTT连接时出错: {e}")

# 全局MQTT监听器实例
mqtt_listener = MQTTListener()

def get_all_battery_packs() -> List[Dict[str, Any]]:
    """获取所有电池包信息"""
    try:
        response = supabase.table("battery_pack_info").select("packsn").execute()
        if response.data:
            # 清理每个packsn的首尾空格
            cleaned_packs = []
            for pack in response.data:
                packsn = pack.get('packsn')
                if packsn:
                    cleaned_packsn = packsn.strip()
                    if cleaned_packsn:  # 只添加非空字符串
                        pack['packsn'] = cleaned_packsn
                        cleaned_packs.append(pack)
            return cleaned_packs
        return []
    except Exception as e:
        print(f"❌ 获取电池包信息失败: {e}")
        return []

def get_unique_battery_packs() -> List[str]:
    """获取唯一的电池包序列号列表"""
    try:
        response = supabase.table("battery_pack_info").select("packsn").execute()
        if response.data:
            # 使用集合来存储唯一的packsn
            unique_packsn = set()
            cleaned_packsn = []
            
            for pack in response.data:
                packsn = pack.get('packsn')
                if packsn:
                    cleaned_packsn = packsn.strip()
                    if cleaned_packsn and cleaned_packsn not in unique_packsn:
                        unique_packsn.add(cleaned_packsn)
                        
            return list(unique_packsn)
        return []
    except Exception as e:
        print(f"❌ 获取唯一电池包信息失败: {e}")
        return []

def get_all_battery_pack_info() -> List[Dict[str, Any]]:
    """获取所有电池包详细信息"""
    try:
        response = supabase.table("battery_pack_info").select("*").execute()
        if response.data:
            return response.data
        return []
    except Exception as e:
        print(f"❌ 获取电池包详细信息失败: {e}")
        return []

def publish_battery_pack_info():
    """发布电池包信息到MQTT"""
    print("📤 开始发布电池包信息到MQTT...")
    
    # 获取所有电池包信息
    pack_info_list = get_all_battery_pack_info()
    
    if not pack_info_list:
        print("⚠️ 没有电池包信息可发布")
        return
    
    print(f"📊 准备发布 {len(pack_info_list)} 条电池包信息")
    
    # 逐条发布电池包信息
    for i, pack_info in enumerate(pack_info_list, 1):
        try:
            # 创建要发布的数据
            publish_data = {
                "id": pack_info.get('id'),
                "packsn": pack_info.get('packsn', '').strip(),
                "bmssn": pack_info.get('bmssn', '').strip(),
                "manufacturer": pack_info.get('manufacturer', '').strip(),
                "device_type": pack_info.get('device_type', '').strip(),
                "rated_capacity": pack_info.get('rated_capacity'),
                "rated_voltage": pack_info.get('rated_voltage'),
                "battery_type": pack_info.get('battery_type', '').strip(),
                "number_of_cells": pack_info.get('number_of_cells'),
                "number_of_temperature_sensors": pack_info.get('number_of_temperature_sensors'),
                "bms_hardware_version": pack_info.get('bms_hardware_version', '').strip(),
                "bms_software_version": pack_info.get('bms_software_version', '').strip(),
                "created_at": pack_info.get('created_at'),
                "updated_at": pack_info.get('updated_at'),
                "publish_time": datetime.now().isoformat()
            }
            
            # 移除None值
            publish_data = {k: v for k, v in publish_data.items() if v is not None}
            
            # 发布到MQTT
            success = mqtt_listener.publish(
                topic=MQTT_CONFIG['control_topic'],
                payload=publish_data,
                qos=1,  # 至少送达一次
                retain=False
            )
            
            if success:
                print(f"✅ 第{i}条电池包信息发布成功")
                print(f"   电池包: {publish_data.get('packsn')}")
                print(f"   BMS序列号: {publish_data.get('bmssn')}")
                print(f"   电芯数量: {publish_data.get('number_of_cells')}")
            else:
                print(f"❌ 第{i}条电池包信息发布失败")
            
            # 每条消息之间延迟0.5秒
            if i < len(pack_info_list):
                time.sleep(0.5)
                
        except Exception as e:
            print(f"❌ 处理第{i}条电池包信息时出错: {e}")
    
    print(f"✅ 电池包信息发布完成，共发布 {len(pack_info_list)} 条")

def start_mqtt_listener():
    """启动MQTT监听器"""
    print("🚀 启动MQTT监听器...")
    
    # 连接MQTT
    if not mqtt_listener.connect():
        print("❌ MQTT连接失败")
        return False
    
    # 获取所有唯一的电池包序列号
    unique_packsn = get_unique_battery_packs()
    print(f"📊 发现 {len(unique_packsn)} 个唯一的电池包")
    
    # 订阅每个唯一的电池包
    for packsn in unique_packsn:
        if packsn:
            # 添加延迟以减轻服务器压力
            time.sleep(0.1)  # 100毫秒延迟
            mqtt_listener.subscribe_to_pack(packsn)
    
    # 发布电池包信息
    publish_battery_pack_info()
    
    print("✅ MQTT监听器已启动")
    return True

def stop_mqtt_listener():
    """停止MQTT监听器"""
    print("🛑 停止MQTT监听器...")
    mqtt_listener.disconnect()
    print("✅ MQTT监听器已停止")

def get_battery_pack_info(packsn: str) -> Dict[str, Any]:
    """获取指定电池包的详细信息"""
    try:
        # 清理packsn的首尾空格
        packsn = packsn.strip()
        response = supabase.table("battery_pack_info").select("*").eq("packsn", packsn).execute()
        if response.data:
            return response.data[0]
        else:
            print(f"未找到电池包 {packsn} 的信息")
            return None
    except Exception as e:
        print(f"获取电池包信息时出错: {e}")
        return None

def read_latest_battery_cell_data() -> Dict[str, Any]:
    """读取battery_cell_data表的最后一个记录"""
    try:
        # 按创建时间倒序排列，只取第一条
        response = supabase.table("battery_cell_data").select("*").order("created_at", desc=True).limit(1).execute()
        
        if response.data and len(response.data) > 0:
            record = response.data[0]
            print(f"📊 电池单元数据表最新记录:")
            print(f"\n记录详情:")
            print(f"  ID: {record.get('id')}")
            print(f"  电池包序列号: {record.get('packsn')}")
            print(f"  电芯电压: {record.get('cell_voltages')}")
            print(f"  电芯SOC: {record.get('cell_socs')}")
            print(f"  电芯温度: {record.get('cell_temperatures')}")
            print(f"  创建时间: {record.get('created_at')}")
            return record
        else:
            print("电池单元表中没有数据")
            return None
            
    except Exception as e:
        print(f"读取电池单元数据时出错: {e}")
        return None

def read_battery_pack_info() -> List[Dict[str, Any]]:
    """读取battery_pack_info表的所有数据"""
    try:
        response = supabase.table("battery_pack_info").select("*").execute()
        
        if response.data:
            # 获取唯一的电池包序列号
            unique_packsn = set()
            unique_pack_info = []
            
            for record in response.data:
                packsn = record.get('packsn')
                if packsn and packsn not in unique_packsn:
                    unique_packsn.add(packsn)
                    unique_pack_info.append(record)
            
            print(f"\n成功读取到 {len(unique_pack_info)} 条唯一的电池包信息记录:")
            for i, record in enumerate(unique_pack_info, 1):
                print(f"\n电池包记录 {i}:")
                print(f"  ID: {record.get('id')}")
                print(f"  电池包序列号: {record.get('packsn')}")
                print(f"  BMS序列号: {record.get('bmssn')}")
                print(f"  制造商: {record.get('manufacturer')}")
                print(f"  设备类型: {record.get('device_type')}")
                print(f"  额定容量: {record.get('rated_capacity')}Ah")
                print(f"  额定电压: {record.get('rated_voltage')}V")
                print(f"  电池类型: {record.get('battery_type')}")
                print(f"  电芯数量: {record.get('number_of_cells')}")
                print(f"  温度传感器数量: {record.get('number_of_temperature_sensors')}")
                print(f"  BMS硬件版本: {record.get('bms_hardware_version')}")
                print(f"  BMS软件版本: {record.get('bms_software_version')}")
                print(f"  创建时间: {record.get('created_at')}")
                print(f"  更新时间: {record.get('updated_at')}")
            return response.data
        else:
            print("电池包信息表中没有数据")
            return []
            
    except Exception as e:
        print(f"读取电池包信息时出错: {e}")
        return []

def monitor_mqtt_status():
    """监控MQTT状态"""
    while True:
        if mqtt_listener.connected:
            print("📡 MQTT监听器运行中...")
        else:
            print("⚠️ MQTT连接断开，尝试重连...")
            time.sleep(1)  # 重连前等待1秒
            start_mqtt_listener()
        time.sleep(30)  # 每30秒检查一次

if __name__ == "__main__":
    print("🔋 电池管理系统数据工具")
    print("=" * 50)
    
    if start_mqtt_listener():
        print("✅ MQTT监听器启动成功")
    else:
        print("❌ MQTT监听器启动失败")

    time.sleep(3)

    # 读取电池包信息数据
    pack_data = read_battery_pack_info()
    
    # 读取电池单元数据（只显示最后一条记录）
    latest_cell_data = read_latest_battery_cell_data()
    
    # 启动MQTT监听器
    mqtt_thread = threading.Thread(target=monitor_mqtt_status, daemon=True)
    mqtt_thread.start()
    
    # 保持程序运行，持续监听MQTT
    print("\n" + "="*50)
    print("MQTT监听器持续运行中...")
    print("按 Ctrl+C 退出程序")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n⏹️ 程序退出中...")
        stop_mqtt_listener()
        print("✅ 程序已退出")