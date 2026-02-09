import os
import json
import threading
import time
import pytz
import paho.mqtt.client as mqtt
from typing import List, Dict, Any
from datetime import datetime
from supabase import create_client, Client

beijing_tz = pytz.timezone('Asia/Shanghai')

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
            print("[OK] MQTT连接成功")
            self.connected = True
            # 重新订阅所有主题
            for topic in self.subscriptions:
                time.sleep(self.connection_delay)  # 订阅之间添加延迟
                client.subscribe(topic)
                print(f"[OK] 重新订阅主题: {topic}")
        else:
            print(f"[ERROR] MQTT连接失败，返回码: {rc}")
            self.connected = False
            
    def on_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            print(f"[RECEIVE] 收到MQTT消息 - 主题: {topic}  消息内容: {payload[:68]}...")
            
            # 解析消息
            data = json.loads(payload)
            
            # 从主题中提取packsn (bms/telemetry/PKG001 -> PKG001)
            packsn = topic.split('/')[-1] if '/' in topic else topic
            # 去除首尾空格
            packsn = packsn.strip()
            
            # 处理电池数据
            self.process_battery_data(packsn, data)
            
        except Exception as e:
            print(f"[ERROR] 处理MQTT消息时出错: {e}")
            
    def on_disconnect(self, client, userdata, flags,rc, properties=None):
        """MQTT断开连接回调 - 使用最新API版本"""
        print("[WARNING] MQTT连接断开")
        self.connected = False
        
    def process_battery_data(self, packsn: str, data: Dict):
        """处理电池数据并插入数据库"""
        try:
            # 验证必需的字段
            required_fields = ['cell_voltages', 'cell_socs', 'cell_temperatures']
            for field in required_fields:
                if field not in data:
                    print(f"[ERROR] 消息缺少必需字段: {field}")
                    return
            
            # 准备插入数据
            insert_data = {
                "packsn": packsn,
                "cell_voltages": data['cell_voltages'],
                "cell_socs": data['cell_socs'],
                "cell_temperatures": data['cell_temperatures'],
                # "created_at": datetime.now().isoformat()
                "created_at": datetime.now(beijing_tz).replace(microsecond=0).isoformat()
            }
            
            # 插入到Supabase
            response = supabase.table("battery_cell_data").insert(insert_data).execute()
            
            if response.data:
                # print(f"[OK] 成功插入电池包 {packsn} 的数据")
                # print(f"   电压数组长度: {len(data['cell_voltages'])}")
                # print(f"   SOC数组长度: {len(data['cell_socs'])}")
                # print(f"   温度数组长度: {len(data['cell_temperatures'])}")
                pass
            else:
                print(f"[ERROR] 插入电池包 {packsn} 数据失败")
                
        except Exception as e:
            print(f"[ERROR] 处理电池数据时出错: {e}")
            
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
            print(f"[ERROR] MQTT连接失败: {e}")
            return False
            
    def subscribe_packs_all(self):
        """订阅指定电池包的主题"""
        try:
            topic = f"{MQTT_CONFIG['base_topic']}{'#'}"
            
            # 检查是否已经订阅了这个主题
            if topic in self.subscriptions:
                print(f"[INFO] 已经订阅了主题 {topic}，跳过重复订阅")
                return True
                
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 订阅前添加延迟
                result, mid = self.client.subscribe(topic)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self.subscriptions.add(topic)
                    print(f"[OK] 订阅主题: {topic}")
                    return True
                else:
                    print(f"[ERROR] 订阅主题失败: {topic}")
                    return False
            else:
                print("[WARNING] MQTT客户端未连接")
                return False
                
        except Exception as e:
            print(f"[ERROR] 订阅主题时出错: {e}")
            return False


    def subscribe_to_pack(self, packsn: str):
        """订阅指定电池包的主题"""
        try:
            # 去除首尾空格
            packsn = packsn.strip()
            if not packsn:  # 如果为空字符串，跳过
                print("[WARNING] 电池包序列号为空，跳过订阅")
                return False
                
            topic = f"{MQTT_CONFIG['base_topic']}{packsn}"
            
            # 检查是否已经订阅了这个主题
            if topic in self.subscriptions:
                print(f"[INFO] 已经订阅了主题 {topic}，跳过重复订阅")
                return True
                
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 订阅前添加延迟
                result, mid = self.client.subscribe(topic)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self.subscriptions.add(topic)
                    print(f"[OK] 订阅主题: {topic}")
                    return True
                else:
                    print(f"[ERROR] 订阅主题失败: {topic}")
                    return False
            else:
                print("[WARNING] MQTT客户端未连接")
                return False
                
        except Exception as e:
            print(f"[ERROR] 订阅主题时出错: {e}")
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
                print(f"[OK] 取消订阅主题: {topic}")
                
        except Exception as e:
            print(f"[ERROR] 取消订阅主题时出错: {e}")
            
    def publish(self, topic: str, payload: Dict, qos: int = 0, retain: bool = False):
        """发布消息到MQTT服务器"""
        try:
            if self.connected and self.client:
                time.sleep(self.connection_delay)  # 发布前添加延迟
                result = self.client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    # print(f"[OK] 发布成功 - 主题: {topic}")
                    return True
                else:
                    print(f"[ERROR] 发布失败 - 主题: {topic}, 错误码: {result.rc}")
                    return False
            else:
                print("[WARNING] MQTT客户端未连接，无法发布")
                return False
        except Exception as e:
            print(f"[ERROR] 发布消息时出错: {e}")
            return False
            
    def disconnect(self):
        """断开MQTT连接"""
        try:
            if self.client:
                time.sleep(self.connection_delay)  # 断开连接前添加延迟
                self.client.loop_stop()
                self.client.disconnect()
                self.connected = False
                print("[OK] MQTT连接已断开")
                
        except Exception as e:
            print(f"[ERROR] 断开MQTT连接时出错: {e}")

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
        print(f"[ERROR] 获取电池包信息失败: {e}")
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
        print(f"[ERROR] 获取唯一电池包信息失败: {e}")
        return []

def get_all_battery_pack_info() -> List[Dict[str, Any]]:
    """获取所有电池包详细信息"""
    try:
        response = supabase.table("battery_pack_info").select("*").execute()
        if response.data:
            return response.data
        return []
    except Exception as e:
        print(f"[ERROR] 获取电池包详细信息失败: {e}")
        return []

def initilize_battery_pack_info_publishing():
    """发布电池包信息到MQTT"""
    print("[PUBLISH] 开始发布电池包信息到MQTT...")

    publish_status = publish_battery_pack_info()
        
    if len(publish_status) == 0:
        print("[WARNING] 没有电池包信息可发布")
        return

    # print(f"[STATS] 准备发布 {len(publish_status)} 条电池包信息")

    for i, statu in enumerate(publish_status, 1):
        if statu:
            print(f"[OK] 第{i}条电池包信息发布成功")
        else:
            print(f"[ERROR] 第{i}条电池包信息发布失败")
        
        time.sleep(0.5)
    
    print(f"[OK] 电池包信息发布完成，共发布 {sum(publish_status)} 条")

def publish_battery_pack_info():
    """发布电池包信息到MQTT"""
    if not (pack_info_list := get_all_battery_pack_info()):
        return []
    
    publish_status = []
    
    for i, pack_info in enumerate(pack_info_list):
        try:
            # 简洁的数据构建
            publish_data = {
                key: pack_info.get(key, '').strip() 
                if isinstance(pack_info.get(key), str) 
                else pack_info.get(key)
                for key in [
                    'id', 'packsn', 'bmssn', 'manufacturer', 'device_type',
                    'rated_capacity', 'rated_voltage', 'battery_type', 'number_of_cells',
                    'number_of_temperature_sensors', 'bms_hardware_version',
                    'bms_software_version', 'created_at', 'updated_at'
                ]
                if pack_info.get(key) is not None
            }
            
            publish_data['publish_time'] = datetime.now(beijing_tz).replace(microsecond=0).isoformat()
            
            # 发布并记录状态
            success = mqtt_listener.publish(
                topic=MQTT_CONFIG['control_topic'],
                payload=publish_data,
                qos=1,
                retain=False
            )
            
            publish_status.append(int(success))
            
            # 条件延迟
            if i < len(pack_info_list) - 1:
                time.sleep(0.5)
                
        except Exception:
            publish_status.append(0)
    
    return publish_status

def start_mqtt_listener():
    """启动MQTT监听器"""
    print("[START] 启动MQTT监听器...")
    
    # 连接MQTT
    if not mqtt_listener.connect():
        print("[ERROR] MQTT连接失败")
        return False
    
    # 获取所有唯一的电池包序列号
    unique_packsn = get_unique_battery_packs()
    print(f"[STATS] 发现 {len(unique_packsn)} 个唯一的电池包")
    
    mqtt_listener.subscribe_packs_all()
    
    # 发布电池包信息
    initilize_battery_pack_info_publishing()
    
    print("[OK] MQTT监听器已启动")
    return True

def stop_mqtt_listener():
    """停止MQTT监听器"""
    print("[STOP] 停止MQTT监听器...")
    mqtt_listener.disconnect()
    print("[OK] MQTT监听器已停止")

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
            print(f"[STATS] 电池单元数据表最新记录:")
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
            print("[RUNNING] MQTT监听器运行中...")
            publish_status = publish_battery_pack_info()
            if sum(publish_status) > 0:
                print(f"[OK] 电池包信息发布完成，共发布 {sum(publish_status)} 条")
        else:
            print("[WARNING] MQTT连接断开，尝试重连...")
            time.sleep(1)  # 重连前等待1秒
            start_mqtt_listener()
        time.sleep(30)  # 每30秒检查一次

if __name__ == "__main__":
    print("[BATTERY] 电池管理系统数据工具")
    print("=" * 50)
    
    # 读取电池包信息数据
    pack_data = read_battery_pack_info()
    
    # 读取电池单元数据（只显示最后一条记录）
    # latest_cell_data = read_latest_battery_cell_data()

    if start_mqtt_listener():
        print("[OK] MQTT监听器启动成功")
    else:
        print("[ERROR] MQTT监听器启动失败")

    time.sleep(3)
    
    # 启动MQTT监听器
    mqtt_thread = threading.Thread(target=monitor_mqtt_status, daemon=True)
    mqtt_thread.start()
    
    # 保持程序运行，持续监听MQTT
    print("\n" + "="*50 + "\n")
    print("MQTT监听器持续运行中...")
    print("按 Ctrl+C 退出程序")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\n[STOP] 程序退出中...")
        stop_mqtt_listener()
        print("[OK] 程序已退出")