"""
极简MQTT数据采集器 - 支持SSL/TLS连接EMQX (Windows兼容版)
支持多个主题订阅
目标：通过SSL/TLS验证EMQX到Supabase的数据流
"""
import json
import os
import sys
import signal
import time
import ssl
from datetime import datetime
from typing import Dict, Any, Optional, List

import paho.mqtt.client as mqtt
from supabase import create_client
from dotenv import load_dotenv
import logging

# 加载配置
load_dotenv()

class SimpleMQTTIngestor:
    def __init__(self):
        # 基础配置
        self.mqtt_host = os.getenv("MQTT_HOST", "localhost")
        self.mqtt_port = int(os.getenv("MQTT_PORT", 1883))
        self.mqtt_user = os.getenv("MQTT_USERNAME", "")
        self.mqtt_pass = os.getenv("MQTT_PASSWORD", "")
        
        # 支持多个主题订阅
        self.mqtt_topics = self._parse_topics(os.getenv("MQTT_TOPICS", "bms/status,bms/telemetry"))
        
        # SSL/TLS配置
        self.use_tls = os.getenv("MQTT_USE_TLS", "false").lower() == "true"
        self.tls_ca_cert = os.getenv("MQTT_TLS_CA_CERT", "")
        self.tls_certfile = os.getenv("MQTT_TLS_CERTFILE", "")
        self.tls_keyfile = os.getenv("MQTT_TLS_KEYFILE", "")
        self.tls_insecure = os.getenv("MQTT_TLS_INSECURE", "false").lower() == "true"
        self.tls_version = os.getenv("MQTT_TLS_VERSION", "tlsv1.2")
        self.tls_ciphers = os.getenv("MQTT_TLS_CIPHERS", "")
        
        # Supabase配置
        self.supabase_url = os.getenv("SUPABASE_URL", "")
        self.supabase_key = os.getenv("SUPABASE_KEY", "")
        
        # 初始化客户端
        self.mqtt_client: Optional[mqtt.Client] = None
        self.supabase_client = None
        
        # 统计
        self.message_count = 0
        self.error_count = 0
        self.running = True
        
        # 设置日志（Windows兼容）
        self._setup_windows_safe_logging()
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        # 打印配置
        self.log_config()
    
    def _parse_topics(self, topics_str: str) -> List[str]:
        """解析主题字符串，支持逗号、分号分隔"""
        if not topics_str:
            return ["bms/telemetry"]  # 默认主题
        
        # 去除空格，按逗号或分号分割
        topics = []
        for topic in topics_str.replace(';', ',').split(','):
            topic = topic.strip()
            if topic:  # 只添加非空主题
                topics.append(topic)
        
        return topics if topics else ["bms/telemetry"]
    
    def _setup_windows_safe_logging(self):
        """设置Windows安全的日志配置（避免Unicode编码问题）"""
        # 移除所有现有的处理器
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 创建logger
        self.logger = logging.getLogger("MQTTIngestor")
        self.logger.setLevel(logging.INFO)
        
        # 清除已有的handler
        self.logger.handlers.clear()
        
        # Windows兼容的控制台handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # 使用简单的格式，避免Unicode字符
        console_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)
        
        # 文件handler（使用UTF-8编码）
        try:
            file_handler = logging.FileHandler('mqtt_ingestor.log', encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_format = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_format)
            self.logger.addHandler(file_handler)
        except Exception as e:
            self.logger.error(f"无法创建日志文件: {e}")
    
    def log_config(self):
        """打印当前配置（使用文本替代表情符号）"""
        self.logger.info("=" * 50)
        self.logger.info("MQTT数据采集器配置")
        self.logger.info("=" * 50)
        self.logger.info(f"MQTT服务器: {self.mqtt_host}:{self.mqtt_port}")
        self.logger.info(f"使用TLS/SSL: {'是' if self.use_tls else '否'}")
        if self.use_tls:
            self.logger.info(f"TLS版本: {self.tls_version}")
            self.logger.info(f"不验证证书: {'是' if self.tls_insecure else '否'}")
        self.logger.info(f"订阅主题数: {len(self.mqtt_topics)}")
        for i, topic in enumerate(self.mqtt_topics, 1):
            self.logger.info(f"  主题 {i}: {topic}")
        self.logger.info(f"Supabase: {'已配置' if self.supabase_url and self.supabase_key else '未配置'}")
        self.logger.info("=" * 50)
    
    def signal_handler(self, signum, frame):
        """处理退出信号"""
        self.logger.info(f"收到信号 {signum}，正在关闭...")
        self.running = False
        if self.mqtt_client:
            self.mqtt_client.disconnect()
    
    def setup_supabase(self) -> bool:
        """初始化Supabase客户端"""
        try:
            if not self.supabase_url or not self.supabase_key:
                self.logger.error("Supabase配置缺失")
                self.logger.info("请在.env文件中设置SUPABASE_URL和SUPABASE_KEY")
                return False
            
            self.supabase_client = create_client(self.supabase_url, self.supabase_key)
            self.logger.info("Supabase客户端初始化成功")
            return True
        except Exception as e:
            self.logger.error(f"Supabase初始化失败: {e}")
            return False
    
    def setup_tls(self):
        """配置TLS/SSL连接"""
        try:
            # 设置TLS版本
            tls_version_map = {
                "tlsv1": ssl.PROTOCOL_TLSv1,
                "tlsv1.1": ssl.PROTOCOL_TLSv1_1,
                "tlsv1.2": ssl.PROTOCOL_TLSv1_2,
                "tlsv1.3": ssl.PROTOCOL_TLS,
            }
            
            tls_version = tls_version_map.get(self.tls_version.lower(), ssl.PROTOCOL_TLS)
            
            # 配置TLS
            if self.tls_ca_cert:
                self.logger.info(f"使用CA证书: {self.tls_ca_cert}")
                self.mqtt_client.tls_set(
                    ca_certs=self.tls_ca_cert,
                    certfile=self.tls_certfile if self.tls_certfile else None,
                    keyfile=self.tls_keyfile if self.tls_keyfile else None,
                    cert_reqs=ssl.CERT_NONE if self.tls_insecure else ssl.CERT_REQUIRED,
                    tls_version=tls_version,
                    ciphers=self.tls_ciphers if self.tls_ciphers else None
                )
            else:
                self.logger.info("使用默认系统CA证书")
                self.mqtt_client.tls_set(
                    cert_reqs=ssl.CERT_NONE if self.tls_insecure else ssl.CERT_REQUIRED,
                    tls_version=tls_version,
                    ciphers=self.tls_ciphers if self.tls_ciphers else None
                )
            
            if self.tls_insecure:
                self.logger.warning("警告: TLS证书验证已禁用（仅用于测试）")
                # 禁用主机名验证
                self.mqtt_client.tls_insecure_set(True)
            
            self.logger.info("TLS/SSL配置完成")
            return True
            
        except Exception as e:
            self.logger.error(f"TLS配置失败: {e}")
            return False
    
    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """MQTT连接回调（使用V2 API）"""
        if reason_code == 0:
            self.logger.info(f"[OK] MQTT连接成功（使用{'TLS/SSL' if self.use_tls else '普通连接'}）")
            
            # 订阅所有主题
            success_count = 0
            for topic in self.mqtt_topics:
                # QoS 1: 确保消息至少被接收一次
                result = client.subscribe(topic, qos=1)
                if result[0] == 0:
                    self.logger.info(f"[OK] 订阅成功: {topic}")
                    success_count += 1
                else:
                    self.logger.error(f"[ERROR] 订阅失败: {topic} (错误码: {result[0]})")
            
            self.logger.info(f"订阅完成: {success_count}/{len(self.mqtt_topics)} 个主题订阅成功")
        else:
            error_messages = {
                1: "不正确的协议版本",
                2: "客户端标识符无效",
                3: "服务器不可用",
                4: "错误的用户名或密码",
                5: "未经授权"
            }
            error_msg = error_messages.get(reason_code, f"未知错误代码: {reason_code}")
            self.logger.error(f"[ERROR] MQTT连接失败: {error_msg} (代码: {reason_code})")
            
            # 如果是TLS连接失败，给出更多提示
            if self.use_tls and reason_code in [1, 3, 5]:
                self.logger.info("TLS连接失败可能原因:")
                self.logger.info("1. 服务器端口错误（TLS通常使用8883端口）")
                self.logger.info("2. 证书不匹配或过期")
                self.logger.info("3. TLS版本不支持")
                self.logger.info("4. 防火墙阻止")
    
    def on_mqtt_message(self, client, userdata, msg):
        """MQTT消息回调"""
        try:
            # 解析消息
            payload = msg.payload.decode('utf-8').lower()
            
            # 尝试解析JSON
            try:
                data = json.loads(payload)
                is_json = True
            except json.JSONDecodeError:
                # 如果不是JSON，保存为文本
                data = {"raw_data": payload}
                is_json = False
            
            data_str = json.dumps(data)
            # self.logger.debug(f"收到消息 - 主题: {msg.topic}, QoS: {msg.qos}, payload: {data_str}")
            self.logger.info(f"收到消息 - 主题: {msg.topic}, QoS: {msg.qos}, payload: {data_str}")

            # 存储到Supabase
            self.save_to_supabase(data)
            
            # 更新统计
            self.message_count += 1
            if self.message_count % 10 == 0:
                self.logger.info(f"已处理 {self.message_count} 条消息，错误: {self.error_count}")
                
        except Exception as e:
            self.logger.error(f"消息处理失败: {e}")
            self.error_count += 1
    
    def save_to_supabase(self, data: Dict[str, Any]):
        """保存数据到Supabase"""
        try:
            if not self.supabase_client:
                self.logger.error("Supabase客户端未初始化")
                return
            
            # 根据数据结构选择表
            table_name = self.determine_table(data)

            # 确保数据包含 `created_at` 字段。如果原数据没有，则使用当前UTC时间。
            # Supabase会自动识别此字段。
            data_to_insert = data.copy()  # 避免修改原始数据
            if 'created_at' not in data_to_insert:
                data_to_insert['created_at'] = datetime.utcnow().isoformat() + "Z"

            # 插入数据
            response = self.supabase_client.table(table_name).insert(data_to_insert).execute()            
                        
            # 检查响应
            if hasattr(response, 'data'):
                self.logger.debug(f"数据插入成功到 {table_name}: {data.get('device_id', 'unknown')}")
            else:
                self.logger.error(f"Supabase插入失败: {response}")
                self.error_count += 1
                
        except Exception as e:
            self.logger.error(f"保存到Supabase失败: {e}")
            self.error_count += 1
    
    def determine_table(self, data: Dict[str, Any]) -> str:
        """根据数据内容确定表名"""
        data_str = str(data).lower()
        
        # 检查是否有遥测数据字段
        telemetry_fields = ["voltage", "current", "soc", "temperature", "power", "capacity", "energy"]
        if any(field in data_str for field in telemetry_fields):
            return "telemetry_data"
        
        # 检查告警字段
        alert_fields = ["alert", "warning", "error", "fault", "alarm", "critical"]
        if any(field in data_str for field in alert_fields):
            return "alerts"

        # 检查状态字段
        status_fields = ["status", "state", "mode", "online", "offline", "connected", "disconnected"]
        if any(field in data_str for field in status_fields):
            return "device_status"

        # 根据主题判断
        if "telemetry" in data.get("topic", "").lower():
            return "telemetry_data"
        elif "status" in data.get("topic", "").lower():
            return "device_status"
        elif "alert" in data.get("topic", "").lower() or "alarm" in data.get("topic", "").lower():
            return "alerts"
        
        # 默认表
        return "raw_messages"
    
    def on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """MQTT断开连接回调（使用V2 API）"""
        if reason_code == 0:
            self.logger.info("正常断开MQTT连接")
        else:
            self.logger.warning(f"MQTT连接断开，原因码: {reason_code}")
            # 如果不是主动断开，尝试重连
            if self.running:
                self.logger.info("10秒后尝试重新连接...")
                time.sleep(10)
                try:
                    client.reconnect()
                except:
                    pass
    
    def on_mqtt_log(self, client, userdata, level, buf):
        """MQTT日志回调"""
        if level == mqtt.MQTT_LOG_ERR:
            self.logger.error(f"MQTT错误: {buf}")
        elif level == mqtt.MQTT_LOG_WARNING:
            self.logger.warning(f"MQTT警告: {buf}")
        elif level == mqtt.MQTT_LOG_INFO:
            self.logger.info(f"MQTT信息: {buf}")
        else:
            self.logger.debug(f"MQTT调试: {buf}")
    
    def start(self):
        """启动服务"""
        # 初始化Supabase
        if not self.setup_supabase():
            self.logger.error("Supabase初始化失败，退出")
            sys.exit(1)
        
        # 初始化MQTT客户端（使用V2回调API）
        try:
            # 使用回调API版本2
            self.mqtt_client = mqtt.Client(
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
                client_id=f"mqtt_ingestor_{int(time.time())}"
            )
            
            # 设置回调
            self.mqtt_client.on_connect = self.on_mqtt_connect
            self.mqtt_client.on_message = self.on_mqtt_message
            self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
            self.mqtt_client.on_log = self.on_mqtt_log
            
            # 设置认证
            if self.mqtt_user and self.mqtt_pass:
                self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_pass)
            
            # 配置TLS/SSL
            if self.use_tls:
                if not self.setup_tls():
                    self.logger.error("TLS配置失败，退出")
                    sys.exit(1)
            
            # 设置最后遗言
            self.mqtt_client.will_set("ingestor/status", "offline", qos=1, retain=True)
            
            # 设置连接参数
            self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
            
            # 连接MQTT
            self.logger.info(f"连接MQTT服务器 {self.mqtt_host}:{self.mqtt_port}")
            self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
            
            # 发布上线状态
            self.mqtt_client.publish("ingestor/status", "online", qos=1, retain=True)
            
            # 启动MQTT循环（非阻塞方式）
            self.mqtt_client.loop_start()
            
            # 主循环
            self.logger.info("MQTT服务启动成功，等待消息...")
            while self.running:
                # 每分钟打印一次统计
                time.sleep(60)
                self.logger.info(f"运行统计 - 总消息: {self.message_count}, 错误: {self.error_count}")
                
        except KeyboardInterrupt:
            self.logger.info("收到键盘中断，关闭连接...")
        except Exception as e:
            self.logger.error(f"MQTT连接失败: {e}")
            sys.exit(1)
        finally:
            self.cleanup()
    
    def cleanup(self):
        """清理资源"""
        self.logger.info("正在清理资源...")
        
        # 发布离线状态
        if self.mqtt_client:
            try:
                self.mqtt_client.publish("ingestor/status", "offline", qos=1, retain=True)
                time.sleep(1)
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except:
                pass
        
        self.logger.info(f"服务停止。总计处理消息: {self.message_count}, 错误: {self.error_count}")

def main():
    """主函数"""
    print("=" * 50)
    print("MQTT数据采集器 v2.0 (Windows兼容版)")
    print("支持多主题订阅")
    print("=" * 50)
    
    ingestor = SimpleMQTTIngestor()
    ingestor.start()

if __name__ == "__main__":
    main()