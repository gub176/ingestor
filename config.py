"""
配置管理
可以根据需要从环境变量、配置文件或命令行参数读取配置
"""
import os
from typing import Dict, Any

class Config:
    # MQTT配置
    MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
    MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
    MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
    MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
    MQTT_TOPICS = os.getenv("MQTT_TOPICS", "bms/+/telemetry").split(",")
    
    # Supabase配置
    SUPABASE_URL = os.getenv("SUPABASE_URL", "")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
    
    # 应用配置
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100))
    BATCH_INTERVAL = int(os.getenv("BATCH_INTERVAL", 10))  # 秒
    
    @classmethod
    def validate(cls) -> bool:
        """验证必要配置"""
        required = ["SUPABASE_URL", "SUPABASE_KEY"]
        missing = [var for var in required if not getattr(cls, var)]
        
        if missing:
            print(f"错误: 缺少必要配置: {missing}")
            return False
        return True
    
    @classmethod
    def print_config(cls):
        """打印配置（隐藏敏感信息）"""
        print("当前配置:")
        for key in dir(cls):
            if not key.startswith("_") and key.isupper():
                value = getattr(cls, key)
                # 隐藏敏感信息的部分内容
                if "KEY" in key or "PASSWORD" in key:
                    print(f"  {key}: {'*' * 8}{str(value)[-4:] if value else ''}")
                else:
                    print(f"  {key}: {value}")