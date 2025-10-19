import os
import json
import logging
import ssl  # Thêm import ssl
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pydantic import BaseModel
from typing import List, Optional

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'smart_agriculture')
SENSOR_COLLECTION = 'sensor_data'
ALERT_COLLECTION = 'alerts'

# MQTT (HiveMQ Cloud)
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
# ------------------

app = FastAPI(title="Smart Agriculture API")

# --- CẤU HÌNH CORS ---
# Cho phép dashboard gọi API
origins = [
    "http://localhost",
    "http://localhost:8501",  # Địa chỉ của Streamlit
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------

# --- Kết nối MongoDB ---
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info()
    db = mongo_client[DB_NAME]
    sensor_collection = db[SENSOR_COLLECTION]
    alert_collection = db[ALERT_COLLECTION]
    logger.info("API kết nối MongoDB thành công.")
except ConnectionFailure as e:
    logger.error(f"API Lỗi kết nối MongoDB: {e}")
    
# --- Models ---
class ControlCommand(BaseModel):
    device_id: str
    action: str  # 'irrigation_on', 'fan_on'
    payload: Optional[dict] = {}

# --- API Endpoints ---

@app.get("/")
def read_root():
    return {"message": "Chào mừng đến với Smart Agriculture API"}

@app.get("/sensor-data/", summary="Lấy dữ liệu cảm biến mới nhất")
async def get_sensor_data(limit: int = 100):
    """
    Đọc dữ liệu cảm biến (đã qua xử lý) từ MongoDB.
    """
    try:
        data = list(sensor_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
        return data
    except Exception as e:
        logger.error(f"Lỗi đọc sensor-data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/", summary="Lấy lịch sử cảnh báo")
async def get_alert_history(limit: int = 50):
    """
    Đọc lịch sử các cảnh báo đã được consumer ghi lại.
    """
    try:
        data = list(alert_collection.find({}, {"_id": 0}).sort("timestamp", -1).limit(limit))
        return data
    except Exception as e:
        logger.error(f"Lỗi đọc alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/control/", summary="Gửi lệnh điều khiển (Tưới nước/Quạt)")
async def send_control_command(command: ControlCommand):
    """
    Nhận lệnh từ giao diện web và publish tới MQTT (Hỗ trợ HiveMQ Cloud).
    """
    if not MQTT_BROKER or not MQTT_USERNAME:
        logger.warning("API chưa cấu hình MQTT (BROKER, USERNAME). Không thể gửi lệnh.")
        raise HTTPException(status_code=500, detail="API chưa cấu hình MQTT")

    try:
        client = mqtt.Client(client_id=f"api_controller_{os.getpid()}")

        # --- CẬP NHẬT CHO HIVEMQ ---
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        # ----------------------------
        
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        if command.action == 'fan_on':
            topic = 'agriculture/control/fan'
        elif command.action == 'irrigation_on':
            topic = 'agriculture/control/irrigation'
        else:
            raise HTTPException(status_code=400, detail="Hành động không hợp lệ")

        payload = json.dumps({
            "device_id": command.device_id,
            "command": "ON",
            "params": command.payload
        })
        
        client.publish(topic, payload)
        client.disconnect()
        
        logger.info(f"API đã gửi lệnh MQTT: {topic} - {payload} qua {MQTT_BROKER}")
        return {"status": "success", "message": f"Đã gửi lệnh {command.action} đến {command.device_id}"}
        
    except Exception as e:
        logger.error(f"Lỗi khi gửi MQTT: {e}")
        raise HTTPException(status_code=500, detail=f"Lỗi khi gửi MQTT: {str(e)}")

# Dòng này chỉ dùng để test local, khi chạy bằng uvicorn trong Docker thì không cần
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)