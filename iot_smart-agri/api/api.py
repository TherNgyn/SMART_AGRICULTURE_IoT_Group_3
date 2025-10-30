import os
import json
import redis
import logging
import ssl
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CẤU HÌNH ---
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
# ------------------

# --- Kết nối Redis ---
try:
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    redis_client.ping()
    logger.info("API ket noi Redis thanh cong.")
except Exception as e:
    logger.error(f"API LỖI ket noi Redis: {e}")
    redis_client = None

app = FastAPI(title="Smart Agriculture API - Realtime")

# --- CẤU HÌNH CORS ---
origins = ["http://localhost", "http://localhost:8501"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Models ---
class ControlCommand(BaseModel):
    device_id: str
    action: str
    payload: Optional[dict] = {}

# === HÀM HELPER ĐỂ ĐỌC REDIS LIST CHO BIỂU ĐỒ ===
def get_chart_data_from_redis(list_key: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        chart_data_json_list = redis_client.lrange(list_key, 0, 49)
        chart_data = [json.loads(item) for item in chart_data_json_list]
        chart_data.reverse() # Đảo ngược để có thứ tự thời gian tăng dần
        return chart_data
    except Exception as e:
        logger.error(f"Loi doc Redis List '{list_key}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- API Endpoints ---

@app.get("/")
def read_root():
    return {"message": "Chào mừng đến với Smart Agriculture API (Realtime / Redis)"}

# === ENDPOINT CHO CHỈ SỐ ===
@app.get("/metrics/realtime", summary="Lấy chỉ số tổng hợp realtime từ Redis")
async def get_realtime_metrics():
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        metrics_json = redis_client.get("dashboard_realtime_metrics")
        if metrics_json:
            return json.loads(metrics_json)
        else:
            return {"message": "Dang cho du lieu tu Spark..."}
    except Exception as e:
        logger.error(f"Loi doc Redis: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# === ENDPOINTS CHO BIỂU ĐỒ ===
@app.get("/charts/realtime_temp", summary="Lấy dữ liệu biểu đồ nhiệt độ realtime")
async def get_chart_temp():
    return get_chart_data_from_redis("chart_realtime_temp")

@app.get("/charts/realtime_moisture", summary="Lấy dữ liệu biểu đồ độ ẩm realtime")
async def get_chart_moisture():
    return get_chart_data_from_redis("chart_realtime_moisture")

@app.get("/charts/realtime_ndi", summary="Lấy dữ liệu biểu đồ NDI realtime")
async def get_chart_ndi():
    return get_chart_data_from_redis("chart_realtime_ndi")

@app.get("/charts/realtime_pdi", summary="Lấy dữ liệu biểu đồ PDI realtime")
async def get_chart_pdi():
    return get_chart_data_from_redis("chart_realtime_pdi")

@app.get("/charts/realtime_rgb", summary="Lấy dữ liệu biểu đồ RGB realtime")
async def get_chart_rgb():
    return get_chart_data_from_redis("chart_realtime_rgb")

# === ENDPOINT CHO LỊCH SỬ CẢNH BÁO ===
@app.get("/alerts/realtime", summary="Lấy lịch sử cảnh báo realtime từ Redis")
async def get_realtime_alerts(limit: int = 20):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        alert_json_list = redis_client.lrange("realtime_alert_history", 0, limit - 1)
        alerts = [json.loads(item) for item in alert_json_list]
        return alerts
    except Exception as e:
        logger.error(f"Loi doc Redis List (Alerts): {e}")
        return []

# === ENDPOINT ĐIỀU KHIỂN ===
@app.post("/control/", summary="Gửi lệnh điều khiển (Tưới nước/Quạt)")
async def send_control_command(command: ControlCommand):
    if not MQTT_BROKER or not MQTT_USERNAME:
        logger.warning("API chưa cấu hình MQTT (BROKER, USERNAME). Không thể gửi lệnh.")
        raise HTTPException(status_code=500, detail="API chưa cấu hình MQTT")
    try:
        client = mqtt.Client(client_id=f"api_controller_{os.getpid()}")
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
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