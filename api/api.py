import os
import json
import redis
import logging
import ssl
import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')

try:
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    redis_client.ping()
    logger.info("API ket noi Redis thanh cong.")
except Exception as e:
    logger.error(f"API LỖI ket noi Redis: {e}")
    redis_client = None

app = FastAPI(title="Smart Agriculture API")

origins = ["http://localhost", "http://localhost:8501"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ControlCommand(BaseModel):
    device_id: str
    action: str
    payload: Optional[dict] = {}

def get_chart_data_from_redis(list_key: str):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        chart_data_json_list = redis_client.lrange(list_key, 0, 49)
        chart_data = []
        for item in chart_data_json_list:
            data = json.loads(item)
            ts = data.get("event_timestamp")
            
            if ts is None:
                ts_str = data.get("timestamp")
                if ts_str:
                    
                    if isinstance(ts_str, (int, float)):
                        ts = int(ts_str)
                    else:
                        
                        from datetime import datetime
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        ts = int(dt.timestamp() * 1000)
            
            chart_data.append({
                "event_timestamp": ts,
                "value": data.get("value", data.get("prediction"))
            })
        chart_data.reverse()
        return chart_data
    except Exception as e:
        logger.error(f"Loi doc Redis List '{list_key}': {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def read_root():
    return {"message": "Smart Agriculture API"}

@app.get("/metrics/realtime")
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

@app.get("/charts/realtime_temp")
async def get_chart_temp():
    return get_chart_data_from_redis("chart_realtime_temp")

@app.get("/charts/realtime_moisture")
async def get_chart_moisture():
    return get_chart_data_from_redis("chart_realtime_moisture")

@app.get("/predictions/model")
async def get_model_predictions():
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        predictions_json = redis_client.get("dashboard_model_predictions")
        if predictions_json:
            data = json.loads(predictions_json)
            return {
                "event_timestamp": data.get("event_timestamp", data.get("timestamp")),
                "nutrition_level": data.get("NDI_Prediction"),  
                "pest_disease_level": data.get("PDI_Prediction"),
                "action_suggestion": data.get("Action_Suggestion"),  # NEW
                "nutrition_confidence": data.get("NDI_confidence", 0),
                "pest_disease_confidence": data.get("PDI_confidence", 0),
                "action_confidence": data.get("Action_confidence", 0)  # NEW
            }
        else:
            return {"message": "Dang cho du lieu tu model..."}
    except Exception as e:
        logger.error(f"Loi doc Redis predictions: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/charts/nutrition_level")
async def get_chart_nutrition():
    return get_chart_data_from_redis("chart_realtime_ndi_prediction")

@app.get("/charts/pest_disease_level")  
async def get_chart_pest_disease():
    return get_chart_data_from_redis("chart_realtime_pdi_prediction")

@app.get("/charts/action_suggestion")
async def get_chart_action():
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        chart_data_json_list = redis_client.lrange("chart_realtime_action_prediction", 0, 49)
        chart_data = []
        for item in chart_data_json_list:
            data = json.loads(item)
            ts = data.get("event_timestamp")
            
            if ts is None:
                ts_str = data.get("timestamp")
                if ts_str:
                    # Check if timestamp is already integer (milliseconds)  
                    if isinstance(ts_str, (int, float)):
                        ts = int(ts_str)
                    else:
                        # Convert ISO string to milliseconds
                        from datetime import datetime
                        dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                        ts = int(dt.timestamp() * 1000)
            
            chart_data.append({
                "event_timestamp": ts,
                "value": data.get("prediction"),
                "confidence": data.get("confidence", 0)
            })
        chart_data.reverse()
        return chart_data
    except Exception as e:
        logger.error(f"Loi doc Redis Action chart: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/alerts/realtime")
async def get_realtime_alerts(limit: int = 20):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        alert_json_list = redis_client.lrange("realtime_alert_history", 0, limit - 1)
        alerts = []
        for item in alert_json_list:
            data = json.loads(item)
            ts = data.get("event_timestamp", data.get("timestamp"))

            if ts is not None and not isinstance(ts, (int, float)):
                if isinstance(ts, str):
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                    ts = int(dt.timestamp() * 1000)
            elif ts is not None:
                ts = int(ts)
                
            alerts.append({
                "event_timestamp": ts,
                **data
            })
        alerts.reverse()
        return alerts
    except Exception as e:
        logger.error(f"Loi doc Redis List (Alerts): {e}")
        return []

@app.get("/raw_sensor_data")
async def get_raw_sensor_data(limit: int = 100):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis service khong san sang.")
    try:
        raw_data_list = redis_client.lrange("raw_sensor_data", 0, limit - 1)
        raw_data = []
        for item in raw_data_list:
            data = json.loads(item)
            
            # Handle timestamp conversion
            ts = data.get("event_timestamp", data.get("timestamp"))
            
            # If timestamp is ISO string, convert to milliseconds
            if ts and isinstance(ts, str) and 'T' in ts:
                from datetime import datetime
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                ts = int(dt.timestamp() * 1000)
            elif ts and isinstance(ts, (int, float)):
                ts = int(ts)
                # If it's seconds, convert to milliseconds
                if ts < 1e10:  # Less than year 2286
                    ts = ts * 1000
                    
            raw_data.append({
                "event_timestamp": ts,
                **data
            })
        raw_data.reverse()
        return raw_data
    except Exception as e:
        logger.error(f"Loi doc raw sensor data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/control/")
async def send_control_command(command: ControlCommand):
    logger.info(f"Nhan lenh dieu khien: {command.action} cho device {command.device_id}")
    
    if not MQTT_BROKER or not MQTT_USERNAME:
        logger.error("MQTT chua duoc cau hinh trong bien moi truong")
        logger.error(f"MQTT_BROKER: {MQTT_BROKER}, MQTT_USERNAME: {MQTT_USERNAME}")
        raise HTTPException(status_code=500, detail="API chua cau hinh MQTT")
    
    try:
        # DEBUG: Log thong tin ket noi
        logger.info(f"Ket noi MQTT: {MQTT_BROKER}:{MQTT_PORT}")
        
        client = mqtt.Client(client_id=f"api_controller_{os.getpid()}_{int(time.time())}")
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
        
        # Them callback de debug ket noi
        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("MQTT Connected successfully!")
            else:
                logger.error(f"MQTT Connection failed: {rc}")
        
        def on_publish(client, userdata, mid):
            logger.info(f"Message published successfully, mid: {mid}")
        
        client.on_connect = on_connect
        client.on_publish = on_publish
        
        # Ket noi voi timeout
        logger.info("Dang ket noi MQTT...")
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        
        # Cho ket noi thanh cong
        time.sleep(1)
        
        if command.action == 'fan_on':
            topic = 'agriculture/control/fan'
        elif command.action == 'irrigation_on':
            topic = 'agriculture/control/irrigation'
        else:
            logger.error(f"Action khong hop le: {command.action}")
            raise HTTPException(status_code=400, detail="Hanh dong khong hop le")

        payload = json.dumps({
            "device_id": command.device_id,
            "command": "ON",
            "params": command.payload,
            "timestamp": int(time.time() * 1000)
        })
        
        logger.info(f"Publishing to {topic}: {payload}")
        
        # Publish voi QoS=1 de dam bao gui thanh cong
        result = client.publish(topic, payload, qos=1)
        
        # Cho message duoc gui
        time.sleep(0.5)
        
        logger.info(f"Publish result: rc={result.rc}, mid={result.mid}")
        
        client.disconnect()
        logger.info(f"API da gui lenh MQTT: {topic} - {payload}")
        
        return {"status": "success", "message": f"Da gui lenh {command.action} den {command.device_id}"}
        
    except Exception as e:
        logger.error(f"Loi khi gui MQTT: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Loi khi gui MQTT: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)