import os
import json
import ssl
import time
import logging
import smtplib
import threading
from email.mime.text import MIMEText
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_SENSOR_TOPIC = os.getenv('MQTT_TOPIC', 'sensor-data')
MQTT_CONTROL_FAN = 'agriculture/control/fan'
MQTT_CONTROL_IRRIGATION = 'agriculture/control/irrigation'

SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_TO = os.getenv('EMAIL_TO')

# Fire thresholds
FIXED_TEMP_THRESHOLD = 60.0
HUMIDITY_THRESHOLD = 25.0

control_client = None

def send_email_alert(subject, body):
    """Gửi email ngoài luồng, không block critical path."""
    def _send():
        if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
            logger.warning("Chua cau hinh email, bo qua gui email.")
            return
        try:
            msg = MIMEText(body)
            msg['Subject'] = subject
            msg['From'] = EMAIL_USER
            msg['To'] = EMAIL_TO
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=5) as server:
                server.starttls()
                server.login(EMAIL_USER, EMAIL_PASS)
                server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
            logger.info(f"Da gui email canh bao: {subject}")
        except Exception as e:
            logger.error(f"Loi khi gui email: {e}")
    
    threading.Thread(target=_send, daemon=True).start()

def trigger_control_mqtt(device_id, control_type='fan'):
    """Trigger fan/irrigation ngay lập tức."""
    global control_client
    if not control_client:
        logger.warning("Control MQTT client chua san sang.")
        return
    try:
        topic = MQTT_CONTROL_FAN if control_type == 'fan' else MQTT_CONTROL_IRRIGATION
        payload = json.dumps({
            "device_id": device_id,
            "command": "ON",
            "timestamp": int(time.time() * 1000)
        })
        start_publish = time.time()
        result = control_client.publish(topic, payload, qos=0)  # QoS=0 để giảm latency
        end_publish = time.time()
        logger.info(f"MQTT {control_type.upper()} triggered for {device_id} (latency={int((end_publish-start_publish)*1000)}ms)")
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Loi gui lenh MQTT: {result.rc}")
    except Exception as e:
        logger.error(f"Loi khi gui MQTT control: {e}")

def on_message(client, userdata, msg):
    start_time = time.time() 
    try:
        data = json.loads(msg.payload.decode('utf-8'))
        device_id = data.get('device_id', 'unknown')
        temp = data.get('Temperature')
        humidity = data.get('Humidity')

        if temp is None or humidity is None:
            return

        fire_detected = False
        detection_type = None
        severity = "NORMAL"

        if temp >= FIXED_TEMP_THRESHOLD:
            fire_detected = True
            detection_type = "FIXED_TEMPERATURE"
            severity = "CRITICAL"
        elif temp >= 45.0 and humidity <= HUMIDITY_THRESHOLD:
            fire_detected = True
            detection_type = "HIGH_TEMP_LOW_HUMIDITY"
            severity = "HIGH"

        if fire_detected:
            trigger_control_mqtt(device_id, 'fan')
            trigger_control_mqtt(device_id, 'irrigation') # Nước
            critical_path_latency = int((time.time() - start_time) * 1000)
            logger.warning(f"Critical path executed in {critical_path_latency} ms")

            subject = f"[CANH BAO CHAY] {severity} - {detection_type} - {device_id}"
            body = f"""
CANH BAO CHAY!
Loai cam bien: {detection_type}
Thiet bi: {device_id}
Nhiet do: {temp}°C
Do am: {humidity}%
Muc do: {severity}

DA KICH HOAT:
- Quat lam mat
- He thong tuoi nuoc

Thoi gian: {time.strftime('%Y-%m-%d %H:%M:%S')}
"""
            send_email_alert(subject, body)

    except json.JSONDecodeError as e:
        logger.warning(f"Loi decode JSON: {e}")
    except Exception as e:
        logger.error(f"Loi xu ly message: {e}")

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"MQTT connected! Subscribing {MQTT_SENSOR_TOPIC}")
        client.subscribe(MQTT_SENSOR_TOPIC, qos=1)
    else:
        logger.error(f"MQTT connection failed code {rc}")

def on_disconnect(client, userdata, rc):
    logger.warning(f"MQTT disconnected code {rc}")

if __name__ == "__main__":
    try:
        logger.info(f"MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
        logger.info(f"Sensor Topic: {MQTT_SENSOR_TOPIC}")
        logger.info(f"NGUONG CANH BAO: Fixed Temp >= {FIXED_TEMP_THRESHOLD}°C, High Temp + Low Humidity >=45°C & Humidity <= {HUMIDITY_THRESHOLD}%")

        control_client = mqtt.Client(client_id="control_client")
        control_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        ssl_context = ssl.create_default_context()
        control_client.tls_set_context(ssl_context)
        control_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        control_client.loop_start()

        sensor_client = mqtt.Client(client_id="sensor_client")
        sensor_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        sensor_client.tls_set_context(ssl_context)
        sensor_client.on_connect = on_connect
        sensor_client.on_message = on_message
        sensor_client.on_disconnect = on_disconnect

        sensor_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        sensor_client.loop_forever()

    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if control_client:
            control_client.loop_stop()
            control_client.disconnect()
        logger.info("Fire detection system shutdown")
