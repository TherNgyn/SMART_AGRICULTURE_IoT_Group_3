import json
import smtplib
import paho.mqtt.client as mqtt
import os
import time
import logging
import ssl  # Thêm import ssl
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG ---
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')

MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.getenv('DB_NAME', 'smart_agriculture')
ALERT_COLLECTION = 'alerts'

# SMTP (Email)
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_TO = os.getenv('EMAIL_TO')

# MQTT (HiveMQ Cloud)
MQTT_BROKER = os.getenv('MQTT_BROKER')
MQTT_PORT = int(os.getenv('MQTT_PORT', 8883))
MQTT_USERNAME = os.getenv('MQTT_USERNAME')
MQTT_PASSWORD = os.getenv('MQTT_PASSWORD')
MQTT_TOPIC_CONTROL = 'agriculture/control/fan'
# -----------------------------------

# --- Kết nối MongoDB ---
try:
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    mongo_client.server_info() 
    db = mongo_client[DB_NAME]
    alert_collection = db[ALERT_COLLECTION]
    logger.info("Kết nối MongoDB thành công.")
except ConnectionFailure as e:
    logger.error(f"Lỗi kết nối MongoDB: {e}")
    exit(1)

def send_email_alert(subject, body):
    """Gửi cảnh báo qua Email."""
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        logger.warning("Chưa cấu hình email (EMAIL_USER, EMAIL_PASS, EMAIL_TO). Bỏ qua gửi email.")
        return

    try:
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_USER
        msg['To'] = EMAIL_TO

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASS)
            server.sendmail(EMAIL_USER, [EMAIL_TO], msg.as_string())
        logger.info(f"Đã gửi email cảnh báo: {subject}")
    except Exception as e:
        logger.error(f"Lỗi khi gửi email: {e}")

def trigger_fan_mqtt(device_id, command='ON'):
    """Gửi lệnh MQTT để kích hoạt quạt (Hỗ trợ HiveMQ Cloud)."""
    if not MQTT_BROKER or not MQTT_USERNAME:
        logger.warning("Chưa cấu hình MQTT (BROKER, USERNAME). Bỏ qua gửi lệnh MQTT.")
        return

    try:
        client = mqtt.Client(client_id=f"alert_consumer_actuator_{os.getpid()}")
        
        # --- CẬP NHẬT CHO HIVEMQ ---
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        # Sử dụng context SSL mặc định cho TLS
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT) 
        # ----------------------------

        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        payload = json.dumps({"device_id": device_id, "command": command})
        client.publish(MQTT_TOPIC_CONTROL, payload)
        client.disconnect()
        logger.info(f"Đã gửi lệnh MQTT bật quạt cho {device_id} qua {MQTT_BROKER}")
    except Exception as e:
        logger.error(f"Lỗi khi gửi MQTT: {e}")

def process_message(data):
    """Xử lý logic cảnh báo cho mỗi tin nhắn từ Kafka."""
    device_id = data.get('device_id', 'N/A')
    timestamp = data.get('timestamp')
    alerts_triggered = []
    send_fan_command = False

    # 1. Kiểm tra ngưỡng cảm biến
    temp = data.get('Temperature')
    moisture = data.get('Moisture')
    rgb_index = data.get('RGB_Index') 

    if temp is not None and temp > 35:
        alerts_triggered.append(f"Nhiệt độ cao: {temp:.2f}°C")
        send_fan_command = True

    if moisture is not None and moisture < 20:
        alerts_triggered.append(f"Độ ẩm thấp: {moisture:.2f}%")

    if rgb_index is not None and rgb_index > 0.7:
        alerts_triggered.append(f"Chỉ số RGB cao (bệnh): {rgb_index:.2f}")

    # 2. Kiểm tra nhãn dự đoán (NDI_Label, PDI_Label)
    ndi_label = data.get('NDI_Label')
    pdi_label = data.get('PDI_Label')

    if ndi_label in [1, 2]:
        alerts_triggered.append(f"Cảnh báo NDI: Mức {ndi_label}")
    
    if pdi_label in [1, 2]:
        alerts_triggered.append(f"Cảnh báo PDI: Mức {pdi_label}")

    # 3. Gửi cảnh báo nếu có
    if alerts_triggered:
        subject = f"[CẢNH BÁO] Phát hiện vấn đề tại thiết bị {device_id}"
        body = f"Phát hiện các cảnh báo sau tại thời điểm {timestamp}:\n\n" + "\n".join(alerts_triggered)
        
        send_email_alert(subject, body)
        
        alert_doc = {
            "device_id": device_id,
            "timestamp": timestamp,
            "alerts": alerts_triggered,
            "raw_data": data
        }
        alert_collection.insert_one(alert_doc)
        logger.info(f"Đã lưu cảnh báo vào MongoDB cho {device_id}")

        if send_fan_command:
            trigger_fan_mqtt(device_id, 'ON')

def connect_to_kafka():
    """Hàm kết nối Kafka với cơ chế retry."""
    consumer = None
    while consumer is None:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                auto_offset_reset='latest',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='alert_consumer_group'
            )
            logger.info(f"Kết nối Kafka thành công, đang lắng nghe topic {KAFKA_TOPIC}...")
            return consumer
        except KafkaError as e:
            logger.error(f"Không thể kết nối Kafka. Thử lại sau 5 giây... Lỗi: {e}")
            time.sleep(5)

def start_consumer():
    """Khởi động Kafka Consumer."""
    consumer = connect_to_kafka()
    
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Nhận dữ liệu: {data}")
            process_message(data)
        except json.JSONDecodeError:
            logger.warning(f"Lỗi decode JSON: {message.value}")
        except Exception as e:
            logger.error(f"Lỗi xử lý tin nhắn: {e}")

if __name__ == "__main__":
    start_consumer()