import json
import smtplib
import paho.mqtt.client as mqtt
import os
import time
import logging
import ssl  # Thêm import ssl
import redis
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from kafka.errors import KafkaError
# from pymongo import MongoClient
# from pymongo.errors import ConnectionFailure

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CẤU HÌNH TỪ BIẾN MÔI TRƯỜNG ---
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')

# MONGO_URI = os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
# DB_NAME = os.getenv('DB_NAME', 'smart_agriculture')
# ALERT_COLLECTION = 'alerts'

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

# --- CẤU HÌNH ---
KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')
REDIS_HOST = 'redis'
REDIS_PORT = 6379

# --- KẾT NỐI REDIS ---
try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    logger.info("Alert Consumer ket noi Redis thanh cong.")
except Exception as e:
    logger.error(f"Alert Consumer LỖI ket noi Redis: {e}")
    redis_client = None

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
    """Gửi lệnh MQTT để kích hoạt quạt."""
    if not MQTT_BROKER or not MQTT_USERNAME:
        logger.warning("Chưa cấu hình MQTT (BROKER, USERNAME). Bỏ qua gửi lệnh MQTT.")
        return

    try:
        client = mqtt.Client(client_id=f"alert_consumer_actuator_{os.getpid()}")
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT) 
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        payload = json.dumps({"device_id": device_id, "command": command})
        client.publish(MQTT_TOPIC_CONTROL, payload)
        client.disconnect()
        logger.info(f"Đã gửi lệnh MQTT bật quạt cho {device_id} qua {MQTT_BROKER}")
    except Exception as e:
        logger.error(f"Lỗi khi gửi MQTT: {e}")

# --- HÀM XỬ LÝ MESSAGE ---
def process_message(data):
    """Xử lý logic cảnh báo cho mỗi tin nhắn từ Kafka."""
    device_id = data.get('device_id', 'N/A')
    timestamp = data.get('window', {}).get('start', time.time()) 
    alerts_triggered = []
    send_fan_command = False

    alert_type = data.get('Alert_Type')
    
    if alert_type == 'High_Temperature_Alert':
        avg_temp = data.get('avg_temp', 0)
        alerts_triggered.append(f"Nhiệt độ cao: {avg_temp:.2f}°C")
        send_fan_command = True # Kích hoạt quạt
    
    elif alert_type == 'Low_Moisture_Alert':
        avg_moisture = data.get('avg_moisture', 0)
        alerts_triggered.append(f"Độ ẩm thấp: {avg_moisture:.2f}%")
        
    elif alert_type == 'Heat_Stress_Alert':
        stress_index = data.get('Stress_Index', 0)
        alerts_triggered.append(f"Sốc nhiệt: Chỉ số {stress_index}")
        send_fan_command = True
    
    # 3. Gửi cảnh báo nếu có
    if alerts_triggered:
        subject = f"[CẢNH BÁO] Phát hiện vấn đề tại thiết bị {device_id}"
        body = f"Phát hiện các cảnh báo sau tại thời điểm {timestamp}:\n\n" + "\n".join(alerts_triggered)
        
        # Gửi Email
        send_email_alert(subject, body)
        
        # --- GHI VÀO REDIS ---
        alert_doc = {
            "device_id": device_id,
            "timestamp": timestamp,
            "alerts": alerts_triggered,
            "raw_data": data # Lưu toàn bộ JSON cảnh báo từ Spark
        }
        
        if redis_client:
            try:
                # Ghi vào Redis List cho dashboard
                redis_client.lpush("realtime_alert_history", json.dumps(alert_doc))
                redis_client.ltrim("realtime_alert_history", 0, 49) # Giữ 50 cảnh báo
                logger.info(f"Đã lưu cảnh báo vào Redis cho {device_id}")
            except Exception as e:
                logger.error(f"Loi khi ghi canh bao vao Redis: {e}")
        # --- KẾT THÚC GHI REDIS ---

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
                # Parse JSON nhận từ Spark
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                group_id='alert_consumer_group'
            )
            logger.info(f"Kết nối Kafka thành công, đang lắng nghe topic {KAFKA_TOPIC}...")
            return consumer
        except KafkaError as e:
            logger.error(f"Không thể kết nối Kafka. Thử lại sau 5 giây... Lỗi: {e}")
            time.sleep(5)
        except json.JSONDecodeError:
            logger.warning("Loi JSONDecodeError khi ket noi, co the topic chua co du lieu. Thu lai sau 5s...")
            time.sleep(5)

def start_consumer():
    """Khởi động Kafka Consumer."""
    consumer = connect_to_kafka()
    
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Nhan du lieu canh bao: {data}")
            process_message(data)
        except json.JSONDecodeError:
            logger.warning(f"Lỗi decode JSON: {message.value}")
        except Exception as e:
            logger.error(f"Lỗi xử lý tin nhắn: {e}")

if __name__ == "__main__":
    start_consumer()