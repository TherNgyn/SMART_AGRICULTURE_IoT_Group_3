import json
import smtplib
import os
import time
import logging
import redis
from email.mime.text import MIMEText
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA_SERVER = os.getenv('KAFKA_SERVER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC_ALERTS', 'alerts')

SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', 587))
EMAIL_USER = os.getenv('EMAIL_USER')
EMAIL_PASS = os.getenv('EMAIL_PASS')
EMAIL_TO = os.getenv('EMAIL_TO')

REDIS_HOST = 'redis'
REDIS_PORT = 6379

try:
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
    logger.info("Alert Consumer ket noi Redis thanh cong.")
except Exception as e:
    logger.error(f"Alert Consumer LỖI ket noi Redis: {e}")
    redis_client = None

def send_email_alert(subject, body):
    if not EMAIL_USER or not EMAIL_PASS or not EMAIL_TO:
        logger.warning("Chua cau hinh email. Bo qua gui email.")
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
        logger.info(f"Da gui email canh bao: {subject}")
    except Exception as e:
        logger.error(f"Loi khi gui email: {e}")

def process_message(data):
    device_id = data.get('device_id', 'N/A')

    window_start = data.get('window', [None, None])[0]
    window_end = data.get('window', [None, None])[1]
    
    timestamp = window_start if window_start else time.time() 
    
    alerts_triggered = []

    alert_type = data.get('Alert_Type')
    
    if alert_type == 'High_Temperature_Alert':
        avg_temp = data.get('avg_temp', 0)
        alerts_triggered.append(f"Nhiet do cao: {avg_temp:.2f}°C")
    
    elif alert_type == 'Low_Moisture_Alert':
        avg_moisture = data.get('avg_moisture', 0)
        alerts_triggered.append(f"Do am thap: {avg_moisture:.2f}%")
    
    if alerts_triggered:
        subject = f"[CANH BAO] Phat hien van de tai thiet bi {device_id}"
        body = f"Phat hien cac canh bao sau tai thoi diem {timestamp}:\n\n" + "\n".join(alerts_triggered)
        
        send_email_alert(subject, body)
        
        alert_doc = {
            "device_id": device_id,
            "timestamp": timestamp,
            "alerts": alerts_triggered,
            "raw_data": data
        }
        
        if redis_client:
            try:
                redis_client.lpush("realtime_alert_history", json.dumps(alert_doc))
                redis_client.ltrim("realtime_alert_history", 0, 49)
                logger.info(f"Da luu canh bao vao Redis cho {device_id}")
            except Exception as e:
                logger.error(f"Loi khi ghi canh bao vao Redis: {e}")

def connect_to_kafka():
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
            logger.info(f"Ket noi Kafka thanh cong, dang lang nghe topic {KAFKA_TOPIC}...")
            return consumer
        except KafkaError as e:
            logger.error(f"Khong the ket noi Kafka. Thu lai sau 5 giay... Loi: {e}")
            time.sleep(5)
        except json.JSONDecodeError:
            logger.warning("Loi JSONDecodeError khi ket noi. Thu lai sau 5s...")
            time.sleep(5)

def start_consumer():
    consumer = connect_to_kafka()
    
    for message in consumer:
        try:
            data = message.value
            logger.info(f"Nhan du lieu canh bao: {data}")
            process_message(data)
        except json.JSONDecodeError:
            logger.warning(f"Loi decode JSON: {message.value}")
        except Exception as e:
            logger.error(f"Loi xu ly tin nhan: {e}")

if __name__ == "__main__":
    start_consumer()